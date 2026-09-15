from __future__ import annotations

import json
import uuid
from typing import Any, Mapping
from urllib.parse import urlsplit

from sqlalchemy import text

QUEUE = "gfo_render_operator_queue"
# Mission checkpoints deliberately reuse the existing canonical queue contract.
# The request_id prefix + mission_contract distinguish controller rows from engine rows.
MISSION_OPERATION = "FIND_GOLD_BATCH"
MISSION_SCHEMA = "gfo.operator-request.v1"
MISSION_CONTRACT = "REQUEST_SCOPED_NEW_GOLD_60_40_V1"
_CANONICAL_QUEUE_STATUSES = {"PENDING", "RUNNING", "SUCCEEDED", "FAILED"}
# Mission rows are durable controller checkpoints, not executable transport jobs.
# Store every live checkpoint as SUCCEEDED so the legacy Render stale-job sweep
# cannot claim or fail it while Motor/authority work is legitimately in flight.
MISSION_CHECKPOINT_STORAGE_STATUS = "SUCCEEDED"
TERMINAL_MISSION_STATES = {
    "TARGET_MET",
    "STOPPED_BY_USER",
    "FAILED",
    "FAILED_WITH_PROGRESS",
    "MOTOR_STOPPED_WITHOUT_AUTHORITY_BATCH",
}


def balanced_targets(total: int) -> tuple[int, int]:
    if total < 2:
        raise RuntimeError("mission FIND target must be at least 2")
    agency = (total * 6 + 9) // 10
    agency = min(agency, total - 1)
    direct = total - agency
    return agency, direct


def mission_request_id(mission_id: str) -> str:
    return f"mission-find_gold-{mission_id}"


def _normalize_source_url(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
    except ValueError:
        return raw.casefold()
    host = parts.netloc.casefold().removeprefix("www.")
    path = parts.path.rstrip("/") or "/"
    return f"{parts.scheme.casefold() or 'https'}://{host}{path}"


def _candidate_frontier(stage_result: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    raw = stage_result.get("engine_candidates")
    if isinstance(raw, (str, bytes)) or not isinstance(raw, list):
        return [], []
    sources: list[str] = []
    routes: list[str] = []
    for lead in raw:
        if not isinstance(lead, Mapping):
            continue
        search = lead.get("search")
        route = lead.get("contact_route")
        source = _normalize_source_url(
            search.get("source_url") if isinstance(search, Mapping) else ""
        )
        email = str(
            route.get("route_value") if isinstance(route, Mapping) else ""
        ).strip().lower()
        if source:
            sources.append(source)
        if "@" in email:
            routes.append(email)
    return list(dict.fromkeys(sources)), list(dict.fromkeys(routes))


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    return {}


def _current_fast_cash_counts(connection) -> dict[str, int]:
    rows = connection.execute(
        text(
            """
            SELECT entity_id, payload
            FROM critical_events
            WHERE release_id = 'R0007'
              AND event_type = 'FAST_CASH_ELIGIBLE'
              AND entity_type = 'BUYER'
            ORDER BY occurred_at ASC, event_id ASC
            """
        )
    ).mappings().all()
    latest: dict[str, str] = {}
    for row in rows:
        payload = _json_object(row.get("payload"))
        mode = str(payload.get("buyer_mode") or "")
        if mode in {"AGENCY", "DIRECT"}:
            latest[str(row["entity_id"])] = mode
    return {
        "AGENCY": sum(1 for value in latest.values() if value == "AGENCY"),
        "DIRECT": sum(1 for value in latest.values() if value == "DIRECT"),
    }


def _load_locked(connection, mission_id: str) -> Mapping[str, Any] | None:
    return connection.execute(
        text(
            f"""
            SELECT *
            FROM {QUEUE}
            WHERE request_id = :request_id
              AND operation = :operation
              AND payload->>'mission_contract' = :contract
            ORDER BY created_at DESC
            LIMIT 1
            FOR UPDATE
            """
        ),
        {
            "request_id": mission_request_id(mission_id),
            "operation": MISSION_OPERATION,
            "contract": MISSION_CONTRACT,
        },
    ).mappings().first()


def ensure_mission(
    transport,
    *,
    mission_id: str,
    requested_new_gold: int,
    source_request_id: str,
) -> dict[str, Any]:
    agency_target, direct_target = balanced_targets(requested_new_gold)
    db = transport.engine()
    try:
        with db.begin() as connection:
            connection.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": f"GFO-001:find-gold-mission:{mission_id}"},
            )
            existing = _load_locked(connection, mission_id)
            if existing is not None:
                payload = _json_object(existing.get("payload"))
                if int(payload.get("requested_new_gold") or 0) != requested_new_gold:
                    raise RuntimeError("mission target cannot be rebound")
                return {
                    "payload": payload,
                    "result": _json_object(existing.get("result")),
                    "status": str(existing.get("status") or ""),
                }

            baseline = _current_fast_cash_counts(connection)
            mission_req_id = mission_request_id(mission_id)
            payload = {
                "schema": MISSION_SCHEMA,
                "operation": MISSION_OPERATION,
                "release_id": "R0007",
                "request_id": mission_req_id,
                "target_count": requested_new_gold,
                "mission_contract": MISSION_CONTRACT,
                "mission_record": True,
                "transport_claimable": False,
                "checkpoint_record": True,
                "mission_id": mission_id,
                "requested_new_gold": requested_new_gold,
                "target_mix": {"AGENCY": agency_target, "DIRECT": direct_target},
                "mission_fast_cash_baseline": baseline,
                "source_request_id": source_request_id,
                "send_authority": "NOT_GRANTED",
                "outbound_side_effects": False,
            }
            result = {
                "state": "RUNNING",
                "wave": 1,
                "found_new_gold": 0,
                "mission_new_gold": 0,
                "mission_target": requested_new_gold,
                "deficit": requested_new_gold,
                "found_agency": 0,
                "found_direct": 0,
                "last_request_id": source_request_id,
                "last_checkpoint": "MISSION_STARTED",
                "next_action": "RUN_MOTOR",
                "stop_requested": False,
                "seen_source_urls": [],
                "seen_routes": [],
            }
            connection.execute(
                text(
                    f"""
                    INSERT INTO {QUEUE} (
                        job_id, request_id, operation, payload, status, result, error,
                        attempts, created_at, started_at, finished_at, heartbeat_at, lease_owner
                    )
                    VALUES (
                        CAST(:job_id AS uuid), :request_id, :operation, CAST(:payload AS jsonb),
                        :storage_status, CAST(:result AS jsonb), NULL,
                        1, now(), now(), now(), now(), 'github-public-runner-mission-checkpoint'
                    )
                    """
                ),
                {
                    "job_id": str(uuid.uuid4()),
                    "request_id": mission_req_id,
                    "operation": MISSION_OPERATION,
                    "payload": json.dumps(payload),
                    "result": json.dumps(result),
                    "storage_status": MISSION_CHECKPOINT_STORAGE_STATUS,
                },
            )
            return {
                "payload": payload,
                "result": result,
                "status": MISSION_CHECKPOINT_STORAGE_STATUS,
            }
    finally:
        db.dispose()


def load_mission(transport, *, mission_id: str) -> dict[str, Any]:
    db = transport.engine()
    try:
        with db.begin() as connection:
            row = connection.execute(
                text(
                    f"""
                    SELECT payload, result, status, error
                    FROM {QUEUE}
                    WHERE request_id = :request_id
                      AND operation = :operation
                      AND payload->>'mission_contract' = :contract
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {
                    "request_id": mission_request_id(mission_id),
                    "operation": MISSION_OPERATION,
                    "contract": MISSION_CONTRACT,
                },
            ).mappings().first()
    finally:
        db.dispose()
    if row is None:
        raise RuntimeError("mission state not found")
    return {
        "payload": _json_object(row.get("payload")),
        "result": _json_object(row.get("result")),
        "status": str(row.get("status") or ""),
        "error": row.get("error"),
    }


def mission_fields(mission: Mapping[str, Any], *, wave: int | None = None) -> dict[str, Any]:
    payload = _json_object(mission.get("payload"))
    result = _json_object(mission.get("result"))
    selected_wave = int(wave if wave is not None else result.get("wave") or 1)
    return {
        "mission_id": str(payload["mission_id"]),
        "mission_contract": MISSION_CONTRACT,
        "mission_requested_new_gold": int(payload["requested_new_gold"]),
        "mission_fast_cash_baseline": dict(payload["mission_fast_cash_baseline"]),
        "mission_target_mix": dict(payload["target_mix"]),
        "mission_wave": selected_wave,
        "mission_seen_source_urls": list(result.get("seen_source_urls") or ()),
        "mission_seen_routes": list(result.get("seen_routes") or ()),
    }


def _update(
    transport,
    *,
    mission_id: str,
    state: str,
    status: str | None = None,
    next_action: str | None = None,
    wave: int | None = None,
    found_agency: int | None = None,
    found_direct: int | None = None,
    last_request_id: str | None = None,
    candidate_count: int | None = None,
    survivor_count: int | None = None,
    last_checkpoint: str | None = None,
    failure_at: str | None = None,
    error: str | None = None,
    stop_requested: bool | None = None,
    seen_source_urls: list[str] | None = None,
    seen_routes: list[str] | None = None,
) -> dict[str, Any]:
    db = transport.engine()
    try:
        with db.begin() as connection:
            connection.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": f"GFO-001:find-gold-mission:{mission_id}"},
            )
            row = _load_locked(connection, mission_id)
            if row is None:
                raise RuntimeError("mission state not found")
            result = _json_object(row.get("result"))
            result["state"] = state
            if next_action is not None:
                result["next_action"] = next_action
            if wave is not None:
                result["wave"] = int(wave)
            if found_agency is not None:
                result["found_agency"] = int(found_agency)
            if found_direct is not None:
                result["found_direct"] = int(found_direct)
            result["found_new_gold"] = int(result.get("found_agency") or 0) + int(result.get("found_direct") or 0)
            if last_request_id is not None:
                result["last_request_id"] = last_request_id
            if candidate_count is not None:
                result["candidate_count"] = int(candidate_count)
            if survivor_count is not None:
                result["survivor_count"] = int(survivor_count)
            if last_checkpoint is not None:
                result["last_checkpoint"] = str(last_checkpoint)
            if failure_at is not None:
                result["failure_at"] = str(failure_at)
            if stop_requested is not None:
                result["stop_requested"] = bool(stop_requested)
            if seen_source_urls is not None:
                result["seen_source_urls"] = list(
                    dict.fromkeys(
                        [*(result.get("seen_source_urls") or ()), *seen_source_urls]
                    )
                )
            if seen_routes is not None:
                result["seen_routes"] = list(
                    dict.fromkeys(
                        [*(result.get("seen_routes") or ()), *seen_routes]
                    )
                )
            target = int(_json_object(row.get("payload")).get("requested_new_gold") or 0)
            result["mission_target"] = target
            result["mission_new_gold"] = int(result["found_new_gold"])
            result["deficit"] = max(target - int(result["found_new_gold"]), 0)
            selected_status = status or str(row.get("status") or MISSION_CHECKPOINT_STORAGE_STATUS)
            if selected_status == "RUNNING":
                selected_status = MISSION_CHECKPOINT_STORAGE_STATUS
            if selected_status not in _CANONICAL_QUEUE_STATUSES:
                raise RuntimeError(f"mission controller attempted invalid queue status: {selected_status}")
            finished = selected_status in {"SUCCEEDED", "FAILED"}
            connection.execute(
                text(
                    f"""
                    UPDATE {QUEUE}
                    SET status = :status,
                        result = CAST(:result AS jsonb),
                        error = :error,
                        heartbeat_at = now(),
                        finished_at = CASE WHEN :finished THEN now() ELSE NULL END
                    WHERE job_id = CAST(:job_id AS uuid)
                    """
                ),
                {
                    "status": selected_status,
                    "result": json.dumps(result),
                    "error": error,
                    "finished": finished,
                    "job_id": str(row["job_id"]),
                },
            )
            return result
    finally:
        db.dispose()


def record_stage1(
    transport,
    *,
    mission_id: str,
    wave: int,
    request_id: str,
    stage1_result: Mapping[str, Any],
) -> dict[str, Any]:
    state = str(stage1_result.get("status") or "")
    candidates = stage1_result.get("engine_candidates")
    candidate_count = len(candidates) if isinstance(candidates, list) else 0
    seen_source_urls, seen_routes = _candidate_frontier(stage1_result)
    if state == "EXTERNAL_AUTHORITY_BATCH_REQUIRED":
        return _update(
            transport,
            mission_id=mission_id,
            state="AUTHORITY_REQUIRED",
            status="RUNNING",
            next_action="RUN_EXTERNAL_AUTHORITY",
            wave=wave,
            last_request_id=request_id,
            candidate_count=candidate_count,
            last_checkpoint=f"PASS_{wave}_MOTOR_DONE",
            seen_source_urls=seen_source_urls,
            seen_routes=seen_routes,
        )
    if state == "TARGET_MET":
        return _update(
            transport,
            mission_id=mission_id,
            state="TARGET_MET",
            status="SUCCEEDED",
            next_action="NONE",
            wave=wave,
            last_request_id=request_id,
            last_checkpoint=f"PASS_{wave}_MOTOR_DONE_TARGET_MET",
            seen_source_urls=seen_source_urls,
            seen_routes=seen_routes,
        )
    return _update(
        transport,
        mission_id=mission_id,
        state="MOTOR_STOPPED_WITHOUT_AUTHORITY_BATCH",
        status="FAILED",
        next_action="REPORT_PROGRESS",
        wave=wave,
        last_request_id=request_id,
        candidate_count=candidate_count,
        last_checkpoint=f"PASS_{wave}_MOTOR_DONE",
        failure_at=f"PASS_{wave}_MOTOR",
        error=f"Motor stage ended with {state or 'UNKNOWN'}",
        seen_source_urls=seen_source_urls,
        seen_routes=seen_routes,
    )


def record_authority_done(
    transport,
    *,
    mission_id: str,
    wave: int,
    request_id: str,
    survivor_count: int,
) -> dict[str, Any]:
    """Checkpoint a real four-source receipt immediately before canonical Gold."""
    return _update(
        transport,
        mission_id=mission_id,
        state="FOUR_SOURCE_DONE",
        status="RUNNING",
        next_action="RUN_GOLD",
        wave=wave,
        last_request_id=request_id,
        survivor_count=survivor_count,
        last_checkpoint=f"PASS_{wave}_FOUR_SOURCE_DONE",
    )


def record_gold_result(
    transport,
    *,
    mission_id: str,
    wave: int,
    request_id: str,
    result: Mapping[str, Any],
) -> dict[str, Any]:
    current = load_mission(transport, mission_id=mission_id)
    prior = _json_object(current.get("result"))
    found_agency = int(result.get("total_agency_count") if result.get("total_agency_count") is not None else prior.get("found_agency") or 0)
    found_direct = int(result.get("total_direct_count") if result.get("total_direct_count") is not None else prior.get("found_direct") or 0)
    target = int(_json_object(current.get("payload")).get("requested_new_gold") or 0)
    found_total = found_agency + found_direct
    if str(result.get("status") or "") == "TARGET_MET" or found_total >= target:
        return _update(
            transport,
            mission_id=mission_id,
            state="TARGET_MET",
            status="SUCCEEDED",
            next_action="NONE",
            wave=wave,
            found_agency=found_agency,
            found_direct=found_direct,
            last_request_id=request_id,
            last_checkpoint=f"PASS_{wave}_GOLD_DONE_TARGET_MET",
        )
    if stop_is_requested(current):
        return _update(
            transport,
            mission_id=mission_id,
            state="STOPPED_BY_USER",
            status="SUCCEEDED",
            next_action="NONE",
            wave=wave,
            found_agency=found_agency,
            found_direct=found_direct,
            last_request_id=request_id,
            last_checkpoint=f"PASS_{wave}_GOLD_DONE_STOPPED",
            stop_requested=True,
        )
    return _update(
        transport,
        mission_id=mission_id,
        state="GOLD_PASS_DONE",
        status="RUNNING",
        next_action="RUN_MOTOR",
        wave=wave,
        found_agency=found_agency,
        found_direct=found_direct,
        last_request_id=request_id,
        last_checkpoint=f"PASS_{wave}_GOLD_DONE",
    )


def stop_is_requested(mission: Mapping[str, Any]) -> bool:
    result = _json_object(mission.get("result"))
    return result.get("stop_requested") is True or result.get("state") in {"STOP_REQUESTED", "STOPPED_BY_USER"}


def request_stop(transport, *, mission_id: str) -> dict[str, Any]:
    mission = load_mission(transport, mission_id=mission_id)
    result = _json_object(mission.get("result"))
    if result.get("state") in TERMINAL_MISSION_STATES:
        return result
    return _update(
        transport,
        mission_id=mission_id,
        state="STOP_REQUESTED",
        status="RUNNING",
        next_action="STOP_AFTER_CURRENT_SAFE_STAGE",
        last_checkpoint=str(result.get("last_checkpoint") or "MISSION_STARTED"),
        stop_requested=True,
    )
