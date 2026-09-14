from __future__ import annotations

import json
import uuid
from typing import Any, Mapping

from sqlalchemy import text

QUEUE = "gfo_render_operator_queue"
MISSION_OPERATION = "FIND_GOLD_MISSION"
MISSION_SCHEMA = "gfo.r0007.find-gold-mission.v1"
MISSION_CONTRACT = "REQUEST_SCOPED_NEW_GOLD_60_40_V1"


def balanced_targets(total: int) -> tuple[int, int]:
    if total < 2:
        raise RuntimeError("mission FIND target must be at least 2")
    agency = (total * 6 + 9) // 10
    agency = min(agency, total - 1)
    direct = total - agency
    return agency, direct


def mission_request_id(mission_id: str) -> str:
    return f"mission-find_gold-{mission_id}"


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
            ORDER BY created_at DESC
            LIMIT 1
            FOR UPDATE
            """
        ),
        {
            "request_id": mission_request_id(mission_id),
            "operation": MISSION_OPERATION,
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
            payload = {
                "schema": MISSION_SCHEMA,
                "contract": MISSION_CONTRACT,
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
                "found_agency": 0,
                "found_direct": 0,
                "last_request_id": source_request_id,
                "next_action": "RUN_MOTOR",
                "stop_requested": False,
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
                        'RUNNING', CAST(:result AS jsonb), NULL,
                        1, now(), now(), NULL, now(), 'github-public-runner-mission'
                    )
                    """
                ),
                {
                    "job_id": str(uuid.uuid4()),
                    "request_id": mission_request_id(mission_id),
                    "operation": MISSION_OPERATION,
                    "payload": json.dumps(payload),
                    "result": json.dumps(result),
                },
            )
            return {"payload": payload, "result": result, "status": "RUNNING"}
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
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {
                    "request_id": mission_request_id(mission_id),
                    "operation": MISSION_OPERATION,
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
    error: str | None = None,
    stop_requested: bool | None = None,
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
            if stop_requested is not None:
                result["stop_requested"] = bool(stop_requested)
            selected_status = status or str(row.get("status") or "RUNNING")
            finished = selected_status in {"SUCCEEDED", "STOPPED", "FAILED"}
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
        error=f"Motor stage ended with {state or 'UNKNOWN'}",
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
        )
    if stop_is_requested(current):
        return _update(
            transport,
            mission_id=mission_id,
            state="STOPPED_BY_USER",
            status="STOPPED",
            next_action="NONE",
            wave=wave,
            found_agency=found_agency,
            found_direct=found_direct,
            last_request_id=request_id,
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
    )


def stop_is_requested(mission: Mapping[str, Any]) -> bool:
    status = str(mission.get("status") or "")
    result = _json_object(mission.get("result"))
    return status in {"STOP_REQUESTED", "STOPPED"} or result.get("stop_requested") is True


def request_stop(transport, *, mission_id: str) -> dict[str, Any]:
    mission = load_mission(transport, mission_id=mission_id)
    if str(mission.get("status")) in {"SUCCEEDED", "STOPPED", "FAILED"}:
        return _json_object(mission.get("result"))
    return _update(
        transport,
        mission_id=mission_id,
        state="STOP_REQUESTED",
        status="STOP_REQUESTED",
        next_action="STOP_AFTER_CURRENT_SAFE_STAGE",
        stop_requested=True,
    )
