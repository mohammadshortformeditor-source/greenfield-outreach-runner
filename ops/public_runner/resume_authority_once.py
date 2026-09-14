from __future__ import annotations

import json
import os
import re
import uuid

from sqlalchemy import text

import mission_control
import runner_once as base

QUEUE = "gfo_render_operator_queue"


def _source_request_id() -> str:
    title = os.environ.get("GFO_PUBLIC_TRIGGER_TITLE", "").strip()
    numeric = re.match(r"^\[GFO-RESUME\]\s+([0-9]+)(?:\s|$)", title, flags=re.IGNORECASE)
    if numeric:
        return f"publicrunner-find_gold_batch-{numeric.group(1)}"
    exact = re.match(r"^\[GFO-RESUME\]\s+REQUEST\s+([A-Za-z0-9_.:-]+)(?:\s|$)", title, flags=re.IGNORECASE)
    if exact:
        return exact.group(1)
    raise RuntimeError("trigger must be [GFO-RESUME] <source github run id> or [GFO-RESUME] REQUEST <request_id>")


def _json_object(value, label: str) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise RuntimeError(f"{label} must be a JSON object")


def _load_stage1(transport, source_request_id: str) -> tuple[dict, dict]:
    db = transport.engine()
    try:
        with db.begin() as connection:
            row = connection.execute(
                text(
                    f"""
                    SELECT payload, result
                    FROM {QUEUE}
                    WHERE request_id = :request_id
                      AND status = 'SUCCEEDED'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"request_id": source_request_id},
            ).mappings().first()
    finally:
        db.dispose()
    if row is None:
        raise RuntimeError("source stage-1 result not found")
    payload = _json_object(row["payload"], "stage1 payload")
    result = _json_object(row["result"], "stage1 result")
    if result.get("status") != "EXTERNAL_AUTHORITY_BATCH_REQUIRED":
        raise RuntimeError("source result is not awaiting external authority")
    return result, payload


def _verified_authority_receipt(stage1: dict, routes: list, primary: dict, snapshot: dict) -> dict:
    authority_receipt = stage1.get("verified_external_authority_batch_receipt")
    if not isinstance(authority_receipt, dict):
        raise RuntimeError("verified external authority receipt missing; refusing synthetic authority")
    if authority_receipt.get("schema") != "gfo.r0007.external-authority-batch-receipt.v1":
        raise RuntimeError("verified external authority receipt schema mismatch")
    if authority_receipt.get("queried_routes") != routes:
        raise RuntimeError("verified external authority queried_routes mismatch")

    gmail = authority_receipt.get("gmail")
    legacy_primary = authority_receipt.get("legacy_primary")
    legacy_snapshot = authority_receipt.get("legacy_snapshot")
    verification = authority_receipt.get("verification")
    if not all(isinstance(item, dict) for item in (gmail, legacy_primary, legacy_snapshot, verification)):
        raise RuntimeError("verified external authority receipt incomplete")
    if gmail.get("provider") != "CHATGPT_WORK_GMAIL_CONNECTOR" or gmail.get("mode") != "BATCH_EXACT_RECIPIENT_OR_QUERY":
        raise RuntimeError("verified Gmail authority binding mismatch")
    if not str(gmail.get("receipt_id") or "").strip():
        raise RuntimeError("verified Gmail receipt id missing")
    if legacy_primary.get("spreadsheet_id") != primary.get("spreadsheet_id") or legacy_primary.get("sheet") != primary.get("sheet"):
        raise RuntimeError("verified legacy primary binding mismatch")
    if legacy_snapshot.get("spreadsheet_id") != snapshot.get("spreadsheet_id") or legacy_snapshot.get("sheet") != snapshot.get("sheet"):
        raise RuntimeError("verified legacy snapshot binding mismatch")
    if int(legacy_primary.get("contacted_rows_indexed") or 0) < 480:
        raise RuntimeError("verified legacy primary scan too small")
    if int(legacy_snapshot.get("contacted_rows_indexed") or 0) < 480:
        raise RuntimeError("verified legacy snapshot scan too small")
    for surface in (gmail, legacy_primary, legacy_snapshot):
        if not isinstance(surface.get("matched_routes"), list):
            raise RuntimeError("verified authority matched_routes missing")
        if any(route not in routes for route in surface["matched_routes"]):
            raise RuntimeError("verified authority contains route outside candidate batch")
    if verification.get("gmail_real_connector_query") is not True:
        raise RuntimeError("real Gmail authority verification missing")
    if verification.get("legacy_primary_real_sheet_scan") is not True or verification.get("legacy_snapshot_real_sheet_scan") is not True:
        raise RuntimeError("real legacy authority verification missing")
    if verification.get("send_authority") != "NOT_GRANTED" or verification.get("outbound_side_effects") is not False:
        raise RuntimeError("verified authority safety binding mismatch")
    return authority_receipt


def _resume_payload(stage1: dict, stage1_payload: dict) -> dict:
    leads = stage1.get("engine_candidates")
    receipt = stage1.get("engine_execution_receipt")
    authority = stage1.get("external_authority_batch_request")
    if not isinstance(leads, list) or not leads:
        raise RuntimeError("stage1 engine candidates missing")
    if not isinstance(receipt, dict) or not isinstance(authority, dict):
        raise RuntimeError("stage1 receipts missing")

    routes = authority.get("routes")
    primary = authority.get("legacy_primary")
    snapshot = authority.get("legacy_snapshot")
    if not isinstance(routes, list) or not routes:
        raise RuntimeError("stage1 authority routes missing")
    if not isinstance(primary, dict) or not isinstance(snapshot, dict):
        raise RuntimeError("stage1 legacy authority bindings missing")

    authority_receipt = _verified_authority_receipt(stage1, routes, primary, snapshot)
    target_count = int(stage1_payload.get("target_count") or receipt.get("search_run_requested_count") or stage1.get("target_count") or 10)
    mission_id = str(stage1_payload.get("mission_id") or "").strip()
    mission_wave = int(stage1_payload.get("mission_wave") or 1)
    run_id = os.environ.get("GITHUB_RUN_ID", uuid.uuid4().hex)
    request_id = (
        f"publicrunner-mission-{mission_id}-wave-{mission_wave}-resume-{run_id}"
        if mission_id
        else f"publicrunner-resume-find_gold_batch-{run_id}"
    )
    payload = {
        "schema": "gfo.operator-request.v1",
        "operation": "FIND_GOLD_BATCH",
        "release_id": "R0007",
        "request_id": request_id,
        "target_count": target_count,
        "goal_profile_id": base.GOAL_PROFILE_ID,
        "target_constraints_hash": base.TARGET_CONSTRAINTS_HASH,
        "leads": leads,
        "engine_execution_receipt": receipt,
        "external_authority_batch_receipt": authority_receipt,
        "send_authority": "NOT_GRANTED",
        "outbound_side_effects": False,
    }
    for key in (
        "mission_id",
        "mission_contract",
        "mission_requested_new_gold",
        "mission_fast_cash_baseline",
        "mission_target_mix",
        "mission_wave",
    ):
        if key in stage1_payload:
            payload[key] = stage1_payload[key]
    return payload


def _next_motor_payload(mission: dict, *, mission_id: str, wave: int) -> dict:
    fields = mission_control.mission_fields(mission, wave=wave)
    target_count = int(fields["mission_requested_new_gold"])
    run_id = os.environ.get("GITHUB_RUN_ID", uuid.uuid4().hex)
    return {
        "schema": "gfo.operator-request.v1",
        "operation": "FIND_GOLD_BATCH",
        "release_id": "R0007",
        "request_id": f"publicrunner-mission-{mission_id}-wave-{wave}-find_gold_batch-{run_id}",
        "target_count": target_count,
        "goal_profile_id": base.GOAL_PROFILE_ID,
        "target_constraints_hash": base.TARGET_CONSTRAINTS_HASH,
        **fields,
        "send_authority": "NOT_GRANTED",
        "outbound_side_effects": False,
    }


def _authority_survivor_count(payload: dict) -> int:
    leads = payload.get("leads")
    receipt = payload.get("external_authority_batch_receipt")
    if not isinstance(leads, list) or not isinstance(receipt, dict):
        return 0
    blocked: set[str] = set()
    for key in ("gmail", "legacy_primary", "legacy_snapshot"):
        section = receipt.get(key)
        if isinstance(section, dict):
            blocked.update(str(item).strip().lower() for item in section.get("matched_routes", []) if item)
    survivors = 0
    for lead in leads:
        route = lead.get("contact_route") if isinstance(lead, dict) else None
        value = str(route.get("route_value") or "").strip().lower() if isinstance(route, dict) else ""
        if value and value not in blocked:
            survivors += 1
    return survivors


def main() -> int:
    transport = base._load_transport()
    source_request_id = _source_request_id()
    stage1, stage1_payload = _load_stage1(transport, source_request_id)
    payload = _resume_payload(stage1, stage1_payload)
    request_id = str(payload["request_id"])
    mission_id = str(payload.get("mission_id") or "").strip() or None
    mission_wave = int(payload.get("mission_wave") or 1)

    if mission_id is not None:
        mission_control.record_authority_done(
            transport,
            mission_id=mission_id,
            wave=mission_wave,
            request_id=request_id,
            survivor_count=_authority_survivor_count(payload),
        )

    execution_error: Exception | None = None
    try:
        result, exit_code = base._execute_payload(transport, payload)
    except Exception as exc:
        execution_error = exc
        result = {
            "status": "FAILED_WITH_PROGRESS",
            "error_type": type(exc).__name__,
            "error": str(exc)[:4000],
            "send_authority": "NOT_GRANTED",
            "outbound_side_effects": False,
        }
        exit_code = 1
    base._persist_private_result(transport, payload, result, exit_code)

    mission_state = None
    next_stage1_request_id = None
    next_stage1_status = None
    if mission_id is not None:
        if execution_error is not None or exit_code != 0 or result.get("status") in {"FAILED_FAIL_CLOSED", "FAILED_WITH_PROGRESS"}:
            mission_state = mission_control._update(
                transport,
                mission_id=mission_id,
                state="FAILED_WITH_PROGRESS",
                status="FAILED",
                next_action="REPORT_PROGRESS",
                wave=mission_wave,
                last_request_id=request_id,
                last_checkpoint=f"PASS_{mission_wave}_FOUR_SOURCE_DONE",
                failure_at=f"PASS_{mission_wave}_GOLD",
                error=str(result.get("error") or "Gold resume failed")[:4000],
            )
        else:
            mission_state = mission_control.record_gold_result(
                transport,
                mission_id=mission_id,
                wave=mission_wave,
                request_id=request_id,
                result=result,
            )
            if mission_state.get("next_action") == "RUN_MOTOR":
                latest = mission_control.load_mission(transport, mission_id=mission_id)
                if mission_control.stop_is_requested(latest):
                    mission_state = mission_control.request_stop(transport, mission_id=mission_id)
                    mission_state = mission_control._update(
                        transport,
                        mission_id=mission_id,
                        state="STOPPED_BY_USER",
                        status="SUCCEEDED",
                        next_action="NONE",
                        wave=mission_wave,
                        found_agency=int(mission_state.get("found_agency") or 0),
                        found_direct=int(mission_state.get("found_direct") or 0),
                        last_checkpoint=f"PASS_{mission_wave}_GOLD_DONE_STOPPED",
                        stop_requested=True,
                    )
                else:
                    next_wave = mission_wave + 1
                    mission = mission_control.load_mission(transport, mission_id=mission_id)
                    next_payload = _next_motor_payload(mission, mission_id=mission_id, wave=next_wave)
                    next_stage1_request_id = str(next_payload["request_id"])
                    next_result, next_exit, mission_state = base._run_motor_until_boundary(
                        transport,
                        mission_id=mission_id,
                        first_payload=next_payload,
                        first_wave=next_wave,
                    )
                    next_stage1_status = next_result.get("status")
                    if next_exit != 0:
                        exit_code = next_exit

    safe = {
        "public_runner": "AUTHORITY_RESUME_FINISHED",
        "private_result_persisted": True,
        "source_request_id": source_request_id,
        "request_id": request_id,
        "operation": "FIND_GOLD_BATCH",
        "status": result.get("status"),
        "gold_count": int(result.get("gold_count") or len(result.get("gold_results") or [])),
        "gold_gates_unchanged": result.get("gold_gates_unchanged"),
        "quality_relaxation": result.get("quality_relaxation"),
        "send_authority": result.get("send_authority", "NOT_GRANTED"),
        "outbound_side_effects": result.get("outbound_side_effects", False),
        "mission_id": mission_id,
        "mission_state": mission_state.get("state") if mission_state else None,
        "mission_found_new_gold": mission_state.get("found_new_gold") if mission_state else None,
        "mission_target": mission_state.get("mission_target") if mission_state else None,
        "mission_deficit": mission_state.get("deficit") if mission_state else None,
        "mission_wave": mission_state.get("wave") if mission_state else None,
        "last_checkpoint": mission_state.get("last_checkpoint") if mission_state else None,
        "failure_at": mission_state.get("failure_at") if mission_state else None,
        "mission_next_action": mission_state.get("next_action") if mission_state else None,
        "next_stage1_request_id": next_stage1_request_id,
        "next_stage1_status": next_stage1_status,
        "exit_code": exit_code,
    }
    print(json.dumps(safe, sort_keys=True), flush=True)
    return 0 if exit_code == 0 and result.get("status") != "FAILED_FAIL_CLOSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
