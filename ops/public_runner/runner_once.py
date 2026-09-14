from __future__ import annotations

import importlib.util
import json
import os
import re
import subprocess
import tempfile
import uuid
from pathlib import Path

from sqlalchemy import text

import mission_control

GOAL_PROFILE_ID = "GFO_SHORT_FORM_EDITING_REVENUE_2026Q3"
TARGET_CONSTRAINTS_HASH = "sha256:00449ca149a50b7dd2a352e9180eb81b199158442a7b58338d677e150db7c570"
QUEUE = "gfo_render_operator_queue"
DEFAULT_MOTOR_ONLY_WAVE_CAP = 5


def _load_transport():
    engine_root = Path(os.environ.get("GFO_ENGINE_ROOT", "engine")).resolve()
    module_path = engine_root / "tools" / "render_operator_server_r0007.py"
    if not module_path.exists():
        raise RuntimeError(f"canonical transport module not found: {module_path}")
    spec = importlib.util.spec_from_file_location("gfo_r0007_transport", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load canonical R0007 transport module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _request_from_trigger() -> dict:
    title = os.environ.get("GFO_PUBLIC_TRIGGER_TITLE", "").strip()
    if re.match(r"^\[GFO-RUN\]\s+BOOT(?:\s|$)", title, flags=re.IGNORECASE):
        operation = "BOOT"
        target_count = None
    else:
        match = re.match(r"^\[GFO-RUN\]\s+FIND\s+([1-9][0-9]{0,2})(?:\s|$)", title, flags=re.IGNORECASE)
        if not match:
            raise RuntimeError("trigger must be [GFO-RUN] BOOT or [GFO-RUN] FIND N")
        operation = "FIND_GOLD_BATCH"
        target_count = int(match.group(1))
        if target_count > 100:
            raise RuntimeError("FIND target must be <= 100")
        if target_count < 2:
            raise RuntimeError("FIND target must be >= 2 while 60/40 dispatch is active")

    run_id = os.environ.get("GITHUB_RUN_ID", uuid.uuid4().hex)
    request_id = f"publicrunner-{operation.lower()}-{run_id}"
    payload: dict[str, object] = {
        "schema": "gfo.operator-request.v1",
        "operation": operation,
        "release_id": "R0007",
        "request_id": request_id,
        "send_authority": "NOT_GRANTED",
        "outbound_side_effects": False,
    }
    if target_count is not None:
        payload.update(
            {
                "target_count": target_count,
                "goal_profile_id": GOAL_PROFILE_ID,
                "target_constraints_hash": TARGET_CONSTRAINTS_HASH,
            }
        )
    return payload


def _persist_private_result(transport, payload: dict, result: dict, exit_code: int) -> None:
    db = transport.engine()
    try:
        with db.begin() as connection:
            connection.execute(
                text(
                    f"""
                    INSERT INTO {QUEUE} (
                        job_id, request_id, operation, payload, status, result, error,
                        attempts, created_at, started_at, finished_at, heartbeat_at, lease_owner
                    )
                    VALUES (
                        CAST(:job_id AS uuid), :request_id, :operation, CAST(:payload AS jsonb),
                        :status, CAST(:result AS jsonb), :error,
                        1, now(), now(), now(), now(), 'github-public-runner-audit'
                    )
                    """
                ),
                {
                    "job_id": str(uuid.uuid4()),
                    "request_id": str(payload["request_id"]),
                    "operation": str(payload["operation"]),
                    "payload": json.dumps(payload),
                    "status": "SUCCEEDED" if exit_code == 0 and result.get("status") != "FAILED_FAIL_CLOSED" else "FAILED",
                    "result": json.dumps(result),
                    "error": None if exit_code == 0 and result.get("status") != "FAILED_FAIL_CLOSED" else str(result.get("error") or "public runner canonical launcher failed")[:4000],
                },
            )
    finally:
        db.dispose()


def _safe_last_error(stderr: str, env: dict[str, str]) -> str:
    lines = [line.strip() for line in (stderr or "").splitlines() if line.strip()]
    detail = lines[-1] if lines else "unavailable"
    for key in ("GFO_RENDER_DB_BROKER_TOKEN", "GFO_PRODUCTION_DSN"):
        secret = env.get(key)
        if secret:
            detail = detail.replace(secret, "***")
    return detail[:500]


def _execute_payload(transport, payload: dict) -> tuple[dict, int]:
    operation = str(payload["operation"])
    request_id = str(payload["request_id"])
    env = os.environ.copy()
    env["GFO_PRODUCTION_DSN"] = transport.lease(operation, request_id)
    env["PYTHONPATH"] = str(transport.ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    with tempfile.TemporaryDirectory() as temp_dir:
        request_path = Path(temp_dir) / "request.json"
        output_path = Path(temp_dir) / "output.json"
        request_path.write_text(json.dumps(payload), encoding="utf-8")
        completed = subprocess.run(
            [
                "python",
                str(transport.LAUNCHER),
                "--request",
                str(request_path),
                "--output",
                str(output_path),
            ],
            cwd=str(transport.ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=int(os.environ.get("GFO_RENDER_RUN_TIMEOUT_SECONDS", "1800")),
            check=False,
        )
        if not output_path.exists():
            raise RuntimeError(
                "canonical R0007 launcher exited without result; "
                f"exit_code={completed.returncode}; last_error={_safe_last_error(completed.stderr, env)}"
            )
        result = json.loads(output_path.read_text(encoding="utf-8"))
    return result, completed.returncode


def _assert_no_other_active_mission(transport, *, mission_id: str) -> None:
    db = transport.engine()
    try:
        with db.begin() as connection:
            other = connection.execute(
                text(
                    f"""
                    SELECT request_id
                    FROM {QUEUE}
                    WHERE operation = :operation
                      AND status = 'RUNNING'
                      AND payload->>'mission_contract' = :contract
                      AND request_id <> :request_id
                    ORDER BY created_at ASC
                    LIMIT 1
                    """
                ),
                {
                    "operation": mission_control.MISSION_OPERATION,
                    "contract": mission_control.MISSION_CONTRACT,
                    "request_id": mission_control.mission_request_id(mission_id),
                },
            ).scalar_one_or_none()
    finally:
        db.dispose()
    if other is not None:
        raise RuntimeError("another FIND_GOLD mission is still active; finish or stop it before starting a new mission")


def _motor_payload_for_wave(mission: dict, *, mission_id: str, wave: int, request_id: str) -> dict:
    fields = mission_control.mission_fields(mission, wave=wave)
    return {
        "schema": "gfo.operator-request.v1",
        "operation": "FIND_GOLD_BATCH",
        "release_id": "R0007",
        "request_id": request_id,
        "target_count": int(fields["mission_requested_new_gold"]),
        "goal_profile_id": GOAL_PROFILE_ID,
        "target_constraints_hash": TARGET_CONSTRAINTS_HASH,
        **fields,
        "send_authority": "NOT_GRANTED",
        "outbound_side_effects": False,
    }


def _stop_mission_now(transport, *, mission_id: str, wave: int) -> dict:
    current = mission_control.request_stop(transport, mission_id=mission_id)
    return mission_control._update(
        transport,
        mission_id=mission_id,
        state="STOPPED_BY_USER",
        status="SUCCEEDED",
        next_action="NONE",
        wave=wave,
        found_agency=int(current.get("found_agency") or 0),
        found_direct=int(current.get("found_direct") or 0),
        stop_requested=True,
    )


def _run_motor_until_boundary(
    transport,
    *,
    mission_id: str,
    first_payload: dict,
    first_wave: int,
) -> tuple[dict, int, dict]:
    wave = int(first_wave)
    payload = dict(first_payload)
    cap = max(1, int(os.environ.get("GFO_MISSION_MOTOR_ONLY_WAVE_CAP", str(DEFAULT_MOTOR_ONLY_WAVE_CAP))))
    waves_run = 0

    while True:
        mission = mission_control.load_mission(transport, mission_id=mission_id)
        if mission_control.stop_is_requested(mission):
            stopped = _stop_mission_now(transport, mission_id=mission_id, wave=wave)
            return {
                "status": "STOPPED_BY_USER",
                "send_authority": "NOT_GRANTED",
                "outbound_side_effects": False,
            }, 0, stopped

        mission_control._update(
            transport,
            mission_id=mission_id,
            state="MOTOR_RUNNING",
            status="RUNNING",
            next_action="RUN_MOTOR",
            wave=wave,
            last_request_id=str(payload["request_id"]),
            last_checkpoint=f"PASS_{wave}_STARTED",
        )
        try:
            result, exit_code = _execute_payload(transport, payload)
        except Exception as exc:
            result = {
                "status": "FAILED_WITH_PROGRESS",
                "error_type": type(exc).__name__,
                "error": str(exc)[:4000],
                "send_authority": "NOT_GRANTED",
                "outbound_side_effects": False,
            }
            failed = mission_control._update(
                transport,
                mission_id=mission_id,
                state="FAILED_WITH_PROGRESS",
                status="FAILED",
                next_action="REPORT_PROGRESS",
                wave=wave,
                last_request_id=str(payload["request_id"]),
                last_checkpoint=f"PASS_{wave}_STARTED",
                failure_at=f"PASS_{wave}_MOTOR",
                error=str(exc)[:4000],
            )
            return result, 1, failed
        _persist_private_result(transport, payload, result, exit_code)
        waves_run += 1
        request_id = str(payload["request_id"])

        latest_after_motor = mission_control.load_mission(transport, mission_id=mission_id)
        if mission_control.stop_is_requested(latest_after_motor):
            stopped = _stop_mission_now(transport, mission_id=mission_id, wave=wave)
            return {
                "status": "STOPPED_BY_USER",
                "send_authority": "NOT_GRANTED",
                "outbound_side_effects": False,
            }, 0, stopped

        if exit_code != 0 or result.get("status") == "FAILED_FAIL_CLOSED":
            failed = mission_control._update(
                transport,
                mission_id=mission_id,
                state="FAILED",
                status="FAILED",
                next_action="REPORT_PROGRESS",
                wave=wave,
                last_request_id=request_id,
                last_checkpoint=f"PASS_{wave}_MOTOR_FAILED",
                failure_at=f"PASS_{wave}_MOTOR",
                error=str(result.get("error") or "Motor execution failed")[:4000],
            )
            return result, exit_code or 1, failed

        if result.get("status") in {"EXTERNAL_AUTHORITY_BATCH_REQUIRED", "TARGET_MET"}:
            boundary = mission_control.record_stage1(
                transport,
                mission_id=mission_id,
                wave=wave,
                request_id=request_id,
                stage1_result=result,
            )
            return result, exit_code, boundary

        if result.get("continue_discovery") is not True:
            terminal = mission_control.record_stage1(
                transport,
                mission_id=mission_id,
                wave=wave,
                request_id=request_id,
                stage1_result=result,
            )
            return result, exit_code, terminal

        checkpoint = mission_control._update(
            transport,
            mission_id=mission_id,
            state="MOTOR_PASS_DONE_NO_AUTHORITY",
            status="RUNNING",
            next_action="RUN_MOTOR",
            wave=wave,
            last_request_id=request_id,
            candidate_count=len(result.get("engine_candidates") or []) if isinstance(result.get("engine_candidates"), list) else 0,
            last_checkpoint=f"PASS_{wave}_MOTOR_DONE_NO_AUTHORITY",
        )
        if waves_run >= cap:
            checkpoint = mission_control._update(
                transport,
                mission_id=mission_id,
                state="CHECKPOINTED_MOTOR_WAVE_CAP",
                status="RUNNING",
                next_action="RUN_MOTOR",
                wave=wave,
                last_request_id=request_id,
                last_checkpoint=f"PASS_{wave}_MOTOR_DONE_NO_AUTHORITY",
            )
            return result, 0, checkpoint

        latest = mission_control.load_mission(transport, mission_id=mission_id)
        if mission_control.stop_is_requested(latest):
            stopped = _stop_mission_now(transport, mission_id=mission_id, wave=wave)
            return {
                "status": "STOPPED_BY_USER",
                "send_authority": "NOT_GRANTED",
                "outbound_side_effects": False,
            }, 0, stopped

        wave += 1
        mission_control._update(
            transport,
            mission_id=mission_id,
            state="MOTOR_RUNNING",
            status="RUNNING",
            next_action="RUN_MOTOR",
            wave=wave,
            last_checkpoint=f"PASS_{wave}_STARTED",
        )
        latest = mission_control.load_mission(transport, mission_id=mission_id)
        payload = _motor_payload_for_wave(
            latest,
            mission_id=mission_id,
            wave=wave,
            request_id=f"publicrunner-mission-{mission_id}-wave-{wave}-find_gold_batch-{os.environ.get('GITHUB_RUN_ID', uuid.uuid4().hex)}",
        )


def main() -> int:
    transport = _load_transport()
    payload = _request_from_trigger()
    operation = str(payload["operation"])
    request_id = str(payload["request_id"])
    mission_id: str | None = None
    mission_state: dict | None = None

    if operation == "FIND_GOLD_BATCH":
        mission_id = str(os.environ.get("GITHUB_RUN_ID") or uuid.uuid4().hex)
        _assert_no_other_active_mission(transport, mission_id=mission_id)
        mission = mission_control.ensure_mission(
            transport,
            mission_id=mission_id,
            requested_new_gold=int(payload["target_count"]),
            source_request_id=request_id,
        )
        payload.update(mission_control.mission_fields(mission, wave=1))
        result, exit_code, mission_state = _run_motor_until_boundary(
            transport,
            mission_id=mission_id,
            first_payload=payload,
            first_wave=1,
        )
    else:
        result, exit_code = _execute_payload(transport, payload)
        _persist_private_result(transport, payload, result, exit_code)

    safe = {
        "public_runner": "FINISHED",
        "private_result_persisted": True,
        "request_id": request_id,
        "operation": operation,
        "status": result.get("status"),
        "transport_status": result.get("transport_status"),
        "operator_transport_state": result.get("operator_transport_state"),
        "booted_full": result.get("booted_full"),
        "operator_transport_ready": result.get("operator_transport_ready"),
        "motor_role": result.get("motor_role"),
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
        "exit_code": exit_code,
    }
    print(json.dumps(safe, sort_keys=True), flush=True)
    return 0 if exit_code == 0 and result.get("status") != "FAILED_FAIL_CLOSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
