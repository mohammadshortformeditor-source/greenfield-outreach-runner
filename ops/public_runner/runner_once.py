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


def main() -> int:
    transport = _load_transport()
    payload = _request_from_trigger()
    operation = str(payload["operation"])
    request_id = str(payload["request_id"])
    mission_id: str | None = None
    mission_state: dict | None = None

    if operation == "FIND_GOLD_BATCH":
        mission_id = str(os.environ.get("GITHUB_RUN_ID") or uuid.uuid4().hex)
        mission = mission_control.ensure_mission(
            transport,
            mission_id=mission_id,
            requested_new_gold=int(payload["target_count"]),
            source_request_id=request_id,
        )
        if mission_control.stop_is_requested(mission):
            mission_state = mission_control.request_stop(transport, mission_id=mission_id)
            safe = {
                "public_runner": "FINISHED",
                "private_result_persisted": False,
                "request_id": request_id,
                "operation": operation,
                "status": "STOPPED_BY_USER",
                "mission_id": mission_id,
                "mission_state": mission_state.get("state"),
                "mission_found_new_gold": mission_state.get("found_new_gold", 0),
                "send_authority": "NOT_GRANTED",
                "outbound_side_effects": False,
                "exit_code": 0,
            }
            print(json.dumps(safe, sort_keys=True), flush=True)
            return 0
        payload.update(mission_control.mission_fields(mission, wave=1))

    result, exit_code = _execute_payload(transport, payload)
    _persist_private_result(transport, payload, result, exit_code)

    if mission_id is not None:
        if exit_code == 0 and result.get("status") != "FAILED_FAIL_CLOSED":
            mission_state = mission_control.record_stage1(
                transport,
                mission_id=mission_id,
                wave=1,
                request_id=request_id,
                stage1_result=result,
            )
        else:
            mission_state = mission_control._update(
                transport,
                mission_id=mission_id,
                state="FAILED",
                status="FAILED",
                next_action="REPORT_PROGRESS",
                wave=1,
                last_request_id=request_id,
                error=str(result.get("error") or "initial Motor execution failed")[:4000],
            )

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
        "mission_next_action": mission_state.get("next_action") if mission_state else None,
        "exit_code": exit_code,
    }
    print(json.dumps(safe, sort_keys=True), flush=True)
    return 0 if exit_code == 0 and result.get("status") != "FAILED_FAIL_CLOSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
