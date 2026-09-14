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


def main() -> int:
    transport = _load_transport()
    payload = _request_from_trigger()
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
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=int(os.environ.get("GFO_RENDER_RUN_TIMEOUT_SECONDS", "1800")),
            check=False,
        )
        if not output_path.exists():
            raise RuntimeError("canonical R0007 launcher exited without result")
        result = json.loads(output_path.read_text(encoding="utf-8"))

    _persist_private_result(transport, payload, result, completed.returncode)

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
        "exit_code": completed.returncode,
    }
    print(json.dumps(safe, sort_keys=True), flush=True)
    return 0 if completed.returncode == 0 and result.get("status") != "FAILED_FAIL_CLOSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
