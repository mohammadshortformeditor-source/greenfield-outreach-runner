from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path

import runner_once as base


def _normalize_trigger_for_base() -> None:
    title = os.environ.get("GFO_PUBLIC_TRIGGER_TITLE", "").strip()
    if title.upper().startswith("[GFO-GUARD]"):
        os.environ["GFO_PUBLIC_TRIGGER_TITLE"] = "[GFO-RUN]" + title[len("[GFO-GUARD]"):]


def main() -> int:
    _normalize_trigger_for_base()
    transport = base._load_transport()
    payload = base._request_from_trigger()
    operation = str(payload["operation"])
    request_id = str(payload["request_id"])

    env = os.environ.copy()
    env["GFO_PRODUCTION_DSN"] = transport.lease(operation, request_id)
    env["PYTHONPATH"] = str(transport.ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        request_path = temp_root / "request.json"
        output_path = temp_root / "output.json"
        shim_path = temp_root / "existing_guard_launcher.py"
        request_path.write_text(json.dumps(payload), encoding="utf-8")
        shim_path.write_text(
            "import runpy, sys\n"
            "import outreach.runtime.r0007_route_identity_binding  # existing GREENFIELD guard\n"
            "launcher = sys.argv[1]\n"
            "sys.argv = [launcher] + sys.argv[2:]\n"
            "runpy.run_path(launcher, run_name='__main__')\n",
            encoding="utf-8",
        )

        completed = subprocess.run(
            [
                "python",
                str(shim_path),
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
            raise RuntimeError("existing-guard canary launcher exited without result")
        result = json.loads(output_path.read_text(encoding="utf-8"))

    base._persist_private_result(transport, payload, result, completed.returncode)

    safe = {
        "public_runner": "EXISTING_GUARD_CANARY_FINISHED",
        "private_result_persisted": True,
        "request_id": request_id,
        "operation": operation,
        "status": result.get("status"),
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
