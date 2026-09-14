from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path


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


def main() -> int:
    transport = _load_transport()
    job = None
    try:
        job = transport.claim()
        if not job:
            print(json.dumps({"public_runner": "NO_PENDING_JOB"}), flush=True)
            return 0

        print(
            json.dumps(
                {
                    "public_runner": "CLAIMED_ONCE",
                    "request_id": job.get("request_id"),
                    "operation": job.get("operation"),
                    "attempts": job.get("attempts"),
                    "canonical_motor": "R0007",
                    "send_authority": "NOT_GRANTED",
                }
            ),
            flush=True,
        )

        payload = job.get("payload") if isinstance(job.get("payload"), dict) else dict(job.get("payload") or {})
        result = transport.run(
            payload,
            job_id=str(job["job_id"]),
            owner=transport.WORKER_ID,
        )
        if not transport.finish(str(job["job_id"]), transport.WORKER_ID, result):
            raise RuntimeError("queue lease lost before result persistence")

        status = str(result.get("status") or "")
        exit_code = int(result.get("_render_exit_code") or 0)
        print(
            json.dumps(
                {
                    "public_runner": "FINISHED",
                    "request_id": job.get("request_id"),
                    "status": status,
                    "operator_exit_code": exit_code,
                    "send_authority": result.get("send_authority", "NOT_GRANTED"),
                    "outbound_side_effects": result.get("outbound_side_effects", False),
                }
            ),
            flush=True,
        )
        return 0 if exit_code == 0 and status != "FAILED_FAIL_CLOSED" else 1

    except Exception as exc:
        if job:
            try:
                transport.finish(
                    str(job["job_id"]),
                    transport.WORKER_ID,
                    {
                        "schema": "gfo.operator-result.v1",
                        "release_id": "R0007",
                        "status": "FAILED_FAIL_CLOSED",
                        "error_type": type(exc).__name__,
                        "error": str(exc)[:4000],
                        "send_authority": "NOT_GRANTED",
                        "outbound_side_effects": False,
                        "_render_exit_code": 1,
                        "public_runner_role": "THIN_TRANSPORT_ONLY",
                        "canonical_motor_owns_discovery": True,
                    },
                )
            except Exception:
                pass
        print(
            json.dumps(
                {
                    "public_runner": "ERROR",
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:1200],
                    "request_id": job.get("request_id") if job else None,
                }
            ),
            flush=True,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
