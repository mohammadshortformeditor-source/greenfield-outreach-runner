from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path

from sqlalchemy import create_engine

import runner_once as base


def _normalize_trigger_for_base() -> None:
    title = os.environ.get("GFO_PUBLIC_TRIGGER_TITLE", "").strip()
    if title.upper().startswith("[GFO-GUARD]"):
        os.environ["GFO_PUBLIC_TRIGGER_TITLE"] = "[GFO-RUN]" + title[len("[GFO-GUARD]"):]


def main() -> int:
    _normalize_trigger_for_base()
    transport = base._load_transport()
    payload = base._request_from_trigger()
    request_id = str(payload["request_id"])

    engine_root = Path(os.environ.get("GFO_ENGINE_ROOT", "engine")).resolve()
    os.chdir(engine_root)
    sys.path.insert(0, str(engine_root / "src"))
    sys.path.insert(0, str(engine_root))

    from outreach.runtime.r0007_raw_motor_to_gold_bridge import (
        R0007RawMotorToGoldBridgeOperatorExecutionService,
    )

    dsn = transport.norm(transport.lease("FIND_GOLD_BATCH", request_id))
    db = create_engine(dsn, future=True, pool_pre_ping=True)
    try:
        with db.begin() as connection:
            result = dict(
                R0007RawMotorToGoldBridgeOperatorExecutionService().execute(
                    connection,
                    payload,
                )
            )
    except Exception as exc:
        result = {
            "status": "FAILED_FAIL_CLOSED",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "quality_relaxation": False,
            "send_authority": "NOT_GRANTED",
            "outbound_side_effects": False,
        }
        exit_code = 1
    else:
        exit_code = 0
    finally:
        db.dispose()

    base._persist_private_result(transport, payload, result, exit_code)
    external = result.get("external_authority_batch_request")
    sources = external.get("sources") if isinstance(external, dict) else None
    safe = {
        "public_runner": "FRESH_MOTOR_CANARY_FINISHED",
        "private_result_persisted": True,
        "request_id": request_id,
        "status": result.get("status"),
        "raw_motor_to_gold_bridge_id": result.get("raw_motor_to_gold_bridge_id"),
        "motor_role": result.get("motor_role"),
        "motor_performs_gold_qualification": result.get("motor_performs_gold_qualification"),
        "external_authority_sources": sources,
        "external_authority_candidate_count": result.get("external_authority_candidate_count"),
        "pre_gold_filter_policy": result.get("pre_gold_filter_policy"),
        "pre_gold_path": result.get("pre_gold_path"),
        "gold_gates_unchanged": result.get("gold_gates_unchanged"),
        "quality_relaxation": result.get("quality_relaxation"),
        "send_authority": result.get("send_authority", "NOT_GRANTED"),
        "exit_code": exit_code,
    }
    print(json.dumps(safe, sort_keys=True), flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
