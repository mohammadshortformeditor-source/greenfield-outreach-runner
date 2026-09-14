from __future__ import annotations

import json
import os
import re
import sys
import uuid
from pathlib import Path

from sqlalchemy import create_engine

import runner_once as base
import resume_authority_once as authority


def _source_request_id() -> str:
    title = os.environ.get("GFO_PUBLIC_TRIGGER_TITLE", "").strip()
    match = re.match(r"^\[GFO-PATHFIX\]\s+([0-9]+)(?:\s|$)", title, flags=re.IGNORECASE)
    if not match:
        raise RuntimeError("trigger must be [GFO-PATHFIX] <source github run id>")
    return f"publicrunner-find_gold_batch-{match.group(1)}"


def main() -> int:
    transport = base._load_transport()
    stage1 = authority._load_stage1(transport, _source_request_id())
    payload = authority._resume_payload(stage1)
    payload["request_id"] = f"pathfix-canary-find_gold_batch-{os.environ.get('GITHUB_RUN_ID', uuid.uuid4().hex)}"
    request_id = str(payload["request_id"])

    engine_root = Path(os.environ.get("GFO_ENGINE_ROOT", "engine")).resolve()
    # GREENFIELD uses a src layout and several runtime config paths are resolved from the
    # source checkout. Load the canary from that checkout, never from site-packages.
    os.chdir(engine_root)
    sys.path.insert(0, str(engine_root / "src"))
    sys.path.insert(0, str(engine_root))

    import outreach.runtime.r0007_cheap_first_wavefront as cheap_first
    import outreach.runtime.r0007_raw_motor_to_gold_bridge as raw_bridge
    from outreach.runtime.r0007_four_source_to_gold_fix import (
        R0007FourSourceDirectToGoldOperatorExecutionService,
    )

    # Match the existing free-transport tuning without changing motor/Gold logic.
    cheap_first.MAX_ENGINE_WORKERS = 8
    cheap_first.DEFAULT_HTTP_TIMEOUT_SECONDS = 3.0
    raw_bridge.TRANSPORT_SAFE_HTTP_CACHE_MAX_ENTRIES = 4
    raw_bridge.TRANSPORT_SAFE_ROUTE_HIT_CAP = 72

    dsn = transport.lease("FIND_GOLD_BATCH", request_id)
    db = create_engine(dsn, future=True, pool_pre_ping=True)
    try:
        with db.begin() as connection:
            result = dict(
                R0007FourSourceDirectToGoldOperatorExecutionService().execute(
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
    safe = {
        "public_runner": "PATHFIX_CANARY_FINISHED",
        "private_result_persisted": True,
        "request_id": request_id,
        "status": result.get("status"),
        "four_source_to_gold_fix_id": result.get("four_source_to_gold_fix_id"),
        "authority_survivors": result.get("external_authority_survivor_count"),
        "gold_entry_count": result.get("gold_entry_count"),
        "gold_count": result.get("gold_count"),
        "pre_gold_filter_policy": result.get("pre_gold_filter_policy"),
        "gold_gates_unchanged": result.get("gold_gates_unchanged"),
        "quality_relaxation": result.get("quality_relaxation"),
        "send_authority": result.get("send_authority", "NOT_GRANTED"),
        "exit_code": exit_code,
    }
    print(json.dumps(safe, sort_keys=True), flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
