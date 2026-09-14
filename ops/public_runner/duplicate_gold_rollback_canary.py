from __future__ import annotations

import json
import os
import re
import sys
import uuid
from pathlib import Path

from sqlalchemy import create_engine

import mission_control
import runner_once as base
import resume_authority_once as authority


def main() -> int:
    title = os.environ.get("GFO_PUBLIC_TRIGGER_TITLE", "").strip()
    match = re.match(r"^\[GFO-DUPCANARY\]\s+REQUEST\s+([A-Za-z0-9_.:-]+)(?:\s|$)", title, flags=re.IGNORECASE)
    if not match:
        raise RuntimeError("trigger must be [GFO-DUPCANARY] REQUEST <request_id>")
    source_request_id = match.group(1)

    transport = base._load_transport()
    stage1, stage1_payload = authority._load_stage1(transport, source_request_id)
    payload = authority._resume_payload(stage1, stage1_payload)
    payload["request_id"] = f"duplicate-rollback-canary-{os.environ.get('GITHUB_RUN_ID', uuid.uuid4().hex)}"

    engine_root = Path(os.environ.get("GFO_ENGINE_ROOT", "engine")).resolve()
    os.chdir(engine_root)
    sys.path.insert(0, str(engine_root / "src"))
    sys.path.insert(0, str(engine_root))
    from outreach.runtime.r0007_raw_motor_to_gold_bridge import R0007RawMotorToGoldBridgeOperatorExecutionService

    dsn = transport.norm(transport.lease("FIND_GOLD_BATCH", payload["request_id"]))
    db = create_engine(dsn, future=True, pool_pre_ping=True)
    connection = db.connect()
    transaction = connection.begin()
    rolled_back = False
    try:
        baseline = mission_control._current_fast_cash_counts(connection)
        target = int(payload.get("target_count") or 10)
        agency_target, direct_target = mission_control.balanced_targets(target)
        payload.update({
            "mission_id": f"duplicate-canary-{os.environ.get('GITHUB_RUN_ID', uuid.uuid4().hex)}",
            "mission_contract": mission_control.MISSION_CONTRACT,
            "mission_requested_new_gold": target,
            "mission_fast_cash_baseline": baseline,
            "mission_target_mix": {"AGENCY": agency_target, "DIRECT": direct_target},
            "mission_wave": 1,
        })
        result = dict(R0007RawMotorToGoldBridgeOperatorExecutionService().execute(connection, payload))
    except Exception as exc:
        result = {
            "status": "FAILED_FAIL_CLOSED",
            "error_type": type(exc).__name__,
            "error": str(exc),
            "quality_relaxation": False,
            "send_authority": "NOT_GRANTED",
        }
    finally:
        transaction.rollback()
        rolled_back = True
        connection.close()
        db.dispose()

    evaluated = result.get("evaluated_results") if isinstance(result.get("evaluated_results"), list) else []
    duplicate_skipped = sum(
        1 for row in evaluated
        if isinstance(row, dict) and row.get("reason") == "ALREADY_GOLD_CURRENT_SCOPE"
    )
    verified = (
        rolled_back
        and result.get("status") != "FAILED_FAIL_CLOSED"
        and duplicate_skipped >= 1
        and result.get("gold_gates_unchanged") is True
        and result.get("quality_relaxation") is False
        and result.get("send_authority") == "NOT_GRANTED"
    )
    safe = {
        "duplicate_guard_verified": verified,
        "duplicate_skipped_count": duplicate_skipped,
        "transaction_rolled_back": rolled_back,
        "status": result.get("status"),
        "error_type": result.get("error_type"),
        "error": str(result.get("error") or "")[:300] or None,
        "gold_gates_unchanged": result.get("gold_gates_unchanged"),
        "quality_relaxation": result.get("quality_relaxation"),
        "send_authority": result.get("send_authority", "NOT_GRANTED"),
    }
    print(json.dumps(safe, sort_keys=True), flush=True)
    return 0 if verified else 1


if __name__ == "__main__":
    raise SystemExit(main())
