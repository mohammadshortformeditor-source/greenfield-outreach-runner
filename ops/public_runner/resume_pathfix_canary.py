from __future__ import annotations

import json
import os
import re
import sys
import uuid
from pathlib import Path

from sqlalchemy import create_engine, text

import mission_control
import runner_once as base
import resume_authority_once as authority


QUEUE = "gfo_render_operator_queue"


def _trigger() -> tuple[str, int | str]:
    title = os.environ.get("GFO_PUBLIC_TRIGGER_TITLE", "").strip()
    fresh = re.match(r"^\[GFO-PATHFIX\]\s+FIND\s+([1-9][0-9]{0,2})(?:\s|$)", title, flags=re.IGNORECASE)
    if fresh:
        return "FRESH", int(fresh.group(1))
    exact = re.match(r"^\[GFO-PATHFIX\]\s+REQUEST\s+([A-Za-z0-9_.:-]+)(?:\s|$)", title, flags=re.IGNORECASE)
    if exact:
        return "REQUEST", exact.group(1)
    resume = re.match(r"^\[GFO-PATHFIX\]\s+([0-9]+)(?:\s|$)", title, flags=re.IGNORECASE)
    if resume:
        return "RESUME", resume.group(1)
    raise RuntimeError("trigger must be [GFO-PATHFIX] FIND N, [GFO-PATHFIX] <source github run id>, or [GFO-PATHFIX] REQUEST <request_id>")


def _fresh_payload(target: int) -> dict:
    run_id = os.environ.get("GITHUB_RUN_ID", uuid.uuid4().hex)
    return {
        "schema": "gfo.operator-request.v1",
        "operation": "FIND_GOLD_BATCH",
        "release_id": "R0007",
        "request_id": f"pathfix-fresh-find_gold_batch-{run_id}",
        "send_authority": "NOT_GRANTED",
        "outbound_side_effects": False,
        "target_count": target,
        "goal_profile_id": base.GOAL_PROFILE_ID,
        "target_constraints_hash": base.TARGET_CONSTRAINTS_HASH,
    }


def _load_pathfix_stage1(transport, source_request_id: str) -> tuple[dict, dict]:
    db = transport.engine()
    try:
        with db.begin() as connection:
            row = connection.execute(
                text(
                    f"""
                    SELECT payload, result
                    FROM {QUEUE}
                    WHERE request_id = :request_id
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"request_id": source_request_id},
            ).mappings().first()
    finally:
        db.dispose()
    if row is None:
        raise RuntimeError("pathfix source stage-1 result not found")
    stage1_payload = authority._json_object(row["payload"], "pathfix stage1 payload")
    stage1 = authority._json_object(row["result"], "pathfix stage1 result")
    external = stage1.get("external_authority_batch_request")
    expected_sources = [
        "CURRENT_GREENFIELD_DB",
        "GMAIL_SENT",
        "LEGACY_PRIMARY",
        "LEGACY_SNAPSHOT",
    ]
    if (
        stage1.get("status") != "EXTERNAL_AUTHORITY_BATCH_REQUIRED"
        or stage1.get("pre_gold_filter_policy") != "FOUR_SOURCE_PRIOR_CONTACT_ONLY"
        or stage1.get("prior_contact_filter_contract") != "FOUR_SOURCE_EXACT_EMAIL_ACTUAL_OUTREACH_ONLY_V1"
        or not isinstance(external, dict)
        or external.get("sources") != expected_sources
        or stage1.get("motor_performs_gold_qualification") is True
        or stage1.get("send_authority") != "NOT_GRANTED"
    ):
        raise RuntimeError("pathfix source stage-1 four-source contract not verified")
    return stage1, stage1_payload


def main() -> int:
    mode, value = _trigger()
    transport = base._load_transport()
    if mode == "FRESH":
        payload = _fresh_payload(int(value))
    else:
        source_request_id = (
            str(value)
            if mode == "REQUEST"
            else f"pathfix-fresh-find_gold_batch-{value}"
        )
        stage1, stage1_payload = _load_pathfix_stage1(transport, source_request_id)
        payload = authority._resume_payload(stage1, stage1_payload)
        payload["request_id"] = f"pathfix-canary-find_gold_batch-{os.environ.get('GITHUB_RUN_ID', uuid.uuid4().hex)}"
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
            if mode == "FRESH":
                baseline = mission_control._current_fast_cash_counts(connection)
                target = int(value)
                agency_target, direct_target = mission_control.balanced_targets(target)
                payload.update(
                    {
                        "mission_id": f"pathfix-{os.environ.get('GITHUB_RUN_ID', uuid.uuid4().hex)}",
                        "mission_contract": mission_control.MISSION_CONTRACT,
                        "mission_requested_new_gold": target,
                        "mission_fast_cash_baseline": baseline,
                        "mission_target_mix": {"AGENCY": agency_target, "DIRECT": direct_target},
                        "mission_wave": 1,
                    }
                )
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

    contract_verified = None
    if mode == "FRESH":
        external = result.get("external_authority_batch_request")
        sources = external.get("sources") if isinstance(external, dict) else None
        active = result.get("active_pre_gold_filter_components")
        expected_sources = [
            "CURRENT_GREENFIELD_DB",
            "GMAIL_SENT",
            "LEGACY_PRIMARY",
            "LEGACY_SNAPSHOT",
        ]
        contract_verified = (
            result.get("status") == "EXTERNAL_AUTHORITY_BATCH_REQUIRED"
            and result.get("pre_gold_filter_policy") == "FOUR_SOURCE_PRIOR_CONTACT_ONLY"
            and result.get("prior_contact_filter_contract") == "FOUR_SOURCE_EXACT_EMAIL_ACTUAL_OUTREACH_ONLY_V1"
            and result.get("mission_scope_contract") == mission_control.MISSION_CONTRACT
            and result.get("mission_scope_active") is True
            and sources == expected_sources
            and active == expected_sources
            and result.get("motor_performs_gold_qualification") is not True
            and result.get("send_authority") == "NOT_GRANTED"
        )
        if not contract_verified:
            exit_code = 1

    base._persist_private_result(transport, payload, result, exit_code)
    safe = {
        "public_runner": "PATHFIX_CANARY_FINISHED",
        "mode": mode,
        "private_result_persisted": True,
        "request_id": request_id,
        "status": result.get("status"),
        "error_type": result.get("error_type"),
        "error": str(result.get("error") or "")[:300] or None,
        "raw_motor_to_gold_bridge_id": result.get("raw_motor_to_gold_bridge_id"),
        "fresh_four_source_contract_verified": contract_verified,
        "four_source_resume_verified": result.get("four_source_resume_verified"),
        "motor_role": result.get("motor_role"),
        "pre_gold_filter_policy": result.get("pre_gold_filter_policy"),
        "prior_contact_filter_contract": result.get("prior_contact_filter_contract"),
        "pre_gold_path": result.get("pre_gold_path"),
        "mission_scope_contract": result.get("mission_scope_contract"),
        "mission_scope_active": result.get("mission_scope_active"),
        "gold_count": result.get("gold_count"),
        "gold_gates_unchanged": result.get("gold_gates_unchanged"),
        "quality_relaxation": result.get("quality_relaxation"),
        "send_authority": result.get("send_authority", "NOT_GRANTED"),
        "exit_code": exit_code,
    }
    print(json.dumps(safe, sort_keys=True), flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
