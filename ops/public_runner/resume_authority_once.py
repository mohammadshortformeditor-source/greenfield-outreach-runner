from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import uuid
from pathlib import Path

from sqlalchemy import text

import runner_once as base

QUEUE = "gfo_render_operator_queue"


def _source_request_id() -> str:
    title = os.environ.get("GFO_PUBLIC_TRIGGER_TITLE", "").strip()
    match = re.match(r"^\[GFO-RESUME\]\s+([0-9]+)(?:\s|$)", title, flags=re.IGNORECASE)
    if not match:
        raise RuntimeError("trigger must be [GFO-RESUME] <source github run id>")
    return f"publicrunner-find_gold_batch-{match.group(1)}"


def _json_object(value, label: str) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise RuntimeError(f"{label} must be a JSON object")


def _load_stage1(transport, source_request_id: str) -> dict:
    db = transport.engine()
    try:
        with db.begin() as connection:
            row = connection.execute(
                text(
                    f"""
                    SELECT result
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
    result = _json_object(row["result"], "stage1 result")
    if result.get("status") != "EXTERNAL_AUTHORITY_BATCH_REQUIRED":
        raise RuntimeError("source result is not awaiting external authority")
    return result


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


def _resume_payload(stage1: dict) -> dict:
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
    target_count = int(receipt.get("search_run_requested_count") or stage1.get("target_count") or 10)
    return {
        "schema": "gfo.operator-request.v1",
        "operation": "FIND_GOLD_BATCH",
        "release_id": "R0007",
        "request_id": f"publicrunner-resume-find_gold_batch-{os.environ.get('GITHUB_RUN_ID', uuid.uuid4().hex)}",
        "target_count": target_count,
        "goal_profile_id": base.GOAL_PROFILE_ID,
        "target_constraints_hash": base.TARGET_CONSTRAINTS_HASH,
        "leads": leads,
        "engine_execution_receipt": receipt,
        "external_authority_batch_receipt": authority_receipt,
        "send_authority": "NOT_GRANTED",
        "outbound_side_effects": False,
    }


def main() -> int:
    transport = base._load_transport()
    stage1 = _load_stage1(transport, _source_request_id())
    payload = _resume_payload(stage1)
    request_id = str(payload["request_id"])

    env = os.environ.copy()
    env["GFO_PRODUCTION_DSN"] = transport.lease("FIND_GOLD_BATCH", request_id)
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
            raise RuntimeError("canonical R0007 resume exited without result")
        result = json.loads(output_path.read_text(encoding="utf-8"))

    base._persist_private_result(transport, payload, result, completed.returncode)
    safe = {
        "public_runner": "AUTHORITY_RESUME_FINISHED",
        "private_result_persisted": True,
        "request_id": request_id,
        "operation": "FIND_GOLD_BATCH",
        "status": result.get("status"),
        "gold_count": len(result.get("gold_leads") or result.get("gold") or result.get("leads") or []),
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
