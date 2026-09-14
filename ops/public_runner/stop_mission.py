from __future__ import annotations

import json
import os
import re

import mission_control
import runner_once as base


def _mission_id_from_trigger() -> str:
    title = os.environ.get("GFO_PUBLIC_TRIGGER_TITLE", "").strip()
    match = re.match(r"^\[GFO-STOP\]\s+([A-Za-z0-9_.:-]+)(?:\s|$)", title, flags=re.IGNORECASE)
    if not match:
        raise RuntimeError("trigger must be [GFO-STOP] <mission_id>")
    return match.group(1)


def main() -> int:
    mission_id = _mission_id_from_trigger()
    transport = base._load_transport()
    before = mission_control.load_mission(transport, mission_id=mission_id)
    before_result = mission_control._json_object(before.get("result"))
    state = mission_control.request_stop(transport, mission_id=mission_id)
    if before_result.get("state") in {
        "AUTHORITY_REQUIRED",
        "GOLD_PASS_DONE",
        "CHECKPOINTED_MOTOR_WAVE_CAP",
        "MOTOR_PASS_DONE_NO_AUTHORITY",
    }:
        state = mission_control._update(
            transport,
            mission_id=mission_id,
            state="STOPPED_BY_USER",
            status="SUCCEEDED",
            next_action="NONE",
            wave=int(state.get("wave") or 1),
            found_agency=int(state.get("found_agency") or 0),
            found_direct=int(state.get("found_direct") or 0),
            last_checkpoint=str(state.get("last_checkpoint") or "MISSION_STARTED"),
            stop_requested=True,
        )
    safe = {
        "mission_id": mission_id,
        "state": state.get("state"),
        "found_new_gold": state.get("found_new_gold"),
        "mission_target": state.get("mission_target"),
        "deficit": state.get("deficit"),
        "wave": state.get("wave"),
        "last_checkpoint": state.get("last_checkpoint"),
        "next_action": state.get("next_action"),
        "stop_requested": state.get("stop_requested"),
        "send_authority": "NOT_GRANTED",
        "outbound_side_effects": False,
    }
    print(json.dumps(safe, sort_keys=True), flush=True)
    return 0 if state.get("stop_requested") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
