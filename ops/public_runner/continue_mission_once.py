from __future__ import annotations

import json
import os
import re
import uuid

import mission_control
import runner_once as base


def _mission_id_from_trigger() -> str:
    title = os.environ.get("GFO_PUBLIC_TRIGGER_TITLE", "").strip()
    match = re.match(r"^\[GFO-CONTINUE\]\s+([A-Za-z0-9_.:-]+)(?:\s|$)", title, flags=re.IGNORECASE)
    if not match:
        raise RuntimeError("trigger must be [GFO-CONTINUE] <mission_id>")
    return match.group(1)


def main() -> int:
    mission_id = _mission_id_from_trigger()
    transport = base._load_transport()
    mission = mission_control.load_mission(transport, mission_id=mission_id)
    state = mission_control._json_object(mission.get("result"))

    if mission_control.stop_is_requested(mission):
        final = base._stop_mission_now(
            transport,
            mission_id=mission_id,
            wave=int(state.get("wave") or 1),
        )
        result = {"status": "STOPPED_BY_USER"}
        exit_code = 0
    else:
        if str(mission.get("status")) != "RUNNING" or state.get("next_action") != "RUN_MOTOR":
            raise RuntimeError("mission is not checkpointed for Motor continuation")
        prior_wave = int(state.get("wave") or 1)
        wave = prior_wave if state.get("state") == "MOTOR_RUNNING" else prior_wave + 1
        request_id = f"publicrunner-mission-{mission_id}-wave-{wave}-continue-{os.environ.get('GITHUB_RUN_ID', uuid.uuid4().hex)}"
        payload = base._motor_payload_for_wave(
            mission,
            mission_id=mission_id,
            wave=wave,
            request_id=request_id,
        )
        result, exit_code, final = base._run_motor_until_boundary(
            transport,
            mission_id=mission_id,
            first_payload=payload,
            first_wave=wave,
        )

    safe = {
        "public_runner": "MISSION_CONTINUE_FINISHED",
        "mission_id": mission_id,
        "status": result.get("status"),
        "mission_state": final.get("state"),
        "mission_found_new_gold": final.get("found_new_gold"),
        "mission_target": final.get("mission_target"),
        "mission_deficit": final.get("deficit"),
        "mission_wave": final.get("wave"),
        "last_checkpoint": final.get("last_checkpoint"),
        "failure_at": final.get("failure_at"),
        "mission_next_action": final.get("next_action"),
        "send_authority": "NOT_GRANTED",
        "outbound_side_effects": False,
        "exit_code": exit_code,
    }
    print(json.dumps(safe, sort_keys=True), flush=True)
    return 0 if exit_code == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
