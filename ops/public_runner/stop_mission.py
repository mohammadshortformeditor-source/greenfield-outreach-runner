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
    state = mission_control.request_stop(transport, mission_id=mission_id)
    safe = {
        "mission_id": mission_id,
        "state": state.get("state"),
        "found_new_gold": state.get("found_new_gold"),
        "wave": state.get("wave"),
        "next_action": state.get("next_action"),
        "stop_requested": state.get("stop_requested"),
        "send_authority": "NOT_GRANTED",
        "outbound_side_effects": False,
    }
    print(json.dumps(safe, sort_keys=True), flush=True)
    return 0 if state.get("stop_requested") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
