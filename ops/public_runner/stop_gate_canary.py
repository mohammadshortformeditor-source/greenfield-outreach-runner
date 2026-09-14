from __future__ import annotations

import json
import os
import uuid

from sqlalchemy import text

import mission_control
import runner_once


def _cleanup(transport, mission_id: str) -> None:
    db = transport.engine()
    try:
        with db.begin() as connection:
            connection.execute(
                text(
                    f"DELETE FROM {mission_control.QUEUE} WHERE request_id=:request_id AND operation=:operation"
                ),
                {
                    "request_id": mission_control.mission_request_id(mission_id),
                    "operation": mission_control.MISSION_OPERATION,
                },
            )
    finally:
        db.dispose()


def main() -> int:
    transport = runner_once._load_transport()
    mission_id = f"stop-canary-{os.environ.get('GITHUB_RUN_ID', uuid.uuid4().hex)}"
    source_request_id = f"stop-canary-source-{mission_id}"
    motor_called = False
    original_execute = runner_once._execute_payload

    def forbidden_motor(*args, **kwargs):
        nonlocal motor_called
        motor_called = True
        raise RuntimeError("Motor executed after STOP_REQUESTED")

    try:
        mission = mission_control.ensure_mission(
            transport,
            mission_id=mission_id,
            requested_new_gold=2,
            source_request_id=source_request_id,
        )
        stop_state = mission_control.request_stop(transport, mission_id=mission_id)
        runner_once._execute_payload = forbidden_motor
        result, exit_code, final_state = runner_once._run_motor_until_boundary(
            transport,
            mission_id=mission_id,
            first_payload={
                "schema": "gfo.operator-request.v1",
                "operation": "FIND_GOLD_BATCH",
                "release_id": "R0007",
                "request_id": source_request_id,
                "target_count": 2,
                **mission_control.mission_fields(mission, wave=1),
                "send_authority": "NOT_GRANTED",
                "outbound_side_effects": False,
            },
            first_wave=1,
        )
        verified = (
            stop_state.get("state") == "STOP_REQUESTED"
            and stop_state.get("stop_requested") is True
            and motor_called is False
            and exit_code == 0
            and result.get("status") == "STOPPED_BY_USER"
            and final_state.get("state") == "STOPPED_BY_USER"
            and final_state.get("next_action") == "NONE"
            and final_state.get("stop_requested") is True
        )
        safe = {
            "stop_gate_verified": verified,
            "motor_called_after_stop": motor_called,
            "initial_stop_state": stop_state.get("state"),
            "final_state": final_state.get("state"),
            "found_new_gold": final_state.get("found_new_gold"),
            "next_action": final_state.get("next_action"),
            "send_authority": "NOT_GRANTED",
            "outbound_side_effects": False,
        }
        print(json.dumps(safe, sort_keys=True), flush=True)
        return 0 if verified else 1
    finally:
        runner_once._execute_payload = original_execute
        _cleanup(transport, mission_id)


if __name__ == "__main__":
    raise SystemExit(main())
