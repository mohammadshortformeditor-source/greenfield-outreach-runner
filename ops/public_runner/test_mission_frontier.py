from __future__ import annotations

import pytest

import mission_control
import runner_once


def test_failed_route_attempts_are_preserved_without_emitted_candidates():
    sources, routes = mission_control._candidate_frontier({
        "motor_attempted_source_urls": ["https://www.acme.test/jobs/editor/"],
        "engine_candidates": [],
    })
    assert sources == ["https://acme.test/jobs/editor"]
    assert routes == []


def test_candidate_frontier_tracks_exact_routes_and_normalized_sources() -> None:
    sources, routes = mission_control._candidate_frontier(
        {
            "engine_candidates": [
                {
                    "search": {"source_url": "HTTPS://WWW.Example.com/jobs/editor/"},
                    "contact_route": {"route_value": " Hiring@Example.com "},
                },
                {
                    "search": {"source_url": "https://example.com/jobs/editor"},
                    "contact_route": {"route_value": "hiring@example.com"},
                },
            ]
        }
    )
    assert sources == ["https://example.com/jobs/editor"]
    assert routes == ["hiring@example.com"]


def test_mission_fields_forward_prior_wave_frontier_to_motor() -> None:
    fields = mission_control.mission_fields(
        {
            "payload": {
                "mission_id": "mission-1",
                "requested_new_gold": 2,
                "mission_fast_cash_baseline": {"AGENCY": 3, "DIRECT": 4},
                "target_mix": {"AGENCY": 1, "DIRECT": 1},
            },
            "result": {
                "wave": 2,
                "seen_source_urls": ["https://example.com/jobs/editor"],
                "seen_routes": ["hiring@example.com"],
            },
        },
        wave=3,
    )
    assert fields["mission_wave"] == 3
    assert fields["mission_seen_source_urls"] == ["https://example.com/jobs/editor"]
    assert fields["mission_seen_routes"] == ["hiring@example.com"]


def test_completed_mission_never_starts_another_motor_wave(monkeypatch) -> None:
    monkeypatch.setattr(mission_control, "load_mission", lambda *a, **k: {
        "result": {"state": "TARGET_MET", "deficit": 0}
    })
    monkeypatch.setattr(mission_control, "_update", lambda *a, **k: k)
    monkeypatch.setattr(runner_once, "_execute_payload", lambda *a, **k: (
        pytest.fail("Motor must stay idle after target")
    ))
    result, code, checkpoint = runner_once._run_motor_until_boundary(
        object(), mission_id="done", first_payload={"request_id": "never"}, first_wave=2
    )
    assert code == 0
    assert result["status"] == checkpoint["state"] == "TARGET_MET"


def test_no_authority_wave_also_checkpoints_frontier(monkeypatch) -> None:
    updates = []
    monkeypatch.setenv("GFO_MISSION_MOTOR_ONLY_WAVE_CAP", "1")
    monkeypatch.setattr(mission_control, "load_mission", lambda *a, **k: {
        "result": {"state": "RUNNING", "deficit": 2}
    })
    def update(*args, **kwargs):
        updates.append(kwargs)
        return kwargs
    monkeypatch.setattr(mission_control, "_update", update)
    monkeypatch.setattr(runner_once, "_persist_private_result", lambda *a, **k: None)
    monkeypatch.setattr(runner_once, "_execute_payload", lambda *a, **k: ({
        "status": "MOTOR_DONE", "continue_discovery": True,
        "engine_candidates": [{
            "search": {"source_url": "https://example.com/editor"},
            "contact_route": {"route_value": "hiring@example.com"},
        }],
    }, 0))
    runner_once._run_motor_until_boundary(
        object(), mission_id="active", first_payload={"request_id": "wave-1"}, first_wave=1
    )
    checkpoint = next(x for x in updates if x["state"] == "MOTOR_PASS_DONE_NO_AUTHORITY")
    assert checkpoint["seen_source_urls"] == ["https://example.com/editor"]
    assert checkpoint["seen_routes"] == ["hiring@example.com"]
