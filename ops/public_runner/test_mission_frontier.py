from __future__ import annotations

import mission_control


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
