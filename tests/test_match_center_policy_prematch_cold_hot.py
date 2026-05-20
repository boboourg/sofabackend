"""Stage 5.2 (2026-05-21): pre-match cold/hot matrix + highlights 180m.

Pre-match endpoint selection used to be a single flat allowlist that
included BOTH static data (h2h, managers, pregame-form, team-streaks)
AND dynamic editorial data (odds, votes, winning-odds). For a match
10 days out, fetching odds + votes is wasteful — they will rotate
many times before kickoff. The new matrix splits the pre-match
window:

* Cold lane (1-14 days before kickoff): static endpoints only.
* Hot lane (< 24 h before kickoff): cold lane + odds + votes + winning-odds.

Also: ``FOOTBALL_HIGHLIGHTS_DELAY_SECONDS`` bumped from 150 minutes to
180 minutes (3 h) so the highlights poll lands once upstream actually
publishes the clip — empirical observation shows the 150-minute cap
fired on a still-empty highlights bucket for top matches.
"""

from __future__ import annotations

import unittest


_COLD_ENDPOINTS = frozenset(
    {
        "/api/v1/event/{event_id}/h2h",
        "/api/v1/event/{custom_id}/h2h/events",
        "/api/v1/event/{event_id}/managers",
        "/api/v1/event/{event_id}/pregame-form",
        "/api/v1/event/{event_id}/team-streaks",
    }
)
_DYNAMIC_ENDPOINTS = frozenset(
    {
        "/api/v1/event/{event_id}/odds/{provider_id}/all",
        "/api/v1/event/{event_id}/odds/{provider_id}/featured",
        "/api/v1/event/{event_id}/provider/{provider_id}/winning-odds",
        "/api/v1/event/{event_id}/votes",
        "/api/v1/event/{event_id}/team-streaks/betting-odds/{provider_id}",
    }
)


class PrematchHighlightsDelayTests(unittest.TestCase):
    def test_highlights_delay_is_180_minutes(self) -> None:
        from schema_inspector.match_center_policy import FOOTBALL_HIGHLIGHTS_DELAY_SECONDS

        self.assertEqual(
            FOOTBALL_HIGHLIGHTS_DELAY_SECONDS,
            180 * 60,
            msg=(
                "FOOTBALL_HIGHLIGHTS_DELAY_SECONDS must be 180 minutes "
                "(3 hours). Was 150 — upstream highlights bucket "
                "still empty at 150m for top matches."
            ),
        )

    def test_highlights_allowed_at_exactly_180_minutes(self) -> None:
        """status_code=100 (finished) + 180 minutes after kickoff →
        allowed exactly at threshold (>=)."""
        from schema_inspector.match_center_policy import football_highlights_allowed

        start_ts = 1_700_000_000
        now_ts = start_ts + 180 * 60  # exactly 180m after kickoff
        self.assertTrue(
            football_highlights_allowed(
                sport_slug="football",
                detail_id=1,  # tier_1
                status_type="finished",
                has_global_highlights=True,
                start_timestamp=start_ts,
                now_timestamp=now_ts,
            ),
            msg="At exactly 180m after kickoff, highlights must be allowed.",
        )

    def test_highlights_blocked_at_179_minutes(self) -> None:
        from schema_inspector.match_center_policy import football_highlights_allowed

        start_ts = 1_700_000_000
        now_ts = start_ts + 179 * 60
        self.assertFalse(
            football_highlights_allowed(
                sport_slug="football",
                detail_id=1,
                status_type="finished",
                has_global_highlights=True,
                start_timestamp=start_ts,
                now_timestamp=now_ts,
            ),
            msg="Before 180m the highlights call must still be suppressed.",
        )


class PrematchColdHotMatrixTests(unittest.TestCase):
    """``football_detail_endpoint_allowed`` for notstarted events must
    apply cold (>24h) / hot (<=24h) split based on start_timestamp."""

    def _allowed(self, *, endpoint_pattern: str, seconds_until_kickoff: int) -> bool:
        from schema_inspector.match_center_policy import football_detail_endpoint_allowed

        now_ts = 1_700_000_000
        start_ts = now_ts + seconds_until_kickoff
        return football_detail_endpoint_allowed(
            sport_slug="football",
            endpoint_pattern=endpoint_pattern,
            detail_id=1,  # tier_1
            status_type="notstarted",
            has_xg=True,
            has_event_player_heat_map=True,
            has_event_player_statistics=True,
            has_global_highlights=True,
            start_timestamp=start_ts,
            now_timestamp=now_ts,
        )

    def test_cold_window_10_days_allows_static_endpoints(self) -> None:
        """Match 10 days out → cold-lane static endpoints all allowed."""
        ten_days = 10 * 86_400
        for endpoint in _COLD_ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                self.assertTrue(
                    self._allowed(endpoint_pattern=endpoint, seconds_until_kickoff=ten_days),
                    msg=f"Cold-window endpoint must be allowed 10 days out: {endpoint}",
                )

    def test_cold_window_10_days_blocks_dynamic_endpoints(self) -> None:
        """Match 10 days out → /odds, /votes, /winning-odds, betting-odds
        team-streaks all blocked (the editorial picks rotate too often)."""
        ten_days = 10 * 86_400
        for endpoint in _DYNAMIC_ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                self.assertFalse(
                    self._allowed(endpoint_pattern=endpoint, seconds_until_kickoff=ten_days),
                    msg=(
                        f"Dynamic endpoint must NOT be fetched 10 days out: {endpoint}. "
                        "Cold lane covers static pre-match metadata only — odds/votes "
                        "rotate, so this far out the fetch is wasted."
                    ),
                )

    def test_hot_window_2_hours_allows_static_endpoints(self) -> None:
        two_hours = 2 * 3600
        for endpoint in _COLD_ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                self.assertTrue(
                    self._allowed(endpoint_pattern=endpoint, seconds_until_kickoff=two_hours),
                    msg=f"Static endpoint still allowed in hot lane: {endpoint}",
                )

    def test_hot_window_2_hours_allows_dynamic_endpoints(self) -> None:
        """Match 2h before kickoff → odds, votes, winning-odds all
        allowed — these are now meaningful to fetch."""
        two_hours = 2 * 3600
        for endpoint in _DYNAMIC_ENDPOINTS:
            with self.subTest(endpoint=endpoint):
                self.assertTrue(
                    self._allowed(endpoint_pattern=endpoint, seconds_until_kickoff=two_hours),
                    msg=(
                        f"Dynamic endpoint must be allowed in hot window (<24h): {endpoint}"
                    ),
                )

    def test_boundary_exactly_24h_classified_as_hot(self) -> None:
        """At exactly the 24-hour boundary, the matrix treats the
        event as hot — the cutoff is inclusive (<=24h = hot).
        Otherwise events 24h+30s away would silently flip back to
        cold mid-fetch."""
        twenty_four_hours = 24 * 3600
        self.assertTrue(
            self._allowed(
                endpoint_pattern="/api/v1/event/{event_id}/odds/{provider_id}/all",
                seconds_until_kickoff=twenty_four_hours,
            ),
            msg="At exactly 24h to kickoff, odds endpoint must be hot-allowed.",
        )

    def test_just_outside_24h_window_blocks_dynamic(self) -> None:
        """24h + 60s = still cold."""
        self.assertFalse(
            self._allowed(
                endpoint_pattern="/api/v1/event/{event_id}/votes",
                seconds_until_kickoff=24 * 3600 + 60,
            ),
            msg="24h+60s before kickoff → votes endpoint blocked (cold lane).",
        )


if __name__ == "__main__":
    unittest.main()
