"""Phase 1 of REDUNDANT_ENDPOINTS_AUDIT.md — disable redundant refresh paths.

Rationale (see ``docs/REDUNDANT_ENDPOINTS_AUDIT.md``):

* ``team/{id}/events/last|next`` write into the ``event`` table — exact same
  rows the canonical UT-level pipeline already produces via
  ``unique-tournament/{ut}/season/{s}/events/{round|last|next}``. Refreshing
  them every 6h burns ~160 HTTP/day per EPL-sized league for **zero** unique
  rows. Action: clear ``scope_kind`` + ``refresh_interval_seconds`` so the
  resource refresh planner stops picking them up.

* ``player/{id}/events/last/{page}`` carries ``playedForTeamMap`` and
  ``statisticsMap`` keys that ARE derivable from
  ``event_player_statistics JOIN event``. With ``auto_paginate=True`` and
  ``max_pages=50`` it costs ~10K HTTP/day on a single EPL for **zero new
  event_id**. Action: keep page=0 refresh (cheap, ~500 HTTP/day) but drop
  ``auto_paginate``. The local API server already has a synthesize fallback
  in ``scheduled_events_synthesizer._FETCH_QUERY_PLAYER_EVENTS_LAST``.

These tests pin the new state so a future config tweak cannot silently
re-enable the redundant fetches.
"""

from __future__ import annotations

import unittest


class TeamEventsEndpointsRefreshDisabledTests(unittest.TestCase):
    """Phase 1.1 — team/{id}/events/{last,next} must NOT auto-refresh."""

    def test_team_last_events_endpoint_has_no_scope_kind(self) -> None:
        from schema_inspector.endpoints import team_last_events_endpoint

        endpoint = team_last_events_endpoint()
        self.assertIsNone(
            endpoint.scope_kind,
            "team/{id}/events/last must not be picked up by resource refresh "
            "planner — it writes the same rows as the UT-level canonical "
            "discovery path.",
        )

    def test_team_last_events_endpoint_has_no_refresh_interval(self) -> None:
        from schema_inspector.endpoints import team_last_events_endpoint

        endpoint = team_last_events_endpoint()
        self.assertIsNone(endpoint.refresh_interval_seconds)

    def test_team_last_events_endpoint_has_no_freshness_ttl(self) -> None:
        from schema_inspector.endpoints import team_last_events_endpoint

        endpoint = team_last_events_endpoint()
        self.assertIsNone(endpoint.freshness_ttl_seconds)

    def test_team_next_events_endpoint_has_no_scope_kind(self) -> None:
        from schema_inspector.endpoints import team_next_events_endpoint

        endpoint = team_next_events_endpoint()
        self.assertIsNone(endpoint.scope_kind)

    def test_team_next_events_endpoint_has_no_refresh_interval(self) -> None:
        from schema_inspector.endpoints import team_next_events_endpoint

        endpoint = team_next_events_endpoint()
        self.assertIsNone(endpoint.refresh_interval_seconds)

    def test_team_next_events_endpoint_has_no_freshness_ttl(self) -> None:
        from schema_inspector.endpoints import team_next_events_endpoint

        endpoint = team_next_events_endpoint()
        self.assertIsNone(endpoint.freshness_ttl_seconds)

    def test_team_last_events_endpoint_path_template_unchanged(self) -> None:
        """The endpoint URL stays available for on-demand callers
        (CLI, team_detail_cli) — only the refresh hook is disabled."""
        from schema_inspector.endpoints import team_last_events_endpoint

        endpoint = team_last_events_endpoint()
        self.assertEqual(
            endpoint.path_template,
            "/api/v1/team/{team_id}/events/last/{page}",
        )
        self.assertEqual(endpoint.target_table, "event")


class PlayerEventsLastEndpointAutoPaginateDisabledTests(unittest.TestCase):
    """Phase 1.2 — player/{id}/events/last/{page} must NOT auto-paginate.

    Page=0 stays on the 6h refresh cadence (the planner still picks it up
    via ``scope_kind="player-of-active-squad-first-page"``) but the worker
    no longer chains page=K+1. ``hasNextPage=true`` is ignored.
    """

    def test_player_events_last_endpoint_auto_paginate_is_false(self) -> None:
        from schema_inspector.endpoints import PLAYER_EVENTS_LAST_ENDPOINT

        self.assertFalse(
            PLAYER_EVENTS_LAST_ENDPOINT.auto_paginate,
            "player/{id}/events/last must not chain page=K+1 — page>=1 is "
            "redundant with event_player_statistics JOIN event aggregations "
            "available locally.",
        )

    def test_player_events_last_endpoint_audit_interval_is_none(self) -> None:
        """Without auto-pagination the PaginationDoneStore marker is moot."""
        from schema_inspector.endpoints import PLAYER_EVENTS_LAST_ENDPOINT

        self.assertIsNone(PLAYER_EVENTS_LAST_ENDPOINT.audit_interval_seconds)

    def test_player_events_last_endpoint_keeps_page_zero_refresh(self) -> None:
        """Page=0 must keep refreshing — frontend's player "Matches" tab needs
        recency. Only the chain is severed, not the page=0 entry."""
        from schema_inspector.endpoints import PLAYER_EVENTS_LAST_ENDPOINT

        self.assertEqual(
            PLAYER_EVENTS_LAST_ENDPOINT.scope_kind,
            "player-of-active-squad-first-page",
        )
        self.assertEqual(
            PLAYER_EVENTS_LAST_ENDPOINT.refresh_interval_seconds,
            6 * 3600,
        )


if __name__ == "__main__":
    unittest.main()
