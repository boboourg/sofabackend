"""Stage 5.1 (2026-05-21): featured-players gated by upcoming-match.

Before this fix ``TEAM_FEATURED_PLAYERS_ENDPOINT.scope_kind`` was
``"team-of-active-ut"`` — every team in every active UT, refreshed
once per day, regardless of whether that team had an upcoming
match. Counted across 13 days that landed 13-14 snapshots per
active team, ~1000 active teams → ~365k requests/year for an
editorial pick that only matters between matches.

The fix introduces ``TeamWithUpcomingMatchResolver`` which returns
only team_ids whose event has status_code = 0 (notstarted) AND
start_timestamp BETWEEN now AND now + 14 days. Combined with
freshness_ttl_seconds = 14 * 86400 the endpoint fetches:

 * exactly once when a new upcoming match appears in the 14-day window
   (and freshness TTL has expired)
 * never while the team is in live or finished states
 * never when there is no future match scheduled in the next 14 days

The user-facing semantics: "fetched once after the previous match
ended, available for the next match without re-fetching".
"""

from __future__ import annotations

import unittest


class TeamWithUpcomingMatchResolverContractTests(unittest.IsolatedAsyncioTestCase):
    def test_resolver_kind_constant(self) -> None:
        from schema_inspector.services.resource_scope.team_with_upcoming_match import (
            TeamWithUpcomingMatchResolver,
        )
        self.assertEqual(
            TeamWithUpcomingMatchResolver.kind, "team-with-upcoming-match"
        )

    def test_scope_kind_registered_in_known_set(self) -> None:
        from schema_inspector._scope_kinds import KNOWN_SCOPE_KINDS
        self.assertIn(
            "team-with-upcoming-match",
            KNOWN_SCOPE_KINDS,
            msg=(
                "KNOWN_SCOPE_KINDS must include the new resolver kind, "
                "otherwise SofascoreEndpoint.__post_init__ validation "
                "raises at module-import time."
            ),
        )

    def test_resolver_exported_from_package(self) -> None:
        """``services.resource_scope`` must re-export the resolver so
        the planner picks it up alongside existing ones."""
        from schema_inspector.services.resource_scope import (
            TeamWithUpcomingMatchResolver,
        )
        self.assertEqual(
            TeamWithUpcomingMatchResolver.kind, "team-with-upcoming-match"
        )

    async def test_resolve_filters_by_status_code_and_future_window(self) -> None:
        """The SQL behind ``resolve()`` must include WHERE-filters on
        ``event.status_code = 0`` (notstarted) AND a forward-looking
        start_timestamp window. This is the only place that gates the
        endpoint to 'pre-match only'."""
        from pathlib import Path

        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "services"
            / "resource_scope"
            / "team_with_upcoming_match.py"
        ).read_text(encoding="utf-8")
        # Source-level pinning — the resolver SQL must filter by
        # status_code AND a future-window timestamp.
        self.assertIn(
            "status_code = 0",
            text,
            msg=(
                "Resolver SQL must filter event.status_code = 0 "
                "(notstarted) — otherwise live/finished teams would "
                "be returned and the fix becomes a no-op."
            ),
        )
        self.assertIn(
            "start_timestamp",
            text,
            msg=(
                "Resolver SQL must filter event.start_timestamp to a "
                "forward window so only upcoming matches qualify."
            ),
        )


class TeamFeaturedPlayersEndpointTests(unittest.TestCase):
    def test_featured_players_endpoint_uses_upcoming_match_scope(self) -> None:
        from schema_inspector.endpoints import TEAM_FEATURED_PLAYERS_ENDPOINT
        self.assertEqual(
            TEAM_FEATURED_PLAYERS_ENDPOINT.scope_kind,
            "team-with-upcoming-match",
            msg=(
                "TEAM_FEATURED_PLAYERS_ENDPOINT.scope_kind must be "
                "switched from 'team-of-active-ut' to "
                "'team-with-upcoming-match' so the endpoint is gated "
                "by the new resolver."
            ),
        )

    def test_featured_players_endpoint_has_long_freshness_ttl(self) -> None:
        """14-day freshness_ttl_seconds means a successful fetch
        before match-day caches through the entire pre-match window
        (and beyond, until the team's next future match)."""
        from schema_inspector.endpoints import TEAM_FEATURED_PLAYERS_ENDPOINT
        self.assertGreaterEqual(
            TEAM_FEATURED_PLAYERS_ENDPOINT.freshness_ttl_seconds,
            14 * 86400,
            msg=(
                "freshness_ttl_seconds must be at least 14 days so the "
                "single pre-match fetch survives without re-querying "
                "upstream during the live + finished phases."
            ),
        )


if __name__ == "__main__":
    unittest.main()
