"""TDD tests for Phase 4 — named-knockout-round endpoint
``/events/round/{N}/slug/{slug}``.

Background
----------
Sofascore exposes cup-stage events via two related URLs:

* ``/season/{s}/events/round/{N}`` — works for group-stage rounds whose
  ``round_slug`` is NULL (e.g. matchday 1, 2, 3).
* ``/season/{s}/events/round/{N}/slug/{slug}`` — Sofascore's knockout
  URL with named round (e.g. ``round/29/slug/final``,
  ``round/27/slug/quarterfinals``).

The latter is what their frontend uses for cup knockout pages. We never
declared it, so for cup-style seasons (FIFA WC, EURO, UCL knockout)
we couldn't fetch the named rounds — all 64 WC 2022 events ended up
with ``round_number=5`` and ``slug='round-of-16'`` (the Sofascore
``/events/last/{p}`` fallback's compact tag).

This module pins the constant + URL-build contract before any
parser/job wiring (steps 2-4 in the Phase 4 plan).
"""

from __future__ import annotations

import unittest


class RoundSlugEndpointDeclarationTests(unittest.TestCase):
    """``UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT`` ships with the
    correct path template, envelope key, and target table so the
    parser registry can dispatch it like the existing round endpoint."""

    def test_endpoint_constant_exists(self) -> None:
        from schema_inspector.endpoints import (
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
        )

        self.assertIsNotNone(UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT)

    def test_path_template_matches_sofascore_url(self) -> None:
        from schema_inspector.endpoints import (
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
        )

        self.assertEqual(
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT.path_template,
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}"
            "/events/round/{round_number}/slug/{round_slug}",
        )

    def test_envelope_key_is_events(self) -> None:
        """Same envelope as the slug-less round endpoint — Sofascore
        wraps the list in ``{"events": [...]}`` either way."""
        from schema_inspector.endpoints import (
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
        )

        self.assertEqual(
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT.envelope_key, "events"
        )

    def test_target_table_is_event(self) -> None:
        """Like the slug-less variant: rows land in the ``event`` table."""
        from schema_inspector.endpoints import (
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
        )

        self.assertEqual(
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT.target_table, "event"
        )

    def test_no_scope_kind_on_demand_only(self) -> None:
        """On-demand only — the orchestrator fetches per ``season_round``
        catalog entry. No resource refresh loop participation, no
        ``scope_kind`` required."""
        from schema_inspector.endpoints import (
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
        )

        self.assertIsNone(UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT.scope_kind)
        self.assertIsNone(
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT.refresh_interval_seconds
        )

    def test_origin_default_upstream(self) -> None:
        from schema_inspector.endpoints import (
            EndpointOrigin,
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
        )

        self.assertEqual(
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT.origin,
            EndpointOrigin.UPSTREAM,
        )


class RoundSlugEndpointUrlBuildTests(unittest.TestCase):
    """``build_url`` interpolates path params correctly for the FIFA WC
    Final case the audit caught — ``ut=16, season=58210, round=29,
    slug=final``."""

    def test_build_url_for_wc_2026_final(self) -> None:
        from schema_inspector.endpoints import (
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
        )

        url = UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT.build_url(
            unique_tournament_id=16,
            season_id=58210,
            round_number=29,
            round_slug="final",
        )
        self.assertEqual(
            url,
            "https://www.sofascore.com/api/v1/unique-tournament/16/season/58210"
            "/events/round/29/slug/final",
        )

    def test_build_url_for_quarterfinals(self) -> None:
        from schema_inspector.endpoints import (
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
        )

        url = UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT.build_url(
            unique_tournament_id=7,  # UEFA Champions League
            season_id=52162,  # 2023/2024 season
            round_number=27,
            round_slug="quarterfinals",
        )
        self.assertEqual(
            url,
            "https://www.sofascore.com/api/v1/unique-tournament/7/season/52162"
            "/events/round/27/slug/quarterfinals",
        )

    def test_build_url_for_match_for_3rd_place(self) -> None:
        """Hyphenated slugs (``match-for-3rd-place``) must pass through
        unchanged — Sofascore uses them as-is in URLs."""
        from schema_inspector.endpoints import (
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
        )

        url = UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT.build_url(
            unique_tournament_id=16,
            season_id=41087,
            round_number=50,
            round_slug="match-for-3rd-place",
        )
        self.assertEqual(
            url,
            "https://www.sofascore.com/api/v1/unique-tournament/16/season/41087"
            "/events/round/50/slug/match-for-3rd-place",
        )


class RoundSlugEndpointRegistryEntryTests(unittest.TestCase):
    """Prod-bug regression pin (2026-05-19): the slug-aware endpoint
    must show up in ``event_list_registry_entries`` so the seeder
    populates the ``endpoint_registry`` table. Without this, every
    slug-routed fetch hits an FK constraint violation when the worker
    tries to write the snapshot:

        Key (endpoint_pattern)=(...events/round/{round_number}/slug/{round_slug})
        is not present in table "endpoint_registry".

    Observed in prod within seconds of the initial Phase 4 deploy.
    """

    def test_endpoint_in_event_list_registry_entries(self) -> None:
        from schema_inspector.endpoints import (
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
            event_list_registry_entries,
        )

        patterns = {entry.pattern for entry in event_list_registry_entries()}
        self.assertIn(UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT.pattern, patterns)

    def test_endpoint_in_event_list_endpoints_tuple(self) -> None:
        """``event_list_endpoints()`` is the upstream of the registry
        entries function — pin membership there too, with each sport
        slug we ship, so a future ``sport_slug`` switch can't drop the
        endpoint silently."""
        from schema_inspector.endpoints import (
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
            event_list_endpoints,
        )

        for sport_slug in ("football", "basketball", "tennis"):
            endpoints = event_list_endpoints(sport_slug)
            self.assertIn(
                UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
                endpoints,
                f"Slug-aware round endpoint missing from event_list_endpoints({sport_slug!r}). "
                f"Without it the endpoint_registry seeder skips the row and prod "
                f"hits FK violations on every slug-routed fetch.",
            )


if __name__ == "__main__":
    unittest.main()
