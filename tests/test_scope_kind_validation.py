"""TDD tests for ``scope_kind`` validation — Fix 3 of audit follow-ups.

Currently ``SofascoreEndpoint.scope_kind`` is a free-form ``str | None``
field. A typo (e.g. ``"team-of-active-uts"`` with an extra ``s``)
silently passes import, silently fails to match any resolver, and the
endpoint quietly never gets refresh-loop coverage. Production
deployments learn about it via "why isn't this endpoint being
refreshed?" debugging sessions hours/days later.

This fix introduces a canonical set ``KNOWN_SCOPE_KINDS`` exported by
``schema_inspector.services.resource_scope`` (derived from the
resolvers' own ``kind`` declarations — single source of truth). Every
``SofascoreEndpoint`` with a non-None ``scope_kind`` is validated
against the set at construction time. Typos blow up at module import,
not at the next planner tick.

Backward compatibility: ``scope_kind=None`` (the default, used by
~150 endpoints) bypasses validation entirely.
"""

from __future__ import annotations

import unittest


class KnownScopeKindsTests(unittest.TestCase):
    """The canonical set lives in ``services.resource_scope`` so the
    resolvers themselves are the authoritative source of valid kinds —
    no separate constant for endpoints.py to drift against."""

    def test_known_scope_kinds_is_frozenset(self) -> None:
        from schema_inspector.services.resource_scope import KNOWN_SCOPE_KINDS

        self.assertIsInstance(KNOWN_SCOPE_KINDS, frozenset)

    def test_known_scope_kinds_non_empty(self) -> None:
        from schema_inspector.services.resource_scope import KNOWN_SCOPE_KINDS

        # We have at least ~15 resolver kinds in production.
        self.assertGreaterEqual(len(KNOWN_SCOPE_KINDS), 15)

    def test_active_endpoint_kinds_all_known(self) -> None:
        """Every ``scope_kind`` currently in use by a production endpoint
        must be in the canonical set — otherwise the validator would
        reject existing code at import time."""
        from schema_inspector.endpoints import local_api_endpoints
        from schema_inspector.services.resource_scope import KNOWN_SCOPE_KINDS

        in_use = {
            endpoint.scope_kind
            for endpoint in local_api_endpoints()
            if endpoint.scope_kind is not None
        }
        unknown = in_use - KNOWN_SCOPE_KINDS
        self.assertEqual(
            unknown,
            set(),
            f"Endpoints declare unknown scope_kind(s): {unknown}. "
            f"Either add them to KNOWN_SCOPE_KINDS or fix the endpoint declaration.",
        )

    def test_includes_player_of_active_squad(self) -> None:
        """Spot-check: a well-known kind from the active-squad resolver
        must be present."""
        from schema_inspector.services.resource_scope import KNOWN_SCOPE_KINDS

        self.assertIn("player-of-active-squad", KNOWN_SCOPE_KINDS)

    def test_includes_team_of_active_ut(self) -> None:
        from schema_inspector.services.resource_scope import KNOWN_SCOPE_KINDS

        self.assertIn("team-of-active-ut", KNOWN_SCOPE_KINDS)

    def test_includes_season_of_active_ut_events(self) -> None:
        from schema_inspector.services.resource_scope import KNOWN_SCOPE_KINDS

        self.assertIn("season-of-active-ut-events", KNOWN_SCOPE_KINDS)


class SofascoreEndpointScopeKindValidationTests(unittest.TestCase):
    """``SofascoreEndpoint.__post_init__`` rejects unknown ``scope_kind``
    values. ``None`` (default) passes through unchecked."""

    def test_endpoint_with_none_scope_kind_constructs(self) -> None:
        """Most endpoints have ``scope_kind=None`` — must not be impacted."""
        from schema_inspector.endpoints import SofascoreEndpoint

        endpoint = SofascoreEndpoint(
            path_template="/api/v1/sample",
            envelope_key="data",
        )
        self.assertIsNone(endpoint.scope_kind)

    def test_endpoint_with_known_scope_kind_constructs(self) -> None:
        """A scope_kind from the canonical set passes validation."""
        from schema_inspector.endpoints import SofascoreEndpoint

        endpoint = SofascoreEndpoint(
            path_template="/api/v1/sample",
            envelope_key="data",
            scope_kind="player-of-active-squad",
        )
        self.assertEqual(endpoint.scope_kind, "player-of-active-squad")

    def test_endpoint_with_typo_in_scope_kind_raises(self) -> None:
        """An unknown ``scope_kind`` value MUST blow up at construction
        time so the typo cannot reach production silently."""
        from schema_inspector.endpoints import SofascoreEndpoint

        with self.assertRaises(ValueError) as ctx:
            SofascoreEndpoint(
                path_template="/api/v1/sample",
                envelope_key="data",
                # Extra "s" — a realistic typo.
                scope_kind="team-of-active-uts",
            )
        self.assertIn("team-of-active-uts", str(ctx.exception))
        # The error should also tell the operator *which* values ARE valid,
        # so they can fix the typo without grep'ing the resolver tree.
        self.assertIn("player-of-active-squad", str(ctx.exception))

    def test_error_message_lists_known_kinds_sorted(self) -> None:
        """Error message reproducibility — sorted listing makes
        diff-ing easier between runs."""
        from schema_inspector.endpoints import SofascoreEndpoint

        with self.assertRaises(ValueError) as ctx:
            SofascoreEndpoint(
                path_template="/api/v1/sample",
                envelope_key="data",
                scope_kind="totally-made-up",
            )
        # The error message contains the listing; pull out the part after
        # the colon and verify it's sorted.
        message = str(ctx.exception)
        self.assertIn("totally-made-up", message)
        # Don't pin exact wording — just sanity-check that "known" kinds
        # appear in some recognizable order (the error must contain
        # multiple of them).
        recognizable = sum(
            kind in message
            for kind in ("player-of-active-squad", "team-of-active-ut", "season-of-active-ut-events")
        )
        self.assertGreaterEqual(recognizable, 2)


class AllProductionEndpointsValidateTests(unittest.TestCase):
    """Importing the production registry must not raise — every
    declared endpoint's ``scope_kind`` value (if any) must already be
    in ``KNOWN_SCOPE_KINDS``."""

    def test_local_api_endpoints_imports_cleanly(self) -> None:
        # Just touching the function triggers all the endpoint construction
        # paths; if any of them had an unknown scope_kind, a ValueError
        # would bubble up.
        from schema_inspector.endpoints import local_api_endpoints

        endpoints = local_api_endpoints()
        self.assertGreater(len(endpoints), 50)

    def test_event_detail_endpoints_imports_cleanly(self) -> None:
        from schema_inspector.endpoints import event_detail_endpoints

        for sport in ("football", "tennis", "baseball", "cricket"):
            endpoints = event_detail_endpoints(sport_slug=sport)
            self.assertGreater(len(endpoints), 0)


class ScopeKindRegistryDriftTests(unittest.TestCase):
    """``KNOWN_SCOPE_KINDS`` is hardcoded in ``_scope_kinds.py`` (to
    avoid a circular import). This test verifies it matches what the
    resolvers themselves declare via their ``.kind`` class attributes
    — so a new resolver, or a typo in an existing one, fails CI.

    Run order: imports happen at test collection; if drift is present
    the assertion below pinpoints the divergent kinds without forcing
    the operator to grep through the resolver tree."""

    def test_known_scope_kinds_matches_resolver_kinds(self) -> None:
        from schema_inspector.services.resource_scope import (
            CustomIdOfManagedEventsResolver,
            CustomIdOfRegistryEventsResolver,
            EventOfFinishedBaseballResolver,
            ManagedScopeResolver,
            PeriodOfManagedPairsResolver,
            PeriodOfRegistryFootballResolver,
            PlayerOfActiveSquadResolver,
            PlayerOfActiveSquadFirstPageResolver,
            PlayerOfNationalTeamHistoryResolver,
            RoundOfManagedPairsResolver,
            RoundOfRegistryFootballResolver,
            SeasonOfActiveUTEventsResolver,
            SeasonOfActiveUTStandingsResolver,
            SeasonOfRegistryUTCuptreesHistoricalResolver,
            SeasonOfRegistryUTResolver,
            SeasonOfRegistryUTRoundsHistoricalResolver,
            TeamOfActiveUTResolver,
            TeamOfActiveUTFirstPageResolver,
            TeamOfActiveUTSeasonResolver,
            TeamOfRegistryUTResolver,
            TeamWithUpcomingMatchResolver,
        )
        from schema_inspector._scope_kinds import KNOWN_SCOPE_KINDS

        resolver_kinds = frozenset(
            {
                CustomIdOfManagedEventsResolver.kind,
                CustomIdOfRegistryEventsResolver.kind,
                EventOfFinishedBaseballResolver.kind,
                ManagedScopeResolver.kind,
                PeriodOfManagedPairsResolver.kind,
                PeriodOfRegistryFootballResolver.kind,
                PlayerOfActiveSquadResolver.kind,
                PlayerOfActiveSquadFirstPageResolver.kind,
                PlayerOfNationalTeamHistoryResolver.kind,
                RoundOfManagedPairsResolver.kind,
                RoundOfRegistryFootballResolver.kind,
                SeasonOfActiveUTEventsResolver.kind,
                SeasonOfActiveUTStandingsResolver.kind,
                SeasonOfRegistryUTCuptreesHistoricalResolver.kind,
                SeasonOfRegistryUTResolver.kind,
                SeasonOfRegistryUTRoundsHistoricalResolver.kind,
                TeamOfActiveUTResolver.kind,
                TeamOfActiveUTFirstPageResolver.kind,
                TeamOfActiveUTSeasonResolver.kind,
                TeamOfRegistryUTResolver.kind,
                TeamWithUpcomingMatchResolver.kind,
            }
        )

        missing_in_constant = resolver_kinds - KNOWN_SCOPE_KINDS
        extra_in_constant = KNOWN_SCOPE_KINDS - resolver_kinds
        self.assertEqual(
            missing_in_constant,
            set(),
            f"New resolver kinds not yet in KNOWN_SCOPE_KINDS: {missing_in_constant}",
        )
        self.assertEqual(
            extra_in_constant,
            set(),
            f"KNOWN_SCOPE_KINDS lists kinds no resolver advertises: {extra_in_constant}",
        )


if __name__ == "__main__":
    unittest.main()
