"""TDD tests for Item 1 — historical ``/rounds`` pre-fetch resolver.

Background (UCL strategy C)
---------------------------
The Phase 4 ``round_slug`` orchestrator path needs ``season_round`` to
be populated before it can route the per-round fetches. The existing
``SeasonOfRegistryUTResolver`` covers only the ±60 day window through
``/rounds`` — so for historical cup-style seasons (UCL 23/24, EURO
2020, FIFA WC 2018, etc.) ``season_round`` stays empty and Phase 4
silently falls through to ``/events/last/{p}`` fallback.

Worse — Phase 4 effect can't reach UCL at all because UCL category
priority is 19, and the strict cat-priority barrier holds cat=19+
behind cat=20 for an indefinite drain.

Item 1 decouples the round catalog fetch from the cursor walk. A new
scope ``season-of-registry-ut-rounds-historical`` yields (UT, season)
pairs from ``event`` × ``tournament_registry`` where ``season_round``
is empty. The resource refresh loop fetches ``/rounds`` for them once
(per-pair refresh_interval gate keeps it cheap). After the fetch
lands, the pair drops out of the resolver — no re-fetch.

Trade-offs
----------
* No periodic refresh: ``/rounds`` for active seasons would normally
  benefit from a refresh cadence (round metadata can drift mid-
  season). This resolver fires once per pair, ever. The compensating
  signal is acceptable for cup formats where round structure is set
  at the start of the tournament and doesn't morph.
* Recent ±60d seasons also covered (no special-case): once their
  catalog lands the pair drops out, mirroring the historical path.
"""

from __future__ import annotations

import json
import unittest


class _StubConnection:
    """asyncpg-shaped connection that returns a fixed row set on fetch."""

    def __init__(self, rows: list[dict] | None = None) -> None:
        self.rows = rows or []
        self.last_query: str | None = None
        self.last_args: tuple = ()

    async def fetch(self, query: str, *args):
        self.last_query = query
        self.last_args = args
        return list(self.rows)


class _StubDatabase:
    def __init__(self, connection: _StubConnection) -> None:
        self._connection = connection

    def connection(self):  # noqa: D401 — used like ``async with database.connection()``
        conn = self._connection

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *args):
                return None

        return _Ctx()


class SeasonOfRegistryUTRoundsHistoricalResolverContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_kind_is_distinct_from_legacy_scope(self) -> None:
        """Item 1 introduces a new scope_kind separate from the legacy
        ±60d ``season-of-registry-ut`` so we don't accidentally
        broaden ``/cuptrees`` coverage (that's Item 4's job)."""
        from schema_inspector.services.resource_scope.season_of_registry_ut_rounds_historical import (
            SeasonOfRegistryUTRoundsHistoricalResolver,
        )

        self.assertEqual(
            SeasonOfRegistryUTRoundsHistoricalResolver.kind,
            "season-of-registry-ut-rounds-historical",
        )

    async def test_known_scope_kinds_includes_new_kind(self) -> None:
        """Fix 3 ``KNOWN_SCOPE_KINDS`` validation rejects unknown
        scope_kinds at module-import time. Item 1 must register the
        new kind there or the ``SofascoreEndpoint`` switchover trips
        the validator."""
        from schema_inspector._scope_kinds import KNOWN_SCOPE_KINDS

        self.assertIn(
            "season-of-registry-ut-rounds-historical", KNOWN_SCOPE_KINDS
        )

    async def test_resolver_exports_from_resource_scope_package(self) -> None:
        from schema_inspector.services.resource_scope import (
            SeasonOfRegistryUTRoundsHistoricalResolver,
        )

        self.assertEqual(
            SeasonOfRegistryUTRoundsHistoricalResolver.kind,
            "season-of-registry-ut-rounds-historical",
        )

    async def test_query_filters_by_active_historical_enabled_registry(self) -> None:
        """Only registered + active + historical_enabled UTs are in
        scope. ``tournament_registry`` is the source of truth for
        which UTs we agreed to ingest."""
        from schema_inspector.services.resource_scope.season_of_registry_ut_rounds_historical import (
            SeasonOfRegistryUTRoundsHistoricalResolver,
        )

        conn = _StubConnection(rows=[])
        resolver = SeasonOfRegistryUTRoundsHistoricalResolver(
            database=_StubDatabase(conn),
            redis_backend=None,
            env={},
            sport_slug="football",
        )
        await resolver.resolve()
        query = conn.last_query or ""
        self.assertIn("tournament_registry", query)
        self.assertIn("is_active", query)
        self.assertIn("historical_enabled", query)

    async def test_query_excludes_seasons_already_populated(self) -> None:
        """A (UT, season) pair whose ``season_round`` already has rows
        MUST NOT be returned. Otherwise we'd re-fetch ``/rounds`` for
        every season on every planner tick, defeating the
        "fetch-once-then-drop-out" semantic."""
        from schema_inspector.services.resource_scope.season_of_registry_ut_rounds_historical import (
            SeasonOfRegistryUTRoundsHistoricalResolver,
        )

        conn = _StubConnection(rows=[])
        resolver = SeasonOfRegistryUTRoundsHistoricalResolver(
            database=_StubDatabase(conn),
            redis_backend=None,
            env={},
            sport_slug="football",
        )
        await resolver.resolve()
        query = conn.last_query or ""
        # The SQL must NOT EXISTS-filter against ``season_round``.
        self.assertIn("season_round", query)
        self.assertIn("NOT EXISTS", query)

    async def test_query_filters_by_sport_slug(self) -> None:
        """Default sport is football, but tournaments from other
        sports (tennis/baseball/etc.) shouldn't pollute the football
        rounds queue."""
        from schema_inspector.services.resource_scope.season_of_registry_ut_rounds_historical import (
            SeasonOfRegistryUTRoundsHistoricalResolver,
        )

        conn = _StubConnection(rows=[])
        resolver = SeasonOfRegistryUTRoundsHistoricalResolver(
            database=_StubDatabase(conn),
            redis_backend=None,
            env={},
            sport_slug="football",
        )
        await resolver.resolve()
        # sport_slug is the first positional bind ($1).
        self.assertEqual(conn.last_args[0], "football")

    async def test_resolve_returns_resource_targets(self) -> None:
        from schema_inspector.services.resource_scope.season_of_registry_ut_rounds_historical import (
            SeasonOfRegistryUTRoundsHistoricalResolver,
        )

        rows = [
            {"ut_id": 7, "season_id": 61644},  # UCL 24/25
            {"ut_id": 16, "season_id": 41087},  # WC 2022
        ]
        conn = _StubConnection(rows=rows)
        resolver = SeasonOfRegistryUTRoundsHistoricalResolver(
            database=_StubDatabase(conn),
            redis_backend=None,
            env={},
            sport_slug="football",
        )
        targets = list(await resolver.resolve())
        self.assertEqual(len(targets), 2)
        # First target — UCL 24/25.
        self.assertEqual(targets[0].entity_type, "season")
        self.assertEqual(targets[0].entity_id, 61644)
        self.assertEqual(
            targets[0].path_params,
            {"unique_tournament_id": 7, "season_id": 61644},
        )
        # Context attrs are populated for downstream wiring (proxy
        # priority, audit logs, etc.).
        self.assertEqual(targets[0].context_unique_tournament_id, 7)
        self.assertEqual(targets[0].context_season_id, 61644)
        self.assertEqual(targets[0].sport_slug, "football")


class RoundsEndpointPointsToNewScopeTests(unittest.TestCase):
    """``UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT`` switches scope from
    the legacy ±60d ``season-of-registry-ut`` to the new historical
    pre-fetch scope. ``/cuptrees`` keeps the legacy scope — its
    historical coverage is Item 4."""

    def test_rounds_endpoint_uses_new_historical_scope(self) -> None:
        from schema_inspector.endpoints import UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT

        self.assertEqual(
            UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT.scope_kind,
            "season-of-registry-ut-rounds-historical",
        )

    def test_cuptrees_endpoint_keeps_legacy_scope(self) -> None:
        """Item 4 will handle ``/cuptrees`` separately. Item 1 must
        not silently broaden cuptree coverage."""
        from schema_inspector.endpoints import UNIQUE_TOURNAMENT_SEASON_CUPTREES_ENDPOINT

        self.assertEqual(
            UNIQUE_TOURNAMENT_SEASON_CUPTREES_ENDPOINT.scope_kind,
            "season-of-registry-ut",
        )


if __name__ == "__main__":
    unittest.main()
