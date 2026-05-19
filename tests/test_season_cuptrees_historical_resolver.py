"""TDD tests for Item 4 — historical ``/cuptrees`` pre-fetch resolver.

Background
----------
``UNIQUE_TOURNAMENT_SEASON_CUPTREES_ENDPOINT`` currently rides on the
legacy ``season-of-registry-ut`` scope which is bounded to a ±60-day
window. For historical cup-style competitions (FIFA WC 2018-2022,
UCL knockout history, EURO 2020, Copa America 2016-2024, ...) the
cuptree payload was never fetched — the cursor walk doesn't
naturally enter these seasons under the strict cat-priority barrier,
and the recent-window scope skips them.

Item 4 introduces a parallel scope for ``/cuptrees`` that mirrors the
Item-1 design for ``/rounds``: a new resolver yielding (UT, season)
pairs where ``season_cup_tree`` is still empty, regardless of date.
After the cuptree lands (or after Sofascore returns a structural 404
which the negative cache absorbs), the pair drops out of the scope.

``/rounds`` historical scope (Item 1) and ``/cuptrees`` historical
scope (this) live as separate resolvers so we can tune their cadences
and pilot rollouts independently. Their shape is identical otherwise.
"""

from __future__ import annotations

import unittest


class _StubConnection:
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

    def connection(self):
        conn = self._connection

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *args):
                return None

        return _Ctx()


class CuptreesHistoricalResolverContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_kind_is_distinct_from_rounds_scope(self) -> None:
        """Item 4 introduces its own scope_kind separate from Item 1's
        rounds scope — we want independent rollout + cadence control."""
        from schema_inspector.services.resource_scope.season_of_registry_ut_cuptrees_historical import (
            SeasonOfRegistryUTCuptreesHistoricalResolver,
        )

        self.assertEqual(
            SeasonOfRegistryUTCuptreesHistoricalResolver.kind,
            "season-of-registry-ut-cuptrees-historical",
        )

    async def test_known_scope_kinds_includes_new_kind(self) -> None:
        from schema_inspector._scope_kinds import KNOWN_SCOPE_KINDS

        self.assertIn(
            "season-of-registry-ut-cuptrees-historical", KNOWN_SCOPE_KINDS
        )

    async def test_resolver_exports_from_resource_scope_package(self) -> None:
        from schema_inspector.services.resource_scope import (
            SeasonOfRegistryUTCuptreesHistoricalResolver,
        )

        self.assertEqual(
            SeasonOfRegistryUTCuptreesHistoricalResolver.kind,
            "season-of-registry-ut-cuptrees-historical",
        )

    async def test_query_excludes_seasons_already_populated(self) -> None:
        """If ``season_cup_tree`` has at least one row for the pair,
        the resolver MUST NOT yield it. Otherwise we'd re-fetch
        ``/cuptrees`` every tick for the same season."""
        from schema_inspector.services.resource_scope.season_of_registry_ut_cuptrees_historical import (
            SeasonOfRegistryUTCuptreesHistoricalResolver,
        )

        conn = _StubConnection(rows=[])
        resolver = SeasonOfRegistryUTCuptreesHistoricalResolver(
            database=_StubDatabase(conn),
            redis_backend=None,
            env={},
            sport_slug="football",
        )
        await resolver.resolve()
        query = conn.last_query or ""
        self.assertIn("season_cup_tree", query)
        self.assertIn("NOT EXISTS", query)

    async def test_query_filters_by_active_historical_enabled_registry(self) -> None:
        from schema_inspector.services.resource_scope.season_of_registry_ut_cuptrees_historical import (
            SeasonOfRegistryUTCuptreesHistoricalResolver,
        )

        conn = _StubConnection(rows=[])
        resolver = SeasonOfRegistryUTCuptreesHistoricalResolver(
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

    async def test_resolve_returns_resource_targets(self) -> None:
        from schema_inspector.services.resource_scope.season_of_registry_ut_cuptrees_historical import (
            SeasonOfRegistryUTCuptreesHistoricalResolver,
        )

        rows = [
            {"ut_id": 7, "season_id": 61644},  # UCL 24/25
            {"ut_id": 16, "season_id": 41087},  # WC 2022
        ]
        conn = _StubConnection(rows=rows)
        resolver = SeasonOfRegistryUTCuptreesHistoricalResolver(
            database=_StubDatabase(conn),
            redis_backend=None,
            env={},
            sport_slug="football",
        )
        targets = list(await resolver.resolve())
        self.assertEqual(len(targets), 2)
        self.assertEqual(targets[0].entity_type, "season")
        self.assertEqual(
            targets[0].path_params,
            {"unique_tournament_id": 7, "season_id": 61644},
        )
        self.assertEqual(targets[0].context_unique_tournament_id, 7)


class CuptreesEndpointPointsToNewScopeTests(unittest.TestCase):
    def test_cuptrees_endpoint_uses_new_historical_scope(self) -> None:
        from schema_inspector.endpoints import UNIQUE_TOURNAMENT_SEASON_CUPTREES_ENDPOINT

        self.assertEqual(
            UNIQUE_TOURNAMENT_SEASON_CUPTREES_ENDPOINT.scope_kind,
            "season-of-registry-ut-cuptrees-historical",
        )


if __name__ == "__main__":
    unittest.main()
