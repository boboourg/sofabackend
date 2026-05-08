"""Tests for GateContextResolver (P3.2)."""

from __future__ import annotations

import asyncio
import unittest
from dataclasses import dataclass

from schema_inspector.services.gate_context_resolver import (
    GateContext,
    GateContextResolver,
)


@dataclass
class _FakeTask:
    endpoint_pattern: str
    sport_slug: str | None = None
    context_event_id: int | None = None
    context_unique_tournament_id: int | None = None
    context_season_id: int | None = None


class _FakeDatabase:
    """Minimal fake matching Database.fetchrow signature.

    Keeps a registry of canned rows keyed by (sql_marker, *args) to avoid
    parsing actual SQL strings.
    """

    def __init__(self) -> None:
        self.event_rows: dict[int, dict[str, object]] = {}
        self.ut_sport_rows: dict[int, str] = {}
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, sql: str, *args: object) -> dict[str, object] | None:
        self.fetchrow_calls.append((sql, args))
        # Detect query type by SQL substring (avoids brittle parsing).
        if "FROM event ev" in sql:
            event_id = int(args[0])
            return self.event_rows.get(event_id)
        if "FROM unique_tournament ut" in sql:
            ut_id = int(args[0])
            slug = self.ut_sport_rows.get(ut_id)
            return {"slug": slug} if slug else None
        return None


class GateContextResolverTests(unittest.TestCase):
    def test_event_path_with_full_db_lookup(self) -> None:
        db = _FakeDatabase()
        db.event_rows[12345] = {"sport_slug": "football", "ut_id": 17, "is_editor": False}
        resolver = GateContextResolver(database=db)
        task = _FakeTask(
            endpoint_pattern="/api/v1/event/{event_id}/lineups",
            context_event_id=12345,
        )
        ctx = asyncio.run(resolver.resolve(task))
        self.assertTrue(ctx.has_full_context)
        self.assertEqual(ctx.sport_slug, "football")
        self.assertEqual(ctx.unique_tournament_id, 17)
        self.assertEqual(ctx.event_id, 12345)
        self.assertFalse(ctx.is_editor)

    def test_event_path_is_editor_true(self) -> None:
        db = _FakeDatabase()
        db.event_rows[99999] = {"sport_slug": "football", "ut_id": 17, "is_editor": True}
        resolver = GateContextResolver(database=db)
        task = _FakeTask(
            endpoint_pattern="/api/v1/event/{event_id}",
            context_event_id=99999,
        )
        ctx = asyncio.run(resolver.resolve(task))
        self.assertTrue(ctx.has_full_context)
        self.assertTrue(ctx.is_editor)

    def test_event_path_missing_event_id_returns_missing_context(self) -> None:
        db = _FakeDatabase()
        resolver = GateContextResolver(database=db)
        task = _FakeTask(endpoint_pattern="/api/v1/event/{event_id}/lineups")
        ctx = asyncio.run(resolver.resolve(task))
        self.assertFalse(ctx.has_full_context)
        self.assertIn("context_event_id missing", ctx.missing_reason)

    def test_event_not_in_db_returns_missing_context(self) -> None:
        db = _FakeDatabase()
        # Event 77777 not registered in db.event_rows
        resolver = GateContextResolver(database=db)
        task = _FakeTask(
            endpoint_pattern="/api/v1/event/{event_id}/lineups",
            context_event_id=77777,
        )
        ctx = asyncio.run(resolver.resolve(task))
        self.assertFalse(ctx.has_full_context)

    def test_event_lookup_is_cached(self) -> None:
        db = _FakeDatabase()
        db.event_rows[55555] = {"sport_slug": "football", "ut_id": 17, "is_editor": False}
        resolver = GateContextResolver(database=db)
        task = _FakeTask(
            endpoint_pattern="/api/v1/event/{event_id}/lineups",
            context_event_id=55555,
        )
        asyncio.run(resolver.resolve(task))
        asyncio.run(resolver.resolve(task))
        # 2 resolve calls but DB hit only once for the same event_id.
        self.assertEqual(len(db.fetchrow_calls), 1)
        self.assertEqual(resolver.cache_size(), 1)

    def test_ut_path_with_task_supplied_context(self) -> None:
        # When task carries ut_id + sport_slug, no DB lookup needed.
        db = _FakeDatabase()
        resolver = GateContextResolver(database=db)
        task = _FakeTask(
            endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall",
            sport_slug="football",
            context_unique_tournament_id=17,
            context_season_id=76986,
        )
        ctx = asyncio.run(resolver.resolve(task))
        self.assertTrue(ctx.has_full_context)
        self.assertEqual(ctx.unique_tournament_id, 17)
        self.assertEqual(ctx.season_id, 76986)
        self.assertEqual(ctx.sport_slug, "football")
        self.assertEqual(len(db.fetchrow_calls), 0)

    def test_ut_path_missing_ut_id_returns_missing_context(self) -> None:
        db = _FakeDatabase()
        resolver = GateContextResolver(database=db)
        task = _FakeTask(
            endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall",
            sport_slug="football",
        )
        ctx = asyncio.run(resolver.resolve(task))
        self.assertFalse(ctx.has_full_context)
        self.assertIn("unique_tournament_id missing", ctx.missing_reason)

    def test_ut_path_resolves_sport_via_db_when_missing(self) -> None:
        db = _FakeDatabase()
        db.ut_sport_rows[17] = "football"
        resolver = GateContextResolver(database=db)
        task = _FakeTask(
            endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall",
            context_unique_tournament_id=17,
        )
        ctx = asyncio.run(resolver.resolve(task))
        self.assertTrue(ctx.has_full_context)
        self.assertEqual(ctx.sport_slug, "football")

    def test_team_path_returns_missing_context_phase1(self) -> None:
        db = _FakeDatabase()
        resolver = GateContextResolver(database=db)
        task = _FakeTask(endpoint_pattern="/api/v1/team/{team_id}/players")
        ctx = asyncio.run(resolver.resolve(task))
        self.assertFalse(ctx.has_full_context)
        self.assertIn("team-level", ctx.missing_reason)
        # No DB hit — short-circuit.
        self.assertEqual(len(db.fetchrow_calls), 0)

    def test_player_path_returns_missing_context_phase1(self) -> None:
        db = _FakeDatabase()
        resolver = GateContextResolver(database=db)
        task = _FakeTask(endpoint_pattern="/api/v1/player/{player_id}/statistics")
        ctx = asyncio.run(resolver.resolve(task))
        self.assertFalse(ctx.has_full_context)
        self.assertIn("player-level", ctx.missing_reason)

    def test_sport_listing_path_returns_missing_context(self) -> None:
        db = _FakeDatabase()
        resolver = GateContextResolver(database=db)
        task = _FakeTask(endpoint_pattern="/api/v1/sport/football/events/live")
        ctx = asyncio.run(resolver.resolve(task))
        self.assertFalse(ctx.has_full_context)
        self.assertIn("sport-listing", ctx.missing_reason)

    def test_unsupported_path_returns_missing_context(self) -> None:
        db = _FakeDatabase()
        resolver = GateContextResolver(database=db)
        task = _FakeTask(endpoint_pattern="/api/v1/manager/{manager_id}")
        ctx = asyncio.run(resolver.resolve(task))
        self.assertFalse(ctx.has_full_context)

    def test_empty_endpoint_returns_missing_context(self) -> None:
        db = _FakeDatabase()
        resolver = GateContextResolver(database=db)
        task = _FakeTask(endpoint_pattern="")
        ctx = asyncio.run(resolver.resolve(task))
        self.assertFalse(ctx.has_full_context)

    def test_no_database_event_path(self) -> None:
        # Resolver works without a database — falls back to task fields.
        resolver = GateContextResolver(database=None)
        task = _FakeTask(
            endpoint_pattern="/api/v1/event/{event_id}/lineups",
            sport_slug="football",
            context_event_id=12345,
            context_unique_tournament_id=17,
        )
        ctx = asyncio.run(resolver.resolve(task))
        self.assertTrue(ctx.has_full_context)
        self.assertEqual(ctx.sport_slug, "football")
        self.assertEqual(ctx.unique_tournament_id, 17)

    def test_db_lookup_failure_falls_open_safely(self) -> None:
        class BrokenDB:
            async def fetchrow(self, sql, *args):  # noqa: ARG002
                raise RuntimeError("connection refused")

        resolver = GateContextResolver(database=BrokenDB())
        task = _FakeTask(
            endpoint_pattern="/api/v1/event/{event_id}/lineups",
            context_event_id=12345,
        )
        ctx = asyncio.run(resolver.resolve(task))
        # Lookup raised → cached as (None, None, False), missing context.
        self.assertFalse(ctx.has_full_context)


if __name__ == "__main__":
    unittest.main()
