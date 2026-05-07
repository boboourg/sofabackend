from __future__ import annotations

import asyncio
import json
import unittest

from schema_inspector.services.resource_scope.team_of_active_ut_season import (
    CACHE_KEY,
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_LIMIT,
    DEFAULT_WINDOW_DAYS,
    TeamOfActiveUTSeasonResolver,
)


class _FakeConnection:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.fetch_calls: list[tuple[str, tuple]] = []
        self.closed = False

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        return self.rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.closed = True
        return False


class _FakeDatabase:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows
        self.connection_calls = 0
        self._connection: _FakeConnection | None = None

    def connection(self):
        self.connection_calls += 1
        self._connection = _FakeConnection(self._rows)
        return self._connection


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, tuple[str, int | None]] = {}
        self.set_calls: list[tuple[str, str, int | None]] = []

    def get(self, key):
        item = self.store.get(key)
        return None if item is None else item[0]

    def set(self, key, value, ex=None):
        self.set_calls.append((key, value, ex))
        self.store[key] = (str(value), ex)
        return True


def _resolve(resolver: TeamOfActiveUTSeasonResolver):
    return tuple(asyncio.run(resolver.resolve()))


class TeamOfActiveUTSeasonResolverTests(unittest.TestCase):
    def test_kind_matches_endpoint_metadata(self) -> None:
        # The endpoint contract relies on this exact string.
        self.assertEqual(TeamOfActiveUTSeasonResolver.kind, "team-of-active-ut-season")

    def test_defaults_match_documented_values(self) -> None:
        self.assertEqual(DEFAULT_WINDOW_DAYS, 30)
        self.assertEqual(DEFAULT_LIMIT, 20_000)
        self.assertEqual(DEFAULT_CACHE_TTL_SECONDS, 1800)

    def test_resolves_triples_from_database_when_cache_empty(self) -> None:
        rows = [
            {"team_id": 2817, "unique_tournament_id": 8, "season_id": 77559},
            {"team_id": 35, "unique_tournament_id": 17, "season_id": 76986},
        ]
        database = _FakeDatabase(rows)
        redis_backend = _FakeRedis()
        resolver = TeamOfActiveUTSeasonResolver(database=database, redis_backend=redis_backend, env={})

        targets = _resolve(resolver)

        self.assertEqual(len(targets), 2)
        first = targets[0]
        self.assertEqual(first.entity_type, "team")
        self.assertEqual(first.entity_id, 2817)
        self.assertEqual(
            first.path_params,
            {"team_id": 2817, "unique_tournament_id": 8, "season_id": 77559},
        )
        self.assertEqual(first.context_unique_tournament_id, 8)
        self.assertEqual(first.context_season_id, 77559)
        self.assertEqual(database.connection_calls, 1)
        # Result was cached as a list of [team, ut, season] triples:
        self.assertEqual(len(redis_backend.set_calls), 1)
        cache_key, cached_value, ex = redis_backend.set_calls[0]
        self.assertEqual(cache_key, CACHE_KEY)
        self.assertEqual(json.loads(cached_value), [[2817, 8, 77559], [35, 17, 76986]])
        self.assertEqual(ex, DEFAULT_CACHE_TTL_SECONDS)

    def test_uses_cache_on_second_call_without_db(self) -> None:
        database = _FakeDatabase([])
        redis_backend = _FakeRedis()
        redis_backend.store[CACHE_KEY] = (json.dumps([[7, 1, 100], [9, 2, 101]]), DEFAULT_CACHE_TTL_SECONDS)
        resolver = TeamOfActiveUTSeasonResolver(database=database, redis_backend=redis_backend, env={})

        targets = _resolve(resolver)

        self.assertEqual(len(targets), 2)
        self.assertEqual([t.entity_id for t in targets], [7, 9])
        self.assertEqual([t.context_season_id for t in targets], [100, 101])
        self.assertEqual(database.connection_calls, 0)

    def test_garbage_cache_falls_back_to_database(self) -> None:
        rows = [{"team_id": 1, "unique_tournament_id": 2, "season_id": 3}]
        database = _FakeDatabase(rows)
        redis_backend = _FakeRedis()
        redis_backend.store[CACHE_KEY] = ("not-json", None)
        resolver = TeamOfActiveUTSeasonResolver(database=database, redis_backend=redis_backend, env={})

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [1])
        self.assertEqual(database.connection_calls, 1)

    def test_redis_failure_falls_back_to_database(self) -> None:
        class _BrokenRedis:
            def get(self, key):
                raise RuntimeError("boom")

            def set(self, key, value, ex=None):
                raise RuntimeError("boom-write")

        rows = [{"team_id": 99, "unique_tournament_id": 8, "season_id": 77559}]
        database = _FakeDatabase(rows)
        resolver = TeamOfActiveUTSeasonResolver(database=database, redis_backend=_BrokenRedis(), env={})

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [99])
        self.assertEqual(database.connection_calls, 1)

    def test_env_overrides_window_and_limit_and_ttl(self) -> None:
        rows = [{"team_id": 7, "unique_tournament_id": 8, "season_id": 1}]
        database = _FakeDatabase(rows)
        redis_backend = _FakeRedis()
        resolver = TeamOfActiveUTSeasonResolver(
            database=database,
            redis_backend=redis_backend,
            env={
                "SCHEMA_INSPECTOR_RESOURCE_TEAM_SEASON_ACTIVE_WINDOW_DAYS": "5",
                "SCHEMA_INSPECTOR_RESOURCE_TEAM_SEASON_ACTIVE_LIMIT": "33",
                "SCHEMA_INSPECTOR_RESOURCE_TEAM_SEASON_ACTIVE_CACHE_TTL_SECONDS": "60",
            },
        )

        _resolve(resolver)

        sql_args = database._connection.fetch_calls[0][1]
        self.assertEqual(sql_args, (5 * 86_400, 33))
        self.assertEqual(redis_backend.set_calls[0][2], 60)


if __name__ == "__main__":
    unittest.main()
