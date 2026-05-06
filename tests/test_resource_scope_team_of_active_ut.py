from __future__ import annotations

import asyncio
import json
import unittest

from schema_inspector.services.resource_scope.team_of_active_ut import (
    CACHE_KEY,
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_LIMIT,
    DEFAULT_WINDOW_DAYS,
    TeamOfActiveUTResolver,
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
        self.get_calls: list[str] = []
        self.set_calls: list[tuple[str, str, int | None]] = []

    def get(self, key):
        self.get_calls.append(key)
        item = self.store.get(key)
        if item is None:
            return None
        return item[0]

    def set(self, key, value, ex=None):
        self.set_calls.append((key, value, ex))
        self.store[key] = (str(value), ex)
        return True


def _resolve(resolver: TeamOfActiveUTResolver):
    return tuple(asyncio.run(resolver.resolve()))


class TeamOfActiveUTResolverTests(unittest.TestCase):
    def test_kind_matches_endpoint_metadata(self) -> None:
        # The endpoint contract relies on this exact string.
        self.assertEqual(TeamOfActiveUTResolver.kind, "team-of-active-ut")

    def test_defaults_match_documented_values(self) -> None:
        self.assertEqual(DEFAULT_WINDOW_DAYS, 30)
        self.assertEqual(DEFAULT_LIMIT, 10_000)
        self.assertEqual(DEFAULT_CACHE_TTL_SECONDS, 1800)

    def test_resolves_from_database_when_cache_empty(self) -> None:
        database = _FakeDatabase([{"team_id": 42}, {"team_id": 17}, {"team_id": 44}])
        redis_backend = _FakeRedis()
        resolver = TeamOfActiveUTResolver(database=database, redis_backend=redis_backend, env={})

        targets = _resolve(resolver)

        team_ids = [t.entity_id for t in targets]
        self.assertEqual(team_ids, [42, 17, 44])
        for target in targets:
            self.assertEqual(target.entity_type, "team")
            self.assertEqual(target.path_params, {"team_id": target.entity_id})
        self.assertEqual(database.connection_calls, 1)
        # Result was cached:
        self.assertEqual(len(redis_backend.set_calls), 1)
        cache_key, cached_value, ex = redis_backend.set_calls[0]
        self.assertEqual(cache_key, CACHE_KEY)
        self.assertEqual(json.loads(cached_value), [42, 17, 44])
        self.assertEqual(ex, DEFAULT_CACHE_TTL_SECONDS)

    def test_uses_cache_on_second_call_without_db(self) -> None:
        database = _FakeDatabase([])
        redis_backend = _FakeRedis()
        redis_backend.store[CACHE_KEY] = (json.dumps([7, 9, 11]), DEFAULT_CACHE_TTL_SECONDS)
        resolver = TeamOfActiveUTResolver(database=database, redis_backend=redis_backend, env={})

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [7, 9, 11])
        self.assertEqual(database.connection_calls, 0)

    def test_invalid_cache_falls_back_to_database(self) -> None:
        database = _FakeDatabase([{"team_id": 1}, {"team_id": 2}])
        redis_backend = _FakeRedis()
        redis_backend.store[CACHE_KEY] = ("not-json", None)
        resolver = TeamOfActiveUTResolver(database=database, redis_backend=redis_backend, env={})

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [1, 2])
        self.assertEqual(database.connection_calls, 1)

    def test_redis_failure_falls_back_to_database(self) -> None:
        class _BrokenRedis:
            def get(self, key):
                raise RuntimeError("boom")

            def set(self, key, value, ex=None):
                raise RuntimeError("boom-write")

        database = _FakeDatabase([{"team_id": 99}])
        resolver = TeamOfActiveUTResolver(database=database, redis_backend=_BrokenRedis(), env={})

        # Should not raise even though Redis is broken on both read and write.
        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [99])
        self.assertEqual(database.connection_calls, 1)

    def test_env_overrides_window_and_limit_and_ttl(self) -> None:
        database = _FakeDatabase([{"team_id": 7}])
        redis_backend = _FakeRedis()
        resolver = TeamOfActiveUTResolver(
            database=database,
            redis_backend=redis_backend,
            env={
                "SCHEMA_INSPECTOR_RESOURCE_TEAM_ACTIVE_WINDOW_DAYS": "7",
                "SCHEMA_INSPECTOR_RESOURCE_TEAM_ACTIVE_LIMIT": "500",
                "SCHEMA_INSPECTOR_RESOURCE_TEAM_ACTIVE_CACHE_TTL_SECONDS": "120",
            },
        )

        _resolve(resolver)

        # SQL params (cutoff_seconds, limit). cutoff = 7*86400 = 604800.
        sql_args = database._connection.fetch_calls[0][1]
        self.assertEqual(sql_args, (604_800, 500))
        # Cache TTL respected:
        self.assertEqual(redis_backend.set_calls[0][2], 120)

    def test_invalid_env_falls_back_to_default(self) -> None:
        database = _FakeDatabase([])
        resolver = TeamOfActiveUTResolver(
            database=database,
            redis_backend=None,
            env={
                "SCHEMA_INSPECTOR_RESOURCE_TEAM_ACTIVE_WINDOW_DAYS": "not-a-number",
                "SCHEMA_INSPECTOR_RESOURCE_TEAM_ACTIVE_LIMIT": "-99",
            },
        )
        self.assertEqual(resolver.window_days, DEFAULT_WINDOW_DAYS)
        self.assertEqual(resolver.limit, DEFAULT_LIMIT)


if __name__ == "__main__":
    unittest.main()
