from __future__ import annotations

import asyncio
import json
import unittest

from schema_inspector.services.resource_scope.player_of_active_squad import (
    CACHE_KEY,
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_LIMIT,
    DEFAULT_WINDOW_DAYS,
    PlayerOfActiveSquadResolver,
)


class _FakeConnection:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.fetch_calls: list[tuple[str, tuple]] = []

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        return self.rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
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
        return item[0] if item else None

    def set(self, key, value, ex=None):
        self.set_calls.append((key, value, ex))
        self.store[key] = (str(value), ex)
        return True


def _resolve(resolver: PlayerOfActiveSquadResolver):
    return tuple(asyncio.run(resolver.resolve()))


class PlayerOfActiveSquadResolverTests(unittest.TestCase):
    def test_kind_matches_endpoint_metadata_string(self) -> None:
        # PLAYER_STATISTICS_ENDPOINT.scope_kind must read "player-of-active-squad" verbatim.
        self.assertEqual(PlayerOfActiveSquadResolver.kind, "player-of-active-squad")

    def test_documented_defaults(self) -> None:
        self.assertEqual(DEFAULT_WINDOW_DAYS, 7)
        self.assertEqual(DEFAULT_LIMIT, 200_000)
        self.assertEqual(DEFAULT_CACHE_TTL_SECONDS, 1800)

    def test_resolves_from_database_when_cache_empty(self) -> None:
        database = _FakeDatabase(
            [{"player_id": 100}, {"player_id": 200}, {"player_id": 300}]
        )
        redis_backend = _FakeRedis()
        resolver = PlayerOfActiveSquadResolver(
            database=database, redis_backend=redis_backend, env={}
        )

        targets = _resolve(resolver)

        player_ids = [t.entity_id for t in targets]
        self.assertEqual(player_ids, [100, 200, 300])
        for target in targets:
            self.assertEqual(target.entity_type, "player")
            self.assertEqual(target.path_params, {"player_id": target.entity_id})

        # Result was cached.
        self.assertEqual(len(redis_backend.set_calls), 1)
        cache_key, value, ttl = redis_backend.set_calls[0]
        self.assertEqual(cache_key, CACHE_KEY)
        self.assertEqual(json.loads(value), [100, 200, 300])
        self.assertEqual(ttl, DEFAULT_CACHE_TTL_SECONDS)

    def test_uses_cache_on_second_call_without_db(self) -> None:
        database = _FakeDatabase([])
        redis_backend = _FakeRedis()
        redis_backend.store[CACHE_KEY] = (json.dumps([5, 6, 7]), DEFAULT_CACHE_TTL_SECONDS)
        resolver = PlayerOfActiveSquadResolver(
            database=database, redis_backend=redis_backend, env={}
        )

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [5, 6, 7])
        self.assertEqual(database.connection_calls, 0)

    def test_invalid_cache_falls_back_to_database(self) -> None:
        database = _FakeDatabase([{"player_id": 9}])
        redis_backend = _FakeRedis()
        redis_backend.store[CACHE_KEY] = ("not-json", None)
        resolver = PlayerOfActiveSquadResolver(
            database=database, redis_backend=redis_backend, env={}
        )

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [9])
        self.assertEqual(database.connection_calls, 1)

    def test_redis_failure_falls_back_to_database(self) -> None:
        class _BrokenRedis:
            def get(self, key):
                raise RuntimeError("boom")

            def set(self, key, value, ex=None):
                raise RuntimeError("boom-write")

        database = _FakeDatabase([{"player_id": 1}])
        resolver = PlayerOfActiveSquadResolver(
            database=database, redis_backend=_BrokenRedis(), env={}
        )

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [1])
        self.assertEqual(database.connection_calls, 1)

    def test_env_overrides_window_limit_ttl(self) -> None:
        database = _FakeDatabase([])
        redis_backend = _FakeRedis()
        resolver = PlayerOfActiveSquadResolver(
            database=database,
            redis_backend=redis_backend,
            env={
                "SCHEMA_INSPECTOR_RESOURCE_PLAYER_ACTIVE_WINDOW_DAYS": "3",
                "SCHEMA_INSPECTOR_RESOURCE_PLAYER_ACTIVE_LIMIT": "777",
                "SCHEMA_INSPECTOR_RESOURCE_PLAYER_ACTIVE_CACHE_TTL_SECONDS": "60",
            },
        )

        _resolve(resolver)

        sql_args = database._connection.fetch_calls[0][1]
        # cutoff = 3 * 86400
        self.assertEqual(sql_args, (259_200, 777))
        self.assertEqual(redis_backend.set_calls[0][2], 60)


if __name__ == "__main__":
    unittest.main()
