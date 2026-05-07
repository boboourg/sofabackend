from __future__ import annotations

import asyncio
import json
import unittest

from schema_inspector.services.resource_scope.player_of_national_team_history import (
    CACHE_KEY,
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_LIMIT,
    PlayerOfNationalTeamHistoryResolver,
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
        self.set_calls: list[tuple[str, str, int | None]] = []

    def get(self, key):
        item = self.store.get(key)
        return None if item is None else item[0]

    def set(self, key, value, ex=None):
        self.set_calls.append((key, value, ex))
        self.store[key] = (str(value), ex)
        return True


def _resolve(resolver: PlayerOfNationalTeamHistoryResolver):
    return tuple(asyncio.run(resolver.resolve()))


class PlayerOfNationalTeamHistoryResolverTests(unittest.TestCase):
    def test_kind_matches_endpoint_metadata(self) -> None:
        self.assertEqual(
            PlayerOfNationalTeamHistoryResolver.kind, "player-of-national-team-history"
        )

    def test_defaults_match_documented_values(self) -> None:
        self.assertEqual(DEFAULT_LIMIT, 200_000)
        self.assertEqual(DEFAULT_CACHE_TTL_SECONDS, 1800)

    def test_yields_one_target_per_player_with_player_path_param(self) -> None:
        rows = [{"player_id": 750}, {"player_id": 12994}, {"player_id": 159665}]
        database = _FakeDatabase(rows)
        redis_backend = _FakeRedis()
        resolver = PlayerOfNationalTeamHistoryResolver(
            database=database, redis_backend=redis_backend, env={}
        )

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [750, 12994, 159665])
        for target in targets:
            self.assertEqual(target.entity_type, "player")
            self.assertEqual(target.path_params, {"player_id": target.entity_id})
        cache_key, cached_value, ex = redis_backend.set_calls[0]
        self.assertEqual(cache_key, CACHE_KEY)
        self.assertEqual(json.loads(cached_value), [750, 12994, 159665])
        self.assertEqual(ex, DEFAULT_CACHE_TTL_SECONDS)

    def test_sql_uses_only_limit_param(self) -> None:
        rows = [{"player_id": 1}]
        database = _FakeDatabase(rows)
        resolver = PlayerOfNationalTeamHistoryResolver(
            database=database, redis_backend=None, env={}
        )

        _resolve(resolver)

        sql_args = database._connection.fetch_calls[0][1]
        self.assertEqual(sql_args, (DEFAULT_LIMIT,))

    def test_uses_cache_on_second_call_without_db(self) -> None:
        database = _FakeDatabase([])
        redis_backend = _FakeRedis()
        redis_backend.store[CACHE_KEY] = (json.dumps([7, 9]), DEFAULT_CACHE_TTL_SECONDS)
        resolver = PlayerOfNationalTeamHistoryResolver(
            database=database, redis_backend=redis_backend, env={}
        )

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [7, 9])
        self.assertEqual(database.connection_calls, 0)

    def test_redis_failure_falls_back_to_database(self) -> None:
        class _BrokenRedis:
            def get(self, key):
                raise RuntimeError("boom")

            def set(self, key, value, ex=None):
                raise RuntimeError("boom-write")

        rows = [{"player_id": 5}]
        database = _FakeDatabase(rows)
        resolver = PlayerOfNationalTeamHistoryResolver(
            database=database, redis_backend=_BrokenRedis(), env={}
        )

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [5])
        self.assertEqual(database.connection_calls, 1)

    def test_env_override_for_limit_and_ttl(self) -> None:
        rows = [{"player_id": 7}]
        database = _FakeDatabase(rows)
        redis_backend = _FakeRedis()
        resolver = PlayerOfNationalTeamHistoryResolver(
            database=database,
            redis_backend=redis_backend,
            env={
                "SCHEMA_INSPECTOR_RESOURCE_PLAYER_NATIONAL_LIMIT": "150",
                "SCHEMA_INSPECTOR_RESOURCE_PLAYER_NATIONAL_CACHE_TTL_SECONDS": "55",
            },
        )

        _resolve(resolver)

        sql_args = database._connection.fetch_calls[0][1]
        self.assertEqual(sql_args, (150,))
        self.assertEqual(redis_backend.set_calls[0][2], 55)


if __name__ == "__main__":
    unittest.main()
