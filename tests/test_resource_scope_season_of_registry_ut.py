from __future__ import annotations

import asyncio
import json
import unittest

from schema_inspector.services.resource_scope.season_of_registry_ut import (
    CACHE_KEY,
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_LIMIT,
    DEFAULT_WINDOW_DAYS_FUTURE,
    DEFAULT_WINDOW_DAYS_PAST,
    PILOT_ENV_KEY,
    SeasonOfRegistryUTResolver,
)


class _FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.fetch_calls = []

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        return self.rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeDatabase:
    def __init__(self, rows):
        self._rows = rows
        self.connection_calls = 0
        self._connection = None

    def connection(self):
        self.connection_calls += 1
        self._connection = _FakeConnection(self._rows)
        return self._connection


class _FakeRedis:
    def __init__(self):
        self.store = {}
        self.set_calls = []

    def get(self, key):
        item = self.store.get(key)
        return None if item is None else item[0]

    def set(self, key, value, ex=None):
        self.set_calls.append((key, value, ex))
        self.store[key] = (str(value), ex)
        return True


def _resolve(resolver):
    return tuple(asyncio.run(resolver.resolve()))


class SeasonOfRegistryUTResolverTests(unittest.TestCase):
    def test_kind_matches_endpoint_metadata(self) -> None:
        self.assertEqual(SeasonOfRegistryUTResolver.kind, "season-of-registry-ut")

    def test_defaults_match_documented_values(self) -> None:
        self.assertEqual(DEFAULT_WINDOW_DAYS_PAST, 60)
        self.assertEqual(DEFAULT_WINDOW_DAYS_FUTURE, 60)
        self.assertEqual(DEFAULT_LIMIT, 20_000)
        self.assertEqual(DEFAULT_CACHE_TTL_SECONDS, 1800)

    def test_resolves_pairs_with_path_params_and_context_ids(self) -> None:
        rows = [
            {"ut_id": 7, "season_id": 76953},
            {"ut_id": 19, "season_id": 82557},
            {"ut_id": 8, "season_id": 77559},
        ]
        database = _FakeDatabase(rows)
        redis_backend = _FakeRedis()
        resolver = SeasonOfRegistryUTResolver(
            database=database, redis_backend=redis_backend, env={}
        )

        targets = _resolve(resolver)

        self.assertEqual(len(targets), 3)
        first = targets[0]
        self.assertEqual(first.entity_type, "season")
        self.assertEqual(first.entity_id, 76953)
        self.assertEqual(
            first.path_params,
            {"unique_tournament_id": 7, "season_id": 76953},
        )
        self.assertEqual(first.context_unique_tournament_id, 7)
        self.assertEqual(first.context_season_id, 76953)
        self.assertEqual(first.sport_slug, "football")
        cache_key, cached_value, ex = redis_backend.set_calls[0]
        self.assertEqual(cache_key, CACHE_KEY)
        self.assertEqual(json.loads(cached_value), [[7, 76953], [19, 82557], [8, 77559]])
        self.assertEqual(ex, DEFAULT_CACHE_TTL_SECONDS)

    def test_sql_passes_sport_window_pilot_and_limit(self) -> None:
        rows = [{"ut_id": 7, "season_id": 76953}]
        database = _FakeDatabase(rows)
        resolver = SeasonOfRegistryUTResolver(
            database=database,
            redis_backend=None,
            env={
                PILOT_ENV_KEY: "7,19",
                "SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_WINDOW_DAYS_PAST": "30",
                "SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_WINDOW_DAYS_FUTURE": "30",
                "SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_LIMIT": "500",
            },
        )

        _resolve(resolver)

        sql_args = database._connection.fetch_calls[0][1]
        self.assertEqual(sql_args, ("football", 30 * 86_400, 30 * 86_400, [7, 19], 500))

    def test_pilot_unset_passes_empty_list(self) -> None:
        rows = [{"ut_id": 7, "season_id": 76953}]
        database = _FakeDatabase(rows)
        resolver = SeasonOfRegistryUTResolver(database=database, redis_backend=None, env={})

        _resolve(resolver)

        self.assertEqual(database._connection.fetch_calls[0][1][3], [])

    def test_uses_cache_on_second_call_without_db(self) -> None:
        database = _FakeDatabase([])
        redis_backend = _FakeRedis()
        redis_backend.store[CACHE_KEY] = (
            json.dumps([[7, 76953], [19, 82557]]),
            DEFAULT_CACHE_TTL_SECONDS,
        )
        resolver = SeasonOfRegistryUTResolver(
            database=database, redis_backend=redis_backend, env={}
        )

        targets = _resolve(resolver)

        self.assertEqual([(t.context_unique_tournament_id, t.entity_id) for t in targets], [(7, 76953), (19, 82557)])
        self.assertEqual(database.connection_calls, 0)

    def test_redis_failure_falls_back_to_database(self) -> None:
        class _BrokenRedis:
            def get(self, key):
                raise RuntimeError("boom")

            def set(self, key, value, ex=None):
                raise RuntimeError("boom-write")

        rows = [{"ut_id": 7, "season_id": 76953}]
        database = _FakeDatabase(rows)
        resolver = SeasonOfRegistryUTResolver(
            database=database, redis_backend=_BrokenRedis(), env={}
        )

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [76953])

    def test_invalid_env_falls_back_to_defaults(self) -> None:
        resolver = SeasonOfRegistryUTResolver(
            database=_FakeDatabase([]),
            redis_backend=None,
            env={
                "SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_WINDOW_DAYS_PAST": "junk",
                "SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_LIMIT": "-9",
            },
        )
        self.assertEqual(resolver.window_days_past, DEFAULT_WINDOW_DAYS_PAST)
        self.assertEqual(resolver.limit, DEFAULT_LIMIT)


if __name__ == "__main__":
    unittest.main()
