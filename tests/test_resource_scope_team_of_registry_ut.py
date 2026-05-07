from __future__ import annotations

import asyncio
import json
import unittest

from schema_inspector.services.resource_scope.team_of_registry_ut import (
    CACHE_KEY,
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_LIMIT,
    DEFAULT_WINDOW_DAYS_FUTURE,
    DEFAULT_WINDOW_DAYS_PAST,
    PILOT_ENV_KEY,
    TeamOfRegistryUTResolver,
    _parse_pilot_ut_list,
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


def _resolve(resolver: TeamOfRegistryUTResolver):
    return tuple(asyncio.run(resolver.resolve()))


class TeamOfRegistryUTResolverTests(unittest.TestCase):
    def test_kind_is_distinct_from_standings_resolver(self) -> None:
        # The activation switch flips an endpoint's scope_kind to this exact
        # string; if it diverges from the resolver registration the planner
        # silently skips the endpoint.
        self.assertEqual(TeamOfRegistryUTResolver.kind, "team-of-registry-ut")

    def test_defaults_match_documented_values(self) -> None:
        self.assertEqual(DEFAULT_WINDOW_DAYS_PAST, 60)
        self.assertEqual(DEFAULT_WINDOW_DAYS_FUTURE, 60)
        self.assertEqual(DEFAULT_LIMIT, 100_000)
        self.assertEqual(DEFAULT_CACHE_TTL_SECONDS, 1800)

    def test_resolves_team_ids_with_path_params_and_sport_slug(self) -> None:
        rows = [{"team_id": 23400}, {"team_id": 23036}, {"team_id": 35}]
        database = _FakeDatabase(rows)
        redis_backend = _FakeRedis()
        resolver = TeamOfRegistryUTResolver(database=database, redis_backend=redis_backend, env={})

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [23400, 23036, 35])
        for target in targets:
            self.assertEqual(target.entity_type, "team")
            self.assertEqual(target.path_params, {"team_id": target.entity_id})
            self.assertEqual(target.sport_slug, "football")
        # Cached as a flat int list — same shape the standings-driven resolver uses.
        cache_key, cached_value, ex = redis_backend.set_calls[0]
        self.assertEqual(cache_key, CACHE_KEY)
        self.assertEqual(json.loads(cached_value), [23400, 23036, 35])
        self.assertEqual(ex, DEFAULT_CACHE_TTL_SECONDS)

    def test_sql_passes_sport_window_pilot_and_limit_params(self) -> None:
        rows = [{"team_id": 23400}]
        database = _FakeDatabase(rows)
        resolver = TeamOfRegistryUTResolver(
            database=database,
            redis_backend=None,
            env={
                PILOT_ENV_KEY: "955,242,152",
                "SCHEMA_INSPECTOR_RESOURCE_REGISTRY_WINDOW_DAYS_PAST": "30",
                "SCHEMA_INSPECTOR_RESOURCE_REGISTRY_WINDOW_DAYS_FUTURE": "45",
                "SCHEMA_INSPECTOR_RESOURCE_REGISTRY_LIMIT": "1000",
            },
        )

        _resolve(resolver)

        sql_args = database._connection.fetch_calls[0][1]
        self.assertEqual(
            sql_args,
            ("football", 30 * 86_400, 45 * 86_400, [955, 242, 152], 1000),
        )

    def test_pilot_unset_yields_empty_pilot_list_so_query_has_no_filter(self) -> None:
        # When the pilot env is empty the resolver must still call the database
        # — the SQL will then evaluate cardinality([]) = 0 and skip the
        # ``unique_tournament_id = ANY(...)`` filter, scoping to the full
        # registry. We assert the empty list is what gets passed to the query.
        rows = [{"team_id": 1}]
        database = _FakeDatabase(rows)
        resolver = TeamOfRegistryUTResolver(database=database, redis_backend=None, env={})

        _resolve(resolver)

        sql_args = database._connection.fetch_calls[0][1]
        self.assertEqual(sql_args[3], [])  # 4th arg is the pilot UT list

    def test_uses_cache_on_second_call_without_db(self) -> None:
        database = _FakeDatabase([])
        redis_backend = _FakeRedis()
        redis_backend.store[CACHE_KEY] = (json.dumps([1, 2, 3]), DEFAULT_CACHE_TTL_SECONDS)
        resolver = TeamOfRegistryUTResolver(database=database, redis_backend=redis_backend, env={})

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [1, 2, 3])
        self.assertEqual(database.connection_calls, 0)

    def test_garbage_cache_falls_back_to_database(self) -> None:
        rows = [{"team_id": 9}]
        database = _FakeDatabase(rows)
        redis_backend = _FakeRedis()
        redis_backend.store[CACHE_KEY] = ("not-json", None)
        resolver = TeamOfRegistryUTResolver(database=database, redis_backend=redis_backend, env={})

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [9])
        self.assertEqual(database.connection_calls, 1)

    def test_redis_failure_falls_back_to_database(self) -> None:
        class _BrokenRedis:
            def get(self, key):
                raise RuntimeError("boom")

            def set(self, key, value, ex=None):
                raise RuntimeError("boom-write")

        rows = [{"team_id": 7}]
        database = _FakeDatabase(rows)
        resolver = TeamOfRegistryUTResolver(database=database, redis_backend=_BrokenRedis(), env={})

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [7])
        self.assertEqual(database.connection_calls, 1)

    def test_pilot_list_parser_handles_whitespace_dups_and_garbage(self) -> None:
        # whitespace, duplicates, mixed valid/invalid tokens
        result = _parse_pilot_ut_list(" 955, 242 ,242, abc, -1, 152 ")
        self.assertEqual(result, (955, 242, 152))

    def test_pilot_env_via_resolver_property(self) -> None:
        resolver = TeamOfRegistryUTResolver(
            database=_FakeDatabase([]),
            redis_backend=None,
            env={PILOT_ENV_KEY: "955,242"},
        )
        self.assertEqual(resolver.pilot_unique_tournament_ids(), (955, 242))

    def test_invalid_env_falls_back_to_defaults(self) -> None:
        resolver = TeamOfRegistryUTResolver(
            database=_FakeDatabase([]),
            redis_backend=None,
            env={
                "SCHEMA_INSPECTOR_RESOURCE_REGISTRY_WINDOW_DAYS_PAST": "not-a-number",
                "SCHEMA_INSPECTOR_RESOURCE_REGISTRY_LIMIT": "-99",
            },
        )
        self.assertEqual(resolver.window_days_past, DEFAULT_WINDOW_DAYS_PAST)
        self.assertEqual(resolver.limit, DEFAULT_LIMIT)


if __name__ == "__main__":
    unittest.main()
