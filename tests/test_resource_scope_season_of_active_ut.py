from __future__ import annotations

import asyncio
import json
import unittest

from schema_inspector.services.resource_scope import (
    SeasonOfActiveUTBaseResolver,
    SeasonOfActiveUTEventsResolver,
    SeasonOfActiveUTStandingsResolver,
)
from schema_inspector.services.resource_scope.season_of_active_ut import (
    CACHE_KEY,
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_LIMIT,
    DEFAULT_WINDOW_DAYS,
    STANDINGS_SCOPES,
    _parse_pilot_pairs,
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
        return item[0] if item else None

    def set(self, key, value, ex=None):
        self.set_calls.append((key, value, ex))
        self.store[key] = (str(value), ex)
        return True


def _resolve(resolver):
    return tuple(asyncio.run(resolver.resolve()))


class ParsePilotPairsTests(unittest.TestCase):
    def test_empty(self) -> None:
        self.assertEqual(_parse_pilot_pairs(""), ())

    def test_typical(self) -> None:
        result = _parse_pilot_pairs("8:77559, 17:76986 ,7:76953")
        self.assertEqual(result, ((8, 77559), (17, 76986), (7, 76953)))

    def test_drops_malformed(self) -> None:
        result = _parse_pilot_pairs("8:77559, oops, 17:abc, 23, 7:76953, 0:1, -1:2")
        self.assertEqual(result, ((8, 77559), (7, 76953)))

    def test_dedupes(self) -> None:
        result = _parse_pilot_pairs("8:77559,8:77559,17:76986")
        self.assertEqual(result, ((8, 77559), (17, 76986)))


class SeasonOfActiveUTBaseResolverTests(unittest.TestCase):
    def test_defaults(self) -> None:
        self.assertEqual(DEFAULT_WINDOW_DAYS, 30)
        self.assertEqual(DEFAULT_LIMIT, 5_000)
        self.assertEqual(DEFAULT_CACHE_TTL_SECONDS, 1800)
        self.assertEqual(STANDINGS_SCOPES, ("total", "home", "away"))

    def test_pilot_env_short_circuits_db(self) -> None:
        database = _FakeDatabase([])
        redis_backend = _FakeRedis()
        resolver = SeasonOfActiveUTBaseResolver(
            database=database,
            redis_backend=redis_backend,
            env={"SCHEMA_INSPECTOR_RESOURCE_PILOT_SEASONS": "8:77559,17:76986"},
        )

        pairs = asyncio.run(resolver.pairs())

        self.assertEqual(pairs, ((8, 77559), (17, 76986)))
        # Pilot path must NOT touch the database or Redis.
        self.assertEqual(database.connection_calls, 0)
        self.assertEqual(redis_backend.set_calls, [])

    def test_sql_path_when_no_pilot_env(self) -> None:
        database = _FakeDatabase(
            [{"ut_id": 8, "season_id": 77559}, {"ut_id": 17, "season_id": 76986}]
        )
        redis_backend = _FakeRedis()
        resolver = SeasonOfActiveUTBaseResolver(
            database=database, redis_backend=redis_backend, env={}
        )

        pairs = asyncio.run(resolver.pairs())

        self.assertEqual(pairs, ((8, 77559), (17, 76986)))
        self.assertEqual(database.connection_calls, 1)
        # SQL got the cutoff (30d in seconds) and limit:
        sql_args = database._connection.fetch_calls[0][1]
        self.assertEqual(sql_args, (30 * 86_400, DEFAULT_LIMIT))
        # Cached:
        self.assertEqual(len(redis_backend.set_calls), 1)
        cache_value = json.loads(redis_backend.set_calls[0][1])
        self.assertEqual(cache_value, [[8, 77559], [17, 76986]])

    def test_cache_hit_skips_db(self) -> None:
        database = _FakeDatabase([])
        redis_backend = _FakeRedis()
        redis_backend.store[CACHE_KEY] = (json.dumps([[8, 77559]]), DEFAULT_CACHE_TTL_SECONDS)
        resolver = SeasonOfActiveUTBaseResolver(
            database=database, redis_backend=redis_backend, env={}
        )

        pairs = asyncio.run(resolver.pairs())

        self.assertEqual(pairs, ((8, 77559),))
        self.assertEqual(database.connection_calls, 0)


class SeasonOfActiveUTEventsResolverTests(unittest.TestCase):
    def test_kind_constant(self) -> None:
        self.assertEqual(SeasonOfActiveUTEventsResolver.kind, "season-of-active-ut-events")

    def test_each_pair_emits_one_target_with_page_zero(self) -> None:
        base = SeasonOfActiveUTBaseResolver(
            database=_FakeDatabase([]),
            redis_backend=None,
            env={"SCHEMA_INSPECTOR_RESOURCE_PILOT_SEASONS": "8:77559,17:76986"},
        )
        resolver = SeasonOfActiveUTEventsResolver(base=base)

        targets = _resolve(resolver)

        self.assertEqual(len(targets), 2)
        for target, (ut_id, season_id) in zip(targets, [(8, 77559), (17, 76986)]):
            self.assertEqual(target.entity_type, "season")
            self.assertEqual(target.entity_id, season_id)
            self.assertEqual(
                target.path_params,
                {"unique_tournament_id": ut_id, "season_id": season_id, "page": 0},
            )
            self.assertEqual(target.context_unique_tournament_id, ut_id)
            self.assertEqual(target.context_season_id, season_id)


class SeasonOfActiveUTStandingsResolverTests(unittest.TestCase):
    def test_kind_constant(self) -> None:
        self.assertEqual(
            SeasonOfActiveUTStandingsResolver.kind, "season-of-active-ut-standings"
        )

    def test_each_pair_fans_out_three_scopes(self) -> None:
        base = SeasonOfActiveUTBaseResolver(
            database=_FakeDatabase([]),
            redis_backend=None,
            env={"SCHEMA_INSPECTOR_RESOURCE_PILOT_SEASONS": "8:77559"},
        )
        resolver = SeasonOfActiveUTStandingsResolver(base=base)

        targets = _resolve(resolver)

        self.assertEqual(len(targets), 3)  # total + home + away
        scopes_seen = {t.path_params["scope"] for t in targets}
        self.assertEqual(scopes_seen, {"total", "home", "away"})
        for target in targets:
            self.assertEqual(
                target.path_params["unique_tournament_id"], 8
            )
            self.assertEqual(target.path_params["season_id"], 77559)

    def test_pairs_x_scopes_total(self) -> None:
        base = SeasonOfActiveUTBaseResolver(
            database=_FakeDatabase([]),
            redis_backend=None,
            env={"SCHEMA_INSPECTOR_RESOURCE_PILOT_SEASONS": "8:77559,17:76986,7:76953"},
        )
        resolver = SeasonOfActiveUTStandingsResolver(base=base)

        targets = _resolve(resolver)

        self.assertEqual(len(targets), 3 * 3)


if __name__ == "__main__":
    unittest.main()
