from __future__ import annotations

import asyncio
import json
import unittest

from schema_inspector.services.resource_scope.custom_id_of_managed_events import (
    CACHE_KEY as CUSTOM_ID_CACHE_KEY,
    CustomIdOfManagedEventsResolver,
)
from schema_inspector.services.resource_scope.managed_football_pairs import (
    ENV_KEY,
    parse_managed_pairs,
)
from schema_inspector.services.resource_scope.period_of_managed_pairs import (
    CACHE_KEY as PERIOD_CACHE_KEY,
    PeriodOfManagedPairsResolver,
)
from schema_inspector.services.resource_scope.round_of_managed_pairs import (
    CACHE_KEY as ROUND_CACHE_KEY,
    RoundOfManagedPairsResolver,
)


class _FakeConnection:
    def __init__(self, rows: list[dict]):
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
    def __init__(self, rows: list[dict]):
        self._rows = rows
        self._connection: _FakeConnection | None = None
        self.connection_calls = 0

    def connection(self):
        self.connection_calls += 1
        self._connection = _FakeConnection(self._rows)
        return self._connection


class _FakeRedis:
    def __init__(self):
        self.store: dict[str, tuple[str, int | None]] = {}
        self.set_calls: list[tuple[str, str, int | None]] = []

    def get(self, key):
        item = self.store.get(key)
        return None if item is None else item[0]

    def set(self, key, value, ex=None):
        self.set_calls.append((key, value, ex))
        self.store[key] = (str(value), ex)
        return True


def _resolve(resolver):
    return tuple(asyncio.run(resolver.resolve()))


class ManagedPairsParserTests(unittest.TestCase):
    def test_parses_well_formed(self) -> None:
        self.assertEqual(parse_managed_pairs("679:76984,17:76986"), ((679, 76984), (17, 76986)))

    def test_dedups_and_handles_whitespace(self) -> None:
        self.assertEqual(
            parse_managed_pairs(" 679:76984 , 679:76984 ,17:76986 "),
            ((679, 76984), (17, 76986)),
        )

    def test_drops_invalid_tokens(self) -> None:
        self.assertEqual(parse_managed_pairs("679:abc, foo, ,17:76986, -1:5"), ((17, 76986),))

    def test_empty_yields_empty(self) -> None:
        self.assertEqual(parse_managed_pairs(""), ())
        self.assertEqual(parse_managed_pairs(None), ())


class RoundOfManagedPairsTests(unittest.TestCase):
    def test_kind_is_distinct(self) -> None:
        self.assertEqual(
            RoundOfManagedPairsResolver.kind, "round-of-managed-football-pairs"
        )

    def test_empty_env_yields_empty_no_db_call(self) -> None:
        db = _FakeDatabase([])
        resolver = RoundOfManagedPairsResolver(database=db, redis_backend=None, env={})
        targets = _resolve(resolver)
        self.assertEqual(targets, ())
        self.assertEqual(db.connection_calls, 0)

    def test_yields_one_target_per_round(self) -> None:
        rows = [
            {"ut": 17, "season": 76986, "round_number": 1},
            {"ut": 17, "season": 76986, "round_number": 2},
            {"ut": 679, "season": 76984, "round_number": 5},
        ]
        db = _FakeDatabase(rows)
        redis_backend = _FakeRedis()
        resolver = RoundOfManagedPairsResolver(
            database=db, redis_backend=redis_backend,
            env={ENV_KEY: "17:76986,679:76984"},
        )
        targets = _resolve(resolver)
        self.assertEqual(len(targets), 3)
        self.assertEqual(
            targets[0].path_params,
            {"unique_tournament_id": 17, "season_id": 76986, "round_number": 1},
        )
        self.assertEqual(targets[2].context_unique_tournament_id, 679)
        self.assertEqual(targets[2].sport_slug, "football")

    def test_sql_passes_pair_arrays(self) -> None:
        db = _FakeDatabase([])
        resolver = RoundOfManagedPairsResolver(
            database=db, redis_backend=None,
            env={ENV_KEY: "17:76986,679:76984"},
        )
        _resolve(resolver)
        sql_args = db._connection.fetch_calls[0][1]
        self.assertEqual(sql_args, ([17, 679], [76986, 76984]))

    def test_uses_redis_cache_on_second_call(self) -> None:
        db = _FakeDatabase([])
        redis_backend = _FakeRedis()
        redis_backend.store[ROUND_CACHE_KEY] = (
            json.dumps([[17, 76986, 1], [17, 76986, 2]]),
            1800,
        )
        resolver = RoundOfManagedPairsResolver(
            database=db, redis_backend=redis_backend,
            env={ENV_KEY: "17:76986"},
        )
        targets = _resolve(resolver)
        self.assertEqual(len(targets), 2)
        self.assertEqual(db.connection_calls, 0)


class PeriodOfManagedPairsTests(unittest.TestCase):
    def test_kind_is_distinct(self) -> None:
        self.assertEqual(PeriodOfManagedPairsResolver.kind, "period-of-managed-football-pairs")

    def test_empty_env_yields_empty(self) -> None:
        db = _FakeDatabase([])
        resolver = PeriodOfManagedPairsResolver(database=db, redis_backend=None, env={})
        self.assertEqual(_resolve(resolver), ())

    def test_yields_period_targets(self) -> None:
        rows = [
            {"ut": 17, "season": 76986, "period_id": 100},
            {"ut": 17, "season": 76986, "period_id": 101},
        ]
        db = _FakeDatabase(rows)
        resolver = PeriodOfManagedPairsResolver(
            database=db, redis_backend=None, env={ENV_KEY: "17:76986"},
        )
        targets = _resolve(resolver)
        self.assertEqual(len(targets), 2)
        self.assertEqual(
            targets[0].path_params,
            {"unique_tournament_id": 17, "season_id": 76986, "period_id": 100},
        )
        self.assertEqual(targets[0].entity_type, "period")


class CustomIdOfManagedEventsTests(unittest.TestCase):
    def test_kind_is_distinct(self) -> None:
        self.assertEqual(
            CustomIdOfManagedEventsResolver.kind, "custom-id-of-managed-events"
        )

    def test_empty_env_yields_empty(self) -> None:
        db = _FakeDatabase([])
        resolver = CustomIdOfManagedEventsResolver(database=db, redis_backend=None, env={})
        self.assertEqual(_resolve(resolver), ())

    def test_yields_event_targets_with_custom_id(self) -> None:
        rows = [
            {"event_id": 14023930, "custom_id": "rY"},
            {"event_id": 15632622, "custom_id": "NabsZjb"},
        ]
        db = _FakeDatabase(rows)
        resolver = CustomIdOfManagedEventsResolver(
            database=db, redis_backend=None, env={ENV_KEY: "17:76986,679:76984"},
        )
        targets = _resolve(resolver)
        self.assertEqual(len(targets), 2)
        self.assertEqual(targets[0].entity_id, 14023930)
        self.assertEqual(targets[0].path_params, {"custom_id": "rY"})
        self.assertEqual(targets[1].path_params, {"custom_id": "NabsZjb"})

    def test_sql_passes_pair_arrays_and_limit(self) -> None:
        db = _FakeDatabase([])
        resolver = CustomIdOfManagedEventsResolver(
            database=db, redis_backend=None,
            env={ENV_KEY: "17:76986,679:76984",
                 "SCHEMA_INSPECTOR_RESOURCE_CUSTOM_ID_LIMIT": "1000"},
        )
        _resolve(resolver)
        sql_args = db._connection.fetch_calls[0][1]
        self.assertEqual(sql_args, ([17, 679], [76986, 76984], 1000))

    def test_cache_handles_string_custom_id(self) -> None:
        db = _FakeDatabase([])
        redis_backend = _FakeRedis()
        redis_backend.store[CUSTOM_ID_CACHE_KEY] = (
            json.dumps([[14023930, "rY"], [15632622, "NabsZjb"]]),
            1800,
        )
        resolver = CustomIdOfManagedEventsResolver(
            database=db, redis_backend=redis_backend,
            env={ENV_KEY: "17:76986"},
        )
        targets = _resolve(resolver)
        self.assertEqual(len(targets), 2)
        self.assertEqual(targets[0].path_params, {"custom_id": "rY"})
        self.assertEqual(db.connection_calls, 0)


if __name__ == "__main__":
    unittest.main()
