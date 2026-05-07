from __future__ import annotations

import asyncio
import json
import unittest

from schema_inspector.services.resource_scope.custom_id_of_registry_events import (
    CACHE_KEY as CUSTOM_ID_CACHE_KEY,
    CustomIdOfRegistryEventsResolver,
)
from schema_inspector.services.resource_scope.period_of_registry_football import (
    CACHE_KEY as PERIOD_CACHE_KEY,
    PeriodOfRegistryFootballResolver,
)
from schema_inspector.services.resource_scope.round_of_registry_football import (
    CACHE_KEY as ROUND_CACHE_KEY,
    RoundOfRegistryFootballResolver,
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


class RoundOfRegistryFootballTests(unittest.TestCase):
    def test_kind_is_distinct(self) -> None:
        self.assertEqual(
            RoundOfRegistryFootballResolver.kind, "round-of-registry-football"
        )

    def test_yields_one_target_per_round_no_env_dependency(self) -> None:
        rows = [
            {"ut": 17, "season": 76986, "round_number": 1},
            {"ut": 17, "season": 76986, "round_number": 2},
            {"ut": 679, "season": 76984, "round_number": 5},
        ]
        db = _FakeDatabase(rows)
        # Empty env: registry-driven resolver works without env vars.
        resolver = RoundOfRegistryFootballResolver(
            database=db,
            redis_backend=None,
            env={},
        )
        targets = _resolve(resolver)
        self.assertEqual(len(targets), 3)
        self.assertEqual(
            targets[0].path_params,
            {"unique_tournament_id": 17, "season_id": 76986, "round_number": 1},
        )
        self.assertEqual(targets[2].context_unique_tournament_id, 679)
        self.assertEqual(targets[2].sport_slug, "football")

    def test_pilot_uts_passed_to_sql_and_default_window(self) -> None:
        db = _FakeDatabase([])
        resolver = RoundOfRegistryFootballResolver(
            database=db,
            redis_backend=None,
            env={"SCHEMA_INSPECTOR_RESOURCE_ROUND_REGISTRY_PILOT_UTS": "17,679"},
        )
        _resolve(resolver)
        sql, sql_args = db._connection.fetch_calls[0]
        # Sport filter param 1, window past param 2, window future param 3,
        # pilot list param 4, rounds-pattern param 5, limit param 6.
        self.assertEqual(sql_args[0], "football")
        self.assertEqual(sql_args[1], 60 * 86_400)
        self.assertEqual(sql_args[2], 60 * 86_400)
        self.assertEqual(sql_args[3], [17, 679])
        self.assertIn("rounds", sql_args[4])
        self.assertEqual(sql_args[5], 100_000)

    def test_uses_redis_cache_on_second_call(self) -> None:
        db = _FakeDatabase([])
        redis_backend = _FakeRedis()
        redis_backend.store[ROUND_CACHE_KEY] = (
            json.dumps([[17, 76986, 1], [17, 76986, 2]]),
            1800,
        )
        resolver = RoundOfRegistryFootballResolver(
            database=db,
            redis_backend=redis_backend,
            env={},
        )
        targets = _resolve(resolver)
        self.assertEqual(len(targets), 2)
        self.assertEqual(db.connection_calls, 0)


class PeriodOfRegistryFootballTests(unittest.TestCase):
    def test_kind_is_distinct(self) -> None:
        self.assertEqual(
            PeriodOfRegistryFootballResolver.kind, "period-of-registry-football"
        )

    def test_yields_period_targets_no_env(self) -> None:
        rows = [
            {"ut": 17, "season": 76986, "period_id": 100},
            {"ut": 17, "season": 76986, "period_id": 101},
        ]
        db = _FakeDatabase(rows)
        resolver = PeriodOfRegistryFootballResolver(
            database=db, redis_backend=None, env={}
        )
        targets = _resolve(resolver)
        self.assertEqual(len(targets), 2)
        self.assertEqual(
            targets[0].path_params,
            {"unique_tournament_id": 17, "season_id": 76986, "period_id": 100},
        )
        self.assertEqual(targets[0].entity_type, "period")

    def test_skips_blacklisted_seasons_via_totw_store(self) -> None:
        rows = [
            {"ut": 17, "season": 76986, "period_id": 100},
            {"ut": 999, "season": 88888, "period_id": 200},
            {"ut": 999, "season": 88888, "period_id": 201},
        ]
        db = _FakeDatabase(rows)

        class _BlackList:
            def __init__(self) -> None:
                self.calls: list[tuple[int, int]] = []

            def is_blacklisted(self, ut: int, season: int) -> bool:
                self.calls.append((ut, season))
                return ut == 999 and season == 88888

        blacklist = _BlackList()
        resolver = PeriodOfRegistryFootballResolver(
            database=db,
            redis_backend=None,
            env={},
            totw_404_store=blacklist,
        )
        targets = _resolve(resolver)
        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].path_params["unique_tournament_id"], 17)
        self.assertEqual(targets[0].path_params["period_id"], 100)
        # blacklist should be consulted for every triple, but redundantly
        # consulting the same (ut, season) twice is OK — store is cheap.
        self.assertIn((999, 88888), blacklist.calls)

    def test_blacklist_check_failure_fails_open(self) -> None:
        rows = [{"ut": 17, "season": 76986, "period_id": 100}]
        db = _FakeDatabase(rows)

        class _BrokenStore:
            def is_blacklisted(self, ut, season):
                raise RuntimeError("redis offline")

        resolver = PeriodOfRegistryFootballResolver(
            database=db,
            redis_backend=None,
            env={},
            totw_404_store=_BrokenStore(),
        )
        targets = _resolve(resolver)
        self.assertEqual(len(targets), 1)


class CustomIdOfRegistryEventsTests(unittest.TestCase):
    def test_kind_is_distinct(self) -> None:
        self.assertEqual(
            CustomIdOfRegistryEventsResolver.kind, "custom-id-of-registry-events"
        )

    def test_yields_event_targets_with_string_custom_id(self) -> None:
        rows = [
            {"event_id": 14023930, "custom_id": "rY"},
            {"event_id": 15632622, "custom_id": "NabsZjb"},
        ]
        db = _FakeDatabase(rows)
        resolver = CustomIdOfRegistryEventsResolver(
            database=db, redis_backend=None, env={}
        )
        targets = _resolve(resolver)
        self.assertEqual(len(targets), 2)
        self.assertEqual(targets[0].entity_id, 14023930)
        self.assertEqual(targets[0].path_params, {"custom_id": "rY"})
        self.assertEqual(targets[1].path_params, {"custom_id": "NabsZjb"})

    def test_pilot_uts_and_default_limit(self) -> None:
        db = _FakeDatabase([])
        resolver = CustomIdOfRegistryEventsResolver(
            database=db,
            redis_backend=None,
            env={
                "SCHEMA_INSPECTOR_RESOURCE_CUSTOM_ID_REGISTRY_PILOT_UTS": "17,679",
                "SCHEMA_INSPECTOR_RESOURCE_CUSTOM_ID_REGISTRY_LIMIT": "1000",
            },
        )
        _resolve(resolver)
        sql_args = db._connection.fetch_calls[0][1]
        # sport, past_seconds, future_seconds, pilot_uts, limit
        self.assertEqual(sql_args[0], "football")
        self.assertEqual(sql_args[3], [17, 679])
        self.assertEqual(sql_args[4], 1000)

    def test_cache_handles_string_custom_id(self) -> None:
        db = _FakeDatabase([])
        redis_backend = _FakeRedis()
        redis_backend.store[CUSTOM_ID_CACHE_KEY] = (
            json.dumps([[14023930, "rY"], [15632622, "NabsZjb"]]),
            1800,
        )
        resolver = CustomIdOfRegistryEventsResolver(
            database=db,
            redis_backend=redis_backend,
            env={},
        )
        targets = _resolve(resolver)
        self.assertEqual(len(targets), 2)
        self.assertEqual(targets[0].path_params, {"custom_id": "rY"})
        self.assertEqual(db.connection_calls, 0)


if __name__ == "__main__":
    unittest.main()
