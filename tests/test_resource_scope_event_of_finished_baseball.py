from __future__ import annotations

import asyncio
import json
import unittest

from schema_inspector.services.resource_scope.event_of_finished_baseball import (
    BASEBALL_ACTIVE_STATUS_CODES,
    BASEBALL_FINISHED_STATUS_CODE,
    CACHE_KEY,
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_LIMIT,
    DEFAULT_WINDOW_DAYS,
    EventOfFinishedBaseballResolver,
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


def _resolve(resolver: EventOfFinishedBaseballResolver):
    return tuple(asyncio.run(resolver.resolve()))


class EventOfFinishedBaseballResolverTests(unittest.TestCase):
    def test_kind_matches_endpoint_metadata(self) -> None:
        # D7: covers both finished and live baseball events, hence the rename.
        self.assertEqual(EventOfFinishedBaseballResolver.kind, "event-of-active-baseball")

    def test_active_status_codes_cover_finished_and_inprogress(self) -> None:
        # 100/110 = finished/AET; 23/24/29 = innings; 30 = inter-inning pause.
        self.assertEqual(BASEBALL_ACTIVE_STATUS_CODES, (100, 110, 23, 24, 29, 30))
        # The deprecated alias keeps its historical value so any external
        # importer continues to compile.
        self.assertEqual(BASEBALL_FINISHED_STATUS_CODE, 100)

    def test_defaults_match_documented_values(self) -> None:
        self.assertEqual(DEFAULT_WINDOW_DAYS, 30)
        self.assertEqual(DEFAULT_LIMIT, 20_000)
        self.assertEqual(DEFAULT_CACHE_TTL_SECONDS, 1800)

    def test_yields_one_target_per_event_with_baseball_sport_slug(self) -> None:
        rows = [{"event_id": 15501356}, {"event_id": 16070184}]
        database = _FakeDatabase(rows)
        redis_backend = _FakeRedis()
        resolver = EventOfFinishedBaseballResolver(
            database=database, redis_backend=redis_backend, env={}
        )

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [15501356, 16070184])
        for target in targets:
            self.assertEqual(target.entity_type, "event")
            self.assertEqual(target.path_params, {"event_id": target.entity_id})
            self.assertEqual(target.context_event_id, target.entity_id)
            self.assertEqual(target.sport_slug, "baseball")
        # Cached as plain int list:
        cache_key, cached_value, ex = redis_backend.set_calls[0]
        self.assertEqual(cache_key, CACHE_KEY)
        self.assertEqual(json.loads(cached_value), [15501356, 16070184])
        self.assertEqual(ex, DEFAULT_CACHE_TTL_SECONDS)

    def test_sql_params_include_active_status_window_and_limit(self) -> None:
        rows = [{"event_id": 1}]
        database = _FakeDatabase(rows)
        resolver = EventOfFinishedBaseballResolver(database=database, redis_backend=None, env={})

        _resolve(resolver)

        sql_args = database._connection.fetch_calls[0][1]
        # SQL now passes ``status_code = ANY($1::int[])`` with the full
        # active-statuses tuple in $1.
        self.assertEqual(
            sql_args,
            (list(BASEBALL_ACTIVE_STATUS_CODES), DEFAULT_WINDOW_DAYS * 86_400, DEFAULT_LIMIT),
        )

    def test_uses_cache_on_second_call_without_db(self) -> None:
        database = _FakeDatabase([])
        redis_backend = _FakeRedis()
        redis_backend.store[CACHE_KEY] = (json.dumps([42, 43]), DEFAULT_CACHE_TTL_SECONDS)
        resolver = EventOfFinishedBaseballResolver(
            database=database, redis_backend=redis_backend, env={}
        )

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [42, 43])
        self.assertEqual(database.connection_calls, 0)

    def test_redis_failure_falls_back_to_database(self) -> None:
        class _BrokenRedis:
            def get(self, key):
                raise RuntimeError("boom")

            def set(self, key, value, ex=None):
                raise RuntimeError("boom-write")

        rows = [{"event_id": 9}]
        database = _FakeDatabase(rows)
        resolver = EventOfFinishedBaseballResolver(
            database=database, redis_backend=_BrokenRedis(), env={}
        )

        targets = _resolve(resolver)

        self.assertEqual([t.entity_id for t in targets], [9])
        self.assertEqual(database.connection_calls, 1)


if __name__ == "__main__":
    unittest.main()
