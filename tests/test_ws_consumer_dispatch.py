"""Unit tests for the consumer's dispatch logic.

The IO glue (curl_cffi ws_connect, reconnect loop, asyncpg pool
acquire) is not tested here — only the pure ``dispatch_message`` that
takes a parsed NATS message and routes it to the normalizer + writer.
"""
from __future__ import annotations
import json
import unittest
from typing import Any


class FakePool:
    """asyncpg-pool-shaped fake."""

    def __init__(self) -> None:
        self.transactions: list[list[tuple[str, tuple[Any, ...]]]] = []

    def acquire(self) -> "FakeAcquireCtx":
        return FakeAcquireCtx(self)


class FakeAcquireCtx:
    def __init__(self, pool: FakePool) -> None:
        self._pool = pool
        self._conn = FakeConn(pool)

    async def __aenter__(self) -> "FakeConn":
        return self._conn

    async def __aexit__(self, *_) -> None:
        pass


class FakeConn:
    def __init__(self, pool: FakePool) -> None:
        self._pool = pool
        self._txn_queries: list[tuple[str, tuple[Any, ...]]] = []
        self._in_txn = False

    def transaction(self) -> "FakeTxn":
        return FakeTxn(self)

    async def execute(self, query: str, *args: Any) -> str:
        self._txn_queries.append((query, args))
        return "OK"

    async def fetchval(self, query: str, *args: Any) -> Any:
        self._txn_queries.append((query, args))
        return None


class FakeTxn:
    def __init__(self, conn: FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> None:
        self._conn._in_txn = True

    async def __aexit__(self, *_) -> None:
        self._conn._in_txn = False
        # snapshot what was issued in this transaction
        self._conn._pool.transactions.append(list(self._conn._txn_queries))
        self._conn._txn_queries.clear()


class DispatchMessageTests(unittest.IsolatedAsyncioTestCase):
    async def test_msg_event_payload_routes_through_normalizer_and_writer(self) -> None:
        from schema_inspector.services.ws_consumer_service import dispatch_message

        pool = FakePool()
        msg = ("MSG", ("sport.football", 1, json.dumps({
            "id": 15171570,
            "homeScore.current": 2,
            "homeScore.display": 2,
            "changes.changeTimestamp": 1779063160,
        })))
        await dispatch_message(pool, msg)

        # One transaction was opened; multiple statements applied.
        self.assertEqual(len(pool.transactions), 1)
        queries = pool.transactions[0]
        self.assertTrue(any("INSERT INTO event_score" in q for q, _ in queries))
        # Cache invalidation always fires
        self.assertTrue(any("DELETE FROM event_payload_cache" in q for q, _ in queries))

    async def test_msg_odds_payload_is_noop(self) -> None:
        from schema_inspector.services.ws_consumer_service import dispatch_message

        pool = FakePool()
        msg = ("MSG", ("odds.football.1", 101, json.dumps({
            "id": 306234958,
            "choice1.fractionalValue": "5/6",
        })))
        await dispatch_message(pool, msg)
        # No DB transaction
        self.assertEqual(pool.transactions, [])

    async def test_invalid_json_payload_silently_dropped(self) -> None:
        from schema_inspector.services.ws_consumer_service import dispatch_message

        pool = FakePool()
        msg = ("MSG", ("sport.football", 1, "{ broken"))
        await dispatch_message(pool, msg)
        self.assertEqual(pool.transactions, [])

    async def test_non_msg_kind_is_noop(self) -> None:
        from schema_inspector.services.ws_consumer_service import dispatch_message

        pool = FakePool()
        await dispatch_message(pool, ("PING", None))
        await dispatch_message(pool, ("PONG", None))
        await dispatch_message(pool, ("OK", None))
        await dispatch_message(pool, ("ERR", "some error"))
        self.assertEqual(pool.transactions, [])

    async def test_unknown_sport_subject_is_noop(self) -> None:
        from schema_inspector.services.ws_consumer_service import dispatch_message

        pool = FakePool()
        msg = ("MSG", ("custom.foo", 999, json.dumps({"id": 1})))
        await dispatch_message(pool, msg)
        self.assertEqual(pool.transactions, [])


if __name__ == "__main__":
    unittest.main()
