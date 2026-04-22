from __future__ import annotations

import unittest

from schema_inspector.db import AsyncpgDatabase, DatabaseConfig, register_post_commit_hook


class _FakeTransaction:
    def __init__(self) -> None:
        self.started = False
        self.committed = False
        self.rolled_back = False

    async def start(self) -> None:
        self.started = True

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True


class _FakeConnection:
    def __init__(self) -> None:
        self.transaction_handle = _FakeTransaction()

    def transaction(self) -> _FakeTransaction:
        return self.transaction_handle


class _FakePool:
    def __init__(self, connection: _FakeConnection) -> None:
        self.connection = connection
        self.release_calls = 0

    async def acquire(self) -> _FakeConnection:
        return self.connection

    async def release(self, connection: _FakeConnection) -> None:
        self.release_calls += 1


class AsyncpgDatabaseTransactionTests(unittest.IsolatedAsyncioTestCase):
    async def test_transaction_runs_post_commit_hooks_after_commit(self) -> None:
        connection = _FakeConnection()
        database = AsyncpgDatabase(DatabaseConfig(dsn="postgresql://example"))
        database._pool = _FakePool(connection)
        callbacks: list[str] = []

        async with database.transaction() as transactional_connection:
            self.assertIs(transactional_connection, connection)
            self.assertTrue(register_post_commit_hook(lambda: callbacks.append("ran")))
            self.assertEqual(callbacks, [])
            self.assertFalse(connection.transaction_handle.committed)

        self.assertEqual(callbacks, ["ran"])
        self.assertTrue(connection.transaction_handle.started)
        self.assertTrue(connection.transaction_handle.committed)
        self.assertFalse(connection.transaction_handle.rolled_back)

    async def test_transaction_does_not_run_post_commit_hooks_after_rollback(self) -> None:
        connection = _FakeConnection()
        database = AsyncpgDatabase(DatabaseConfig(dsn="postgresql://example"))
        database._pool = _FakePool(connection)
        callbacks: list[str] = []

        with self.assertRaisesRegex(RuntimeError, "boom"):
            async with database.transaction():
                self.assertTrue(register_post_commit_hook(lambda: callbacks.append("ran")))
                raise RuntimeError("boom")

        self.assertEqual(callbacks, [])
        self.assertTrue(connection.transaction_handle.started)
        self.assertFalse(connection.transaction_handle.committed)
        self.assertTrue(connection.transaction_handle.rolled_back)
