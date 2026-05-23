from __future__ import annotations

from unittest.mock import patch
import unittest
import importlib.util

HAS_ASYNCPG = importlib.util.find_spec("asyncpg") is not None

from schema_inspector.db import (
    AsyncpgDatabase,
    DatabaseConfig,
    connect_with_fallback,
    create_pool_with_fallback,
    load_database_config,
    register_post_commit_hook,
)


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
        self.acquire_calls: list[dict[str, object]] = []

    async def acquire(self, **kwargs) -> _FakeConnection:
        # Stage 1.4 (2026-05-20): record acquire-time kwargs so tests
        # can assert ``timeout`` is propagated from AsyncpgDatabase.
        self.acquire_calls.append(kwargs)
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


class DatabaseConfigTests(unittest.TestCase):
    def test_load_database_config_derives_application_name_from_argv(self) -> None:
        with patch(
            "schema_inspector.db.sys.argv",
            ["python", "-m", "schema_inspector.cli", "worker-live-hot", "--consumer-name", "live-hot-1"],
        ):
            config = load_database_config(env={"SOFASCORE_DATABASE_URL": "postgresql://localhost/example"})

        self.assertEqual(config.application_name, "schema_inspector.cli-worker-live-hot")

    def test_connect_kwargs_prefer_unix_socket_for_local_linux_dsn(self) -> None:
        config = DatabaseConfig(
            dsn="postgresql://localhost:5432/sofascore_schema_inspector",
            application_name="worker-live-hot",
            unix_socket_dir="/var/run/postgresql",
        )

        kwargs = config.connect_kwargs(platform="linux", socket_dir="/var/run/postgresql")

        self.assertEqual(kwargs["dsn"], config.dsn)
        self.assertEqual(kwargs["host"], "/var/run/postgresql")
        self.assertEqual(kwargs["port"], 5432)
        self.assertEqual(kwargs["server_settings"], {"application_name": "worker-live-hot"})

    def test_connect_kwargs_keep_tcp_on_windows(self) -> None:
        config = DatabaseConfig(
            dsn="postgresql://localhost:5432/sofascore_schema_inspector",
            application_name="local_api",
            unix_socket_dir="/var/run/postgresql",
        )

        kwargs = config.connect_kwargs(platform="win32", socket_dir="/var/run/postgresql")

        self.assertEqual(kwargs["dsn"], config.dsn)
        self.assertNotIn("host", kwargs)
        self.assertEqual(kwargs["server_settings"], {"application_name": "local_api"})


# ---------------------------------------------------------------------------
# Stage 1.4 (2026-05-20 stability re-audit): pool.acquire() must use an
# explicit timeout. Without it a saturated pool turns workers into zombies
# stuck on await pool.acquire() — SIGTERM cannot interrupt that await,
# systemd waits 90s then SIGKILLs, the open transaction lingers until
# TCP keepalive expires (~5min).
#
# Constraint #2a (Бобур, 2026-05-20): "падать по таймауту → уходить
# в ретрай, а не висеть зомби-процессом".
# ---------------------------------------------------------------------------


class PoolAcquireTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_database_config_exposes_acquire_timeout(self) -> None:
        config = DatabaseConfig(dsn="postgresql://example")
        self.assertTrue(
            hasattr(config, "acquire_timeout"),
            msg="DatabaseConfig must expose acquire_timeout (default 30s)",
        )
        self.assertGreater(
            getattr(config, "acquire_timeout"),
            0,
            msg="acquire_timeout default must be > 0",
        )

    async def test_connection_passes_timeout_to_pool_acquire(self) -> None:
        connection = _FakeConnection()
        database = AsyncpgDatabase(
            DatabaseConfig(dsn="postgresql://example", acquire_timeout=12.5)
        )
        pool = _FakePool(connection)
        database._pool = pool

        async with database.connection() as conn:
            self.assertIs(conn, connection)

        self.assertEqual(len(pool.acquire_calls), 1)
        self.assertEqual(pool.acquire_calls[0].get("timeout"), 12.5)

    async def test_connection_raises_timeout_when_pool_hangs(self) -> None:
        import asyncio

        class _HangingPool:
            async def acquire(self, *, timeout: float):
                # Simulate asyncpg.Pool.acquire semantics: it wraps the
                # internal acquire-wait with wait_for(timeout) and lets
                # asyncio.TimeoutError bubble up. We mirror that here so
                # the test guarantees AsyncpgDatabase.connection
                # propagates the timeout, not silences it.
                async def _wait_forever():
                    await asyncio.sleep(timeout * 100)
                await asyncio.wait_for(_wait_forever(), timeout=timeout)
                raise AssertionError("unreachable — wait_for must raise")

            async def release(self, connection):  # noqa: ARG002
                pass

        database = AsyncpgDatabase(
            DatabaseConfig(dsn="postgresql://example", acquire_timeout=0.05)
        )
        database._pool = _HangingPool()

        with self.assertRaises(asyncio.TimeoutError):
            async with database.connection():
                self.fail("body should not be entered")


# ---------------------------------------------------------------------------
# End Stage 1.4 tests
# ---------------------------------------------------------------------------


@unittest.skipIf(not HAS_ASYNCPG, "asyncpg not installed")
class DatabaseFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_create_pool_falls_back_to_tcp_when_socket_auth_fails(self) -> None:
        class InvalidAuthorizationSpecificationError(Exception):
            pass

        config = DatabaseConfig(
            dsn="postgresql://localhost:5432/sofascore_schema_inspector",
            application_name="worker-live-hot",
            unix_socket_dir="/var/run/postgresql",
        )
        attempts: list[dict[str, object]] = []
        result = object()

        async def fake_create_pool(**kwargs):
            attempts.append(kwargs)
            if len(attempts) == 1:
                raise InvalidAuthorizationSpecificationError("peer auth failed")
            return result

        with patch("schema_inspector.db.sys.platform", "linux"), patch(
            "asyncpg.create_pool",
            side_effect=fake_create_pool,
        ):
            pool = await create_pool_with_fallback(config)

        self.assertIs(pool, result)
        self.assertEqual(attempts[0]["host"], "/var/run/postgresql")
        self.assertNotIn("host", attempts[1])

    async def test_connect_falls_back_to_tcp_when_socket_auth_fails(self) -> None:
        class InvalidAuthorizationSpecificationError(Exception):
            pass

        config = DatabaseConfig(
            dsn="postgresql://localhost:5432/sofascore_schema_inspector",
            application_name="local_api",
            unix_socket_dir="/var/run/postgresql",
        )
        attempts: list[dict[str, object]] = []
        result = object()

        async def fake_connect(**kwargs):
            attempts.append(kwargs)
            if len(attempts) == 1:
                raise InvalidAuthorizationSpecificationError("peer auth failed")
            return result

        with patch("schema_inspector.db.sys.platform", "linux"), patch(
            "asyncpg.connect",
            side_effect=fake_connect,
        ):
            connection = await connect_with_fallback(config)

        self.assertIs(connection, result)
        self.assertEqual(attempts[0]["host"], "/var/run/postgresql")
        self.assertNotIn("host", attempts[1])
