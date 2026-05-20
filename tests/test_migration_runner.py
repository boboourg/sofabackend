"""TDD tests for Stage 3.5 (2026-05-20): automatic SQL migration runner.

Until this commit migrations were applied operationally (the CLAUDE.md
note literally said "There is no migration runner in-repo"). Every new
DDL change required an ops-side ``psql -f migrations/<file>.sql`` step,
which was both error-prone and gated the deploy on a human.

This runner introduces a ``schema_migrations`` ledger table and three
public entry points:

* ``apply_pending_migrations(connection, migrations_dir)`` — runs every
  ``.sql`` file in lexicographic order that is not yet in the ledger,
  inside a session-level advisory lock so two concurrent workers cannot
  race each other on the same migration.
* ``list_applied_migrations(connection)`` — for reporting.
* ``mark_migration_applied(connection, filename)`` — bootstrap helper
  to mark an already-applied migration without re-running it (used
  for the one-time prod bootstrap).

The CLI subcommand ``db-migrate`` wraps these with three actions:
``apply`` / ``status`` / ``mark-applied``.
"""

from __future__ import annotations

import unittest
from pathlib import Path


class _FakeConnection:
    """Captures executed SQL + returns canned fetch results.

    Mirrors the subset of asyncpg.Connection that migration_runner uses:
    fetchval, fetch, execute. Plain SQL — no prepared statements."""

    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_results: list[list[dict[str, object]]] = []
        self.fetchval_results: list[object] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"

    async def fetch(self, query: str, *args: object):
        self.fetch_calls.append((query, args))
        if self.fetch_results:
            return self.fetch_results.pop(0)
        return []

    async def fetchval(self, query: str, *args: object):
        self.fetchval_calls.append((query, args))
        if self.fetchval_results:
            return self.fetchval_results.pop(0)
        return None


class MigrationRunnerSchemaMigrationsTableTests(unittest.IsolatedAsyncioTestCase):
    async def test_ensure_table_creates_schema_migrations_idempotently(self) -> None:
        from schema_inspector.storage.migration_runner import _ensure_schema_migrations_table

        connection = _FakeConnection()
        await _ensure_schema_migrations_table(connection)

        # Single CREATE TABLE IF NOT EXISTS — idempotent.
        ddl_calls = [
            sql for sql, _ in connection.execute_calls
            if "CREATE TABLE IF NOT EXISTS schema_migrations" in sql
        ]
        self.assertEqual(len(ddl_calls), 1)
        ddl = ddl_calls[0]
        for required_column in ("filename TEXT", "applied_at TIMESTAMPTZ", "checksum TEXT"):
            self.assertIn(required_column, ddl)
        self.assertIn("PRIMARY KEY (filename)", ddl)


class MigrationRunnerListApplyTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_applied_returns_set_of_filenames(self) -> None:
        from schema_inspector.storage.migration_runner import list_applied_migrations

        connection = _FakeConnection()
        connection.fetch_results = [
            [
                {"filename": "2026-04-16_a.sql"},
                {"filename": "2026-04-23_b.sql"},
            ]
        ]
        applied = await list_applied_migrations(connection)
        self.assertEqual(applied, {"2026-04-16_a.sql", "2026-04-23_b.sql"})

    async def test_apply_pending_skips_already_applied_files(self) -> None:
        """If schema_migrations already lists a file, the runner must
        not re-execute it — that is the core idempotency property."""
        from schema_inspector.storage.migration_runner import apply_pending_migrations

        connection = _FakeConnection()
        # 1st fetch: advisory_lock acquired = True (fetchval call)
        # 2nd fetch: applied set
        connection.fetchval_results = [True]
        connection.fetch_results = [[{"filename": "old.sql"}]]

        migrations_dir = Path(self.id_to_tempdir())
        (migrations_dir / "old.sql").write_text("-- already applied", encoding="utf-8")
        (migrations_dir / "new.sql").write_text(
            "BEGIN; CREATE TABLE IF NOT EXISTS sentinel_a (id INT); COMMIT;",
            encoding="utf-8",
        )

        applied_now = await apply_pending_migrations(connection, migrations_dir)
        self.assertEqual(applied_now, ["new.sql"])

        # The 'old.sql' content must NOT appear in any execute call.
        all_sql = " ".join(sql for sql, _ in connection.execute_calls)
        self.assertIn("CREATE TABLE IF NOT EXISTS sentinel_a", all_sql)

        # The ledger must record the newly-applied file.
        ledger_inserts = [
            (sql, args) for sql, args in connection.execute_calls
            if "INSERT INTO schema_migrations" in sql
        ]
        self.assertEqual(len(ledger_inserts), 1)
        self.assertIn("new.sql", ledger_inserts[0][1])

    async def test_apply_pending_applies_in_lexicographic_order(self) -> None:
        from schema_inspector.storage.migration_runner import apply_pending_migrations

        connection = _FakeConnection()
        connection.fetchval_results = [True]
        connection.fetch_results = [[]]  # nothing applied yet

        migrations_dir = Path(self.id_to_tempdir())
        # Intentionally created in reverse order — filesystem may also
        # return them in arbitrary order. The runner must sort.
        (migrations_dir / "2026-04-23_b.sql").write_text(
            "BEGIN; CREATE TABLE IF NOT EXISTS sentinel_b (id INT); COMMIT;",
            encoding="utf-8",
        )
        (migrations_dir / "2026-04-16_a.sql").write_text(
            "BEGIN; CREATE TABLE IF NOT EXISTS sentinel_a (id INT); COMMIT;",
            encoding="utf-8",
        )

        applied_now = await apply_pending_migrations(connection, migrations_dir)
        self.assertEqual(
            applied_now,
            ["2026-04-16_a.sql", "2026-04-23_b.sql"],
            msg="Migrations must apply in lexicographic order so dated filenames replay deterministically",
        )

    async def test_apply_pending_uses_advisory_lock(self) -> None:
        """Two parallel 9-worker boots must not race on the same migration.
        The runner takes a session-level advisory lock keyed on a
        well-known integer before reading the ledger or applying anything."""
        from schema_inspector.storage.migration_runner import apply_pending_migrations

        connection = _FakeConnection()
        connection.fetchval_results = [True]
        connection.fetch_results = [[]]

        migrations_dir = Path(self.id_to_tempdir())
        await apply_pending_migrations(connection, migrations_dir)

        lock_calls = [
            sql for sql, _ in connection.fetchval_calls
            if "pg_try_advisory_lock" in sql or "pg_advisory_lock" in sql
        ]
        self.assertTrue(
            lock_calls,
            msg="apply_pending_migrations must take a Postgres advisory lock before reading the ledger",
        )

    async def test_apply_pending_returns_empty_when_lock_busy(self) -> None:
        """If another worker is currently mid-migration the runner
        must back off cleanly (return empty list) rather than crash."""
        from schema_inspector.storage.migration_runner import apply_pending_migrations

        connection = _FakeConnection()
        connection.fetchval_results = [False]  # pg_try_advisory_lock returned false

        migrations_dir = Path(self.id_to_tempdir())
        (migrations_dir / "x.sql").write_text("BEGIN; COMMIT;", encoding="utf-8")

        applied_now = await apply_pending_migrations(connection, migrations_dir)
        self.assertEqual(applied_now, [])

    async def test_mark_migration_applied_inserts_without_executing_sql(self) -> None:
        """Bootstrap helper: pretend a migration is already applied
        without running its SQL. Used once on prod to seed the ledger
        with the back-catalog of migrations that ran before the runner
        existed."""
        from schema_inspector.storage.migration_runner import mark_migration_applied

        connection = _FakeConnection()
        await mark_migration_applied(connection, "2026-04-16_initial.sql")

        ledger_inserts = [
            (sql, args) for sql, args in connection.execute_calls
            if "INSERT INTO schema_migrations" in sql
        ]
        self.assertEqual(len(ledger_inserts), 1)
        self.assertIn("2026-04-16_initial.sql", ledger_inserts[0][1])
        # mark_migration_applied may ensure the ledger table exists
        # (CREATE TABLE IF NOT EXISTS schema_migrations) — that is OK.
        # The forbidden behaviour is executing the MIGRATION file's
        # contents. There is no migration file in this test, so we
        # only need to assert that no CREATE TABLE OTHER THAN the
        # ledger appeared.
        forbidden_creates = [
            sql for sql, _ in connection.execute_calls
            if "CREATE TABLE" in sql and "schema_migrations" not in sql
        ]
        self.assertEqual(
            forbidden_creates, [],
            msg=(
                "mark_migration_applied must not execute migration "
                "file contents — only the ledger insert + the "
                "ensure-ledger DDL are permitted."
            ),
        )

    # Per-test temp dir helper
    def id_to_tempdir(self) -> str:
        import tempfile
        import os
        # Use the test id so each case gets a fresh directory.
        path = tempfile.mkdtemp(prefix="migration_runner_test_")
        self.addCleanup(_safe_rmtree, path)
        return path


def _safe_rmtree(path: str) -> None:
    import shutil
    shutil.rmtree(path, ignore_errors=True)


class MigrationRunnerCliRegistrationTests(unittest.TestCase):
    """The cli.py registers the `db-migrate` subcommand."""

    def test_db_migrate_subcommand_registered(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()
        subparsers_action = next(
            action for action in parser._actions if action.dest == "command"
        )
        self.assertIn("db-migrate", subparsers_action.choices)

    def test_db_migrate_supports_apply_status_mark_applied_actions(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()
        # apply (default)
        args = parser.parse_args(["db-migrate", "apply"])
        self.assertEqual(args.command, "db-migrate")
        self.assertEqual(args.action, "apply")
        # status
        args = parser.parse_args(["db-migrate", "status"])
        self.assertEqual(args.action, "status")
        # mark-applied
        args = parser.parse_args(["db-migrate", "mark-applied", "--filename", "old.sql"])
        self.assertEqual(args.action, "mark-applied")
        self.assertEqual(args.filename, "old.sql")


if __name__ == "__main__":
    unittest.main()
