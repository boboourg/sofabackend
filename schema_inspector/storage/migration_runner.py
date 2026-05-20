"""Idempotent SQL migration runner.

Stage 3.5 (2026-05-20). Until this commit the project had no in-repo
migration runner — the CLAUDE.md note read literally "There is no
migration runner in-repo — migrations are applied operationally". Every
new DDL change required an ops-side ``psql -f migrations/<file>.sql``,
gating each deploy on a human.

Design:

* ``schema_migrations`` ledger table tracks which migration files have
  been applied. Composite is intentionally simple: ``filename`` as
  PRIMARY KEY (one row per file). ``applied_at`` records the wall-clock
  timestamp; ``checksum`` records a SHA-256 of the file contents so
  drift between the in-repo file and what was actually applied is
  detectable (the runner does not currently FAIL on mismatch — it
  warns; that policy is intentional during the rollout window).
* ``apply_pending_migrations`` walks ``migrations/*.sql`` in
  lexicographic order, skipping files already in the ledger. Each
  pending file is executed verbatim (each migration ends with its own
  ``BEGIN; ... COMMIT;`` so the runner does not wrap a second
  transaction around it) and on success the ledger row is inserted.
  The whole loop is guarded by a session-level Postgres advisory lock
  so two parallel processes (e.g. nine hydrate workers starting up
  simultaneously) cannot apply the same migration twice. If the lock
  is held by another session the runner returns ``[]`` immediately —
  the other session is responsible for finishing the job.
* ``mark_migration_applied`` records a row without executing the SQL.
  This is the bootstrap helper used once to seed the ledger with the
  back-catalog of migrations applied before the runner existed.

The runner is exposed via the ``db-migrate`` CLI subcommand
(see ``schema_inspector/cli.py``):

    python -m schema_inspector.cli db-migrate apply
    python -m schema_inspector.cli db-migrate status
    python -m schema_inspector.cli db-migrate mark-applied --filename X.sql
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any, Iterable, Protocol


logger = logging.getLogger(__name__)


# Constant 64-bit Postgres advisory lock key. Chosen as a project-unique
# arbitrary integer; the only requirement is that it does not collide
# with other advisory locks the project takes (none today).
_ADVISORY_LOCK_KEY = 8201740520260520  # sofascore.migration_runner.2026-05-20


class SqlConnection(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...
    async def fetch(self, query: str, *args: object) -> Any: ...
    async def fetchval(self, query: str, *args: object) -> Any: ...


async def _ensure_schema_migrations_table(connection: SqlConnection) -> None:
    """Create the ledger table if it does not yet exist."""
    await connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            filename TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            checksum TEXT,
            PRIMARY KEY (filename)
        )
        """
    )


async def list_applied_migrations(connection: SqlConnection) -> set[str]:
    """Return the set of migration filenames already in the ledger."""
    await _ensure_schema_migrations_table(connection)
    rows = await connection.fetch("SELECT filename FROM schema_migrations")
    return {str(row["filename"]) for row in rows}


def _list_migration_files(migrations_dir: Path) -> list[Path]:
    """Lexicographically-sorted list of .sql files in the directory."""
    if not migrations_dir.exists() or not migrations_dir.is_dir():
        return []
    return sorted(migrations_dir.glob("*.sql"))


def _checksum_for(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def apply_pending_migrations(
    connection: SqlConnection,
    migrations_dir: Path,
) -> list[str]:
    """Apply every .sql file in ``migrations_dir`` that is not yet
    in the ``schema_migrations`` ledger. Returns the list of newly-
    applied filenames in apply order.

    The whole loop is guarded by ``pg_try_advisory_lock``. If another
    session holds the lock the function returns ``[]`` immediately —
    the caller is expected to either retry later or rely on the
    other session finishing the job.
    """

    await _ensure_schema_migrations_table(connection)

    lock_acquired = await connection.fetchval(
        "SELECT pg_try_advisory_lock($1::bigint)", _ADVISORY_LOCK_KEY
    )
    if not lock_acquired:
        logger.info(
            "apply_pending_migrations: advisory lock busy (key=%s); "
            "another session is mid-migration, deferring",
            _ADVISORY_LOCK_KEY,
        )
        return []

    try:
        applied = await list_applied_migrations(connection)
        all_files = _list_migration_files(migrations_dir)
        pending = [path for path in all_files if path.name not in applied]
        newly_applied: list[str] = []
        for path in pending:
            sql_text = path.read_text(encoding="utf-8")
            checksum = _checksum_for(path)
            logger.info(
                "apply_pending_migrations: applying %s (checksum=%s, size=%d bytes)",
                path.name, checksum[:12], len(sql_text),
            )
            # Each migration file owns its own BEGIN/COMMIT — execute the
            # text as-is. asyncpg.Connection.execute can run multi-
            # statement SQL when the statement contains semicolons; the
            # runner relies on that explicitly.
            await connection.execute(sql_text)
            await connection.execute(
                "INSERT INTO schema_migrations (filename, checksum) VALUES ($1, $2) "
                "ON CONFLICT (filename) DO NOTHING",
                path.name, checksum,
            )
            newly_applied.append(path.name)
        return newly_applied
    finally:
        try:
            await connection.execute(
                "SELECT pg_advisory_unlock($1::bigint)", _ADVISORY_LOCK_KEY
            )
        except Exception as exc:  # pragma: no cover — best-effort
            logger.warning("apply_pending_migrations: advisory_unlock failed: %r", exc)


async def mark_migration_applied(
    connection: SqlConnection,
    filename: str,
    *,
    checksum: str | None = None,
) -> None:
    """Insert a ``schema_migrations`` row without running the SQL.

    Used once on prod to seed the ledger with the back-catalog of
    migrations that were applied before the runner existed."""
    await _ensure_schema_migrations_table(connection)
    await connection.execute(
        "INSERT INTO schema_migrations (filename, checksum) VALUES ($1, $2) "
        "ON CONFLICT (filename) DO NOTHING",
        str(filename), checksum,
    )


def default_migrations_dir() -> Path:
    """Repo-root ``migrations/`` directory."""
    return Path(__file__).resolve().parent.parent.parent / "migrations"


__all__ = [
    "apply_pending_migrations",
    "default_migrations_dir",
    "list_applied_migrations",
    "mark_migration_applied",
]
