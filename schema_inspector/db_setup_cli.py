"""CLI for creating PostgreSQL database and applying schema migrations."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import SplitResult, urlsplit, urlunsplit

from .db import load_database_config

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MIGRATION_TABLE = "schema_migration"
BASE_SCHEMA_RECORD = "__base_schema__"


@dataclass(frozen=True)
class DatabaseSetupResult:
    """Summary of database initialization work."""

    database_name: str
    database_created: bool
    schema_initialized: bool
    applied_migrations: tuple[str, ...]
    skipped_migrations: tuple[str, ...]


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description=(
            "Create the target PostgreSQL database when missing, initialize the base "
            "schema on an empty database, then apply all SQL migrations from migrations/."
        ),
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Target PostgreSQL DSN. Falls back to SOFASCORE_DATABASE_URL / DATABASE_URL / POSTGRES_DSN.",
    )
    parser.add_argument(
        "--maintenance-url",
        default=None,
        help=(
            "Optional PostgreSQL DSN used only for CREATE DATABASE. "
            "If omitted, the target DSN is reused with the database name replaced."
        ),
    )
    parser.add_argument(
        "--maintenance-database",
        default="postgres",
        help="Database name used for admin connection when --maintenance-url is omitted.",
    )
    parser.add_argument(
        "--skip-create-database",
        action="store_true",
        help="Skip CREATE DATABASE and only connect to the target database.",
    )
    parser.add_argument(
        "--schema-file",
        default="postgres_schema.sql",
        help="Path to the base schema SQL file. Relative paths are resolved from the project root.",
    )
    parser.add_argument(
        "--migrations-dir",
        default="migrations",
        help="Path to the directory with SQL migrations. Relative paths are resolved from the project root.",
    )
    args = parser.parse_args()
    return asyncio.run(_run(args))


async def _run(args: argparse.Namespace) -> int:
    try:
        import asyncpg
    except ImportError as exc:
        raise RuntimeError(
            "asyncpg is required for PostgreSQL setup. Install project dependencies before running this command."
        ) from exc

    database_config = load_database_config(dsn=args.database_url)
    target_dsn = database_config.dsn
    target_database = _database_name_from_dsn(target_dsn)
    maintenance_dsn = args.maintenance_url or _replace_database_in_dsn(target_dsn, args.maintenance_database)
    schema_file = _resolve_project_path(args.schema_file)
    migrations_dir = _resolve_project_path(args.migrations_dir)

    if not schema_file.exists():
        raise FileNotFoundError(f"Base schema file not found: {schema_file}")
    if not migrations_dir.exists():
        raise FileNotFoundError(f"Migrations directory not found: {migrations_dir}")

    database_created = False
    if not args.skip_create_database:
        print(f"[db] checking database `{target_database}`", flush=True)
        database_created = await _ensure_database_exists(
            asyncpg,
            maintenance_dsn=maintenance_dsn,
            target_database=target_database,
        )
        if database_created:
            print(f"[db] created database `{target_database}`", flush=True)
        else:
            print(f"[db] database `{target_database}` already exists", flush=True)

    print(f"[db] connecting to `{target_database}`", flush=True)
    connection = await asyncpg.connect(target_dsn, command_timeout=database_config.command_timeout)
    try:
        existing_tables = await _list_user_tables(connection)
        schema_initialized = False
        if not existing_tables:
            print(f"[db] applying base schema `{schema_file.name}`", flush=True)
            await _apply_sql_file(connection, schema_file)
            schema_initialized = True
        else:
            print(f"[db] existing schema detected with {len(existing_tables)} table(s), base schema skipped", flush=True)

        await _ensure_migration_table(connection)
        if schema_initialized:
            await _record_migration(
                connection,
                name=BASE_SCHEMA_RECORD,
                checksum=_sha256_file(schema_file),
            )

        applied_migrations: list[str] = []
        skipped_migrations: list[str] = []
        for migration_file in _discover_migration_files(migrations_dir):
            checksum = _sha256_file(migration_file)
            recorded_checksum = await _fetch_recorded_checksum(connection, migration_file.name)
            if recorded_checksum is not None:
                if recorded_checksum != checksum:
                    raise RuntimeError(
                        "Migration checksum mismatch for "
                        f"{migration_file.name}. The file changed after it had already been applied."
                    )
                skipped_migrations.append(migration_file.name)
                continue

            print(f"[db] applying migration `{migration_file.name}`", flush=True)
            await _apply_sql_file(connection, migration_file)
            await _record_migration(connection, name=migration_file.name, checksum=checksum)
            applied_migrations.append(migration_file.name)
    finally:
        await connection.close()

    result = DatabaseSetupResult(
        database_name=target_database,
        database_created=database_created,
        schema_initialized=schema_initialized,
        applied_migrations=tuple(applied_migrations),
        skipped_migrations=tuple(skipped_migrations),
    )
    print(
        "postgres_setup "
        f"database={result.database_name} "
        f"created={'yes' if result.database_created else 'no'} "
        f"schema_initialized={'yes' if result.schema_initialized else 'no'} "
        f"applied_migrations={len(result.applied_migrations)} "
        f"skipped_migrations={len(result.skipped_migrations)}",
        flush=True,
    )
    return 0


async def _ensure_database_exists(asyncpg_module, *, maintenance_dsn: str, target_database: str) -> bool:
    connection = await asyncpg_module.connect(maintenance_dsn)
    try:
        exists = await connection.fetchval("SELECT 1 FROM pg_database WHERE datname = $1", target_database)
        if exists:
            return False
        await connection.execute(f'CREATE DATABASE {_quote_identifier(target_database)}')
        return True
    finally:
        await connection.close()


async def _list_user_tables(connection) -> tuple[str, ...]:
    rows = await connection.fetch(
        """
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'public'
          AND tablename <> $1
        ORDER BY tablename
        """,
        MIGRATION_TABLE,
    )
    return tuple(row["tablename"] for row in rows)


async def _ensure_migration_table(connection) -> None:
    await connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {MIGRATION_TABLE} (
            name TEXT PRIMARY KEY,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )


async def _fetch_recorded_checksum(connection, name: str) -> str | None:
    return await connection.fetchval(f"SELECT checksum FROM {MIGRATION_TABLE} WHERE name = $1", name)


async def _record_migration(connection, *, name: str, checksum: str) -> None:
    await connection.execute(
        f"""
        INSERT INTO {MIGRATION_TABLE} (name, checksum)
        VALUES ($1, $2)
        ON CONFLICT (name) DO UPDATE
        SET checksum = EXCLUDED.checksum,
            applied_at = NOW()
        """,
        name,
        checksum,
    )


async def _apply_sql_file(connection, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    if not sql.strip():
        return
    await connection.execute(sql)


def _discover_migration_files(migrations_dir: Path) -> tuple[Path, ...]:
    return tuple(sorted(path for path in migrations_dir.glob("*.sql") if path.is_file()))


def _resolve_project_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _database_name_from_dsn(dsn: str) -> str:
    parsed = urlsplit(dsn)
    database = parsed.path.lstrip("/")
    if not database:
        raise ValueError(f"Database name is missing in DSN: {dsn}")
    return database


def _replace_database_in_dsn(dsn: str, database_name: str) -> str:
    parsed = urlsplit(dsn)
    replaced = SplitResult(
        scheme=parsed.scheme,
        netloc=parsed.netloc,
        path=f"/{database_name}",
        query=parsed.query,
        fragment=parsed.fragment,
    )
    return urlunsplit(replaced)


def _quote_identifier(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


if __name__ == "__main__":
    raise SystemExit(main())
