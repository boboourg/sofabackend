from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

from schema_inspector.db import AsyncpgDatabase, load_database_config


DEFAULT_SHOW_LIMIT = 20
DEFAULT_DUMP_LIMIT = 100
READ_ONLY_PREFIXES = ("select", "with", "explain")


@dataclass(frozen=True)
class ColumnInfo:
    column_name: str
    data_type: str
    is_nullable: bool
    column_default: str | None
    ordinal_position: int
    is_primary_key: bool


async def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = _build_parser()
    args = parser.parse_args()

    database_config = load_database_config(dsn=args.database_url)

    async with AsyncpgDatabase(database_config) as database:
        async with database.connection() as connection:
            if args.command == "summary":
                await _print_summary(connection)
                return 0

            if args.command == "tables":
                await _print_tables(connection)
                return 0

            if args.command == "describe":
                await _print_table_schema(connection, args.table)
                return 0

            if args.command == "show":
                await _print_table_rows(
                    connection,
                    table_name=args.table,
                    limit=args.limit,
                    offset=args.offset,
                )
                return 0

            if args.command == "dump":
                await _dump_data(
                    connection,
                    table_name=args.table,
                    limit=None if args.all else args.limit,
                    offset=args.offset,
                    output_path=args.output,
                )
                return 0

            if args.command == "query":
                await _run_read_only_query(connection, args.sql)
                return 0

            await _interactive_shell(connection)
            return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Interactive explorer for the dedicated Sofascore PostgreSQL backend.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Override database DSN. Falls back to SOFASCORE_DATABASE_URL from .env.",
    )

    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("summary", help="Show tables and exact row counts.")
    subparsers.add_parser("tables", help="List all public tables.")

    describe = subparsers.add_parser("describe", help="Show table schema.")
    describe.add_argument("table", help="Table name.")

    show = subparsers.add_parser("show", help="Show rows from a table.")
    show.add_argument("table", help="Table name.")
    show.add_argument("--limit", type=int, default=DEFAULT_SHOW_LIMIT)
    show.add_argument("--offset", type=int, default=0)

    dump = subparsers.add_parser("dump", help="Dump one table or the whole database as JSON.")
    dump.add_argument("--table", default=None, help="Optional table name. If omitted, dumps all tables.")
    dump.add_argument("--limit", type=int, default=DEFAULT_DUMP_LIMIT, help="Per-table limit unless --all is used.")
    dump.add_argument("--offset", type=int, default=0)
    dump.add_argument("--all", action="store_true", help="Dump all rows instead of applying the limit.")
    dump.add_argument("--output", default=None, help="Optional output JSON path.")

    query = subparsers.add_parser("query", help="Run a read-only SQL query.")
    query.add_argument("sql", help="Only SELECT / WITH / EXPLAIN queries are allowed.")

    return parser


async def _interactive_shell(connection: Any) -> None:
    while True:
        print("\nSofascore DB Explorer")
        print("1. Summary")
        print("2. Tables")
        print("3. Describe table")
        print("4. Show table rows")
        print("5. Dump table or all tables")
        print("6. Run read-only SQL")
        print("0. Exit")

        choice = input("Choose an action: ").strip()
        if choice == "0":
            return
        if choice == "1":
            await _print_summary(connection)
            continue
        if choice == "2":
            await _print_tables(connection)
            continue
        if choice == "3":
            table_name = input("Table name: ").strip()
            if table_name:
                await _print_table_schema(connection, table_name)
            continue
        if choice == "4":
            table_name = input("Table name: ").strip()
            if not table_name:
                continue
            limit = _read_int("Limit", DEFAULT_SHOW_LIMIT)
            offset = _read_int("Offset", 0)
            await _print_table_rows(connection, table_name=table_name, limit=limit, offset=offset)
            continue
        if choice == "5":
            table_name = input("Table name (leave empty for all tables): ").strip() or None
            dump_all = _read_bool("Dump all rows", default=False)
            limit = None if dump_all else _read_int("Limit", DEFAULT_DUMP_LIMIT)
            offset = _read_int("Offset", 0)
            output_path = input("Output JSON path (leave empty to print): ").strip() or None
            await _dump_data(connection, table_name=table_name, limit=limit, offset=offset, output_path=output_path)
            continue
        if choice == "6":
            sql = input("SQL (SELECT/WITH/EXPLAIN only): ").strip()
            if sql:
                await _run_read_only_query(connection, sql)
            continue

        print("Unknown option.")


async def _print_summary(connection: Any) -> None:
    table_names = await _fetch_table_names(connection)
    rows = []
    for table_name in table_names:
        count = await _count_rows(connection, table_name)
        rows.append({"table": table_name, "rows": count})

    print(json.dumps(rows, ensure_ascii=False, indent=2))


async def _print_tables(connection: Any) -> None:
    table_names = await _fetch_table_names(connection)
    print(json.dumps(table_names, ensure_ascii=False, indent=2))


async def _print_table_schema(connection: Any, table_name: str) -> None:
    validated_table = await _validate_table_name(connection, table_name)
    columns = await _fetch_columns(connection, validated_table)
    payload = [
        {
            "column_name": column.column_name,
            "data_type": column.data_type,
            "is_nullable": column.is_nullable,
            "column_default": column.column_default,
            "ordinal_position": column.ordinal_position,
            "is_primary_key": column.is_primary_key,
        }
        for column in columns
    ]
    print(json.dumps({"table": validated_table, "columns": payload}, ensure_ascii=False, indent=2))


async def _print_table_rows(connection: Any, *, table_name: str, limit: int, offset: int) -> None:
    validated_table = await _validate_table_name(connection, table_name)
    rows = await _fetch_rows(connection, validated_table, limit=limit, offset=offset)
    payload = {
        "table": validated_table,
        "limit": limit,
        "offset": offset,
        "rows": _normalize_rows(rows),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default))


async def _dump_data(
    connection: Any,
    *,
    table_name: str | None,
    limit: int | None,
    offset: int,
    output_path: str | None,
) -> None:
    if table_name is not None:
        validated_table = await _validate_table_name(connection, table_name)
        payload: object = {
            validated_table: _normalize_rows(
                await _fetch_rows(connection, validated_table, limit=limit, offset=offset)
            )
        }
    else:
        payload = {}
        for name in await _fetch_table_names(connection):
            payload[name] = _normalize_rows(await _fetch_rows(connection, name, limit=limit, offset=offset))

    rendered = json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default)
    if output_path:
        target = Path(output_path)
        target.write_text(rendered, encoding="utf-8")
        print(f"Saved dump to {target}")
        return
    print(rendered)


async def _run_read_only_query(connection: Any, sql: str) -> None:
    normalized = sql.strip().lower()
    if not normalized.startswith(READ_ONLY_PREFIXES):
        raise RuntimeError("Only SELECT / WITH / EXPLAIN queries are allowed.")

    rows = await connection.fetch(sql)
    print(json.dumps(_normalize_rows(rows), ensure_ascii=False, indent=2, default=_json_default))


async def _fetch_table_names(connection: Any) -> list[str]:
    rows = await connection.fetch(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    )
    return [row["table_name"] for row in rows]


async def _fetch_columns(connection: Any, table_name: str) -> list[ColumnInfo]:
    rows = await connection.fetch(
        """
        SELECT
            c.column_name,
            c.data_type,
            c.is_nullable = 'YES' AS is_nullable,
            c.column_default,
            c.ordinal_position,
            EXISTS (
                SELECT 1
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = 'public'
                    AND tc.table_name = c.table_name
                    AND tc.constraint_type = 'PRIMARY KEY'
                    AND kcu.column_name = c.column_name
            ) AS is_primary_key
        FROM information_schema.columns c
        WHERE c.table_schema = 'public' AND c.table_name = $1
        ORDER BY c.ordinal_position
        """,
        table_name,
    )
    return [ColumnInfo(**dict(row)) for row in rows]


async def _fetch_primary_key_columns(connection: Any, table_name: str) -> list[str]:
    rows = await connection.fetch(
        """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = 'public'
            AND tc.table_name = $1
            AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY kcu.ordinal_position
        """,
        table_name,
    )
    return [row["column_name"] for row in rows]


async def _fetch_rows(connection: Any, table_name: str, *, limit: int | None, offset: int) -> list[dict[str, Any]]:
    primary_key_columns = await _fetch_primary_key_columns(connection, table_name)
    order_clause = ""
    if primary_key_columns:
        order_clause = " ORDER BY " + ", ".join(_quote_identifier(column) for column in primary_key_columns)

    sql = f'SELECT * FROM {_quote_identifier(table_name)}{order_clause}'
    if limit is None:
        rows = await connection.fetch(sql)
    else:
        sql = f"{sql} LIMIT $1 OFFSET $2"
        rows = await connection.fetch(sql, limit, offset)
    return _normalize_rows(rows)


async def _count_rows(connection: Any, table_name: str) -> int:
    row = await connection.fetchrow(f"SELECT COUNT(*) AS count_value FROM {_quote_identifier(table_name)}")
    return int(row["count_value"])


async def _validate_table_name(connection: Any, table_name: str) -> str:
    available_tables = set(await _fetch_table_names(connection))
    if table_name not in available_tables:
        raise RuntimeError(f"Unknown table: {table_name}")
    return table_name


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _normalize_rows(rows: list[Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in rows]


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def _read_int(label: str, default: int) -> int:
    raw = input(f"{label} [{default}]: ").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _read_bool(label: str, *, default: bool) -> bool:
    suffix = "Y/n" if default else "y/N"
    raw = input(f"{label} [{suffix}]: ").strip().lower()
    if not raw:
        return default
    return raw in {"y", "yes", "1", "true"}


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
