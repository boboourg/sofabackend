"""Sorted bulk write helpers for deterministic PostgreSQL mutation order."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

_EXECUTEMANY_BATCH_SIZE = 100


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...

    async def executemany(self, query: str, args: list[tuple[object, ...]]) -> Any: ...


@dataclass(frozen=True)
class RowsBatch:
    columns: tuple[str, ...]
    values: tuple[tuple[object, ...], ...]


async def sorted_upsert(
    conn: SqlExecutor,
    table: str,
    rows,
    sort_keys,
    conflict_target,
    update_cols,
) -> int:
    columns, normalized_rows = _normalize_rows(rows)
    if not normalized_rows:
        return 0

    key_indexes = _indexes_for(columns, sort_keys)
    sorted_rows = sorted(normalized_rows, key=lambda row: _sort_key(row, key_indexes))
    placeholders = ", ".join(f"${index}" for index in range(1, len(columns) + 1))
    sql = (
        f"INSERT INTO {_quote_ident(table)} ({_column_list(columns)}) "
        f"VALUES ({placeholders})"
    )
    resolved_conflict_target = _normalize_columns(conflict_target)
    if resolved_conflict_target:
        sql += f" ON CONFLICT ({_column_list(resolved_conflict_target)})"
        assignments = _update_assignments(table, update_cols)
        if assignments:
            sql += " DO UPDATE SET " + ", ".join(assignments)
        else:
            sql += " DO NOTHING"

    await _executemany(conn, sql, sorted_rows)
    return len(sorted_rows)


async def sorted_delete_insert(
    conn: SqlExecutor,
    table: str,
    rows,
    delete_key,
    sort_keys,
) -> int:
    columns, normalized_rows = _normalize_rows(rows)
    if not normalized_rows:
        return 0

    delete_columns = _normalize_columns(delete_key)
    delete_indexes = _indexes_for(columns, delete_columns)
    sorted_delete_keys = sorted(
        {
            tuple(row[index] for index in delete_indexes)
            for row in normalized_rows
            if all(row[index] is not None for index in delete_indexes)
        },
        key=lambda values: tuple((value is None, value) for value in values),
    )
    if sorted_delete_keys:
        where_clause = " AND ".join(
            f"{_quote_ident(column)} = ${index}"
            for index, column in enumerate(delete_columns, start=1)
        )
        delete_sql = f"DELETE FROM {_quote_ident(table)} WHERE {where_clause}"
        for delete_values in sorted_delete_keys:
            await conn.execute(delete_sql, *delete_values)

    key_indexes = _indexes_for(columns, sort_keys)
    sorted_rows = sorted(normalized_rows, key=lambda row: _sort_key(row, key_indexes))
    placeholders = ", ".join(f"${index}" for index in range(1, len(columns) + 1))
    insert_sql = (
        f"INSERT INTO {_quote_ident(table)} ({_column_list(columns)}) "
        f"VALUES ({placeholders})"
    )
    await _executemany(conn, insert_sql, sorted_rows)
    return len(sorted_rows)


def _normalize_rows(rows) -> tuple[tuple[str, ...], list[tuple[object, ...]]]:
    if isinstance(rows, RowsBatch):
        return tuple(rows.columns), list(rows.values)

    resolved_rows = list(rows)
    if not resolved_rows:
        return (), []
    first = resolved_rows[0]
    if not isinstance(first, Mapping):
        raise TypeError("rows must be a RowsBatch or a sequence of mappings")

    columns = tuple(first.keys())
    values: list[tuple[object, ...]] = []
    for row in resolved_rows:
        if not isinstance(row, Mapping):
            raise TypeError("rows must contain only mappings")
        if tuple(row.keys()) != columns:
            raise ValueError("all row mappings must share identical column order")
        values.append(tuple(row[column] for column in columns))
    return columns, values


def _normalize_columns(value) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(str(item) for item in value)


def _indexes_for(columns: Sequence[str], selected_columns) -> tuple[int, ...]:
    normalized = _normalize_columns(selected_columns)
    index_by_column = {column: index for index, column in enumerate(columns)}
    return tuple(index_by_column[column] for column in normalized)


def _sort_key(row: Sequence[object], indexes: Sequence[int]) -> tuple[tuple[bool, object], ...]:
    return tuple((row[index] is None, row[index]) for index in indexes)


def _update_assignments(table: str, update_cols) -> tuple[str, ...]:
    if update_cols is None:
        return ()
    if isinstance(update_cols, Mapping):
        return tuple(
            f"{_quote_ident(column)} = {expression}"
            for column, expression in update_cols.items()
        )
    return tuple(
        f"{_quote_ident(column)} = EXCLUDED.{_quote_ident(column)}"
        for column in update_cols
    )


def _column_list(columns: Sequence[str]) -> str:
    return ", ".join(_quote_ident(column) for column in columns)


def _quote_ident(value: str) -> str:
    return str(value)


async def _executemany(conn: SqlExecutor, sql: str, rows: list[tuple[object, ...]]) -> None:
    for start in range(0, len(rows), _EXECUTEMANY_BATCH_SIZE):
        await conn.executemany(sql, rows[start : start + _EXECUTEMANY_BATCH_SIZE])


__all__ = [
    "RowsBatch",
    "sorted_delete_insert",
    "sorted_upsert",
]
