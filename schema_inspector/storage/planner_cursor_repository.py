"""PostgreSQL repository for persistent planner cursors."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


class SqlFetchExecutor(SqlExecutor, Protocol):
    async def fetch(self, query: str, *args: object) -> list[object]: ...


@dataclass(frozen=True)
class PlannerCursorRecord:
    planner_name: str
    source_slug: str
    sport_slug: str
    scope_type: str
    scope_id: int | None = None
    cursor_date: date | None = None
    cursor_id: int | None = None


class PlannerCursorRepository:
    async def load_cursor(
        self,
        executor: SqlFetchExecutor,
        *,
        planner_name: str,
        source_slug: str,
        sport_slug: str,
        scope_type: str,
        scope_id: int | None = None,
    ) -> PlannerCursorRecord | None:
        rows = await executor.fetch(
            """
            SELECT
                planner_name,
                source_slug,
                sport_slug,
                scope_type,
                scope_id,
                cursor_date,
                cursor_id
            FROM etl_planner_cursor
            WHERE planner_name = $1
              AND source_slug = $2
              AND sport_slug = $3
              AND scope_type = $4
              AND scope_id = $5
            LIMIT 1
            """,
            str(planner_name).strip().lower(),
            str(source_slug).strip().lower(),
            str(sport_slug).strip().lower(),
            str(scope_type).strip().lower(),
            _scope_id_value(scope_id),
        )
        if not rows:
            return None
        row = rows[0]
        return PlannerCursorRecord(
            planner_name=str(row["planner_name"]),
            source_slug=str(row["source_slug"]),
            sport_slug=str(row["sport_slug"]),
            scope_type=str(row["scope_type"]),
            scope_id=_scope_id_model_value(row["scope_id"]),
            cursor_date=row["cursor_date"],
            cursor_id=None if row["cursor_id"] is None else int(row["cursor_id"]),
        )

    async def upsert_cursor(
        self,
        executor: SqlExecutor,
        *,
        planner_name: str,
        source_slug: str,
        sport_slug: str,
        scope_type: str,
        scope_id: int | None = None,
        cursor_date: date | None = None,
        cursor_id: int | None = None,
    ) -> None:
        await executor.execute(
            """
            INSERT INTO etl_planner_cursor (
                planner_name,
                source_slug,
                sport_slug,
                scope_type,
                scope_id,
                cursor_date,
                cursor_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (planner_name, source_slug, sport_slug, scope_type, scope_id)
            DO UPDATE SET
                cursor_date = EXCLUDED.cursor_date,
                cursor_id = EXCLUDED.cursor_id,
                updated_at = now()
            """,
            str(planner_name).strip().lower(),
            str(source_slug).strip().lower(),
            str(sport_slug).strip().lower(),
            str(scope_type).strip().lower(),
            _scope_id_value(scope_id),
            cursor_date,
            None if cursor_id is None else int(cursor_id),
        )


def _scope_id_value(scope_id: int | None) -> int:
    return 0 if scope_id is None else int(scope_id)


def _scope_id_model_value(scope_id: object) -> int | None:
    if scope_id in (None, 0):
        return None
    return int(scope_id)
