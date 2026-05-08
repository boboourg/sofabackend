"""Repository for the ``sportradar_coverage`` table (P3.2).

Provides:
* ``upsert_coverage(executor, record)`` — write reconciled SR row.
* ``fetch_coverage_for_ut(executor, ut_id)`` — gate-side lookup (hot path).
* ``count_by_method(executor)`` — diagnostic for sync reports.
* ``get_unmatched(executor)`` — diagnostic for manual-review backlog.

The gate (``SportradarCoverageGate``) consumes this via the ``_CoverageLookup``
protocol; this file's adapter ``SportradarCoverageLookup`` wraps a
DB connection to satisfy that protocol.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, Protocol


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...
    async def fetchrow(self, query: str, *args: object) -> Any: ...
    async def fetch(self, query: str, *args: object) -> Any: ...


@dataclass(frozen=True)
class SportradarCoverageRecord:
    sr_competition_id: int
    sr_competition_name: str
    sr_category_name: str
    sport_slug: str
    unique_tournament_id: int | None
    reconciliation_method: str
    reconciliation_confidence: float
    coverage_attrs: Mapping[str, int]
    tier: int | None
    season_id: int | None
    season_start_date: str | None
    last_changed: str


class SportradarCoverageRepository:
    """CRUD for sportradar_coverage rows.

    All methods take an ``SqlExecutor`` (a connection or transaction) so that
    callers can compose with their own transaction boundaries.
    """

    async def upsert_coverage(
        self, executor: SqlExecutor, record: SportradarCoverageRecord
    ) -> None:
        await executor.execute(
            """
            INSERT INTO sportradar_coverage (
                sr_competition_id, sr_competition_name, sr_category_name, sport_slug,
                unique_tournament_id, reconciliation_method, reconciliation_confidence,
                coverage_attrs, tier, season_id, season_start_date, last_changed,
                synced_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10,
                CAST($11 AS TIMESTAMPTZ), CAST($12 AS TIMESTAMPTZ), now()
            )
            ON CONFLICT (sr_competition_id) DO UPDATE SET
                sr_competition_name = EXCLUDED.sr_competition_name,
                sr_category_name = EXCLUDED.sr_category_name,
                sport_slug = EXCLUDED.sport_slug,
                unique_tournament_id = EXCLUDED.unique_tournament_id,
                reconciliation_method = EXCLUDED.reconciliation_method,
                reconciliation_confidence = EXCLUDED.reconciliation_confidence,
                coverage_attrs = EXCLUDED.coverage_attrs,
                tier = EXCLUDED.tier,
                season_id = EXCLUDED.season_id,
                season_start_date = EXCLUDED.season_start_date,
                last_changed = EXCLUDED.last_changed,
                synced_at = now()
            """,
            record.sr_competition_id,
            record.sr_competition_name,
            record.sr_category_name,
            record.sport_slug,
            record.unique_tournament_id,
            record.reconciliation_method,
            record.reconciliation_confidence,
            json.dumps(dict(record.coverage_attrs)),
            record.tier,
            record.season_id,
            record.season_start_date,
            record.last_changed,
        )

    async def fetch_coverage_for_ut(
        self, executor: SqlExecutor, unique_tournament_id: int
    ) -> dict[str, Any] | None:
        """Hot-path read for gate. Returns dict or None.

        Only the highest-confidence row per ut_id is returned (multiple SR
        competition rows could in theory map to one Sofascore UT after
        manual override edits — pick the most reliable).
        """
        row = await executor.fetchrow(
            """
            SELECT sr_competition_id, sr_competition_name, sport_slug,
                   reconciliation_method, reconciliation_confidence,
                   coverage_attrs, tier, last_changed
            FROM sportradar_coverage
            WHERE unique_tournament_id = $1
            ORDER BY reconciliation_confidence DESC, last_changed DESC
            LIMIT 1
            """,
            unique_tournament_id,
        )
        if row is None:
            return None
        attrs_raw = row["coverage_attrs"]
        if isinstance(attrs_raw, str):
            try:
                attrs = json.loads(attrs_raw)
            except (TypeError, ValueError):
                attrs = {}
        else:
            attrs = dict(attrs_raw or {})
        return {
            "sr_competition_id": row["sr_competition_id"],
            "sr_competition_name": row["sr_competition_name"],
            "sport_slug": row["sport_slug"],
            "reconciliation_method": row["reconciliation_method"],
            "reconciliation_confidence": float(row["reconciliation_confidence"]),
            "coverage_attrs": attrs,
            "tier": row["tier"],
            "last_changed": row["last_changed"],
        }

    async def count_by_method(self, executor: SqlExecutor) -> dict[str, int]:
        rows = await executor.fetch(
            """
            SELECT reconciliation_method, COUNT(*)::int AS n
            FROM sportradar_coverage
            GROUP BY reconciliation_method
            """
        )
        return {row["reconciliation_method"]: int(row["n"]) for row in rows}

    async def get_unmatched(
        self, executor: SqlExecutor, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        rows = await executor.fetch(
            """
            SELECT sr_competition_id, sr_competition_name, sr_category_name
            FROM sportradar_coverage
            WHERE reconciliation_method = 'unmatched'
            ORDER BY sr_competition_id
            LIMIT $1
            """,
            limit,
        )
        return [dict(row) for row in rows]


class SportradarCoverageLookup:
    """Adapter satisfying SportradarCoverageGate._CoverageLookup protocol.

    Wraps a Database (or a connection) to expose ``fetch_for_ut`` as the
    gate expects. Read-only.
    """

    def __init__(self, database: Any) -> None:
        self._database = database
        self._repo = SportradarCoverageRepository()

    async def fetch_for_ut(self, ut_id: int) -> dict[str, Any] | None:
        async with self._database.connection() as conn:
            return await self._repo.fetch_coverage_for_ut(conn, ut_id)


__all__ = [
    "SportradarCoverageLookup",
    "SportradarCoverageRecord",
    "SportradarCoverageRepository",
]
