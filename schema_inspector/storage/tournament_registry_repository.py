"""PostgreSQL repository for tournament registry control-plane rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Protocol

from ._temporal import coerce_timestamptz


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


class SqlBatchExecutor(SqlExecutor, Protocol):
    async def executemany(self, query: str, args: Iterable[tuple[object, ...]]) -> Any: ...


class SqlFetchExecutor(SqlExecutor, Protocol):
    async def fetch(self, query: str, *args: object) -> list[object]: ...


@dataclass(frozen=True)
class TournamentRegistryRecord:
    source_slug: str
    sport_slug: str
    category_id: int
    unique_tournament_id: int
    discovery_surface: str
    priority_rank: int
    is_active: bool
    first_seen_at: str | None = None
    last_seen_at: str | None = None


@dataclass(frozen=True)
class TournamentRegistryTarget:
    source_slug: str
    sport_slug: str
    unique_tournament_id: int
    structure_enabled: bool = True
    current_enabled: bool = True
    live_enabled: bool = True
    historical_enabled: bool = True
    enrichment_enabled: bool = True
    refresh_interval_seconds: int | None = None
    historical_backfill_start_date: date | None = None
    historical_backfill_end_date: date | None = None
    recent_refresh_days: int | None = None


class TournamentRegistryRepository:
    """Writes and reads registry-backed managed tournament targets."""

    _SURFACE_PREDICATES = {
        "structure": "structure_enabled = TRUE",
        "current": "current_enabled = TRUE",
        "live": "live_enabled = TRUE",
        "historical": "historical_enabled = TRUE",
        "enrichment": "enrichment_enabled = TRUE",
    }

    async def upsert_records(
        self,
        executor: SqlExecutor,
        records: Iterable[TournamentRegistryRecord],
    ) -> None:
        rows = [
            (
                record.source_slug,
                record.sport_slug,
                int(record.category_id),
                int(record.unique_tournament_id),
                record.discovery_surface,
                int(record.priority_rank),
                bool(record.is_active),
                coerce_timestamptz(record.first_seen_at),
                coerce_timestamptz(record.last_seen_at),
            )
            for record in records
        ]
        if not rows:
            return
        query = """
            INSERT INTO tournament_registry (
                source_slug,
                sport_slug,
                category_id,
                unique_tournament_id,
                discovery_surface,
                priority_rank,
                is_active,
                first_seen_at,
                last_seen_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                COALESCE($8, now()),
                COALESCE($9, now())
            )
            ON CONFLICT (source_slug, sport_slug, unique_tournament_id) DO UPDATE SET
                category_id = EXCLUDED.category_id,
                discovery_surface = EXCLUDED.discovery_surface,
                priority_rank = EXCLUDED.priority_rank,
                is_active = EXCLUDED.is_active,
                last_seen_at = EXCLUDED.last_seen_at,
                updated_at = now()
        """
        executemany = getattr(executor, "executemany", None)
        if callable(executemany):
            await executemany(query, rows)
            return
        for row in rows:
            await executor.execute(query, *row)

    async def list_active_targets(
        self,
        executor: SqlFetchExecutor,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        surface: str | None = None,
    ) -> tuple[TournamentRegistryTarget, ...]:
        normalized_sports = tuple(str(item).strip().lower() for item in (sport_slugs or ()) if str(item).strip())
        normalized_surface = str(surface or "").strip().lower()
        surface_predicate = self._SURFACE_PREDICATES.get(normalized_surface)
        if normalized_surface and surface_predicate is None:
            raise ValueError(f"Unsupported tournament registry surface: {surface}")
        rows = await executor.fetch(
            f"""
            SELECT
                source_slug,
                sport_slug,
                unique_tournament_id,
                structure_enabled,
                current_enabled,
                live_enabled,
                historical_enabled,
                enrichment_enabled,
                refresh_interval_seconds,
                historical_backfill_start_date,
                historical_backfill_end_date,
                recent_refresh_days
            FROM tournament_registry
            WHERE is_active = TRUE
              AND (
                $2::text = ''
                OR {surface_predicate or 'TRUE'}
              )
              AND (
                cardinality($1::text[]) = 0
                OR sport_slug = ANY($1::text[])
              )
            ORDER BY sport_slug ASC, priority_rank ASC, unique_tournament_id ASC
            """,
            list(normalized_sports),
            normalized_surface,
        )
        return tuple(
            TournamentRegistryTarget(
                source_slug=str(row["source_slug"]),
                sport_slug=str(row["sport_slug"]),
                unique_tournament_id=int(row["unique_tournament_id"]),
                structure_enabled=bool(row["structure_enabled"]),
                current_enabled=bool(row["current_enabled"]),
                live_enabled=bool(row["live_enabled"]),
                historical_enabled=bool(row["historical_enabled"]),
                enrichment_enabled=bool(row["enrichment_enabled"]),
                refresh_interval_seconds=(
                    None
                    if row["refresh_interval_seconds"] is None
                    else int(row["refresh_interval_seconds"])
                ),
                historical_backfill_start_date=row["historical_backfill_start_date"],
                historical_backfill_end_date=row["historical_backfill_end_date"],
                recent_refresh_days=(
                    None
                    if row["recent_refresh_days"] is None
                    else int(row["recent_refresh_days"])
                ),
            )
            for row in rows
        )

    async def has_rows(self, executor: SqlFetchExecutor, *, sport_slug: str) -> bool:
        rows = await executor.fetch(
            """
            SELECT 1
            FROM tournament_registry
            WHERE sport_slug = $1
            LIMIT 1
            """,
            str(sport_slug).strip().lower(),
        )
        return bool(rows)
