"""PostgreSQL repository for daily categories seed bundles."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Protocol

from .db import register_post_commit_hook
from .categories_seed_parser import CategoriesSeedBundle
from .storage.raw_repository import RawRepository


_RAW_REPOSITORY = RawRepository()


class SqlExecutor(Protocol):
    async def executemany(self, command: str, args: Iterable[tuple[Any, ...]]) -> Any: ...


@dataclass(frozen=True)
class CategoriesSeedWriteResult:
    endpoint_registry_rows: int
    payload_snapshot_rows: int
    sport_rows: int
    country_rows: int
    category_rows: int
    daily_summary_rows: int
    daily_unique_tournament_rows: int
    daily_team_rows: int


class CategoriesSeedRepository:
    """Writes categories discovery data into PostgreSQL."""

    def __init__(self) -> None:
        self._country_cache: dict[str, tuple[str | None, str | None, str | None]] = {}
        self._category_cache: dict[
            int,
            tuple[str | None, str | None, str | None, str | None, int | None, int | None, str | None, str | None],
        ] = {}

    async def upsert_bundle(self, executor: SqlExecutor, bundle: CategoriesSeedBundle) -> CategoriesSeedWriteResult:
        await self._upsert_endpoint_registry(executor, bundle)
        await self._upsert_sports(executor, bundle)
        await self._upsert_countries(executor, bundle)
        await self._upsert_categories(executor, bundle)
        await self._upsert_daily_summaries(executor, bundle)
        await self._upsert_daily_unique_tournaments(executor, bundle)
        await self._upsert_daily_teams(executor, bundle)
        await self._insert_payload_snapshots(executor, bundle)

        return CategoriesSeedWriteResult(
            endpoint_registry_rows=len(bundle.registry_entries),
            payload_snapshot_rows=len(bundle.payload_snapshots),
            sport_rows=len(bundle.sports),
            country_rows=len(bundle.countries),
            category_rows=len(bundle.categories),
            daily_summary_rows=len(bundle.daily_summaries),
            daily_unique_tournament_rows=len(bundle.daily_unique_tournaments),
            daily_team_rows=len(bundle.daily_teams),
        )

    async def _upsert_endpoint_registry(self, executor: SqlExecutor, bundle: CategoriesSeedBundle) -> None:
        await _RAW_REPOSITORY.upsert_endpoint_registry_entries(executor, bundle.registry_entries)

    async def _upsert_sports(self, executor: SqlExecutor, bundle: CategoriesSeedBundle) -> None:
        rows = [(item.id, item.slug, item.name) for item in bundle.sports]
        await _executemany(
            executor,
            """
            INSERT INTO sport (id, slug, name)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE SET slug = EXCLUDED.slug, name = EXCLUDED.name
            """,
            rows,
        )

    async def _upsert_countries(self, executor: SqlExecutor, bundle: CategoriesSeedBundle) -> None:
        rows_by_alpha2 = {
            str(item.alpha2): (item.alpha2, item.alpha3, item.slug, item.name)
            for item in bundle.countries
            if item.alpha2 is not None
        }
        rows = [
            row
            for alpha2, row in rows_by_alpha2.items()
            if self._country_cache.get(alpha2) != (row[1], row[2], row[3])
        ]
        await _executemany(
            executor,
            """
            INSERT INTO country (alpha2, alpha3, slug, name)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (alpha2) DO UPDATE SET
                alpha3 = EXCLUDED.alpha3,
                slug = EXCLUDED.slug,
                name = EXCLUDED.name
            WHERE country.alpha3 IS DISTINCT FROM EXCLUDED.alpha3
               OR country.slug IS DISTINCT FROM EXCLUDED.slug
               OR country.name IS DISTINCT FROM EXCLUDED.name
            """,
            rows,
        )
        if rows_by_alpha2:
            def _commit_country_cache() -> None:
                for alpha2, row in rows_by_alpha2.items():
                    self._country_cache[alpha2] = (row[1], row[2], row[3])

            if not register_post_commit_hook(_commit_country_cache):
                _commit_country_cache()

    async def _upsert_categories(self, executor: SqlExecutor, bundle: CategoriesSeedBundle) -> None:
        rows_by_id = {
            int(item.id): (
                item.id,
                item.slug,
                item.name,
                item.flag,
                item.alpha2,
                item.priority,
                item.sport_id,
                item.country_alpha2,
                _jsonb(item.field_translations),
            )
            for item in bundle.categories
        }
        rows = [
            row
            for category_id, row in rows_by_id.items()
            if self._category_cache.get(category_id) != row[1:]
        ]
        await _executemany(
            executor,
            """
            INSERT INTO category (
                id, slug, name, flag, alpha2, priority, sport_id, country_alpha2, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                flag = EXCLUDED.flag,
                alpha2 = EXCLUDED.alpha2,
                priority = EXCLUDED.priority,
                sport_id = EXCLUDED.sport_id,
                country_alpha2 = EXCLUDED.country_alpha2,
                field_translations = EXCLUDED.field_translations
            WHERE category.slug IS DISTINCT FROM EXCLUDED.slug
               OR category.name IS DISTINCT FROM EXCLUDED.name
               OR category.flag IS DISTINCT FROM EXCLUDED.flag
               OR category.alpha2 IS DISTINCT FROM EXCLUDED.alpha2
               OR category.priority IS DISTINCT FROM EXCLUDED.priority
               OR category.sport_id IS DISTINCT FROM EXCLUDED.sport_id
               OR category.country_alpha2 IS DISTINCT FROM EXCLUDED.country_alpha2
               OR category.field_translations IS DISTINCT FROM EXCLUDED.field_translations
            """,
            rows,
        )
        if rows_by_id:
            def _commit_category_cache() -> None:
                for category_id, row in rows_by_id.items():
                    self._category_cache[category_id] = row[1:]

            if not register_post_commit_hook(_commit_category_cache):
                _commit_category_cache()

    async def _upsert_daily_summaries(self, executor: SqlExecutor, bundle: CategoriesSeedBundle) -> None:
        rows = [
            (
                item.observed_date,
                item.timezone_offset_seconds,
                item.category_id,
                item.total_events,
                item.total_event_player_statistics,
                item.total_videos,
            )
            for item in bundle.daily_summaries
        ]
        await _executemany(
            executor,
            """
            INSERT INTO category_daily_summary (
                observed_date,
                timezone_offset_seconds,
                category_id,
                total_events,
                total_event_player_statistics,
                total_videos
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (observed_date, timezone_offset_seconds, category_id) DO UPDATE SET
                total_events = EXCLUDED.total_events,
                total_event_player_statistics = EXCLUDED.total_event_player_statistics,
                total_videos = EXCLUDED.total_videos
            """,
            rows,
        )

    async def _upsert_daily_unique_tournaments(self, executor: SqlExecutor, bundle: CategoriesSeedBundle) -> None:
        rows = [
            (
                item.observed_date,
                item.timezone_offset_seconds,
                item.category_id,
                item.unique_tournament_id,
                item.ordinal,
            )
            for item in bundle.daily_unique_tournaments
        ]
        await _executemany(
            executor,
            """
            INSERT INTO category_daily_unique_tournament (
                observed_date,
                timezone_offset_seconds,
                category_id,
                unique_tournament_id,
                ordinal
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (observed_date, timezone_offset_seconds, category_id, unique_tournament_id) DO UPDATE SET
                ordinal = EXCLUDED.ordinal
            """,
            rows,
        )

    async def _upsert_daily_teams(self, executor: SqlExecutor, bundle: CategoriesSeedBundle) -> None:
        rows = [
            (
                item.observed_date,
                item.timezone_offset_seconds,
                item.category_id,
                item.team_id,
                item.ordinal,
            )
            for item in bundle.daily_teams
        ]
        await _executemany(
            executor,
            """
            INSERT INTO category_daily_team (
                observed_date,
                timezone_offset_seconds,
                category_id,
                team_id,
                ordinal
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (observed_date, timezone_offset_seconds, category_id, team_id) DO UPDATE SET
                ordinal = EXCLUDED.ordinal
            """,
            rows,
        )

    async def _insert_payload_snapshots(self, executor: SqlExecutor, bundle: CategoriesSeedBundle) -> None:
        rows = [
            (
                item.endpoint_pattern,
                item.source_url,
                item.envelope_key,
                item.context_entity_type,
                item.context_entity_id,
                _jsonb(item.payload),
                _parse_timestamptz(item.fetched_at),
            )
            for item in bundle.payload_snapshots
        ]
        await _executemany(
            executor,
            """
            INSERT INTO api_payload_snapshot (
                endpoint_pattern,
                source_url,
                envelope_key,
                context_entity_type,
                context_entity_id,
                payload,
                fetched_at
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
            """,
            rows,
        )


async def _executemany(executor: SqlExecutor, command: str, rows: list[tuple[Any, ...]]) -> None:
    if rows:
        await executor.executemany(command, rows)


def _jsonb(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def _parse_timestamptz(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)
