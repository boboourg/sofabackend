"""PostgreSQL repository for competition-family ETL bundles."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Protocol

from .db import register_post_commit_hook
from .competition_parser import CompetitionBundle
from .storage.raw_repository import RawRepository


_RAW_REPOSITORY = RawRepository()


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: Any) -> Any: ...

    async def executemany(self, command: str, args: Iterable[tuple[Any, ...]]) -> Any: ...


@dataclass(frozen=True)
class CompetitionWriteResult:
    endpoint_registry_rows: int
    payload_snapshot_rows: int
    image_asset_rows: int
    sport_rows: int
    country_rows: int
    category_rows: int
    team_rows: int
    unique_tournament_rows: int
    unique_tournament_relation_rows: int
    unique_tournament_most_title_team_rows: int
    season_rows: int
    unique_tournament_season_rows: int


class CompetitionRepository:
    """Writes normalized competition data into PostgreSQL."""

    def __init__(self) -> None:
        self._sport_cache: dict[int, tuple[str | None, str | None]] = {}
        self._country_cache: dict[str, tuple[str | None, str | None, str | None]] = {}

    async def upsert_bundle(self, executor: SqlExecutor, bundle: CompetitionBundle) -> CompetitionWriteResult:
        await self._upsert_endpoint_registry(executor, bundle)
        await self._upsert_image_assets(executor, bundle)
        await self._upsert_sports(executor, bundle)
        await self._upsert_countries(executor, bundle)
        await self._upsert_categories(executor, bundle)
        await self._upsert_teams(executor, bundle)
        await self._upsert_unique_tournaments(executor, bundle)
        await self._upsert_seasons(executor, bundle)
        await self._upsert_unique_tournament_seasons(executor, bundle)
        await self._upsert_unique_tournament_relations(executor, bundle)
        await self._upsert_unique_tournament_most_title_teams(executor, bundle)
        await self._insert_payload_snapshots(executor, bundle)

        return CompetitionWriteResult(
            endpoint_registry_rows=len(bundle.registry_entries),
            payload_snapshot_rows=len(bundle.payload_snapshots),
            image_asset_rows=len(bundle.image_assets),
            sport_rows=len(bundle.sports),
            country_rows=len(bundle.countries),
            category_rows=len(bundle.categories),
            team_rows=len(bundle.teams),
            unique_tournament_rows=len(bundle.unique_tournaments),
            unique_tournament_relation_rows=len(bundle.unique_tournament_relations),
            unique_tournament_most_title_team_rows=len(bundle.unique_tournament_most_title_teams),
            season_rows=len(bundle.seasons),
            unique_tournament_season_rows=len(bundle.unique_tournament_seasons),
        )

    async def _upsert_endpoint_registry(self, executor: SqlExecutor, bundle: CompetitionBundle) -> None:
        await _RAW_REPOSITORY.upsert_endpoint_registry_entries(executor, bundle.registry_entries)

    async def _upsert_image_assets(self, executor: SqlExecutor, bundle: CompetitionBundle) -> None:
        rows = [(item.id, item.md5) for item in bundle.image_assets]
        await _executemany(
            executor,
            """
            INSERT INTO image_asset (id, md5)
            VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE SET
                md5 = EXCLUDED.md5
            """,
            rows,
        )

    async def _upsert_sports(self, executor: SqlExecutor, bundle: CompetitionBundle) -> None:
        rows_by_id = {
            int(item.id): (item.id, item.slug, item.name)
            for item in bundle.sports
        }
        rows = [
            row
            for sport_id, row in rows_by_id.items()
            if self._sport_cache.get(sport_id) != (row[1], row[2])
        ]
        await _executemany(
            executor,
            """
            INSERT INTO sport (id, slug, name)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name
            """,
            rows,
        )
        for sport_id, row in rows_by_id.items():
            self._sport_cache[sport_id] = (row[1], row[2])

    async def _upsert_countries(self, executor: SqlExecutor, bundle: CompetitionBundle) -> None:
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

    async def _upsert_categories(self, executor: SqlExecutor, bundle: CompetitionBundle) -> None:
        rows = [
            (
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
        ]
        await _executemany(
            executor,
            """
            INSERT INTO category (
                id,
                slug,
                name,
                flag,
                alpha2,
                priority,
                sport_id,
                country_alpha2,
                field_translations
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

    async def _upsert_teams(self, executor: SqlExecutor, bundle: CompetitionBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.short_name,
                item.name_code,
                item.sport_id,
                item.country_alpha2,
                item.gender,
                item.type,
                item.national,
                item.disabled,
                item.user_count,
                _jsonb(item.team_colors),
                _jsonb(item.field_translations),
            )
            for item in bundle.teams
        ]
        await _executemany(
            executor,
            """
            INSERT INTO team (
                id,
                slug,
                name,
                short_name,
                name_code,
                sport_id,
                country_alpha2,
                gender,
                type,
                national,
                disabled,
                user_count,
                team_colors,
                field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                short_name = EXCLUDED.short_name,
                name_code = EXCLUDED.name_code,
                sport_id = EXCLUDED.sport_id,
                country_alpha2 = EXCLUDED.country_alpha2,
                gender = EXCLUDED.gender,
                type = EXCLUDED.type,
                national = EXCLUDED.national,
                disabled = EXCLUDED.disabled,
                user_count = EXCLUDED.user_count,
                team_colors = EXCLUDED.team_colors,
                field_translations = EXCLUDED.field_translations
            """,
            rows,
        )

    async def _upsert_unique_tournaments(self, executor: SqlExecutor, bundle: CompetitionBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.category_id,
                item.country_alpha2,
                item.logo_asset_id,
                item.dark_logo_asset_id,
                item.title_holder_team_id,
                item.title_holder_titles,
                item.most_titles,
                item.gender,
                item.primary_color_hex,
                item.secondary_color_hex,
                item.start_date_timestamp,
                item.end_date_timestamp,
                item.tier,
                item.user_count,
                item.has_rounds,
                item.has_groups,
                item.has_event_player_statistics,
                item.has_performance_graph_feature,
                item.has_playoff_series,
                item.disabled_home_away_standings,
                item.display_inverse_home_away_teams,
                _jsonb(item.field_translations),
                _jsonb(item.period_length),
            )
            for item in bundle.unique_tournaments
        ]
        await _executemany(
            executor,
            """
            INSERT INTO unique_tournament (
                id,
                slug,
                name,
                category_id,
                country_alpha2,
                logo_asset_id,
                dark_logo_asset_id,
                title_holder_team_id,
                title_holder_titles,
                most_titles,
                gender,
                primary_color_hex,
                secondary_color_hex,
                start_date_timestamp,
                end_date_timestamp,
                tier,
                user_count,
                has_rounds,
                has_groups,
                has_event_player_statistics,
                has_performance_graph_feature,
                has_playoff_series,
                disabled_home_away_standings,
                display_inverse_home_away_teams,
                field_translations,
                period_length
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25::jsonb, $26::jsonb
            )
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                category_id = EXCLUDED.category_id,
                country_alpha2 = EXCLUDED.country_alpha2,
                logo_asset_id = EXCLUDED.logo_asset_id,
                dark_logo_asset_id = EXCLUDED.dark_logo_asset_id,
                title_holder_team_id = EXCLUDED.title_holder_team_id,
                title_holder_titles = EXCLUDED.title_holder_titles,
                most_titles = EXCLUDED.most_titles,
                gender = EXCLUDED.gender,
                primary_color_hex = EXCLUDED.primary_color_hex,
                secondary_color_hex = EXCLUDED.secondary_color_hex,
                start_date_timestamp = EXCLUDED.start_date_timestamp,
                end_date_timestamp = EXCLUDED.end_date_timestamp,
                tier = EXCLUDED.tier,
                user_count = EXCLUDED.user_count,
                has_rounds = EXCLUDED.has_rounds,
                has_groups = EXCLUDED.has_groups,
                has_event_player_statistics = EXCLUDED.has_event_player_statistics,
                has_performance_graph_feature = EXCLUDED.has_performance_graph_feature,
                has_playoff_series = EXCLUDED.has_playoff_series,
                disabled_home_away_standings = EXCLUDED.disabled_home_away_standings,
                display_inverse_home_away_teams = EXCLUDED.display_inverse_home_away_teams,
                field_translations = EXCLUDED.field_translations,
                period_length = EXCLUDED.period_length
            WHERE unique_tournament.slug IS DISTINCT FROM EXCLUDED.slug
               OR unique_tournament.name IS DISTINCT FROM EXCLUDED.name
               OR unique_tournament.category_id IS DISTINCT FROM EXCLUDED.category_id
               OR unique_tournament.country_alpha2 IS DISTINCT FROM EXCLUDED.country_alpha2
               OR unique_tournament.logo_asset_id IS DISTINCT FROM EXCLUDED.logo_asset_id
               OR unique_tournament.dark_logo_asset_id IS DISTINCT FROM EXCLUDED.dark_logo_asset_id
               OR unique_tournament.title_holder_team_id IS DISTINCT FROM EXCLUDED.title_holder_team_id
               OR unique_tournament.title_holder_titles IS DISTINCT FROM EXCLUDED.title_holder_titles
               OR unique_tournament.most_titles IS DISTINCT FROM EXCLUDED.most_titles
               OR unique_tournament.gender IS DISTINCT FROM EXCLUDED.gender
               OR unique_tournament.primary_color_hex IS DISTINCT FROM EXCLUDED.primary_color_hex
               OR unique_tournament.secondary_color_hex IS DISTINCT FROM EXCLUDED.secondary_color_hex
               OR unique_tournament.start_date_timestamp IS DISTINCT FROM EXCLUDED.start_date_timestamp
               OR unique_tournament.end_date_timestamp IS DISTINCT FROM EXCLUDED.end_date_timestamp
               OR unique_tournament.tier IS DISTINCT FROM EXCLUDED.tier
               OR unique_tournament.user_count IS DISTINCT FROM EXCLUDED.user_count
               OR unique_tournament.has_rounds IS DISTINCT FROM EXCLUDED.has_rounds
               OR unique_tournament.has_groups IS DISTINCT FROM EXCLUDED.has_groups
               OR unique_tournament.has_event_player_statistics IS DISTINCT FROM EXCLUDED.has_event_player_statistics
               OR unique_tournament.has_performance_graph_feature IS DISTINCT FROM EXCLUDED.has_performance_graph_feature
               OR unique_tournament.has_playoff_series IS DISTINCT FROM EXCLUDED.has_playoff_series
               OR unique_tournament.disabled_home_away_standings IS DISTINCT FROM EXCLUDED.disabled_home_away_standings
               OR unique_tournament.display_inverse_home_away_teams IS DISTINCT FROM EXCLUDED.display_inverse_home_away_teams
               OR unique_tournament.field_translations IS DISTINCT FROM EXCLUDED.field_translations
               OR unique_tournament.period_length IS DISTINCT FROM EXCLUDED.period_length
            """,
            rows,
        )

    async def _upsert_seasons(self, executor: SqlExecutor, bundle: CompetitionBundle) -> None:
        rows = [(item.id, item.name, item.year, item.editor) for item in bundle.seasons]
        await _executemany(
            executor,
            """
            INSERT INTO season (id, name, year, editor)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                year = EXCLUDED.year,
                editor = EXCLUDED.editor
            WHERE season.name IS DISTINCT FROM EXCLUDED.name
               OR season.year IS DISTINCT FROM EXCLUDED.year
               OR season.editor IS DISTINCT FROM EXCLUDED.editor
            """,
            rows,
        )

    async def _upsert_unique_tournament_seasons(self, executor: SqlExecutor, bundle: CompetitionBundle) -> None:
        rows = [(item.unique_tournament_id, item.season_id) for item in bundle.unique_tournament_seasons]
        await _executemany(
            executor,
            """
            INSERT INTO unique_tournament_season (unique_tournament_id, season_id)
            VALUES ($1, $2)
            ON CONFLICT (unique_tournament_id, season_id) DO NOTHING
            """,
            rows,
        )

    async def _upsert_unique_tournament_relations(self, executor: SqlExecutor, bundle: CompetitionBundle) -> None:
        rows = [
            (item.unique_tournament_id, item.related_unique_tournament_id, item.relation_type)
            for item in bundle.unique_tournament_relations
        ]
        await _executemany(
            executor,
            """
            INSERT INTO unique_tournament_relation (
                unique_tournament_id,
                related_unique_tournament_id,
                relation_type
            )
            VALUES ($1, $2, $3)
            ON CONFLICT (unique_tournament_id, related_unique_tournament_id, relation_type) DO NOTHING
            """,
            rows,
        )

    async def _upsert_unique_tournament_most_title_teams(self, executor: SqlExecutor, bundle: CompetitionBundle) -> None:
        rows = [(item.unique_tournament_id, item.team_id) for item in bundle.unique_tournament_most_title_teams]
        await _executemany(
            executor,
            """
            INSERT INTO unique_tournament_most_title_team (unique_tournament_id, team_id)
            VALUES ($1, $2)
            ON CONFLICT (unique_tournament_id, team_id) DO NOTHING
            """,
            rows,
        )

    async def _insert_payload_snapshots(self, executor: SqlExecutor, bundle: CompetitionBundle) -> None:
        rows = [
            (
                item.endpoint_pattern,
                item.source_url,
                item.envelope_key,
                item.context_entity_type,
                item.context_entity_id,
                _jsonb(item.payload),
                _timestamp(item.fetched_at),
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
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::timestamptz)
            """,
            rows,
        )


async def _executemany(executor: SqlExecutor, sql: str, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    await executor.executemany(sql, rows)


def _jsonb(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _timestamp(value: str | datetime | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)
