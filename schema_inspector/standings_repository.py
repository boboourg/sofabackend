"""PostgreSQL repository for standings ETL bundles."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .event_list_repository import SqlExecutor, _executemany, _jsonb, _timestamp
from .standings_parser import StandingsBundle
from .storage.raw_repository import RawRepository


_RAW_REPOSITORY = RawRepository()


@dataclass(frozen=True)
class StandingsWriteResult:
    endpoint_registry_rows: int
    payload_snapshot_rows: int
    sport_rows: int
    country_rows: int
    category_rows: int
    unique_tournament_rows: int
    tournament_rows: int
    team_rows: int
    tie_breaking_rule_rows: int
    promotion_rows: int
    standing_rows: int
    standing_row_rows: int


class StandingsRepository:
    """Writes normalized standings data into PostgreSQL."""

    async def upsert_bundle(self, executor: SqlExecutor, bundle: StandingsBundle) -> StandingsWriteResult:
        await self._upsert_endpoint_registry(executor, bundle)
        await self._upsert_sports(executor, bundle)
        await self._upsert_countries(executor, bundle)
        await self._upsert_categories(executor, bundle)
        await self._upsert_unique_tournaments(executor, bundle)
        await self._upsert_tournaments(executor, bundle)
        await self._upsert_teams(executor, bundle)
        await self._upsert_tie_breaking_rules(executor, bundle)
        await self._upsert_promotions(executor, bundle)
        await self._upsert_standings(executor, bundle)
        await self._upsert_standing_rows(executor, bundle)
        await self._insert_payload_snapshots(executor, bundle)

        return StandingsWriteResult(
            endpoint_registry_rows=len(bundle.registry_entries),
            payload_snapshot_rows=len(bundle.payload_snapshots),
            sport_rows=len(bundle.sports),
            country_rows=len(bundle.countries),
            category_rows=len(bundle.categories),
            unique_tournament_rows=len(bundle.unique_tournaments),
            tournament_rows=len(bundle.tournaments),
            team_rows=len(bundle.teams),
            tie_breaking_rule_rows=len(bundle.tie_breaking_rules),
            promotion_rows=len(bundle.promotions),
            standing_rows=len(bundle.standings),
            standing_row_rows=len(bundle.standing_rows),
        )

    async def _upsert_endpoint_registry(self, executor: SqlExecutor, bundle: StandingsBundle) -> None:
        await _RAW_REPOSITORY.upsert_endpoint_registry_entries(executor, bundle.registry_entries)

    async def _upsert_sports(self, executor: SqlExecutor, bundle: StandingsBundle) -> None:
        rows = [(item.id, item.slug, item.name) for item in bundle.sports]
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

    async def _upsert_countries(self, executor: SqlExecutor, bundle: StandingsBundle) -> None:
        rows = [(item.alpha2, item.alpha3, item.slug, item.name) for item in bundle.countries]
        await _executemany(
            executor,
            """
            INSERT INTO country (alpha2, alpha3, slug, name)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (alpha2) DO UPDATE SET
                alpha3 = COALESCE(EXCLUDED.alpha3, country.alpha3),
                slug = COALESCE(EXCLUDED.slug, country.slug),
                name = EXCLUDED.name
            """,
            rows,
        )

    async def _upsert_categories(self, executor: SqlExecutor, bundle: StandingsBundle) -> None:
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
                id, slug, name, flag, alpha2, priority, sport_id, country_alpha2, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                flag = COALESCE(EXCLUDED.flag, category.flag),
                alpha2 = COALESCE(EXCLUDED.alpha2, category.alpha2),
                priority = COALESCE(EXCLUDED.priority, category.priority),
                sport_id = EXCLUDED.sport_id,
                country_alpha2 = COALESCE(EXCLUDED.country_alpha2, category.country_alpha2),
                field_translations = COALESCE(EXCLUDED.field_translations, category.field_translations)
            """,
            rows,
        )

    async def _upsert_unique_tournaments(self, executor: SqlExecutor, bundle: StandingsBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.category_id,
                item.country_alpha2,
                item.gender,
                item.primary_color_hex,
                item.secondary_color_hex,
                item.user_count,
                item.has_performance_graph_feature,
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
                id, slug, name, category_id, country_alpha2, gender, primary_color_hex,
                secondary_color_hex, user_count, has_performance_graph_feature,
                display_inverse_home_away_teams, field_translations, period_length
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                category_id = EXCLUDED.category_id,
                country_alpha2 = COALESCE(EXCLUDED.country_alpha2, unique_tournament.country_alpha2),
                gender = COALESCE(EXCLUDED.gender, unique_tournament.gender),
                primary_color_hex = COALESCE(EXCLUDED.primary_color_hex, unique_tournament.primary_color_hex),
                secondary_color_hex = COALESCE(EXCLUDED.secondary_color_hex, unique_tournament.secondary_color_hex),
                user_count = COALESCE(EXCLUDED.user_count, unique_tournament.user_count),
                has_performance_graph_feature = COALESCE(
                    EXCLUDED.has_performance_graph_feature,
                    unique_tournament.has_performance_graph_feature
                ),
                display_inverse_home_away_teams = COALESCE(
                    EXCLUDED.display_inverse_home_away_teams,
                    unique_tournament.display_inverse_home_away_teams
                ),
                field_translations = COALESCE(EXCLUDED.field_translations, unique_tournament.field_translations),
                period_length = COALESCE(EXCLUDED.period_length, unique_tournament.period_length)
            """,
            rows,
        )

    async def _upsert_tournaments(self, executor: SqlExecutor, bundle: StandingsBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.category_id,
                item.unique_tournament_id,
                item.group_name,
                item.group_sign,
                item.is_group,
                item.is_live,
                item.priority,
                _jsonb(item.field_translations),
            )
            for item in bundle.tournaments
        ]
        await _executemany(
            executor,
            """
            INSERT INTO tournament (
                id, slug, name, category_id, unique_tournament_id, group_name,
                group_sign, is_group, is_live, priority, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                category_id = EXCLUDED.category_id,
                unique_tournament_id = COALESCE(EXCLUDED.unique_tournament_id, tournament.unique_tournament_id),
                group_name = COALESCE(EXCLUDED.group_name, tournament.group_name),
                group_sign = COALESCE(EXCLUDED.group_sign, tournament.group_sign),
                is_group = COALESCE(EXCLUDED.is_group, tournament.is_group),
                is_live = COALESCE(EXCLUDED.is_live, tournament.is_live),
                priority = COALESCE(EXCLUDED.priority, tournament.priority),
                field_translations = COALESCE(EXCLUDED.field_translations, tournament.field_translations)
            """,
            rows,
        )

    async def _upsert_teams(self, executor: SqlExecutor, bundle: StandingsBundle) -> None:
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
                id, slug, name, short_name, name_code, sport_id, country_alpha2, gender,
                type, national, disabled, user_count, team_colors, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                short_name = COALESCE(EXCLUDED.short_name, team.short_name),
                name_code = COALESCE(EXCLUDED.name_code, team.name_code),
                sport_id = COALESCE(EXCLUDED.sport_id, team.sport_id),
                country_alpha2 = COALESCE(EXCLUDED.country_alpha2, team.country_alpha2),
                gender = COALESCE(EXCLUDED.gender, team.gender),
                type = COALESCE(EXCLUDED.type, team.type),
                national = COALESCE(EXCLUDED.national, team.national),
                disabled = COALESCE(EXCLUDED.disabled, team.disabled),
                user_count = COALESCE(EXCLUDED.user_count, team.user_count),
                team_colors = COALESCE(EXCLUDED.team_colors, team.team_colors),
                field_translations = COALESCE(EXCLUDED.field_translations, team.field_translations)
            """,
            rows,
        )

    async def _upsert_tie_breaking_rules(self, executor: SqlExecutor, bundle: StandingsBundle) -> None:
        rows = [(item.id, item.text) for item in bundle.tie_breaking_rules]
        await _executemany(
            executor,
            """
            INSERT INTO standing_tie_breaking_rule (id, text)
            VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE SET
                text = EXCLUDED.text
            """,
            rows,
        )

    async def _upsert_promotions(self, executor: SqlExecutor, bundle: StandingsBundle) -> None:
        rows = [(item.id, item.text) for item in bundle.promotions]
        await _executemany(
            executor,
            """
            INSERT INTO standing_promotion (id, text)
            VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE SET
                text = EXCLUDED.text
            """,
            rows,
        )

    async def _upsert_standings(self, executor: SqlExecutor, bundle: StandingsBundle) -> None:
        rows = [
            (
                item.id,
                item.season_id,
                item.tournament_id,
                item.name,
                item.type,
                item.updated_at_timestamp,
                item.tie_breaking_rule_id,
                _jsonb(item.descriptions),
            )
            for item in bundle.standings
        ]
        await _executemany(
            executor,
            """
            INSERT INTO standing (
                id, season_id, tournament_id, name, type, updated_at_timestamp,
                tie_breaking_rule_id, descriptions
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                season_id = EXCLUDED.season_id,
                tournament_id = EXCLUDED.tournament_id,
                name = EXCLUDED.name,
                type = EXCLUDED.type,
                updated_at_timestamp = EXCLUDED.updated_at_timestamp,
                tie_breaking_rule_id = EXCLUDED.tie_breaking_rule_id,
                descriptions = EXCLUDED.descriptions
            """,
            rows,
        )

    async def _upsert_standing_rows(self, executor: SqlExecutor, bundle: StandingsBundle) -> None:
        rows = [
            (
                item.id,
                item.standing_id,
                item.team_id,
                item.position,
                item.matches,
                item.wins,
                item.draws,
                item.losses,
                item.points,
                item.scores_for,
                item.scores_against,
                item.score_diff_formatted,
                item.promotion_id,
                _jsonb(item.descriptions),
            )
            for item in bundle.standing_rows
        ]
        await _executemany(
            executor,
            """
            INSERT INTO standing_row (
                id, standing_id, team_id, position, matches, wins, draws, losses,
                points, scores_for, scores_against, score_diff_formatted, promotion_id, descriptions
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                standing_id = EXCLUDED.standing_id,
                team_id = EXCLUDED.team_id,
                position = EXCLUDED.position,
                matches = EXCLUDED.matches,
                wins = EXCLUDED.wins,
                draws = EXCLUDED.draws,
                losses = EXCLUDED.losses,
                points = EXCLUDED.points,
                scores_for = EXCLUDED.scores_for,
                scores_against = EXCLUDED.scores_against,
                score_diff_formatted = EXCLUDED.score_diff_formatted,
                promotion_id = EXCLUDED.promotion_id,
                descriptions = EXCLUDED.descriptions
            """,
            rows,
        )

    async def _insert_payload_snapshots(self, executor: SqlExecutor, bundle: StandingsBundle) -> None:
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
                endpoint_pattern, source_url, envelope_key, context_entity_type, context_entity_id, payload, fetched_at
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::timestamptz)
            """,
            rows,
        )
