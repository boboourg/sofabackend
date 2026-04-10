"""PostgreSQL repository for season-statistics ETL bundles."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Protocol

from .event_list_repository import _executemany, _jsonb, _timestamp
from .statistics_parser import StatisticsBundle


class SqlExecutor(Protocol):
    async def executemany(self, command: str, args: Iterable[tuple[Any, ...]]) -> Any: ...

    async def fetchval(self, query: str, *args: Any) -> Any: ...


@dataclass(frozen=True)
class StatisticsWriteResult:
    endpoint_registry_rows: int
    payload_snapshot_rows: int
    sport_rows: int
    team_rows: int
    player_rows: int
    config_rows: int
    config_team_rows: int
    nationality_rows: int
    group_item_rows: int
    snapshot_rows: int
    result_rows: int


class StatisticsRepository:
    """Writes normalized season-statistics data into PostgreSQL."""

    async def upsert_bundle(self, executor: SqlExecutor, bundle: StatisticsBundle) -> StatisticsWriteResult:
        await self._upsert_endpoint_registry(executor, bundle)
        await self._upsert_sports(executor, bundle)
        await self._upsert_teams(executor, bundle)
        await self._upsert_players(executor, bundle)
        await self._upsert_configs(executor, bundle)
        await self._upsert_config_teams(executor, bundle)
        await self._upsert_nationalities(executor, bundle)
        await self._upsert_group_items(executor, bundle)
        await self._insert_payload_snapshots(executor, bundle)
        result_rows = await self._insert_statistics_snapshots(executor, bundle)

        return StatisticsWriteResult(
            endpoint_registry_rows=len(bundle.registry_entries),
            payload_snapshot_rows=len(bundle.payload_snapshots),
            sport_rows=len(bundle.sports),
            team_rows=len(bundle.teams),
            player_rows=len(bundle.players),
            config_rows=len(bundle.configs),
            config_team_rows=len(bundle.config_teams),
            nationality_rows=len(bundle.nationalities),
            group_item_rows=len(bundle.group_items),
            snapshot_rows=len(bundle.snapshots),
            result_rows=result_rows,
        )

    async def _upsert_endpoint_registry(self, executor: SqlExecutor, bundle: StatisticsBundle) -> None:
        rows = [
            (
                item.pattern,
                item.path_template,
                item.query_template,
                item.envelope_key,
                item.target_table,
                item.notes,
            )
            for item in bundle.registry_entries
        ]
        await _executemany(
            executor,
            """
            INSERT INTO endpoint_registry (pattern, path_template, query_template, envelope_key, target_table, notes)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (pattern) DO UPDATE SET
                path_template = EXCLUDED.path_template,
                query_template = EXCLUDED.query_template,
                envelope_key = EXCLUDED.envelope_key,
                target_table = EXCLUDED.target_table,
                notes = EXCLUDED.notes
            """,
            rows,
        )

    async def _upsert_sports(self, executor: SqlExecutor, bundle: StatisticsBundle) -> None:
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

    async def _upsert_teams(self, executor: SqlExecutor, bundle: StatisticsBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.short_name,
                item.name_code,
                item.sport_id,
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
                id, slug, name, short_name, name_code, sport_id, gender, type,
                national, disabled, user_count, team_colors, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                short_name = COALESCE(EXCLUDED.short_name, team.short_name),
                name_code = COALESCE(EXCLUDED.name_code, team.name_code),
                sport_id = COALESCE(EXCLUDED.sport_id, team.sport_id),
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

    async def _upsert_players(self, executor: SqlExecutor, bundle: StatisticsBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.short_name,
                item.team_id,
                item.gender,
                item.user_count,
                _jsonb(item.field_translations),
            )
            for item in bundle.players
        ]
        await _executemany(
            executor,
            """
            INSERT INTO player (
                id, slug, name, short_name, team_id, gender, user_count, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = COALESCE(EXCLUDED.slug, player.slug),
                name = EXCLUDED.name,
                short_name = COALESCE(EXCLUDED.short_name, player.short_name),
                team_id = COALESCE(EXCLUDED.team_id, player.team_id),
                gender = COALESCE(EXCLUDED.gender, player.gender),
                user_count = COALESCE(EXCLUDED.user_count, player.user_count),
                field_translations = COALESCE(EXCLUDED.field_translations, player.field_translations)
            """,
            rows,
        )

    async def _upsert_configs(self, executor: SqlExecutor, bundle: StatisticsBundle) -> None:
        rows = [
            (item.unique_tournament_id, item.season_id, item.hide_home_and_away)
            for item in bundle.configs
        ]
        await _executemany(
            executor,
            """
            INSERT INTO season_statistics_config (unique_tournament_id, season_id, hide_home_and_away)
            VALUES ($1, $2, $3)
            ON CONFLICT (unique_tournament_id, season_id) DO UPDATE SET
                hide_home_and_away = EXCLUDED.hide_home_and_away
            """,
            rows,
        )

    async def _upsert_config_teams(self, executor: SqlExecutor, bundle: StatisticsBundle) -> None:
        rows = [
            (item.unique_tournament_id, item.season_id, item.team_id, item.ordinal)
            for item in bundle.config_teams
        ]
        await _executemany(
            executor,
            """
            INSERT INTO season_statistics_config_team (
                unique_tournament_id, season_id, team_id, ordinal
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (unique_tournament_id, season_id, team_id) DO UPDATE SET
                ordinal = EXCLUDED.ordinal
            """,
            rows,
        )

    async def _upsert_nationalities(self, executor: SqlExecutor, bundle: StatisticsBundle) -> None:
        rows = [
            (item.unique_tournament_id, item.season_id, item.nationality_code, item.nationality_name)
            for item in bundle.nationalities
        ]
        await _executemany(
            executor,
            """
            INSERT INTO season_statistics_nationality (
                unique_tournament_id, season_id, nationality_code, nationality_name
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (unique_tournament_id, season_id, nationality_code) DO UPDATE SET
                nationality_name = EXCLUDED.nationality_name
            """,
            rows,
        )

    async def _upsert_group_items(self, executor: SqlExecutor, bundle: StatisticsBundle) -> None:
        rows = [
            (
                item.unique_tournament_id,
                item.season_id,
                item.group_scope,
                item.group_name,
                item.stat_field,
                item.ordinal,
            )
            for item in bundle.group_items
        ]
        await _executemany(
            executor,
            """
            INSERT INTO season_statistics_group_item (
                unique_tournament_id, season_id, group_scope, group_name, stat_field, ordinal
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (unique_tournament_id, season_id, group_scope, group_name, stat_field) DO UPDATE SET
                ordinal = EXCLUDED.ordinal
            """,
            rows,
        )

    async def _insert_payload_snapshots(self, executor: SqlExecutor, bundle: StatisticsBundle) -> None:
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

    async def _insert_statistics_snapshots(self, executor: SqlExecutor, bundle: StatisticsBundle) -> int:
        total_result_rows = 0
        for snapshot in bundle.snapshots:
            snapshot_id = await executor.fetchval(
                """
                INSERT INTO season_statistics_snapshot (
                    endpoint_pattern, unique_tournament_id, season_id, source_url, page, pages,
                    limit_value, offset_value, order_code, accumulation, group_code, fields, filters, fetched_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb, $14::timestamptz
                )
                RETURNING id
                """,
                snapshot.endpoint_pattern,
                snapshot.unique_tournament_id,
                snapshot.season_id,
                snapshot.source_url,
                snapshot.page,
                snapshot.pages,
                snapshot.limit_value,
                snapshot.offset_value,
                snapshot.order_code,
                snapshot.accumulation,
                snapshot.group_code,
                _jsonb(snapshot.fields),
                _jsonb(snapshot.filters),
                _timestamp(snapshot.fetched_at),
            )

            rows = [
                (
                    snapshot_id,
                    item.row_number,
                    item.player_id,
                    item.team_id,
                    item.accurate_passes,
                    item.accurate_passes_percentage,
                    item.assists,
                    item.big_chances_created,
                    item.big_chances_missed,
                    item.blocked_shots,
                    item.clean_sheet,
                    item.clearances,
                    item.dribbled_past,
                    item.error_lead_to_goal,
                    item.error_lead_to_shot,
                    item.expected_goals,
                    item.goal_conversion_percentage,
                    item.goals,
                    item.goals_from_outside_the_box,
                    item.hit_woodwork,
                    item.interceptions,
                    item.key_passes,
                    item.outfielder_blocks,
                    item.own_goals,
                    item.penalty_conceded,
                    item.penalty_save,
                    item.penalty_won,
                    item.rating,
                    item.runs_out,
                    item.saved_shots_from_inside_the_box,
                    item.saves,
                    item.successful_dribbles,
                    item.successful_dribbles_percentage,
                    item.tackles,
                    item.total_shots,
                )
                for item in snapshot.results
            ]
            await _executemany(
                executor,
                """
                INSERT INTO season_statistics_result (
                    snapshot_id, row_number, player_id, team_id, accurate_passes, accurate_passes_percentage,
                    assists, big_chances_created, big_chances_missed, blocked_shots, clean_sheet, clearances,
                    dribbled_past, error_lead_to_goal, error_lead_to_shot, expected_goals, goal_conversion_percentage,
                    goals, goals_from_outside_the_box, hit_woodwork, interceptions, key_passes, outfielder_blocks,
                    own_goals, penalty_conceded, penalty_save, penalty_won, rating, runs_out,
                    saved_shots_from_inside_the_box, saves, successful_dribbles, successful_dribbles_percentage,
                    tackles, total_shots
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                    $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                    $31, $32, $33, $34, $35
                )
                ON CONFLICT (snapshot_id, row_number) DO UPDATE SET
                    player_id = EXCLUDED.player_id,
                    team_id = EXCLUDED.team_id,
                    accurate_passes = EXCLUDED.accurate_passes,
                    accurate_passes_percentage = EXCLUDED.accurate_passes_percentage,
                    assists = EXCLUDED.assists,
                    big_chances_created = EXCLUDED.big_chances_created,
                    big_chances_missed = EXCLUDED.big_chances_missed,
                    blocked_shots = EXCLUDED.blocked_shots,
                    clean_sheet = EXCLUDED.clean_sheet,
                    clearances = EXCLUDED.clearances,
                    dribbled_past = EXCLUDED.dribbled_past,
                    error_lead_to_goal = EXCLUDED.error_lead_to_goal,
                    error_lead_to_shot = EXCLUDED.error_lead_to_shot,
                    expected_goals = EXCLUDED.expected_goals,
                    goal_conversion_percentage = EXCLUDED.goal_conversion_percentage,
                    goals = EXCLUDED.goals,
                    goals_from_outside_the_box = EXCLUDED.goals_from_outside_the_box,
                    hit_woodwork = EXCLUDED.hit_woodwork,
                    interceptions = EXCLUDED.interceptions,
                    key_passes = EXCLUDED.key_passes,
                    outfielder_blocks = EXCLUDED.outfielder_blocks,
                    own_goals = EXCLUDED.own_goals,
                    penalty_conceded = EXCLUDED.penalty_conceded,
                    penalty_save = EXCLUDED.penalty_save,
                    penalty_won = EXCLUDED.penalty_won,
                    rating = EXCLUDED.rating,
                    runs_out = EXCLUDED.runs_out,
                    saved_shots_from_inside_the_box = EXCLUDED.saved_shots_from_inside_the_box,
                    saves = EXCLUDED.saves,
                    successful_dribbles = EXCLUDED.successful_dribbles,
                    successful_dribbles_percentage = EXCLUDED.successful_dribbles_percentage,
                    tackles = EXCLUDED.tackles,
                    total_shots = EXCLUDED.total_shots
                """,
                rows,
            )
            total_result_rows += len(rows)
        return total_result_rows
