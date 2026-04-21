"""PostgreSQL repository for season-statistics ETL bundles."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Protocol

from .event_list_repository import _executemany, _jsonb, _timestamp
from .statistics_parser import StatisticsBundle
from .storage.raw_repository import RawRepository


_RAW_REPOSITORY = RawRepository()


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
        await _RAW_REPOSITORY.upsert_endpoint_registry_entries(executor, bundle.registry_entries)

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
                    item.accurate_crosses,
                    item.accurate_crosses_percentage,
                    item.accurate_final_third_passes,
                    item.accurate_long_balls,
                    item.accurate_long_balls_percentage,
                    item.accurate_opposition_half_passes,
                    item.accurate_own_half_passes,
                    item.accurate_passes,
                    item.accurate_passes_percentage,
                    item.aerial_duels_won,
                    item.aerial_duels_won_percentage,
                    item.appearances,
                    item.assists,
                    item.big_chances_created,
                    item.big_chances_missed,
                    item.blocked_shots,
                    item.clean_sheet,
                    item.clearances,
                    item.crosses_not_claimed,
                    item.dispossessed,
                    item.dribbled_past,
                    item.error_lead_to_goal,
                    item.error_lead_to_shot,
                    item.expected_goals,
                    item.fouls,
                    item.free_kick_goal,
                    item.goal_conversion_percentage,
                    item.goals,
                    item.goals_conceded_inside_the_box,
                    item.goals_conceded_outside_the_box,
                    item.goals_from_inside_the_box,
                    item.goals_from_outside_the_box,
                    item.ground_duels_won,
                    item.ground_duels_won_percentage,
                    item.headed_goals,
                    item.high_claims,
                    item.hit_woodwork,
                    item.inaccurate_passes,
                    item.interceptions,
                    item.key_passes,
                    item.left_foot_goals,
                    item.matches_started,
                    item.minutes_played,
                    item.offsides,
                    item.outfielder_blocks,
                    item.own_goals,
                    item.pass_to_assist,
                    item.penalties_taken,
                    item.penalty_conceded,
                    item.penalty_conversion,
                    item.penalty_faced,
                    item.penalty_goals,
                    item.penalty_save,
                    item.penalty_won,
                    item.possession_lost,
                    item.punches,
                    item.rating,
                    item.red_cards,
                    item.right_foot_goals,
                    item.runs_out,
                    item.saved_shots_from_inside_the_box,
                    item.saved_shots_from_outside_the_box,
                    item.saves,
                    item.set_piece_conversion,
                    item.shot_from_set_piece,
                    item.shots_off_target,
                    item.shots_on_target,
                    item.successful_dribbles,
                    item.successful_dribbles_percentage,
                    item.successful_runs_out,
                    item.tackles,
                    item.total_duels_won,
                    item.total_duels_won_percentage,
                    item.total_passes,
                    item.total_shots,
                    item.was_fouled,
                    item.yellow_cards,
                )
                for item in snapshot.results
            ]
            await _executemany(
                executor,
                """
                INSERT INTO season_statistics_result (
                    snapshot_id, row_number, player_id, team_id, accurate_crosses, accurate_crosses_percentage,
                    accurate_final_third_passes, accurate_long_balls, accurate_long_balls_percentage,
                    accurate_opposition_half_passes, accurate_own_half_passes, accurate_passes,
                    accurate_passes_percentage, aerial_duels_won, aerial_duels_won_percentage, appearances,
                    assists, big_chances_created, big_chances_missed, blocked_shots, clean_sheet, clearances,
                    crosses_not_claimed, dispossessed, dribbled_past, error_lead_to_goal, error_lead_to_shot,
                    expected_goals, fouls, free_kick_goal, goal_conversion_percentage, goals,
                    goals_conceded_inside_the_box, goals_conceded_outside_the_box, goals_from_inside_the_box,
                    goals_from_outside_the_box, ground_duels_won, ground_duels_won_percentage, headed_goals,
                    high_claims, hit_woodwork, inaccurate_passes, interceptions, key_passes, left_foot_goals,
                    matches_started, minutes_played, offsides, outfielder_blocks, own_goals, pass_to_assist,
                    penalties_taken, penalty_conceded, penalty_conversion, penalty_faced, penalty_goals,
                    penalty_save, penalty_won, possession_lost, punches, rating, red_cards, right_foot_goals,
                    runs_out, saved_shots_from_inside_the_box, saved_shots_from_outside_the_box, saves,
                    set_piece_conversion, shot_from_set_piece, shots_off_target, shots_on_target,
                    successful_dribbles, successful_dribbles_percentage, successful_runs_out, tackles,
                    total_duels_won, total_duels_won_percentage, total_passes, total_shots, was_fouled,
                    yellow_cards
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                    $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                    $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
                    $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
                    $51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
                    $61, $62, $63, $64, $65, $66, $67, $68, $69, $70,
                    $71, $72, $73, $74, $75, $76, $77, $78, $79, $80,
                    $81
                )
                ON CONFLICT (snapshot_id, row_number) DO UPDATE SET
                    accurate_crosses = EXCLUDED.accurate_crosses,
                    accurate_crosses_percentage = EXCLUDED.accurate_crosses_percentage,
                    accurate_final_third_passes = EXCLUDED.accurate_final_third_passes,
                    accurate_long_balls = EXCLUDED.accurate_long_balls,
                    accurate_long_balls_percentage = EXCLUDED.accurate_long_balls_percentage,
                    accurate_opposition_half_passes = EXCLUDED.accurate_opposition_half_passes,
                    accurate_own_half_passes = EXCLUDED.accurate_own_half_passes,
                    player_id = EXCLUDED.player_id,
                    team_id = EXCLUDED.team_id,
                    accurate_passes = EXCLUDED.accurate_passes,
                    accurate_passes_percentage = EXCLUDED.accurate_passes_percentage,
                    aerial_duels_won = EXCLUDED.aerial_duels_won,
                    aerial_duels_won_percentage = EXCLUDED.aerial_duels_won_percentage,
                    appearances = EXCLUDED.appearances,
                    assists = EXCLUDED.assists,
                    big_chances_created = EXCLUDED.big_chances_created,
                    big_chances_missed = EXCLUDED.big_chances_missed,
                    blocked_shots = EXCLUDED.blocked_shots,
                    clean_sheet = EXCLUDED.clean_sheet,
                    clearances = EXCLUDED.clearances,
                    crosses_not_claimed = EXCLUDED.crosses_not_claimed,
                    dispossessed = EXCLUDED.dispossessed,
                    dribbled_past = EXCLUDED.dribbled_past,
                    error_lead_to_goal = EXCLUDED.error_lead_to_goal,
                    error_lead_to_shot = EXCLUDED.error_lead_to_shot,
                    expected_goals = EXCLUDED.expected_goals,
                    fouls = EXCLUDED.fouls,
                    free_kick_goal = EXCLUDED.free_kick_goal,
                    goal_conversion_percentage = EXCLUDED.goal_conversion_percentage,
                    goals = EXCLUDED.goals,
                    goals_conceded_inside_the_box = EXCLUDED.goals_conceded_inside_the_box,
                    goals_conceded_outside_the_box = EXCLUDED.goals_conceded_outside_the_box,
                    goals_from_inside_the_box = EXCLUDED.goals_from_inside_the_box,
                    goals_from_outside_the_box = EXCLUDED.goals_from_outside_the_box,
                    ground_duels_won = EXCLUDED.ground_duels_won,
                    ground_duels_won_percentage = EXCLUDED.ground_duels_won_percentage,
                    headed_goals = EXCLUDED.headed_goals,
                    high_claims = EXCLUDED.high_claims,
                    hit_woodwork = EXCLUDED.hit_woodwork,
                    inaccurate_passes = EXCLUDED.inaccurate_passes,
                    interceptions = EXCLUDED.interceptions,
                    key_passes = EXCLUDED.key_passes,
                    left_foot_goals = EXCLUDED.left_foot_goals,
                    matches_started = EXCLUDED.matches_started,
                    minutes_played = EXCLUDED.minutes_played,
                    offsides = EXCLUDED.offsides,
                    outfielder_blocks = EXCLUDED.outfielder_blocks,
                    own_goals = EXCLUDED.own_goals,
                    pass_to_assist = EXCLUDED.pass_to_assist,
                    penalties_taken = EXCLUDED.penalties_taken,
                    penalty_conceded = EXCLUDED.penalty_conceded,
                    penalty_conversion = EXCLUDED.penalty_conversion,
                    penalty_faced = EXCLUDED.penalty_faced,
                    penalty_goals = EXCLUDED.penalty_goals,
                    penalty_save = EXCLUDED.penalty_save,
                    penalty_won = EXCLUDED.penalty_won,
                    possession_lost = EXCLUDED.possession_lost,
                    punches = EXCLUDED.punches,
                    rating = EXCLUDED.rating,
                    red_cards = EXCLUDED.red_cards,
                    right_foot_goals = EXCLUDED.right_foot_goals,
                    runs_out = EXCLUDED.runs_out,
                    saved_shots_from_inside_the_box = EXCLUDED.saved_shots_from_inside_the_box,
                    saved_shots_from_outside_the_box = EXCLUDED.saved_shots_from_outside_the_box,
                    saves = EXCLUDED.saves,
                    set_piece_conversion = EXCLUDED.set_piece_conversion,
                    shot_from_set_piece = EXCLUDED.shot_from_set_piece,
                    shots_off_target = EXCLUDED.shots_off_target,
                    shots_on_target = EXCLUDED.shots_on_target,
                    successful_dribbles = EXCLUDED.successful_dribbles,
                    successful_dribbles_percentage = EXCLUDED.successful_dribbles_percentage,
                    successful_runs_out = EXCLUDED.successful_runs_out,
                    tackles = EXCLUDED.tackles,
                    total_duels_won = EXCLUDED.total_duels_won,
                    total_duels_won_percentage = EXCLUDED.total_duels_won_percentage,
                    total_passes = EXCLUDED.total_passes,
                    total_shots = EXCLUDED.total_shots,
                    was_fouled = EXCLUDED.was_fouled,
                    yellow_cards = EXCLUDED.yellow_cards
                """,
                rows,
            )
            total_result_rows += len(rows)
        return total_result_rows
