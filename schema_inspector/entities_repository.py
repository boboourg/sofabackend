"""PostgreSQL repository for player/team enrichment bundles."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .event_detail_repository import EventDetailRepository, SqlExecutor
from .event_list_repository import _executemany, _jsonb
from .entities_parser import EntitiesBundle


@dataclass(frozen=True)
class EntitiesWriteResult:
    endpoint_registry_rows: int
    payload_snapshot_rows: int
    sport_rows: int
    country_rows: int
    category_rows: int
    category_transfer_period_rows: int
    unique_tournament_rows: int
    season_rows: int
    unique_tournament_season_rows: int
    tournament_rows: int
    team_rows: int
    venue_rows: int
    manager_rows: int
    manager_team_membership_rows: int
    player_rows: int
    transfer_history_rows: int
    player_season_statistics_rows: int
    entity_statistics_season_rows: int
    entity_statistics_type_rows: int
    season_statistics_type_rows: int


class EntitiesRepository(EventDetailRepository):
    """Writes normalized entity/enrichment data into PostgreSQL."""

    async def upsert_bundle(self, executor: SqlExecutor, bundle: EntitiesBundle) -> EntitiesWriteResult:
        await self._upsert_endpoint_registry(executor, bundle)
        await self._upsert_sports(executor, bundle)
        await self._upsert_countries(executor, bundle)
        await self._upsert_categories(executor, bundle)
        await self._upsert_category_transfer_periods(executor, bundle)
        await self._upsert_unique_tournaments(executor, bundle)
        await self._upsert_seasons(executor, bundle)
        await self._upsert_unique_tournament_seasons(executor, bundle)
        await self._upsert_tournaments(executor, bundle)
        await self._upsert_venues(executor, bundle)
        await self._upsert_teams_base(executor, bundle)
        await self._upsert_managers(executor, bundle)
        await self._upsert_team_manager_links(executor, bundle)
        await self._upsert_manager_team_memberships(executor, bundle)
        await self._upsert_players(executor, bundle)
        await self._upsert_transfer_histories(executor, bundle)
        await self._upsert_player_season_statistics(executor, bundle)
        await self._upsert_entity_statistics_seasons(executor, bundle)
        await self._upsert_entity_statistics_types(executor, bundle)
        await self._upsert_season_statistics_types(executor, bundle)
        await self._insert_payload_snapshots(executor, bundle)

        return EntitiesWriteResult(
            endpoint_registry_rows=len(bundle.registry_entries),
            payload_snapshot_rows=len(bundle.payload_snapshots),
            sport_rows=len(bundle.sports),
            country_rows=len(bundle.countries),
            category_rows=len(bundle.categories),
            category_transfer_period_rows=len(bundle.category_transfer_periods),
            unique_tournament_rows=len(bundle.unique_tournaments),
            season_rows=len(bundle.seasons),
            unique_tournament_season_rows=len(bundle.unique_tournament_seasons),
            tournament_rows=len(bundle.tournaments),
            team_rows=len(bundle.teams),
            venue_rows=len(bundle.venues),
            manager_rows=len(bundle.managers),
            manager_team_membership_rows=len(bundle.manager_team_memberships),
            player_rows=len(bundle.players),
            transfer_history_rows=len(bundle.transfer_histories),
            player_season_statistics_rows=len(bundle.player_season_statistics),
            entity_statistics_season_rows=len(bundle.entity_statistics_seasons),
            entity_statistics_type_rows=len(bundle.entity_statistics_types),
            season_statistics_type_rows=len(bundle.season_statistics_types),
        )

    async def _upsert_category_transfer_periods(self, executor: SqlExecutor, bundle: EntitiesBundle) -> None:
        rows = [
            (item.category_id, item.active_from, item.active_to)
            for item in bundle.category_transfer_periods
        ]
        await _executemany(
            executor,
            """
            INSERT INTO category_transfer_period (category_id, active_from, active_to)
            VALUES ($1, $2, $3)
            ON CONFLICT (category_id, active_from, active_to) DO NOTHING
            """,
            rows,
        )

    async def _upsert_unique_tournament_seasons(self, executor: SqlExecutor, bundle: EntitiesBundle) -> None:
        rows = [
            (item.unique_tournament_id, item.season_id)
            for item in bundle.unique_tournament_seasons
        ]
        await _executemany(
            executor,
            """
            INSERT INTO unique_tournament_season (unique_tournament_id, season_id)
            VALUES ($1, $2)
            ON CONFLICT (unique_tournament_id, season_id) DO NOTHING
            """,
            rows,
        )

    async def _upsert_transfer_histories(self, executor: SqlExecutor, bundle: EntitiesBundle) -> None:
        rows = [
            (
                item.id,
                item.player_id,
                item.transfer_from_team_id,
                item.transfer_to_team_id,
                item.from_team_name,
                item.to_team_name,
                item.transfer_date_timestamp,
                item.transfer_fee,
                item.transfer_fee_description,
                _jsonb(item.transfer_fee_raw),
                item.type,
            )
            for item in bundle.transfer_histories
        ]
        await _executemany(
            executor,
            """
            INSERT INTO player_transfer_history (
                id, player_id, transfer_from_team_id, transfer_to_team_id, from_team_name, to_team_name,
                transfer_date_timestamp, transfer_fee, transfer_fee_description, transfer_fee_raw, type
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
            ON CONFLICT (id) DO UPDATE SET
                player_id = EXCLUDED.player_id,
                transfer_from_team_id = EXCLUDED.transfer_from_team_id,
                transfer_to_team_id = EXCLUDED.transfer_to_team_id,
                from_team_name = EXCLUDED.from_team_name,
                to_team_name = EXCLUDED.to_team_name,
                transfer_date_timestamp = EXCLUDED.transfer_date_timestamp,
                transfer_fee = EXCLUDED.transfer_fee,
                transfer_fee_description = EXCLUDED.transfer_fee_description,
                transfer_fee_raw = EXCLUDED.transfer_fee_raw,
                type = EXCLUDED.type
            """,
            rows,
        )

    async def _upsert_player_season_statistics(self, executor: SqlExecutor, bundle: EntitiesBundle) -> None:
        rows = [
            (
                item.player_id,
                item.unique_tournament_id,
                item.season_id,
                item.team_id,
                item.stat_type,
                item.statistics_id,
                item.season_year,
                item.start_year,
                item.end_year,
                item.accurate_crosses,
                item.accurate_crosses_percentage,
                item.accurate_long_balls,
                item.accurate_long_balls_percentage,
                item.accurate_passes,
                item.accurate_passes_percentage,
                item.aerial_duels_won,
                item.appearances,
                item.assists,
                item.big_chances_created,
                item.big_chances_missed,
                item.blocked_shots,
                item.clean_sheet,
                item.count_rating,
                item.dribbled_past,
                item.error_lead_to_goal,
                item.expected_assists,
                item.expected_goals,
                item.goals,
                item.goals_assists_sum,
                item.goals_conceded,
                item.goals_prevented,
                item.interceptions,
                item.key_passes,
                item.minutes_played,
                item.outfielder_blocks,
                item.pass_to_assist,
                item.rating,
                item.red_cards,
                item.saves,
                item.shots_from_inside_the_box,
                item.shots_on_target,
                item.successful_dribbles,
                item.tackles,
                item.total_cross,
                item.total_long_balls,
                item.total_passes,
                item.total_rating,
                item.total_shots,
                item.yellow_cards,
                _jsonb(item.statistics_payload),
            )
            for item in bundle.player_season_statistics
        ]
        await _executemany(
            executor,
            """
            INSERT INTO player_season_statistics (
                player_id, unique_tournament_id, season_id, team_id, stat_type, statistics_id,
                season_year, start_year, end_year, accurate_crosses, accurate_crosses_percentage,
                accurate_long_balls, accurate_long_balls_percentage, accurate_passes,
                accurate_passes_percentage, aerial_duels_won, appearances, assists,
                big_chances_created, big_chances_missed, blocked_shots, clean_sheet,
                count_rating, dribbled_past, error_lead_to_goal, expected_assists,
                expected_goals, goals, goals_assists_sum, goals_conceded, goals_prevented,
                interceptions, key_passes, minutes_played, outfielder_blocks, pass_to_assist,
                rating, red_cards, saves, shots_from_inside_the_box, shots_on_target,
                successful_dribbles, tackles, total_cross, total_long_balls, total_passes,
                total_rating, total_shots, yellow_cards, statistics_payload
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
                $41, $42, $43, $44, $45, $46, $47, $48, $49, $50::jsonb
            )
            ON CONFLICT (player_id, unique_tournament_id, season_id, team_id, stat_type) DO UPDATE SET
                statistics_id = EXCLUDED.statistics_id,
                season_year = EXCLUDED.season_year,
                start_year = EXCLUDED.start_year,
                end_year = EXCLUDED.end_year,
                accurate_crosses = EXCLUDED.accurate_crosses,
                accurate_crosses_percentage = EXCLUDED.accurate_crosses_percentage,
                accurate_long_balls = EXCLUDED.accurate_long_balls,
                accurate_long_balls_percentage = EXCLUDED.accurate_long_balls_percentage,
                accurate_passes = EXCLUDED.accurate_passes,
                accurate_passes_percentage = EXCLUDED.accurate_passes_percentage,
                aerial_duels_won = EXCLUDED.aerial_duels_won,
                appearances = EXCLUDED.appearances,
                assists = EXCLUDED.assists,
                big_chances_created = EXCLUDED.big_chances_created,
                big_chances_missed = EXCLUDED.big_chances_missed,
                blocked_shots = EXCLUDED.blocked_shots,
                clean_sheet = EXCLUDED.clean_sheet,
                count_rating = EXCLUDED.count_rating,
                dribbled_past = EXCLUDED.dribbled_past,
                error_lead_to_goal = EXCLUDED.error_lead_to_goal,
                expected_assists = EXCLUDED.expected_assists,
                expected_goals = EXCLUDED.expected_goals,
                goals = EXCLUDED.goals,
                goals_assists_sum = EXCLUDED.goals_assists_sum,
                goals_conceded = EXCLUDED.goals_conceded,
                goals_prevented = EXCLUDED.goals_prevented,
                interceptions = EXCLUDED.interceptions,
                key_passes = EXCLUDED.key_passes,
                minutes_played = EXCLUDED.minutes_played,
                outfielder_blocks = EXCLUDED.outfielder_blocks,
                pass_to_assist = EXCLUDED.pass_to_assist,
                rating = EXCLUDED.rating,
                red_cards = EXCLUDED.red_cards,
                saves = EXCLUDED.saves,
                shots_from_inside_the_box = EXCLUDED.shots_from_inside_the_box,
                shots_on_target = EXCLUDED.shots_on_target,
                successful_dribbles = EXCLUDED.successful_dribbles,
                tackles = EXCLUDED.tackles,
                total_cross = EXCLUDED.total_cross,
                total_long_balls = EXCLUDED.total_long_balls,
                total_passes = EXCLUDED.total_passes,
                total_rating = EXCLUDED.total_rating,
                total_shots = EXCLUDED.total_shots,
                yellow_cards = EXCLUDED.yellow_cards,
                statistics_payload = EXCLUDED.statistics_payload
            """,
            rows,
        )

    async def _upsert_entity_statistics_seasons(self, executor: SqlExecutor, bundle: EntitiesBundle) -> None:
        rows = [
            (
                item.subject_type,
                item.subject_id,
                item.unique_tournament_id,
                item.season_id,
                item.all_time_season_id,
            )
            for item in bundle.entity_statistics_seasons
        ]
        await _executemany(
            executor,
            """
            INSERT INTO entity_statistics_season (
                subject_type, subject_id, unique_tournament_id, season_id, all_time_season_id
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (subject_type, subject_id, unique_tournament_id, season_id) DO UPDATE SET
                all_time_season_id = EXCLUDED.all_time_season_id
            """,
            rows,
        )

    async def _upsert_entity_statistics_types(self, executor: SqlExecutor, bundle: EntitiesBundle) -> None:
        rows = [
            (
                item.subject_type,
                item.subject_id,
                item.unique_tournament_id,
                item.season_id,
                item.stat_type,
            )
            for item in bundle.entity_statistics_types
        ]
        await _executemany(
            executor,
            """
            INSERT INTO entity_statistics_type (
                subject_type, subject_id, unique_tournament_id, season_id, stat_type
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (subject_type, subject_id, unique_tournament_id, season_id, stat_type) DO NOTHING
            """,
            rows,
        )

    async def _upsert_season_statistics_types(self, executor: SqlExecutor, bundle: EntitiesBundle) -> None:
        rows = [
            (
                item.subject_type,
                item.unique_tournament_id,
                item.season_id,
                item.stat_type,
            )
            for item in bundle.season_statistics_types
        ]
        await _executemany(
            executor,
            """
            INSERT INTO season_statistics_type (
                subject_type, unique_tournament_id, season_id, stat_type
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (subject_type, unique_tournament_id, season_id, stat_type) DO NOTHING
            """,
            rows,
        )
