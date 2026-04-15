"""PostgreSQL repository for seasonal leaderboard bundles."""

from __future__ import annotations

from dataclasses import dataclass

from .event_detail_repository import EventDetailRepository, SqlExecutor
from .event_list_repository import _executemany, _jsonb, _timestamp
from .leaderboards_parser import LeaderboardsBundle


@dataclass(frozen=True)
class LeaderboardsWriteResult:
    endpoint_registry_rows: int
    payload_snapshot_rows: int
    sport_rows: int
    country_rows: int
    category_rows: int
    unique_tournament_rows: int
    season_rows: int
    tournament_rows: int
    venue_rows: int
    team_rows: int
    player_rows: int
    event_status_rows: int
    event_rows: int
    period_rows: int
    team_of_the_week_rows: int
    team_of_the_week_player_rows: int
    season_group_rows: int
    season_player_of_the_season_rows: int
    season_statistics_type_rows: int
    tournament_team_event_snapshot_rows: int
    tournament_team_event_bucket_rows: int
    top_player_snapshot_rows: int
    top_player_entry_rows: int
    top_team_snapshot_rows: int
    top_team_entry_rows: int


class LeaderboardsRepository(EventDetailRepository):
    """Writes normalized leaderboard data into PostgreSQL."""

    async def upsert_bundle(self, executor: SqlExecutor, bundle: LeaderboardsBundle) -> LeaderboardsWriteResult:
        await self._upsert_endpoint_registry(executor, bundle)
        await self._upsert_sports(executor, bundle)
        await self._upsert_countries(executor, bundle)
        await self._upsert_categories(executor, bundle)
        await self._upsert_unique_tournaments(executor, bundle)
        await self._upsert_seasons(executor, bundle)
        await self._upsert_tournaments(executor, bundle)
        await self._upsert_venues(executor, bundle)
        await self._upsert_teams_base(executor, bundle)
        await self._upsert_players(executor, bundle)
        await self._upsert_event_statuses(executor, bundle)
        await self._upsert_events(executor, bundle)
        await self._upsert_event_round_infos(executor, bundle)
        await self._upsert_event_status_times(executor, bundle)
        await self._upsert_event_times(executor, bundle)
        await self._upsert_event_var_in_progress(executor, bundle)
        await self._upsert_event_scores(executor, bundle)
        await self._upsert_event_filter_values(executor, bundle)
        await self._upsert_event_change_items(executor, bundle)
        await self._upsert_periods(executor, bundle)
        await self._upsert_team_of_the_week(executor, bundle)
        await self._upsert_team_of_the_week_players(executor, bundle)
        await self._upsert_season_groups(executor, bundle)
        await self._upsert_season_player_of_the_season(executor, bundle)
        await self._upsert_season_statistics_types(executor, bundle)
        tournament_team_event_bucket_rows = await self._insert_tournament_team_event_snapshots(executor, bundle)
        top_player_entry_rows = await self._insert_top_player_snapshots(executor, bundle)
        top_team_entry_rows = await self._insert_top_team_snapshots(executor, bundle)
        await self._insert_payload_snapshots(executor, bundle)

        return LeaderboardsWriteResult(
            endpoint_registry_rows=len(bundle.registry_entries),
            payload_snapshot_rows=len(bundle.payload_snapshots),
            sport_rows=len(bundle.sports),
            country_rows=len(bundle.countries),
            category_rows=len(bundle.categories),
            unique_tournament_rows=len(bundle.unique_tournaments),
            season_rows=len(bundle.seasons),
            tournament_rows=len(bundle.tournaments),
            venue_rows=len(bundle.venues),
            team_rows=len(bundle.teams),
            player_rows=len(bundle.players),
            event_status_rows=len(bundle.event_statuses),
            event_rows=len(bundle.events),
            period_rows=len(bundle.periods),
            team_of_the_week_rows=len(bundle.team_of_the_week),
            team_of_the_week_player_rows=len(bundle.team_of_the_week_players),
            season_group_rows=len(bundle.season_groups),
            season_player_of_the_season_rows=len(bundle.season_player_of_the_season),
            season_statistics_type_rows=len(bundle.season_statistics_types),
            tournament_team_event_snapshot_rows=len(bundle.tournament_team_event_snapshots),
            tournament_team_event_bucket_rows=tournament_team_event_bucket_rows,
            top_player_snapshot_rows=len(bundle.top_player_snapshots),
            top_player_entry_rows=top_player_entry_rows,
            top_team_snapshot_rows=len(bundle.top_team_snapshots),
            top_team_entry_rows=top_team_entry_rows,
        )

    async def _upsert_periods(self, executor: SqlExecutor, bundle: LeaderboardsBundle) -> None:
        rows = [
            (
                item.id,
                item.unique_tournament_id,
                item.season_id,
                item.period_name,
                item.type,
                item.start_date_timestamp,
                item.created_at_timestamp,
                item.round_name,
                item.round_number,
                item.round_slug,
            )
            for item in bundle.periods
        ]
        await _executemany(
            executor,
            """
            INSERT INTO period (
                id, unique_tournament_id, season_id, period_name, type,
                start_date_timestamp, created_at_timestamp, round_name, round_number, round_slug
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (id) DO UPDATE SET
                unique_tournament_id = EXCLUDED.unique_tournament_id,
                season_id = EXCLUDED.season_id,
                period_name = EXCLUDED.period_name,
                type = EXCLUDED.type,
                start_date_timestamp = EXCLUDED.start_date_timestamp,
                created_at_timestamp = EXCLUDED.created_at_timestamp,
                round_name = EXCLUDED.round_name,
                round_number = EXCLUDED.round_number,
                round_slug = EXCLUDED.round_slug
            """,
            rows,
        )

    async def _upsert_team_of_the_week(self, executor: SqlExecutor, bundle: LeaderboardsBundle) -> None:
        rows = [(item.period_id, item.formation) for item in bundle.team_of_the_week]
        await _executemany(
            executor,
            """
            INSERT INTO team_of_the_week (period_id, formation)
            VALUES ($1, $2)
            ON CONFLICT (period_id) DO UPDATE SET
                formation = EXCLUDED.formation
            """,
            rows,
        )

    async def _upsert_team_of_the_week_players(self, executor: SqlExecutor, bundle: LeaderboardsBundle) -> None:
        rows = [
            (
                item.period_id,
                item.entry_id,
                item.player_id,
                item.team_id,
                item.order_value,
                item.rating,
            )
            for item in bundle.team_of_the_week_players
        ]
        await _executemany(
            executor,
            """
            INSERT INTO team_of_the_week_player (
                period_id, entry_id, player_id, team_id, order_value, rating
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (period_id, entry_id) DO UPDATE SET
                player_id = EXCLUDED.player_id,
                team_id = EXCLUDED.team_id,
                order_value = EXCLUDED.order_value,
                rating = EXCLUDED.rating
            """,
            rows,
        )

    async def _upsert_season_groups(self, executor: SqlExecutor, bundle: LeaderboardsBundle) -> None:
        rows = [
            (
                item.unique_tournament_id,
                item.season_id,
                item.tournament_id,
                item.group_name,
            )
            for item in bundle.season_groups
        ]
        await _executemany(
            executor,
            """
            INSERT INTO season_group (
                unique_tournament_id, season_id, tournament_id, group_name
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (unique_tournament_id, season_id, tournament_id) DO UPDATE SET
                group_name = EXCLUDED.group_name
            """,
            rows,
        )

    async def _upsert_season_player_of_the_season(self, executor: SqlExecutor, bundle: LeaderboardsBundle) -> None:
        rows = [
            (
                item.unique_tournament_id,
                item.season_id,
                item.player_id,
                item.team_id,
                item.player_of_the_tournament,
                item.statistics_id,
                _jsonb(item.statistics_payload),
            )
            for item in bundle.season_player_of_the_season
        ]
        await _executemany(
            executor,
            """
            INSERT INTO season_player_of_the_season (
                unique_tournament_id, season_id, player_id, team_id,
                player_of_the_tournament, statistics_id, statistics_payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            ON CONFLICT (unique_tournament_id, season_id) DO UPDATE SET
                player_id = EXCLUDED.player_id,
                team_id = EXCLUDED.team_id,
                player_of_the_tournament = EXCLUDED.player_of_the_tournament,
                statistics_id = EXCLUDED.statistics_id,
                statistics_payload = EXCLUDED.statistics_payload
            """,
            rows,
        )

    async def _upsert_season_statistics_types(self, executor: SqlExecutor, bundle: LeaderboardsBundle) -> None:
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

    async def _insert_tournament_team_event_snapshots(self, executor: SqlExecutor, bundle: LeaderboardsBundle) -> int:
        total_bucket_rows = 0
        for snapshot in bundle.tournament_team_event_snapshots:
            snapshot_id = await executor.fetchval(
                """
                INSERT INTO tournament_team_event_snapshot (
                    endpoint_pattern, unique_tournament_id, season_id, scope, source_url, fetched_at
                )
                VALUES ($1, $2, $3, $4, $5, $6::timestamptz)
                RETURNING id
                """,
                snapshot.endpoint_pattern,
                snapshot.unique_tournament_id,
                snapshot.season_id,
                snapshot.scope,
                snapshot.source_url,
                _timestamp(snapshot.fetched_at),
            )
            rows = [
                (
                    snapshot_id,
                    item.level_1_key,
                    item.level_2_key,
                    item.event_id,
                    item.ordinal,
                )
                for item in snapshot.buckets
            ]
            await _executemany(
                executor,
                """
                INSERT INTO tournament_team_event_bucket (
                    snapshot_id, level_1_key, level_2_key, event_id, ordinal
                )
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (snapshot_id, level_1_key, level_2_key, event_id) DO UPDATE SET
                    ordinal = EXCLUDED.ordinal
                """,
                rows,
            )
            total_bucket_rows += len(rows)
        return total_bucket_rows

    async def _insert_top_player_snapshots(self, executor: SqlExecutor, bundle: LeaderboardsBundle) -> int:
        total_entry_rows = 0
        for snapshot in bundle.top_player_snapshots:
            snapshot_id = await executor.fetchval(
                """
                INSERT INTO top_player_snapshot (
                    endpoint_pattern, unique_tournament_id, season_id, source_url, statistics_type, fetched_at
                )
                VALUES ($1, $2, $3, $4, $5::jsonb, $6::timestamptz)
                RETURNING id
                """,
                snapshot.endpoint_pattern,
                snapshot.unique_tournament_id,
                snapshot.season_id,
                snapshot.source_url,
                _jsonb(snapshot.statistics_type),
                _timestamp(snapshot.fetched_at),
            )
            rows = [
                (
                    snapshot_id,
                    item.metric_name,
                    item.ordinal,
                    item.player_id,
                    item.team_id,
                    item.event_id,
                    item.played_enough,
                    item.statistic,
                    item.statistics_id,
                    _jsonb(item.statistics_payload),
                )
                for item in snapshot.entries
            ]
            await _executemany(
                executor,
                """
                INSERT INTO top_player_entry (
                    snapshot_id, metric_name, ordinal, player_id, team_id, event_id,
                    played_enough, statistic, statistics_id, statistics_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
                ON CONFLICT (snapshot_id, metric_name, ordinal) DO UPDATE SET
                    player_id = EXCLUDED.player_id,
                    team_id = EXCLUDED.team_id,
                    event_id = EXCLUDED.event_id,
                    played_enough = EXCLUDED.played_enough,
                    statistic = EXCLUDED.statistic,
                    statistics_id = EXCLUDED.statistics_id,
                    statistics_payload = EXCLUDED.statistics_payload
                """,
                rows,
            )
            total_entry_rows += len(rows)
        return total_entry_rows

    async def _insert_top_team_snapshots(self, executor: SqlExecutor, bundle: LeaderboardsBundle) -> int:
        total_entry_rows = 0
        for snapshot in bundle.top_team_snapshots:
            snapshot_id = await executor.fetchval(
                """
                INSERT INTO top_team_snapshot (
                    endpoint_pattern, unique_tournament_id, season_id, source_url, fetched_at
                )
                VALUES ($1, $2, $3, $4, $5::timestamptz)
                RETURNING id
                """,
                snapshot.endpoint_pattern,
                snapshot.unique_tournament_id,
                snapshot.season_id,
                snapshot.source_url,
                _timestamp(snapshot.fetched_at),
            )
            rows = [
                (
                    snapshot_id,
                    item.metric_name,
                    item.ordinal,
                    item.team_id,
                    item.statistics_id,
                    _jsonb(item.statistics_payload),
                )
                for item in snapshot.entries
            ]
            await _executemany(
                executor,
                """
                INSERT INTO top_team_entry (
                    snapshot_id, metric_name, ordinal, team_id, statistics_id, statistics_payload
                )
                VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                ON CONFLICT (snapshot_id, metric_name, ordinal) DO UPDATE SET
                    team_id = EXCLUDED.team_id,
                    statistics_id = EXCLUDED.statistics_id,
                    statistics_payload = EXCLUDED.statistics_payload
                """,
                rows,
            )
            total_entry_rows += len(rows)
        return total_entry_rows
