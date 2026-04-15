from __future__ import annotations

import unittest

from schema_inspector.competition_parser import ApiPayloadSnapshotRecord, CategoryRecord, CountryRecord, SportRecord, UniqueTournamentRecord
from schema_inspector.endpoints import leaderboards_registry_entries
from schema_inspector.event_detail_parser import EventDetailTeamRecord, EventDetailTournamentRecord, PlayerRecord
from schema_inspector.event_list_parser import EventSeasonRecord
from schema_inspector.entities_parser import SeasonStatisticsTypeRecord
from schema_inspector.leaderboards_job import LeaderboardsIngestJob
from schema_inspector.leaderboards_parser import (
    LeaderboardsBundle,
    PeriodRecord,
    SeasonGroupRecord,
    SeasonPlayerOfTheSeasonRecord,
    TeamOfTheWeekPlayerRecord,
    TeamOfTheWeekRecord,
    TopPlayerEntryRecord,
    TopPlayerSnapshotRecord,
    TopTeamEntryRecord,
    TopTeamSnapshotRecord,
    TournamentTeamEventBucketRecord,
    TournamentTeamEventSnapshotRecord,
)
from schema_inspector.leaderboards_repository import LeaderboardsRepository, LeaderboardsWriteResult


class _FakeExecutor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self._next_id = 1

    async def executemany(self, command: str, args):
        self.executemany_calls.append((command, list(args)))
        return None

    async def fetchval(self, query: str, *args):
        self.fetchval_calls.append((query, args))
        value = self._next_id
        self._next_id += 1
        return value


class _FakeParser:
    def __init__(self, bundle: LeaderboardsBundle) -> None:
        self.bundle = bundle
        self.calls: list[tuple[int, int, float]] = []

    async def fetch_bundle(self, unique_tournament_id: int, season_id: int, **kwargs) -> LeaderboardsBundle:
        del kwargs
        self.calls.append((unique_tournament_id, season_id, 20.0))
        return self.bundle


class _FakeRepository:
    def __init__(self, result: LeaderboardsWriteResult) -> None:
        self.result = result
        self.calls: list[tuple[object, LeaderboardsBundle]] = []

    async def upsert_bundle(self, executor, bundle: LeaderboardsBundle) -> LeaderboardsWriteResult:
        self.calls.append((executor, bundle))
        return self.result


class _FakeTransaction:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def __aenter__(self) -> object:
        return self.connection

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        return None


class _FakeDatabase:
    def __init__(self, connection: object) -> None:
        self.connection = connection
        self.transaction_calls = 0

    def transaction(self) -> _FakeTransaction:
        self.transaction_calls += 1
        return _FakeTransaction(self.connection)


def _build_bundle() -> LeaderboardsBundle:
    return LeaderboardsBundle(
        registry_entries=leaderboards_registry_entries(),
        payload_snapshots=(
            ApiPayloadSnapshotRecord(
                endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall",
                source_url="https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/top-players/overall",
                envelope_key="topPlayers",
                context_entity_type="season",
                context_entity_id=76986,
                payload={"topPlayers": {}},
                fetched_at="2026-04-10T10:00:00+00:00",
            ),
        ),
        sports=(SportRecord(id=1, slug="football", name="Football"),),
        countries=(CountryRecord(alpha2="EN", alpha3="ENG", slug="england", name="England"),),
        categories=(CategoryRecord(id=1, slug="england", name="England", sport_id=1, country_alpha2="EN"),),
        unique_tournaments=(UniqueTournamentRecord(id=17, slug="premier-league", name="Premier League", category_id=1),),
        seasons=(EventSeasonRecord(id=76986, name="Premier League 25/26", year="25/26", editor=False),),
        tournaments=(
            EventDetailTournamentRecord(
                id=100,
                slug="premier-league",
                name="Premier League",
                category_id=1,
                unique_tournament_id=17,
                competition_type=1,
            ),
        ),
        venues=(),
        teams=(EventDetailTeamRecord(id=42, slug="arsenal", name="Arsenal", sport_id=1, country_alpha2="EN"),),
        players=(PlayerRecord(id=700, slug="player-700", name="Player 700", team_id=42, country_alpha2="EN", position="F"),),
        event_statuses=(),
        events=(),
        event_round_infos=(),
        event_status_times=(),
        event_times=(),
        event_var_in_progress_items=(),
        event_scores=(),
        event_filter_values=(),
        event_change_items=(),
        top_player_snapshots=(
            TopPlayerSnapshotRecord(
                endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall",
                unique_tournament_id=17,
                season_id=76986,
                source_url="https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/top-players/overall",
                statistics_type={"code": "overall"},
                fetched_at="2026-04-10T10:00:00+00:00",
                entries=(
                    TopPlayerEntryRecord(
                        metric_name="goals",
                        ordinal=0,
                        player_id=700,
                        team_id=42,
                        event_id=None,
                        played_enough=True,
                        statistic=11,
                        statistics_id=10,
                        statistics_payload={"id": 10, "goals": 11},
                    ),
                ),
            ),
        ),
        top_team_snapshots=(
            TopTeamSnapshotRecord(
                endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/overall",
                unique_tournament_id=17,
                season_id=76986,
                source_url="https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/top-teams/overall",
                fetched_at="2026-04-10T10:00:00+00:00",
                entries=(
                    TopTeamEntryRecord(
                        metric_name="goals",
                        ordinal=0,
                        team_id=42,
                        statistics_id=13,
                        statistics_payload={"id": 13, "goals": 70},
                    ),
                ),
            ),
        ),
        periods=(
            PeriodRecord(
                id=19564,
                unique_tournament_id=17,
                season_id=76986,
                period_name="Round 1",
                type="round",
                start_date_timestamp=1710000000,
                created_at_timestamp=1710000100,
                round_name="Round 1",
                round_number=1,
                round_slug="round-1",
            ),
        ),
        team_of_the_week=(TeamOfTheWeekRecord(period_id=19564, formation="4-3-3"),),
        team_of_the_week_players=(
            TeamOfTheWeekPlayerRecord(period_id=19564, entry_id=1, player_id=700, team_id=42, order_value=0, rating="7.9"),
        ),
        season_groups=(SeasonGroupRecord(unique_tournament_id=17, season_id=76986, tournament_id=100, group_name="Group A"),),
        season_player_of_the_season=(
            SeasonPlayerOfTheSeasonRecord(
                unique_tournament_id=17,
                season_id=76986,
                player_id=700,
                team_id=42,
                player_of_the_tournament=True,
                statistics_id=15,
                statistics_payload={"id": 15, "rating": 8.2},
            ),
        ),
        season_statistics_types=(
            SeasonStatisticsTypeRecord(subject_type="player", unique_tournament_id=17, season_id=76986, stat_type="summary"),
        ),
        tournament_team_event_snapshots=(
            TournamentTeamEventSnapshotRecord(
                endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/{scope}",
                unique_tournament_id=17,
                season_id=76986,
                scope="total",
                source_url="https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/team-events/total",
                fetched_at="2026-04-10T10:00:00+00:00",
                buckets=(
                    TournamentTeamEventBucketRecord(
                        level_1_key="results",
                        level_2_key="__items__",
                        event_id=14083191,
                        ordinal=0,
                    ),
                ),
            ),
        ),
    )


class LeaderboardsStorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_leaderboards_repository_writes_expected_tables(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = LeaderboardsRepository()

        result = await repository.upsert_bundle(executor, bundle)

        self.assertEqual(result.top_player_entry_rows, 1)
        self.assertEqual(result.top_team_entry_rows, 1)
        statements = [sql for sql, _ in executor.executemany_calls]
        self.assertTrue(any("INSERT INTO period " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO team_of_the_week " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO team_of_the_week_player " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_group " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_player_of_the_season " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_statistics_type " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO tournament_team_event_bucket " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO top_player_entry " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO top_team_entry " in sql for sql in statements))
        self.assertEqual(len(executor.fetchval_calls), 3)

    async def test_leaderboards_ingest_job_uses_transaction_and_repository(self) -> None:
        bundle = _build_bundle()
        parser = _FakeParser(bundle)
        repository_result = LeaderboardsWriteResult(
            endpoint_registry_rows=15,
            payload_snapshot_rows=1,
            sport_rows=1,
            country_rows=1,
            category_rows=1,
            unique_tournament_rows=1,
            season_rows=1,
            tournament_rows=1,
            venue_rows=0,
            team_rows=1,
            player_rows=1,
            event_status_rows=0,
            event_rows=0,
            period_rows=1,
            team_of_the_week_rows=1,
            team_of_the_week_player_rows=1,
            season_group_rows=1,
            season_player_of_the_season_rows=1,
            season_statistics_type_rows=1,
            tournament_team_event_snapshot_rows=1,
            tournament_team_event_bucket_rows=1,
            top_player_snapshot_rows=1,
            top_player_entry_rows=1,
            top_team_snapshot_rows=1,
            top_team_entry_rows=1,
        )
        repository = _FakeRepository(repository_result)
        connection = object()
        database = _FakeDatabase(connection)
        job = LeaderboardsIngestJob(parser, repository, database)

        result = await job.run(17, 76986)

        self.assertEqual(parser.calls, [(17, 76986, 20.0)])
        self.assertEqual(repository.calls, [(connection, bundle)])
        self.assertEqual(database.transaction_calls, 1)
        self.assertEqual(result.unique_tournament_id, 17)
        self.assertEqual(result.season_id, 76986)
        self.assertEqual(result.written.top_player_entry_rows, 1)


if __name__ == "__main__":
    unittest.main()
