from __future__ import annotations

import unittest

from schema_inspector.competition_parser import (
    ApiPayloadSnapshotRecord,
    CategoryRecord,
    CountryRecord,
    SportRecord,
    UniqueTournamentRecord,
    UniqueTournamentSeasonRecord,
)
from schema_inspector.endpoints import entities_registry_entries
from schema_inspector.entities_job import EntitiesIngestJob
from schema_inspector.entities_parser import (
    CategoryTransferPeriodRecord,
    EntitiesBundle,
    EntityStatisticsSeasonRecord,
    EntityStatisticsTypeRecord,
    PlayerSeasonStatisticsRecord,
    PlayerTransferHistoryRecord,
    SeasonStatisticsTypeRecord,
)
from schema_inspector.entities_repository import EntitiesRepository, EntitiesWriteResult
from schema_inspector.event_detail_parser import EventDetailTeamRecord, EventDetailTournamentRecord, ManagerRecord, ManagerTeamMembershipRecord, PlayerRecord, VenueRecord
from schema_inspector.event_list_parser import EventSeasonRecord


class _FakeExecutor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    async def executemany(self, command: str, args):
        self.executemany_calls.append((command, list(args)))
        return None


class _FakeParser:
    def __init__(self, bundle: EntitiesBundle) -> None:
        self.bundle = bundle
        self.calls: list[tuple[tuple[int, ...], tuple[int, ...], tuple[int, ...], float]] = []

    async def fetch_bundle(
        self,
        *,
        player_ids=(),
        player_statistics_ids=(),
        team_ids=(),
        player_overall_requests=(),
        team_overall_requests=(),
        player_heatmap_requests=(),
        team_performance_graph_requests=(),
        include_player_statistics=True,
        include_player_statistics_seasons=True,
        include_player_transfer_history=True,
        include_team_statistics_seasons=True,
        include_team_player_statistics_seasons=True,
        timeout: float = 20.0,
    ) -> EntitiesBundle:
        del player_overall_requests
        del team_overall_requests
        del player_heatmap_requests
        del team_performance_graph_requests
        del include_player_statistics
        del include_player_statistics_seasons
        del include_player_transfer_history
        del include_team_statistics_seasons
        del include_team_player_statistics_seasons
        self.calls.append((tuple(player_ids), tuple(player_statistics_ids), tuple(team_ids), timeout))
        return self.bundle


class _FakeRepository:
    def __init__(self, result: EntitiesWriteResult) -> None:
        self.result = result
        self.calls: list[tuple[object, EntitiesBundle]] = []

    async def upsert_bundle(self, executor, bundle: EntitiesBundle) -> EntitiesWriteResult:
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


def _build_bundle() -> EntitiesBundle:
    return EntitiesBundle(
        registry_entries=entities_registry_entries(),
        payload_snapshots=(
            ApiPayloadSnapshotRecord(
                endpoint_pattern="/api/v1/team/{team_id}",
                source_url="https://www.sofascore.com/api/v1/team/41",
                envelope_key="team",
                context_entity_type="team",
                context_entity_id=41,
                payload={"team": {"id": 41}},
                fetched_at="2026-04-10T10:00:00+00:00",
            ),
        ),
        sports=(SportRecord(id=1, slug="football", name="Football"),),
        countries=(CountryRecord(alpha2="EN", alpha3="ENG", slug="england", name="England"),),
        categories=(CategoryRecord(id=1, slug="england", name="England", sport_id=1, country_alpha2="EN"),),
        category_transfer_periods=(
            CategoryTransferPeriodRecord(category_id=1, active_from="2026-06-01", active_to="2026-08-31"),
        ),
        unique_tournaments=(UniqueTournamentRecord(id=17, slug="premier-league", name="Premier League", category_id=1),),
        seasons=(EventSeasonRecord(id=76986, name="Premier League 25/26", year="25/26", editor=False),),
        unique_tournament_seasons=(UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=76986),),
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
        teams=(EventDetailTeamRecord(id=41, slug="arsenal", name="Arsenal", sport_id=1, country_alpha2="EN", manager_id=500),),
        venues=(VenueRecord(id=55, slug="emirates", name="Emirates Stadium", country_alpha2="EN"),),
        managers=(ManagerRecord(id=500, slug="arteta", name="Mikel Arteta", team_id=41),),
        manager_team_memberships=(ManagerTeamMembershipRecord(manager_id=500, team_id=41),),
        players=(PlayerRecord(id=1152, slug="bukayo-saka", name="Bukayo Saka", team_id=41, country_alpha2="EN", position="F"),),
        transfer_histories=(
            PlayerTransferHistoryRecord(
                id=1,
                player_id=1152,
                transfer_from_team_id=30,
                transfer_to_team_id=41,
                from_team_name="Academy",
                to_team_name="Arsenal",
                transfer_date_timestamp=1710000000,
                transfer_fee=0,
                transfer_fee_description="Free",
                transfer_fee_raw={"value": 0},
                type=1,
            ),
        ),
        player_season_statistics=(
            PlayerSeasonStatisticsRecord(
                player_id=1152,
                unique_tournament_id=17,
                season_id=76986,
                team_id=41,
                stat_type="overall",
                expected_goals=0.96,
                assists=1,
                statistics_payload={"type": "overall", "expectedGoals": 0.96, "assists": 1},
            ),
        ),
        entity_statistics_seasons=(
            EntityStatisticsSeasonRecord(subject_type="player", subject_id=1152, unique_tournament_id=17, season_id=76986, all_time_season_id=999),
            EntityStatisticsSeasonRecord(subject_type="team", subject_id=41, unique_tournament_id=17, season_id=76986, all_time_season_id=888),
        ),
        entity_statistics_types=(
            EntityStatisticsTypeRecord(subject_type="player", subject_id=1152, unique_tournament_id=17, season_id=76986, stat_type="summary"),
            EntityStatisticsTypeRecord(subject_type="team", subject_id=41, unique_tournament_id=17, season_id=76986, stat_type="overall"),
        ),
        season_statistics_types=(
            SeasonStatisticsTypeRecord(subject_type="player", unique_tournament_id=17, season_id=76986, stat_type="summary"),
        ),
    )


class EntitiesStorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_entities_repository_inherits_guarded_topology_writes(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EntitiesRepository()

        await repository.upsert_bundle(executor, bundle)
        await repository.upsert_bundle(executor, bundle)

        sport_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO sport" in sql]
        self.assertEqual(len(sport_statements), 1)

        category_sql = next(sql for sql, _ in executor.executemany_calls if "INSERT INTO category" in sql)
        unique_tournament_sql = next(
            sql for sql, _ in executor.executemany_calls if "INSERT INTO unique_tournament" in sql
        )
        season_sql = next(sql for sql, _ in executor.executemany_calls if "INSERT INTO season" in sql)
        self.assertIn("IS DISTINCT FROM", category_sql)
        self.assertIn("IS DISTINCT FROM", unique_tournament_sql)
        self.assertIn("IS DISTINCT FROM", season_sql)

    async def test_entities_repository_writes_expected_tables(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EntitiesRepository()

        result = await repository.upsert_bundle(executor, bundle)

        self.assertEqual(result.transfer_history_rows, 1)
        self.assertEqual(result.player_season_statistics_rows, 1)
        self.assertEqual(result.entity_statistics_season_rows, 2)
        statements = [sql for sql, _ in executor.executemany_calls]
        endpoint_registry_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO endpoint_registry" in sql)
        self.assertEqual(endpoint_registry_rows[0][6], "sofascore")
        self.assertEqual(endpoint_registry_rows[0][7], "v1")
        self.assertTrue(any("INSERT INTO category_transfer_period" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO unique_tournament_season" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO player_transfer_history" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO player_season_statistics" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO entity_statistics_season" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO entity_statistics_type" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_statistics_type" in sql for sql in statements))

    async def test_entities_ingest_job_uses_transaction_and_repository(self) -> None:
        bundle = _build_bundle()
        parser = _FakeParser(bundle)
        repository_result = EntitiesWriteResult(
            endpoint_registry_rows=11,
            payload_snapshot_rows=1,
            sport_rows=1,
            country_rows=1,
            category_rows=1,
            category_transfer_period_rows=1,
            unique_tournament_rows=1,
            season_rows=1,
            unique_tournament_season_rows=1,
            tournament_rows=1,
            team_rows=1,
            venue_rows=1,
            manager_rows=1,
            manager_team_membership_rows=1,
            player_rows=1,
            transfer_history_rows=1,
            player_season_statistics_rows=1,
            entity_statistics_season_rows=2,
            entity_statistics_type_rows=2,
            season_statistics_type_rows=1,
        )
        repository = _FakeRepository(repository_result)
        connection = object()
        database = _FakeDatabase(connection)
        job = EntitiesIngestJob(parser, repository, database)

        result = await job.run(player_ids=(1152,), team_ids=(41,), timeout=12.5)

        self.assertEqual(parser.calls, [((1152,), (), (41,), 12.5)])
        self.assertEqual(repository.calls, [(connection, bundle)])
        self.assertEqual(database.transaction_calls, 1)
        self.assertEqual(result.written.transfer_history_rows, 1)


if __name__ == "__main__":
    unittest.main()
