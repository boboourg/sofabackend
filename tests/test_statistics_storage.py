from __future__ import annotations

import unittest

from schema_inspector.competition_parser import ApiPayloadSnapshotRecord, SportRecord
from schema_inspector.endpoints import statistics_registry_entries
from schema_inspector.statistics_job import StatisticsIngestJob
from schema_inspector.statistics_parser import (
    SeasonStatisticsConfigRecord,
    SeasonStatisticsConfigTeamRecord,
    SeasonStatisticsGroupItemRecord,
    SeasonStatisticsNationalityRecord,
    SeasonStatisticsResultRecord,
    SeasonStatisticsSnapshotRecord,
    StatisticsBundle,
    StatisticsPlayerRecord,
    StatisticsQuery,
    StatisticsTeamRecord,
)
from schema_inspector.statistics_repository import StatisticsRepository, StatisticsWriteResult


class _FakeExecutor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self._next_snapshot_id = 1

    async def executemany(self, command: str, args):
        self.executemany_calls.append((command, list(args)))
        return None

    async def fetchval(self, query: str, *args):
        self.fetchval_calls.append((query, args))
        snapshot_id = self._next_snapshot_id
        self._next_snapshot_id += 1
        return snapshot_id


class _FakeParser:
    def __init__(self, bundle: StatisticsBundle) -> None:
        self.bundle = bundle
        self.calls: list[tuple[int, int, tuple[StatisticsQuery, ...], bool, float]] = []

    async def fetch_bundle(
        self,
        unique_tournament_id: int,
        season_id: int,
        *,
        queries=(),
        include_info: bool = True,
        timeout: float = 20.0,
    ) -> StatisticsBundle:
        self.calls.append((unique_tournament_id, season_id, tuple(queries), include_info, timeout))
        return self.bundle


class _FakeRepository:
    def __init__(self, result: StatisticsWriteResult) -> None:
        self.result = result
        self.calls: list[tuple[object, StatisticsBundle]] = []

    async def upsert_bundle(self, executor, bundle: StatisticsBundle) -> StatisticsWriteResult:
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


def _build_bundle() -> StatisticsBundle:
    query = StatisticsQuery(
        limit=20,
        order="-rating",
        accumulation="per90",
        group="summary",
        fields=("goals", "tackles"),
        filters=("team.in.42~40",),
    )
    return StatisticsBundle(
        registry_entries=statistics_registry_entries(),
        payload_snapshots=(
            ApiPayloadSnapshotRecord(
                endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/info",
                source_url="https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/statistics/info",
                envelope_key="hideHomeAndAway,teams,statisticsGroups,nationalities",
                context_entity_type="season",
                context_entity_id=76986,
                payload={"hideHomeAndAway": False},
                fetched_at="2026-04-10T10:00:00+00:00",
            ),
        ),
        sports=(SportRecord(id=1, slug="football", name="Football"),),
        teams=(
            StatisticsTeamRecord(id=42, slug="arsenal", name="Arsenal", sport_id=1, type=0, national=False),
            StatisticsTeamRecord(id=40, slug="liverpool", name="Liverpool", sport_id=1, type=0, national=False),
        ),
        players=(
            StatisticsPlayerRecord(id=1445189, slug="tom-edozie", name="Tom Edozie", team_id=42, gender="M"),
        ),
        configs=(SeasonStatisticsConfigRecord(unique_tournament_id=17, season_id=76986, hide_home_and_away=False),),
        config_teams=(
            SeasonStatisticsConfigTeamRecord(unique_tournament_id=17, season_id=76986, team_id=42, ordinal=0),
            SeasonStatisticsConfigTeamRecord(unique_tournament_id=17, season_id=76986, team_id=40, ordinal=1),
        ),
        nationalities=(
            SeasonStatisticsNationalityRecord(
                unique_tournament_id=17,
                season_id=76986,
                nationality_code="EN",
                nationality_name="England",
            ),
        ),
        group_items=(
            SeasonStatisticsGroupItemRecord(
                unique_tournament_id=17,
                season_id=76986,
                group_scope="regular",
                group_name="summary",
                stat_field="rating",
                ordinal=0,
            ),
        ),
        snapshots=(
            SeasonStatisticsSnapshotRecord(
                endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics?limit={limit}&offset={offset}&order={order}&accumulation={accumulation}&group={group}&fields={fields}&filters={filters}",
                unique_tournament_id=17,
                season_id=76986,
                source_url="https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/statistics?limit=20&order=-rating&accumulation=per90&group=summary&fields=goals%2Ctackles&filters=team.in.42~40",
                page=1,
                pages=27,
                limit_value=query.limit,
                offset_value=query.offset,
                order_code=query.order,
                accumulation=query.accumulation,
                group_code=query.group,
                fields=query.parsed_fields(),
                filters=query.parsed_filters(),
                fetched_at="2026-04-10T10:00:00+00:00",
                results=(
                    SeasonStatisticsResultRecord(
                        row_number=1,
                        player_id=1445189,
                        team_id=42,
                        rating=7.6,
                        goals=6,
                        tackles=6,
                        total_shots=4,
                        shots_on_target=2,
                        total_passes=88,
                        minutes_played=166,
                        red_cards=1,
                        yellow_cards=2,
                        accurate_passes_percentage=66.67,
                    ),
                ),
            ),
        ),
    )


class StatisticsStorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_statistics_repository_writes_expected_tables(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = StatisticsRepository()

        result = await repository.upsert_bundle(executor, bundle)

        self.assertEqual(result.snapshot_rows, 1)
        self.assertEqual(result.result_rows, 1)
        statements = [sql for sql, _ in executor.executemany_calls]
        endpoint_registry_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO endpoint_registry" in sql)
        self.assertEqual(endpoint_registry_rows[0][6], "sofascore")
        self.assertEqual(endpoint_registry_rows[0][7], "v1")
        self.assertTrue(any("INSERT INTO season_statistics_config " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_statistics_config_team " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_statistics_nationality " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_statistics_group_item " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_statistics_result " in sql for sql in statements))
        self.assertEqual(len(executor.fetchval_calls), 1)
        self.assertIn("INSERT INTO season_statistics_snapshot", executor.fetchval_calls[0][0])

    async def test_statistics_ingest_job_uses_transaction_and_repository(self) -> None:
        bundle = _build_bundle()
        query = StatisticsQuery(limit=20, order="-rating", accumulation="per90", group="summary")
        parser = _FakeParser(bundle)
        repository_result = StatisticsWriteResult(
            endpoint_registry_rows=2,
            payload_snapshot_rows=1,
            sport_rows=1,
            team_rows=2,
            player_rows=1,
            config_rows=1,
            config_team_rows=2,
            nationality_rows=1,
            group_item_rows=1,
            snapshot_rows=1,
            result_rows=1,
        )
        repository = _FakeRepository(repository_result)
        connection = object()
        database = _FakeDatabase(connection)
        job = StatisticsIngestJob(parser, repository, database)

        result = await job.run(17, 76986, queries=(query,), include_info=True, timeout=12.5)

        self.assertEqual(parser.calls, [(17, 76986, (query,), True, 12.5)])
        self.assertEqual(repository.calls, [(connection, bundle)])
        self.assertEqual(database.transaction_calls, 1)
        self.assertEqual(result.unique_tournament_id, 17)
        self.assertEqual(result.season_id, 76986)
        self.assertEqual(result.queries, (query,))
        self.assertEqual(result.written.snapshot_rows, 1)


if __name__ == "__main__":
    unittest.main()
