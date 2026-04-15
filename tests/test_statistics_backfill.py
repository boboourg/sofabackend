from __future__ import annotations

import unittest

from schema_inspector.statistics_backfill_job import StatisticsBackfillJob
from schema_inspector.statistics_job import StatisticsIngestResult
from schema_inspector.statistics_parser import StatisticsQuery
from schema_inspector.statistics_repository import StatisticsWriteResult


class _FakeConnection:
    def __init__(self) -> None:
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        normalized = " ".join(sql.split())
        if "SELECT DISTINCT e.unique_tournament_id, e.season_id" in normalized:
            return [
                {"unique_tournament_id": 17, "season_id": 76986},
                {"unique_tournament_id": 16, "season_id": 41087},
            ]
        if "FROM api_payload_snapshot" in normalized:
            return [
                {
                    "source_url": "https://www.sofascore.com/api/v1/unique-tournament/16/season/41087/statistics/info"
                },
            ]
        if "FROM season_statistics_snapshot" in normalized:
            return [
                {
                    "unique_tournament_id": 16,
                    "season_id": 41087,
                    "pages": 1,
                    "limit_value": 20,
                    "offset_value": 0,
                    "order_code": None,
                    "accumulation": None,
                    "group_code": None,
                    "fields": None,
                    "filters": None,
                },
            ]
        return []


class _FakeConnectionContext:
    def __init__(self, connection) -> None:
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        del exc_type, exc, tb
        return None


class _FakeDatabase:
    def __init__(self, connection) -> None:
        self.connection_obj = connection

    def connection(self):
        return _FakeConnectionContext(self.connection_obj)


class _FakeIngestJob:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int, tuple[StatisticsQuery, ...], bool, float]] = []

    async def run(self, unique_tournament_id: int, season_id: int, **kwargs):
        self.calls.append(
            (
                unique_tournament_id,
                season_id,
                tuple(kwargs["queries"]),
                kwargs["include_info"],
                kwargs["timeout"],
            )
        )
        return StatisticsIngestResult(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            queries=tuple(kwargs["queries"]),
            parsed=None,  # type: ignore[arg-type]
            written=StatisticsWriteResult(
                endpoint_registry_rows=0,
                payload_snapshot_rows=0,
                sport_rows=0,
                team_rows=0,
                player_rows=0,
                config_rows=0,
                config_team_rows=0,
                nationality_rows=0,
                group_item_rows=0,
                snapshot_rows=0,
                result_rows=0,
            ),
        )


class StatisticsBackfillTests(unittest.IsolatedAsyncioTestCase):
    async def test_statistics_backfill_filters_existing_seasons_and_uses_default_query(self) -> None:
        connection = _FakeConnection()
        database = _FakeDatabase(connection)
        ingest_job = _FakeIngestJob()
        job = StatisticsBackfillJob(ingest_job, database)

        result = await job.run(season_limit=2, only_missing=True, timeout=12.5)

        self.assertEqual(result.total_candidates, 1)
        self.assertEqual(result.succeeded, 1)
        self.assertEqual(result.failed, 0)
        self.assertEqual(
            ingest_job.calls,
            [(17, 76986, (StatisticsQuery(limit=20, offset=0),), True, 12.5)],
        )
        self.assertEqual(result.items[0].unique_tournament_id, 17)
        self.assertEqual(result.items[0].season_id, 76986)

    async def test_statistics_backfill_treats_zero_limit_as_unbounded(self) -> None:
        connection = _FakeConnection()
        database = _FakeDatabase(connection)
        ingest_job = _FakeIngestJob()
        job = StatisticsBackfillJob(ingest_job, database)

        result = await job.run(season_limit=0, only_missing=False)

        sql, args = connection.fetch_calls[0]
        self.assertNotIn("LIMIT $2", sql)
        self.assertEqual(args, (0,))
        self.assertEqual(result.total_candidates, 2)
        self.assertEqual(result.succeeded, 2)

    async def test_statistics_backfill_reprocesses_incomplete_paged_snapshots(self) -> None:
        class _IncompletePagesConnection(_FakeConnection):
            async def fetch(self, sql: str, *args):
                self.fetch_calls.append((sql, args))
                normalized = " ".join(sql.split())
                if "SELECT DISTINCT e.unique_tournament_id, e.season_id" in normalized:
                    return [{"unique_tournament_id": 16, "season_id": 41087}]
                if "FROM api_payload_snapshot" in normalized:
                    return [
                        {
                            "source_url": "https://www.sofascore.com/api/v1/unique-tournament/16/season/41087/statistics/info"
                        }
                    ]
                if "FROM season_statistics_snapshot" in normalized:
                    return [
                        {
                            "unique_tournament_id": 16,
                            "season_id": 41087,
                            "pages": 2,
                            "limit_value": 20,
                            "offset_value": 0,
                            "order_code": None,
                            "accumulation": None,
                            "group_code": None,
                            "fields": None,
                            "filters": None,
                        }
                    ]
                return []

        connection = _IncompletePagesConnection()
        database = _FakeDatabase(connection)
        ingest_job = _FakeIngestJob()
        job = StatisticsBackfillJob(ingest_job, database)

        result = await job.run(season_limit=1, only_missing=True)

        self.assertEqual(result.total_candidates, 1)
        self.assertEqual(result.succeeded, 1)
        self.assertEqual(ingest_job.calls, [(16, 41087, (StatisticsQuery(limit=20, offset=0),), True, 20.0)])


if __name__ == "__main__":
    unittest.main()
