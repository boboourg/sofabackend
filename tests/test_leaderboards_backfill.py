from __future__ import annotations

import unittest

from schema_inspector.leaderboards_backfill_job import LeaderboardsBackfillJob
from schema_inspector.leaderboards_job import LeaderboardsIngestResult
from schema_inspector.leaderboards_repository import LeaderboardsWriteResult


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
                    "source_url": "https://www.sofascore.com/api/v1/unique-tournament/16/season/41087/top-players/overall"
                }
            ]
        if "SELECT seed.team_id" in normalized:
            return [{"team_id": 42}, {"team_id": 43}]
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
        self.calls: list[tuple[int, int, tuple[int, ...], float]] = []

    async def run(self, unique_tournament_id: int, season_id: int, **kwargs):
        self.calls.append(
            (
                unique_tournament_id,
                season_id,
                tuple(kwargs["team_top_players_team_ids"]),
                kwargs["timeout"],
            )
        )
        return LeaderboardsIngestResult(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            parsed=None,  # type: ignore[arg-type]
            written=LeaderboardsWriteResult(
                endpoint_registry_rows=0,
                payload_snapshot_rows=0,
                sport_rows=0,
                country_rows=0,
                category_rows=0,
                unique_tournament_rows=0,
                season_rows=0,
                tournament_rows=0,
                venue_rows=0,
                team_rows=0,
                player_rows=0,
                event_status_rows=0,
                event_rows=0,
                period_rows=0,
                team_of_the_week_rows=0,
                team_of_the_week_player_rows=0,
                season_group_rows=0,
                season_player_of_the_season_rows=0,
                season_statistics_type_rows=0,
                tournament_team_event_snapshot_rows=0,
                tournament_team_event_bucket_rows=0,
                top_player_snapshot_rows=0,
                top_player_entry_rows=0,
                top_team_snapshot_rows=0,
                top_team_entry_rows=0,
            ),
        )


class LeaderboardsBackfillTests(unittest.IsolatedAsyncioTestCase):
    async def test_leaderboards_backfill_filters_existing_seasons_and_passes_team_ids(self) -> None:
        connection = _FakeConnection()
        database = _FakeDatabase(connection)
        ingest_job = _FakeIngestJob()
        job = LeaderboardsBackfillJob(ingest_job, database)

        result = await job.run(season_limit=2, only_missing=True, timeout=12.5)

        self.assertEqual(result.total_candidates, 1)
        self.assertEqual(result.succeeded, 1)
        self.assertEqual(result.failed, 0)
        self.assertEqual(ingest_job.calls, [(17, 76986, (42, 43), 12.5)])
        self.assertEqual(result.items[0].unique_tournament_id, 17)
        self.assertEqual(result.items[0].season_id, 76986)
        self.assertEqual(result.items[0].team_top_players_team_ids, (42, 43))

    async def test_leaderboards_backfill_treats_zero_limit_as_unbounded(self) -> None:
        connection = _FakeConnection()
        database = _FakeDatabase(connection)
        ingest_job = _FakeIngestJob()
        job = LeaderboardsBackfillJob(ingest_job, database)

        result = await job.run(season_limit=0, only_missing=False)

        sql, args = connection.fetch_calls[0]
        self.assertNotIn("LIMIT $2", sql)
        self.assertEqual(args, (0,))
        self.assertEqual(result.total_candidates, 2)
        self.assertEqual(result.succeeded, 2)


if __name__ == "__main__":
    unittest.main()
