from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from schema_inspector.entities_backfill_job import EntitiesBackfillJob
from schema_inspector.entities_job import EntitiesIngestResult
from schema_inspector.entities_parser import (
    PlayerHeatmapRequest,
    PlayerOverallRequest,
    TeamOverallRequest,
    TeamPerformanceGraphRequest,
)
from schema_inspector.entities_repository import EntitiesWriteResult


class _FakeConnection:
    def __init__(self) -> None:
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        normalized = " ".join(sql.split())
        if "SELECT seed.player_id AS id" in normalized:
            return [{"id": 1152}, {"id": 2001}]
        if "SELECT seed.team_id AS id" in normalized:
            return [{"id": 41}, {"id": 43}]
        if "SELECT seed.player_id, seed.unique_tournament_id, seed.season_id" in normalized:
            return [
                {"player_id": 1152, "unique_tournament_id": 17, "season_id": 76986},
                {"player_id": 2001, "unique_tournament_id": 17, "season_id": 76986},
            ]
        if "FROM player AS p" in normalized:
            return [{"id": 1152}, {"id": 2001}]
        if "FROM team AS t" in normalized:
            return [{"id": 41}, {"id": 43}]
        if "FROM season_statistics_result AS r" in normalized:
            return [
                {"player_id": 1152, "unique_tournament_id": 17, "season_id": 76986},
                {"player_id": 2001, "unique_tournament_id": 17, "season_id": 76986},
            ]
        if "FROM ( SELECT DISTINCT e.home_team_id AS team_id" in normalized:
            return [
                {"team_id": 42, "unique_tournament_id": 17, "season_id": 76986},
                {"team_id": 43, "unique_tournament_id": 17, "season_id": 76986},
            ]
        if "FROM api_payload_snapshot" in normalized:
            endpoint_pattern = args[0]
            urls = set(args[1])
            existing = []
            if endpoint_pattern == "/api/v1/player/{player_id}" and "https://www.sofascore.com/api/v1/player/2001" in urls:
                existing.append({"source_url": "https://www.sofascore.com/api/v1/player/2001"})
            if (
                endpoint_pattern == "/api/v1/player/{player_id}/statistics"
                and "https://www.sofascore.com/api/v1/player/2001/statistics" in urls
            ):
                existing.append({"source_url": "https://www.sofascore.com/api/v1/player/2001/statistics"})
            if endpoint_pattern == "/api/v1/team/{team_id}" and "https://www.sofascore.com/api/v1/team/43" in urls:
                existing.append({"source_url": "https://www.sofascore.com/api/v1/team/43"})
            if (
                endpoint_pattern
                == "/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall"
                and "https://www.sofascore.com/api/v1/player/2001/unique-tournament/17/season/76986/statistics/overall"
                in urls
            ):
                existing.append(
                    {
                        "source_url": "https://www.sofascore.com/api/v1/player/2001/unique-tournament/17/season/76986/statistics/overall"
                    }
                )
            if (
                endpoint_pattern
                == "/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall"
                and "https://www.sofascore.com/api/v1/team/43/unique-tournament/17/season/76986/statistics/overall"
                in urls
            ):
                existing.append(
                    {
                        "source_url": "https://www.sofascore.com/api/v1/team/43/unique-tournament/17/season/76986/statistics/overall"
                    }
                )
            return existing
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
        self.calls: list[dict[str, object]] = []

    async def run(self, **kwargs):
        self.calls.append(kwargs)
        return EntitiesIngestResult(
            parsed=None,  # type: ignore[arg-type]
            written=EntitiesWriteResult(
                endpoint_registry_rows=0,
                payload_snapshot_rows=0,
                sport_rows=0,
                country_rows=0,
                category_rows=0,
                category_transfer_period_rows=0,
                unique_tournament_rows=0,
                season_rows=0,
                unique_tournament_season_rows=0,
                tournament_rows=0,
                team_rows=0,
                venue_rows=0,
                manager_rows=0,
                manager_team_membership_rows=0,
                player_rows=0,
                transfer_history_rows=0,
                player_season_statistics_rows=0,
                entity_statistics_season_rows=0,
                entity_statistics_type_rows=0,
                season_statistics_type_rows=0,
            ),
        )


class EntitiesBackfillTests(unittest.IsolatedAsyncioTestCase):
    async def test_entities_backfill_filters_existing_source_urls(self) -> None:
        connection = _FakeConnection()
        database = _FakeDatabase(connection)
        ingest_job = _FakeIngestJob()
        job = EntitiesBackfillJob(ingest_job, database)

        result = await job.run(
            player_limit=2,
            team_limit=2,
            player_request_limit=2,
            team_request_limit=2,
            only_missing=True,
            timeout=12.5,
        )

        self.assertEqual(result.player_ids, (1152,))
        self.assertEqual(result.player_statistics_ids, (1152,))
        self.assertEqual(result.team_ids, (41,))
        self.assertEqual(
            result.player_overall_requests,
            (PlayerOverallRequest(player_id=1152, unique_tournament_id=17, season_id=76986),),
        )
        self.assertEqual(
            result.player_heatmap_requests,
            (
                PlayerHeatmapRequest(player_id=1152, unique_tournament_id=17, season_id=76986),
                PlayerHeatmapRequest(player_id=2001, unique_tournament_id=17, season_id=76986),
            ),
        )
        self.assertEqual(
            result.team_overall_requests,
            (TeamOverallRequest(team_id=42, unique_tournament_id=17, season_id=76986),),
        )
        self.assertEqual(
            result.team_performance_graph_requests,
            (
                TeamPerformanceGraphRequest(team_id=42, unique_tournament_id=17, season_id=76986),
                TeamPerformanceGraphRequest(team_id=43, unique_tournament_id=17, season_id=76986),
            ),
        )
        self.assertEqual(len(ingest_job.calls), 1)
        self.assertEqual(ingest_job.calls[0]["player_ids"], (1152,))
        self.assertEqual(ingest_job.calls[0]["player_statistics_ids"], (1152,))
        self.assertEqual(ingest_job.calls[0]["team_ids"], (41,))
        self.assertEqual(ingest_job.calls[0]["timeout"], 12.5)

    async def test_entities_backfill_treats_zero_limits_as_unbounded(self) -> None:
        connection = _FakeConnection()
        database = _FakeDatabase(connection)
        ingest_job = _FakeIngestJob()
        job = EntitiesBackfillJob(ingest_job, database)

        result = await job.run(
            player_limit=0,
            team_limit=0,
            player_request_limit=0,
            team_request_limit=0,
            only_missing=False,
        )

        self.assertEqual(connection.fetch_calls[0][1], (0,))
        self.assertEqual(connection.fetch_calls[1][1], (0,))
        self.assertEqual(connection.fetch_calls[2][1], (0,))
        self.assertEqual(connection.fetch_calls[3][1], (0,))
        self.assertEqual(connection.fetch_calls[4][1], (0,))
        self.assertEqual(result.player_ids, (1152, 2001))
        self.assertEqual(result.player_statistics_ids, (1152, 2001))
        self.assertEqual(result.team_ids, (41, 43))

    async def test_entities_backfill_only_missing_defaults_to_recent_event_window_when_unscoped(self) -> None:
        connection = _FakeConnection()
        database = _FakeDatabase(connection)
        ingest_job = _FakeIngestJob()
        fixed_now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
        expected_from = int((fixed_now - timedelta(days=180)).timestamp())
        expected_to = int((fixed_now + timedelta(days=7)).timestamp())
        job = EntitiesBackfillJob(ingest_job, database, now_factory=lambda: fixed_now)

        await job.run(only_missing=True)

        seed_calls = [
            call for call in connection.fetch_calls if "FROM api_payload_snapshot" not in " ".join(call[0].split())
        ]
        self.assertIn("FROM (", " ".join(seed_calls[0][0].split()))
        self.assertEqual(seed_calls[0][1], (expected_from, expected_to, None, 0))
        self.assertEqual(seed_calls[1][1], (expected_from, expected_to, None, 0))
        self.assertEqual(seed_calls[2][1], (expected_from, expected_to, None, 0))
        self.assertEqual(seed_calls[3][1], (expected_from, expected_to, None, 0))


if __name__ == "__main__":
    unittest.main()
