from __future__ import annotations

import unittest

from schema_inspector.standings_backfill_job import StandingsBackfillJob
from schema_inspector.standings_job import StandingsIngestResult
from schema_inspector.standings_repository import StandingsWriteResult


class _FakeConnection:
    def __init__(self) -> None:
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        normalized = " ".join(sql.split())
        if "SELECT DISTINCT e.unique_tournament_id, e.tournament_id, e.season_id" in normalized:
            return [
                {"unique_tournament_id": 17, "tournament_id": 1, "season_id": 76986},
                {"unique_tournament_id": None, "tournament_id": 99, "season_id": 41087},
            ]
        if "FROM api_payload_snapshot" in normalized:
            return [
                {
                    "source_url": "https://www.sofascore.com/api/v1/tournament/99/season/41087/standings/total"
                }
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
        self.unique_tournament_calls: list[tuple[int, int, tuple[str, ...], float]] = []
        self.tournament_calls: list[tuple[int, int, tuple[str, ...], float]] = []

    async def run_for_unique_tournament(self, unique_tournament_id: int, season_id: int, **kwargs):
        self.unique_tournament_calls.append(
            (unique_tournament_id, season_id, tuple(kwargs["scopes"]), kwargs["timeout"])
        )
        return StandingsIngestResult(
            source_kind="unique_tournament",
            source_id=unique_tournament_id,
            season_id=season_id,
            scopes=tuple(kwargs["scopes"]),
            parsed=None,  # type: ignore[arg-type]
            written=_build_write_result(),
        )

    async def run_for_tournament(self, tournament_id: int, season_id: int, **kwargs):
        self.tournament_calls.append((tournament_id, season_id, tuple(kwargs["scopes"]), kwargs["timeout"]))
        return StandingsIngestResult(
            source_kind="tournament",
            source_id=tournament_id,
            season_id=season_id,
            scopes=tuple(kwargs["scopes"]),
            parsed=None,  # type: ignore[arg-type]
            written=_build_write_result(),
        )


def _build_write_result() -> StandingsWriteResult:
    return StandingsWriteResult(
        endpoint_registry_rows=0,
        payload_snapshot_rows=0,
        sport_rows=0,
        country_rows=0,
        category_rows=0,
        unique_tournament_rows=0,
        tournament_rows=0,
        team_rows=0,
        tie_breaking_rule_rows=0,
        promotion_rows=0,
        standing_rows=0,
        standing_row_rows=0,
    )


class StandingsBackfillTests(unittest.IsolatedAsyncioTestCase):
    async def test_standings_backfill_filters_existing_seasons_and_prefers_unique_tournament(self) -> None:
        connection = _FakeConnection()
        database = _FakeDatabase(connection)
        ingest_job = _FakeIngestJob()
        job = StandingsBackfillJob(ingest_job, database)

        result = await job.run(season_limit=2, only_missing=True, scopes=("total",), timeout=12.5)

        self.assertEqual(result.total_candidates, 1)
        self.assertEqual(result.succeeded, 1)
        self.assertEqual(result.failed, 0)
        self.assertEqual(ingest_job.unique_tournament_calls, [(17, 76986, ("total",), 12.5)])
        self.assertEqual(ingest_job.tournament_calls, [])
        self.assertEqual(result.items[0].source_kind, "unique_tournament")
        self.assertEqual(result.items[0].source_id, 17)
        self.assertEqual(result.items[0].season_id, 76986)

    async def test_standings_backfill_treats_zero_limit_as_unbounded(self) -> None:
        connection = _FakeConnection()
        database = _FakeDatabase(connection)
        ingest_job = _FakeIngestJob()
        job = StandingsBackfillJob(ingest_job, database)

        result = await job.run(season_limit=0, only_missing=False, scopes=("total",))

        sql, args = connection.fetch_calls[0]
        self.assertNotIn("LIMIT $2", sql)
        self.assertEqual(args, (0,))
        self.assertEqual(result.total_candidates, 2)
        self.assertEqual(result.succeeded, 2)


if __name__ == "__main__":
    unittest.main()
