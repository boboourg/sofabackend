from __future__ import annotations

import asyncio
import unittest

from schema_inspector.services.leaderboards_backfill import (
    LeaderboardsBackfillReport,
    run_leaderboards_backfill,
)


class _FakeConn:
    def __init__(self, *, pos_pairs, overall_pairs, regular_pairs):
        self._pos = pos_pairs
        self._overall = overall_pairs
        self._regular = regular_pairs
        self.fetch_calls: list[str] = []

    async def fetch(self, sql, *args):
        self.fetch_calls.append(sql)
        if "season_player_of_the_season" in sql:
            return [{"ut": p[0], "s": p[1]} for p in self._pos]
        if "regularSeason" in sql or "ANY($1::bigint[])" in sql:
            return [{"ut": p[0], "s": p[1]} for p in self._regular]
        return [{"ut": p[0], "s": p[1]} for p in self._overall]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeDatabase:
    def __init__(self, **kwargs):
        self._kwargs = kwargs
        self.connection_calls = 0

    def connection(self):
        self.connection_calls += 1
        return _FakeConn(**self._kwargs)


class _RecordingQueue:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    def publish(self, stream, values):
        self.published.append((stream, dict(values)))
        return f"id-{len(self.published)}"


class LeaderboardsBackfillTests(unittest.TestCase):
    def test_publishes_envelope_per_missing_target(self) -> None:
        db = _FakeDatabase(
            pos_pairs=[(17, 61627), (8, 60000)],
            overall_pairs=[(17, 61627)],
            regular_pairs=[(132, 80229)],
        )
        queue = _RecordingQueue()
        report = asyncio.run(
            run_leaderboards_backfill(
                database=db,
                stream_queue=queue,
                sport_slug="football",
                priority_rank_threshold=30,
            )
        )
        # 2 POS + 1 top-players-overall + 1 top-teams-overall + 1 reg-players + 1 reg-teams
        self.assertEqual(report.pos_published, 2)
        self.assertEqual(report.top_players_overall_published, 1)
        self.assertEqual(report.top_teams_overall_published, 1)
        self.assertEqual(report.top_players_regular_season_published, 1)
        self.assertEqual(report.top_teams_regular_season_published, 1)
        self.assertEqual(report.total, 6)
        self.assertEqual(len(queue.published), 6)
        # All envelopes go to stream:etl:resource_refresh
        for stream, _ in queue.published:
            self.assertEqual(stream, "stream:etl:resource_refresh")

    def test_zero_pairs_publishes_nothing(self) -> None:
        db = _FakeDatabase(pos_pairs=[], overall_pairs=[], regular_pairs=[])
        queue = _RecordingQueue()
        report = asyncio.run(
            run_leaderboards_backfill(
                database=db,
                stream_queue=queue,
                sport_slug="football",
            )
        )
        self.assertEqual(report.total, 0)
        self.assertEqual(queue.published, [])

    def test_envelope_path_params_carry_ut_and_season(self) -> None:
        db = _FakeDatabase(
            pos_pairs=[(17, 61627)],
            overall_pairs=[],
            regular_pairs=[],
        )
        queue = _RecordingQueue()
        asyncio.run(
            run_leaderboards_backfill(
                database=db,
                stream_queue=queue,
                sport_slug="football",
            )
        )
        self.assertEqual(len(queue.published), 1)
        _, values = queue.published[0]
        # JobEnvelope encodes params as JSON in params_json field.
        import json
        params = json.loads(values["params_json"])
        self.assertEqual(params["endpoint_pattern"],
                         "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season")
        self.assertEqual(params["path_params"], {"unique_tournament_id": 17, "season_id": 61627})

    def test_skips_when_queue_is_none(self) -> None:
        db = _FakeDatabase(pos_pairs=[(17, 61627)], overall_pairs=[], regular_pairs=[])
        report = asyncio.run(
            run_leaderboards_backfill(
                database=db,
                stream_queue=None,
                sport_slug="football",
            )
        )
        self.assertEqual(report.total, 0)


if __name__ == "__main__":
    unittest.main()
