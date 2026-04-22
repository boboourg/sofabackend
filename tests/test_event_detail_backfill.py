from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from schema_inspector.event_detail_backfill_job import EventDetailBackfillJob
from schema_inspector.event_detail_job import EventDetailIngestProfile, EventDetailIngestResult
from schema_inspector.event_detail_repository import EventDetailWriteResult


class _FakeConnection:
    def __init__(self, rows=None, *, fetch_results=None):
        self.rows = rows if rows is not None else []
        self.fetch_results = list(fetch_results or [])
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        if self.fetch_results:
            return self.fetch_results.pop(0)
        return self.rows


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


class _FakeDetailJob:
    def __init__(self, failing_ids=(), *, profiles_by_event=None) -> None:
        self.failing_ids = set(failing_ids)
        self.profiles_by_event = dict(profiles_by_event or {})
        self.calls: list[tuple[int, tuple[int, ...], float]] = []

    async def run(self, event_id: int, *, provider_ids=(1,), timeout: float = 20.0):
        self.calls.append((event_id, tuple(provider_ids), timeout))
        if event_id in self.failing_ids:
            raise RuntimeError(f"boom-{event_id}")
        return EventDetailIngestResult(
            event_id=event_id,
            provider_ids=tuple(provider_ids),
            parsed=None,  # type: ignore[arg-type]
            profile=self.profiles_by_event.get(
                event_id,
                EventDetailIngestProfile(
                    upstream_fetch_ms=0,
                    parse_ms=0,
                    registry_sync_ms=0,
                    db_persist_ms=0,
                ),
            ),
            written=EventDetailWriteResult(
                endpoint_registry_rows=0,
                payload_snapshot_rows=0,
                sport_rows=0,
                country_rows=0,
                category_rows=0,
                unique_tournament_rows=0,
                season_rows=0,
                tournament_rows=0,
                team_rows=0,
                venue_rows=0,
                referee_rows=0,
                manager_rows=0,
                manager_performance_rows=0,
                manager_team_membership_rows=0,
                player_rows=0,
                event_status_rows=0,
                event_rows=0,
                event_round_info_rows=0,
                event_status_time_rows=0,
                event_time_rows=0,
                event_var_in_progress_rows=0,
                event_score_rows=0,
                event_filter_value_rows=0,
                event_change_item_rows=0,
                event_manager_assignment_rows=0,
                event_duel_rows=0,
                event_pregame_form_rows=0,
                event_pregame_form_side_rows=0,
                event_pregame_form_item_rows=0,
                event_vote_option_rows=0,
                event_comment_feed_rows=0,
                event_comment_rows=0,
                event_graph_rows=0,
                event_graph_point_rows=0,
                event_team_heatmap_rows=0,
                event_team_heatmap_point_rows=0,
                provider_rows=0,
                provider_configuration_rows=0,
                event_market_rows=0,
                event_market_choice_rows=0,
                event_winning_odds_rows=0,
                event_lineup_rows=0,
                event_lineup_player_rows=0,
                event_lineup_missing_player_rows=0,
            ),
        )


class EventDetailBackfillTests(unittest.IsolatedAsyncioTestCase):
    async def test_backfill_job_loads_missing_event_ids_and_collects_results(self) -> None:
        connection = _FakeConnection(
            fetch_results=[
                [{"id": 14083191}, {"id": 14083192}],
                [{"scope_key": "event:14083192:/api/v1/event/{event_id}"}],
            ]
        )
        database = _FakeDatabase(connection)
        detail_job = _FakeDetailJob(failing_ids=(14083192,))
        fixed_now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
        expected_from = int((fixed_now - timedelta(days=180)).timestamp())
        expected_to = int((fixed_now + timedelta(days=7)).timestamp())
        job = EventDetailBackfillJob(detail_job, database, now_factory=lambda: fixed_now)

        result = await job.run(limit=2, offset=5, only_missing=True, provider_ids=(1, 2, 1), concurrency=2, timeout=12.5)

        self.assertEqual(len(connection.fetch_calls), 2)
        candidate_sql, candidate_args = connection.fetch_calls[0]
        self.assertIn("FROM event AS e", candidate_sql)
        self.assertNotIn("api_payload_snapshot", candidate_sql)
        self.assertEqual(candidate_args, (None, None, None, expected_from, expected_to, 5, 1000))
        head_sql, head_args = connection.fetch_calls[1]
        self.assertIn("FROM api_snapshot_head", head_sql)
        self.assertEqual(
            head_args,
            (["event:14083191:/api/v1/event/{event_id}", "event:14083192:/api/v1/event/{event_id}"],),
        )
        self.assertEqual(detail_job.calls, [(14083191, (1, 2), 12.5)])
        self.assertEqual(result.total_candidates, 1)
        self.assertEqual(result.processed, 1)
        self.assertEqual(result.succeeded, 1)
        self.assertEqual(result.failed, 0)
        self.assertEqual(result.items[0].event_id, 14083191)

    async def test_backfill_job_treats_zero_limit_as_unbounded(self) -> None:
        connection = _FakeConnection(rows=[{"id": 14083191}])
        database = _FakeDatabase(connection)
        detail_job = _FakeDetailJob()
        job = EventDetailBackfillJob(detail_job, database)

        result = await job.run(limit=0, offset=7, only_missing=False)

        self.assertEqual(
            connection.fetch_calls,
            [(connection.fetch_calls[0][0], (None, None, None, None, None, 7, 1000))],
        )
        self.assertEqual(result.total_candidates, 1)
        self.assertEqual(detail_job.calls, [(14083191, (1,), 20.0)])

    async def test_backfill_job_applies_unique_tournament_filter(self) -> None:
        connection = _FakeConnection(rows=[{"id": 14083191}])
        database = _FakeDatabase(connection)
        detail_job = _FakeDetailJob()
        job = EventDetailBackfillJob(detail_job, database)

        result = await job.run(limit=3, offset=0, only_missing=False, unique_tournament_id=17)

        self.assertEqual(
            connection.fetch_calls,
            [(connection.fetch_calls[0][0], (17, None, None, None, None, 0, 1000))],
        )
        self.assertEqual(result.total_candidates, 1)
        self.assertEqual(detail_job.calls, [(14083191, (1,), 20.0)])

    async def test_backfill_job_applies_season_filter(self) -> None:
        connection = _FakeConnection(rows=[{"id": 14083191}])
        database = _FakeDatabase(connection)
        detail_job = _FakeDetailJob()
        job = EventDetailBackfillJob(detail_job, database)

        result = await job.run(
            limit=3,
            offset=0,
            only_missing=False,
            unique_tournament_id=17,
            season_ids=(701, 702),
        )

        self.assertEqual(
            connection.fetch_calls,
            [(connection.fetch_calls[0][0], (17, None, [701, 702], None, None, 0, 1000))],
        )
        self.assertEqual(result.total_candidates, 1)
        self.assertEqual(detail_job.calls, [(14083191, (1,), 20.0)])

    async def test_event_detail_backfill_only_missing_defaults_to_recent_window_when_unscoped(self) -> None:
        connection = _FakeConnection(fetch_results=[[{"id": 14083191}], []])
        database = _FakeDatabase(connection)
        detail_job = _FakeDetailJob()
        fixed_now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
        expected_from = int((fixed_now - timedelta(days=180)).timestamp())
        expected_to = int((fixed_now + timedelta(days=7)).timestamp())
        job = EventDetailBackfillJob(detail_job, database, now_factory=lambda: fixed_now)

        await job.run(only_missing=True)

        self.assertEqual(len(connection.fetch_calls), 2)
        self.assertEqual(connection.fetch_calls[0][1], (None, None, None, expected_from, expected_to, 0, 1000))
        self.assertEqual(
            connection.fetch_calls[1][1],
            (["event:14083191:/api/v1/event/{event_id}"],),
        )

    async def test_backfill_job_loads_additional_candidate_pages_when_first_page_is_already_hydrated(self) -> None:
        connection = _FakeConnection(
            fetch_results=[
                [{"id": 14083191}, {"id": 14083192}],
                [
                    {"scope_key": "event:14083191:/api/v1/event/{event_id}"},
                    {"scope_key": "event:14083192:/api/v1/event/{event_id}"},
                ],
                [{"id": 14083193}, {"id": 14083194}],
                [{"scope_key": "event:14083194:/api/v1/event/{event_id}"}],
            ]
        )
        database = _FakeDatabase(connection)
        detail_job = _FakeDetailJob()
        fixed_now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
        expected_from = int((fixed_now - timedelta(days=180)).timestamp())
        expected_to = int((fixed_now + timedelta(days=7)).timestamp())
        job = EventDetailBackfillJob(detail_job, database, now_factory=lambda: fixed_now)

        with patch(
            "schema_inspector.event_detail_backfill_job._candidate_page_size",
            return_value=2,
        ):
            result = await job.run(limit=1, only_missing=True)

        self.assertEqual(
            [call[1] for call in connection.fetch_calls[::2]],
            [
                (None, None, None, expected_from, expected_to, 0, 2),
                (None, None, None, expected_from, expected_to, 2, 2),
            ],
        )
        self.assertEqual(detail_job.calls, [(14083193, (1,), 20.0)])
        self.assertEqual(result.total_candidates, 1)

    async def test_backfill_job_logs_aggregated_batch_profile_metrics(self) -> None:
        connection = _FakeConnection(rows=[{"id": 14083191}, {"id": 14083192}])
        database = _FakeDatabase(connection)
        detail_job = _FakeDetailJob(
            profiles_by_event={
                14083191: EventDetailIngestProfile(upstream_fetch_ms=11, parse_ms=22, registry_sync_ms=3, db_persist_ms=33),
                14083192: EventDetailIngestProfile(upstream_fetch_ms=7, parse_ms=8, registry_sync_ms=4, db_persist_ms=9),
            }
        )
        job = EventDetailBackfillJob(detail_job, database)

        with self.assertLogs("schema_inspector.event_detail_backfill_job", level="INFO") as captured:
            result = await job.run(limit=2, only_missing=False)

        self.assertEqual(result.upstream_fetch_ms, 18)
        self.assertEqual(result.parse_ms, 30)
        self.assertEqual(result.registry_sync_ms, 7)
        self.assertEqual(result.db_persist_ms, 42)
        self.assertTrue(
            any(
                "Batch completed. Size: 2. Fetch: 18 ms, Parse: 30 ms, Registry Sync: 7 ms, DB Persist: 42 ms."
                in line
                for line in captured.output
            )
        )


if __name__ == "__main__":
    unittest.main()
