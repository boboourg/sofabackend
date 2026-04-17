from __future__ import annotations

import unittest

from schema_inspector.fetch_executor import FetchExecutor
from schema_inspector.fetch_models import FetchTask
from schema_inspector.runtime import TransportAttempt, TransportResult


class _FakeTransport:
    def __init__(self, result: TransportResult) -> None:
        self.result = result
        self.calls: list[tuple[str, dict[str, str] | None, float]] = []

    async def fetch(self, url: str, *, headers=None, timeout: float = 20.0):
        self.calls.append((url, headers, timeout))
        return self.result


class _FakeRawRepository:
    def __init__(self) -> None:
        self.request_logs = []
        self.snapshots = []
        self.snapshot_heads = []

    async def insert_request_log(self, executor, record) -> None:
        del executor
        self.request_logs.append(record)

    async def insert_payload_snapshot_returning_id(self, executor, record) -> int:
        del executor
        self.snapshots.append(record)
        return 101

    async def upsert_snapshot_head(self, executor, record) -> None:
        del executor
        self.snapshot_heads.append(record)


class FetchExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def test_executor_fetches_classifies_and_writes_snapshot(self) -> None:
        transport = _FakeTransport(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/1",
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"event":{"id":1,"slug":"match"}}',
                attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
                final_proxy_name="proxy_1",
                challenge_reason=None,
            )
        )
        raw_repository = _FakeRawRepository()
        executor = FetchExecutor(transport=transport, raw_repository=raw_repository, sql_executor=object())

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-1",
                job_id="job-1",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/1",
                timeout_profile="standard_json",
                timeout_seconds=12.5,
                context_entity_type="event",
                context_entity_id=1,
                context_unique_tournament_id=17,
                context_season_id=76986,
                context_event_id=1,
                expected_content_type="application/json",
                fetch_reason="hydrate_event_root",
            )
        )

        self.assertEqual(outcome.classification, "success_json")
        self.assertEqual(outcome.snapshot_id, 101)
        self.assertEqual(outcome.payload_root_keys, ("event",))
        self.assertEqual(len(raw_repository.request_logs), 1)
        self.assertEqual(len(raw_repository.snapshots), 1)
        self.assertEqual(len(raw_repository.snapshot_heads), 1)
        self.assertEqual(transport.calls[0][2], 12.5)

    async def test_executor_writes_request_log_for_guarded_non_json_response(self) -> None:
        transport = _FakeTransport(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/1/comments",
                status_code=403,
                headers={"Content-Type": "text/html"},
                body_bytes=b"<html>captcha</html>",
                attempts=(TransportAttempt(1, "proxy_2", 403, None, "bot_challenge"),),
                final_proxy_name="proxy_2",
                challenge_reason="bot_challenge",
            )
        )
        raw_repository = _FakeRawRepository()
        executor = FetchExecutor(transport=transport, raw_repository=raw_repository, sql_executor=object())

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-2",
                job_id="job-2",
                sport_slug="tennis",
                endpoint_pattern="/api/v1/event/{event_id}/comments",
                source_url="https://www.sofascore.com/api/v1/event/1/comments",
                timeout_profile="fast_live_edge",
                timeout_seconds=5.0,
                context_entity_type="event",
                context_entity_id=1,
                context_unique_tournament_id=None,
                context_season_id=None,
                context_event_id=1,
                expected_content_type="application/json",
                fetch_reason="refresh_live_event",
            )
        )

        self.assertEqual(outcome.classification, "challenge_detected")
        self.assertEqual(outcome.snapshot_id, 101)
        self.assertEqual(len(raw_repository.request_logs), 1)
        self.assertEqual(len(raw_repository.snapshots), 1)
        self.assertEqual(len(raw_repository.snapshot_heads), 0)


if __name__ == "__main__":
    unittest.main()
