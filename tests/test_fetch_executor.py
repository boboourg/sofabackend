from __future__ import annotations

import unittest

from schema_inspector.fetch_executor import FetchExecutor
from schema_inspector.fetch_models import FetchOutcomeEnvelope, FetchTask
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
        self.idempotent_snapshot_ids: list[int] = []

    async def insert_request_log(self, executor, record) -> None:
        del executor
        self.request_logs.append(record)

    async def insert_payload_snapshot_returning_id(self, executor, record) -> int:
        del executor
        self.snapshots.append(record)
        return 101

    async def insert_payload_snapshot_if_missing_returning_id(self, executor, record) -> int:
        del executor
        self.snapshots.append(record)
        snapshot_id = self.idempotent_snapshot_ids.pop(0) if self.idempotent_snapshot_ids else 101
        return snapshot_id

    async def upsert_snapshot_head(self, executor, record) -> None:
        del executor
        self.snapshot_heads.append(record)


class _FakeSnapshotStore:
    def __init__(self) -> None:
        self.records = []
        self.next_snapshot_id = -1

    def stage_snapshot(self, record) -> int:
        self.records.append(record)
        snapshot_id = self.next_snapshot_id
        self.next_snapshot_id -= 1
        return snapshot_id


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
        self.assertEqual(raw_repository.request_logs[0].payload_bytes, len(b'{"event":{"id":1,"slug":"match"}}'))
        self.assertEqual(raw_repository.request_logs[0].attempts_json[0]["proxy_name"], "proxy_1")
        self.assertIsNone(raw_repository.request_logs[0].error_message)

    async def test_executor_can_defer_raw_writes_and_collect_prefetched_record(self) -> None:
        transport = _FakeTransport(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/5",
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"event":{"id":5,"slug":"prefetched"}}',
                attempts=(TransportAttempt(1, "proxy_5", 200, None, None),),
                final_proxy_name="proxy_5",
                challenge_reason=None,
            )
        )
        raw_repository = _FakeRawRepository()
        snapshot_store = _FakeSnapshotStore()
        executor = FetchExecutor(
            transport=transport,
            raw_repository=raw_repository,
            sql_executor=None,
            snapshot_store=snapshot_store,
            write_mode="deferred",
        )

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-prefetch",
                job_id="job-prefetch",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/5",
                timeout_profile="pilot",
                context_entity_type="event",
                context_entity_id=5,
                context_event_id=5,
                fetch_reason="hydrate_event_root",
            )
        )

        self.assertEqual(outcome.snapshot_id, -1)
        self.assertEqual(len(raw_repository.request_logs), 0)
        self.assertEqual(len(raw_repository.snapshots), 0)
        self.assertEqual(len(snapshot_store.records), 1)
        self.assertEqual(len(executor.prefetched_records), 1)

    async def test_commit_prefetched_record_persists_append_only_log_and_idempotent_snapshot(self) -> None:
        transport = _FakeTransport(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/9/statistics",
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"statistics":[]}',
                attempts=(TransportAttempt(1, "proxy_9", 200, None, None),),
                final_proxy_name="proxy_9",
                challenge_reason=None,
            )
        )
        raw_repository = _FakeRawRepository()
        raw_repository.idempotent_snapshot_ids.append(404)
        deferred_executor = FetchExecutor(
            transport=transport,
            raw_repository=raw_repository,
            sql_executor=None,
            snapshot_store=_FakeSnapshotStore(),
            write_mode="deferred",
        )
        await deferred_executor.execute(
            FetchTask(
                trace_id="trace-commit",
                job_id="job-commit",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}/statistics",
                source_url="https://www.sofascore.com/api/v1/event/9/statistics",
                timeout_profile="pilot",
                context_entity_type="event",
                context_entity_id=9,
                context_event_id=9,
                fetch_reason="hydrate_event_edge",
            )
        )

        committing_executor = FetchExecutor(
            transport=transport,
            raw_repository=raw_repository,
            sql_executor=object(),
        )

        committed = await committing_executor.commit_prefetched_record(deferred_executor.prefetched_records[0])

        self.assertIsInstance(committed, FetchOutcomeEnvelope)
        self.assertEqual(committed.snapshot_id, 404)
        self.assertEqual(len(raw_repository.request_logs), 1)
        self.assertEqual(len(raw_repository.snapshots), 1)
        self.assertEqual(len(raw_repository.snapshot_heads), 1)

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
        self.assertEqual(raw_repository.request_logs[0].attempts_json[0]["challenge_reason"], "bot_challenge")
        self.assertEqual(raw_repository.request_logs[0].payload_bytes, len(b"<html>captcha</html>"))

    async def test_executor_writes_network_error_message_into_request_log(self) -> None:
        class _FailingTransport:
            async def fetch(self, url: str, *, headers=None, timeout: float = 20.0):
                del url, headers, timeout
                raise TimeoutError("transport timed out")

        raw_repository = _FakeRawRepository()
        executor = FetchExecutor(
            transport=_FailingTransport(),
            raw_repository=raw_repository,
            sql_executor=object(),
        )

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-timeout",
                job_id="job-timeout",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/77",
                timeout_profile="standard_json",
                fetch_reason="hydrate_event_root",
            )
        )

        self.assertEqual(outcome.classification, "network_error")
        self.assertEqual(len(raw_repository.request_logs), 1)
        self.assertEqual(raw_repository.request_logs[0].error_message, "transport timed out")
        self.assertIsNone(raw_repository.request_logs[0].attempts_json)


if __name__ == "__main__":
    unittest.main()
