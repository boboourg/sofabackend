from __future__ import annotations

from datetime import datetime
import unittest

from schema_inspector.storage.raw_repository import (
    ApiRequestLogRecord,
    ApiSnapshotHeadRecord,
    PayloadSnapshotRecord,
    RawRepository,
)


class _FakeExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"


class _FakeReturningExecutor(_FakeExecutor):
    def __init__(self, return_value: int | None) -> None:
        super().__init__()
        self.return_value = return_value
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, query: str, *args: object) -> int | None:
        self.fetchval_calls.append((query, args))
        return self.return_value


class RawRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_repository_writes_request_snapshot_and_head(self) -> None:
        repository = RawRepository()
        executor = _FakeExecutor()

        await repository.insert_request_log(
            executor,
            ApiRequestLogRecord(
                trace_id="trace-1",
                job_id="job-1",
                job_type="hydrate_event_root",
                sport_slug="football",
                method="GET",
                source_url="https://www.sofascore.com/api/v1/event/1",
                endpoint_pattern="/api/v1/event/{event_id}",
                request_headers_redacted={"accept": "application/json"},
                query_params={},
                proxy_id="proxy_1",
                transport_attempt=1,
                http_status=200,
                challenge_reason=None,
                started_at="2026-04-16T09:00:00+00:00",
                finished_at="2026-04-16T09:00:01+00:00",
                latency_ms=123,
            ),
        )
        await repository.insert_payload_snapshot(
            executor,
            PayloadSnapshotRecord(
                trace_id="trace-1",
                job_id="job-1",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/1",
                resolved_url="https://www.sofascore.com/api/v1/event/1",
                envelope_key="event",
                context_entity_type="event",
                context_entity_id=1,
                context_unique_tournament_id=17,
                context_season_id=76986,
                context_event_id=1,
                http_status=200,
                payload={"event": {"id": 1}},
                payload_hash="hash-1",
                payload_size_bytes=18,
                content_type="application/json",
                is_valid_json=True,
                is_soft_error_payload=False,
                fetched_at="2026-04-16T09:00:01+00:00",
            ),
        )
        await repository.upsert_snapshot_head(
            executor,
            ApiSnapshotHeadRecord(
                endpoint_pattern="/api/v1/event/{event_id}",
                context_entity_type="event",
                context_entity_id=1,
                scope_key="event:1",
                latest_snapshot_id=99,
                latest_payload_hash="hash-1",
                latest_fetched_at="2026-04-16T09:00:01+00:00",
            ),
        )

        statements = [sql for sql, _ in executor.execute_calls]
        self.assertTrue(any("INSERT INTO api_request_log" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO api_payload_snapshot" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO api_snapshot_head" in sql for sql in statements))
        request_args = executor.execute_calls[0][1]
        snapshot_args = executor.execute_calls[1][1]
        head_args = executor.execute_calls[2][1]
        self.assertIsInstance(request_args[13], datetime)
        self.assertIsInstance(request_args[14], datetime)
        self.assertIsInstance(snapshot_args[10], datetime)
        self.assertIsInstance(head_args[6], datetime)

    async def test_repository_inserts_payload_snapshot_idempotently_by_scope_and_hash(self) -> None:
        repository = RawRepository()
        executor = _FakeReturningExecutor(return_value=42)

        snapshot_id = await repository.insert_payload_snapshot_if_missing_returning_id(
            executor,
            PayloadSnapshotRecord(
                trace_id="trace-1",
                job_id="job-1",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/1",
                resolved_url="https://www.sofascore.com/api/v1/event/1",
                envelope_key="event",
                context_entity_type="event",
                context_entity_id=1,
                context_unique_tournament_id=17,
                context_season_id=76986,
                context_event_id=1,
                http_status=200,
                payload={"event": {"id": 1}},
                payload_hash="hash-1",
                payload_size_bytes=18,
                content_type="application/json",
                is_valid_json=True,
                is_soft_error_payload=False,
                fetched_at="2026-04-16T09:00:01+00:00",
            ),
        )

        self.assertEqual(snapshot_id, 42)
        self.assertEqual(len(executor.fetchval_calls), 1)
        query, args = executor.fetchval_calls[0]
        self.assertIn("INSERT INTO api_payload_snapshot", query)
        self.assertIn("ON CONFLICT (scope_key, payload_hash)", query)
        self.assertIn("WHERE scope_key IS NOT NULL AND payload_hash IS NOT NULL", query)
        self.assertIn("RETURNING id", query)
        self.assertNotIn("IS NOT DISTINCT FROM", query)
        # scope_key is the last positional parameter ($21); payload_hash is $16.
        self.assertEqual(args[15], "hash-1")
        self.assertEqual(args[20], "event:1:/api/v1/event/{event_id}")


if __name__ == "__main__":
    unittest.main()
