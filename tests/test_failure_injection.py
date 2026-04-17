from __future__ import annotations

import json
import unittest

from schema_inspector.fetch_executor import FetchExecutor
from schema_inspector.fetch_models import FetchTask
from schema_inspector.runtime import TransportAttempt, TransportResult


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
        return 501

    async def upsert_snapshot_head(self, executor, record) -> None:
        del executor
        self.snapshot_heads.append(record)


class _StaticTransport:
    def __init__(self, result: TransportResult) -> None:
        self.result = result

    async def fetch(self, url: str, *, headers=None, timeout: float = 20.0):
        del url, headers, timeout
        return self.result


class _TimeoutTransport:
    async def fetch(self, url: str, *, headers=None, timeout: float = 20.0):
        del url, headers, timeout
        raise TimeoutError("request timed out")


class FailureInjectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_executor_marks_403_as_access_denied(self) -> None:
        repository = _FakeRawRepository()
        executor = FetchExecutor(
            transport=_StaticTransport(
                TransportResult(
                    resolved_url="https://www.sofascore.com/api/v1/event/1",
                    status_code=403,
                    headers={"Content-Type": "text/html"},
                    body_bytes=b"<html>access denied</html>",
                    attempts=(TransportAttempt(1, "proxy_1", 403, None, "access_denied"),),
                    final_proxy_name="proxy_1",
                    challenge_reason="access_denied",
                )
            ),
            raw_repository=repository,
            sql_executor=object(),
        )

        outcome = await executor.execute(_task("/api/v1/event/{event_id}"))

        self.assertEqual(outcome.classification, "access_denied")
        self.assertTrue(outcome.retry_recommended)
        self.assertEqual(len(repository.request_logs), 1)

    async def test_fetch_executor_marks_429_as_rate_limited(self) -> None:
        repository = _FakeRawRepository()
        executor = FetchExecutor(
            transport=_StaticTransport(
                TransportResult(
                    resolved_url="https://www.sofascore.com/api/v1/event/1/comments",
                    status_code=429,
                    headers={"Content-Type": "application/json"},
                    body_bytes=json.dumps({"error": {"code": 429}}).encode("utf-8"),
                    attempts=(TransportAttempt(1, "proxy_2", 429, None, "rate_limited"),),
                    final_proxy_name="proxy_2",
                    challenge_reason="rate_limited",
                )
            ),
            raw_repository=repository,
            sql_executor=object(),
        )

        outcome = await executor.execute(_task("/api/v1/event/{event_id}/comments"))

        self.assertEqual(outcome.classification, "rate_limited")
        self.assertTrue(outcome.retry_recommended)
        self.assertEqual(outcome.capability_signal, "guarded")

    async def test_fetch_executor_returns_network_error_on_timeout(self) -> None:
        repository = _FakeRawRepository()
        executor = FetchExecutor(
            transport=_TimeoutTransport(),
            raw_repository=repository,
            sql_executor=object(),
        )

        outcome = await executor.execute(_task("/api/v1/event/{event_id}/statistics"))

        self.assertEqual(outcome.classification, "network_error")
        self.assertTrue(outcome.retry_recommended)
        self.assertIsNone(outcome.snapshot_id)
        self.assertEqual(len(repository.request_logs), 1)

    async def test_fetch_executor_treats_empty_json_as_success_empty_json(self) -> None:
        repository = _FakeRawRepository()
        executor = FetchExecutor(
            transport=_StaticTransport(
                TransportResult(
                    resolved_url="https://www.sofascore.com/api/v1/event/1/graph",
                    status_code=200,
                    headers={"Content-Type": "application/json"},
                    body_bytes=b"{}",
                    attempts=(TransportAttempt(1, "proxy_3", 200, None, None),),
                    final_proxy_name="proxy_3",
                    challenge_reason=None,
                )
            ),
            raw_repository=repository,
            sql_executor=object(),
        )

        outcome = await executor.execute(_task("/api/v1/event/{event_id}/graph"))

        self.assertEqual(outcome.classification, "success_empty_json")
        self.assertFalse(outcome.retry_recommended)
        self.assertEqual(len(repository.snapshot_heads), 1)


def _task(pattern: str) -> FetchTask:
    return FetchTask(
        trace_id="trace-1",
        job_id="job-1",
        sport_slug="football",
        endpoint_pattern=pattern,
        source_url=f"https://www.sofascore.com{pattern.replace('{event_id}', '1')}",
        timeout_profile="standard_json",
        timeout_seconds=5.0,
        context_entity_type="event",
        context_entity_id=1,
        context_event_id=1,
        fetch_reason="phase9_test",
    )


if __name__ == "__main__":
    unittest.main()
