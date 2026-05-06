from __future__ import annotations

import unittest
from typing import Any

from schema_inspector.endpoints import TEAM_PLAYERS_ENDPOINT, SofascoreEndpoint
from schema_inspector.fetch_models import FetchOutcomeEnvelope, FetchTask
from schema_inspector.queue.streams import StreamEntry
from schema_inspector.workers._stream_jobs import encode_stream_job
from schema_inspector.jobs.envelope import JobEnvelope
from schema_inspector.jobs.types import JOB_REFRESH_RESOURCE
from schema_inspector.workers.resource_refresh_worker import ResourceRefreshWorker


class _CapturingExecutor:
    """Stand-in for FetchExecutor that records the FetchTask it was handed."""

    def __init__(self, *, http_status: int | None = 200) -> None:
        self.tasks: list[FetchTask] = []
        self.http_status = http_status

    async def execute(self, task: FetchTask) -> FetchOutcomeEnvelope:
        self.tasks.append(task)
        return FetchOutcomeEnvelope(
            trace_id=task.trace_id,
            job_id=task.job_id,
            endpoint_pattern=task.endpoint_pattern,
            source_url=task.source_url,
            resolved_url=task.source_url,
            http_status=self.http_status,
            classification="success_json" if self.http_status else "transport_error",
            proxy_id=None,
            challenge_reason=None,
            snapshot_id=None,
            payload_hash=None,
        )


class _StubScheduler:
    def __init__(self) -> None:
        self.scheduled: list[tuple[str, int]] = []

    def schedule(self, job_id: str, *, run_at_epoch_ms: int) -> None:
        self.scheduled.append((job_id, run_at_epoch_ms))


class _StubPayloadStore:
    def __init__(self) -> None:
        self.saved: list[StreamEntry] = []

    def save_entry(self, entry: StreamEntry) -> None:
        self.saved.append(entry)


def _make_envelope(
    *,
    pattern: str,
    entity_type: str,
    entity_id: int,
    path_params: dict[str, object],
    freshness_key: str = "freshness:test",
    freshness_ttl: int = 600,
) -> JobEnvelope:
    return JobEnvelope.create(
        job_type=JOB_REFRESH_RESOURCE,
        sport_slug=None,
        entity_type=entity_type,
        entity_id=entity_id,
        scope="resource_refresh",
        params={
            "endpoint_pattern": pattern,
            "path_params": path_params,
            "freshness_key": freshness_key,
            "freshness_ttl_seconds": freshness_ttl,
        },
        priority=40,
        trace_id=None,
        capability_hint="resource_refresh",
    )


def _entry(envelope: JobEnvelope) -> StreamEntry:
    payload = {str(k): str(v) for k, v in encode_stream_job(envelope).items()}
    return StreamEntry(stream="stream:etl:resource_refresh", message_id="0-1", values=payload)


def _build_worker(executor: _CapturingExecutor, *, endpoints: tuple[SofascoreEndpoint, ...]) -> ResourceRefreshWorker:
    return ResourceRefreshWorker(
        fetch_executor=executor,
        delayed_scheduler=_StubScheduler(),
        delayed_payload_store=_StubPayloadStore(),
        completion_store=None,
        queue=None,  # WorkerRuntime is never .run_forever'd in these unit tests
        consumer="test-consumer-1",
        endpoints=endpoints,
    )


class ResourceRefreshWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_handle_builds_fetch_task_from_envelope(self) -> None:
        executor = _CapturingExecutor()
        worker = _build_worker(executor, endpoints=(TEAM_PLAYERS_ENDPOINT,))
        envelope = _make_envelope(
            pattern=TEAM_PLAYERS_ENDPOINT.pattern,
            entity_type="team",
            entity_id=42,
            path_params={"team_id": 42},
            freshness_key="freshness:/api/v1/team/{team_id}/players|{\"team_id\":42}",
            freshness_ttl=11 * 3600,
        )

        result = await worker.handle(_entry(envelope))

        self.assertEqual(result, "completed")
        self.assertEqual(len(executor.tasks), 1)
        task = executor.tasks[0]
        self.assertEqual(task.endpoint_pattern, TEAM_PLAYERS_ENDPOINT.pattern)
        self.assertEqual(task.source_url, "https://www.sofascore.com/api/v1/team/42/players")
        self.assertEqual(task.context_entity_type, "team")
        self.assertEqual(task.context_entity_id, 42)
        self.assertEqual(task.fetch_reason, "resource_refresh")
        self.assertEqual(task.freshness_ttl_seconds, 11 * 3600)
        self.assertTrue(task.freshness_key.startswith("freshness:"))

    async def test_handle_raises_when_envelope_missing_pattern(self) -> None:
        executor = _CapturingExecutor()
        worker = _build_worker(executor, endpoints=(TEAM_PLAYERS_ENDPOINT,))
        # Forge an envelope without endpoint_pattern in params.
        envelope = JobEnvelope.create(
            job_type=JOB_REFRESH_RESOURCE,
            sport_slug=None,
            entity_type="team",
            entity_id=42,
            scope="resource_refresh",
            params={"path_params": {"team_id": 42}},
            priority=40,
            trace_id=None,
            capability_hint="resource_refresh",
        )
        with self.assertRaises(RuntimeError) as ctx:
            await worker.handle(_entry(envelope))
        self.assertIn("missing endpoint_pattern", str(ctx.exception))

    async def test_handle_raises_when_endpoint_pattern_unknown(self) -> None:
        executor = _CapturingExecutor()
        worker = _build_worker(executor, endpoints=(TEAM_PLAYERS_ENDPOINT,))
        envelope = _make_envelope(
            pattern="/api/v1/something/never-registered",
            entity_type="team",
            entity_id=42,
            path_params={"team_id": 42},
        )
        with self.assertRaises(RuntimeError) as ctx:
            await worker.handle(_entry(envelope))
        self.assertIn("unknown endpoint_pattern", str(ctx.exception))

    async def test_handle_raises_on_transport_failure(self) -> None:
        executor = _CapturingExecutor(http_status=None)
        worker = _build_worker(executor, endpoints=(TEAM_PLAYERS_ENDPOINT,))
        envelope = _make_envelope(
            pattern=TEAM_PLAYERS_ENDPOINT.pattern,
            entity_type="team",
            entity_id=42,
            path_params={"team_id": 42},
        )
        with self.assertRaises(RuntimeError) as ctx:
            await worker.handle(_entry(envelope))
        self.assertIn("transport failed", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
