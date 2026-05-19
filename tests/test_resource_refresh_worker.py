from __future__ import annotations

import dataclasses
import unittest
from typing import Any

from schema_inspector.endpoints import (
    PLAYER_EVENTS_LAST_ENDPOINT,
    TEAM_PLAYERS_ENDPOINT,
    SofascoreEndpoint,
)


# Phase 1 (2026-05-19): PLAYER_EVENTS_LAST_ENDPOINT no longer opts into
# auto-pagination in production (see docs/REDUNDANT_ENDPOINTS_AUDIT.md §A.2).
# The chain mechanism itself stays in the worker so any future endpoint can
# opt back in. These tests exercise the *mechanism* through a derived copy
# that re-enables auto_paginate -- pattern and target_table stay identical
# so the worker dispatch path is unchanged.
_AUTO_PAGINATE_ENDPOINT = dataclasses.replace(
    PLAYER_EVENTS_LAST_ENDPOINT,
    auto_paginate=True,
    audit_interval_seconds=14 * 86400,
    max_pages=50,
)
from schema_inspector.fetch_models import FetchOutcomeEnvelope, FetchTask
from schema_inspector.queue.pagination_done import PaginationDoneStore
from schema_inspector.queue.resource_negative_cache import ResourceNegativeCache
from schema_inspector.queue.streams import StreamEntry
from schema_inspector.workers._stream_jobs import decode_stream_payload, encode_stream_job
from schema_inspector.jobs.envelope import JobEnvelope
from schema_inspector.jobs.types import JOB_REFRESH_RESOURCE
from schema_inspector.workers.resource_refresh_worker import ResourceRefreshWorker


class _CapturingExecutor:
    """Stand-in for FetchExecutor that records the FetchTask it was handed."""

    def __init__(self, *, http_status: int | None = 200, snapshot_id: int | None = None) -> None:
        self.tasks: list[FetchTask] = []
        self.http_status = http_status
        self.snapshot_id = snapshot_id

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
            snapshot_id=self.snapshot_id,
            payload_hash=None,
        )


class _PublishingQueue:
    """Stand-in for RedisStreamQueue that records publish() calls."""

    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []

    def publish(self, stream: str, values) -> str:
        self.published.append((stream, dict(values)))
        return f"id-{len(self.published)}"


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


def _build_worker(
    executor: _CapturingExecutor,
    *,
    endpoints: tuple[SofascoreEndpoint, ...],
    negative_cache: ResourceNegativeCache | None = None,
    pagination_done: PaginationDoneStore | None = None,
    snapshot_reader=None,
    queue=None,
) -> ResourceRefreshWorker:
    return ResourceRefreshWorker(
        fetch_executor=executor,
        delayed_scheduler=_StubScheduler(),
        delayed_payload_store=_StubPayloadStore(),
        completion_store=None,
        queue=queue,  # only used by auto-pagination publish
        consumer="test-consumer-1",
        endpoints=endpoints,
        negative_cache=negative_cache,
        pagination_done=pagination_done,
        snapshot_reader=snapshot_reader,
    )


class _FakePaginationBackend:
    def __init__(self) -> None:
        self.store: dict[str, dict[str, str]] = {}

    def hget(self, key, field):
        return self.store.get(key, {}).get(field)

    def hset(self, key, mapping=None, **kwargs):
        if mapping is None:
            mapping = kwargs.get("mapping")
        bucket = self.store.setdefault(key, {})
        bucket.update(mapping or {})
        return len(mapping or {})

    def hdel(self, key, field):
        return 1 if self.store.get(key, {}).pop(field, None) is not None else 0


def _make_async_reader(payload):
    async def reader(snapshot_id):
        return payload
    return reader


class _FakeNegativeCacheBackend:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

    def exists(self, key):
        return 1 if key in self.store else 0

    def set(self, key, value, ex=None):
        self.store[key] = str(value)
        return True

    def delete(self, key):
        return 1 if self.store.pop(key, None) is not None else 0


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

    async def test_handle_marks_negative_cache_on_404(self) -> None:
        executor = _CapturingExecutor(http_status=404)
        backend = _FakeNegativeCacheBackend()
        cache = ResourceNegativeCache(backend, env={})
        worker = _build_worker(executor, endpoints=(TEAM_PLAYERS_ENDPOINT,), negative_cache=cache)
        envelope = _make_envelope(
            pattern=TEAM_PLAYERS_ENDPOINT.pattern,
            entity_type="team",
            entity_id=1161574,
            path_params={"team_id": 1161574},
        )

        result = await worker.handle(_entry(envelope))

        self.assertEqual(result, "completed")
        # Subsequent lookup should hit the negative cache.
        self.assertTrue(
            cache.is_negatively_cached(
                endpoint_pattern=TEAM_PLAYERS_ENDPOINT.pattern,
                path_params={"team_id": 1161574},
            )
        )

    async def test_handle_does_not_mark_negative_cache_on_200(self) -> None:
        executor = _CapturingExecutor(http_status=200)
        backend = _FakeNegativeCacheBackend()
        cache = ResourceNegativeCache(backend, env={})
        worker = _build_worker(executor, endpoints=(TEAM_PLAYERS_ENDPOINT,), negative_cache=cache)
        envelope = _make_envelope(
            pattern=TEAM_PLAYERS_ENDPOINT.pattern,
            entity_type="team",
            entity_id=42,
            path_params={"team_id": 42},
        )

        await worker.handle(_entry(envelope))

        self.assertFalse(
            cache.is_negatively_cached(
                endpoint_pattern=TEAM_PLAYERS_ENDPOINT.pattern,
                path_params={"team_id": 42},
            )
        )

    async def test_handle_marks_negative_cache_on_403(self) -> None:
        # 4xx other than 429 should also be marked (no proxy will fix 403).
        executor = _CapturingExecutor(http_status=403)
        backend = _FakeNegativeCacheBackend()
        cache = ResourceNegativeCache(backend, env={})
        worker = _build_worker(executor, endpoints=(TEAM_PLAYERS_ENDPOINT,), negative_cache=cache)
        envelope = _make_envelope(
            pattern=TEAM_PLAYERS_ENDPOINT.pattern,
            entity_type="team",
            entity_id=999,
            path_params={"team_id": 999},
        )

        await worker.handle(_entry(envelope))

        self.assertTrue(
            cache.is_negatively_cached(
                endpoint_pattern=TEAM_PLAYERS_ENDPOINT.pattern,
                path_params={"team_id": 999},
            )
        )

    # --- Stage 8 / C.4: worker-side auto-pagination -----------------------------

    async def test_chain_publishes_next_page_when_has_next_true(self) -> None:
        executor = _CapturingExecutor(http_status=200, snapshot_id=42)
        queue = _PublishingQueue()
        pagination = PaginationDoneStore(_FakePaginationBackend())
        reader = _make_async_reader({"events": [{"id": 1}], "hasNextPage": True})
        worker = _build_worker(
            executor,
            endpoints=(_AUTO_PAGINATE_ENDPOINT,),
            pagination_done=pagination,
            snapshot_reader=reader,
            queue=queue,
        )
        envelope = _make_envelope(
            pattern=_AUTO_PAGINATE_ENDPOINT.pattern,
            entity_type="player",
            entity_id=750,
            path_params={"player_id": 750, "page": 0},
        )

        result = await worker.handle(_entry(envelope))

        self.assertEqual(result, "completed")
        self.assertEqual(len(queue.published), 1)
        published_payload = queue.published[0][1]
        next_envelope = decode_stream_payload(published_payload)
        self.assertEqual(next_envelope.params["path_params"], {"player_id": 750, "page": 1})
        self.assertEqual(next_envelope.entity_id, 750)
        self.assertEqual(next_envelope.parent_job_id, envelope.job_id)
        # Pagination_done NOT marked yet -- chain still in progress.
        self.assertFalse(
            pagination.is_completed_recently(
                endpoint_pattern=_AUTO_PAGINATE_ENDPOINT.pattern,
                entity_id=750,
                audit_interval_seconds=14 * 86400,
            )
        )

    async def test_chain_marks_completed_when_has_next_false(self) -> None:
        executor = _CapturingExecutor(http_status=200, snapshot_id=99)
        queue = _PublishingQueue()
        pagination = PaginationDoneStore(_FakePaginationBackend())
        reader = _make_async_reader({"events": [], "hasNextPage": False})
        worker = _build_worker(
            executor,
            endpoints=(_AUTO_PAGINATE_ENDPOINT,),
            pagination_done=pagination,
            snapshot_reader=reader,
            queue=queue,
        )
        envelope = _make_envelope(
            pattern=_AUTO_PAGINATE_ENDPOINT.pattern,
            entity_type="player",
            entity_id=750,
            path_params={"player_id": 750, "page": 17},
        )

        await worker.handle(_entry(envelope))

        # No further pages published.
        self.assertEqual(queue.published, [])
        # pagination_done stamped.
        self.assertTrue(
            pagination.is_completed_recently(
                endpoint_pattern=_AUTO_PAGINATE_ENDPOINT.pattern,
                entity_id=750,
                audit_interval_seconds=14 * 86400,
            )
        )

    async def test_chain_skipped_at_page_zero_when_completed_recently(self) -> None:
        executor = _CapturingExecutor(http_status=200, snapshot_id=1)
        queue = _PublishingQueue()
        pagination = PaginationDoneStore(_FakePaginationBackend())
        # Pre-mark as completed within the audit window.
        pagination.mark_completed(
            endpoint_pattern=PLAYER_EVENTS_LAST_ENDPOINT.pattern,
            entity_id=750,
        )
        reader = _make_async_reader({"events": [], "hasNextPage": True})  # would chain
        worker = _build_worker(
            executor,
            endpoints=(PLAYER_EVENTS_LAST_ENDPOINT,),
            pagination_done=pagination,
            snapshot_reader=reader,
            queue=queue,
        )
        envelope = _make_envelope(
            pattern=PLAYER_EVENTS_LAST_ENDPOINT.pattern,
            entity_type="player",
            entity_id=750,
            path_params={"player_id": 750, "page": 0},
        )

        await worker.handle(_entry(envelope))

        # Skipped chain because pagination_done is fresh.
        self.assertEqual(queue.published, [])

    async def test_chain_continues_past_page_zero_regardless_of_completion_marker(self) -> None:
        # Once a chain has started (page>=1), do not let the cool-down marker
        # interrupt it -- otherwise we get a half-completed walk.
        executor = _CapturingExecutor(http_status=200, snapshot_id=1)
        queue = _PublishingQueue()
        pagination = PaginationDoneStore(_FakePaginationBackend())
        pagination.mark_completed(
            endpoint_pattern=_AUTO_PAGINATE_ENDPOINT.pattern,
            entity_id=750,
        )
        reader = _make_async_reader({"events": [], "hasNextPage": True})
        worker = _build_worker(
            executor,
            endpoints=(_AUTO_PAGINATE_ENDPOINT,),
            pagination_done=pagination,
            snapshot_reader=reader,
            queue=queue,
        )
        envelope = _make_envelope(
            pattern=_AUTO_PAGINATE_ENDPOINT.pattern,
            entity_type="player",
            entity_id=750,
            path_params={"player_id": 750, "page": 5},
        )

        await worker.handle(_entry(envelope))

        self.assertEqual(len(queue.published), 1)
        next_envelope = decode_stream_payload(queue.published[0][1])
        self.assertEqual(next_envelope.params["path_params"]["page"], 6)

    async def test_chain_safety_fuse_at_max_pages(self) -> None:
        executor = _CapturingExecutor(http_status=200, snapshot_id=1)
        queue = _PublishingQueue()
        pagination = PaginationDoneStore(_FakePaginationBackend())
        reader = _make_async_reader({"events": [], "hasNextPage": True})
        worker = _build_worker(
            executor,
            endpoints=(_AUTO_PAGINATE_ENDPOINT,),
            pagination_done=pagination,
            snapshot_reader=reader,
            queue=queue,
        )
        # _AUTO_PAGINATE_ENDPOINT.max_pages == 50, so page=49 + 1 hits the fuse.
        envelope = _make_envelope(
            pattern=_AUTO_PAGINATE_ENDPOINT.pattern,
            entity_type="player",
            entity_id=750,
            path_params={"player_id": 750, "page": 49},
        )

        await worker.handle(_entry(envelope))

        self.assertEqual(queue.published, [])
        # Stamped as completed even though there were more pages -- prevents
        # the next planner tick from immediately starting another walk.
        self.assertTrue(
            pagination.is_completed_recently(
                endpoint_pattern=_AUTO_PAGINATE_ENDPOINT.pattern,
                entity_id=750,
                audit_interval_seconds=14 * 86400,
            )
        )

    async def test_chain_skipped_when_endpoint_auto_paginate_false(self) -> None:
        # TEAM_PLAYERS_ENDPOINT has auto_paginate=False -> never chains.
        executor = _CapturingExecutor(http_status=200, snapshot_id=1)
        queue = _PublishingQueue()
        pagination = PaginationDoneStore(_FakePaginationBackend())
        reader = _make_async_reader({"hasNextPage": True})
        worker = _build_worker(
            executor,
            endpoints=(TEAM_PLAYERS_ENDPOINT,),
            pagination_done=pagination,
            snapshot_reader=reader,
            queue=queue,
        )
        envelope = _make_envelope(
            pattern=TEAM_PLAYERS_ENDPOINT.pattern,
            entity_type="team",
            entity_id=42,
            path_params={"team_id": 42},
        )

        await worker.handle(_entry(envelope))

        self.assertEqual(queue.published, [])

    async def test_chain_skipped_on_dedup_snapshot_id_none(self) -> None:
        # snapshot_id=None means upstream payload was a duplicate of an
        # existing row -> we have nothing fresh to inspect -> skip chain.
        executor = _CapturingExecutor(http_status=200, snapshot_id=None)
        queue = _PublishingQueue()
        worker = _build_worker(
            executor,
            endpoints=(PLAYER_EVENTS_LAST_ENDPOINT,),
            pagination_done=PaginationDoneStore(_FakePaginationBackend()),
            snapshot_reader=_make_async_reader({"hasNextPage": True}),
            queue=queue,
        )
        envelope = _make_envelope(
            pattern=PLAYER_EVENTS_LAST_ENDPOINT.pattern,
            entity_type="player",
            entity_id=750,
            path_params={"player_id": 750, "page": 0},
        )

        await worker.handle(_entry(envelope))

        self.assertEqual(queue.published, [])

    async def test_chain_skipped_when_next_page_negatively_cached(self) -> None:
        executor = _CapturingExecutor(http_status=200, snapshot_id=7)
        queue = _PublishingQueue()
        neg = ResourceNegativeCache(_FakeNegativeCacheBackend(), env={})
        # Pre-mark page=1 as bad.
        neg.mark_404(
            endpoint_pattern=PLAYER_EVENTS_LAST_ENDPOINT.pattern,
            path_params={"player_id": 750, "page": 1},
        )
        worker = _build_worker(
            executor,
            endpoints=(PLAYER_EVENTS_LAST_ENDPOINT,),
            negative_cache=neg,
            pagination_done=PaginationDoneStore(_FakePaginationBackend()),
            snapshot_reader=_make_async_reader({"hasNextPage": True}),
            queue=queue,
        )
        envelope = _make_envelope(
            pattern=PLAYER_EVENTS_LAST_ENDPOINT.pattern,
            entity_type="player",
            entity_id=750,
            path_params={"player_id": 750, "page": 0},
        )

        await worker.handle(_entry(envelope))

        self.assertEqual(queue.published, [])

    async def test_handle_does_not_mark_negative_cache_on_429(self) -> None:
        # 429 = rate limited; a worker retry might succeed soon.
        executor = _CapturingExecutor(http_status=429)
        backend = _FakeNegativeCacheBackend()
        cache = ResourceNegativeCache(backend, env={})
        worker = _build_worker(executor, endpoints=(TEAM_PLAYERS_ENDPOINT,), negative_cache=cache)
        envelope = _make_envelope(
            pattern=TEAM_PLAYERS_ENDPOINT.pattern,
            entity_type="team",
            entity_id=42,
            path_params={"team_id": 42},
        )

        await worker.handle(_entry(envelope))

        self.assertFalse(
            cache.is_negatively_cached(
                endpoint_pattern=TEAM_PLAYERS_ENDPOINT.pattern,
                path_params={"team_id": 42},
            )
        )


if __name__ == "__main__":
    unittest.main()
