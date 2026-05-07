from __future__ import annotations

import asyncio
import unittest

from schema_inspector.endpoints import (
    UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT,
)
from schema_inspector.fetch_models import FetchOutcomeEnvelope, FetchTask
from schema_inspector.jobs.envelope import JobEnvelope
from schema_inspector.jobs.types import JOB_NORMALIZE_SNAPSHOT, JOB_REFRESH_RESOURCE
from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.queue.streams import StreamEntry
from schema_inspector.workers._stream_jobs import (
    decode_stream_payload,
    encode_stream_job,
)
from schema_inspector.workers.normalize_stream_worker import NormalizeStreamWorker
from schema_inspector.workers.resource_refresh_worker import ResourceRefreshWorker


class _CapturingExecutor:
    def __init__(self, *, http_status=200, snapshot_id=None) -> None:
        self.http_status = http_status
        self.snapshot_id = snapshot_id

    async def execute(self, task: FetchTask) -> FetchOutcomeEnvelope:
        return FetchOutcomeEnvelope(
            trace_id=task.trace_id,
            job_id=task.job_id,
            endpoint_pattern=task.endpoint_pattern,
            source_url=task.source_url,
            resolved_url=task.source_url,
            http_status=self.http_status,
            classification="success_json" if self.http_status == 200 else "transport_error",
            proxy_id=None,
            challenge_reason=None,
            snapshot_id=self.snapshot_id,
            payload_hash=None,
        )


class _PublishingQueue:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []

    def publish(self, stream: str, values) -> str:
        self.published.append((stream, dict(values)))
        return f"id-{len(self.published)}"


class _StubScheduler:
    def schedule(self, job_id, *, run_at_epoch_ms) -> None:
        pass


class _StubPayloadStore:
    def save_entry(self, entry) -> None:
        pass


def _make_refresh_envelope(*, pattern: str, snapshot_id: int | None) -> JobEnvelope:
    del snapshot_id
    return JobEnvelope.create(
        job_type=JOB_REFRESH_RESOURCE,
        sport_slug="football",
        entity_type="season",
        entity_id=76986,
        scope="resource_refresh",
        params={
            "endpoint_pattern": pattern,
            "path_params": {"unique_tournament_id": 17, "season_id": 76986},
            "freshness_key": "freshness:test",
            "freshness_ttl_seconds": 600,
        },
        priority=40,
        trace_id="t-1",
        capability_hint="resource_refresh",
    )


def _refresh_entry(envelope: JobEnvelope) -> StreamEntry:
    payload = {str(k): str(v) for k, v in encode_stream_job(envelope).items()}
    return StreamEntry(
        stream="stream:etl:resource_refresh",
        message_id="0-1",
        values=payload,
    )


class ResourceRefreshWorkerNormalizePublishTests(unittest.IsolatedAsyncioTestCase):
    async def test_publishes_normalize_envelope_on_success(self) -> None:
        executor = _CapturingExecutor(http_status=200, snapshot_id=12345)
        published: list[dict[str, object]] = []

        def _publisher(*, snapshot_id, endpoint_pattern, sport_slug, trace_id, parent_job_id):
            published.append(
                {
                    "snapshot_id": snapshot_id,
                    "endpoint_pattern": endpoint_pattern,
                    "sport_slug": sport_slug,
                    "trace_id": trace_id,
                    "parent_job_id": parent_job_id,
                }
            )

        worker = ResourceRefreshWorker(
            fetch_executor=executor,
            delayed_scheduler=_StubScheduler(),
            delayed_payload_store=_StubPayloadStore(),
            completion_store=None,
            queue=_PublishingQueue(),
            consumer="t-1",
            endpoints=(UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT,),
            normalize_publisher=_publisher,
        )
        envelope = _make_refresh_envelope(
            pattern=UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT.pattern,
            snapshot_id=12345,
        )
        result = await worker.handle(_refresh_entry(envelope))
        self.assertEqual(result, "completed")
        self.assertEqual(len(published), 1)
        self.assertEqual(published[0]["snapshot_id"], 12345)
        self.assertEqual(published[0]["endpoint_pattern"], UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT.pattern)
        self.assertEqual(published[0]["sport_slug"], "football")
        self.assertEqual(published[0]["trace_id"], "t-1")
        self.assertEqual(published[0]["parent_job_id"], envelope.job_id)

    async def test_does_not_publish_on_404(self) -> None:
        executor = _CapturingExecutor(http_status=404, snapshot_id=None)
        published: list = []

        def _publisher(**kwargs):
            published.append(kwargs)

        worker = ResourceRefreshWorker(
            fetch_executor=executor,
            delayed_scheduler=_StubScheduler(),
            delayed_payload_store=_StubPayloadStore(),
            completion_store=None,
            queue=_PublishingQueue(),
            consumer="t-1",
            endpoints=(UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT,),
            normalize_publisher=_publisher,
        )
        envelope = _make_refresh_envelope(
            pattern=UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT.pattern,
            snapshot_id=None,
        )
        result = await worker.handle(_refresh_entry(envelope))
        self.assertEqual(result, "completed")
        self.assertEqual(published, [])

    async def test_does_not_publish_when_snapshot_id_missing(self) -> None:
        executor = _CapturingExecutor(http_status=200, snapshot_id=None)
        published: list = []

        def _publisher(**kwargs):
            published.append(kwargs)

        worker = ResourceRefreshWorker(
            fetch_executor=executor,
            delayed_scheduler=_StubScheduler(),
            delayed_payload_store=_StubPayloadStore(),
            completion_store=None,
            queue=_PublishingQueue(),
            consumer="t-1",
            endpoints=(UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT,),
            normalize_publisher=_publisher,
        )
        envelope = _make_refresh_envelope(
            pattern=UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT.pattern,
            snapshot_id=None,
        )
        result = await worker.handle(_refresh_entry(envelope))
        self.assertEqual(result, "completed")
        self.assertEqual(published, [])

    async def test_publish_failure_does_not_fail_job(self) -> None:
        executor = _CapturingExecutor(http_status=200, snapshot_id=999)

        def _failing_publisher(**kwargs):
            raise RuntimeError("redis temporarily unreachable")

        worker = ResourceRefreshWorker(
            fetch_executor=executor,
            delayed_scheduler=_StubScheduler(),
            delayed_payload_store=_StubPayloadStore(),
            completion_store=None,
            queue=_PublishingQueue(),
            consumer="t-1",
            endpoints=(UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT,),
            normalize_publisher=_failing_publisher,
        )
        envelope = _make_refresh_envelope(
            pattern=UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT.pattern,
            snapshot_id=999,
        )
        # Must NOT raise — publish failure is logged best-effort.
        result = await worker.handle(_refresh_entry(envelope))
        self.assertEqual(result, "completed")


# ----------------------------------------------------------------------
# NormalizeStreamWorker
# ----------------------------------------------------------------------


class _RecordingParserRegistry:
    def __init__(self) -> None:
        self.parsed: list[RawSnapshot] = []

    def parse(self, snapshot: RawSnapshot):
        self.parsed.append(snapshot)
        return _ParseResultStub(snapshot.snapshot_id)


class _ParseResultStub:
    def __init__(self, snapshot_id) -> None:
        self.snapshot_id = snapshot_id
        self.parser_family = "season_rounds"
        self.parser_version = "v1"
        self.status = "parsed"
        self.entity_upserts: dict = {}
        self.relation_upserts: dict = {}
        self.metric_rows: dict = {"season_round": ()}
        self.warnings: tuple = ()
        self.unsupported_fragments: tuple = ()
        self.observed_root_keys: tuple = ()


class _FakeRawRepository:
    def __init__(self, snapshots: dict[int, RawSnapshot]) -> None:
        self.snapshots = snapshots
        self.fetched: list[int] = []

    async def fetch_payload_snapshot(self, executor, snapshot_id: int) -> RawSnapshot:
        self.fetched.append(int(snapshot_id))
        snap = self.snapshots.get(int(snapshot_id))
        if snap is None:
            raise KeyError(snapshot_id)
        return snap


class _FakeNormalizeRepository:
    def __init__(self) -> None:
        self.persisted: list = []

    async def persist_parse_result(self, executor, result, *, skip_entity_upserts=False) -> None:
        self.persisted.append((result, skip_entity_upserts))


class _FakeConnection:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeDatabase:
    def __init__(self) -> None:
        self.tx_count = 0

    def transaction(self):
        self.tx_count += 1
        return _FakeConnection()


class _FakeStreamQueue:
    def ensure_group(self, *args, **kwargs):
        pass


class NormalizeStreamWorkerTests(unittest.IsolatedAsyncioTestCase):
    def _make_envelope(self, *, snapshot_id):
        return JobEnvelope.create(
            job_type=JOB_NORMALIZE_SNAPSHOT,
            sport_slug="football",
            entity_type=None,
            entity_id=None,
            scope="normalize",
            params={
                "snapshot_id": int(snapshot_id),
                "endpoint_pattern": UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT.pattern,
            },
            priority=0,
            trace_id="t-x",
            capability_hint="normalize",
        )

    def _entry(self, envelope) -> StreamEntry:
        payload = {str(k): str(v) for k, v in encode_stream_job(envelope).items()}
        return StreamEntry(
            stream="stream:etl:normalize",
            message_id="0-1",
            values=payload,
        )

    async def test_loads_snapshot_and_persists_parse_result(self) -> None:
        snapshot = RawSnapshot(
            snapshot_id=42,
            endpoint_pattern=UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT.pattern,
            sport_slug="football",
            source_url="https://example/test",
            resolved_url="https://example/test",
            envelope_key="payload",
            http_status=200,
            payload={"rounds": [{"round": 1}, {"round": 2}]},
            fetched_at="2026-01-01T00:00:00Z",
            context_unique_tournament_id=17,
            context_season_id=76986,
        )
        raw_repo = _FakeRawRepository({42: snapshot})
        norm_repo = _FakeNormalizeRepository()
        registry = _RecordingParserRegistry()

        worker = NormalizeStreamWorker(
            database=_FakeDatabase(),
            raw_repository=raw_repo,
            normalize_repository=norm_repo,
            parser_registry=registry,
            delayed_scheduler=_StubScheduler(),
            delayed_payload_store=_StubPayloadStore(),
            queue=_FakeStreamQueue(),
            consumer="normalize-1",
        )
        envelope = self._make_envelope(snapshot_id=42)
        result = await worker.handle(self._entry(envelope))
        self.assertEqual(result, "completed")
        self.assertEqual(raw_repo.fetched, [42])
        self.assertEqual(len(registry.parsed), 1)
        self.assertEqual(registry.parsed[0].snapshot_id, 42)
        self.assertEqual(len(norm_repo.persisted), 1)
        persisted_result, skip = norm_repo.persisted[0]
        self.assertFalse(skip)
        self.assertEqual(persisted_result.snapshot_id, 42)

    async def test_drops_envelope_with_missing_snapshot_id(self) -> None:
        raw_repo = _FakeRawRepository({})
        norm_repo = _FakeNormalizeRepository()

        worker = NormalizeStreamWorker(
            database=_FakeDatabase(),
            raw_repository=raw_repo,
            normalize_repository=norm_repo,
            parser_registry=_RecordingParserRegistry(),
            delayed_scheduler=_StubScheduler(),
            delayed_payload_store=_StubPayloadStore(),
            queue=_FakeStreamQueue(),
            consumer="normalize-1",
        )
        envelope = JobEnvelope.create(
            job_type=JOB_NORMALIZE_SNAPSHOT,
            sport_slug=None,
            entity_type=None,
            entity_id=None,
            scope="normalize",
            params={"endpoint_pattern": "/x"},
            priority=0,
            trace_id=None,
            capability_hint="normalize",
        )
        result = await worker.handle(self._entry(envelope))
        self.assertEqual(result, "completed")
        self.assertEqual(raw_repo.fetched, [])
        self.assertEqual(norm_repo.persisted, [])

    async def test_drops_envelope_when_snapshot_missing_in_db(self) -> None:
        raw_repo = _FakeRawRepository({})  # 999 not present
        norm_repo = _FakeNormalizeRepository()

        worker = NormalizeStreamWorker(
            database=_FakeDatabase(),
            raw_repository=raw_repo,
            normalize_repository=norm_repo,
            parser_registry=_RecordingParserRegistry(),
            delayed_scheduler=_StubScheduler(),
            delayed_payload_store=_StubPayloadStore(),
            queue=_FakeStreamQueue(),
            consumer="normalize-1",
        )
        envelope = self._make_envelope(snapshot_id=999)
        result = await worker.handle(self._entry(envelope))
        self.assertEqual(result, "completed")
        self.assertEqual(raw_repo.fetched, [999])
        self.assertEqual(norm_repo.persisted, [])


if __name__ == "__main__":
    unittest.main()
