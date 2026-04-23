from __future__ import annotations

import json
import unittest

from schema_inspector.queue.streams import STREAM_DISCOVERY, STREAM_HYDRATE, StreamEntry


class DiscoveryWorkerServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_discovery_worker_expands_scheduled_surface_into_hydrate_jobs(self) -> None:
        from schema_inspector.workers.discovery_worker import DiscoveryWorker

        orchestrator = _FakeDiscoveryOrchestrator(event_ids=(501, 777))
        queue = _FakeQueue()
        worker = DiscoveryWorker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="worker-discovery-1",
            timeout_s=12.5,
        )

        result = await worker.handle(
            StreamEntry(
                stream=STREAM_DISCOVERY,
                message_id="1-1",
                values={
                    "job_id": "job-1",
                    "job_type": "discover_sport_surface",
                    "sport_slug": "football",
                    "entity_type": "sport",
                    "entity_id": "",
                    "scope": "scheduled",
                    "params_json": '{"date":"2026-04-17"}',
                    "priority": "50",
                    "attempt": "1",
                    "scheduled_at": "2026-04-17T20:00:00+00:00",
                    "idempotency_key": "key-1",
                },
            )
        )

        self.assertEqual(result, "published:2")
        self.assertEqual(orchestrator.scheduled_calls, [("football", "2026-04-17", 12.5)])
        self.assertEqual(orchestrator.live_calls, [])
        self.assertEqual(queue.published_streams, [STREAM_HYDRATE, STREAM_HYDRATE])
        first_payload = queue.published_payloads[0]
        self.assertEqual(first_payload["job_type"], "hydrate_event_root")
        self.assertEqual(first_payload["entity_id"], 501)
        self.assertEqual(json.loads(str(first_payload["params_json"])), {"hydration_mode": "core"})
        self.assertEqual(worker.runtime.group, "cg:discovery")
        self.assertEqual(worker.runtime.stream, STREAM_DISCOVERY)

    async def test_discovery_worker_expands_live_surface_into_full_hydrate_jobs(self) -> None:
        from schema_inspector.workers.discovery_worker import DiscoveryWorker

        orchestrator = _FakeDiscoveryOrchestrator(event_ids=(901,))
        queue = _FakeQueue()
        worker = DiscoveryWorker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="worker-discovery-1",
            timeout_s=9.0,
        )

        result = await worker.handle(
            StreamEntry(
                stream=STREAM_DISCOVERY,
                message_id="1-2",
                values={
                    "job_id": "job-2",
                    "job_type": "discover_sport_surface",
                    "sport_slug": "tennis",
                    "entity_type": "sport",
                    "entity_id": "",
                    "scope": "live",
                    "params_json": "{}",
                    "priority": "50",
                    "attempt": "1",
                    "scheduled_at": "2026-04-17T20:00:00+00:00",
                    "idempotency_key": "key-2",
                },
            )
        )

        self.assertEqual(result, "published:1")
        self.assertEqual(orchestrator.live_calls, [("tennis", 9.0)])
        payload = queue.published_payloads[0]
        self.assertEqual(json.loads(str(payload["params_json"])), {"hydration_mode": "full"})

    async def test_discovery_worker_skips_duplicate_hydrate_jobs_when_freshness_blocks_publish(self) -> None:
        from schema_inspector.workers.discovery_worker import DiscoveryWorker

        orchestrator = _FakeDiscoveryOrchestrator(event_ids=(501, 777))
        queue = _FakeQueue()
        freshness_policy = _FakeFreshnessPolicy(allowed_event_ids={501})
        worker = DiscoveryWorker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="worker-discovery-1",
            timeout_s=12.5,
            freshness_policy=freshness_policy,
        )

        result = await worker.handle(
            StreamEntry(
                stream=STREAM_DISCOVERY,
                message_id="1-3",
                values={
                    "job_id": "job-3",
                    "job_type": "discover_sport_surface",
                    "sport_slug": "football",
                    "entity_type": "sport",
                    "entity_id": "",
                    "scope": "scheduled",
                    "params_json": '{"date":"2026-04-17"}',
                    "priority": "50",
                    "attempt": "1",
                    "scheduled_at": "2026-04-17T20:00:00+00:00",
                    "idempotency_key": "key-3",
                },
            )
        )

        self.assertEqual(result, "published:1")
        self.assertEqual(queue.published_streams, [STREAM_HYDRATE])
        self.assertEqual(freshness_policy.calls, [(501, "core", False), (777, "core", False)])

    async def test_discovery_worker_force_rehydrates_corrected_events_even_when_freshness_would_skip(self) -> None:
        from schema_inspector.services.surface_correction_detector import SurfaceCorrection
        from schema_inspector.workers.discovery_worker import DiscoveryWorker

        orchestrator = _FakeDiscoveryOrchestrator(
            event_ids=(777,),
            scheduled_result=_FakeSurfaceDiscoveryResult(
                event_ids=(777,),
                corrections=(SurfaceCorrection(event_id=777, reason="score_changed"),),
            ),
        )
        queue = _FakeQueue()
        freshness_policy = _FakeFreshnessPolicy(allowed_event_ids=())
        worker = DiscoveryWorker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="worker-discovery-1",
            timeout_s=12.5,
            freshness_policy=freshness_policy,
        )

        result = await worker.handle(
            StreamEntry(
                stream=STREAM_DISCOVERY,
                message_id="1-4",
                values={
                    "job_id": "job-4",
                    "job_type": "discover_sport_surface",
                    "sport_slug": "football",
                    "entity_type": "sport",
                    "entity_id": "",
                    "scope": "scheduled",
                    "params_json": '{"date":"2026-04-17"}',
                    "priority": "50",
                    "attempt": "1",
                    "scheduled_at": "2026-04-17T20:00:00+00:00",
                    "idempotency_key": "key-4",
                },
            )
        )

        self.assertEqual(result, "published:1")
        payload = queue.published_payloads[0]
        self.assertEqual(
            json.loads(str(payload["params_json"])),
            {
                "hydration_mode": "full",
                "force_rehydrate": True,
                "correction_reason": "score_changed",
            },
        )
        self.assertEqual(freshness_policy.calls, [])

    async def test_discovery_worker_skips_non_force_hydrate_fanout_when_backpressure_blocks_operational_lane(self) -> None:
        from schema_inspector.workers.discovery_worker import DiscoveryWorker

        orchestrator = _FakeDiscoveryOrchestrator(event_ids=(501, 777))
        queue = _FakeQueue()
        freshness_policy = _FakeFreshnessPolicy(allowed_event_ids={501, 777})
        worker = DiscoveryWorker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="worker-discovery-1",
            timeout_s=12.5,
            freshness_policy=freshness_policy,
            hydrate_backpressure=_FakeBackpressure("stream:etl:hydrate:cg:hydrate:lag=250001>100000"),
        )

        result = await worker.handle(
            StreamEntry(
                stream=STREAM_DISCOVERY,
                message_id="1-5",
                values={
                    "job_id": "job-5",
                    "job_type": "discover_sport_surface",
                    "sport_slug": "football",
                    "entity_type": "sport",
                    "entity_id": "",
                    "scope": "scheduled",
                    "params_json": '{"date":"2026-04-17"}',
                    "priority": "50",
                    "attempt": "1",
                    "scheduled_at": "2026-04-17T20:00:00+00:00",
                    "idempotency_key": "key-5",
                },
            )
        )

        self.assertEqual(result, "published:0")
        self.assertEqual(queue.published_streams, [])
        self.assertEqual(freshness_policy.calls, [])

    async def test_discovery_worker_still_publishes_force_rehydrate_jobs_under_backpressure(self) -> None:
        from schema_inspector.services.surface_correction_detector import SurfaceCorrection
        from schema_inspector.workers.discovery_worker import DiscoveryWorker

        orchestrator = _FakeDiscoveryOrchestrator(
            event_ids=(777, 888),
            scheduled_result=_FakeSurfaceDiscoveryResult(
                event_ids=(777, 888),
                corrections=(SurfaceCorrection(event_id=777, reason="score_changed"),),
            ),
        )
        queue = _FakeQueue()
        freshness_policy = _FakeFreshnessPolicy(allowed_event_ids={888})
        worker = DiscoveryWorker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="worker-discovery-1",
            timeout_s=12.5,
            freshness_policy=freshness_policy,
            hydrate_backpressure=_FakeBackpressure("stream:etl:hydrate:cg:hydrate:lag=250001>100000"),
        )

        result = await worker.handle(
            StreamEntry(
                stream=STREAM_DISCOVERY,
                message_id="1-6",
                values={
                    "job_id": "job-6",
                    "job_type": "discover_sport_surface",
                    "sport_slug": "football",
                    "entity_type": "sport",
                    "entity_id": "",
                    "scope": "scheduled",
                    "params_json": '{"date":"2026-04-17"}',
                    "priority": "50",
                    "attempt": "1",
                    "scheduled_at": "2026-04-17T20:00:00+00:00",
                    "idempotency_key": "key-6",
                },
            )
        )

        self.assertEqual(result, "published:1")
        self.assertEqual(queue.published_streams, [STREAM_HYDRATE])
        self.assertEqual(
            json.loads(str(queue.published_payloads[0]["params_json"])),
            {
                "hydration_mode": "full",
                "force_rehydrate": True,
                "correction_reason": "score_changed",
            },
        )
        self.assertEqual(freshness_policy.calls, [])

    async def test_historical_discovery_worker_defers_non_force_hydrate_fanout_under_backpressure(self) -> None:
        from schema_inspector.services.retry_policy import AdmissionDeferredError
        from schema_inspector.workers.discovery_worker import DiscoveryWorker

        orchestrator = _FakeDiscoveryOrchestrator(event_ids=(901, 902))
        queue = _FakeQueue()
        worker = DiscoveryWorker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="worker-discovery-1",
            timeout_s=12.5,
            delayed_scheduler=_FakeDelayedScheduler(),
            delayed_payload_store=_FakePayloadStore(),
            hydrate_backpressure=_FakeBackpressure("stream:etl:historical_hydrate:cg:historical_hydrate:lag=90001>50000"),
            defer_on_backpressure=True,
            admission_delay_ms=30_000,
        )

        with self.assertRaises(AdmissionDeferredError) as ctx:
            await worker.handle(
                StreamEntry(
                    stream=STREAM_DISCOVERY,
                    message_id="1-7",
                    values={
                        "job_id": "job-7",
                        "job_type": "discover_sport_surface",
                        "sport_slug": "tennis",
                        "entity_type": "sport",
                        "entity_id": "",
                        "scope": "historical",
                        "params_json": '{"date":"2020-04-17"}',
                        "priority": "50",
                        "attempt": "1",
                        "scheduled_at": "2026-04-17T20:00:00+00:00",
                        "idempotency_key": "key-7",
                    },
                )
            )

        self.assertEqual(ctx.exception.delay_ms, 30_000)
        self.assertEqual(queue.published_streams, [])

    async def test_discovery_worker_schedules_retry_for_deadlock_errors(self) -> None:
        from schema_inspector.workers.discovery_worker import DiscoveryWorker

        scheduler = _FakeDelayedScheduler()
        payload_store = _FakePayloadStore()
        worker = DiscoveryWorker(
            orchestrator=_FakeDiscoveryOrchestrator(event_ids=()),
            queue=_FakeQueue(),
            consumer="worker-discovery-1",
            delayed_scheduler=scheduler,
            delayed_payload_store=payload_store,
            now_ms_factory=lambda: 1_800_000_000_000,
        )
        entry = StreamEntry(
            stream=STREAM_DISCOVERY,
            message_id="1-9",
            values={
                "job_id": "job-9",
                "job_type": "discover_sport_surface",
                "sport_slug": "football",
                "entity_type": "sport",
                "entity_id": "",
                "scope": "scheduled",
                "params_json": '{"date":"2026-04-17"}',
                "attempt": "3",
                "idempotency_key": "key-9",
            },
        )

        result = await worker.retry_later(entry, RuntimeError("deadlock detected"), delay_ms=20_000)

        self.assertEqual(result, "requeued")
        self.assertEqual(payload_store.saved_message_ids, ["1-9"])
        self.assertEqual(scheduler.calls, [("job-9", 1_800_000_020_000)])

    async def test_discovery_worker_runtime_requeues_upstream_access_denied_without_crashing(self) -> None:
        from schema_inspector.sofascore_client import SofascoreAccessDeniedError
        from schema_inspector.workers.discovery_worker import DiscoveryWorker

        scheduler = _FakeDelayedScheduler()
        payload_store = _FakePayloadStore()
        queue = _FakeQueue()
        worker = DiscoveryWorker(
            orchestrator=_FailingDiscoveryOrchestrator(SofascoreAccessDeniedError("Access denied by upstream")),
            queue=queue,
            consumer="worker-discovery-1",
            delayed_scheduler=scheduler,
            delayed_payload_store=payload_store,
            now_ms_factory=lambda: 1_800_000_000_000,
            block_ms=0,
        )
        entry = StreamEntry(
            stream=STREAM_DISCOVERY,
            message_id="1-10",
            values={
                "job_id": "job-10",
                "job_type": "discover_sport_surface",
                "sport_slug": "football",
                "entity_type": "sport",
                "entity_id": "",
                "scope": "live",
                "params_json": "{}",
                "attempt": "1",
            },
        )

        result = await worker.runtime._handle_entry(entry)

        self.assertEqual(result, "requeued")
        self.assertEqual(payload_store.saved_message_ids, ["1-10"])
        self.assertEqual(scheduler.calls, [("job-10", 1_800_000_030_000)])
        self.assertEqual(queue.acked, [(STREAM_DISCOVERY, "cg:discovery", ("1-10",))])


class _FakeDiscoveryOrchestrator:
    def __init__(
        self,
        *,
        event_ids: tuple[int, ...],
        scheduled_result: _FakeSurfaceDiscoveryResult | None = None,
    ) -> None:
        self.event_ids = tuple(event_ids)
        self.scheduled_result = scheduled_result
        self.scheduled_calls: list[tuple[str, str, float]] = []
        self.live_calls: list[tuple[str, float]] = []

    async def discover_scheduled_event_ids(self, *, sport_slug: str, date: str, timeout: float):
        self.scheduled_calls.append((sport_slug, date, timeout))
        return self.event_ids

    async def discover_scheduled_events(self, *, sport_slug: str, date: str, timeout: float):
        self.scheduled_calls.append((sport_slug, date, timeout))
        if self.scheduled_result is not None:
            return self.scheduled_result
        return _FakeSurfaceDiscoveryResult(event_ids=self.event_ids, corrections=())

    async def discover_live_event_ids(self, *, sport_slug: str, timeout: float):
        self.live_calls.append((sport_slug, timeout))
        return self.event_ids


class _FailingDiscoveryOrchestrator:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    async def discover_live_events(self, *, sport_slug: str, timeout: float):
        del sport_slug, timeout
        raise self.exc


class _FakeQueue:
    def __init__(self) -> None:
        self.published_streams: list[str] = []
        self.published_payloads: list[dict[str, object]] = []
        self.acked: list[tuple[str, str, tuple[str, ...]]] = []

    def publish(self, stream: str, values: dict[str, object]) -> str:
        self.published_streams.append(stream)
        self.published_payloads.append(dict(values))
        return f"{stream}:{len(self.published_streams)}"

    def read_group(self, *args, **kwargs):
        del args, kwargs
        return ()

    def ack(self, *args, **kwargs):
        stream, group, message_ids = args
        del kwargs
        self.acked.append((stream, group, tuple(message_ids)))
        return len(message_ids)


class _FakeDelayedScheduler:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def schedule(self, job_id: str, *, run_at_epoch_ms: int) -> None:
        self.calls.append((job_id, run_at_epoch_ms))


class _FakePayloadStore:
    def __init__(self) -> None:
        self.saved_message_ids: list[str] = []

    def save_entry(self, entry: StreamEntry) -> None:
        self.saved_message_ids.append(entry.message_id)


class _FakeFreshnessPolicy:
    def __init__(self, *, allowed_event_ids: tuple[int, ...] | set[int]) -> None:
        self.allowed_event_ids = {int(item) for item in allowed_event_ids}
        self.calls: list[tuple[int, str, bool]] = []

    def claim_event_hydration(
        self,
        *,
        event_id: int,
        hydration_mode: str,
        force_rehydrate: bool,
        now_ms: int | None = None,
    ) -> bool:
        del now_ms
        self.calls.append((int(event_id), str(hydration_mode), bool(force_rehydrate)))
        return int(event_id) in self.allowed_event_ids


class _FakeBackpressure:
    def __init__(self, reason: str | None) -> None:
        self.reason = reason

    def blocking_reason(self) -> str | None:
        return self.reason


class _FakeParsedEvent:
    def __init__(self, event_id: int) -> None:
        self.id = int(event_id)


class _FakeParsedBundle:
    def __init__(self, event_ids: tuple[int, ...]) -> None:
        self.events = tuple(_FakeParsedEvent(event_id) for event_id in event_ids)


class _FakeSurfaceDiscoveryResult:
    def __init__(self, *, event_ids: tuple[int, ...], corrections: tuple[object, ...]) -> None:
        self.parsed = _FakeParsedBundle(event_ids)
        self.corrections = tuple(corrections)


if __name__ == "__main__":
    unittest.main()
