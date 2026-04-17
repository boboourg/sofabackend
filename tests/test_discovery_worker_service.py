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


class _FakeDiscoveryOrchestrator:
    def __init__(self, *, event_ids: tuple[int, ...]) -> None:
        self.event_ids = tuple(event_ids)
        self.scheduled_calls: list[tuple[str, str, float]] = []
        self.live_calls: list[tuple[str, float]] = []

    async def discover_scheduled_event_ids(self, *, sport_slug: str, date: str, timeout: float):
        self.scheduled_calls.append((sport_slug, date, timeout))
        return self.event_ids

    async def discover_live_event_ids(self, *, sport_slug: str, timeout: float):
        self.live_calls.append((sport_slug, timeout))
        return self.event_ids


class _FakeQueue:
    def __init__(self) -> None:
        self.published_streams: list[str] = []
        self.published_payloads: list[dict[str, object]] = []

    def publish(self, stream: str, values: dict[str, object]) -> str:
        self.published_streams.append(stream)
        self.published_payloads.append(dict(values))
        return f"{stream}:{len(self.published_streams)}"

    def read_group(self, *args, **kwargs):
        del args, kwargs
        return ()

    def ack(self, *args, **kwargs):
        del args, kwargs
        return 0


if __name__ == "__main__":
    unittest.main()
