from __future__ import annotations

import unittest
from types import SimpleNamespace

from schema_inspector.queue.streams import StreamEntry
from schema_inspector.services.service_app import DelayedEnvelopeStore, ServiceApp


class DelayedEnvelopeStoreTests(unittest.TestCase):
    def test_save_entry_increments_attempt_for_exponential_backoff(self) -> None:
        backend = _FakeRedisBackend()
        store = DelayedEnvelopeStore(backend)

        store.save_entry(
            StreamEntry(
                stream="stream:etl:test",
                message_id="1-1",
                values={
                    "job_id": "job-1",
                    "job_type": "discover_sport_surface",
                    "attempt": "2",
                },
            )
        )

        job = store.load("job-1")

        self.assertIsNotNone(job)
        self.assertEqual(job.attempt, 3)


class ServiceAppTests(unittest.TestCase):
    def test_build_live_worker_wires_shared_inflight_store(self) -> None:
        app = SimpleNamespace(
            redis_backend=_FakeRedisBackend(),
            stream_queue=_FakeStreamQueue(),
            live_state_store=None,
            database=None,
        )
        service_app = ServiceApp(app)

        worker = service_app.build_live_worker(lane="tier_1", consumer_name="worker-live-tier-1-1")

        self.assertIs(worker.in_flight_store, service_app.live_event_inflight_store)


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(key, {}).update(dict(mapping))
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))

    def hdel(self, key: str, field: str) -> int:
        bucket = self.hashes.setdefault(key, {})
        existed = field in bucket
        bucket.pop(field, None)
        return 1 if existed else 0


class _FakeStreamQueue:
    def __init__(self) -> None:
        self.groups: list[tuple[str, str]] = []

    def ensure_group(self, stream: str, group: str) -> None:
        self.groups.append((stream, group))


if __name__ == "__main__":
    unittest.main()
