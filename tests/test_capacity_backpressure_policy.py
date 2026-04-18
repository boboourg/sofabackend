from __future__ import annotations

import unittest

from schema_inspector.jobs.envelope import JobEnvelope
from schema_inspector.jobs.types import JOB_DISCOVER_SPORT_SURFACE
from schema_inspector.queue.streams import (
    STREAM_DISCOVERY,
    STREAM_HISTORICAL_DISCOVERY,
    STREAM_HISTORICAL_HYDRATE,
    STREAM_HYDRATE,
    ConsumerGroupInfo,
    StreamEntry,
)


class CapacityBackpressurePolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_planner_resumes_scheduled_publish_after_hydrate_lag_recovers(self) -> None:
        from schema_inspector.services.backpressure import BackpressureLimit, QueueBackpressure
        from schema_inspector.services.planner_daemon import PlannerDaemon, ScheduledPlanningTarget

        queue = _MutableQueue(
            group_infos={
                (STREAM_HYDRATE, "cg:hydrate"): ConsumerGroupInfo(
                    consumers=3,
                    pending=0,
                    last_delivered_id="1-0",
                    entries_read=100,
                    lag=250_000,
                ),
            }
        )
        daemon = PlannerDaemon(
            queue=queue,
            delayed_scheduler=_FakeDelayedScheduler(()),
            delayed_job_loader=lambda job_id: None,
            live_state_store=_FakeLiveStateStore(),
            scheduled_targets=(
                ScheduledPlanningTarget(
                    sport_slug="football",
                    interval_ms=60_000,
                    date_factory=lambda now_ms: "2026-04-18",
                ),
            ),
            scheduled_backpressure=QueueBackpressure(
                queue=queue,
                limits=(BackpressureLimit(stream=STREAM_HYDRATE, group="cg:hydrate", max_lag=100_000),),
            ),
            now_ms_factory=lambda: 1_800_000_000_000,
        )

        await daemon.tick(now_ms=1_800_000_000_000)
        self.assertEqual(queue.published, [])

        queue.group_infos[(STREAM_HYDRATE, "cg:hydrate")] = ConsumerGroupInfo(
            consumers=3,
            pending=0,
            last_delivered_id="2-0",
            entries_read=250,
            lag=50,
        )

        await daemon.tick(now_ms=1_800_000_070_000)
        self.assertEqual(len(queue.published), 1)
        self.assertEqual(queue.published[0][0], STREAM_DISCOVERY)

    async def test_operational_and_historical_lanes_keep_separate_admission_rules(self) -> None:
        from schema_inspector.services.retry_policy import AdmissionDeferredError
        from schema_inspector.workers.discovery_worker import DiscoveryWorker

        operational_worker = DiscoveryWorker(
            orchestrator=_FakeDiscoveryOrchestrator(event_ids=(501,)),
            queue=_FakePublishQueue(),
            consumer="discovery-1",
            timeout_s=12.5,
        )
        operational_result = await operational_worker.handle(
            StreamEntry(
                stream=STREAM_DISCOVERY,
                message_id="1-1",
                values=_surface_payload(job_id="job-1", scope="scheduled", sport_slug="football", date="2026-04-18"),
            )
        )
        self.assertEqual(operational_result, "published:1")

        historical_worker = DiscoveryWorker(
            orchestrator=_FakeDiscoveryOrchestrator(event_ids=(901,)),
            queue=_FakePublishQueue(),
            consumer="historical-discovery-1",
            group="cg:historical_discovery",
            stream=STREAM_HISTORICAL_DISCOVERY,
            hydrate_stream=STREAM_HISTORICAL_HYDRATE,
            timeout_s=12.5,
            hydrate_backpressure=_FakeBackpressure("stream:etl:historical_hydrate:cg:historical_hydrate:lag=90001>50000"),
            defer_on_backpressure=True,
            admission_delay_ms=30_000,
            delayed_scheduler=_FakeDelayedScheduler(()),
        )

        with self.assertRaises(AdmissionDeferredError):
            await historical_worker.handle(
                StreamEntry(
                    stream=STREAM_HISTORICAL_DISCOVERY,
                    message_id="1-2",
                    values=_surface_payload(job_id="job-2", scope="historical", sport_slug="tennis", date="2020-04-18"),
                )
            )

    async def test_historical_delayed_jobs_replay_into_historical_stream_after_recovery(self) -> None:
        from schema_inspector.queue.delayed import DelayedJob
        from schema_inspector.services.planner_daemon import PlannerDaemon

        delayed_job = DelayedJob(job_id="job-historical-discovery", run_at_epoch_ms=1_800_000_000_000)
        envelope = JobEnvelope.create(
            job_type=JOB_DISCOVER_SPORT_SURFACE,
            sport_slug="football",
            entity_type="sport",
            entity_id=None,
            scope="historical",
            params={"date": "2020-04-17"},
            priority=20,
            trace_id="trace-historical-discovery",
        )
        queue = _MutableQueue()
        daemon = PlannerDaemon(
            queue=queue,
            delayed_scheduler=_FakeDelayedScheduler((delayed_job,)),
            delayed_job_loader=lambda job_id: envelope if job_id == "job-historical-discovery" else None,
            live_state_store=_FakeLiveStateStore(),
            scheduled_targets=(),
            now_ms_factory=lambda: 1_800_000_000_000,
        )

        await daemon.tick(now_ms=1_800_000_000_000)

        self.assertEqual(len(queue.published), 1)
        self.assertEqual(queue.published[0][0], STREAM_HISTORICAL_DISCOVERY)


def _surface_payload(*, job_id: str, scope: str, sport_slug: str, date: str) -> dict[str, str]:
    return {
        "job_id": job_id,
        "job_type": "discover_sport_surface",
        "sport_slug": sport_slug,
        "entity_type": "sport",
        "entity_id": "",
        "scope": scope,
        "params_json": f'{{"date":"{date}"}}',
        "priority": "50",
        "attempt": "1",
        "scheduled_at": "2026-04-17T20:00:00+00:00",
        "idempotency_key": f"key-{job_id}",
    }


class _FakeDelayedScheduler:
    def __init__(self, jobs):
        self.jobs = tuple(jobs)

    def pop_due(self, *, now_epoch_ms: int):
        del now_epoch_ms
        return tuple(self.jobs)


class _FakeLiveStateStore:
    def due_events(self, lane: str, now_ms: int):
        del lane, now_ms
        return ()

    def fetch(self, event_id: int):
        del event_id
        return None


class _MutableQueue:
    def __init__(self, *, group_infos: dict[tuple[str, str], ConsumerGroupInfo] | None = None) -> None:
        self.group_infos = dict(group_infos or {})
        self.published: list[tuple[str, dict[str, object]]] = []

    def publish(self, stream: str, payload: dict[str, object]) -> str:
        self.published.append((stream, payload))
        return f"{len(self.published)}-0"

    def group_info(self, stream: str, group: str) -> ConsumerGroupInfo | None:
        return self.group_infos.get((stream, group))


class _FakePublishQueue:
    def __init__(self) -> None:
        self.published_streams: list[str] = []

    def publish(self, stream: str, payload: dict[str, object]) -> str:
        del payload
        self.published_streams.append(stream)
        return f"{len(self.published_streams)}-0"


class _FakeDiscoveryOrchestrator:
    def __init__(self, *, event_ids: tuple[int, ...]) -> None:
        self.event_ids = tuple(event_ids)

    async def discover_live_events(self, *, sport_slug: str, timeout: float):
        del sport_slug, timeout
        return self.event_ids

    async def discover_scheduled_events(self, *, sport_slug: str, date: str, timeout: float):
        del sport_slug, date, timeout
        return self.event_ids


class _FakeBackpressure:
    def __init__(self, reason: str | None) -> None:
        self.reason = reason

    def blocking_reason(self) -> str | None:
        return self.reason


if __name__ == "__main__":
    unittest.main()
