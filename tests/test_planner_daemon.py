from __future__ import annotations

import json
import unittest

from schema_inspector.jobs.envelope import JobEnvelope
from schema_inspector.jobs.types import JOB_HYDRATE_EVENT_ROOT, JOB_REFRESH_LIVE_EVENT
from schema_inspector.queue.delayed import DelayedJob
from schema_inspector.queue.live_state import LiveEventState
from schema_inspector.queue.streams import STREAM_DISCOVERY, STREAM_HYDRATE, STREAM_LIVE_HOT, STREAM_LIVE_WARM


class PlannerDaemonTests(unittest.IsolatedAsyncioTestCase):
    async def test_planner_daemon_publishes_due_delayed_jobs_into_target_streams(self) -> None:
        from schema_inspector.services.planner_daemon import PlannerDaemon

        delayed_job = DelayedJob(job_id="job-123", run_at_epoch_ms=1_800_000_000_000)
        envelope = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="football",
            entity_type="event",
            entity_id=501,
            scope="scheduled",
            params={"hydration_mode": "core"},
            priority=20,
            trace_id="trace-1",
        )
        queue = _FakeQueue()
        daemon = PlannerDaemon(
            queue=queue,
            delayed_scheduler=_FakeDelayedScheduler((delayed_job,)),
            delayed_job_loader=lambda job_id: envelope if job_id == "job-123" else None,
            live_state_store=_FakeLiveStateStore(),
            scheduled_targets=(),
            now_ms_factory=lambda: 1_800_000_000_000,
        )

        await daemon.tick(now_ms=1_800_000_000_000)

        self.assertEqual(len(queue.published), 1)
        stream, payload = queue.published[0]
        self.assertEqual(stream, STREAM_HYDRATE)
        self.assertEqual(payload["job_id"], envelope.job_id)
        self.assertEqual(payload["job_type"], JOB_HYDRATE_EVENT_ROOT)
        self.assertEqual(json.loads(payload["params_json"]), {"hydration_mode": "core"})

    async def test_planner_daemon_reads_live_lanes_and_emits_refresh_jobs(self) -> None:
        from schema_inspector.services.planner_daemon import PlannerDaemon

        queue = _FakeQueue()
        live_state_store = _FakeLiveStateStore(
            due_by_lane={
                "hot": (7001,),
                "warm": (7002,),
            },
            states_by_event={
                7001: LiveEventState(
                    event_id=7001,
                    sport_slug="football",
                    status_type="inprogress",
                    poll_profile="hot",
                    last_seen_at=1,
                    last_ingested_at=1,
                    last_changed_at=1,
                    next_poll_at=1_800_000_000_100,
                    hot_until=1_800_000_000_100,
                    home_score=1,
                    away_score=0,
                    version_hint=None,
                    is_finalized=False,
                ),
                7002: LiveEventState(
                    event_id=7002,
                    sport_slug="basketball",
                    status_type="inprogress",
                    poll_profile="warm",
                    last_seen_at=1,
                    last_ingested_at=1,
                    last_changed_at=1,
                    next_poll_at=1_800_000_000_200,
                    hot_until=None,
                    home_score=88,
                    away_score=80,
                    version_hint=None,
                    is_finalized=False,
                ),
            },
        )
        daemon = PlannerDaemon(
            queue=queue,
            delayed_scheduler=_FakeDelayedScheduler(()),
            delayed_job_loader=lambda job_id: None,
            live_state_store=live_state_store,
            scheduled_targets=(),
            now_ms_factory=lambda: 1_800_000_000_000,
        )

        await daemon.tick(now_ms=1_800_000_000_000)

        self.assertEqual([stream for stream, _ in queue.published], [STREAM_LIVE_HOT, STREAM_LIVE_WARM])
        self.assertEqual([payload["job_type"] for _, payload in queue.published], [JOB_REFRESH_LIVE_EVENT, JOB_REFRESH_LIVE_EVENT])
        self.assertEqual(queue.published[0][1]["sport_slug"], "football")
        self.assertEqual(queue.published[1][1]["sport_slug"], "basketball")

    async def test_planner_daemon_runs_scheduled_planning_per_sport_interval(self) -> None:
        from schema_inspector.services.planner_daemon import PlannerDaemon, ScheduledPlanningTarget

        queue = _FakeQueue()
        daemon = PlannerDaemon(
            queue=queue,
            delayed_scheduler=_FakeDelayedScheduler(()),
            delayed_job_loader=lambda job_id: None,
            live_state_store=_FakeLiveStateStore(),
            scheduled_targets=(
                ScheduledPlanningTarget(
                    sport_slug="football",
                    interval_ms=60_000,
                    date_factory=lambda now_ms: "2026-04-17",
                ),
                ScheduledPlanningTarget(
                    sport_slug="baseball",
                    interval_ms=60_000,
                    date_factory=lambda now_ms: "2026-04-17",
                ),
            ),
            now_ms_factory=lambda: 1_800_000_000_000,
        )

        await daemon.tick(now_ms=1_800_000_000_000)
        await daemon.tick(now_ms=1_800_000_030_000)
        await daemon.tick(now_ms=1_800_000_061_000)

        self.assertEqual(len(queue.published), 4)
        self.assertEqual([stream for stream, _ in queue.published], [STREAM_DISCOVERY] * 4)
        self.assertEqual([payload["sport_slug"] for _, payload in queue.published], ["football", "baseball", "football", "baseball"])
        self.assertEqual(json.loads(queue.published[0][1]["params_json"]), {"date": "2026-04-17"})


class _FakeQueue:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []

    def publish(self, stream: str, values: dict[str, object]) -> str:
        self.published.append((stream, dict(values)))
        return f"{stream}:{len(self.published)}"


class _FakeDelayedScheduler:
    def __init__(self, jobs: tuple[DelayedJob, ...]) -> None:
        self.jobs = list(jobs)
        self.calls: list[tuple[int, int]] = []

    def pop_due(self, *, now_epoch_ms: int, limit: int = 100) -> tuple[DelayedJob, ...]:
        self.calls.append((now_epoch_ms, limit))
        jobs = tuple(self.jobs)
        self.jobs.clear()
        return jobs


class _FakeLiveStateStore:
    def __init__(
        self,
        *,
        due_by_lane: dict[str, tuple[int, ...]] | None = None,
        states_by_event: dict[int, LiveEventState] | None = None,
    ) -> None:
        self.due_by_lane = dict(due_by_lane or {})
        self.states_by_event = dict(states_by_event or {})

    def due_events(self, *, lane: str, now_ms: int) -> tuple[int, ...]:
        del now_ms
        return self.due_by_lane.get(lane, ())

    def fetch(self, event_id: int) -> LiveEventState | None:
        return self.states_by_event.get(event_id)


if __name__ == "__main__":
    unittest.main()
