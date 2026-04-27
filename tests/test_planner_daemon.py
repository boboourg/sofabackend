from __future__ import annotations

import json
import unittest

from schema_inspector.jobs.envelope import JobEnvelope
from schema_inspector.jobs.types import (
    JOB_DISCOVER_SPORT_SURFACE,
    JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH,
    JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH,
    JOB_HYDRATE_EVENT_ROOT,
    JOB_REFRESH_LIVE_EVENT,
)
from schema_inspector.queue.delayed import DelayedJob
from schema_inspector.queue.live_state import LiveEventState, LiveEventStateStore
from schema_inspector.queue.streams import (
    STREAM_DISCOVERY,
    STREAM_HISTORICAL_DISCOVERY,
    STREAM_HISTORICAL_ENRICHMENT,
    STREAM_HISTORICAL_HYDRATE,
    STREAM_HYDRATE,
    STREAM_LIVE_TIER_1,
    STREAM_LIVE_HOT,
    STREAM_LIVE_WARM,
    ConsumerGroupInfo,
)


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

    async def test_planner_daemon_routes_historical_delayed_jobs_into_historical_streams(self) -> None:
        from schema_inspector.services.planner_daemon import PlannerDaemon

        delayed_jobs = (
            DelayedJob(job_id="job-historical-discovery", run_at_epoch_ms=1_800_000_000_000),
            DelayedJob(job_id="job-historical-hydrate", run_at_epoch_ms=1_800_000_000_000),
        )
        historical_discovery = JobEnvelope.create(
            job_type=JOB_DISCOVER_SPORT_SURFACE,
            sport_slug="football",
            entity_type="sport",
            entity_id=None,
            scope="historical",
            params={"date": "2020-04-17"},
            priority=20,
            trace_id="trace-hd",
        )
        historical_hydrate = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="football",
            entity_type="event",
            entity_id=501,
            scope="historical",
            params={"hydration_mode": "core"},
            priority=20,
            trace_id="trace-hh",
        )
        queue = _FakeQueue()
        daemon = PlannerDaemon(
            queue=queue,
            delayed_scheduler=_FakeDelayedScheduler(delayed_jobs),
            delayed_job_loader=lambda job_id: {
                "job-historical-discovery": historical_discovery,
                "job-historical-hydrate": historical_hydrate,
            }.get(job_id),
            live_state_store=_FakeLiveStateStore(),
            scheduled_targets=(),
            now_ms_factory=lambda: 1_800_000_000_000,
        )

        await daemon.tick(now_ms=1_800_000_000_000)

        self.assertEqual(
            [stream for stream, _ in queue.published],
            [STREAM_HISTORICAL_DISCOVERY, STREAM_HISTORICAL_HYDRATE],
        )

    async def test_planner_daemon_routes_historical_enrichment_batch_jobs_into_enrichment_stream(self) -> None:
        from schema_inspector.services.planner_daemon import PlannerDaemon

        delayed_jobs = (
            DelayedJob(job_id="job-historical-event-detail", run_at_epoch_ms=1_800_000_000_000),
            DelayedJob(job_id="job-historical-entities", run_at_epoch_ms=1_800_000_000_000),
        )
        event_detail_envelope = JobEnvelope.create(
            job_type=JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH,
            sport_slug="tennis",
            entity_type="unique_tournament",
            entity_id=25831,
            scope="historical",
            params={"season_ids": [90572]},
            priority=20,
            trace_id="trace-he1",
        )
        entities_envelope = JobEnvelope.create(
            job_type=JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH,
            sport_slug="tennis",
            entity_type="unique_tournament",
            entity_id=25831,
            scope="historical",
            params={"season_ids": [90572]},
            priority=20,
            trace_id="trace-he2",
        )
        queue = _FakeQueue()
        daemon = PlannerDaemon(
            queue=queue,
            delayed_scheduler=_FakeDelayedScheduler(delayed_jobs),
            delayed_job_loader=lambda job_id: {
                "job-historical-event-detail": event_detail_envelope,
                "job-historical-entities": entities_envelope,
            }.get(job_id),
            live_state_store=_FakeLiveStateStore(),
            scheduled_targets=(),
            now_ms_factory=lambda: 1_800_000_000_000,
        )

        await daemon.tick(now_ms=1_800_000_000_000)

        self.assertEqual(
            [stream for stream, _ in queue.published],
            [STREAM_HISTORICAL_ENRICHMENT, STREAM_HISTORICAL_ENRICHMENT],
        )

    async def test_planner_daemon_routes_split_historical_scopes_into_historical_streams(self) -> None:
        from schema_inspector.services.planner_daemon import PlannerDaemon

        delayed_jobs = (
            DelayedJob(job_id="job-historical-deep", run_at_epoch_ms=1_800_000_000_000),
            DelayedJob(job_id="job-historical-recent", run_at_epoch_ms=1_800_000_000_000),
        )
        deep_discovery = JobEnvelope.create(
            job_type=JOB_DISCOVER_SPORT_SURFACE,
            sport_slug="football",
            entity_type="sport",
            entity_id=None,
            scope="historical_deep",
            params={"date": "2020-04-17"},
            priority=20,
            trace_id="trace-hd2",
        )
        recent_hydrate = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="football",
            entity_type="event",
            entity_id=501,
            scope="historical_recent_refresh",
            params={"hydration_mode": "core"},
            priority=20,
            trace_id="trace-hr2",
        )
        queue = _FakeQueue()
        daemon = PlannerDaemon(
            queue=queue,
            delayed_scheduler=_FakeDelayedScheduler(delayed_jobs),
            delayed_job_loader=lambda job_id: {
                "job-historical-deep": deep_discovery,
                "job-historical-recent": recent_hydrate,
            }.get(job_id),
            live_state_store=_FakeLiveStateStore(),
            scheduled_targets=(),
            now_ms_factory=lambda: 1_800_000_000_000,
        )

        await daemon.tick(now_ms=1_800_000_000_000)

        self.assertEqual(
            [stream for stream, _ in queue.published],
            [STREAM_HISTORICAL_DISCOVERY, STREAM_HISTORICAL_HYDRATE],
        )

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
                    dispatch_tier="tier_1",
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
                    dispatch_tier="tier_3",
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

        self.assertEqual([stream for stream, _ in queue.published], [STREAM_LIVE_TIER_1, STREAM_LIVE_WARM])
        self.assertEqual([payload["job_type"] for _, payload in queue.published], [JOB_REFRESH_LIVE_EVENT, JOB_REFRESH_LIVE_EVENT])
        self.assertEqual(queue.published[0][1]["sport_slug"], "football")
        self.assertEqual(queue.published[1][1]["sport_slug"], "basketball")

    async def test_planner_daemon_claims_due_live_events_until_worker_clears_them(self) -> None:
        from schema_inspector.services.planner_daemon import PlannerDaemon

        queue = _FakeQueue()
        backend = _ClaimingRedisBackend()
        live_state_store = LiveEventStateStore(backend)
        live_state_store.upsert(
            LiveEventState(
                event_id=7002,
                sport_slug="basketball",
                status_type="inprogress",
                poll_profile="warm",
                last_seen_at=1,
                last_ingested_at=1,
                last_changed_at=1,
                next_poll_at=1_800_000_000_000,
                hot_until=None,
                home_score=88,
                away_score=80,
                version_hint=None,
                is_finalized=False,
            ),
            lane="warm",
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
        await daemon.tick(now_ms=1_800_000_005_000)

        self.assertEqual(len(queue.published), 1)
        self.assertIn("live:dispatch_claim:7002", backend.claims)

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

    async def test_planner_daemon_skips_scheduled_targets_when_operational_backpressure_blocks(self) -> None:
        from schema_inspector.services.backpressure import BackpressureLimit, QueueBackpressure
        from schema_inspector.services.planner_daemon import PlannerDaemon, ScheduledPlanningTarget

        queue = _FakeQueue(
            group_infos={
                (STREAM_HYDRATE, "cg:hydrate"): ConsumerGroupInfo(
                    consumers=2,
                    pending=0,
                    last_delivered_id="1-0",
                    entries_read=10,
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

    async def test_planner_daemon_still_publishes_delayed_jobs_while_scheduled_targets_are_blocked(self) -> None:
        from schema_inspector.services.backpressure import BackpressureLimit, QueueBackpressure
        from schema_inspector.services.planner_daemon import PlannerDaemon, ScheduledPlanningTarget

        delayed_job = DelayedJob(job_id="job-124", run_at_epoch_ms=1_800_000_000_000)
        delayed_envelope = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="football",
            entity_type="event",
            entity_id=502,
            scope="scheduled",
            params={"hydration_mode": "core"},
            priority=20,
            trace_id="trace-2",
        )
        queue = _FakeQueue(
            group_infos={
                (STREAM_HYDRATE, "cg:hydrate"): ConsumerGroupInfo(
                    consumers=2,
                    pending=0,
                    last_delivered_id="1-0",
                    entries_read=10,
                    lag=250_000,
                ),
            }
        )
        daemon = PlannerDaemon(
            queue=queue,
            delayed_scheduler=_FakeDelayedScheduler((delayed_job,)),
            delayed_job_loader=lambda job_id: delayed_envelope if job_id == "job-124" else None,
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

        self.assertEqual(len(queue.published), 1)
        self.assertEqual(queue.published[0][0], STREAM_HYDRATE)
        self.assertEqual(queue.published[0][1]["job_id"], delayed_envelope.job_id)

    async def test_planner_daemon_skips_live_refreshes_when_live_backpressure_blocks(self) -> None:
        from schema_inspector.services.backpressure import BackpressureLimit, QueueBackpressure
        from schema_inspector.services.planner_daemon import PlannerDaemon

        queue = _FakeQueue(
            group_infos={
                (STREAM_LIVE_WARM, "cg:live_warm"): ConsumerGroupInfo(
                    consumers=2,
                    pending=0,
                    last_delivered_id="1-0",
                    entries_read=10,
                    lag=900_000,
                ),
            }
        )
        live_state_store = _FakeLiveStateStore(
            due_by_lane={"warm": (7002,)},
            states_by_event={
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
                )
            },
        )
        daemon = PlannerDaemon(
            queue=queue,
            delayed_scheduler=_FakeDelayedScheduler(()),
            delayed_job_loader=lambda job_id: None,
            live_state_store=live_state_store,
            scheduled_targets=(),
            live_backpressure=QueueBackpressure(
                queue=queue,
                limits=(BackpressureLimit(stream=STREAM_LIVE_WARM, group="cg:live_warm", max_lag=100_000),),
            ),
            now_ms_factory=lambda: 1_800_000_000_000,
        )

        await daemon.tick(now_ms=1_800_000_000_000)

        self.assertEqual(queue.published, [])


class _FakeQueue:
    def __init__(self, *, group_infos: dict[tuple[str, str], ConsumerGroupInfo] | None = None) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []
        self.group_infos = dict(group_infos or {})

    def publish(self, stream: str, values: dict[str, object]) -> str:
        self.published.append((stream, dict(values)))
        return f"{stream}:{len(self.published)}"

    def group_info(self, stream: str, group: str) -> ConsumerGroupInfo | None:
        return self.group_infos.get((stream, group))


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


class _ClaimingRedisBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.claims: dict[str, object] = {}

    def hset(self, name: str, key: str | None = None, value: object | None = None, mapping: dict[str, object] | None = None):
        del key, value
        if mapping is None:
            raise TypeError("mapping keyword is required")
        self.hashes.setdefault(name, {}).update(dict(mapping))
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[str(member)] = float(score)
        return len(mapping)

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    def zrangebyscore(self, key: str, min_score: float, max_score: float):
        return [
            member
            for member, score in sorted(self.zsets.get(key, {}).items(), key=lambda item: (item[1], item[0]))
            if min_score <= score <= max_score
        ]

    def set(
        self,
        key: str,
        value: object,
        *,
        nx: bool | None = None,
        px: int | None = None,
    ) -> bool:
        del px
        if nx and key in self.claims:
            return False
        self.claims[key] = value
        return True

    def delete(self, key: str) -> int:
        if key in self.claims:
            del self.claims[key]
            return 1
        return 0


if __name__ == "__main__":
    unittest.main()
