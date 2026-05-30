"""Tests for UpcomingHydratePlannerDaemon (2026-05-30).

Verifies the planner:
* publishes JOB_HYDRATE_EVENT_ROOT with scope="prematch" +
  hydration_mode="full" to stream:etl:hydrate (the mode/scope combo that
  makes the orchestrator fetch the full pre-match matrix),
* honours the per-tick publish cap,
* pauses the whole tick under cg:hydrate lag backpressure,
* threads season_id + the horizon window into the repository query.
"""

from __future__ import annotations

import json
import unittest

from schema_inspector.jobs.types import JOB_HYDRATE_EVENT_ROOT
from schema_inspector.queue.streams import STREAM_HYDRATE
from schema_inspector.services.upcoming_hydrate_planner import (
    UPCOMING_HYDRATE_SCOPE,
    UpcomingHydratePlannerDaemon,
)


class _FakeConnCtx:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, *exc):
        return False


class _FakeDatabase:
    def connection(self):
        return _FakeConnCtx()


class _FakeRepo:
    def __init__(self, ids):
        self._ids = list(ids)
        self.calls: list[dict] = []

    async def upcoming_notstarted_event_ids(
        self, executor, *, horizon_seconds, limit, season_id=None
    ):
        self.calls.append(
            {"horizon_seconds": horizon_seconds, "limit": limit, "season_id": season_id}
        )
        return list(self._ids)[:limit]


class _FakeInfo:
    def __init__(self, lag):
        self.lag = lag


class _FakeQueue:
    def __init__(self, lag=0):
        self.published: list[tuple[str, dict]] = []
        self._lag = lag

    def publish(self, stream, payload):
        self.published.append((stream, dict(payload)))
        return f"{stream}-{len(self.published)}"

    def group_info(self, stream, group):
        return _FakeInfo(self._lag)


def _daemon(*, ids, queue=None, **kw):
    return UpcomingHydratePlannerDaemon(
        database=_FakeDatabase(),
        queue=queue or _FakeQueue(),
        repository=_FakeRepo(ids),
        **kw,
    )


class UpcomingHydratePlannerTests(unittest.IsolatedAsyncioTestCase):
    async def test_publishes_prematch_full_hydrate_jobs(self) -> None:
        queue = _FakeQueue()
        daemon = _daemon(ids=[15186710, 15186720, 15186836], queue=queue)
        published = await daemon.tick()

        self.assertEqual(published, 3)
        self.assertEqual(len(queue.published), 3)
        for stream, payload in queue.published:
            self.assertEqual(stream, STREAM_HYDRATE)
            self.assertEqual(payload["job_type"], JOB_HYDRATE_EVENT_ROOT)
            self.assertEqual(payload["scope"], UPCOMING_HYDRATE_SCOPE)  # "prematch"
            params = json.loads(payload["params_json"])
            self.assertEqual(params["hydration_mode"], "full")
        # entity ids preserved (encode_stream_job emits "entity_id")
        sent_ids = {int(p["entity_id"]) for _, p in queue.published}
        self.assertEqual(sent_ids, {15186710, 15186720, 15186836})

    async def test_per_tick_cap_is_honoured(self) -> None:
        queue = _FakeQueue()
        daemon = _daemon(
            ids=list(range(1, 101)), queue=queue, publish_per_tick_cap=10, batch_size=500
        )
        published = await daemon.tick()
        self.assertEqual(published, 10)
        self.assertEqual(len(queue.published), 10)

    async def test_lag_backpressure_skips_tick(self) -> None:
        queue = _FakeQueue(lag=9999)
        daemon = _daemon(ids=[1, 2, 3], queue=queue, lag_threshold=5000)
        published = await daemon.tick()
        self.assertEqual(published, 0)
        self.assertEqual(queue.published, [])

    async def test_lag_threshold_zero_disables_backpressure(self) -> None:
        queue = _FakeQueue(lag=10**9)
        daemon = _daemon(ids=[1], queue=queue, lag_threshold=0)
        published = await daemon.tick()
        self.assertEqual(published, 1)

    async def test_season_and_horizon_threaded_to_query(self) -> None:
        repo = _FakeRepo([1])
        daemon = UpcomingHydratePlannerDaemon(
            database=_FakeDatabase(),
            queue=_FakeQueue(),
            repository=repo,
            horizon_days=60,
            batch_size=500,
            season_id=58210,
        )
        await daemon.tick()
        self.assertEqual(repo.calls[-1]["season_id"], 58210)
        self.assertEqual(repo.calls[-1]["horizon_seconds"], 60 * 86400)
        self.assertEqual(repo.calls[-1]["limit"], 500)

    def test_horizon_seconds_property(self) -> None:
        daemon = _daemon(ids=[], horizon_days=2.5)
        self.assertEqual(daemon.horizon_seconds, int(2.5 * 86400))


if __name__ == "__main__":
    unittest.main()
