from __future__ import annotations

import json
import unittest

from schema_inspector.queue.streams import STREAM_HYDRATE, STREAM_LIVE_DISCOVERY, ConsumerGroupInfo


class LiveDiscoveryPlannerRunForeverGuardTests(unittest.IsolatedAsyncioTestCase):
    """Regression (2026-05-29 audit): run_forever must survive a tick that
    raises. This single-instance daemon feeds NEW live football events into
    zset:live:hot; an unhandled tick exception used to kill the process and
    silently stop live discovery until systemd restarted it."""

    async def test_run_forever_survives_raising_tick(self) -> None:
        import asyncio
        from schema_inspector.services.live_discovery_planner import (
            LiveDiscoveryPlannerDaemon,
        )

        daemon = LiveDiscoveryPlannerDaemon(queue=_FakeQueue(), targets=())
        calls = {"n": 0}

        async def _boom(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient redis/db blip")
            daemon.request_shutdown()  # 2nd iteration: clean exit
            return 0

        daemon.tick = _boom  # type: ignore[assignment]

        orig_sleep = asyncio.sleep

        async def _no_sleep(_seconds):
            return None

        asyncio.sleep = _no_sleep  # type: ignore[assignment]
        try:
            # Without the guard the RuntimeError propagates out and fails
            # the test; with it the loop logs, continues, then shuts down.
            await daemon.run_forever()
        finally:
            asyncio.sleep = orig_sleep  # type: ignore[assignment]

        self.assertEqual(calls["n"], 2)  # survived the raise, ran again


class LiveDiscoveryPlannerTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_discovery_planner_publishes_due_targets_with_live_scope(self) -> None:
        from schema_inspector.services.live_discovery_planner import (
            LiveDiscoveryPlannerDaemon,
            LiveDiscoveryPlanningTarget,
        )

        queue = _FakeQueue()
        daemon = LiveDiscoveryPlannerDaemon(
            queue=queue,
            targets=(
                LiveDiscoveryPlanningTarget(sport_slug="football", interval_ms=60_000, priority=10),
                LiveDiscoveryPlanningTarget(sport_slug="rugby", interval_ms=120_000, priority=20),
            ),
        )

        first = await daemon.tick(now_ms=1_800_000_000_000)
        second = await daemon.tick(now_ms=1_800_000_030_000)
        third = await daemon.tick(now_ms=1_800_000_061_000)

        self.assertEqual(first, 2)
        self.assertEqual(second, 0)
        self.assertEqual(third, 1)
        self.assertEqual([stream for stream, _ in queue.published], [STREAM_LIVE_DISCOVERY] * 3)
        self.assertEqual([payload["sport_slug"] for _, payload in queue.published], ["football", "rugby", "football"])
        self.assertEqual([payload["scope"] for _, payload in queue.published], ["live", "live", "live"])
        self.assertEqual(json.loads(queue.published[0][1]["params_json"]), {})

    async def test_live_discovery_planner_publishes_regular_targets_under_operational_backpressure(self) -> None:
        from schema_inspector.services.backpressure import BackpressureLimit, QueueBackpressure
        from schema_inspector.services.live_discovery_planner import (
            LiveDiscoveryPlannerDaemon,
            LiveDiscoveryPlanningTarget,
        )

        queue = _FakeQueue(
            group_infos={
                (STREAM_HYDRATE, "cg:hydrate"): ConsumerGroupInfo(
                    consumers=3,
                    pending=0,
                    last_delivered_id="1-0",
                    entries_read=100,
                    lag=300_000,
                ),
            }
        )
        daemon = LiveDiscoveryPlannerDaemon(
            queue=queue,
            targets=(LiveDiscoveryPlanningTarget(sport_slug="football", interval_ms=60_000, priority=10),),
            backpressure=QueueBackpressure(
                queue=queue,
                limits=(BackpressureLimit(stream=STREAM_HYDRATE, group="cg:hydrate", max_lag=100_000),),
            ),
        )

        published = await daemon.tick(now_ms=1_800_000_000_000)

        self.assertEqual(published, 1)
        self.assertEqual([stream for stream, _ in queue.published], [STREAM_LIVE_DISCOVERY])
        self.assertEqual([payload["sport_slug"] for _, payload in queue.published], ["football"])
        self.assertEqual([payload["scope"] for _, payload in queue.published], ["live"])

    async def test_live_discovery_planner_bypasses_backpressure_for_drifted_sport(self) -> None:
        from schema_inspector.services.backpressure import BackpressureLimit, QueueBackpressure
        from schema_inspector.services.live_discovery_planner import (
            LiveDiscoveryPlannerDaemon,
            LiveDiscoveryPlanningTarget,
        )

        queue = _FakeQueue(
            group_infos={
                (STREAM_HYDRATE, "cg:hydrate"): ConsumerGroupInfo(
                    consumers=3,
                    pending=0,
                    last_delivered_id="1-0",
                    entries_read=100,
                    lag=300_000,
                ),
            }
        )
        daemon = LiveDiscoveryPlannerDaemon(
            queue=queue,
            targets=(LiveDiscoveryPlanningTarget(sport_slug="football", interval_ms=60_000, priority=10),),
            backpressure=QueueBackpressure(
                queue=queue,
                limits=(BackpressureLimit(stream=STREAM_HYDRATE, group="cg:hydrate", max_lag=100_000),),
            ),
            drifted_sports_loader=_load_drifted_sports({"football": "snapshot_older_than_terminal_state"}),
        )

        published = await daemon.tick(now_ms=1_800_000_000_000)

        self.assertEqual(published, 1)
        self.assertEqual([stream for stream, _ in queue.published], [STREAM_LIVE_DISCOVERY])
        self.assertEqual([payload["sport_slug"] for _, payload in queue.published], ["football"])
        self.assertEqual([payload["scope"] for _, payload in queue.published], ["live"])

    async def test_live_discovery_planner_republishes_drifted_sport_after_repair_cooldown(self) -> None:
        from schema_inspector.services.backpressure import BackpressureLimit, QueueBackpressure
        from schema_inspector.services.live_discovery_planner import (
            LiveDiscoveryPlannerDaemon,
            LiveDiscoveryPlanningTarget,
        )

        queue = _FakeQueue(
            group_infos={
                (STREAM_HYDRATE, "cg:hydrate"): ConsumerGroupInfo(
                    consumers=3,
                    pending=0,
                    last_delivered_id="1-0",
                    entries_read=100,
                    lag=300_000,
                ),
            }
        )
        daemon = LiveDiscoveryPlannerDaemon(
            queue=queue,
            targets=(LiveDiscoveryPlanningTarget(sport_slug="football", interval_ms=60_000, priority=10),),
            backpressure=QueueBackpressure(
                queue=queue,
                limits=(BackpressureLimit(stream=STREAM_HYDRATE, group="cg:hydrate", max_lag=100_000),),
            ),
            drifted_sports_loader=_load_drifted_sports({"football": "snapshot_older_than_terminal_state"}),
            repair_cooldown_ms=30_000,
        )

        first = await daemon.tick(now_ms=1_800_000_000_000)
        second = await daemon.tick(now_ms=1_800_000_010_000)
        third = await daemon.tick(now_ms=1_800_000_031_000)

        self.assertEqual(first, 1)
        self.assertEqual(second, 0)
        self.assertEqual(third, 1)
        self.assertEqual([payload["sport_slug"] for _, payload in queue.published], ["football", "football"])


class _FakeQueue:
    def __init__(self, *, group_infos: dict[tuple[str, str], ConsumerGroupInfo] | None = None) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []
        self.group_infos = dict(group_infos or {})

    def publish(self, stream: str, values: dict[str, object]) -> str:
        self.published.append((stream, dict(values)))
        return f"{stream}:{len(self.published)}"

    def group_info(self, stream: str, group: str) -> ConsumerGroupInfo | None:
        return self.group_infos.get((stream, group))


def _load_drifted_sports(flags: dict[str, str]):
    async def _loader(*, now_ms: int) -> dict[str, str]:
        del now_ms
        return dict(flags)

    return _loader


if __name__ == "__main__":
    unittest.main()
