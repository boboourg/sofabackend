from __future__ import annotations

import json
import unittest

from schema_inspector.queue.streams import STREAM_LIVE_DISCOVERY


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


class _FakeQueue:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []

    def publish(self, stream: str, values: dict[str, object]) -> str:
        self.published.append((stream, dict(values)))
        return f"{stream}:{len(self.published)}"


if __name__ == "__main__":
    unittest.main()
