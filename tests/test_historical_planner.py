from __future__ import annotations

import json
import unittest


class HistoricalPlannerTests(unittest.IsolatedAsyncioTestCase):
    async def test_historical_planner_publishes_dates_progressively_and_persists_cursor(self) -> None:
        from schema_inspector.services.historical_planner import (
            HistoricalCursorStore,
            HistoricalPlannerDaemon,
            HistoricalPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()
        cursor_store = HistoricalCursorStore(backend)
        daemon = HistoricalPlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            targets=(
                HistoricalPlanningTarget(
                    sport_slug="football",
                    date_from="2025-01-01",
                    date_to="2025-01-02",
                ),
            ),
            dates_per_tick=1,
        )

        first_published = await daemon.tick()
        second_published = await daemon.tick()
        third_published = await daemon.tick()

        self.assertEqual(first_published, 1)
        self.assertEqual(second_published, 1)
        self.assertEqual(third_published, 0)
        self.assertEqual([stream for stream, _ in queue.published], ["stream:etl:historical_discovery"] * 2)
        self.assertEqual(
            [json.loads(payload["params_json"])["date"] for _, payload in queue.published],
            ["2025-01-01", "2025-01-02"],
        )
        self.assertEqual(cursor_store.load_next_date("football", "2025-01-01", "2025-01-02"), "2025-01-03")


class _FakeQueue:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []

    def publish(self, stream: str, values: dict[str, object]) -> str:
        self.published.append((stream, dict(values)))
        return f"{stream}:{len(self.published)}"


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(key, {}).update(dict(mapping))
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))


if __name__ == "__main__":
    unittest.main()
