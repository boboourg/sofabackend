"""Verify that HistoricalTournamentPlannerDaemon respects the
``BackfillPriorityConfig`` when one is supplied.
"""
from __future__ import annotations
import unittest
from collections import Counter
from typing import Any

from schema_inspector.services.backfill_priority_config import (
    BackfillPriorityConfig,
)
from schema_inspector.services.historical_tournament_planner import (
    HistoricalTournamentCursorStore,
    HistoricalTournamentPlannerDaemon,
    HistoricalTournamentPlanningTarget,
)


class FakeQueue:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []

    def publish(self, stream: str, payload: bytes) -> None:
        self.published.append((stream, payload))


class FakeBackend:
    def __init__(self) -> None:
        self._hash: dict[str, str] = {}

    def hgetall(self, _: str) -> dict[str, str]:
        return dict(self._hash)

    def hset(self, _: str, *args, **kwargs) -> int:
        if "mapping" in kwargs:
            self._hash.update(kwargs["mapping"])
        elif args:
            mapping = args[0]
            if isinstance(mapping, dict):
                self._hash.update(mapping)
        return 1


def _make_planner(
    *,
    cursor_rows_per_sport: dict[str, list[dict[str, Any]]],
    priority_config: BackfillPriorityConfig | None = None,
    tournaments_per_tick: int = 10,
) -> tuple[HistoricalTournamentPlannerDaemon, FakeQueue]:
    queue = FakeQueue()
    cursor_store = HistoricalTournamentCursorStore(FakeBackend())

    async def selector(*, sport_slug: str, **_: Any) -> tuple[int, ...]:
        return ()

    async def backfill_cursor_selector(*, sport_slug: str, limit: int) -> list[dict]:
        return cursor_rows_per_sport.get(sport_slug, [])[:limit]

    targets = tuple(
        HistoricalTournamentPlanningTarget(sport_slug=s)
        for s in cursor_rows_per_sport
    )
    planner = HistoricalTournamentPlannerDaemon(
        queue=queue,
        cursor_store=cursor_store,
        selector=selector,
        targets=targets,
        tournaments_per_tick=tournaments_per_tick,
        backfill_cursor_selector=backfill_cursor_selector,
        priority_config=priority_config,
    )
    return planner, queue


class NoConfigRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_config_publishes_for_each_sport(self) -> None:
        """When no priority_config is supplied, the planner falls back
        to its pre-config behaviour: each target gets a full tick budget."""
        cursor_rows = {
            "football": [
                {"unique_tournament_id": i, "next_season_backfill_id": 100 + i}
                for i in range(1, 6)
            ],
            "tennis": [
                {"unique_tournament_id": 100 + i, "next_season_backfill_id": 200 + i}
                for i in range(1, 6)
            ],
        }
        planner, queue = _make_planner(
            cursor_rows_per_sport=cursor_rows,
            priority_config=None,
            tournaments_per_tick=5,
        )
        await planner.tick()
        self.assertEqual(len(queue.published), 10)  # 5 + 5


class WeightDistributionTests(unittest.IsolatedAsyncioTestCase):
    async def test_unequal_weights_split_budget_proportionally(self) -> None:
        """When football:tennis = 4:1, ~80 % of the budget goes to
        football. Per-target fetch uses an integer cap derived from the
        global tournaments_per_tick budget."""
        cursor_rows = {
            "football": [
                {"unique_tournament_id": i, "next_season_backfill_id": 100 + i}
                for i in range(1, 100)
            ],
            "tennis": [
                {"unique_tournament_id": 100 + i, "next_season_backfill_id": 200 + i}
                for i in range(1, 100)
            ],
        }
        cfg = BackfillPriorityConfig(
            sport_weights={"football": 4.0, "tennis": 1.0},
            ut_boost={},
            sport_concurrency_caps={},
        )
        planner, queue = _make_planner(
            cursor_rows_per_sport=cursor_rows,
            priority_config=cfg,
            tournaments_per_tick=10,
        )
        await planner.tick()

        published_sports = Counter()
        for _, encoded in queue.published:
            # encode_stream_job returns a dict (Redis stream fields).
            published_sports[encoded["sport_slug"]] += 1

        # Should be ~8 football, ~2 tennis (with rounding may be 8/2 or 9/2)
        self.assertGreaterEqual(published_sports["football"], 7)
        self.assertLessEqual(published_sports["tennis"], 3)

    async def test_zero_weight_sport_paused(self) -> None:
        cursor_rows = {
            "football": [
                {"unique_tournament_id": i, "next_season_backfill_id": 100 + i}
                for i in range(1, 30)
            ],
            "cricket": [
                {"unique_tournament_id": 200 + i, "next_season_backfill_id": 300 + i}
                for i in range(1, 30)
            ],
        }
        cfg = BackfillPriorityConfig(
            sport_weights={"football": 10.0, "cricket": 0.0},
            ut_boost={},
            sport_concurrency_caps={},
        )
        planner, queue = _make_planner(
            cursor_rows_per_sport=cursor_rows,
            priority_config=cfg,
            tournaments_per_tick=10,
        )
        await planner.tick()

        sports = Counter()
        for _, encoded in queue.published:
            sports[encoded["sport_slug"]] += 1

        self.assertEqual(sports["cricket"], 0)
        self.assertEqual(sports["football"], 10)

    async def test_ut_boost_sorts_within_sport(self) -> None:
        """ut_boost should make the boosted UT publish first within
        its sport bucket."""
        cursor_rows = {
            "football": [
                {"unique_tournament_id": 100, "next_season_backfill_id": 200},
                {"unique_tournament_id": 17, "next_season_backfill_id": 300},  # boosted
                {"unique_tournament_id": 200, "next_season_backfill_id": 400},
            ],
        }
        cfg = BackfillPriorityConfig(
            sport_weights={"football": 1.0},
            ut_boost={17: 100.0},  # massive boost
            sport_concurrency_caps={},
        )
        planner, queue = _make_planner(
            cursor_rows_per_sport=cursor_rows,
            priority_config=cfg,
            tournaments_per_tick=3,
        )
        await planner.tick()

        # First publish must be ut_id=17 (boosted)
        first_encoded = queue.published[0][1]
        self.assertEqual(int(first_encoded["entity_id"]), 17)


if __name__ == "__main__":
    unittest.main()
