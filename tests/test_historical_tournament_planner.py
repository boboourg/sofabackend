from __future__ import annotations

import json
import unittest


class HistoricalTournamentPlannerTests(unittest.IsolatedAsyncioTestCase):
    async def test_tournament_planner_publishes_progressively_and_persists_cursor(self) -> None:
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()

        async def selector(*, sport_slug: str, after_unique_tournament_id: int, limit: int) -> tuple[int, ...]:
            self.assertEqual(sport_slug, "football")
            self.assertEqual(limit, 2)
            if after_unique_tournament_id <= 0:
                return (11, 17)
            if after_unique_tournament_id == 17:
                return (23,)
            return ()

        cursor_store = HistoricalTournamentCursorStore(backend)
        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            selector=selector,
            targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
            tournaments_per_tick=2,
        )

        first = await daemon.tick()
        second = await daemon.tick()
        third = await daemon.tick()

        self.assertEqual(first, 2)
        self.assertEqual(second, 1)
        self.assertEqual(third, 0)
        self.assertEqual(
            [stream for stream, _ in queue.published],
            ["stream:etl:historical_tournament"] * 3,
        )
        self.assertEqual(
            [int(payload["entity_id"]) for _, payload in queue.published],
            [11, 17, 23],
        )
        self.assertEqual(cursor_store.load_last_unique_tournament_id("football"), 23)
        self.assertEqual(json.loads(str(queue.published[0][1]["params_json"])), {})

    async def test_tournament_planner_pauses_when_backpressure_is_above_threshold(self) -> None:
        from schema_inspector.services.backpressure import BackpressureLimit, QueueBackpressure
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue(group_info_by_stream={("stream:etl:historical_enrichment", "cg:historical_enrichment"): _FakeGroupInfo(lag=200)})

        async def selector(*, sport_slug: str, after_unique_tournament_id: int, limit: int) -> tuple[int, ...]:
            del sport_slug, after_unique_tournament_id, limit
            return (11, 17)

        cursor_store = HistoricalTournamentCursorStore(backend)
        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            selector=selector,
            targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
            tournaments_per_tick=2,
            backpressure=QueueBackpressure(
                queue=queue,
                limits=(BackpressureLimit(stream="stream:etl:historical_enrichment", group="cg:historical_enrichment", max_lag=100),),
            ),
        )

        published = await daemon.tick()

        self.assertEqual(published, 0)
        self.assertEqual(queue.published, [])
        self.assertEqual(cursor_store.load_last_unique_tournament_id("football"), 0)


class _FakeQueue:
    def __init__(self, *, group_info_by_stream: dict[tuple[str, str], object] | None = None) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []
        self.group_info_by_stream = dict(group_info_by_stream or {})

    def publish(self, stream: str, values: dict[str, object]) -> str:
        self.published.append((stream, dict(values)))
        return f"{stream}:{len(self.published)}"

    def group_info(self, stream: str, group: str):
        return self.group_info_by_stream.get((stream, group))


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(key, {}).update(dict(mapping))
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))


class _FakeGroupInfo:
    def __init__(self, *, lag: int | None) -> None:
        self.lag = lag


if __name__ == "__main__":
    unittest.main()
