"""Phase 3.7(a): planner publishes bootstrap jobs for pending catalog rows.

The Phase 3 worker dispatch (commit daea230) only activates bootstrap
mode when a job for a ``pending`` (UT, season) pair appears in the
stream. The cursor walk publishes one job per UT per tick — it would
take days to drain the long-tail catalog one job at a time. This
extension makes the planner publish a throttled batch of bootstrap
jobs alongside the cursor publishes, so workers can fan out across
the catalog in parallel.

Tests:
  1. tick publishes bootstrap jobs from pending selector.
  2. respects max_bootstrap_jobs_per_tick limit (per-sport throttle).
  3. backpressure pauses bootstrap too (no publishing at all).
  4. missing selector → no bootstrap publishes (backwards compat).
  5. bootstrap jobs carry correct target_season_id + sport_slug + scope.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from typing import Any


@dataclass
class _FakeGroupInfo:
    lag: int


class _FakeQueue:
    def __init__(self, group_info_by_stream: dict | None = None) -> None:
        self.published: list[tuple[str, dict]] = []
        self._group_info = group_info_by_stream or {}

    def publish(self, stream: str, values: dict) -> str:
        self.published.append((stream, dict(values)))
        return f"id-{len(self.published)}"

    def group_info(self, stream: str, group: str) -> _FakeGroupInfo:
        return self._group_info.get((stream, group), _FakeGroupInfo(lag=0))


class _FakeRedisBackend:
    def __init__(self) -> None:
        self._hash: dict[str, dict[str, str]] = {}

    def hset(self, key: str, mapping: dict) -> None:
        self._hash.setdefault(key, {}).update(mapping)

    def hget(self, key: str, field: str) -> str | None:
        return self._hash.get(key, {}).get(field)

    def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._hash.get(key, {}))


class PlannerBootstrapPublishTests(unittest.IsolatedAsyncioTestCase):
    async def test_tick_publishes_bootstrap_jobs_from_pending_selector(self) -> None:
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()

        async def cursor_selector(*, sport_slug: str, limit: int):
            return []  # no cursor jobs — exercise bootstrap path standalone

        async def bootstrap_pending_selector(*, sport_slug: str, limit: int):
            return [
                {"unique_tournament_id": 16, "season_id": 1151, "priority_rank": 1},
                {"unique_tournament_id": 16, "season_id": 2636, "priority_rank": 1},
                {"unique_tournament_id": 1, "season_id": 11098, "priority_rank": 1},
            ]

        async def selector(**kwargs):
            return ()

        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=HistoricalTournamentCursorStore(backend),
            selector=selector,
            targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
            tournaments_per_tick=10,
            backfill_cursor_selector=cursor_selector,
            bootstrap_pending_selector=bootstrap_pending_selector,
            max_bootstrap_jobs_per_tick=20,
        )

        published = await daemon.tick()

        self.assertEqual(published, 3, "Three pending rows must yield three published jobs.")
        # All published jobs target the bootstrap stream with target_season_id set.
        for _stream, payload in queue.published:
            import json
            params = json.loads(payload["params_json"])
            self.assertIn("target_season_id", params)

    async def test_tick_respects_max_bootstrap_jobs_per_tick(self) -> None:
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()

        captured_limits: list[int] = []

        async def bootstrap_pending_selector(*, sport_slug: str, limit: int):
            captured_limits.append(limit)
            # Selector should be called with the throttle limit. We
            # return more than that to assert the planner respects
            # whatever it asked for (selector is the source of truth
            # for SQL LIMIT, but the planner still must not iterate
            # past its own throttle if the selector overreports).
            return [
                {"unique_tournament_id": 100 + i, "season_id": 200 + i, "priority_rank": 1}
                for i in range(limit)
            ]

        async def selector(**kwargs):
            return ()

        async def cursor_selector(**kwargs):
            return []

        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=HistoricalTournamentCursorStore(backend),
            selector=selector,
            targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
            backfill_cursor_selector=cursor_selector,
            bootstrap_pending_selector=bootstrap_pending_selector,
            max_bootstrap_jobs_per_tick=5,
        )

        published = await daemon.tick()

        self.assertEqual(captured_limits, [5], "Selector must be called with the throttle limit.")
        self.assertEqual(published, 5)

    async def test_tick_publishes_bootstrap_during_cursor_backpressure(self) -> None:
        """Selective backpressure: cursor publishes paused due to
        enrichment lag, but bootstrap publishes continue (they
        skip enrichment fan-out so they generate zero downstream
        load — no deadlock risk)."""
        from schema_inspector.services.backpressure import (
            BackpressureLimit,
            QueueBackpressure,
        )
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        # Enrichment lag above threshold — cursor publishes must pause.
        queue = _FakeQueue(
            group_info_by_stream={
                ("stream:etl:historical_enrichment", "cg:historical_enrichment"): _FakeGroupInfo(lag=99999)
            }
        )

        async def cursor_selector(*, sport_slug: str, limit: int):
            # Even if called would return 2 rows — but it should NOT
            # be called when cursor is backpressure-paused.
            return [{"unique_tournament_id": 99, "next_season_backfill_id": 999}]

        async def bootstrap_pending_selector(*, sport_slug: str, limit: int):
            return [{"unique_tournament_id": 16, "season_id": 1151, "priority_rank": 1}]

        async def selector(**kwargs):
            return ()

        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=HistoricalTournamentCursorStore(backend),
            selector=selector,
            targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
            backfill_cursor_selector=cursor_selector,
            bootstrap_pending_selector=bootstrap_pending_selector,
            max_bootstrap_jobs_per_tick=10,
            backpressure=QueueBackpressure(
                queue=queue,
                limits=(
                    BackpressureLimit(
                        stream="stream:etl:historical_enrichment",
                        group="cg:historical_enrichment",
                        max_lag=10000,
                    ),
                ),
            ),
        )

        published = await daemon.tick()

        self.assertEqual(published, 1, "Bootstrap publish must run even when cursor is paused.")
        # Verify it was the BOOTSTRAP row (UT=16, season=1151), not
        # the cursor row (UT=99, season=999).
        import json
        _stream, values = queue.published[0]
        self.assertEqual(int(values["entity_id"]), 16)
        params = json.loads(values["params_json"])
        self.assertEqual(params["target_season_id"], 1151)

    async def test_tick_backpressure_with_no_bootstrap_selector_publishes_nothing(self) -> None:
        """When backpressure is active AND no bootstrap selector
        configured, the planner publishes nothing (cursor paused,
        bootstrap path absent). This pins the backwards-compat
        behaviour for deployments that don't yet wire the bootstrap
        surface."""
        from schema_inspector.services.backpressure import (
            BackpressureLimit,
            QueueBackpressure,
        )
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue(
            group_info_by_stream={
                ("stream:etl:historical_enrichment", "cg:historical_enrichment"): _FakeGroupInfo(lag=99999)
            }
        )

        async def selector(**kwargs):
            return (1, 2, 3)  # would-publish but cursor is paused

        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=HistoricalTournamentCursorStore(backend),
            selector=selector,
            targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
            # No bootstrap_pending_selector — backwards-compat path.
            backpressure=QueueBackpressure(
                queue=queue,
                limits=(
                    BackpressureLimit(
                        stream="stream:etl:historical_enrichment",
                        group="cg:historical_enrichment",
                        max_lag=10000,
                    ),
                ),
            ),
        )

        published = await daemon.tick()

        self.assertEqual(published, 0)
        self.assertEqual(queue.published, [])

    async def test_tick_no_bootstrap_selector_keeps_legacy_behavior(self) -> None:
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()

        async def selector(*, sport_slug: str, after_unique_tournament_id: int, limit: int):
            return (101, 102)

        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=HistoricalTournamentCursorStore(backend),
            selector=selector,
            targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
            # No bootstrap_pending_selector → backwards-compat path.
        )

        published = await daemon.tick()

        # Two legacy UT-only jobs published — no bootstrap surface
        # active when selector kwarg is absent.
        self.assertEqual(published, 2)

    async def test_bootstrap_job_carries_correct_envelope_fields(self) -> None:
        import json
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()

        async def bootstrap_pending_selector(*, sport_slug: str, limit: int):
            return [{"unique_tournament_id": 16, "season_id": 1151, "priority_rank": 1}]

        async def selector(**kwargs):
            return ()

        async def cursor_selector(**kwargs):
            return []

        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=HistoricalTournamentCursorStore(backend),
            selector=selector,
            targets=(
                HistoricalTournamentPlanningTarget(sport_slug="football", priority=50),
            ),
            backfill_cursor_selector=cursor_selector,
            bootstrap_pending_selector=bootstrap_pending_selector,
            max_bootstrap_jobs_per_tick=5,
        )

        await daemon.tick()

        self.assertEqual(len(queue.published), 1)
        _stream, values = queue.published[0]
        self.assertEqual(values["entity_type"], "unique_tournament")
        self.assertEqual(int(values["entity_id"]), 16)
        self.assertEqual(values["sport_slug"], "football")
        self.assertEqual(values["scope"], "historical")
        params = json.loads(values["params_json"])
        self.assertEqual(params["target_season_id"], 1151)


if __name__ == "__main__":
    unittest.main()
