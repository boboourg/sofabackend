"""Regression: replay must respect prefetch's freshness-skip decisions.

Without the fix:
  * prefetch sees freshness key fresh in Redis → skips the fetch → no record.
  * TTL expires before replay phase.
  * replay's PilotOrchestrator queries the same Redis-backed FreshnessStore,
    sees not-fresh → tries to fetch via ReplayFetchExecutor → raises
    ``RuntimeError("No prefetched fetch outcome available")``.

With the fix:
  * PilotOrchestrator captures every freshness_key it skipped during prefetch
    in ``_freshness_skip_keys``.
  * ``PrefetchedRun`` exposes them via ``freshness_skip_keys``.
  * ``_persist_prefetched_run`` builds a ``_ReplayFreshnessStore`` from that
    snapshot and passes it to the replay orchestrator instead of the live
    Redis-backed store. Replay therefore deterministically replays
    prefetch's skip decisions and never races on TTL expiry.
"""

from __future__ import annotations

import unittest
from unittest import mock


class _FakeTransaction:
    def __init__(self, connection) -> None:
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *args) -> None:
        del args


class _FakeDatabase:
    def __init__(self) -> None:
        self.connection_obj = object()

    def transaction(self):
        return _FakeTransaction(self.connection_obj)


class _FakeRedisBackend:
    def ping(self) -> bool:
        return True

    def close(self) -> None:
        pass


class ReplayFreshnessSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_replay_freshness_store_is_fresh_only_for_snapshotted_keys(self) -> None:
        from schema_inspector.cli import _ReplayFreshnessStore

        store = _ReplayFreshnessStore(frozenset({"freshness:a", "freshness:b"}))

        self.assertTrue(store.is_fresh("freshness:a"))
        self.assertTrue(store.is_fresh("freshness:b"))
        self.assertFalse(store.is_fresh("freshness:c"))
        self.assertFalse(store.is_fresh(""))

    async def test_persist_prefetched_run_constructs_replay_freshness_store_from_snapshot(self) -> None:
        import schema_inspector.cli as hybrid_cli
        from schema_inspector.runtime import RuntimeConfig

        app = hybrid_cli.HybridApp(
            database=_FakeDatabase(),
            runtime_config=RuntimeConfig(require_proxy=False),
            redis_backend=_FakeRedisBackend(),
        )
        skip_keys = frozenset(
            {
                "freshness:event-player:99:42:/api/v1/event/{event_id}/player/{player_id}/heatmap",
                "freshness:player:42",
            }
        )
        prefetched = hybrid_cli.PrefetchedRun(
            event_id=99,
            sport_slug="football",
            fetch_records=(),
            snapshot_store=object(),
            initial_capability_rollup={},
            freshness_skip_keys=skip_keys,
        )
        captured_kwargs: list[dict[str, object]] = []

        class _CapturingPilotOrchestrator:
            def __init__(self, **kwargs) -> None:
                captured_kwargs.append(kwargs)

            async def run_event(self, **kwargs):
                import types

                return types.SimpleNamespace(**kwargs)

        with mock.patch.object(hybrid_cli, "PilotOrchestrator", _CapturingPilotOrchestrator):
            await app._persist_prefetched_run(prefetched, hydration_mode="full")

        replay_store = captured_kwargs[0]["freshness_store"]
        self.assertIsInstance(replay_store, hybrid_cli._ReplayFreshnessStore)
        self.assertIsNot(replay_store, app.freshness_store)
        for key in skip_keys:
            self.assertTrue(replay_store.is_fresh(key))
        self.assertFalse(replay_store.is_fresh("freshness:event-player:99:42:.../statistics"))


class PilotOrchestratorFreshnessSkipCaptureTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_gated_event_endpoint_records_freshness_skip_for_player_detail(self) -> None:
        """When freshness check skips a player-detail fetch, the orchestrator records the key."""

        from schema_inspector.endpoints import EVENT_PLAYER_HEATMAP_ENDPOINT
        from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator

        class _AlwaysFreshStore:
            def is_fresh(self, key: str) -> bool:
                del key
                return True

        orchestrator = PilotOrchestrator(
            fetch_executor=mock.Mock(),
            snapshot_store=mock.Mock(),
            normalize_worker=mock.Mock(),
            planner=mock.Mock(),
            capability_repository=None,
            sql_executor=None,
            freshness_store=_AlwaysFreshStore(),
        )
        outcome, parsed = await orchestrator._fetch_gated_event_endpoint(
            endpoint=EVENT_PLAYER_HEATMAP_ENDPOINT,
            sport_slug="football",
            path_params={"event_id": 100, "player_id": 200},
            context_entity_type="player",
            context_entity_id=200,
            context_event_id=100,
            fetch_reason="hydrate_special_route",
            status_phase="terminal",
        )

        self.assertIsNone(outcome)
        self.assertIsNone(parsed)
        self.assertIn(
            f"freshness:event-player:100:200:{EVENT_PLAYER_HEATMAP_ENDPOINT.pattern}",
            orchestrator.freshness_skip_keys,
        )


class FreshnessSkipKeysSurfaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_prefetched_run_carries_freshness_skip_keys_field(self) -> None:
        from schema_inspector.cli import PrefetchedRun

        skip_keys = frozenset({"freshness:x", "freshness:y"})
        prefetched = PrefetchedRun(
            event_id=1,
            sport_slug="football",
            fetch_records=(),
            snapshot_store=object(),
            initial_capability_rollup={},
            freshness_skip_keys=skip_keys,
        )
        self.assertEqual(prefetched.freshness_skip_keys, skip_keys)


if __name__ == "__main__":
    unittest.main()
