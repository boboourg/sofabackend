"""Tests for the A3 Phase 0 LiveTierOverrideRegistry."""

from __future__ import annotations

import unittest

from schema_inspector.live_dispatch_policy import resolve_live_dispatch_tier
from schema_inspector.live_tier_override import (
    NO_OP_REGISTRY,
    LiveTierOverrideRegistry,
)


class _StubExecutor:
    def __init__(self, rows: list[dict[str, object]] | None = None) -> None:
        self.rows = list(rows or [])
        self.fetch_calls = 0
        self.raises = None

    async def fetch(self, query: str):
        del query
        self.fetch_calls += 1
        if self.raises is not None:
            raise self.raises
        return list(self.rows)


class LiveTierOverrideRegistryTests(unittest.IsolatedAsyncioTestCase):
    async def test_load_populates_dict_from_rows(self) -> None:
        executor = _StubExecutor(
            rows=[
                {"unique_tournament_id": 17, "override_tier": "tier_1"},
                {"unique_tournament_id": 8, "override_tier": "tier_1"},
                {"unique_tournament_id": 215, "override_tier": "tier_2"},
            ]
        )
        registry = LiveTierOverrideRegistry(sql_executor=executor)
        loaded = await registry.load()
        self.assertEqual(loaded, 3)
        self.assertEqual(registry.get(17), "tier_1")
        self.assertEqual(registry.get(8), "tier_1")
        self.assertEqual(registry.get(215), "tier_2")
        self.assertIsNone(registry.get(99999))

    async def test_load_skips_invalid_tier_values(self) -> None:
        executor = _StubExecutor(
            rows=[
                {"unique_tournament_id": 1, "override_tier": "tier_1"},
                {"unique_tournament_id": 2, "override_tier": "garbage"},
                {"unique_tournament_id": 3, "override_tier": "tier_5"},  # not allowed
            ]
        )
        registry = LiveTierOverrideRegistry(sql_executor=executor)
        loaded = await registry.load()
        self.assertEqual(loaded, 1)
        self.assertEqual(registry.get(1), "tier_1")
        self.assertIsNone(registry.get(2))
        self.assertIsNone(registry.get(3))

    async def test_load_failure_keeps_previous_snapshot(self) -> None:
        executor = _StubExecutor(rows=[{"unique_tournament_id": 1, "override_tier": "tier_1"}])
        registry = LiveTierOverrideRegistry(sql_executor=executor)
        await registry.load()
        self.assertEqual(registry.get(1), "tier_1")
        # Simulate a transient DB error on next reload.
        executor.raises = RuntimeError("connection refused")
        loaded = await registry.refresh()
        # Snapshot preserved despite failure.
        self.assertEqual(loaded, 1)
        self.assertEqual(registry.get(1), "tier_1")

    def test_get_handles_none_and_invalid_input(self) -> None:
        registry = LiveTierOverrideRegistry(sql_executor=_StubExecutor())
        self.assertIsNone(registry.get(None))
        self.assertIsNone(registry.get("not-an-int"))  # type: ignore[arg-type]

    def test_snapshot_returns_copy(self) -> None:
        registry = LiveTierOverrideRegistry(sql_executor=_StubExecutor())
        registry._overrides = {1: "tier_1"}  # internal seed
        snap = registry.snapshot()
        self.assertEqual(snap, {1: "tier_1"})
        # mutating the snapshot must not affect the registry
        snap[2] = "tier_2"  # type: ignore[index]
        self.assertNotIn(2, registry.snapshot())


class ResolveLiveDispatchTierWithOverrideTests(unittest.TestCase):
    def test_override_short_circuits_heuristic(self) -> None:
        # Without override: Premier League (tier=NULL, but user_count=1.2M)
        # would land in tier_1 via user_count heuristic. With override the
        # registry value wins even when it differs.
        registry = LiveTierOverrideRegistry(sql_executor=_StubExecutor())
        registry._overrides = {17: "tier_3"}  # operator force-demote PL

        tier = resolve_live_dispatch_tier(
            sport_slug="football",
            detail_id=None,
            tournament_tier=None,
            tournament_user_count=1_200_000,
            unique_tournament_id=17,
            tier_override_registry=registry,
        )
        self.assertEqual(tier, "tier_3")

    def test_no_override_falls_through_to_heuristic(self) -> None:
        registry = LiveTierOverrideRegistry(sql_executor=_StubExecutor())
        registry._overrides = {17: "tier_3"}  # only PL has override

        # Different UT id — registry returns None — heuristic runs.
        tier = resolve_live_dispatch_tier(
            sport_slug="football",
            detail_id=1,  # tier_1 in football_detail_tier
            tournament_tier=None,
            tournament_user_count=None,
            unique_tournament_id=99999,
            tier_override_registry=registry,
        )
        self.assertEqual(tier, "tier_1")

    def test_no_registry_keeps_legacy_signature(self) -> None:
        # Backward compat: callers that have not been wired through the
        # override path yet pass tier_override_registry=None or omit it
        # entirely. Heuristic still works.
        tier = resolve_live_dispatch_tier(
            sport_slug="football",
            detail_id=1,
            tournament_tier=None,
            tournament_user_count=None,
        )
        self.assertEqual(tier, "tier_1")

    def test_no_op_registry_is_safe(self) -> None:
        tier = resolve_live_dispatch_tier(
            sport_slug="football",
            detail_id=1,
            tournament_tier=None,
            tournament_user_count=None,
            unique_tournament_id=17,
            tier_override_registry=NO_OP_REGISTRY,
        )
        self.assertEqual(tier, "tier_1")


if __name__ == "__main__":
    unittest.main()
