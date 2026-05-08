"""F-7 Phase 1 tests: tier-aware dispatch lease selection.

These guard the contract that lease_ms_for_dispatch_tier returns a
distinct lease per tier so the staged rollout can dial each tier
independently from the .env file. The defaults are intentionally all
90_000ms so the code change alone is a behaviour no-op — defaults are
not asserted here directly because they are effectively ``int(env)``
values, but the behaviour for a given env override is.

Each test is responsible for restoring the module to the unmodified
state (defaults from current process environment) via tearDown so that
state leaked from importlib.reload does not pollute downstream tests.
"""
from __future__ import annotations

import importlib
import os
import unittest
from unittest import mock

from schema_inspector import live_dispatch_policy


class LeaseMsForDispatchTierTests(unittest.TestCase):
    def tearDown(self) -> None:
        # Always reload with the unmodified env so module-level constants
        # match the canonical defaults again — protects every other test
        # in the run from inheriting overrides we set here.
        for var in (
            "LIVE_DISPATCH_LEASE_TIER_1_MS",
            "LIVE_DISPATCH_LEASE_TIER_2_MS",
            "LIVE_DISPATCH_LEASE_TIER_3_MS",
        ):
            os.environ.pop(var, None)
        importlib.reload(live_dispatch_policy)

    def test_default_lease_matches_legacy_90s_for_each_tier(self) -> None:
        # Re-import the module with no env overrides so we read the
        # canonical defaults — confirming Phase 1 deploy is a no-op.
        for var in (
            "LIVE_DISPATCH_LEASE_TIER_1_MS",
            "LIVE_DISPATCH_LEASE_TIER_2_MS",
            "LIVE_DISPATCH_LEASE_TIER_3_MS",
        ):
            os.environ.pop(var, None)
        module = importlib.reload(live_dispatch_policy)
        self.assertEqual(module.LIVE_DISPATCH_LEASE_TIER_1_MS, 90_000)
        self.assertEqual(module.LIVE_DISPATCH_LEASE_TIER_2_MS, 90_000)
        self.assertEqual(module.LIVE_DISPATCH_LEASE_TIER_3_MS, 90_000)
        self.assertEqual(
            module.lease_ms_for_dispatch_tier("tier_1", default_ms=12345),
            90_000,
        )
        self.assertEqual(
            module.lease_ms_for_dispatch_tier("tier_2", default_ms=12345),
            90_000,
        )
        self.assertEqual(
            module.lease_ms_for_dispatch_tier("tier_3", default_ms=12345),
            90_000,
        )

    def test_legacy_lane_strings_normalise_through_tier_helpers(self) -> None:
        # normalize_live_dispatch_tier maps "hot" → tier_1, "" / "warm" →
        # tier_2, anything else (including None) → tier_3 fallback. Mirrors
        # how poll_seconds_for_live_dispatch_tier works. ``default_ms`` is
        # therefore defensive-only; in practice every input resolves to
        # one of the three tier leases.
        for var in (
            "LIVE_DISPATCH_LEASE_TIER_1_MS",
            "LIVE_DISPATCH_LEASE_TIER_2_MS",
            "LIVE_DISPATCH_LEASE_TIER_3_MS",
        ):
            os.environ.pop(var, None)
        module = importlib.reload(live_dispatch_policy)
        self.assertEqual(module.lease_ms_for_dispatch_tier("hot", default_ms=42), 90_000)
        self.assertEqual(module.lease_ms_for_dispatch_tier("warm", default_ms=42), 90_000)
        self.assertEqual(module.lease_ms_for_dispatch_tier(None, default_ms=42), 90_000)
        self.assertEqual(module.lease_ms_for_dispatch_tier("xxxxx", default_ms=42), 90_000)

    def test_per_tier_env_overrides_picked_up_on_module_reload(self) -> None:
        # Real prod path: ops edits .env and restarts the planner, which
        # re-imports the module → the new env value takes effect for
        # subsequent claim_dispatch calls.
        env_overrides = {
            "LIVE_DISPATCH_LEASE_TIER_1_MS": "5000",
            "LIVE_DISPATCH_LEASE_TIER_2_MS": "15000",
            "LIVE_DISPATCH_LEASE_TIER_3_MS": "60000",
        }
        with mock.patch.dict(os.environ, env_overrides, clear=False):
            module = importlib.reload(live_dispatch_policy)
            self.assertEqual(module.LIVE_DISPATCH_LEASE_TIER_1_MS, 5000)
            self.assertEqual(module.LIVE_DISPATCH_LEASE_TIER_2_MS, 15000)
            self.assertEqual(module.LIVE_DISPATCH_LEASE_TIER_3_MS, 60000)
            self.assertEqual(
                module.lease_ms_for_dispatch_tier("tier_1", default_ms=99_999),
                5000,
            )
            self.assertEqual(
                module.lease_ms_for_dispatch_tier("tier_2", default_ms=99_999),
                15000,
            )
            self.assertEqual(
                module.lease_ms_for_dispatch_tier("tier_3", default_ms=99_999),
                60000,
            )

    def test_invalid_env_value_falls_back_to_default(self) -> None:
        # _env_int swallows ValueError → keeps default.
        with mock.patch.dict(
            os.environ,
            {"LIVE_DISPATCH_LEASE_TIER_1_MS": "not-a-number"},
            clear=False,
        ):
            module = importlib.reload(live_dispatch_policy)
            self.assertEqual(module.LIVE_DISPATCH_LEASE_TIER_1_MS, 90_000)


if __name__ == "__main__":
    unittest.main()
