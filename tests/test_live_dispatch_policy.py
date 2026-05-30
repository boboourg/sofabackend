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


class FetchTimeoutForDispatchTierTests(unittest.TestCase):
    """Phase1-A2 (2026-05-29): per-tier HTTP fetch timeout. Top live matches
    (tier_1, detailId=1) lost their match-center on 2026-05-29 (event
    15728277) because a proxy-latency burst blew the 10s global timeout on
    the root /event fetch; per-tier timeouts give tier_1 headroom while
    letting tier_3 fail fast."""

    def tearDown(self) -> None:
        for var in (
            "SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_1",
            "SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_2",
            "SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_3",
        ):
            os.environ.pop(var, None)
        importlib.reload(live_dispatch_policy)

    def test_defaults_give_tier_1_more_headroom_than_tier_3(self) -> None:
        for var in (
            "SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_1",
            "SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_2",
            "SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_3",
        ):
            os.environ.pop(var, None)
        module = importlib.reload(live_dispatch_policy)
        self.assertEqual(module.fetch_timeout_for_dispatch_tier("tier_1"), 25.0)
        self.assertEqual(module.fetch_timeout_for_dispatch_tier("tier_2"), 20.0)
        self.assertEqual(module.fetch_timeout_for_dispatch_tier("tier_3"), 12.0)
        # tier_1 must clear the proxy-latency bursts that broke the 10s/20s
        # global timeout; tier_3 must fail fast so it cannot pin a proxy.
        self.assertGreater(
            module.fetch_timeout_for_dispatch_tier("tier_1"),
            module.fetch_timeout_for_dispatch_tier("tier_3"),
        )

    def test_lane_strings_map_through_normalize(self) -> None:
        # The live worker passes self.lane (hot/warm/tier_x). normalize maps
        # hot→tier_1, warm→tier_2, cold/unknown→tier_3.
        module = importlib.reload(live_dispatch_policy)
        self.assertEqual(module.fetch_timeout_for_dispatch_tier("hot"), 25.0)
        self.assertEqual(module.fetch_timeout_for_dispatch_tier("warm"), 20.0)
        self.assertEqual(module.fetch_timeout_for_dispatch_tier("cold"), 12.0)

    def test_unrecognised_tier_returns_default_for_non_tier_callers(self) -> None:
        # hydrate / CLI one-shot pass no tier → must fall back to the global
        # default (None here) so they keep SOFASCORE_FETCH_TIMEOUT_SECONDS.
        module = importlib.reload(live_dispatch_policy)
        # normalize_live_dispatch_tier maps everything non-empty to a tier,
        # so the "no override" contract is exercised via default_seconds:
        self.assertEqual(
            module.fetch_timeout_for_dispatch_tier(None, default_seconds=None), None
        )

    def test_env_override_applies(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_1": "30"},
            clear=False,
        ):
            module = importlib.reload(live_dispatch_policy)
            self.assertEqual(module.fetch_timeout_for_dispatch_tier("tier_1"), 30.0)

    def test_invalid_env_falls_back_to_default(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_3": "not-a-number"},
            clear=False,
        ):
            module = importlib.reload(live_dispatch_policy)
            self.assertEqual(module.LIVE_TIER_3_FETCH_TIMEOUT_SECONDS, 12.0)


class FootballTier5FallThroughTests(unittest.TestCase):
    """Phase 0: football tier_5 (detailId ABSENT, ~93% of football) must
    fall through to the user_count / tournament_tier ladder instead of being
    hard-returned as LIVE_TIER_3. The genuine tier_3 cohort (detailId in
    {2,3,5}) and explicit tier_1/tier_2 detailIds stay exactly as before.

    These read the thresholds from module constants so the assertions stay
    correct even if LIVE_TIER_1_MIN_USER_COUNT / LIVE_TIER_2_MIN_USER_COUNT
    are overridden via env on prod.
    """

    def setUp(self) -> None:
        self.policy = live_dispatch_policy
        self.tier1_min = self.policy.LIVE_TIER_1_MIN_USER_COUNT
        self.tier2_min = self.policy.LIVE_TIER_2_MIN_USER_COUNT

    def _resolve(self, **kwargs):
        base = dict(
            sport_slug="football",
            detail_id=None,
            tournament_tier=None,
            tournament_user_count=None,
        )
        base.update(kwargs)
        return self.policy.resolve_live_dispatch_tier(**base)

    # --- the fix: tier_5 now reaches the user_count ladder ---------------

    def test_tier5_high_user_count_promotes_to_tier_1(self) -> None:
        # EPL/UCL: no detailId (tier_5) but ~1.2M users. Previously DEAD
        # CODE -> hard tier_3 (lag 6.1h). Must now reach tier_1.
        tier = self._resolve(tournament_user_count=1_200_000)
        self.assertEqual(tier, self.policy.LIVE_TIER_1)
        # And the documented boundary value also promotes.
        self.assertEqual(
            self._resolve(tournament_user_count=self.tier1_min),
            self.policy.LIVE_TIER_1,
        )

    def test_tier5_mid_user_count_promotes_to_tier_2(self) -> None:
        mid = self.tier2_min  # >= tier2 min, < tier1 min
        self.assertLess(mid, self.tier1_min)
        self.assertEqual(
            self._resolve(tournament_user_count=mid),
            self.policy.LIVE_TIER_2,
        )

    def test_tier5_low_user_count_stays_tier_3(self) -> None:
        self.assertEqual(
            self._resolve(tournament_user_count=50),
            self.policy.LIVE_TIER_3,
        )

    def test_tier5_no_user_count_uses_tournament_tier_ladder(self) -> None:
        # user_count None -> fall through to tournament_tier ladder.
        self.assertEqual(
            self._resolve(tournament_user_count=None, tournament_tier=1),
            self.policy.LIVE_TIER_1,
        )
        self.assertEqual(
            self._resolve(tournament_user_count=None, tournament_tier=3),
            self.policy.LIVE_TIER_2,
        )

    def test_tier5_all_unknown_preserves_tier_3_fallback(self) -> None:
        # live_rescue passes user_count=None, tournament_tier=None. Must keep
        # landing in tier_3 via the final fallback (no behaviour change).
        self.assertEqual(self._resolve(), self.policy.LIVE_TIER_3)

    # --- regression guards: cohorts that MUST NOT change -----------------

    def test_genuine_tier_3_is_not_promoted_by_user_count(self) -> None:
        # detailId in {2,3,5} == genuine tier_3. Even with a huge user_count
        # it must stay tier_3 (it must NOT fall through to the ladder).
        for detail_id in (2, 3, 5):
            self.assertEqual(
                self._resolve(detail_id=detail_id, tournament_user_count=10_000_000),
                self.policy.LIVE_TIER_3,
                msg=f"detailId={detail_id} (genuine tier_3) was promoted",
            )

    def test_explicit_tier_1_and_tier_2_detail_ids_unchanged(self) -> None:
        # detailId 1 -> tier_1, detailId in {4,6} -> tier_2, regardless of
        # user_count (explicit-detailId returns happen before the ladder).
        self.assertEqual(
            self._resolve(detail_id=1, tournament_user_count=0),
            self.policy.LIVE_TIER_1,
        )
        for detail_id in (4, 6):
            self.assertEqual(
                self._resolve(detail_id=detail_id, tournament_user_count=0),
                self.policy.LIVE_TIER_2,
            )

    def test_non_football_unaffected(self) -> None:
        # Non-football skips the football branch entirely and uses the
        # generic ladder both before and after this change.
        self.assertEqual(
            self.policy.resolve_live_dispatch_tier(
                sport_slug="tennis",
                detail_id=None,
                tournament_tier=None,
                tournament_user_count=1_200_000,
            ),
            self.policy.LIVE_TIER_1,
        )


if __name__ == "__main__":
    unittest.main()
