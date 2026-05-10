"""Tests for Variant A-lite: per-endpoint concurrent-lease cap.

The historical ProxyPool was strict single-flight per endpoint
(``_ProxyState.in_use: bool``). Variant A-lite replaces that with a
counter (``in_use_count``) capped at ``max_in_use``. Default
``max_in_use=1`` preserves the legacy semantics exactly so deploy
without env knobs is a behavioural no-op.

Behaviour covered:

* ``max_in_use=1`` (default no-op): one endpoint yields exactly one
  lease, second concurrent acquire blocks until first releases.
* ``max_in_use=4`` on one endpoint: four concurrent leases granted,
  fifth concurrent acquire blocks; releasing one frees one slot.
* 5 endpoints × ``max_in_use=4`` = 20 concurrent leases (the production
  target shape — 8 hydrate processes × 20 = 160 in-flight CONNECTs to
  Smartproxy gateway).
* Release decrements counter correctly, including double-release safety
  (clamped to 0).
* Failure release still applies the endpoint cooldown, blocking ALL
  ``max_in_use`` slots on this endpoint until cooldown expires (the
  documented known risk).
* Env resolution: scoped key wins over global, invalid values fall
  through, ``[1, 100]`` cap applied.
* CLI dispatch table contains the per-lane scoped env keys.
* ``load_runtime_config`` with no env knobs gives
  ``proxy_max_in_use_per_endpoint=1`` — critical safety net for the
  no-op deploy.
* Variant A and Variant B knobs are independent (multiplier inflates
  the slot count, max_in_use multiplies leases per slot — they
  multiply when both set).
"""
from __future__ import annotations

import asyncio
import os
import unittest
from contextlib import contextmanager


@contextmanager
def _patched_env(**kv):
    previous = {k: os.environ.get(k) for k in kv}
    try:
        for k, v in kv.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = str(v)
        yield
    finally:
        for k, v in previous.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class _MaxInUseEnvResolutionTests(unittest.TestCase):
    """Pure-helper tests for ``_resolve_proxy_max_in_use_per_endpoint``."""

    def test_default_no_env_returns_1(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_max_in_use_per_endpoint

        self.assertEqual(_resolve_proxy_max_in_use_per_endpoint({}), 1)

    def test_global_env_value(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_max_in_use_per_endpoint

        env = {"SCHEMA_INSPECTOR_PROXY_MAX_IN_USE_PER_ENDPOINT": "4"}
        self.assertEqual(_resolve_proxy_max_in_use_per_endpoint(env), 4)

    def test_scoped_env_takes_priority_over_global(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_max_in_use_per_endpoint

        env = {
            "SCHEMA_INSPECTOR_PROXY_MAX_IN_USE_PER_ENDPOINT": "2",
            "SCHEMA_INSPECTOR_HYDRATE_PROXY_MAX_IN_USE_PER_ENDPOINT": "8",
        }
        out = _resolve_proxy_max_in_use_per_endpoint(
            env,
            env_keys=("SCHEMA_INSPECTOR_HYDRATE_PROXY_MAX_IN_USE_PER_ENDPOINT",),
        )
        self.assertEqual(out, 8)

    def test_scoped_unset_falls_back_to_global(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_max_in_use_per_endpoint

        env = {"SCHEMA_INSPECTOR_PROXY_MAX_IN_USE_PER_ENDPOINT": "3"}
        out = _resolve_proxy_max_in_use_per_endpoint(
            env,
            env_keys=("SCHEMA_INSPECTOR_HYDRATE_PROXY_MAX_IN_USE_PER_ENDPOINT",),
        )
        self.assertEqual(out, 3)

    def test_invalid_value_falls_through(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_max_in_use_per_endpoint

        env = {
            "SCHEMA_INSPECTOR_HYDRATE_PROXY_MAX_IN_USE_PER_ENDPOINT": "not_a_number",
            "SCHEMA_INSPECTOR_PROXY_MAX_IN_USE_PER_ENDPOINT": "5",
        }
        out = _resolve_proxy_max_in_use_per_endpoint(
            env,
            env_keys=("SCHEMA_INSPECTOR_HYDRATE_PROXY_MAX_IN_USE_PER_ENDPOINT",),
        )
        self.assertEqual(out, 5)

    def test_value_below_1_returns_1(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_max_in_use_per_endpoint

        for bad in ("0", "-1", "-100"):
            with self.subTest(value=bad):
                env = {"SCHEMA_INSPECTOR_PROXY_MAX_IN_USE_PER_ENDPOINT": bad}
                self.assertEqual(_resolve_proxy_max_in_use_per_endpoint(env), 1)

    def test_value_capped_at_100(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_max_in_use_per_endpoint

        env = {"SCHEMA_INSPECTOR_PROXY_MAX_IN_USE_PER_ENDPOINT": "9999"}
        self.assertEqual(_resolve_proxy_max_in_use_per_endpoint(env), 100)


class _ProxyPoolDefaultSingleFlightTests(unittest.IsolatedAsyncioTestCase):
    """``max_in_use_per_endpoint=1`` (default) preserves the historical
    single-flight semantics: one endpoint = one concurrent lease."""

    async def test_default_one_endpoint_only_one_lease(self) -> None:
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import ProxyEndpoint

        endpoints = (
            ProxyEndpoint(name="proxy_1", url="http://user:pass@proxy.smartproxy.net:3120"),
        )
        pool = ProxyPool(endpoints, default_success_cooldown_seconds=0, jitter_seconds=0)

        first = await asyncio.wait_for(pool.acquire(), timeout=0.2)
        self.assertIsNotNone(first)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.05)
        await first.release(success=True)

    async def test_release_then_reacquire(self) -> None:
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import ProxyEndpoint

        endpoints = (
            ProxyEndpoint(name="proxy_1", url="http://user:pass@proxy.smartproxy.net:3120"),
        )
        pool = ProxyPool(endpoints, default_success_cooldown_seconds=0, jitter_seconds=0)

        first = await asyncio.wait_for(pool.acquire(), timeout=0.2)
        await first.release(success=True)
        # Release succeeded → next acquire grants a lease again.
        second = await asyncio.wait_for(pool.acquire(), timeout=0.2)
        self.assertIsNotNone(second)
        await second.release(success=True)


class _ProxyPoolMaxInUseConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    """``max_in_use_per_endpoint=N`` (Variant A-lite) grants N concurrent
    leases per single endpoint."""

    async def test_max_4_one_endpoint_four_concurrent_leases(self) -> None:
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import ProxyEndpoint

        endpoints = (
            ProxyEndpoint(name="proxy_1", url="http://user:pass@proxy.smartproxy.net:3120"),
        )
        pool = ProxyPool(
            endpoints,
            default_success_cooldown_seconds=0,
            jitter_seconds=0,
            max_in_use_per_endpoint=4,
        )

        leases = []
        for _ in range(4):
            lease = await asyncio.wait_for(pool.acquire(), timeout=0.2)
            self.assertIsNotNone(lease)
            leases.append(lease)
        # 5-я acquire blocks — все 4 слота заняты.
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.05)
        for lease in leases:
            await lease.release(success=True)

    async def test_release_one_frees_one_slot_at_max_4(self) -> None:
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import ProxyEndpoint

        endpoints = (
            ProxyEndpoint(name="proxy_1", url="http://user:pass@proxy.smartproxy.net:3120"),
        )
        pool = ProxyPool(
            endpoints,
            default_success_cooldown_seconds=0,
            jitter_seconds=0,
            max_in_use_per_endpoint=4,
        )

        leases = []
        for _ in range(4):
            leases.append(await asyncio.wait_for(pool.acquire(), timeout=0.2))
        # Saturated.
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.05)
        # Release ONE → exactly ONE more acquire goes through.
        await leases[0].release(success=True)
        replacement = await asyncio.wait_for(pool.acquire(), timeout=0.2)
        self.assertIsNotNone(replacement)
        # And the next one blocks again.
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.05)
        await replacement.release(success=True)
        for lease in leases[1:]:
            await lease.release(success=True)

    async def test_5_endpoints_max_4_yields_20_concurrent_leases(self) -> None:
        """Production target shape: 5 Smartproxy URLs × max_in_use=4 = 20
        concurrent CONNECT tunnels per worker process."""
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import ProxyEndpoint

        endpoints = tuple(
            ProxyEndpoint(name=f"proxy_{i + 1}", url=f"http://u{i}:p{i}@proxy.smartproxy.net:3120")
            for i in range(5)
        )
        pool = ProxyPool(
            endpoints,
            default_success_cooldown_seconds=0,
            jitter_seconds=0,
            max_in_use_per_endpoint=4,
        )

        leases = []
        for _ in range(20):
            lease = await asyncio.wait_for(pool.acquire(), timeout=0.5)
            self.assertIsNotNone(lease)
            leases.append(lease)
        # 21-й — все 20 слотов заняты.
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.05)
        for lease in leases:
            await lease.release(success=True)

    async def test_double_release_does_not_underflow_in_use_count(self) -> None:
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import ProxyEndpoint

        endpoints = (
            ProxyEndpoint(name="proxy_1", url="http://user:pass@proxy.smartproxy.net:3120"),
        )
        pool = ProxyPool(
            endpoints,
            default_success_cooldown_seconds=0,
            jitter_seconds=0,
            max_in_use_per_endpoint=2,
        )

        a = await asyncio.wait_for(pool.acquire(), timeout=0.2)
        b = await asyncio.wait_for(pool.acquire(), timeout=0.2)
        # Saturated.
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.05)
        # Release ``a`` twice — second is a no-op via ProxyLease._released
        # guard; even if a different code path called pool._release(state)
        # twice, in_use_count clamps at 0 (regression contract for
        # max(0, in_use_count-1)).
        await a.release(success=True)
        await a.release(success=True)
        # in_use_count must be 1, not 0 or negative. Verify by acquiring
        # exactly ONE more lease — second blocks.
        c = await asyncio.wait_for(pool.acquire(), timeout=0.2)
        self.assertIsNotNone(c)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.05)
        await b.release(success=True)
        await c.release(success=True)

    async def test_failure_release_blocks_all_4_slots_during_cooldown(self) -> None:
        """Documented known semantic: at max_in_use>1 a single failure
        cools down THE WHOLE endpoint for cooldown_seconds, blocking
        all max_in_use slots on it. This test pins the behaviour."""
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import ProxyEndpoint

        endpoints = (
            ProxyEndpoint(
                name="proxy_1",
                url="http://user:pass@proxy.smartproxy.net:3120",
                # Use a long cooldown so the test is deterministic.
                cooldown_seconds=60.0,
            ),
        )
        pool = ProxyPool(
            endpoints,
            default_success_cooldown_seconds=0,
            jitter_seconds=0,
            max_in_use_per_endpoint=4,
        )

        a = await asyncio.wait_for(pool.acquire(), timeout=0.2)
        b = await asyncio.wait_for(pool.acquire(), timeout=0.2)
        # ``a`` fails — endpoint cooldown is now active.
        await a.release(success=False)
        # Even though only 1 of 4 leases were actually used, ALL 4 slots
        # are now blocked until cooldown elapses.
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.05)
        # Release the second lease too — still no acquire, cooldown
        # remains active for 60s.
        await b.release(success=True)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.05)


class _LoadRuntimeConfigDefaultMaxInUseTests(unittest.TestCase):
    """Critical safety net: deploying this change with no env knobs set
    must produce ``proxy_max_in_use_per_endpoint=1`` so ProxyPool is
    single-flight (legacy behaviour) on prod by default."""

    def test_default_env_yields_max_in_use_1(self) -> None:
        from schema_inspector.runtime import load_runtime_config

        env = {
            "SCHEMA_INSPECTOR_PROXY_URLS": (
                "http://u1:p1@proxy.smartproxy.net:3120,"
                "http://u2:p2@proxy.smartproxy.net:3120"
            ),
            # No max_in_use env set anywhere.
        }
        cfg = load_runtime_config(env=env)
        self.assertEqual(cfg.proxy_max_in_use_per_endpoint, 1)

    def test_global_env_value_propagated_to_runtime_config(self) -> None:
        from schema_inspector.runtime import load_runtime_config

        env = {
            "SCHEMA_INSPECTOR_PROXY_URLS": "http://u:p@proxy.smartproxy.net:3120",
            "SCHEMA_INSPECTOR_PROXY_MAX_IN_USE_PER_ENDPOINT": "6",
        }
        cfg = load_runtime_config(env=env)
        self.assertEqual(cfg.proxy_max_in_use_per_endpoint, 6)

    def test_scoped_env_overrides_global(self) -> None:
        from schema_inspector.runtime import load_runtime_config

        env = {
            "SCHEMA_INSPECTOR_PROXY_URLS": "http://u:p@proxy.smartproxy.net:3120",
            "SCHEMA_INSPECTOR_PROXY_MAX_IN_USE_PER_ENDPOINT": "2",
            "SCHEMA_INSPECTOR_HYDRATE_PROXY_MAX_IN_USE_PER_ENDPOINT": "10",
        }
        cfg = load_runtime_config(
            env=env,
            proxy_max_in_use_per_endpoint_env_keys=(
                "SCHEMA_INSPECTOR_HYDRATE_PROXY_MAX_IN_USE_PER_ENDPOINT",
            ),
        )
        self.assertEqual(cfg.proxy_max_in_use_per_endpoint, 10)

    def test_no_env_set_for_any_lane_is_no_op(self) -> None:
        """For every per-lane scoped key, with no env set anywhere, the
        result must be max_in_use=1 (legacy single-flight)."""
        from schema_inspector.runtime import load_runtime_config

        env = {"SCHEMA_INSPECTOR_PROXY_URLS": "http://u:p@proxy.smartproxy.net:3120"}
        for scoped_key in (
            "SCHEMA_INSPECTOR_HYDRATE_PROXY_MAX_IN_USE_PER_ENDPOINT",
            "SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_MAX_IN_USE_PER_ENDPOINT",
            "SCHEMA_INSPECTOR_LIVE_TIER_2_PROXY_MAX_IN_USE_PER_ENDPOINT",
            "SCHEMA_INSPECTOR_LIVE_TIER_3_PROXY_MAX_IN_USE_PER_ENDPOINT",
            "SCHEMA_INSPECTOR_LIVE_WARM_PROXY_MAX_IN_USE_PER_ENDPOINT",
        ):
            with self.subTest(scoped_key=scoped_key):
                cfg = load_runtime_config(
                    env=env,
                    proxy_max_in_use_per_endpoint_env_keys=(scoped_key,),
                )
                self.assertEqual(cfg.proxy_max_in_use_per_endpoint, 1)


class _CliScopedMaxInUseKeyWiringTests(unittest.TestCase):
    """Lock down the CLI dispatch table mapping each per-lane CLI
    subcommand to its scoped max_in_use env key. Mirror of the
    Variant B session-multiplier dispatch test."""

    @staticmethod
    def _read_cli_dispatch_source() -> str:
        import inspect

        from schema_inspector.cli import _dispatch

        return inspect.getsource(_dispatch)

    def test_hydrate_max_in_use_key_present(self) -> None:
        src = self._read_cli_dispatch_source()
        self.assertIn("worker-hydrate", src)
        self.assertIn("SCHEMA_INSPECTOR_HYDRATE_PROXY_MAX_IN_USE_PER_ENDPOINT", src)

    def test_live_tier_1_max_in_use_key_present(self) -> None:
        src = self._read_cli_dispatch_source()
        self.assertIn("SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_MAX_IN_USE_PER_ENDPOINT", src)

    def test_live_tier_2_max_in_use_key_present(self) -> None:
        src = self._read_cli_dispatch_source()
        self.assertIn("SCHEMA_INSPECTOR_LIVE_TIER_2_PROXY_MAX_IN_USE_PER_ENDPOINT", src)

    def test_live_tier_3_max_in_use_key_present(self) -> None:
        src = self._read_cli_dispatch_source()
        self.assertIn("SCHEMA_INSPECTOR_LIVE_TIER_3_PROXY_MAX_IN_USE_PER_ENDPOINT", src)

    def test_live_warm_max_in_use_key_present(self) -> None:
        src = self._read_cli_dispatch_source()
        self.assertIn("SCHEMA_INSPECTOR_LIVE_WARM_PROXY_MAX_IN_USE_PER_ENDPOINT", src)

    def test_kwarg_passed_through_to_load_runtime_config(self) -> None:
        # Lock the kwarg name so a refactor doesn't silently drop the
        # max_in_use propagation while keeping the dict around.
        src = self._read_cli_dispatch_source()
        self.assertIn("proxy_max_in_use_per_endpoint_env_keys=", src)


class _VariantAAndBCoexistenceTests(unittest.TestCase):
    """Variant A (max_in_use_per_endpoint) and Variant B
    (session_multiplier) are independent levers — both can be set
    simultaneously without conflicting. With both set, the slot count
    multiplies (multiplier creates session-expanded virtual slots,
    max_in_use grants concurrent leases per slot)."""

    def test_both_levers_set_multiply(self) -> None:
        from schema_inspector.runtime import load_runtime_config

        env = {
            "SCHEMA_INSPECTOR_PROXY_URLS": "http://u:p@proxy.smartproxy.net:3120",
            # Variant B: 3 virtual slots per URL.
            "SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER": "3",
            # Variant A-lite: 4 concurrent leases per slot.
            "SCHEMA_INSPECTOR_PROXY_MAX_IN_USE_PER_ENDPOINT": "4",
        }
        cfg = load_runtime_config(env=env)
        # 1 URL × 3 multiplier = 3 session-expanded endpoints (slots).
        self.assertEqual(len(cfg.proxy_endpoints), 3)
        for ep in cfg.proxy_endpoints:
            self.assertTrue(ep.is_session_expanded)
        # max_in_use applied uniformly — 3 slots × 4 = 12 logical leases.
        self.assertEqual(cfg.proxy_max_in_use_per_endpoint, 4)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
