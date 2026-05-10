"""Tests for Smartproxy session-id expansion in ProxyEndpoint / ProxyPool.

Behaviour covered:

* ``multiplier=1`` (the default no-op) preserves the historical
  pre-expansion behaviour exactly: one ``ProxyEndpoint`` per URL,
  ``is_session_expanded=False``, no session-id suffix added.
* ``multiplier=N`` (N>1) with credentialed Smartproxy URLs expands
  each URL into N virtual slots whose usernames carry a unique
  ``-session-<id>`` suffix. Slot names are unique
  (``proxy_<i>_s<NN>``). Hostport stays identical so health gating
  still works on a single Smartproxy gateway.
* Username already containing ``-session-<old>`` has the OLD id
  REPLACED, not duplicated. Modifiers after the session segment are
  preserved.
* Username without auth (``http://host:port/``) — no session is
  injected (regeneration would have nothing to mutate). Slots are
  still duplicated for concurrency, but ``is_session_expanded=False``.
* ProxyPool actually grants ``multiplier × len(urls)`` concurrent
  leases when virtual slots are wired in.
* On failure, ``ProxyPool._release`` regenerates the session id of a
  session-expanded slot. Non-session-expanded slot URL is unchanged.
* Scoped env keys override the global multiplier in priority order.
* ``load_runtime_config`` with no env knobs preserves the legacy
  output exactly (regression test for the no-op default).

These tests deliberately use unit-level construction (no Smartproxy
gateway, no asyncio event loop for sync helpers) so they run fast.
"""
from __future__ import annotations

import asyncio
import os
import unittest
from contextlib import contextmanager
from urllib.parse import urlparse


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


class _SessionUsernameTests(unittest.TestCase):
    """Pure-helper tests for ``_set_session_id_in_url`` /
    ``_replace_session_in_username`` so URL parsing is locked down
    independently of the surrounding wiring."""

    def test_appends_session_when_username_has_no_marker(self) -> None:
        from schema_inspector.runtime import _set_session_id_in_url

        url = "http://baseuser:pass@proxy.smartproxy.net:3120"
        out = _set_session_id_in_url(url, "abc12345")
        self.assertEqual(
            out,
            "http://baseuser-session-abc12345:pass@proxy.smartproxy.net:3120",
        )

    def test_replaces_existing_session_id(self) -> None:
        from schema_inspector.runtime import _set_session_id_in_url

        url = "http://baseuser-session-OLDID:pass@proxy.smartproxy.net:3120"
        out = _set_session_id_in_url(url, "newid678")
        # Old id REPLACED, not appended.
        self.assertNotIn("OLDID", out)
        self.assertIn("-session-newid678", out)
        self.assertEqual(
            out,
            "http://baseuser-session-newid678:pass@proxy.smartproxy.net:3120",
        )

    def test_preserves_modifiers_after_session_segment(self) -> None:
        from schema_inspector.runtime import _set_session_id_in_url

        url = "http://baseuser-session-OLD-sessionduration-30:pass@proxy.smartproxy.net:3120"
        out = _set_session_id_in_url(url, "newid678")
        self.assertIn("-session-newid678-sessionduration-30", out)
        self.assertNotIn("OLD", out)

    def test_no_userinfo_returns_url_unchanged(self) -> None:
        from schema_inspector.runtime import _set_session_id_in_url

        url = "http://proxy.smartproxy.net:3120"
        out = _set_session_id_in_url(url, "abc12345")
        self.assertEqual(out, url)

    def test_username_only_no_password(self) -> None:
        from schema_inspector.runtime import _set_session_id_in_url

        url = "http://baseuser@proxy.smartproxy.net:3120"
        out = _set_session_id_in_url(url, "abc12345")
        self.assertEqual(out, "http://baseuser-session-abc12345@proxy.smartproxy.net:3120")


class _ProxyUrlExpansionTests(unittest.TestCase):
    def test_multiplier_1_returns_one_endpoint_per_url_no_session(self) -> None:
        from schema_inspector.runtime import _expand_proxy_urls_with_sessions

        urls = [
            "http://user1:pass1@proxy.smartproxy.net:3120",
            "http://user2:pass2@proxy.smartproxy.net:3120",
        ]
        out = _expand_proxy_urls_with_sessions(urls, multiplier=1)

        self.assertEqual(len(out), 2)
        self.assertEqual(out[0], ("proxy_1", urls[0], False))
        self.assertEqual(out[1], ("proxy_2", urls[1], False))

    def test_multiplier_0_or_negative_treated_as_noop(self) -> None:
        from schema_inspector.runtime import _expand_proxy_urls_with_sessions

        urls = ["http://user1:pass1@proxy.smartproxy.net:3120"]
        for bad_multiplier in (0, -1, -100):
            with self.subTest(multiplier=bad_multiplier):
                out = _expand_proxy_urls_with_sessions(urls, multiplier=bad_multiplier)
                self.assertEqual(out, [("proxy_1", urls[0], False)])

    def test_multiplier_4_with_5_credentialed_urls_yields_20_slots(self) -> None:
        from schema_inspector.runtime import _expand_proxy_urls_with_sessions

        urls = [
            f"http://smart-{i}:secret-{i}@proxy.smartproxy.net:3120"
            for i in range(5)
        ]
        out = _expand_proxy_urls_with_sessions(urls, multiplier=4)

        self.assertEqual(len(out), 20)
        # All slot names unique.
        names = [name for name, _, _ in out]
        self.assertEqual(len(set(names)), 20)
        # Naming pattern.
        for index in range(5):
            for slot in range(4):
                self.assertIn(f"proxy_{index + 1}_s{slot:02d}", names)
        # All session ids unique.
        usernames = [
            urlparse(url).netloc.split("@")[0].split(":")[0] for _, url, _ in out
        ]
        session_ids = [u.split("-session-")[1].split("-")[0] for u in usernames]
        self.assertEqual(len(set(session_ids)), 20, "session ids must be unique across slots")
        # Hostport stays identical (Smartproxy single gateway).
        for _, url, _ in out:
            self.assertIn("@proxy.smartproxy.net:3120", url)
        # All marked is_session_expanded=True.
        for _, _, is_expanded in out:
            self.assertTrue(is_expanded)

    def test_existing_session_id_in_input_url_is_replaced_not_duplicated(self) -> None:
        from schema_inspector.runtime import _expand_proxy_urls_with_sessions

        # Input URL already contains a session id from some earlier config layer.
        urls = ["http://smart-foo-session-PRESET:pass@proxy.smartproxy.net:3120"]
        out = _expand_proxy_urls_with_sessions(urls, multiplier=3)

        self.assertEqual(len(out), 3)
        for _, url, _ in out:
            # PRESET id should NOT appear — it was replaced.
            self.assertNotIn("PRESET", url)
            # Exactly one ``-session-`` segment in the username.
            netloc = urlparse(url).netloc
            username = netloc.split("@")[0].split(":")[0]
            self.assertEqual(username.count("-session-"), 1)

    def test_multiplier_with_no_auth_url_gives_unique_names_but_no_session(self) -> None:
        from schema_inspector.runtime import _expand_proxy_urls_with_sessions

        urls = ["http://proxy.smartproxy.net:3120"]
        out = _expand_proxy_urls_with_sessions(urls, multiplier=4)

        self.assertEqual(len(out), 4)
        # All same URL (no auth → nothing to randomise).
        for _, url, _ in out:
            self.assertEqual(url, urls[0])
        # Names still unique (so ProxyPool sees them as distinct slots).
        names = [name for name, _, _ in out]
        self.assertEqual(len(set(names)), 4)
        # Not session-expanded → regenerate_session() is a no-op for these.
        for _, _, is_expanded in out:
            self.assertFalse(is_expanded)

    def test_blank_urls_are_filtered(self) -> None:
        from schema_inspector.runtime import _expand_proxy_urls_with_sessions

        urls = ["", "http://user:pass@proxy.smartproxy.net:3120", "   "]
        out = _expand_proxy_urls_with_sessions(urls, multiplier=2)

        self.assertEqual(len(out), 2)
        for _, url, _ in out:
            self.assertIn("@proxy.smartproxy.net:3120", url)


class _MultiplierResolutionTests(unittest.TestCase):
    def test_default_no_env_returns_1(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_session_multiplier

        with _patched_env(SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER=None):
            self.assertEqual(_resolve_proxy_session_multiplier({}), 1)

    def test_global_env_value(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_session_multiplier

        env = {"SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER": "8"}
        self.assertEqual(_resolve_proxy_session_multiplier(env), 8)

    def test_scoped_env_takes_priority_over_global(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_session_multiplier

        env = {
            "SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER": "2",
            "SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_SESSION_MULTIPLIER": "10",
        }
        out = _resolve_proxy_session_multiplier(
            env,
            env_keys=("SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_SESSION_MULTIPLIER",),
        )
        self.assertEqual(out, 10)

    def test_scoped_unset_falls_back_to_global(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_session_multiplier

        env = {"SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER": "3"}
        out = _resolve_proxy_session_multiplier(
            env,
            env_keys=("SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_SESSION_MULTIPLIER",),
        )
        self.assertEqual(out, 3)

    def test_invalid_value_falls_through(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_session_multiplier

        env = {
            "SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_SESSION_MULTIPLIER": "not_a_number",
            "SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER": "5",
        }
        out = _resolve_proxy_session_multiplier(
            env,
            env_keys=("SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_SESSION_MULTIPLIER",),
        )
        self.assertEqual(out, 5)

    def test_value_below_1_returns_1(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_session_multiplier

        env = {"SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER": "0"}
        self.assertEqual(_resolve_proxy_session_multiplier(env), 1)

    def test_value_capped_at_100(self) -> None:
        from schema_inspector.runtime import _resolve_proxy_session_multiplier

        env = {"SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER": "9999"}
        self.assertEqual(_resolve_proxy_session_multiplier(env), 100)


class _ProxyEndpointRegenerationTests(unittest.TestCase):
    def test_session_expanded_endpoint_regenerates_url(self) -> None:
        from schema_inspector.runtime import ProxyEndpoint

        ep = ProxyEndpoint(
            name="proxy_1_s00",
            url="http://user-session-OLDID:pass@proxy.smartproxy.net:3120",
            is_session_expanded=True,
        )
        original_url = ep.url
        ep.regenerate_session()

        self.assertNotEqual(ep.url, original_url)
        self.assertNotIn("OLDID", ep.url)
        self.assertIn("-session-", ep.url)
        self.assertIn("@proxy.smartproxy.net:3120", ep.url)
        # Name stays constant — pool indexes by name.
        self.assertEqual(ep.name, "proxy_1_s00")

    def test_non_session_expanded_endpoint_url_unchanged(self) -> None:
        from schema_inspector.runtime import ProxyEndpoint

        ep = ProxyEndpoint(
            name="proxy_1",
            url="http://user:pass@proxy.smartproxy.net:3120",
            is_session_expanded=False,
        )
        ep.regenerate_session()
        # Static endpoint URL must NOT mutate.
        self.assertEqual(ep.url, "http://user:pass@proxy.smartproxy.net:3120")

    def test_repeated_regeneration_yields_distinct_session_ids(self) -> None:
        from schema_inspector.runtime import ProxyEndpoint

        ep = ProxyEndpoint(
            name="proxy_1_s00",
            url="http://user:pass@proxy.smartproxy.net:3120",
            is_session_expanded=True,
        )
        ep.regenerate_session()
        first_id = ep.url.split("-session-")[1].split(":")[0].split("-")[0]
        ep.regenerate_session()
        second_id = ep.url.split("-session-")[1].split(":")[0].split("-")[0]
        # Two consecutive regenerations should produce different ids
        # (uuid4 collision probability is negligible).
        self.assertNotEqual(first_id, second_id)


class _ProxyPoolWithVirtualSlotsTests(unittest.IsolatedAsyncioTestCase):
    async def test_pool_grants_n_concurrent_leases_with_virtual_slots(self) -> None:
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import (
            ProxyEndpoint,
            _expand_proxy_urls_with_sessions,
        )

        urls = ["http://user:pass@proxy.smartproxy.net:3120"]
        expanded = _expand_proxy_urls_with_sessions(urls, multiplier=4)
        endpoints = tuple(
            ProxyEndpoint(name=name, url=url, is_session_expanded=is_expanded)
            for name, url, is_expanded in expanded
        )
        pool = ProxyPool(endpoints, default_success_cooldown_seconds=0, jitter_seconds=0)

        # Acquire 4 leases simultaneously. Without expansion this would
        # block on the 2nd acquire because 1 endpoint = 1 slot.
        leases = []
        for _ in range(4):
            lease = await asyncio.wait_for(pool.acquire(), timeout=0.2)
            self.assertIsNotNone(lease)
            leases.append(lease)
        # 5th acquire must NOT immediately succeed — all 4 slots taken.
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.acquire(), timeout=0.05)
        # Release everything.
        for lease in leases:
            await lease.release(success=True)

    async def test_failure_release_regenerates_session_for_virtual_slot(self) -> None:
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import ProxyEndpoint

        endpoints = (
            ProxyEndpoint(
                name="proxy_1_s00",
                url="http://user-session-OLDID:pass@proxy.smartproxy.net:3120",
                is_session_expanded=True,
            ),
        )
        pool = ProxyPool(endpoints, default_success_cooldown_seconds=0, jitter_seconds=0)

        lease = await asyncio.wait_for(pool.acquire(), timeout=0.2)
        original_url = lease.endpoint.url
        await lease.release(success=False)

        # On failure the session id mutates, picking a fresh upstream
        # exit IP for the next acquire on this slot.
        self.assertNotEqual(endpoints[0].url, original_url)
        self.assertNotIn("OLDID", endpoints[0].url)
        self.assertIn("-session-", endpoints[0].url)

    async def test_failure_release_does_not_mutate_static_endpoint(self) -> None:
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import ProxyEndpoint

        endpoints = (
            ProxyEndpoint(
                name="proxy_1",
                url="http://user:pass@proxy.smartproxy.net:3120",
                is_session_expanded=False,
            ),
        )
        pool = ProxyPool(endpoints, default_success_cooldown_seconds=0, jitter_seconds=0)

        lease = await asyncio.wait_for(pool.acquire(), timeout=0.2)
        original_url = lease.endpoint.url
        await lease.release(success=False)

        # Non-session-expanded endpoint URL stays constant — preserves
        # legacy behaviour for static / non-Smartproxy proxies.
        self.assertEqual(endpoints[0].url, original_url)


class _LoadRuntimeConfigDefaultIsNoOpTests(unittest.TestCase):
    """Regression: deploying this change with no env knobs set must
    produce the SAME proxy_endpoints tuple as before the patch.

    This is the critical safety net — prod is currently running with
    no SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER set, so the next
    deploy must be a behavioural no-op."""

    def test_default_env_yields_one_endpoint_per_url_with_legacy_naming(self) -> None:
        from schema_inspector.runtime import load_runtime_config

        env = {
            "SCHEMA_INSPECTOR_PROXY_URLS": (
                "http://user1:pass1@proxy.smartproxy.net:3120,"
                "http://user2:pass2@proxy.smartproxy.net:3120"
            ),
            # No multiplier env set — default no-op path.
        }
        cfg = load_runtime_config(env=env)

        # Exactly 2 endpoints, named like before.
        self.assertEqual(len(cfg.proxy_endpoints), 2)
        self.assertEqual(cfg.proxy_endpoints[0].name, "proxy_1")
        self.assertEqual(cfg.proxy_endpoints[1].name, "proxy_2")
        # Same URLs, no -session- mutation.
        self.assertEqual(
            cfg.proxy_endpoints[0].url,
            "http://user1:pass1@proxy.smartproxy.net:3120",
        )
        self.assertEqual(
            cfg.proxy_endpoints[1].url,
            "http://user2:pass2@proxy.smartproxy.net:3120",
        )
        # is_session_expanded=False on both — failure won't mutate URLs.
        self.assertFalse(cfg.proxy_endpoints[0].is_session_expanded)
        self.assertFalse(cfg.proxy_endpoints[1].is_session_expanded)

    def test_global_multiplier_4_expands_to_8_endpoints(self) -> None:
        from schema_inspector.runtime import load_runtime_config

        env = {
            "SCHEMA_INSPECTOR_PROXY_URLS": (
                "http://user1:pass1@proxy.smartproxy.net:3120,"
                "http://user2:pass2@proxy.smartproxy.net:3120"
            ),
            "SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER": "4",
        }
        cfg = load_runtime_config(env=env)

        self.assertEqual(len(cfg.proxy_endpoints), 8)
        for ep in cfg.proxy_endpoints:
            self.assertTrue(ep.is_session_expanded)
            self.assertIn("-session-", ep.url)
            self.assertIn("@proxy.smartproxy.net:3120", ep.url)
        names = [ep.name for ep in cfg.proxy_endpoints]
        self.assertEqual(len(set(names)), 8)

    def test_scoped_key_overrides_global(self) -> None:
        from schema_inspector.runtime import load_runtime_config

        env = {
            "SCHEMA_INSPECTOR_PROXY_URLS": "http://user1:pass1@proxy.smartproxy.net:3120",
            "SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER": "2",
            "SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_SESSION_MULTIPLIER": "10",
        }
        cfg = load_runtime_config(
            env=env,
            proxy_session_multiplier_env_keys=(
                "SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_SESSION_MULTIPLIER",
            ),
        )
        # Scoped key wins → 10 slots, not 2.
        self.assertEqual(len(cfg.proxy_endpoints), 10)


# ---------------------------------------------------------------------------
# 6. Scoped multiplier env-key wiring per CLI subcommand.
#    Locks down the mapping CLI command -> scoped env key so a typo or
#    refactor in cli.py cannot silently break the per-lane rollout.
#    Read-only against the cli.py source (AST/regex), no actual CLI run.
# ---------------------------------------------------------------------------
class _CliScopedMultiplierKeyWiringTests(unittest.TestCase):
    """The dispatch path in ``cli.py`` maps CLI subcommands to their
    scoped Smartproxy session-multiplier env key. The mapping is the
    only place that decides which env vars affect which lane — a typo
    or accidental removal here silently makes the per-lane canary a
    no-op (every fetch goes back to multiplier=1 with proxy_X names
    only). These tests assert the mapping is present and correct."""

    @staticmethod
    def _read_cli_dispatch_source() -> str:
        import inspect
        from schema_inspector.cli import _dispatch

        return inspect.getsource(_dispatch)

    def test_hydrate_scoped_key_present(self) -> None:
        src = self._read_cli_dispatch_source()
        self.assertIn("worker-hydrate", src)
        self.assertIn("SCHEMA_INSPECTOR_HYDRATE_PROXY_SESSION_MULTIPLIER", src)

    def test_live_tier_3_scoped_key_present(self) -> None:
        src = self._read_cli_dispatch_source()
        self.assertIn("worker-live-tier-3", src)
        self.assertIn("SCHEMA_INSPECTOR_LIVE_TIER_3_PROXY_SESSION_MULTIPLIER", src)

    def test_live_warm_scoped_key_present(self) -> None:
        src = self._read_cli_dispatch_source()
        self.assertIn("worker-live-warm", src)
        self.assertIn("SCHEMA_INSPECTOR_LIVE_WARM_PROXY_SESSION_MULTIPLIER", src)

    def test_live_tier_2_scoped_key_present(self) -> None:
        src = self._read_cli_dispatch_source()
        self.assertIn("worker-live-tier-2", src)
        self.assertIn("SCHEMA_INSPECTOR_LIVE_TIER_2_PROXY_SESSION_MULTIPLIER", src)

    def test_live_tier_1_scoped_key_still_present(self) -> None:
        # Regression: don't lose the original tier_1 mapping when adding new lanes.
        src = self._read_cli_dispatch_source()
        self.assertIn("worker-live-tier-1", src)
        self.assertIn("SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_SESSION_MULTIPLIER", src)

    def test_global_fallback_path_intact(self) -> None:
        # Regression: the global env knob must still be the fallback —
        # tested via the runtime helper which dispatch passes through.
        from schema_inspector.runtime import _resolve_proxy_session_multiplier

        env = {"SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER": "7"}
        # Empty scoped chain → global wins.
        self.assertEqual(_resolve_proxy_session_multiplier(env, env_keys=()), 7)


# ---------------------------------------------------------------------------
# 7. End-to-end behaviour for each scoped lane: when its env key is set,
#    load_runtime_config with that key emits the expanded endpoint set;
#    when only the global is set, the scoped lane falls back to global.
# ---------------------------------------------------------------------------
class _ScopedLaneBehaviourTests(unittest.TestCase):
    """For each scoped lane key, verify:
    (a) setting only the scoped key gives that multiplier;
    (b) unsetting the scoped key + setting global gives the global multiplier;
    (c) both set → scoped wins.
    """

    _BASE_ENV: dict = {
        "SCHEMA_INSPECTOR_PROXY_URLS": "http://user1:pass1@proxy.smartproxy.net:3120",
    }

    def _expect_endpoints(self, env: dict, scoped_key: str, expected: int) -> None:
        from schema_inspector.runtime import load_runtime_config

        cfg = load_runtime_config(
            env=env,
            proxy_session_multiplier_env_keys=(scoped_key,),
        )
        self.assertEqual(len(cfg.proxy_endpoints), expected)

    def test_hydrate_scoped_only(self) -> None:
        env = dict(self._BASE_ENV)
        env["SCHEMA_INSPECTOR_HYDRATE_PROXY_SESSION_MULTIPLIER"] = "6"
        self._expect_endpoints(env, "SCHEMA_INSPECTOR_HYDRATE_PROXY_SESSION_MULTIPLIER", 6)

    def test_hydrate_scoped_falls_back_to_global(self) -> None:
        env = dict(self._BASE_ENV)
        env["SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER"] = "3"
        self._expect_endpoints(env, "SCHEMA_INSPECTOR_HYDRATE_PROXY_SESSION_MULTIPLIER", 3)

    def test_hydrate_scoped_overrides_global(self) -> None:
        env = dict(self._BASE_ENV)
        env["SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER"] = "3"
        env["SCHEMA_INSPECTOR_HYDRATE_PROXY_SESSION_MULTIPLIER"] = "10"
        self._expect_endpoints(env, "SCHEMA_INSPECTOR_HYDRATE_PROXY_SESSION_MULTIPLIER", 10)

    def test_tier_3_scoped_only(self) -> None:
        env = dict(self._BASE_ENV)
        env["SCHEMA_INSPECTOR_LIVE_TIER_3_PROXY_SESSION_MULTIPLIER"] = "5"
        self._expect_endpoints(env, "SCHEMA_INSPECTOR_LIVE_TIER_3_PROXY_SESSION_MULTIPLIER", 5)

    def test_warm_scoped_only(self) -> None:
        env = dict(self._BASE_ENV)
        env["SCHEMA_INSPECTOR_LIVE_WARM_PROXY_SESSION_MULTIPLIER"] = "4"
        self._expect_endpoints(env, "SCHEMA_INSPECTOR_LIVE_WARM_PROXY_SESSION_MULTIPLIER", 4)

    def test_tier_2_scoped_only(self) -> None:
        env = dict(self._BASE_ENV)
        env["SCHEMA_INSPECTOR_LIVE_TIER_2_PROXY_SESSION_MULTIPLIER"] = "8"
        self._expect_endpoints(env, "SCHEMA_INSPECTOR_LIVE_TIER_2_PROXY_SESSION_MULTIPLIER", 8)

    def test_no_env_set_returns_one_endpoint_no_op(self) -> None:
        # Critical safety net: deploying with NO env keys set is a no-op
        # for every CLI subcommand, regardless of which scoped key the
        # subcommand passes.
        from schema_inspector.runtime import load_runtime_config

        env = dict(self._BASE_ENV)
        for scoped_key in (
            "SCHEMA_INSPECTOR_HYDRATE_PROXY_SESSION_MULTIPLIER",
            "SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_SESSION_MULTIPLIER",
            "SCHEMA_INSPECTOR_LIVE_TIER_2_PROXY_SESSION_MULTIPLIER",
            "SCHEMA_INSPECTOR_LIVE_TIER_3_PROXY_SESSION_MULTIPLIER",
            "SCHEMA_INSPECTOR_LIVE_WARM_PROXY_SESSION_MULTIPLIER",
        ):
            with self.subTest(scoped_key=scoped_key):
                cfg = load_runtime_config(
                    env=env,
                    proxy_session_multiplier_env_keys=(scoped_key,),
                )
                self.assertEqual(len(cfg.proxy_endpoints), 1)
                self.assertFalse(cfg.proxy_endpoints[0].is_session_expanded)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
