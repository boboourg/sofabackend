"""X' patch tests: per-worker ``InspectorTransport._session_cache`` cap.

Production motivation (see /tmp/sofa.pcap analysis 2026-05-12):

* 5 Smartproxy credentials × 5 fingerprint profiles = 25 cache keys
  per worker; 47 active workers × 25 = ~944 sustained ESTAB TCP keep-alives
  to the Smartproxy gateway. Theoretical max with
  ``max_in_use_per_endpoint=4`` was 940 — observed 944 sat exactly at
  Smartproxy's concurrent-CONNECT ceiling and caused timeout cascades on
  high-traffic tier_1 events.
* The patch caps the cache per worker so each worker holds at most N
  entries; older idle entries are LRU-evicted (their ``AsyncSession`` is
  closed). With cap=5 and 47 workers, total ESTAB drops to ~235 — well
  under the ceiling, no per-fetch concurrency reduction needed.

Behaviours covered by this suite:

1. Default unset / 0 keeps legacy unbounded behaviour (the no-op safety
   property — phase 0 deploy MUST be behaviourally identical).
2. ``cap=5`` evicts the oldest IDLE entry when a 6th key is acquired.
3. The evicted ``AsyncSession.close()`` is awaited (via fire-and-forget
   task) — no leak.
4. An IN-FLIGHT session (refcount > 0) is never evicted or closed even
   when the cap is exceeded — the cache temporarily overflows; eviction
   resumes once any caller releases.
5. ``_discard_session`` against an in-flight entry tombstones it; the
   actual ``session.close()`` is deferred until the last in-flight caller
   calls ``_release_session``.
6. Under realistic rotation (5 proxies × 5 fingerprints = 25 unique
   keys) and ``cap=5`` with releases between acquires, the cache size
   stabilises at the cap.
7. ``fetch()`` continues to succeed after eviction has churned the cache.
8. Invalid / negative env value falls back to no-op (cap=0).
9. Scoped env (per-lane) wins over the global key.
"""
from __future__ import annotations

import asyncio
import unittest
from typing import Any
from unittest.mock import patch

from schema_inspector.runtime import (
    RuntimeConfig,
    _resolve_session_cache_max_entries,
    load_runtime_config,
)
from schema_inspector.transport import InspectorTransport, _SessionEntry


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeAsyncResponse:
    """Mimics the parts of ``curl_cffi`` ``Response`` that transport reads."""

    def __init__(self, url: str, status_code: int, body: bytes = b"{}") -> None:
        self.url = url
        self.status_code = status_code
        self.headers: dict[str, str] = {}
        self.content = body


class _FakeAsyncSession:
    """In-memory ``AsyncSession`` substitute.

    Records every ``close()`` call so eviction tests can assert on it.
    ``get()``/``head()`` return canned 200 responses synchronously inside
    the test event loop (no real network).
    """

    instances: list["_FakeAsyncSession"] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.close_call_count = 0
        self.get_call_count = 0
        type(self).instances.append(self)

    async def get(self, url: str, **kwargs: Any) -> _FakeAsyncResponse:
        self.get_call_count += 1
        return _FakeAsyncResponse(url=url, status_code=200, body=b"{}")

    async def head(self, url: str, **kwargs: Any) -> _FakeAsyncResponse:
        return _FakeAsyncResponse(url=url, status_code=200, body=b"")

    async def close(self) -> None:
        self.close_call_count += 1


def _patch_async_session():
    """Patch ``schema_inspector.transport.AsyncSession`` to the fake."""
    _FakeAsyncSession.instances.clear()
    return patch(
        "schema_inspector.transport.AsyncSession",
        _FakeAsyncSession,
    )


def _make_transport(cap: int = 0) -> InspectorTransport:
    """Build a minimal ``InspectorTransport`` for unit testing.

    ``RuntimeConfig`` is used directly (skips env reading) and the cap is
    set explicitly so tests don't depend on os.environ.
    """
    config = RuntimeConfig(session_cache_max_entries=cap)
    return InspectorTransport(config)


def _fp(name: str):
    """Tiny ``FingerprintProfile``-like stub.

    ``InspectorTransport._session_key`` only reads ``.name``; the TLS
    fields are read by ``_session_kwargs`` and may be None.
    """
    from schema_inspector.runtime import FingerprintProfile

    return FingerprintProfile(
        name=name,
        impersonate="chrome110",
        user_agent="ua",
        accept_language="en",
        sec_ch_ua="",
        sec_ch_ua_mobile="?0",
        sec_ch_ua_platform='"Linux"',
        referer="https://example/",
    )


# ---------------------------------------------------------------------------
# 1. Env resolver — pure-function tests (no transport).
# ---------------------------------------------------------------------------


class SessionCacheMaxEntriesEnvTests(unittest.TestCase):
    def test_default_unset_returns_0(self) -> None:
        self.assertEqual(_resolve_session_cache_max_entries({}), 0)

    def test_global_env_value(self) -> None:
        env = {"SCHEMA_INSPECTOR_SESSION_CACHE_MAX_ENTRIES": "5"}
        self.assertEqual(_resolve_session_cache_max_entries(env), 5)

    def test_scoped_env_takes_priority_over_global(self) -> None:
        env = {
            "SCHEMA_INSPECTOR_SESSION_CACHE_MAX_ENTRIES": "5",
            "SCHEMA_INSPECTOR_LIVE_TIER_1_SESSION_CACHE_MAX_ENTRIES": "3",
        }
        out = _resolve_session_cache_max_entries(
            env,
            env_keys=("SCHEMA_INSPECTOR_LIVE_TIER_1_SESSION_CACHE_MAX_ENTRIES",),
        )
        self.assertEqual(out, 3)

    def test_scoped_unset_falls_back_to_global(self) -> None:
        env = {"SCHEMA_INSPECTOR_SESSION_CACHE_MAX_ENTRIES": "7"}
        out = _resolve_session_cache_max_entries(
            env,
            env_keys=("SCHEMA_INSPECTOR_LIVE_TIER_1_SESSION_CACHE_MAX_ENTRIES",),
        )
        self.assertEqual(out, 7)

    def test_invalid_value_falls_through(self) -> None:
        env = {
            "SCHEMA_INSPECTOR_LIVE_TIER_1_SESSION_CACHE_MAX_ENTRIES": "not_a_number",
            "SCHEMA_INSPECTOR_SESSION_CACHE_MAX_ENTRIES": "6",
        }
        out = _resolve_session_cache_max_entries(
            env,
            env_keys=("SCHEMA_INSPECTOR_LIVE_TIER_1_SESSION_CACHE_MAX_ENTRIES",),
        )
        self.assertEqual(out, 6)

    def test_negative_value_returns_0(self) -> None:
        for bad in ("-1", "-50"):
            with self.subTest(value=bad):
                env = {"SCHEMA_INSPECTOR_SESSION_CACHE_MAX_ENTRIES": bad}
                self.assertEqual(_resolve_session_cache_max_entries(env), 0)

    def test_value_capped_at_10000(self) -> None:
        env = {"SCHEMA_INSPECTOR_SESSION_CACHE_MAX_ENTRIES": "999999"}
        self.assertEqual(_resolve_session_cache_max_entries(env), 10000)

    def test_load_runtime_config_default_session_cache_max_is_0(self) -> None:
        """Critical safety net: no env knobs → cap=0 → legacy no-op behaviour."""
        config = load_runtime_config(env={"SCHEMA_INSPECTOR_PROXY_URLS": "http://p.local"})
        self.assertEqual(config.session_cache_max_entries, 0)


# ---------------------------------------------------------------------------
# 2. Default unset behaviour — legacy unbounded cache.
# ---------------------------------------------------------------------------


class DefaultLegacyBehaviourTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_cap_0_keeps_all_sessions(self) -> None:
        with _patch_async_session():
            transport = _make_transport(cap=0)
            # Acquire + release 50 distinct (proxy, fingerprint) tuples.
            for i in range(50):
                _, entry = await transport._acquire_session(
                    f"http://proxy-{i}.local", fingerprint_profile=_fp("fp-0")
                )
                await transport._release_session(entry)
            # All 50 retained — no eviction.
            self.assertEqual(len(transport._session_cache), 50)
            self.assertEqual(len(_FakeAsyncSession.instances), 50)
            for instance in _FakeAsyncSession.instances:
                self.assertEqual(instance.close_call_count, 0)


# ---------------------------------------------------------------------------
# 3. cap=5 evicts oldest IDLE entries.
# ---------------------------------------------------------------------------


class CapEvictsOldestIdleTests(unittest.IsolatedAsyncioTestCase):
    async def test_cap_5_evicts_oldest_when_6th_acquired(self) -> None:
        with _patch_async_session():
            transport = _make_transport(cap=5)
            # 8 sequential acquires + releases. Each insert past size=5
            # should evict the oldest idle entry.
            for i in range(8):
                _, entry = await transport._acquire_session(
                    f"http://proxy-{i}.local", fingerprint_profile=_fp("fp-0")
                )
                await transport._release_session(entry)
            # Cache stays at cap.
            self.assertEqual(len(transport._session_cache), 5)
            # Oldest 3 (proxy-0/1/2) evicted; newest 5 (proxy-3..7) retained.
            self.assertNotIn(
                "http://proxy-0.local|fp-0", transport._session_cache
            )
            self.assertNotIn(
                "http://proxy-2.local|fp-0", transport._session_cache
            )
            self.assertIn(
                "http://proxy-3.local|fp-0", transport._session_cache
            )
            self.assertIn(
                "http://proxy-7.local|fp-0", transport._session_cache
            )

    async def test_cache_hit_moves_entry_to_mru(self) -> None:
        """Re-acquiring an existing entry refreshes its LRU position."""
        with _patch_async_session():
            transport = _make_transport(cap=3)
            # Fill cache with 3 entries A, B, C (A oldest).
            for proxy in ("A", "B", "C"):
                _, entry = await transport._acquire_session(
                    f"http://{proxy}.local", fingerprint_profile=_fp("fp-0")
                )
                await transport._release_session(entry)
            # Touch A — it becomes MRU.
            _, entry_a = await transport._acquire_session(
                "http://A.local", fingerprint_profile=_fp("fp-0")
            )
            await transport._release_session(entry_a)
            # Insert D → must evict B (now oldest), keep A/C/D.
            _, entry_d = await transport._acquire_session(
                "http://D.local", fingerprint_profile=_fp("fp-0")
            )
            await transport._release_session(entry_d)
            self.assertEqual(len(transport._session_cache), 3)
            self.assertIn("http://A.local|fp-0", transport._session_cache)
            self.assertNotIn("http://B.local|fp-0", transport._session_cache)
            self.assertIn("http://C.local|fp-0", transport._session_cache)
            self.assertIn("http://D.local|fp-0", transport._session_cache)


# ---------------------------------------------------------------------------
# 4. Evicted session.close() is awaited.
# ---------------------------------------------------------------------------


class EvictedSessionIsClosedTests(unittest.IsolatedAsyncioTestCase):
    async def test_evicted_session_close_called(self) -> None:
        with _patch_async_session():
            transport = _make_transport(cap=2)
            # 3 acquires + releases → first session evicted on 3rd insert.
            for i in range(3):
                _, entry = await transport._acquire_session(
                    f"http://proxy-{i}.local", fingerprint_profile=_fp("fp-0")
                )
                await transport._release_session(entry)
            # Eviction close is fire-and-forget — yield event loop to let
            # the task run.
            for _ in range(5):
                await asyncio.sleep(0)
            self.assertEqual(_FakeAsyncSession.instances[0].close_call_count, 1)
            # The two retained sessions are NOT closed.
            self.assertEqual(_FakeAsyncSession.instances[1].close_call_count, 0)
            self.assertEqual(_FakeAsyncSession.instances[2].close_call_count, 0)


# ---------------------------------------------------------------------------
# 5. Active in-flight session is not evicted or closed prematurely.
# ---------------------------------------------------------------------------


class InflightSessionNotEvictedTests(unittest.IsolatedAsyncioTestCase):
    async def test_inflight_entries_skipped_by_eviction(self) -> None:
        with _patch_async_session():
            transport = _make_transport(cap=2)
            # Acquire two without releasing — both in-flight (refcount=1).
            s1, e1 = await transport._acquire_session(
                "http://A.local", fingerprint_profile=_fp("fp-0")
            )
            s2, e2 = await transport._acquire_session(
                "http://B.local", fingerprint_profile=_fp("fp-0")
            )
            # Third acquire — every existing entry is in-flight, so cache
            # temporarily overflows (size 3 > cap 2). NO close() called on
            # the in-flight pair.
            s3, e3 = await transport._acquire_session(
                "http://C.local", fingerprint_profile=_fp("fp-0")
            )
            self.assertEqual(len(transport._session_cache), 3)
            for instance in _FakeAsyncSession.instances:
                self.assertEqual(instance.close_call_count, 0)
            # Now release A — next idle acquire should evict it (oldest
            # idle), not B or C (still in-flight).
            await transport._release_session(e1)
            # Trigger another acquire forcing eviction.
            s4, e4 = await transport._acquire_session(
                "http://D.local", fingerprint_profile=_fp("fp-0")
            )
            for _ in range(5):
                await asyncio.sleep(0)
            self.assertEqual(_FakeAsyncSession.instances[0].close_call_count, 1)
            # B, C, D — none closed.
            for instance in _FakeAsyncSession.instances[1:]:
                self.assertEqual(instance.close_call_count, 0)
            # Cleanup.
            await transport._release_session(e2)
            await transport._release_session(e3)
            await transport._release_session(e4)


# ---------------------------------------------------------------------------
# 6. Discard while in-flight tombstones + defers close until last release.
# ---------------------------------------------------------------------------


class DiscardWhileInflightTests(unittest.IsolatedAsyncioTestCase):
    async def test_discard_while_inflight_defers_close(self) -> None:
        with _patch_async_session():
            transport = _make_transport(cap=0)
            # Two concurrent acquires of the SAME (proxy, fp) — share entry.
            s_a, e_a = await transport._acquire_session(
                "http://X.local", fingerprint_profile=_fp("fp-0")
            )
            s_b, e_b = await transport._acquire_session(
                "http://X.local", fingerprint_profile=_fp("fp-0")
            )
            self.assertIs(s_a, s_b)
            self.assertIs(e_a, e_b)
            self.assertEqual(e_a.refcount, 2)
            # Discard fires (e.g. network error on caller A).
            await transport._discard_session(
                "http://X.local", fingerprint_profile=_fp("fp-0")
            )
            # Tombstoned: removed from active cache, NOT yet closed.
            self.assertNotIn("http://X.local|fp-0", transport._session_cache)
            self.assertIn(e_a, transport._pending_close)
            self.assertTrue(e_a.tombstoned)
            self.assertEqual(_FakeAsyncSession.instances[0].close_call_count, 0)
            # Release caller A — refcount 2 → 1; B still using it.
            await transport._release_session(e_a)
            self.assertEqual(e_a.refcount, 1)
            self.assertEqual(_FakeAsyncSession.instances[0].close_call_count, 0)
            self.assertIn(e_a, transport._pending_close)
            # Release caller B — refcount 1 → 0 → close fires.
            await transport._release_session(e_b)
            self.assertEqual(e_b.refcount, 0)
            self.assertEqual(_FakeAsyncSession.instances[0].close_call_count, 1)
            self.assertNotIn(e_a, transport._pending_close)

    async def test_discard_while_idle_closes_immediately(self) -> None:
        with _patch_async_session():
            transport = _make_transport(cap=0)
            _, entry = await transport._acquire_session(
                "http://X.local", fingerprint_profile=_fp("fp-0")
            )
            await transport._release_session(entry)
            self.assertEqual(entry.refcount, 0)
            # No in-flight callers — discard closes synchronously, no tombstone.
            await transport._discard_session(
                "http://X.local", fingerprint_profile=_fp("fp-0")
            )
            self.assertEqual(_FakeAsyncSession.instances[0].close_call_count, 1)
            self.assertEqual(len(transport._pending_close), 0)


# ---------------------------------------------------------------------------
# 7. 5 proxies × 5 fingerprints cycle stabilises at cap.
# ---------------------------------------------------------------------------


class FullRotationStabilisesAtCapTests(unittest.IsolatedAsyncioTestCase):
    async def test_cap_5_under_25_unique_keys_stays_bounded(self) -> None:
        with _patch_async_session():
            transport = _make_transport(cap=5)
            proxies = [f"http://proxy-{i}.local" for i in range(5)]
            fps = [_fp(f"fp-{j}") for j in range(5)]
            for proxy in proxies:
                for fingerprint in fps:
                    _, entry = await transport._acquire_session(
                        proxy, fingerprint_profile=fingerprint
                    )
                    await transport._release_session(entry)
            # All 25 keys cycled through; cache stays at cap.
            self.assertEqual(len(transport._session_cache), 5)
            # 25 sessions created, 20 evicted, 5 retained.
            self.assertEqual(len(_FakeAsyncSession.instances), 25)
            # Wait for any pending background close tasks.
            for _ in range(20):
                await asyncio.sleep(0)
            closed_count = sum(
                1 for inst in _FakeAsyncSession.instances if inst.close_call_count > 0
            )
            self.assertEqual(closed_count, 20)


# ---------------------------------------------------------------------------
# 8. End-to-end fetch() still succeeds after eviction churn.
# ---------------------------------------------------------------------------


class FetchSucceedsAfterEvictionTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_returns_200_under_cap_churn(self) -> None:
        with _patch_async_session():
            # Use 5 proxy endpoints — fetch will cycle through them.
            config = load_runtime_config(
                env={
                    "SCHEMA_INSPECTOR_PROXY_URLS": ",".join(
                        f"http://user:pass@proxy-{i}.local:3128" for i in range(5)
                    ),
                    "SCHEMA_INSPECTOR_SESSION_CACHE_MAX_ENTRIES": "2",
                    "SCHEMA_INSPECTOR_PROXY_REQUEST_COOLDOWN_SECONDS": "0",
                    "SCHEMA_INSPECTOR_PROXY_REQUEST_JITTER_SECONDS": "0",
                },
            )
            self.assertEqual(config.session_cache_max_entries, 2)
            transport = InspectorTransport(config)
            try:
                # 5 fetches — across 5 proxies + 5 fingerprints, so each
                # call lands on a different (proxy, fp) tuple → eviction
                # should churn the cache continuously.
                for i in range(5):
                    result = await transport.fetch(
                        f"https://example.com/path/{i}", timeout=5.0
                    )
                    self.assertEqual(result.status_code, 200)
                # Cache obeys cap.
                self.assertLessEqual(len(transport._session_cache), 2)
            finally:
                await transport.close()


# ---------------------------------------------------------------------------
# 9. RuntimeConfig + InspectorTransport wiring sanity check.
# ---------------------------------------------------------------------------


class WiringSanityTests(unittest.TestCase):
    def test_runtime_config_field_default_is_0(self) -> None:
        config = RuntimeConfig()
        self.assertEqual(config.session_cache_max_entries, 0)

    def test_inspector_transport_reads_cap_from_config(self) -> None:
        config = RuntimeConfig(session_cache_max_entries=7)
        transport = InspectorTransport(config)
        self.assertEqual(transport._session_cache_max, 7)

    def test_inspector_transport_negative_cap_clamps_to_0(self) -> None:
        # Defensive: even if RuntimeConfig is constructed with a bogus
        # value, InspectorTransport must not propagate the negative cap.
        config = RuntimeConfig(session_cache_max_entries=-3)
        transport = InspectorTransport(config)
        self.assertEqual(transport._session_cache_max, 0)


if __name__ == "__main__":
    unittest.main()
