"""Phase 4.7.5 (2026-05-23): pre-warm Redis cache → Redis-only hot path.

Phase 4.8 production flips (twice) failed despite the Phase 4.7.4 batch
fix because the root issue isn't query count — it's per-cluster
connection budget. 73 worker processes x asyncpg pool min_size=20 =
~1460 reserved Postgres connections vs the cluster's max_connections
ceiling (default ~100). Even one extra DB query per match-center fetch
tips the pool over under that pressure.

The fix moves the hot path to **Redis-only**:

  1. ``Repository.list_active_capabilities`` — one bulk SELECT that
     pulls every valid registry row.
  2. ``Registry.warm_cache_from_db`` — primes Redis with a 24-hour TTL
     for each row using the existing ``_cache_key`` format.
  3. ``Registry.get_verdicts_batch`` lazy-warms on its first call (one
     DB roundtrip ever, at process startup). After that every gate
     lookup serves from Redis — zero Postgres pressure on the hot path.
  4. Refresh daemon (Phase 4.5, future) re-runs warm periodically so
     verdicts updated by re-probes propagate.

Fail-safe: warm error logs and returns 0; ``get_verdicts_batch`` falls
back to legacy (empty dict) on any infra hiccup — same semantics as
flag-off behaviour.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone


# ----- Fakes ----------------------------------------------------------------


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.set_calls: list[tuple[str, str, int | None]] = []
        self.get_calls: list[str] = []

    def get(self, key):
        self.get_calls.append(key)
        return self.values.get(key)

    def set(self, key, value, *, ex=None):
        self.set_calls.append((key, value, ex))
        self.values[key] = value
        return True

    def delete(self, key):
        if key in self.values:
            del self.values[key]
            return 1
        return 0


class _FakeBulkRepository:
    """Repository fake exposing ``list_active_capabilities`` for warm."""

    def __init__(self, rows=None) -> None:
        self._rows = list(rows or [])
        self.list_active_calls: int = 0
        self.fetch_quad_calls: int = 0

    async def list_active_capabilities(self, executor):
        self.list_active_calls += 1
        return list(self._rows)

    async def fetch_capabilities_for_quad(
        self,
        executor,
        *,
        unique_tournament_id,
        season_id,
        status_type,
    ):
        # Should NOT be called by the hot path after warm. Test 6 asserts
        # this to lock down the Redis-only invariant.
        self.fetch_quad_calls += 1
        return [
            r for r in self._rows
            if r.unique_tournament_id == unique_tournament_id
            and r.season_id == season_id
            and r.status_type == status_type
        ]


class _FakeExecutor:
    async def __aenter__(self): return self  # noqa: E704
    async def __aexit__(self, *args): return False  # noqa: E704


class _FakeDatabase:
    def __init__(self) -> None:
        self._conn = _FakeExecutor()

    def connection(self):
        return self._conn


def _row(*, ut=17, season=61643, status="finished", endpoint, state="allowed"):
    from schema_inspector.storage.league_capabilities_repository import (
        CapabilityRow, SOURCE_PROBE,
    )
    return CapabilityRow(
        unique_tournament_id=ut,
        season_id=season,
        status_type=status,
        endpoint_pattern=endpoint,
        state=state,
        probed_at=datetime.now(timezone.utc),
        probe_samples_total=5,
        probe_samples_ok=5 if state == "allowed" else 0,
        probe_samples_http_404=0,
        probe_samples_empty=0,
        probe_samples_error=0,
        confidence_score=1.0,
        expires_at=datetime.now(timezone.utc) + timedelta(days=14),
        source=SOURCE_PROBE,
        notes=None,
    )


# ----- 1. Repository bulk SELECT -------------------------------------------


class ListActiveCapabilitiesTests(unittest.IsolatedAsyncioTestCase):
    """Bulk fetch that the warm pass uses. One SELECT, no per-quad
    filter — we want every row that's worth caching."""

    async def test_method_exists_on_repository(self) -> None:
        from schema_inspector.storage.league_capabilities_repository import (
            LeagueCapabilitiesRepository,
        )
        self.assertTrue(
            hasattr(LeagueCapabilitiesRepository, "list_active_capabilities"),
            "Phase 4.7.5 introduces list_active_capabilities for cache warm",
        )

    async def test_emits_single_unfiltered_select(self) -> None:
        from schema_inspector.storage.league_capabilities_repository import (
            LeagueCapabilitiesRepository,
        )
        captured: list[tuple] = []

        class _CapturingExecutor:
            async def fetch(self, query, *args):
                captured.append((query, args))
                return []

        repo = LeagueCapabilitiesRepository()
        result = await repo.list_active_capabilities(_CapturingExecutor())

        self.assertEqual(len(captured), 1, "exactly one SQL roundtrip")
        query, args = captured[0]
        self.assertIn("FROM league_endpoint_capability", query)
        # No WHERE on unique_tournament_id / season_id / status_type — we
        # want every row that's still valid, not a quad scope.
        self.assertNotIn("unique_tournament_id =", query)
        # We DO want to skip permanently-expired rows — refresh daemon
        # will purge those but in the meantime the warm shouldn't load
        # garbage into Redis.
        self.assertIn("expires_at", query)
        self.assertEqual(result, [])


# ----- 2. Registry.warm_cache_from_db --------------------------------------


class WarmCacheFromDbTests(unittest.IsolatedAsyncioTestCase):
    async def test_method_exists_on_registry(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
        )
        self.assertTrue(
            hasattr(LeagueCapabilitiesRegistry, "warm_cache_from_db"),
            "Phase 4.7.5 introduces warm_cache_from_db",
        )

    # Phase 4.7.6 Track 1 Step 2 (2026-05-23) removed worker-side warm
    # SELECTs entirely — warm_cache_from_db is now a no-op that only
    # marks self._warmed = True. The two tests deleted here
    # (test_writes_one_redis_key_per_row, test_writes_with_24h_ttl)
    # both asserted that warm wrote rows into Redis, which is exactly
    # the cold-start storm that crashed Phase 4.8 retry #2. Cache
    # priming now happens out-of-band in
    # ``cli league-capability prime-redis``; those assertions moved to
    # ``test_phase_4_7_6_redis_only_workers.py`` against the new
    # ``prime_redis_from_repository`` helper.

    async def test_empty_rows_short_circuits(self) -> None:
        """No rows in DB → no Redis writes, primed count zero, no error."""
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
        )
        backend = _FakeRedisBackend()
        repo = _FakeBulkRepository(rows=[])
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=repo,
        )

        primed = await registry.warm_cache_from_db()
        self.assertEqual(primed, 0)
        self.assertEqual(backend.set_calls, [])

    async def test_db_failure_does_not_raise(self) -> None:
        """If the warm SELECT fails (DB down, asyncpg pool exhausted on
        startup), we still must let the worker boot. Return 0 and log."""
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
        )

        class _ExplodingRepo:
            async def list_active_capabilities(self, executor):
                raise RuntimeError("simulated DB outage")

        backend = _FakeRedisBackend()
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=_ExplodingRepo(),
        )

        primed = await registry.warm_cache_from_db()
        self.assertEqual(primed, 0)
        self.assertEqual(backend.set_calls, [])


# ----- 3. Hot path is Redis-only --------------------------------------------
#
# Phase 4.7.6 Track 1 Step 2 (2026-05-23) deleted three tests that lived
# here previously because their setup depended on warm_cache_from_db
# actually writing to Redis from worker code:
#
#   * test_lazy_warm_on_first_batch_call — asserted lazy warm wrote the
#     row into Redis on the first batch call. Workers no longer do
#     that; the prime-redis CLI does.
#   * test_second_batch_call_uses_redis_only — depended on the lazy
#     warm priming the cache on call #1 so call #2 was Redis-only.
#   * test_explicit_warm_skips_lazy_re_warm — same dependency.
#
# The "Redis-only after external prime" invariant is now covered by
# test_phase_4_7_6_redis_only_workers.py
# (WorkerReadsPreprimedCacheTests + PrimeRedisCLIHandlerTests). The
# remaining test below (test_warm_failure_does_not_block_batch) is
# still valid: even when warm is a no-op it must not block subsequent
# batch lookups.


class HotPathRedisOnlyTests(unittest.IsolatedAsyncioTestCase):

    async def test_warm_failure_does_not_block_batch(self) -> None:
        """warm fails (DB outage at startup) → batch still runs, just
        misses any rows that would have been pre-cached. Patterns simply
        fall through to legacy via empty dict."""
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
        )

        class _ExplodingRepo:
            async def list_active_capabilities(self, executor):
                raise RuntimeError("DB down at startup")

            async def fetch_capabilities_for_quad(self, executor, **kw):
                # Should also not be called — Phase 4.7.5 hot path is
                # Redis-only, even on warm failure we don't fall back to
                # per-quad. Better an empty dict than another pool stall.
                raise AssertionError(
                    "regression: hot path must not call per-quad fallback"
                )

        backend = _FakeRedisBackend()
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=_ExplodingRepo(),
        )

        result = await registry.get_verdicts_batch(
            unique_tournament_id=17,
            season_id=61643,
            status_type="finished",
            endpoint_patterns=("/api/v1/event/{event_id}/comments",),
        )
        # Empty result is the safe degradation — orchestrator falls back
        # to legacy tier-based gating, identical to flag-off behaviour.
        self.assertEqual(result, {})


if __name__ == "__main__":
    unittest.main()
