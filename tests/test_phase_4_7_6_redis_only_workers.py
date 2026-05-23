"""Phase 4.7.6 Track 1 Steps 2+3 (2026-05-23): worker hot path is
Redis-only — workers NEVER read the registry table at all. A separate
``cli league-capability prime-redis`` process does the SELECT in its
own connection out-of-band.

Why we're going this far: even Phase 4.7.5's "Redis-only AFTER warm"
design failed in production because the warm itself raced the asyncpg
pool at worker startup (73 workers × bulk SELECT × pool not yet ready
= mass TimeoutError; ~42% of workers ended up with empty cache). The
fix is to remove the warm SELECT from worker processes entirely. The
prime-redis CLI runs once at deploy time, then every N hours from a
systemd timer / cron, in its OWN process where there's no contention.

Two changes locked down by this file:

  1. ``Registry.warm_cache_from_db`` is now a no-op. It marks
     ``self._warmed = True`` and returns 0. No DB connection, no
     SELECT, no Redis writes. Workers using ``get_verdicts_batch``
     skip the first-call warm because ``_warmed`` is already True
     after the no-op (cheap bool flip).

  2. A new CLI action ``league-capability prime-redis`` runs in its
     own process. It opens its own asyncpg connection, runs ONE
     SELECT against ``league_endpoint_capability`` for all active
     rows, and SETs each into Redis with a 24h TTL. Reports the
     primed count. Idempotent — safe to run on a schedule.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone


# ----- Fakes (reused from earlier phases) ----------------------------------


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.set_calls: list[tuple[str, str, int | None]] = []

    def get(self, key):
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


class _RecordingRepository:
    def __init__(self, rows=None) -> None:
        self._rows = list(rows or [])
        self.list_active_calls: int = 0

    async def list_active_capabilities(self, executor):
        self.list_active_calls += 1
        return list(self._rows)


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


# ----- 1. warm_cache_from_db is a worker-side no-op ------------------------


class WarmIsNoOpInWorkerTests(unittest.IsolatedAsyncioTestCase):
    """The Phase 4.7.5 warm SELECT raced the asyncpg pool at worker
    startup and stranded ~42% of workers with empty caches. Phase 4.7.6
    Step 2 removes the SELECT entirely from worker code. The method
    stays so the lazy-warm hook keeps working, but it does nothing."""

    async def test_warm_does_not_call_repository(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
        )
        repo = _RecordingRepository(rows=[
            _row(endpoint="/api/v1/event/{event_id}/comments"),
        ])
        backend = _FakeRedisBackend()
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=repo,
        )

        primed = await registry.warm_cache_from_db()

        self.assertEqual(primed, 0)
        self.assertEqual(
            repo.list_active_calls, 0,
            "Phase 4.7.6 Step 2: workers must NOT touch the registry "
            "DB. Cache priming is done out-of-band by the prime-redis "
            "CLI in its own process.",
        )
        self.assertEqual(backend.set_calls, [])

    async def test_warm_marks_self_warmed(self) -> None:
        """The flag still flips so lazy-warm in get_verdicts_batch
        short-circuits and doesn't repeat the no-op."""
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
        )
        registry = LeagueCapabilitiesRegistry(
            redis_backend=_FakeRedisBackend(),
            database=_FakeDatabase(),
            repository=_RecordingRepository(),
        )
        await registry.warm_cache_from_db()
        self.assertTrue(registry._warmed)


# ----- 2. get_verdicts_batch still works against an externally-primed cache


class WorkerReadsPreprimedCacheTests(unittest.IsolatedAsyncioTestCase):
    """When prime-redis (Step 3) populates Redis from a separate process,
    workers must read those keys via the existing ``_cache_key`` format
    and serve them through ``get_verdicts_batch``. This locks down the
    contract between the two halves of the system."""

    async def test_externally_primed_keys_are_served(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry, EndpointVerdict,
        )
        backend = _FakeRedisBackend()
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=_RecordingRepository(),
        )

        # Simulate prime-redis having written this key earlier in a
        # separate process.
        key = registry._cache_key(
            unique_tournament_id=17,
            season_id=61643,
            status_type="finished",
            endpoint_pattern="/api/v1/event/{event_id}/comments",
        )
        backend.values[key] = "disabled"

        result = await registry.get_verdicts_batch(
            unique_tournament_id=17,
            season_id=61643,
            status_type="finished",
            endpoint_patterns=("/api/v1/event/{event_id}/comments",),
        )

        self.assertEqual(
            result.get("/api/v1/event/{event_id}/comments"),
            EndpointVerdict.DISABLED,
        )


# ----- 3. prime-redis CLI subcommand --------------------------------------


class PrimeRedisCLIHandlerTests(unittest.IsolatedAsyncioTestCase):
    """The prime-redis action runs in its own process — it gets its own
    asyncpg connection, runs ONE SELECT, and SETs each row into Redis
    with the 24h warm TTL.

    Tests target the handler logic (the part inside the
    ``if action == 'prime-redis':`` block) rather than spawning the
    actual CLI. The handler is small and pure-ish enough that we can
    drive it through a thin wrapper to keep the test honest without
    booting the whole HybridApp."""

    async def test_prime_redis_writes_one_redis_key_per_row(self) -> None:
        from schema_inspector.cli import prime_redis_from_repository
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
        )

        rows = [
            _row(ut=17, season=76986,
                 endpoint="/api/v1/event/{event_id}/comments", state="allowed"),
            _row(ut=17, season=76986,
                 endpoint="/api/v1/event/{event_id}/highlights", state="disabled"),
            _row(ut=7, season=76953,
                 endpoint="/api/v1/event/{event_id}/comments", state="allowed"),
        ]
        backend = _FakeRedisBackend()
        repo = _RecordingRepository(rows=rows)
        database = _FakeDatabase()

        primed = await prime_redis_from_repository(
            database=database,
            redis_backend=backend,
            repository=repo,
        )

        self.assertEqual(primed, 3)
        self.assertEqual(repo.list_active_calls, 1, "exactly one SELECT")
        # Each row gets keyed identically to the worker-side _cache_key
        # so worker get_verdicts_batch finds them.
        # Use the Registry to compute expected keys.
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend, database=database, repository=repo,
        )
        for row in rows:
            key = registry._cache_key(
                unique_tournament_id=row.unique_tournament_id,
                season_id=row.season_id,
                status_type=row.status_type,
                endpoint_pattern=row.endpoint_pattern,
            )
            self.assertIn(key, backend.values)
            self.assertEqual(backend.values[key], row.state)

    async def test_prime_redis_writes_with_24h_ttl(self) -> None:
        """Same TTL as the Phase 4.7.5 warm path so the operational
        contract (refresh every 24h from a timer/cron) holds."""
        from schema_inspector.cli import prime_redis_from_repository
        rows = [_row(endpoint="/api/v1/event/{event_id}/comments")]
        backend = _FakeRedisBackend()
        await prime_redis_from_repository(
            database=_FakeDatabase(),
            redis_backend=backend,
            repository=_RecordingRepository(rows=rows),
        )
        _key, _value, ex = backend.set_calls[0]
        self.assertEqual(ex, 24 * 3600)

    async def test_prime_redis_aborts_without_redis_backend(self) -> None:
        """If invoked without a real Redis backend, the priming would be
        a useless in-memory write and the workers would still hit empty
        cache → degrade to legacy. Better to fail loudly."""
        from schema_inspector.cli import prime_redis_from_repository
        with self.assertRaises(RuntimeError):
            await prime_redis_from_repository(
                database=_FakeDatabase(),
                redis_backend=None,
                repository=_RecordingRepository(),
            )

    async def test_prime_redis_handles_empty_rows(self) -> None:
        """No rows in DB → no Redis writes, no error, returns 0."""
        from schema_inspector.cli import prime_redis_from_repository
        backend = _FakeRedisBackend()
        primed = await prime_redis_from_repository(
            database=_FakeDatabase(),
            redis_backend=backend,
            repository=_RecordingRepository(rows=[]),
        )
        self.assertEqual(primed, 0)
        self.assertEqual(backend.set_calls, [])


if __name__ == "__main__":
    unittest.main()
