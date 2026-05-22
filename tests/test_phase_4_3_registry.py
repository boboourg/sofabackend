"""Phase 4.3: LeagueCapabilitiesRegistry — Redis-cached read API.

Pins the read-side contract that the orchestrator will use:

  1. Redis cache hit returns immediately without DB roundtrip.
  2. Cache miss falls back to repository, then primes the cache.
  3. Repository miss returns UNKNOWN (fail-safe to legacy policy).
  4. Season-specific row preferred over season=NULL fallback.
  5. Manual-override row prevails even if probed_at is recent (no
     re-cache from probe path).
  6. Explicit invalidate_ut_season_status clears cache so the next
     read goes back to the repository.

All tests are pure-Python — fake Redis backend + fake repository.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone


# ---------- Fake infrastructure -----------------------------------------------


class _FakeRedisBackend:
    """In-memory backend matching the subset of methods the registry uses."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.delete_calls: list[str] = []
        self.set_calls: list[tuple[str, str, int | None]] = []

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def set(self, key: str, value: str, *, ex: int | None = None) -> bool:
        self.set_calls.append((key, value, ex))
        self.values[key] = value
        return True

    def delete(self, key: str) -> int:
        self.delete_calls.append(key)
        if key in self.values:
            del self.values[key]
            return 1
        return 0


class _FakeRepository:
    def __init__(self, rows: list = None) -> None:
        from schema_inspector.storage.league_capabilities_repository import (
            CapabilityRow,
            STATE_ALLOWED,
            SOURCE_PROBE,
        )
        self._rows = list(rows or [])
        self.fetch_calls: list[tuple] = []

    async def fetch_capability(
        self,
        executor,
        *,
        unique_tournament_id,
        season_id,
        status_type,
        endpoint_pattern,
    ):
        self.fetch_calls.append(
            (unique_tournament_id, season_id, status_type, endpoint_pattern)
        )
        for row in self._rows:
            if (
                row.unique_tournament_id == unique_tournament_id
                and row.season_id == season_id
                and row.status_type == status_type
                and row.endpoint_pattern == endpoint_pattern
            ):
                return row
        return None


class _FakeExecutor:
    """async with db.connection() as conn: …. The fake yields itself."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class _FakeDatabase:
    def __init__(self) -> None:
        self._conn = _FakeExecutor()

    def connection(self):
        return self._conn


def _row(
    *,
    ut=17,
    season=61643,
    status="inprogress",
    endpoint="/api/v1/event/{event_id}/incidents",
    state=None,
    source=None,
):
    from schema_inspector.storage.league_capabilities_repository import (
        CapabilityRow,
        STATE_ALLOWED,
        SOURCE_PROBE,
    )
    return CapabilityRow(
        unique_tournament_id=ut,
        season_id=season,
        status_type=status,
        endpoint_pattern=endpoint,
        state=state or STATE_ALLOWED,
        probed_at=datetime.now(timezone.utc),
        probe_samples_total=5,
        probe_samples_ok=5,
        probe_samples_http_404=0,
        probe_samples_empty=0,
        probe_samples_error=0,
        confidence_score=1.0,
        expires_at=datetime.now(timezone.utc) + timedelta(days=14),
        source=source or SOURCE_PROBE,
        notes=None,
    )


# ---------- Tests --------------------------------------------------------------


class RegistryCacheHitTests(unittest.IsolatedAsyncioTestCase):
    async def test_cache_hit_returns_verdict_without_db(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
            EndpointVerdict,
        )

        backend = _FakeRedisBackend()
        repo = _FakeRepository()
        # Pre-populate cache.
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=repo,
        )
        cache_key = registry._cache_key(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_pattern="/api/v1/event/{event_id}/incidents",
        )
        backend.set(cache_key, "allowed", ex=3600)

        verdict = await registry.get_verdict(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_pattern="/api/v1/event/{event_id}/incidents",
        )

        self.assertEqual(verdict, EndpointVerdict.ALLOWED)
        self.assertEqual(repo.fetch_calls, [], "Cache hit must NOT touch DB.")


class RegistryCacheMissTests(unittest.IsolatedAsyncioTestCase):
    async def test_cache_miss_falls_back_to_repository(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
            EndpointVerdict,
        )

        backend = _FakeRedisBackend()
        repo = _FakeRepository([
            _row(state="allowed"),
        ])
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=repo,
        )

        verdict = await registry.get_verdict(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_pattern="/api/v1/event/{event_id}/incidents",
        )

        self.assertEqual(verdict, EndpointVerdict.ALLOWED)
        self.assertEqual(len(repo.fetch_calls), 1)

    async def test_cache_miss_primes_redis_after_db_read(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
            EndpointVerdict,
        )

        backend = _FakeRedisBackend()
        repo = _FakeRepository([_row(state="disabled")])
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=repo,
        )

        await registry.get_verdict(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_pattern="/api/v1/event/{event_id}/incidents",
        )

        self.assertEqual(len(backend.set_calls), 1, "DB result must be cached.")
        key, value, ttl = backend.set_calls[0]
        self.assertEqual(value, "disabled")
        self.assertIsNotNone(ttl, "Cache write must set TTL.")

    async def test_db_miss_returns_unknown_failsafe(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
            EndpointVerdict,
        )

        backend = _FakeRedisBackend()
        repo = _FakeRepository([])  # empty
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=repo,
        )

        verdict = await registry.get_verdict(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_pattern="/api/v1/event/{event_id}/never-probed",
        )

        self.assertEqual(verdict, EndpointVerdict.UNKNOWN)


class RegistrySeasonFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_ut_level_fallback_used_when_season_specific_missing(self) -> None:
        """When per-season row is missing, registry tries the UT-level
        row (season_id=NULL). Used for cup-style competitions where
        per-season probing is sparse."""
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
            EndpointVerdict,
        )

        backend = _FakeRedisBackend()
        repo = _FakeRepository([
            # UT-level fallback row (season=None)
            _row(ut=16, season=None, state="disabled"),
        ])
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=repo,
        )

        verdict = await registry.get_verdict(
            unique_tournament_id=16,
            season_id=99999,  # any season, no specific row
            status_type="inprogress",
            endpoint_pattern="/api/v1/event/{event_id}/incidents",
        )

        self.assertEqual(verdict, EndpointVerdict.DISABLED)
        # Two lookups: season-specific (miss) then UT-level (hit).
        self.assertEqual(len(repo.fetch_calls), 2)
        self.assertEqual(repo.fetch_calls[0][1], 99999, "First lookup is season-specific")
        self.assertIsNone(repo.fetch_calls[1][1], "Fallback lookup is season=None")

    async def test_season_specific_row_preferred_over_ut_level(self) -> None:
        """Season-specific row wins even if a UT-level row exists."""
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
            EndpointVerdict,
        )

        backend = _FakeRedisBackend()
        repo = _FakeRepository([
            _row(ut=16, season=58210, state="allowed"),       # specific
            _row(ut=16, season=None, state="disabled"),       # UT-level fallback
        ])
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=repo,
        )

        verdict = await registry.get_verdict(
            unique_tournament_id=16,
            season_id=58210,
            status_type="inprogress",
            endpoint_pattern="/api/v1/event/{event_id}/incidents",
        )

        self.assertEqual(verdict, EndpointVerdict.ALLOWED, "Specific row wins over UT-level fallback")


class RegistryInvalidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_invalidate_clears_cache_entries_for_quad(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
        )

        backend = _FakeRedisBackend()
        repo = _FakeRepository()
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=repo,
        )

        # Seed cache with two endpoints for (UT, season, status).
        backend.set(
            registry._cache_key(
                unique_tournament_id=17, season_id=61643,
                status_type="inprogress",
                endpoint_pattern="/api/v1/event/{event_id}/incidents",
            ),
            "allowed", ex=3600,
        )
        backend.set(
            registry._cache_key(
                unique_tournament_id=17, season_id=61643,
                status_type="inprogress",
                endpoint_pattern="/api/v1/event/{event_id}/statistics",
            ),
            "disabled", ex=3600,
        )

        await registry.invalidate_quad(
            unique_tournament_id=17, season_id=61643, status_type="inprogress",
            endpoint_patterns=(
                "/api/v1/event/{event_id}/incidents",
                "/api/v1/event/{event_id}/statistics",
            ),
        )

        self.assertEqual(len(backend.delete_calls), 2)
        self.assertEqual(backend.values, {}, "Both keys must be evicted.")


class RegistryKeyShapeTests(unittest.TestCase):
    def test_cache_key_includes_quad_components(self) -> None:
        """Pin the cache key shape so we can XSCAN / debug operationally."""
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
        )
        backend = _FakeRedisBackend()
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=_FakeRepository(),
        )
        key = registry._cache_key(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_pattern="/api/v1/event/{event_id}/incidents",
        )
        self.assertTrue(key.startswith("lcap:"))
        self.assertIn("17", key)
        self.assertIn("61643", key)
        self.assertIn("inprogress", key)

    def test_cache_key_for_ut_level_fallback_uses_zero_season(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
        )
        backend = _FakeRedisBackend()
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=_FakeRepository(),
        )
        key = registry._cache_key(
            unique_tournament_id=16,
            season_id=None,
            status_type="finished",
            endpoint_pattern="/api/v1/event/{event_id}/highlights",
        )
        # Convention: season_id=None → "0" sentinel (NULL in Postgres,
        # 0 in Redis key space) so cache lookups can distinguish UT-level
        # rows from per-season rows.
        self.assertIn(":0:", key)


if __name__ == "__main__":
    unittest.main()
