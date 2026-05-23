"""Phase 4.7.4 (2026-05-23): batch resolve to fix pool starvation.

Phase 4.8 production flip exposed a critical regression: workers' hot path
hit ``TimeoutError`` (30 s asyncpg statement-timeout) because
``PilotOrchestrator._resolve_detail_capability_verdicts`` issued 12
*sequential* ``await registry.get_verdict(...)`` calls per match-center
fetch. With ~70 workers running concurrently, the pool (size ~10) couldn't
service the demand — every worker spent most of its time waiting for a
free connection. Resulting throughput dropped 17–20×, slow-job (>25 s)
rate spiked to 400+/min.

The fix collapses 12 sequential lookups into:

  * One bulk Redis read (sync, no I/O wait).
  * At most one async Postgres roundtrip for the missing patterns of a
    (UT, season, status) quad — using a single ``WHERE`` clause that
    fetches all rows for the quad in one go.
  * At most one more Postgres roundtrip for the (UT, season=NULL, status)
    UT-level fallback for whatever still missed.

So in the worst case a worker holds a DB connection for two queries
instead of twelve.

This file covers the new batch API end-to-end:

  1. ``LeagueCapabilitiesRepository.fetch_capabilities_for_quad`` —
     single SELECT for one (UT, season, status) triple.
  2. ``LeagueCapabilitiesRegistry.get_verdicts_batch`` — Redis-first
     bulk read with single DB fallback + UT-level fallback; primes cache.
  3. ``PilotOrchestrator._resolve_detail_capability_verdicts`` —
     refactored to one ``get_verdicts_batch`` call instead of a per-
     pattern loop.

All tests are pure-Python with fake Redis + fake repository + fake DB.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock


# ----- Fakes ----------------------------------------------------------------


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.get_calls: list[str] = []
        self.set_calls: list[tuple[str, str, int | None]] = []

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


class _FakeBatchRepository:
    """Repository fake exposing the new ``fetch_capabilities_for_quad``."""

    def __init__(self, rows=None) -> None:
        self._rows = list(rows or [])
        self.fetch_quad_calls: list[tuple] = []

    async def fetch_capabilities_for_quad(
        self,
        executor,
        *,
        unique_tournament_id,
        season_id,
        status_type,
    ):
        self.fetch_quad_calls.append(
            (unique_tournament_id, season_id, status_type)
        )
        return [
            row for row in self._rows
            if row.unique_tournament_id == unique_tournament_id
            and row.season_id == season_id
            and row.status_type == status_type
        ]


class _FakeExecutor:
    async def __aenter__(self): return self  # noqa: E704
    async def __aexit__(self, *args): return False  # noqa: E704


class _FakeDatabase:
    def __init__(self) -> None:
        self._conn = _FakeExecutor()

    def connection(self):
        return self._conn


def _row(*, ut=17, season=61643, status="inprogress", endpoint, state="allowed"):
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


# ----- Repository batch SELECT ----------------------------------------------


class FetchCapabilitiesForQuadTests(unittest.IsolatedAsyncioTestCase):
    """The new repository method is the single SQL roundtrip that replaces
    the 12 per-pattern ``fetch_capability`` calls."""

    async def test_method_exists_on_repository(self) -> None:
        from schema_inspector.storage.league_capabilities_repository import (
            LeagueCapabilitiesRepository,
        )
        self.assertTrue(
            hasattr(LeagueCapabilitiesRepository, "fetch_capabilities_for_quad"),
            "Phase 4.7.4 introduces fetch_capabilities_for_quad on repository",
        )

    async def test_signature_kwargs_only_quad_no_pattern_list(self) -> None:
        import inspect
        from schema_inspector.storage.league_capabilities_repository import (
            LeagueCapabilitiesRepository,
        )
        sig = inspect.signature(
            LeagueCapabilitiesRepository.fetch_capabilities_for_quad
        )
        params = list(sig.parameters)
        self.assertEqual(params[0], "self")
        # The executor (asyncpg connection) is the first positional arg.
        self.assertEqual(params[1], "executor")
        # Quad is keyword-only — no per-pattern filter (entire quad is
        # already small, no need for IN-list complexity).
        self.assertIn("unique_tournament_id", params)
        self.assertIn("season_id", params)
        self.assertIn("status_type", params)
        self.assertNotIn(
            "endpoint_pattern", params,
            "intentionally NO endpoint filter — fetching the whole quad "
            "is cheaper than building an IN-list",
        )
        self.assertNotIn("endpoint_patterns", params)

    async def test_emits_single_sql_select_for_quad(self) -> None:
        """One SELECT, one WHERE quad. The captured query must mention all
        three filter columns and NOT contain an IN (...) over endpoint
        patterns — the whole point of the rewrite."""
        from schema_inspector.storage.league_capabilities_repository import (
            LeagueCapabilitiesRepository,
        )
        captured: list[tuple] = []

        class _CapturingExecutor:
            async def fetch(self, query, *args):
                captured.append((query, args))
                return []

        repo = LeagueCapabilitiesRepository()
        result = await repo.fetch_capabilities_for_quad(
            _CapturingExecutor(),
            unique_tournament_id=17,
            season_id=61643,
            status_type="finished",
        )
        self.assertEqual(len(captured), 1, "exactly one SQL roundtrip")
        query, args = captured[0]
        self.assertIn("unique_tournament_id", query)
        self.assertIn("season_id", query)
        self.assertIn("status_type", query)
        self.assertNotIn("endpoint_pattern IN", query)
        self.assertEqual(args, (17, 61643, "finished"))
        self.assertEqual(result, [])


# ----- Registry batch verdict resolve ---------------------------------------


class GetVerdictsBatchCacheTests(unittest.IsolatedAsyncioTestCase):
    """Bulk Redis read first; touch DB only for the patterns that missed."""

    async def test_all_cache_hits_no_db_call(self) -> None:
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry, EndpointVerdict,
        )
        backend = _FakeRedisBackend()
        repo = _FakeBatchRepository()
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=repo,
        )
        patterns = (
            "/api/v1/event/{event_id}/managers",
            "/api/v1/event/{event_id}/h2h",
        )
        for p in patterns:
            key = registry._cache_key(
                unique_tournament_id=17,
                season_id=61643,
                status_type="inprogress",
                endpoint_pattern=p,
            )
            backend.set(key, "allowed", ex=3600)
        backend.set_calls.clear()  # ignore prep

        result = await registry.get_verdicts_batch(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_patterns=patterns,
        )

        self.assertEqual(
            result,
            {p: EndpointVerdict.ALLOWED for p in patterns},
        )
        self.assertEqual(
            repo.fetch_quad_calls, [],
            "all-Redis-hit path must not query Postgres",
        )

    # Phase 4.7.5 (2026-05-23): test_cache_miss_makes_single_batch_db_call,
    # test_batch_db_results_prime_redis_cache, and
    # test_ut_level_fallback_done_in_second_batch_call were deleted here
    # because Phase 4.7.5 removed the per-quad and UT-level DB fallback
    # paths from ``get_verdicts_batch``. The new contract is Redis-only:
    # ``warm_cache_from_db`` primes everything at startup, and
    # ``get_verdicts_batch`` only reads Redis. The deleted assertions
    # would force the removed fallback back into the hot path — exactly
    # the architecture that caused the Phase 4.8 production rollback.
    # The Redis-only contract is now covered in
    # ``test_phase_4_7_5_prewarm.py::HotPathRedisOnlyTests``.

    async def test_patterns_with_no_row_omitted_from_dict(self) -> None:
        """Patterns with neither Redis cache entry nor DB row don't go
        into the returned dict — caller treats absence as 'fall back to
        legacy'. Putting EndpointVerdict.UNKNOWN here would force the
        gate to short-circuit on 'unknown', which is wrong semantics."""
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry,
        )
        backend = _FakeRedisBackend()
        repo = _FakeBatchRepository(rows=[])  # nothing in DB
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=repo,
        )

        result = await registry.get_verdicts_batch(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_patterns=("/api/v1/event/{event_id}/anything",),
        )
        self.assertEqual(result, {})

    async def _deleted_in_phase_4_7_5_ut_level_fallback(self) -> None:
        """Phase 4.7.5 removed UT-level DB fallback from
        get_verdicts_batch. Kept as a stub (the _deleted_ prefix
        prevents unittest discovery) so the test file remains a faithful
        history of the contract evolution.

        Original assertion: second batch DB call fires when season-
        specific quad misses. New contract: no DB calls on the hot path
        at all; ``warm_cache_from_db`` loads everything at startup."""
        from schema_inspector.services.league_capabilities_registry import (
            LeagueCapabilitiesRegistry, EndpointVerdict,
        )
        ut_level_rows = [
            _row(season=None, endpoint="/api/v1/event/{event_id}/h2h", state="allowed"),
        ]
        backend = _FakeRedisBackend()
        repo = _FakeBatchRepository(rows=ut_level_rows)
        registry = LeagueCapabilitiesRegistry(
            redis_backend=backend,
            database=_FakeDatabase(),
            repository=repo,
        )

        result = await registry.get_verdicts_batch(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_patterns=("/api/v1/event/{event_id}/h2h",),
        )

        self.assertEqual(len(repo.fetch_quad_calls), 2,
                         "season-specific call + UT-level fallback call")
        self.assertEqual(repo.fetch_quad_calls[0], (17, 61643, "inprogress"))
        self.assertEqual(repo.fetch_quad_calls[1], (17, None, "inprogress"))
        self.assertEqual(
            result,
            {"/api/v1/event/{event_id}/h2h": EndpointVerdict.ALLOWED},
        )


# ----- Orchestrator uses batch helper instead of per-pattern loop ----------


class OrchestratorBatchResolveTests(unittest.IsolatedAsyncioTestCase):
    """The whole point of Phase 4.7.4: the helper must call
    ``registry.get_verdicts_batch`` exactly once, NOT loop and call
    ``registry.get_verdict`` 12 times."""

    def _make_orchestrator(self, *, registry):
        from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator
        orch = PilotOrchestrator.__new__(PilotOrchestrator)
        orch.league_capabilities = registry
        return orch

    async def test_resolve_detail_calls_batch_exactly_once(self) -> None:
        from unittest.mock import patch
        from schema_inspector.services.league_capabilities_registry import (
            EndpointVerdict,
        )

        # Track calls separately to both APIs. The helper must use the
        # batch one and never the per-pattern one.
        fake_registry = SimpleNamespace(
            get_verdict=AsyncMock(side_effect=AssertionError(
                "Phase 4.7.4 regression: orchestrator must not call "
                "per-pattern get_verdict in the detail-resolve hot path"
            )),
            get_verdicts_batch=AsyncMock(return_value={
                "/api/v1/event/{event_id}/comments": EndpointVerdict.DISABLED,
            }),
        )

        orch = self._make_orchestrator(registry=fake_registry)
        with patch.dict("os.environ", {"SOFASCORE_LEAGUE_CAPABILITIES_ENABLED": "true"}):
            result = await orch._resolve_detail_capability_verdicts(
                unique_tournament_id=17,
                season_id=61643,
                status_type="finished",
            )

        self.assertEqual(fake_registry.get_verdict.await_count, 0)
        self.assertEqual(fake_registry.get_verdicts_batch.await_count, 1)
        # The batch call must pass the full status-specific pattern set.
        kwargs = fake_registry.get_verdicts_batch.await_args.kwargs
        self.assertEqual(kwargs["unique_tournament_id"], 17)
        self.assertEqual(kwargs["season_id"], 61643)
        self.assertEqual(kwargs["status_type"], "finished")
        self.assertGreater(
            len(kwargs["endpoint_patterns"]), 1,
            "batch must include the whole status-specific pattern set",
        )
        # Patterns reaching the helper must be a tuple/sequence — sets
        # have unstable iteration order which would defeat any future
        # SQL ORDER BY optimisation.
        self.assertIsInstance(kwargs["endpoint_patterns"], (tuple, list))
        self.assertEqual(
            result["/api/v1/event/{event_id}/comments"], "disabled",
        )

    async def test_resolve_detail_returns_empty_when_flag_off(self) -> None:
        """Backwards-compatible default: helper bails before touching the
        registry when the flag is off (Phase 4.7.3 invariant preserved)."""
        from unittest.mock import patch

        fake_registry = SimpleNamespace(
            get_verdicts_batch=AsyncMock(return_value={"x": "y"}),
        )
        orch = self._make_orchestrator(registry=fake_registry)
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("SOFASCORE_LEAGUE_CAPABILITIES_ENABLED", None)
            result = await orch._resolve_detail_capability_verdicts(
                unique_tournament_id=17,
                season_id=61643,
                status_type="finished",
            )
        self.assertEqual(result, {})
        self.assertEqual(fake_registry.get_verdicts_batch.await_count, 0)


if __name__ == "__main__":
    unittest.main()
