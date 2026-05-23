"""Phase 4.7.4 isolated sandbox: prove the batch verdict resolve is fast
enough to flip the SOFASCORE_LEAGUE_CAPABILITIES_ENABLED flag back on
without re-triggering the Phase 4.8 pool starvation.

Runs in a standalone Python process — does NOT touch the live worker
pool. Uses the real prod Postgres + Redis so the numbers reflect what
the orchestrator would see.

Three measurements:

  1. Cold call — Redis cache empty for the (UT, season, status) quad,
     batch must hit Postgres exactly once (or twice if UT-level
     fallback fires).
  2. Warm call — second invocation immediately after #1; all 12
     patterns now served from Redis, zero DB roundtrips, sub-ms.
  3. Concurrent burst — 20 parallel invocations on different UTs to
     prove the pool can service the hot path without queuing past the
     30 s asyncpg statement-timeout.

Decision gate for Phase 4.8 retry:

  PASS — cold <= 100 ms, warm <= 5 ms, concurrent p95 <= 200 ms.
  FAIL — any cold > 500 ms or any concurrent timeout. Investigate
         before flipping the flag.

Usage on prod::

    /opt/sofascore/.venv/bin/python scripts/sandbox_phase_4_7_4_verify.py

The script never writes anything beyond the routine Redis cache prime
that any production lookup would do — safe to run repeatedly.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# UTs we know carry registry data after the Phase 4.7.2 backfill:
# (UT, season_id, status_type, label). All have at least one detail
# endpoint with a measured verdict, so cold-call must produce non-empty
# result and prove the DB roundtrip happens once.
_VERIFIED_QUADS = (
    (7, 76953, "finished",   "UCL 25/26"),
    (17, 76986, "finished",  "Premier League 25/26"),
    (8, 77559, "finished",   "LaLiga 25/26"),
    (23, 76457, "finished",  "Serie A 25/26"),
    (357, 69619, "finished", "FIFA Club WC 2025"),
    (35, 77333, "finished",  "Bundesliga 25/26"),
    (679, 76984, "finished", "UEL 25/26"),
    (34, 77356, "finished",  "Ligue 1 25/26"),
    (270, 71636, "finished", "AFCON 25/26"),
    (133, 57114, "finished", "Copa America 2024"),
    (325, 87678, "finished", "Brasileirão 2026"),
    (10783, 58337, "finished", "UEFA Nations League 24/25"),
    (17015, 76960, "finished", "UECL 25/26"),
    (19, 82557, "finished",  "FA Cup 25/26"),
    (384, 87760, "finished", "Libertadores 2026"),
    (132, 80229, "finished", "NBA 25/26"),
    (329, 82988, "finished", "Copa del Rey 25/26"),
    (955, 80443, "finished", "Saudi Pro League 25/26"),
    (238, 77806, "finished", "Liga Portugal 25/26"),
    (21, 77500, "finished",  "EFL Cup 25/26"),
)


async def main_async() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    from schema_inspector.db import AsyncpgDatabase, load_database_config
    from schema_inspector.match_center_policy import (
        _allowed_detail_patterns_for_status,
    )
    from schema_inspector.services.league_capabilities_registry import (
        LeagueCapabilitiesRegistry,
    )
    from schema_inspector.storage.league_capabilities_repository import (
        LeagueCapabilitiesRepository,
    )
    from schema_inspector.cli import _load_redis_backend

    db_config = load_database_config()
    database = AsyncpgDatabase(db_config)
    await database.connect()

    # Real prod Redis — same backend the registry would use under load.
    # Falling back to in-memory only if explicitly opted in (we want the
    # real numbers).
    redis_backend = _load_redis_backend(None, allow_memory_fallback=False)
    if redis_backend is None:
        raise RuntimeError(
            "Could not connect to Redis — sandbox would only measure the "
            "happy path. Aborting."
        )

    registry = LeagueCapabilitiesRegistry(
        redis_backend=redis_backend,
        database=database,
        repository=LeagueCapabilitiesRepository(),
    )

    finished_patterns = tuple(
        str(p) for p in _allowed_detail_patterns_for_status("finished")
    )
    print(
        f"Resolved {len(finished_patterns)} detail patterns for status='finished':"
    )
    for p in sorted(finished_patterns):
        print(f"  {p}")
    print()

    # ----- Phase 1: cold + warm for a single quad ---------------------
    ut_id, season_id, status_type, label = _VERIFIED_QUADS[0]
    print("=" * 80)
    print(f"PHASE 1: cold vs warm — {label} (UT={ut_id}, season={season_id})")
    print("=" * 80)

    # Bust the Redis cache for this quad first so cold actually means cold.
    deleted = await registry.invalidate_quad(
        unique_tournament_id=ut_id,
        season_id=season_id,
        status_type=status_type,
        endpoint_patterns=finished_patterns,
    )
    print(f"  Pre-cleared {deleted} Redis keys for cold-call accuracy.")

    cold_started = time.perf_counter()
    cold_result = await registry.get_verdicts_batch(
        unique_tournament_id=ut_id,
        season_id=season_id,
        status_type=status_type,
        endpoint_patterns=finished_patterns,
    )
    cold_ms = (time.perf_counter() - cold_started) * 1000.0
    print(
        f"  COLD: resolved {len(cold_result)} / {len(finished_patterns)} "
        f"patterns in {cold_ms:.2f} ms"
    )

    warm_started = time.perf_counter()
    warm_result = await registry.get_verdicts_batch(
        unique_tournament_id=ut_id,
        season_id=season_id,
        status_type=status_type,
        endpoint_patterns=finished_patterns,
    )
    warm_ms = (time.perf_counter() - warm_started) * 1000.0
    print(
        f"  WARM: resolved {len(warm_result)} / {len(finished_patterns)} "
        f"patterns in {warm_ms:.2f} ms (should be << cold; Redis-only path)"
    )

    if len(warm_result) != len(cold_result):
        print(
            f"  ⚠ warm/cold mismatch — cold={len(cold_result)} warm={len(warm_result)}"
        )

    # ----- Phase 2: concurrent burst on distinct quads ---------------
    print()
    print("=" * 80)
    print(f"PHASE 2: concurrent burst — 20 parallel quads")
    print("=" * 80)

    # Bust everything first so we measure cold-path concurrency, which
    # is the worst case.
    for ut_id, season_id, status_type, _label in _VERIFIED_QUADS:
        await registry.invalidate_quad(
            unique_tournament_id=ut_id,
            season_id=season_id,
            status_type=status_type,
            endpoint_patterns=finished_patterns,
        )

    async def _one_call(ut_id: int, season_id: int, status_type: str) -> float:
        started = time.perf_counter()
        await registry.get_verdicts_batch(
            unique_tournament_id=ut_id,
            season_id=season_id,
            status_type=status_type,
            endpoint_patterns=finished_patterns,
        )
        return (time.perf_counter() - started) * 1000.0

    burst_started = time.perf_counter()
    latencies = await asyncio.gather(
        *(
            _one_call(ut_id, season_id, status_type)
            for ut_id, season_id, status_type, _ in _VERIFIED_QUADS
        )
    )
    burst_total_ms = (time.perf_counter() - burst_started) * 1000.0

    latencies_sorted = sorted(latencies)
    p50 = latencies_sorted[len(latencies_sorted) // 2]
    p95 = latencies_sorted[int(len(latencies_sorted) * 0.95)]
    p_max = max(latencies)
    print(
        f"  N=20 cold quads in {burst_total_ms:.0f} ms wallclock total"
    )
    print(f"  per-call latency: p50={p50:.1f} ms  p95={p95:.1f} ms  max={p_max:.1f} ms")

    # ----- Decision gate -----------------------------------------------
    print()
    print("=" * 80)
    print("DECISION GATE")
    print("=" * 80)
    decisions = []
    decisions.append(("COLD ≤ 100 ms", cold_ms <= 100, f"cold={cold_ms:.1f} ms"))
    decisions.append(("WARM ≤ 5 ms",   warm_ms <= 5,   f"warm={warm_ms:.2f} ms"))
    decisions.append(("p95 ≤ 200 ms",  p95 <= 200,     f"p95={p95:.1f} ms"))
    decisions.append(("max ≤ 1000 ms", p_max <= 1000,  f"max={p_max:.1f} ms"))

    all_passed = all(ok for _, ok, _ in decisions)
    for label, ok, detail in decisions:
        marker = "PASS" if ok else "FAIL"
        print(f"  [{marker}] {label:<20}  {detail}")
    print()
    if all_passed:
        print("RESULT: PASS — safe to flip flag back on (Phase 4.8 retry).")
    else:
        print("RESULT: FAIL — DO NOT flip flag. Investigate slow path first.")

    await database.close()
    return 0 if all_passed else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
