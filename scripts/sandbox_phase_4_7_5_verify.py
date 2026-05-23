"""Phase 4.7.5 isolated sandbox: prove the Redis-only hot path is fast
enough that 73 concurrent workers can hammer it without ANY contention.

Differs from the Phase 4.7.4 sandbox in two ways:

  1. We measure the one-time warm cost (full bulk SELECT + Redis writes)
     separately. The hot-path numbers come AFTER warm — that's the only
     state production will actually experience post-startup.
  2. The concurrent burst is 100 simultaneous calls (vs 20 in 4.7.4)
     to stress-test the Redis-only invariant. Without DB contention, p95
     should stay sub-50 ms even at this concurrency.

Decision gate for Phase 4.8 retry #2:

  PASS — warm ≤ 500 ms (one-time), batch-post-warm ≤ 50 ms,
         100-burst p95 ≤ 100 ms, sustained p95 ≤ 50 ms.

The "sustained" phase runs 30 req/s for 30 s — that's 3x prod's
expected match-center demand. If THIS doesn't blow up, real prod load
won't either.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# Same verified-quad set as Phase 4.7.4 sandbox.
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

    redis_backend = _load_redis_backend(None, allow_memory_fallback=False)
    if redis_backend is None:
        raise RuntimeError("Could not connect to Redis. Aborting.")

    registry = LeagueCapabilitiesRegistry(
        redis_backend=redis_backend,
        database=database,
        repository=LeagueCapabilitiesRepository(),
    )

    finished_patterns = tuple(
        str(p) for p in _allowed_detail_patterns_for_status("finished")
    )
    print(f"Detail patterns: {len(finished_patterns)}")
    print()

    # ----- Phase 1: explicit warm cost ---------------------------------
    print("=" * 80)
    print("PHASE 1: warm_cache_from_db cost (one-time at startup)")
    print("=" * 80)
    started = time.perf_counter()
    primed = await registry.warm_cache_from_db()
    warm_ms = (time.perf_counter() - started) * 1000.0
    print(f"  Primed {primed} Redis keys in {warm_ms:.1f} ms")

    # ----- Phase 2: single post-warm batch -----------------------------
    print()
    print("=" * 80)
    print("PHASE 2: first batch lookup after warm (must be Redis-only)")
    print("=" * 80)
    ut_id, season_id, status_type, label = _VERIFIED_QUADS[0]
    started = time.perf_counter()
    result = await registry.get_verdicts_batch(
        unique_tournament_id=ut_id,
        season_id=season_id,
        status_type=status_type,
        endpoint_patterns=finished_patterns,
    )
    batch_ms = (time.perf_counter() - started) * 1000.0
    print(
        f"  {label}: resolved {len(result)}/{len(finished_patterns)} patterns "
        f"in {batch_ms:.2f} ms"
    )

    # ----- Phase 3: 100-concurrent burst -------------------------------
    print()
    print("=" * 80)
    print("PHASE 3: 100-concurrent burst (5x larger than Phase 4.7.4)")
    print("=" * 80)

    async def _one_call(ut_id: int, season_id: int, status_type: str) -> float:
        started = time.perf_counter()
        await registry.get_verdicts_batch(
            unique_tournament_id=ut_id,
            season_id=season_id,
            status_type=status_type,
            endpoint_patterns=finished_patterns,
        )
        return (time.perf_counter() - started) * 1000.0

    # Cycle through the 20 verified quads 5x to make 100 calls.
    bursts = []
    for i in range(100):
        ut, season, status, _ = _VERIFIED_QUADS[i % len(_VERIFIED_QUADS)]
        bursts.append((ut, season, status))

    burst_started = time.perf_counter()
    latencies = await asyncio.gather(
        *(_one_call(*args) for args in bursts)
    )
    burst_total_ms = (time.perf_counter() - burst_started) * 1000.0

    latencies_sorted = sorted(latencies)
    burst_p50 = latencies_sorted[len(latencies_sorted) // 2]
    burst_p95 = latencies_sorted[int(len(latencies_sorted) * 0.95)]
    burst_max = max(latencies)
    print(
        f"  100 concurrent batches in {burst_total_ms:.1f} ms wallclock"
    )
    print(
        f"  per-call latency: p50={burst_p50:.2f} ms  p95={burst_p95:.2f} ms  "
        f"max={burst_max:.2f} ms"
    )

    # ----- Phase 4: sustained 30 req/s for 30 s ------------------------
    print()
    print("=" * 80)
    print("PHASE 4: sustained 30 req/s for 30s (3x prod expected demand)")
    print("=" * 80)
    sustained_latencies: list[float] = []
    sustained_started = time.perf_counter()
    target_total = 900  # 30 req/s × 30 s
    inter_request_delay = 1.0 / 30.0  # ~33 ms
    for i in range(target_total):
        ut, season, status, _ = _VERIFIED_QUADS[i % len(_VERIFIED_QUADS)]
        latency = await _one_call(ut, season, status)
        sustained_latencies.append(latency)
        await asyncio.sleep(inter_request_delay)
    sustained_total_s = time.perf_counter() - sustained_started

    sustained_sorted = sorted(sustained_latencies)
    s_mean = sum(sustained_latencies) / len(sustained_latencies)
    s_p50 = sustained_sorted[len(sustained_sorted) // 2]
    s_p95 = sustained_sorted[int(len(sustained_sorted) * 0.95)]
    s_p99 = sustained_sorted[int(len(sustained_sorted) * 0.99)]
    s_max = max(sustained_latencies)
    print(
        f"  {target_total} requests in {sustained_total_s:.1f}s "
        f"(actual rate {target_total / sustained_total_s:.1f} req/s)"
    )
    print(
        f"  per-call: mean={s_mean:.2f} ms  p50={s_p50:.2f} ms  "
        f"p95={s_p95:.2f} ms  p99={s_p99:.2f} ms  max={s_max:.2f} ms"
    )

    # ----- Decision gate -----------------------------------------------
    print()
    print("=" * 80)
    print("DECISION GATE")
    print("=" * 80)
    decisions = []
    decisions.append(("WARM ≤ 500 ms",  warm_ms <= 500, f"warm={warm_ms:.1f} ms"))
    decisions.append(("POST-WARM BATCH ≤ 50 ms",
                      batch_ms <= 50, f"batch={batch_ms:.2f} ms"))
    decisions.append(("BURST p95 ≤ 100 ms",
                      burst_p95 <= 100, f"burst_p95={burst_p95:.2f} ms"))
    decisions.append(("BURST max ≤ 500 ms",
                      burst_max <= 500, f"burst_max={burst_max:.2f} ms"))
    decisions.append(("SUSTAINED p95 ≤ 50 ms",
                      s_p95 <= 50, f"sustained_p95={s_p95:.2f} ms"))
    decisions.append(("SUSTAINED p99 ≤ 100 ms",
                      s_p99 <= 100, f"sustained_p99={s_p99:.2f} ms"))
    decisions.append(("SUSTAINED mean ≤ 20 ms",
                      s_mean <= 20, f"sustained_mean={s_mean:.2f} ms"))

    all_passed = all(ok for _, ok, _ in decisions)
    for label, ok, detail in decisions:
        marker = "PASS" if ok else "FAIL"
        print(f"  [{marker}] {label:<28}  {detail}")
    print()
    if all_passed:
        print("RESULT: PASS — Phase 4.8 retry #2 looks safe.")
    else:
        print("RESULT: FAIL — investigate before re-flipping.")

    await database.close()
    return 0 if all_passed else 1


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
