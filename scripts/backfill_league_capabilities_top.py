"""Phase 4.7.2 backfill (2026-05-23): probe finished cohorts for the top-N
unique tournaments by Sofascore-side popularity (``user_count``).

Pre-populates the ``league_endpoint_capability`` table so that when
``SOFASCORE_LEAGUE_CAPABILITIES_ENABLED`` is flipped on in production, the
PilotOrchestrator gate immediately has measured verdicts instead of falling
through to legacy tier-based logic for every UT (which is the same as the
flag being off, just with extra Redis traffic).

Strategy
--------

  1. Pick top-N unique tournaments by ``user_count DESC NULLS LAST``.
     Empirically this surfaces UCL, EPL, LaLiga, FIFA WC, Serie A,
     Bundesliga, Ligue 1, etc. up top — same league shape we manually
     prioritised in earlier backfill phases.
  2. For each UT, pick the *most recent* season whose catalog row has
     ``bootstrap_state IN ('events_loaded', 'fully_processed')`` —
     meaning the events table actually contains rows for that season, so
     ProbeExecutor._select_sample_events has data to draw from. UTs whose
     catalog hasn't reached ``events_loaded`` yet come back with
     ``season_id IS NULL`` and are skipped here (the refresh daemon in
     Phase 4.5 will pick them up later).
  3. Probe each (UT, season, ``finished``) cohort. By default 12 endpoint
     patterns × 5 samples = up to 60 HTTP calls per cohort. Top-50 × 60 ≈
     3000 HTTP calls in one run.

Usage::

    /opt/sofascore/.venv/bin/python scripts/backfill_league_capabilities_top.py \\
        --limit 50 --status-type finished --samples 5

Use ``--dry-run`` first to see which (UT, season) pairs would be probed
without issuing any HTTP calls.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# fmt: off
_TOP_UT_QUERY = """
WITH top_uts AS (
    SELECT
        ut.id           AS unique_tournament_id,
        ut.name         AS name,
        ut.user_count   AS user_count
    FROM unique_tournament ut
    WHERE ut.user_count IS NOT NULL
    ORDER BY ut.user_count DESC NULLS LAST
    LIMIT $1
),
ranked_seasons AS (
    SELECT
        c.unique_tournament_id,
        c.season_id,
        c.season_year,
        c.events_loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY c.unique_tournament_id
            ORDER BY COALESCE(c.events_loaded_at, '1970-01-01'::timestamptz) DESC,
                     c.season_id DESC
        ) AS rn
    FROM tournament_season_upstream_catalog c
    WHERE c.bootstrap_state IN ('events_loaded', 'fully_processed')
)
SELECT
    t.unique_tournament_id,
    t.name,
    t.user_count,
    rs.season_id,
    rs.season_year
FROM top_uts t
LEFT JOIN ranked_seasons rs
    ON rs.unique_tournament_id = t.unique_tournament_id
   AND rs.rn = 1
ORDER BY t.user_count DESC NULLS LAST
"""
# fmt: on


async def fetch_top_uts_with_seasons(database, *, limit: int) -> list[dict]:
    """Return a list of ``{unique_tournament_id, name, user_count,
    season_id, season_year}`` rows. ``season_id`` is NULL for UTs whose
    catalog has no ``events_loaded``+ row yet — caller skips those."""
    async with database.connection() as conn:
        rows = await conn.fetch(_TOP_UT_QUERY, limit)
    return [dict(r) for r in rows]


# ----- HTTP adapter ---------------------------------------------------------
# Shared with scripts/probe_league_capabilities_sandbox.py — duplicated here
# instead of importing because the sandbox script is meant to stay
# standalone and we don't want a cross-import dance.


class _SofascoreClientAdapter:
    def __init__(self, client, *, timeout: float = 20.0):
        self.client = client
        self.timeout = timeout

    async def get_json(self, *, event_id: int, endpoint_pattern: str):
        from schema_inspector.endpoints import SOFASCORE_BASE_URL
        from schema_inspector.sofascore_client import SofascoreHttpError

        path = endpoint_pattern.replace("{event_id}", str(event_id))
        url = f"{SOFASCORE_BASE_URL}{path}"
        try:
            response = await self.client.get_json(url, timeout=self.timeout)
        except SofascoreHttpError as exc:
            transport = exc.transport_result
            status = (
                int(transport.status_code)
                if transport is not None and transport.status_code is not None
                else 0
            )
            return {"status": status, "payload": None}
        return {"status": int(response.status_code), "payload": response.payload}


class _NoopRedis:
    """Registry needs a Redis backend for cache invalidation. Backfill is
    one-shot; if hot-path readers happen to be running concurrently they'll
    pick up new rows within the 1h TTL window without an explicit DEL."""

    def get(self, key): return None  # noqa: E704
    def set(self, key, value, *, ex=None): return True  # noqa: E704
    def delete(self, key): return 0  # noqa: E704


# ----- main loop ------------------------------------------------------------


async def main_async(args: argparse.Namespace) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    from schema_inspector.db import AsyncpgDatabase, load_database_config
    from schema_inspector.runtime import load_runtime_config
    from schema_inspector.sofascore_client import SofascoreClient
    from schema_inspector.services.league_capabilities_probe import (
        ProbeExecutor, PROBE_ENDPOINTS_BY_STATUS,
    )
    from schema_inspector.services.league_capabilities_registry import (
        LeagueCapabilitiesRegistry,
    )
    from schema_inspector.storage.league_capabilities_repository import (
        LeagueCapabilitiesRepository,
    )

    db_config = load_database_config()
    database = AsyncpgDatabase(db_config)
    await database.connect()

    try:
        rows = await fetch_top_uts_with_seasons(database, limit=args.limit)
        ranked_at_least_one_season = sum(1 for r in rows if r["season_id"] is not None)
        print(
            f"Resolved {len(rows)} UTs (top-{args.limit} by user_count), "
            f"{ranked_at_least_one_season} have at least one events_loaded+ "
            f"season."
        )
        print()
        for i, row in enumerate(rows, start=1):
            mark = "  " if row["season_id"] is not None else "??"
            season_blurb = (
                f"season={row['season_id']} ({row['season_year']})"
                if row["season_id"] is not None
                else "season=NONE — skipped (no events_loaded+ catalog row)"
            )
            print(
                f"{mark} {i:>2}. UT={row['unique_tournament_id']:<6} "
                f"users={row['user_count']:>8}  "
                f"{row['name']:<40}  {season_blurb}"
            )

        if args.dry_run:
            print()
            print("[dry-run] — no HTTP, no DB writes.")
            return 0

        endpoint_patterns = PROBE_ENDPOINTS_BY_STATUS.get(args.status_type)
        if endpoint_patterns is None:
            print(f"Unknown status_type: {args.status_type}", file=sys.stderr)
            return 2

        runtime = load_runtime_config()
        raw_client = SofascoreClient(runtime)
        client_adapter = _SofascoreClientAdapter(raw_client, timeout=args.timeout)
        repository = LeagueCapabilitiesRepository()
        registry = LeagueCapabilitiesRegistry(
            redis_backend=_NoopRedis(),
            database=database,
            repository=repository,
        )
        executor = ProbeExecutor(
            database=database,
            client=client_adapter,
            repository=repository,
            registry=registry,
            samples_per_endpoint=args.samples,
        )

        print()
        print("=" * 80)
        print(
            f"Probing top-{args.limit} (status='{args.status_type}', "
            f"samples={args.samples}, endpoints={len(endpoint_patterns)})"
        )
        print("=" * 80)

        summary = {
            "started_at": time.time(),
            "limit": args.limit,
            "status_type": args.status_type,
            "samples": args.samples,
            "cohorts_attempted": 0,
            "cohorts_skipped_no_season": 0,
            "cohorts_skipped_no_events": 0,
            "cohorts_probed": 0,
            "total_upserts": 0,
            "per_endpoint_state_counts": {},
            "per_cohort": [],
        }
        wall_started = time.perf_counter()

        for i, row in enumerate(rows, start=1):
            summary["cohorts_attempted"] += 1
            ut_id = row["unique_tournament_id"]
            ut_name = row["name"]
            season_id = row["season_id"]
            if season_id is None:
                summary["cohorts_skipped_no_season"] += 1
                continue

            cohort_started = time.perf_counter()
            try:
                report = await executor.probe(
                    unique_tournament_id=int(ut_id),
                    season_id=int(season_id),
                    status_type=args.status_type,
                    endpoint_patterns=endpoint_patterns,
                    samples_per_endpoint=args.samples,
                )
            except Exception as exc:  # noqa: BLE001
                cohort_elapsed = time.perf_counter() - cohort_started
                print(
                    f"[{i:>2}/{len(rows)}] UT={ut_id:<6} {ut_name:<40} "
                    f"season={season_id}  ERROR {type(exc).__name__}: {exc} "
                    f"(elapsed {cohort_elapsed:.1f}s)"
                )
                summary["per_cohort"].append(
                    {
                        "unique_tournament_id": ut_id,
                        "season_id": season_id,
                        "name": ut_name,
                        "error": f"{type(exc).__name__}: {exc}",
                        "elapsed_seconds": cohort_elapsed,
                    }
                )
                continue

            cohort_elapsed = time.perf_counter() - cohort_started
            if report.samples_used == 0:
                summary["cohorts_skipped_no_events"] += 1
                print(
                    f"[{i:>2}/{len(rows)}] UT={ut_id:<6} {ut_name:<40} "
                    f"season={season_id}  no sample events for "
                    f"status='{args.status_type}' — SKIP"
                )
                continue

            summary["cohorts_probed"] += 1
            summary["total_upserts"] += int(report.upserts_count)
            for ep, state in report.by_endpoint.items():
                summary["per_endpoint_state_counts"].setdefault(
                    ep, {"allowed": 0, "disabled": 0, "unknown": 0}
                )
                if state in summary["per_endpoint_state_counts"][ep]:
                    summary["per_endpoint_state_counts"][ep][state] += 1
            allowed_n = sum(1 for s in report.by_endpoint.values() if s == "allowed")
            disabled_n = sum(1 for s in report.by_endpoint.values() if s == "disabled")
            print(
                f"[{i:>2}/{len(rows)}] UT={ut_id:<6} {ut_name:<40} "
                f"season={season_id}  ALLOWED={allowed_n:>2} DISABLED={disabled_n:>2} "
                f"samples={report.samples_used}  ({cohort_elapsed:.1f}s)"
            )
            summary["per_cohort"].append(
                {
                    "unique_tournament_id": ut_id,
                    "season_id": season_id,
                    "name": ut_name,
                    "samples_used": int(report.samples_used),
                    "upserts": int(report.upserts_count),
                    "by_endpoint": dict(report.by_endpoint),
                    "elapsed_seconds": cohort_elapsed,
                }
            )

            if args.cohort_pause_seconds > 0:
                await asyncio.sleep(args.cohort_pause_seconds)

        summary["elapsed_seconds"] = time.perf_counter() - wall_started
        summary["finished_at"] = time.time()

        print()
        print("=" * 80)
        print(
            f"FINISHED  cohorts: probed={summary['cohorts_probed']}  "
            f"skipped_no_season={summary['cohorts_skipped_no_season']}  "
            f"skipped_no_events={summary['cohorts_skipped_no_events']}  "
            f"errors={sum(1 for c in summary['per_cohort'] if 'error' in c)}"
        )
        print(f"  total upserts written: {summary['total_upserts']}")
        print(f"  wallclock elapsed    : {summary['elapsed_seconds']:.1f}s")
        print()
        print("AGGREGATE per-endpoint verdicts (across probed cohorts):")
        for ep in endpoint_patterns:
            counts = summary["per_endpoint_state_counts"].get(
                ep, {"allowed": 0, "disabled": 0, "unknown": 0}
            )
            print(
                f"  {ep:<60}  "
                f"allowed={counts['allowed']:>3}  "
                f"disabled={counts['disabled']:>3}  "
                f"unknown={counts['unknown']:>3}"
            )

        if args.json_out:
            Path(args.json_out).write_text(
                json.dumps(summary, indent=2, default=str), encoding="utf-8"
            )
            print(f"\nJSON summary: {args.json_out}")

        return 0
    finally:
        await database.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit", type=int, default=50,
        help="Top-N unique tournaments by user_count to probe (default 50).",
    )
    parser.add_argument(
        "--status-type",
        choices=("finished", "inprogress", "notstarted"),
        default="finished",
        help="Match status_type to probe. Default 'finished'.",
    )
    parser.add_argument(
        "--samples", type=int, default=5,
        help="Sample events per endpoint pattern (default 5).",
    )
    parser.add_argument(
        "--timeout", type=float, default=20.0,
        help="HTTP timeout per request in seconds (default 20).",
    )
    parser.add_argument(
        "--cohort-pause-seconds", type=float, default=1.0,
        help="Pause between cohorts in seconds (default 1) to avoid bursting.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the resolved UT/season selection and exit without HTTP.",
    )
    parser.add_argument(
        "--json-out", default=None,
        help="Path to write per-cohort + aggregate JSON summary.",
    )
    return parser.parse_args()


def main() -> int:
    return asyncio.run(main_async(_parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
