"""CLI to repair missing rounds in event/event_round_info tables.

Backfill bug discovered 2026-05-26: ``event_list_job.run_round`` may have
been incomplete for some seasons, leaving some round_numbers missing in
``event_round_info`` even though ``season_round`` catalog and upstream
Sofascore both have them. Symptom on the frontend: ``/events/last/{page}``
synthesised from ``event`` table skips entire matchdays.

This CLI:
  1. Queries ``season_round`` catalog and compares against actually
     ingested ``event_round_info.round_number`` rows for one
     ``(unique_tournament_id, season_id)`` pair.
  2. Lists missing rounds.
  3. With ``--dry-run`` — exits after print (default).
  4. Without ``--dry-run`` — invokes ``EventListIngestJob.run_round``
     for each missing round, with a configurable inter-call sleep so
     upstream Sofascore proxy budget isn't burned in a burst.

Safe to run repeatedly: ``run_round`` upserts and is idempotent. Existing
data is left untouched; only previously-missing rounds get filled in.

Usage::

    # Inspect what's missing without touching anything (recommended first):
    python -m schema_inspector.repair_season_rounds_cli \\
        --unique-tournament-id 17 --season-id 76986 --dry-run

    # Actually repair:
    python -m schema_inspector.repair_season_rounds_cli \\
        --unique-tournament-id 17 --season-id 76986 --sport-slug football

    # Cap how many rounds get fetched per invocation (useful on a strained
    # cluster where you want to drip-feed):
    python -m schema_inspector.repair_season_rounds_cli \\
        --unique-tournament-id 17 --season-id 76986 --sport-slug football \\
        --max-rounds 3 --sleep-seconds 5
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any, Sequence

from .db import AsyncpgDatabase, load_database_config
from .runtime import load_runtime_config
from .sources import build_source_adapter

logger = logging.getLogger(__name__)


_FIND_MISSING_ROUNDS_QUERY = """
SELECT sr.round_number
FROM season_round sr
WHERE sr.unique_tournament_id = $1
  AND sr.season_id = $2
  AND NOT EXISTS (
      SELECT 1
      FROM event e
      JOIN event_round_info eri ON eri.event_id = e.id
      WHERE e.unique_tournament_id = sr.unique_tournament_id
        AND e.season_id = sr.season_id
        AND eri.round_number = sr.round_number
  )
ORDER BY sr.round_number
"""


_CATALOG_SUMMARY_QUERY = """
SELECT
    COUNT(*) AS catalog_rounds,
    MIN(round_number) AS min_round,
    MAX(round_number) AS max_round
FROM season_round
WHERE unique_tournament_id = $1 AND season_id = $2
"""


_EVENTS_SUMMARY_QUERY = """
SELECT
    COUNT(*) AS event_count,
    COUNT(DISTINCT eri.round_number) AS distinct_rounds
FROM event e
LEFT JOIN event_round_info eri ON eri.event_id = e.id
WHERE e.unique_tournament_id = $1 AND e.season_id = $2
"""


async def find_missing_rounds(
    connection: Any,
    *,
    unique_tournament_id: int,
    season_id: int,
) -> list[int]:
    """Return the sorted list of round_numbers that are in the catalog
    (``season_round``) but have zero events in ``event_round_info``.
    """
    rows = await connection.fetch(
        _FIND_MISSING_ROUNDS_QUERY,
        int(unique_tournament_id),
        int(season_id),
    )
    return [int(row["round_number"]) for row in rows]


async def collect_summary(
    connection: Any,
    *,
    unique_tournament_id: int,
    season_id: int,
) -> dict[str, Any]:
    """Catalog/event counts used in the printed banner."""
    catalog_row = await connection.fetchrow(
        _CATALOG_SUMMARY_QUERY,
        int(unique_tournament_id),
        int(season_id),
    )
    events_row = await connection.fetchrow(
        _EVENTS_SUMMARY_QUERY,
        int(unique_tournament_id),
        int(season_id),
    )
    return {
        "catalog_rounds": int(catalog_row["catalog_rounds"]) if catalog_row else 0,
        "min_round": catalog_row["min_round"] if catalog_row else None,
        "max_round": catalog_row["max_round"] if catalog_row else None,
        "event_count": int(events_row["event_count"]) if events_row else 0,
        "distinct_rounds": int(events_row["distinct_rounds"]) if events_row else 0,
    }


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description=(
            "Detect and refill missing rounds in event_round_info for a "
            "given (unique_tournament_id, season_id). Idempotent — already "
            "ingested rounds are not re-fetched."
        ),
    )
    parser.add_argument("--unique-tournament-id", type=int, required=True)
    parser.add_argument("--season-id", type=int, required=True)
    parser.add_argument(
        "--sport-slug",
        default="football",
        help="Sport slug for the event-list adapter (default: football).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print missing rounds without fetching.",
    )
    parser.add_argument(
        "--max-rounds",
        type=int,
        default=None,
        help=(
            "Cap how many missing rounds to fetch in this invocation. "
            "Use to drip-feed on a strained cluster. Default: fetch all."
        ),
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=2.0,
        help="Sleep between successive run_round calls (default 2 s).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP request timeout in seconds for each round fetch.",
    )
    parser.add_argument(
        "--proxy",
        action="append",
        default=[],
        help="Optional proxy URL. Repeatable.",
    )
    parser.add_argument("--user-agent", default=None)
    parser.add_argument("--max-attempts", type=int, default=None)
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--db-min-size", type=int, default=None)
    parser.add_argument("--db-max-size", type=int, default=None)
    parser.add_argument("--db-timeout", type=float, default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
    )
    return asyncio.run(_run(args))


async def _run(args: argparse.Namespace) -> int:
    database_config = load_database_config(
        dsn=args.database_url,
        min_size=args.db_min_size,
        max_size=args.db_max_size,
        command_timeout=args.db_timeout,
    )

    async with AsyncpgDatabase(database_config) as database:
        async with database.connection() as connection:
            summary = await collect_summary(
                connection,
                unique_tournament_id=args.unique_tournament_id,
                season_id=args.season_id,
            )
            missing = await find_missing_rounds(
                connection,
                unique_tournament_id=args.unique_tournament_id,
                season_id=args.season_id,
            )

        print(
            f"=== repair-season-rounds ut={args.unique_tournament_id} "
            f"season={args.season_id} ==="
        )
        print(
            f"catalog rounds in season_round:  {summary['catalog_rounds']} "
            f"(min={summary['min_round']}, max={summary['max_round']})"
        )
        print(
            f"already in event_round_info:     {summary['distinct_rounds']} "
            f"distinct rounds, {summary['event_count']} events total"
        )
        print(f"missing rounds ({len(missing)}):           {missing}")

        if not missing:
            print("Nothing to repair — all catalog rounds already have events.")
            return 0

        if args.dry_run:
            print("--dry-run set; exiting without fetching.")
            return 0

        capped_missing: Sequence[int] = missing
        if args.max_rounds is not None and args.max_rounds < len(missing):
            capped_missing = missing[: int(args.max_rounds)]
            print(
                f"--max-rounds={args.max_rounds}; fetching the first "
                f"{len(capped_missing)} of {len(missing)} missing rounds."
            )

        # Initialise the event-list job once and reuse it for all calls so
        # the asyncpg pool + ParserRegistry are warmed up only once.
        runtime_config = load_runtime_config(
            proxy_urls=args.proxy,
            user_agent=args.user_agent,
            max_attempts=args.max_attempts,
        )
        adapter = build_source_adapter(
            runtime_config.source_slug,
            runtime_config=runtime_config,
        )
        job = adapter.build_event_list_job(database)

        total_written_events = 0
        total_written_snapshots = 0
        successes: list[int] = []
        failures: list[tuple[int, str]] = []

        for index, round_number in enumerate(capped_missing):
            print(
                f"[{index + 1}/{len(capped_missing)}] fetching round={round_number} ..."
            )
            try:
                result = await job.run_round(
                    args.unique_tournament_id,
                    args.season_id,
                    int(round_number),
                    sport_slug=args.sport_slug,
                    timeout=args.timeout,
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.error(
                    "repair_season_rounds: run_round failed ut=%s season=%s round=%s err=%r",
                    args.unique_tournament_id,
                    args.season_id,
                    round_number,
                    exc,
                )
                failures.append((round_number, repr(exc)))
            else:
                events = int(getattr(result.written, "event_rows", 0) or 0)
                snapshots = int(getattr(result.written, "payload_snapshot_rows", 0) or 0)
                total_written_events += events
                total_written_snapshots += snapshots
                successes.append(round_number)
                print(
                    f"  → events={events} snapshots={snapshots} "
                    f"job={result.job_name}"
                )

            # Drip-feed: sleep before next round, but not after the last one.
            if index < len(capped_missing) - 1 and args.sleep_seconds > 0:
                await asyncio.sleep(args.sleep_seconds)

        print("")
        print(
            f"=== summary: ok={len(successes)} failed={len(failures)} "
            f"events_written={total_written_events} "
            f"snapshots={total_written_snapshots} ==="
        )
        if failures:
            for round_number, err in failures:
                print(f"  fail round={round_number}: {err}")
            return 2
        return 0
