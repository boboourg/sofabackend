"""API latency audit tool (Phase 0 of N4 perf audit, 2026-05-14).

Replays every endpoint in ``endpoint_registry`` with realistic IDs sampled
from the production database, measures TTFB / total time / payload size /
HTTP status, and emits two artefacts:

* ``<output>.csv`` — one row per endpoint with all measurements.
* ``<summary>.md`` — human-readable summary grouped by latency band.

Usage::

    ssh sofascore-prod
    cd /opt/sofascore
    .venv/bin/python scripts/audit_api_latency.py \
        --base-url http://127.0.0.1:8000 \
        --output /tmp/api_audit.csv \
        --summary /tmp/api_audit.md

Defaults are conservative: sequential requests (concurrency=1) so the
audit itself doesn't contaminate the prod server, 30s per-request
timeout, and idle 50ms between requests to avoid bursting.

Latency bands (configurable thresholds):

* ``FAST`` <50ms          target line — most endpoints should land here.
* ``OK`` 50-200ms          acceptable for first-hit, target for cached.
* ``SLOW`` 200-1000ms      problem zone; needs index or cache.
* ``VERY_SLOW`` 1-5s       priority fix.
* ``CATASTROPHIC`` >5s     blockers for any frontend use.
* ``TIMEOUT`` no response  needs investigation.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import asyncpg
import httpx


logger = logging.getLogger("audit_api_latency")

PLACEHOLDER_RE = re.compile(r"\{(\w+)\}")

LATENCY_BANDS = (
    ("FAST", 0.0, 50.0),
    ("OK", 50.0, 200.0),
    ("SLOW", 200.0, 1000.0),
    ("VERY_SLOW", 1000.0, 5000.0),
    ("CATASTROPHIC", 5000.0, float("inf")),
)


# Path placeholders we can substitute from DB samples. Anything not in
# this map is treated as a query-string placeholder and replaced with a
# sane default.
PATH_PLACEHOLDERS = {
    "event_id",
    "custom_id",
    "player_id",
    "team_id",
    "tournament_id",
    "unique_tournament_id",
    "season_id",
    "category_id",
    "manager_id",
    "period_id",
    "provider_id",
    "round_number",
    "at_bat_id",
    "date",
    "sport_id",
    "sport_slug",
}

# Defaults for query-string placeholders (when present in the pattern).
QUERY_DEFAULTS = {
    "accumulation": "total",
    "fields": "",
    "filters": "",
    "group": "by_event_id",
    "limit": "20",
    "offset": "0",
    "order": "asc",
    "page": "0",
    "scope": "default",
}


def _load_env() -> dict[str, str]:
    merged = dict(os.environ)
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return merged
    for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        merged.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return merged


async def fetch_patterns(pool: asyncpg.Pool, *, include_phase_variants: bool) -> list[str]:
    """Return unique endpoint patterns from registry. Strips ``#phase=*`` by default."""

    if include_phase_variants:
        rows = await pool.fetch(
            "SELECT DISTINCT pattern FROM endpoint_registry ORDER BY pattern"
        )
    else:
        rows = await pool.fetch(
            "SELECT DISTINCT regexp_replace(pattern, '#.*$', '') AS pattern "
            "FROM endpoint_registry ORDER BY 1"
        )
    return [str(row["pattern"]) for row in rows]


async def sample_ids(pool: asyncpg.Pool) -> dict[str, list[str]]:
    """Sample realistic IDs from prod DB for each path placeholder type.

    Strategy:
    * Prefer recent rows (last 7 days) so snapshot waterfall has cache hits.
    * Take 3 samples per type — first one is used for the URL, others
      are kept for cache-miss / variability follow-ups in later phases.
    * For ``date``: today + yesterday + 2 days ago.
    """

    samples: dict[str, list[str]] = {}

    async def fetch_ids(query: str, key: str) -> None:
        try:
            rows = await pool.fetch(query)
            samples[key] = [str(row[0]) for row in rows if row[0] is not None]
        except Exception as exc:  # noqa: BLE001
            logger.warning("sample %s failed: %r", key, exc)
            samples[key] = []

    # event.start_timestamp is BIGINT (unix epoch seconds), not TIMESTAMPTZ.
    # Recent-by-id is good enough — id is auto-increment so newer = higher.
    # We pick rows that actually have data attached (has terminal_state or
    # api_payload_snapshot) so endpoint reconstructions work.
    await asyncio.gather(
        fetch_ids(
            "SELECT e.id FROM event e "
            "WHERE EXISTS ("
            "  SELECT 1 FROM api_payload_snapshot aps "
            "  WHERE aps.context_entity_type = 'event' "
            "    AND aps.context_entity_id = e.id "
            "    AND aps.endpoint_pattern = '/api/v1/event/{event_id}'"
            ") "
            "ORDER BY e.id DESC LIMIT 3",
            "event_id",
        ),
        fetch_ids(
            "SELECT custom_id FROM event "
            "WHERE custom_id IS NOT NULL AND custom_id <> '' "
            "ORDER BY id DESC LIMIT 3",
            "custom_id",
        ),
        fetch_ids("SELECT id FROM team WHERE id IS NOT NULL ORDER BY id DESC LIMIT 3", "team_id"),
        fetch_ids(
            "SELECT id FROM player WHERE id IS NOT NULL ORDER BY id DESC LIMIT 3", "player_id"
        ),
        fetch_ids(
            "SELECT id FROM tournament ORDER BY id DESC LIMIT 3", "tournament_id"
        ),
        fetch_ids(
            "SELECT id FROM unique_tournament ORDER BY id DESC LIMIT 3",
            "unique_tournament_id",
        ),
        fetch_ids("SELECT id FROM season ORDER BY id DESC LIMIT 3", "season_id"),
        fetch_ids("SELECT id FROM category ORDER BY id DESC LIMIT 3", "category_id"),
        fetch_ids("SELECT id FROM manager ORDER BY id DESC LIMIT 3", "manager_id"),
        fetch_ids("SELECT id FROM sport ORDER BY id LIMIT 3", "sport_id"),
        fetch_ids("SELECT slug FROM sport ORDER BY id LIMIT 3", "sport_slug"),
    )

    # Date: today + yesterday + day-before. Each as YYYY-MM-DD.
    today = datetime.now(timezone.utc).date()
    samples["date"] = [
        today.isoformat(),
        (today - timedelta(days=1)).isoformat(),
        (today - timedelta(days=2)).isoformat(),
    ]

    # Period / round / at-bat / provider: no clean source table exposed.
    # Use a sane default that's common across upstream Sofascore samples.
    samples["period_id"] = ["1"]
    samples["round_number"] = ["1"]
    samples["at_bat_id"] = ["1"]
    samples["provider_id"] = ["1"]
    return samples


def build_url(
    pattern: str, samples: dict[str, list[str]]
) -> tuple[str | None, str | None]:
    """Substitute placeholders → real URL. Return (url, skip_reason)."""

    url = pattern
    placeholders = PLACEHOLDER_RE.findall(pattern)
    for ph in placeholders:
        if ph in PATH_PLACEHOLDERS:
            sample = samples.get(ph)
            if not sample:
                return None, f"no sample for path placeholder {ph}"
            url = url.replace("{" + ph + "}", sample[0])
        elif ph in QUERY_DEFAULTS:
            url = url.replace("{" + ph + "}", QUERY_DEFAULTS[ph])
        else:
            # Unknown placeholder — try samples first, else fallback to "1"
            sample = samples.get(ph)
            if sample:
                url = url.replace("{" + ph + "}", sample[0])
            else:
                url = url.replace("{" + ph + "}", "1")
    return url, None


def categorize(elapsed_ms: float, http_code: int) -> str:
    if http_code == 0:
        return "TIMEOUT"
    for label, lo, hi in LATENCY_BANDS:
        if lo <= elapsed_ms < hi:
            return label
    return "CATASTROPHIC"


async def probe_endpoint(
    client: httpx.AsyncClient, base_url: str, url: str, *, timeout_seconds: float
) -> dict[str, Any]:
    full = base_url.rstrip("/") + url
    start = time.perf_counter()
    http_code = 0
    size_bytes = 0
    error_msg: str | None = None
    try:
        response = await client.get(full, timeout=timeout_seconds)
        http_code = int(response.status_code)
        size_bytes = len(response.content or b"")
    except httpx.TimeoutException:
        error_msg = "timeout"
    except Exception as exc:  # noqa: BLE001
        error_msg = repr(exc)[:200]
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    return {
        "url": url,
        "http_code": http_code,
        "ttfb_ms": round(elapsed_ms, 1),
        "size_bytes": size_bytes,
        "category": categorize(elapsed_ms, http_code),
        "error": error_msg,
    }


async def run_audit(
    *,
    base_url: str,
    db_dsn: str,
    output_csv: Path,
    summary_md: Path,
    timeout_seconds: float,
    sleep_between_ms: float,
    include_phase_variants: bool,
) -> None:
    logger.info("Connecting to DB to fetch patterns + samples")
    pool = await asyncpg.create_pool(db_dsn, min_size=1, max_size=3, command_timeout=15.0)
    try:
        patterns = await fetch_patterns(pool, include_phase_variants=include_phase_variants)
        logger.info("Loaded %d unique patterns", len(patterns))
        samples = await sample_ids(pool)
        sample_summary = {
            ph: len(samples.get(ph, [])) for ph in sorted(PATH_PLACEHOLDERS)
        }
        logger.info("Sample counts: %s", sample_summary)
    finally:
        await pool.close()

    urls: list[tuple[str, str]] = []
    skipped: list[tuple[str, str]] = []
    for pattern in patterns:
        url, reason = build_url(pattern, samples)
        if url is None:
            skipped.append((pattern, reason or "unknown"))
            continue
        urls.append((pattern, url))
    logger.info("Built %d URLs (%d skipped)", len(urls), len(skipped))

    results: list[dict[str, Any]] = []
    async with httpx.AsyncClient(
        timeout=timeout_seconds, follow_redirects=False
    ) as client:
        for i, (pattern, url) in enumerate(urls, 1):
            probe = await probe_endpoint(
                client, base_url, url, timeout_seconds=timeout_seconds
            )
            probe["pattern"] = pattern
            results.append(probe)
            if i % 10 == 0 or i == len(urls):
                logger.info(
                    "Progress %d/%d — last: %s = %.0fms (%s)",
                    i,
                    len(urls),
                    url[:60],
                    probe["ttfb_ms"],
                    probe["category"],
                )
            if sleep_between_ms > 0:
                await asyncio.sleep(sleep_between_ms / 1000.0)

    write_csv(results, skipped, output_csv)
    write_markdown_summary(results, skipped, summary_md)
    logger.info("Audit complete. CSV=%s Markdown=%s", output_csv, summary_md)


def write_csv(
    results: list[dict[str, Any]],
    skipped: list[tuple[str, str]],
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "pattern",
                "url",
                "http_code",
                "ttfb_ms",
                "size_bytes",
                "category",
                "error",
            ],
        )
        writer.writeheader()
        for row in results:
            writer.writerow(row)
        # Append skipped rows with category=SKIPPED
        for pattern, reason in skipped:
            writer.writerow(
                {
                    "pattern": pattern,
                    "url": "",
                    "http_code": "",
                    "ttfb_ms": "",
                    "size_bytes": "",
                    "category": "SKIPPED",
                    "error": reason,
                }
            )


def write_markdown_summary(
    results: list[dict[str, Any]],
    skipped: list[tuple[str, str]],
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    totals: dict[str, int] = {label: 0 for label, *_ in LATENCY_BANDS}
    totals["TIMEOUT"] = 0
    for row in results:
        totals[row["category"]] = totals.get(row["category"], 0) + 1
    total_probes = len(results)
    band_order = [label for label, *_ in LATENCY_BANDS] + ["TIMEOUT"]

    sorted_results = sorted(results, key=lambda r: r["ttfb_ms"], reverse=True)

    with path.open("w", encoding="utf-8") as fh:
        fh.write("# API Latency Audit\n\n")
        fh.write(f"- Run: {datetime.now(timezone.utc).isoformat()}\n")
        fh.write(f"- Total probed: {total_probes}\n")
        fh.write(f"- Skipped (no sample for placeholder): {len(skipped)}\n\n")

        fh.write("## Distribution by latency band\n\n")
        fh.write("| Band | TTFB range | Count | % |\n")
        fh.write("| --- | --- | --- | --- |\n")
        for label, lo, hi in LATENCY_BANDS:
            count = totals.get(label, 0)
            pct = (count / total_probes * 100.0) if total_probes else 0.0
            rng = f"<{int(hi)}ms" if lo == 0 else (f"≥{int(lo)}ms" if hi == float("inf") else f"{int(lo)}-{int(hi)}ms")
            fh.write(f"| {label} | {rng} | {count} | {pct:.1f}% |\n")
        fh.write(
            f"| TIMEOUT | — | {totals.get('TIMEOUT', 0)} | "
            f"{(totals.get('TIMEOUT', 0) / total_probes * 100.0) if total_probes else 0.0:.1f}% |\n\n"
        )

        target_under_50 = totals.get("FAST", 0)
        fh.write(
            f"**50ms target: {target_under_50}/{total_probes} = "
            f"{(target_under_50 / total_probes * 100.0) if total_probes else 0.0:.1f}% of endpoints**\n\n"
        )

        fh.write("## Top 30 slowest endpoints\n\n")
        fh.write("| # | TTFB (ms) | HTTP | Size (bytes) | Pattern | URL |\n")
        fh.write("| --- | --- | --- | --- | --- | --- |\n")
        for i, row in enumerate(sorted_results[:30], 1):
            fh.write(
                f"| {i} | {row['ttfb_ms']:.0f} | {row['http_code']} | "
                f"{row['size_bytes']} | `{row['pattern']}` | `{row['url']}` |\n"
            )

        fh.write("\n## Fast endpoints (<50ms) — reference good shape\n\n")
        fast = [r for r in results if r["category"] == "FAST"]
        if not fast:
            fh.write("_No endpoints met the 50ms target._\n")
        else:
            fh.write("| TTFB (ms) | HTTP | Pattern |\n| --- | --- | --- |\n")
            for row in sorted(fast, key=lambda r: r["ttfb_ms"])[:20]:
                fh.write(
                    f"| {row['ttfb_ms']:.0f} | {row['http_code']} | `{row['pattern']}` |\n"
                )

        if skipped:
            fh.write("\n## Skipped patterns (no sample data for placeholder)\n\n")
            for pattern, reason in skipped[:50]:
                fh.write(f"- `{pattern}` — {reason}\n")
            if len(skipped) > 50:
                fh.write(f"- ... and {len(skipped) - 50} more\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--output", default="/tmp/api_audit.csv")
    parser.add_argument("--summary", default="/tmp/api_audit.md")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument(
        "--sleep-between-ms",
        type=float,
        default=50.0,
        help="Pause between probes (ms). Avoids bursting the prod API.",
    )
    parser.add_argument(
        "--include-phase-variants",
        action="store_true",
        help="Include '#phase=...' variant patterns separately.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stdout,
    )

    env = _load_env()
    db_dsn = env.get("SOFASCORE_DATABASE_URL")
    if not db_dsn:
        print("SOFASCORE_DATABASE_URL not set", file=sys.stderr)
        return 1

    asyncio.run(
        run_audit(
            base_url=str(args.base_url),
            db_dsn=str(db_dsn),
            output_csv=Path(args.output),
            summary_md=Path(args.summary),
            timeout_seconds=float(args.timeout_seconds),
            sleep_between_ms=float(args.sleep_between_ms),
            include_phase_variants=bool(args.include_phase_variants),
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
