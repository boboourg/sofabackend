"""Phase 4.4 sandbox: probe one or more (UT, season, status) cohorts
through the real SofascoreClient + persist to league_endpoint_capability.

Wraps the test-grade ProbeExecutor with real prod plumbing:
  * SofascoreClient over curl_cffi (real TLS, real proxies)
  * AsyncpgDatabase + LeagueCapabilitiesRepository
  * LeagueCapabilitiesRegistry (Redis cache invalidation)

Usage::

    python -m schema_inspector.cli ... # NO — this is a standalone script
    /opt/sofascore/.venv/bin/python scripts/probe_league_capabilities_sandbox.py \\
        --unique-tournament-id 17 --season-id 61643 \\
        --status-type inprogress

By default probes the full PROBE_ENDPOINTS_BY_STATUS[status_type] set
with 5 samples each. The probe is read-only against upstream but
WRITES to ``league_endpoint_capability`` and INVALIDATES Redis.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class _SofascoreClientAdapter:
    """Adapt SofascoreClient.get_json(url) to the
    ProbeExecutor.client.get_json(event_id, endpoint_pattern) shape.
    Resolves endpoint_pattern → URL via the endpoints.py registry."""

    def __init__(self, client, *, sport_slug: str = "football", timeout: float = 20.0):
        self.client = client
        self.sport_slug = sport_slug
        self.timeout = timeout

    async def get_json(self, *, event_id: int, endpoint_pattern: str):
        from schema_inspector.endpoints import SOFASCORE_BASE_URL
        from schema_inspector.sofascore_client import SofascoreHttpError

        # Build URL from pattern by substituting {event_id}.
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

    # DB
    db_config = load_database_config()
    database = AsyncpgDatabase(db_config)
    await database.connect()

    # HTTP client (curl_cffi)
    runtime = load_runtime_config()
    raw_client = SofascoreClient(runtime)
    client_adapter = _SofascoreClientAdapter(raw_client, timeout=args.timeout)

    # Repository + registry (registry uses a no-op redis if not provided)
    repository = LeagueCapabilitiesRepository()

    class _NoopRedis:
        def get(self, key): return None
        def set(self, key, value, *, ex=None): return True
        def delete(self, key): return 0

    registry = LeagueCapabilitiesRegistry(
        redis_backend=_NoopRedis(),  # cache invalidation is best-effort here
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

    # Decide endpoint set.
    endpoint_patterns = PROBE_ENDPOINTS_BY_STATUS.get(args.status_type)
    if endpoint_patterns is None:
        print(f"Unknown status_type: {args.status_type}", file=sys.stderr)
        return 2

    print(
        f"Probing UT={args.unique_tournament_id} season={args.season_id} "
        f"status={args.status_type} | endpoints={len(endpoint_patterns)} "
        f"samples={args.samples}"
    )
    started = time.perf_counter()
    try:
        report = await executor.probe(
            unique_tournament_id=args.unique_tournament_id,
            season_id=args.season_id,
            status_type=args.status_type,
            endpoint_patterns=endpoint_patterns,
            samples_per_endpoint=args.samples,
        )
    finally:
        await database.close()

    elapsed = time.perf_counter() - started

    # Pretty report
    print()
    print("=" * 80)
    print(
        f"REPORT  ut={report.unique_tournament_id}  season={report.season_id}  "
        f"status={report.status_type}"
    )
    print("=" * 80)
    print(
        f"  samples_used      : {report.samples_used}"
    )
    print(
        f"  upserts_count     : {report.upserts_count}"
    )
    print(
        f"  elapsed_seconds   : {report.elapsed_seconds:.1f}"
    )
    print(
        f"  total wallclock   : {elapsed:.1f}s"
    )
    print()
    print("PER-ENDPOINT VERDICT:")
    by_endpoint = report.by_endpoint
    width = max((len(ep) for ep in by_endpoint), default=0)
    for ep in endpoint_patterns:
        state = by_endpoint.get(ep, "missing")
        emoji = "✅" if state == "allowed" else ("⛔" if state == "disabled" else "?")
        print(f"  {emoji}  {ep:<{width}}  {state}")
    if report.errors:
        print()
        print(f"ERRORS ({len(report.errors)}):")
        for err in report.errors[:20]:
            print(f"  {err}")

    if args.json_out:
        out_path = Path(args.json_out)
        out_path.write_text(
            json.dumps(
                {
                    "unique_tournament_id": report.unique_tournament_id,
                    "season_id": report.season_id,
                    "status_type": report.status_type,
                    "samples_used": report.samples_used,
                    "upserts_count": report.upserts_count,
                    "elapsed_seconds": report.elapsed_seconds,
                    "by_endpoint": dict(report.by_endpoint),
                    "errors": list(report.errors),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"\nJSON dump: {out_path}")
    return 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--unique-tournament-id", type=int, required=True)
    parser.add_argument("--season-id", type=int, default=None)
    parser.add_argument(
        "--status-type",
        choices=("inprogress", "finished", "notstarted"),
        default="finished",
    )
    parser.add_argument("--samples", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--json-out", default=None)
    return parser.parse_args()


def main() -> int:
    return asyncio.run(main_async(_parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
