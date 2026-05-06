"""CLI for backfilling team-detail snapshots (players + recent events).

This tool fetches a small set of team-level Sofascore endpoints through the
shared :class:`InspectorTransport` and stores each upstream payload into
``api_payload_snapshot`` so the local API can serve them 1:1 via the existing
raw-passthrough waterfall.

Endpoints touched per team:

  * ``/api/v1/team/{team_id}/players``               (always)
  * ``/api/v1/team/{team_id}/events/last/{page}``    (when ``--include-events``)
  * ``/api/v1/team/{team_id}/events/next/{page}``    (when ``--include-events``)

The CLI is designed for ad-hoc / cron-driven refresh. It does not touch the
worker streams, the planner cadence, or the discovery scope. Continuous
integration of these endpoints into ``structure-planner`` is a separate
follow-up.

Usage::

    python -m schema_inspector.team_detail_cli --team-id 42
    python -m schema_inspector.team_detail_cli --team-id 42 --team-id 2672 --include-events --pages 2

Notes:
  * ``team/{id}/players`` is registered in ``endpoint_registry`` via
    ``TEAM_PLAYERS_ENDPOINT`` and ships in ``ENTITIES_ENDPOINTS``.
  * ``team/{id}/events/{last,next}/{page}`` are registered already (they were
    seeded by ``hybrid_runtime_registry_entries_for_sport``); only their
    snapshot coverage was sparse on prod.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone

import orjson

from .db import AsyncpgDatabase, load_database_config
from .endpoints import (
    SofascoreEndpoint,
    TEAM_PLAYERS_ENDPOINT,
    team_last_events_endpoint,
    team_next_events_endpoint,
)
from .runtime import _load_project_env, load_runtime_config
from .storage.raw_repository import PayloadSnapshotRecord, RawRepository
from .transport import InspectorTransport

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TeamFetchJob:
    """One snapshot fetch unit (endpoint + path params + persistence context)."""

    endpoint: SofascoreEndpoint
    path_params: dict[str, object]
    context_entity_type: str
    context_entity_id: int


def build_jobs(
    team_id: int,
    *,
    include_events: bool,
    pages: int,
) -> tuple[TeamFetchJob, ...]:
    """Return the list of fetch jobs for a single team.

    ``team/{id}/players`` is always included. Events lists are added per page
    (0..pages-1) when ``include_events`` is true. We do not auto-paginate
    until ``hasNextPage=false`` here -- that decision belongs to whatever
    drives this CLI (cron / planner). Keeping this function small makes it
    easy to test the fetch plan without any I/O.
    """

    if pages < 0:
        raise ValueError("pages must be >= 0")

    jobs: list[TeamFetchJob] = [
        TeamFetchJob(
            endpoint=TEAM_PLAYERS_ENDPOINT,
            path_params={"team_id": int(team_id)},
            context_entity_type="team",
            context_entity_id=int(team_id),
        )
    ]
    if include_events:
        last_endpoint = team_last_events_endpoint()
        next_endpoint = team_next_events_endpoint()
        for page in range(pages):
            for endpoint in (last_endpoint, next_endpoint):
                jobs.append(
                    TeamFetchJob(
                        endpoint=endpoint,
                        path_params={"team_id": int(team_id), "page": int(page)},
                        context_entity_type="team",
                        context_entity_id=int(team_id),
                    )
                )
    return tuple(jobs)


async def _fetch_and_store(
    transport: InspectorTransport,
    repository: RawRepository,
    database: AsyncpgDatabase,
    job: TeamFetchJob,
    *,
    timeout: float,
) -> tuple[int, int | None]:
    """Fetch one ``TeamFetchJob`` and persist the resulting snapshot.

    Returns ``(http_status, snapshot_id)``. ``snapshot_id`` is None when the
    insert was deduped (same payload_hash + scope_key already stored) or when
    the upstream response had no body.
    """

    url = job.endpoint.build_url(**job.path_params)
    response = await transport.fetch(url, timeout=timeout)
    body = response.body_bytes or b""
    payload_obj: object | None = None
    is_valid_json = False
    if body:
        try:
            payload_obj = orjson.loads(body)
            is_valid_json = True
        except Exception:
            is_valid_json = False
    record = PayloadSnapshotRecord(
        trace_id="team-detail-cli",
        job_id=f"team-detail:{job.context_entity_id}",
        sport_slug=None,
        endpoint_pattern=job.endpoint.pattern,
        source_url=url,
        resolved_url=response.resolved_url,
        envelope_key=job.endpoint.envelope_key,
        context_entity_type=job.context_entity_type,
        context_entity_id=job.context_entity_id,
        context_unique_tournament_id=None,
        context_season_id=None,
        context_event_id=None,
        http_status=response.status_code,
        payload=payload_obj,
        payload_hash=hashlib.sha256(body).hexdigest() if body else None,
        payload_size_bytes=len(body),
        content_type="application/json",
        is_valid_json=is_valid_json,
        is_soft_error_payload=False,
        fetched_at=datetime.now(timezone.utc).isoformat(),
    )
    async with database.transaction() as connection:
        snapshot_id = await repository.insert_payload_snapshot_if_missing_returning_id(
            connection, record
        )
    return int(response.status_code or 0), snapshot_id


async def _run(args: argparse.Namespace) -> int:
    _load_project_env()
    runtime_config = load_runtime_config(
        proxy_urls=args.proxy,
        user_agent=args.user_agent,
        max_attempts=args.max_attempts,
    )
    database_config = load_database_config(
        dsn=args.database_url,
        min_size=args.db_min_size,
        max_size=args.db_max_size,
        command_timeout=args.db_timeout,
    )
    transport = InspectorTransport(runtime_config)
    repository = RawRepository()

    success = 0
    not_found = 0
    failed = 0
    async with AsyncpgDatabase(database_config) as database:
        for team_id in args.team_id:
            for job in build_jobs(team_id, include_events=args.include_events, pages=args.pages):
                try:
                    status, snapshot_id = await _fetch_and_store(
                        transport, repository, database, job, timeout=args.timeout
                    )
                except Exception as exc:
                    failed += 1
                    logger.warning(
                        "team-detail fetch failed: team=%s endpoint=%s error=%s",
                        team_id,
                        job.endpoint.path_template,
                        exc,
                    )
                    continue
                if status == 200:
                    success += 1
                elif status == 404:
                    not_found += 1
                else:
                    failed += 1
                print(
                    f"team={team_id} {job.endpoint.path_template} "
                    f"path_params={job.path_params} status={status} "
                    f"snapshot_id={snapshot_id}"
                )

    try:
        await transport.close()
    except Exception:
        pass

    print(f"team_detail_cli: success={success} not_found={not_found} failed={failed}")
    return 0 if failed == 0 else 1


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description=(
            "Backfill team-detail Sofascore snapshots (players + optional recent events) "
            "for one or more teams into api_payload_snapshot."
        ),
    )
    parser.add_argument(
        "--team-id",
        type=int,
        action="append",
        required=True,
        help="Team ID. Can be passed multiple times to backfill several teams in one run.",
    )
    parser.add_argument(
        "--include-events",
        action="store_true",
        help="Also fetch /events/last/{page} and /events/next/{page}.",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=1,
        help="Number of event pages to fetch (each direction). Only used with --include-events. Default 1.",
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout in seconds.")
    parser.add_argument(
        "--proxy",
        action="append",
        default=[],
        help="Optional proxy URL. Can be passed multiple times.",
    )
    parser.add_argument("--user-agent", default=None, help="Override User-Agent for the transport layer.")
    parser.add_argument("--max-attempts", type=int, default=None, help="Override retry attempts for the transport.")
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL DSN. Falls back to SOFASCORE_DATABASE_URL / DATABASE_URL / POSTGRES_DSN.",
    )
    parser.add_argument("--db-min-size", type=int, default=None, help="Minimum asyncpg pool size.")
    parser.add_argument("--db-max-size", type=int, default=None, help="Maximum asyncpg pool size.")
    parser.add_argument("--db-timeout", type=float, default=None, help="asyncpg command timeout in seconds.")
    args = parser.parse_args()

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
