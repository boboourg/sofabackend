"""Unified cutover CLI for the hybrid ETL backbone."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from .db import AsyncpgDatabase, load_database_config
from .endpoints import hybrid_runtime_registry_entries_for_sport
from .event_list_job import EventListIngestJob
from .event_list_parser import EventListParser
from .event_list_repository import EventListRepository
from .fetch_executor import FetchExecutor
from .normalizers.sink import DurableNormalizeSink
from .normalizers.worker import NormalizeWorker
from .ops.db_audit import collect_db_audit
from .ops.health import collect_health_report
from .ops.recovery import rebuild_live_state_from_postgres
from .parsers.base import RawSnapshot
from .parsers.registry import ParserRegistry
from .pipeline.pilot_orchestrator import PilotOrchestrator
from .planner.planner import Planner
from .queue.live_state import LiveEventStateStore
from .queue.streams import RedisStreamQueue
from .runtime import load_runtime_config
from .services.service_app import ServiceApp
from .sofascore_client import SofascoreClient
from .storage.capability_repository import CapabilityRepository
from .storage.live_state_repository import LiveStateRepository
from .storage.normalize_repository import NormalizeRepository
from .storage.raw_repository import RawRepository
from .transport import InspectorTransport

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HydrationBatchReport:
    processed_event_ids: tuple[int, ...]
    results: tuple[object, ...]


@dataclass(frozen=True)
class ReplayBatchReport:
    snapshot_ids: tuple[int, ...]
    parser_families: tuple[str, ...]


class HybridSnapshotStore:
    def __init__(self, repository: RawRepository, sql_executor) -> None:
        self.repository = repository
        self.sql_executor = sql_executor
        self._cache: dict[int, RawSnapshot] = {}

    async def insert_request_log(self, executor, record) -> None:
        await self.repository.insert_request_log(executor, record)

    async def insert_payload_snapshot_returning_id(self, executor, record) -> int | None:
        snapshot_id = await self.repository.insert_payload_snapshot_returning_id(executor, record)
        if snapshot_id is not None:
            self._cache[int(snapshot_id)] = RawSnapshot(
                snapshot_id=int(snapshot_id),
                endpoint_pattern=record.endpoint_pattern,
                sport_slug=str(record.sport_slug or ""),
                source_url=record.source_url,
                resolved_url=record.resolved_url or record.source_url,
                envelope_key=record.envelope_key,
                http_status=record.http_status,
                payload=record.payload,
                fetched_at=str(record.fetched_at or ""),
                context_entity_type=record.context_entity_type,
                context_entity_id=record.context_entity_id,
                context_unique_tournament_id=record.context_unique_tournament_id,
                context_season_id=record.context_season_id,
                context_event_id=record.context_event_id,
            )
        return snapshot_id

    async def upsert_snapshot_head(self, executor, record) -> None:
        await self.repository.upsert_snapshot_head(executor, record)

    def load_snapshot(self, snapshot_id: int) -> RawSnapshot:
        snapshot = self._cache.get(int(snapshot_id))
        if snapshot is None:
            raise KeyError(snapshot_id)
        return snapshot

    async def load_snapshot_async(self, snapshot_id: int) -> RawSnapshot:
        snapshot_id = int(snapshot_id)
        snapshot = self._cache.get(snapshot_id)
        if snapshot is not None:
            return snapshot
        snapshot = await self.repository.fetch_payload_snapshot(self.sql_executor, snapshot_id)
        self._cache[snapshot_id] = snapshot
        return snapshot


class HybridApp:
    def __init__(self, *, database: AsyncpgDatabase, runtime_config, redis_backend) -> None:
        self.database = database
        self.runtime_config = runtime_config
        self.redis_backend = redis_backend
        self.transport = InspectorTransport(runtime_config)
        self.raw_repository = RawRepository()
        self.capability_repository = CapabilityRepository()
        self.live_state_repository = LiveStateRepository()
        self.normalize_repository = NormalizeRepository()
        self.live_state_store = LiveEventStateStore(redis_backend) if redis_backend is not None else None
        self.stream_queue = RedisStreamQueue(redis_backend) if redis_backend is not None else None
        self.capability_rollup: dict[str, str] = {}
        self._seeded_endpoint_registry_sports: set[str] = set()

        client = SofascoreClient(runtime_config, transport=self.transport)
        self.event_list_job = EventListIngestJob(
            EventListParser(client),
            EventListRepository(),
            database,
        )

    async def close(self) -> None:
        await self.transport.close()
        close_backend = getattr(self.redis_backend, "close", None)
        if callable(close_backend):
            maybe_awaitable = close_backend()
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable

    async def ensure_endpoint_registry(self, sport_slug: str) -> None:
        normalized_sport_slug = str(sport_slug or "").strip().lower() or "football"
        if normalized_sport_slug in self._seeded_endpoint_registry_sports:
            return
        registry_entries = hybrid_runtime_registry_entries_for_sport(normalized_sport_slug)
        async with self.database.transaction() as connection:
            await self.raw_repository.upsert_endpoint_registry_entries(connection, registry_entries)
        self._seeded_endpoint_registry_sports.add(normalized_sport_slug)

    async def run_event(
        self,
        *,
        event_id: int,
        sport_slug: str | None,
        hydration_mode: str = "full",
    ):
        resolved_sport_slug = sport_slug or await self.resolve_event_sport_slug(event_id)
        await self.ensure_endpoint_registry(str(resolved_sport_slug or "football"))
        async with self.database.transaction() as connection:
            snapshot_store = HybridSnapshotStore(self.raw_repository, connection)
            skip_entity_parser_families = {"event_root"} if hydration_mode == "core" else set()
            orchestrator = PilotOrchestrator(
                fetch_executor=FetchExecutor(
                    transport=self.transport,
                    raw_repository=snapshot_store,
                    sql_executor=connection,
                ),
                snapshot_store=snapshot_store,
                normalize_worker=NormalizeWorker(
                    ParserRegistry.default(),
                    result_sink=DurableNormalizeSink(
                        self.normalize_repository,
                        connection,
                        skip_entity_parser_families=skip_entity_parser_families,
                    ),
                ),
                planner=Planner(capability_rollup=self.capability_rollup),
                capability_repository=self.capability_repository,
                sql_executor=connection,
                live_state_store=self.live_state_store,
                live_state_repository=self.live_state_repository,
                stream_queue=self.stream_queue,
            )
            return await orchestrator.run_event(
                event_id=event_id,
                sport_slug=str(resolved_sport_slug or "football"),
                hydration_mode=hydration_mode,
            )

    async def replay_snapshot(self, snapshot_id: int):
        async with self.database.transaction() as connection:
            snapshot_store = HybridSnapshotStore(self.raw_repository, connection)
            snapshot = await snapshot_store.load_snapshot_async(snapshot_id)
            worker = NormalizeWorker(
                ParserRegistry.default(),
                result_sink=DurableNormalizeSink(self.normalize_repository, connection),
            )
            return await worker.handle_async(snapshot)

    async def collect_db_audit(self, *, sport_slug: str, event_ids: tuple[int, ...]):
        async with self.database.connection() as connection:
            return await collect_db_audit(
                sql_executor=connection,
                sport_slug=sport_slug,
                event_ids=tuple(int(item) for item in event_ids),
            )

    async def discover_live_event_ids(self, *, sport_slug: str, timeout: float) -> tuple[int, ...]:
        result = await self.event_list_job.run_live(sport_slug=sport_slug, timeout=timeout)
        return tuple(int(item.id) for item in result.parsed.events)

    async def discover_scheduled_event_ids(self, *, sport_slug: str, date: str, timeout: float) -> tuple[int, ...]:
        result = await self.event_list_job.run_scheduled(date, sport_slug=sport_slug, timeout=timeout)
        return tuple(int(item.id) for item in result.parsed.events)

    async def select_event_ids(self, *, limit: int | None, offset: int, sport_slug: str | None) -> tuple[int, ...]:
        async with self.database.connection() as connection:
            if sport_slug and limit is None:
                rows = await connection.fetch(
                    """
                    SELECT e.id
                    FROM event e
                    JOIN unique_tournament ut ON ut.id = e.unique_tournament_id
                    JOIN category c ON c.id = ut.category_id
                    JOIN sport s ON s.id = c.sport_id
                    WHERE s.slug = $1
                    ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
                    OFFSET $2
                    """,
                    sport_slug,
                    offset,
                )
            elif sport_slug:
                rows = await connection.fetch(
                    """
                    SELECT e.id
                    FROM event e
                    JOIN unique_tournament ut ON ut.id = e.unique_tournament_id
                    JOIN category c ON c.id = ut.category_id
                    JOIN sport s ON s.id = c.sport_id
                    WHERE s.slug = $1
                    ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
                    OFFSET $2 LIMIT $3
                    """,
                    sport_slug,
                    offset,
                    limit,
                )
            elif limit is None:
                rows = await connection.fetch(
                    """
                    SELECT e.id
                    FROM event e
                    ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
                    OFFSET $1
                    """,
                    offset,
                )
            else:
                rows = await connection.fetch(
                    """
                    SELECT e.id
                    FROM event e
                    ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
                    OFFSET $1 LIMIT $2
                    """,
                    offset,
                    limit,
                )
        return tuple(int(row["id"]) for row in rows)

    async def resolve_event_sport_slug(self, event_id: int) -> str | None:
        async with self.database.connection() as connection:
            row = await connection.fetchrow(
                """
                SELECT s.slug
                FROM event e
                JOIN unique_tournament ut ON ut.id = e.unique_tournament_id
                JOIN category c ON c.id = ut.category_id
                JOIN sport s ON s.id = c.sport_id
                WHERE e.id = $1
                """,
                event_id,
            )
        if row is None:
            return None
        return str(row["slug"])

    async def collect_health(self):
        async with self.database.connection() as connection:
            return await collect_health_report(
                sql_executor=connection,
                live_state_store=self.live_state_store,
                redis_backend=self.redis_backend,
            )

    async def recover_live_state(self):
        async with self.database.connection() as connection:
            return await rebuild_live_state_from_postgres(
                repository=self.live_state_repository,
                sql_executor=connection,
                live_state_store=self.live_state_store,
                now_ms=int(time.time() * 1000),
            )


async def run_event_command(args, *, orchestrator) -> HydrationBatchReport:
    event_ids = tuple(int(item) for item in args.event_id)
    hydration_mode = str(getattr(args, "hydration_mode", "full") or "full")
    event_concurrency = max(1, int(getattr(args, "event_concurrency", 1) or 1))
    semaphore = asyncio.Semaphore(event_concurrency)

    async def hydrate_one(event_id: int):
        async with semaphore:
            logger.info(
                "Hybrid hydrate start: sport=%s event_id=%s mode=%s",
                getattr(args, "sport_slug", None),
                event_id,
                hydration_mode,
            )
            result = await orchestrator.run_event(
                event_id=event_id,
                sport_slug=args.sport_slug,
                hydration_mode=hydration_mode,
            )
            logger.info(
                "Hybrid hydrate complete: sport=%s event_id=%s mode=%s",
                getattr(args, "sport_slug", None),
                event_id,
                hydration_mode,
            )
            return result

    results = await asyncio.gather(*(hydrate_one(event_id) for event_id in event_ids))
    return HydrationBatchReport(processed_event_ids=event_ids, results=tuple(results))


async def run_full_backfill_command(args, *, orchestrator, event_selector) -> HydrationBatchReport:
    explicit_event_ids = tuple(int(item) for item in getattr(args, "event_id", ()) or ())
    if explicit_event_ids:
        event_ids = explicit_event_ids
    else:
        event_ids = tuple(
            await event_selector.select_event_ids(
                limit=args.limit,
                offset=args.offset,
                sport_slug=getattr(args, "sport_slug", None),
            )
        )
    delegated_args = argparse.Namespace(
        event_id=event_ids,
        sport_slug=getattr(args, "sport_slug", None),
        hydration_mode=getattr(args, "hydration_mode", "full"),
        event_concurrency=getattr(args, "event_concurrency", 1),
    )
    return await run_event_command(delegated_args, orchestrator=orchestrator)


async def run_replay_command(args, *, replay_service) -> ReplayBatchReport:
    snapshot_ids = tuple(int(item) for item in args.snapshot_id)
    parser_families = []
    for snapshot_id in snapshot_ids:
        result = await replay_service.replay_snapshot(snapshot_id)
        parser_families.append(getattr(result, "parser_family", "unknown"))
    return ReplayBatchReport(snapshot_ids=snapshot_ids, parser_families=tuple(parser_families))


async def _run_post_hydration_audit(args, *, app, report: HydrationBatchReport):
    if not bool(getattr(args, "audit_db", False)):
        return None
    audit_report = await app.collect_db_audit(
        sport_slug=str(getattr(args, "sport_slug", "") or ""),
        event_ids=tuple(int(item) for item in report.processed_event_ids),
    )
    _print_db_audit_report(audit_report)
    if int(audit_report.raw_snapshots) <= 0 or int(audit_report.events) <= 0:
        raise RuntimeError("DB audit failed: raw or durable counts are empty.")
    return audit_report


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    parser = _build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.log_level)
    return asyncio.run(_dispatch(args))


async def _dispatch(args) -> int:
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
    async with AsyncpgDatabase(database_config) as database:
        app = HybridApp(
            database=database,
            runtime_config=runtime_config,
            redis_backend=_load_redis_backend(
                args.redis_url,
                allow_memory_fallback=bool(getattr(args, "allow_memory_redis", False)),
            ),
        )
        logger.info("Redis backend ready: backend=%s", type(app.redis_backend).__name__)
        try:
            if args.command == "event":
                if args.event_concurrency is None:
                    args.event_concurrency = 1
                args.hydration_mode = "full"
                report = await run_event_command(args, orchestrator=app)
                _print_batch_report("event_hydrate", report)
                await _run_post_hydration_audit(args, app=app, report=report)
                return 0
            if args.command == "live":
                event_ids = await app.discover_live_event_ids(sport_slug=args.sport_slug, timeout=args.timeout)
                command_args = argparse.Namespace(
                        event_id=event_ids,
                        sport_slug=args.sport_slug,
                        hydration_mode="full",
                        event_concurrency=args.event_concurrency or 1,
                )
                command_args.audit_db = bool(getattr(args, "audit_db", False))
                report = await run_event_command(command_args, orchestrator=app)
                _print_batch_report("live_hydrate", report)
                await _run_post_hydration_audit(command_args, app=app, report=report)
                return 0
            if args.command == "scheduled":
                event_ids = await app.discover_scheduled_event_ids(sport_slug=args.sport_slug, date=args.date, timeout=args.timeout)
                command_args = argparse.Namespace(
                        event_id=event_ids,
                        sport_slug=args.sport_slug,
                        hydration_mode="core",
                        event_concurrency=args.event_concurrency or 6,
                )
                command_args.audit_db = bool(getattr(args, "audit_db", False))
                report = await run_event_command(command_args, orchestrator=app)
                _print_batch_report("scheduled_hydrate", report)
                await _run_post_hydration_audit(command_args, app=app, report=report)
                return 0
            if args.command == "full-backfill":
                if args.event_concurrency is None:
                    args.event_concurrency = 1
                args.hydration_mode = "full"
                report = await run_full_backfill_command(args, orchestrator=app, event_selector=app)
                _print_batch_report("full_backfill", report)
                await _run_post_hydration_audit(args, app=app, report=report)
                return 0
            if args.command == "replay":
                report = await run_replay_command(args, replay_service=app)
                print(
                    "replay "
                    f"snapshots={','.join(str(item) for item in report.snapshot_ids)} "
                    f"families={','.join(report.parser_families)}"
                )
                return 0
            if args.command == "health":
                report = await app.collect_health()
                print(
                    "health "
                    f"db_ok={int(report.database_ok)} "
                    f"redis_ok={int(report.redis_ok)} "
                    f"redis_backend={report.redis_backend_kind} "
                    f"snapshots={report.snapshot_count} "
                    f"rollups={report.capability_rollup_count} "
                    f"live_hot={report.live_hot_count} "
                    f"live_warm={report.live_warm_count} "
                    f"live_cold={report.live_cold_count}"
                )
                return 0
            if args.command == "audit-db":
                report = await app.collect_db_audit(
                    sport_slug=args.sport_slug,
                    event_ids=tuple(int(item) for item in args.event_id),
                )
                _print_db_audit_report(report)
                return 0
            if args.command == "recover-live-state":
                report = await app.recover_live_state()
                print(
                    "recover_live_state "
                    f"hot={report.restored_hot} warm={report.restored_warm} "
                    f"cold={report.restored_cold} terminal={report.restored_terminal}"
                )
                return 0
            if args.command == "planner-daemon":
                service_app = ServiceApp(app)
                await service_app.run_planner_daemon(
                    sport_slugs=tuple(args.sport_slug or ()),
                    scheduled_interval_seconds=args.scheduled_interval_seconds,
                    loop_interval_seconds=args.loop_interval_seconds,
                )
                return 0
            if args.command == "worker-discovery":
                service_app = ServiceApp(app)
                await service_app.run_discovery_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                    timeout_s=args.timeout,
                )
                return 0
            if args.command == "worker-hydrate":
                service_app = ServiceApp(app)
                await service_app.run_hydrate_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
            if args.command == "worker-live-hot":
                service_app = ServiceApp(app)
                await service_app.run_live_worker(
                    lane="hot",
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
            if args.command == "worker-live-warm":
                service_app = ServiceApp(app)
                await service_app.run_live_worker(
                    lane="warm",
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
            if args.command == "worker-maintenance":
                service_app = ServiceApp(app)
                await service_app.run_maintenance_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
        finally:
            await app.close()
    return 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified hybrid ETL runner.")
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout in seconds.")
    parser.add_argument("--proxy", action="append", default=[], help="Optional proxy URL. Can be passed multiple times.")
    parser.add_argument("--user-agent", default=None, help="Override User-Agent for the transport layer.")
    parser.add_argument("--max-attempts", type=int, default=None, help="Override retry attempts for the transport layer.")
    parser.add_argument("--database-url", default=None, help="PostgreSQL DSN override.")
    parser.add_argument("--db-min-size", type=int, default=None, help="Minimum asyncpg pool size.")
    parser.add_argument("--db-max-size", type=int, default=None, help="Maximum asyncpg pool size.")
    parser.add_argument("--db-timeout", type=float, default=None, help="asyncpg command timeout in seconds.")
    parser.add_argument("--redis-url", default=None, help="Redis URL override.")
    parser.add_argument(
        "--allow-memory-redis",
        action="store_true",
        help="Development-only escape hatch that permits in-memory Redis emulation.",
    )
    parser.add_argument("--event-concurrency", type=int, default=None, help="Optional concurrent event hydration limit.")
    parser.add_argument("--log-level", default="INFO", help="Python log level.")

    subparsers = parser.add_subparsers(dest="command", required=True)
    event = subparsers.add_parser("event", help="Hydrate one or more explicit event ids.")
    event.add_argument("--sport-slug", required=True, help="Sport slug for adapter/planner selection.")
    event.add_argument("--event-id", type=int, action="append", required=True, help="Repeatable event id.")
    event.add_argument("--event-concurrency", type=int, default=None, help="Optional concurrent event hydration limit.")
    event.add_argument("--audit-db", action="store_true", help="Run a post-hydration database audit and fail if raw/durable rows are missing.")

    live = subparsers.add_parser("live", help="Discover live events for a sport and hydrate them.")
    live.add_argument("--sport-slug", required=True, help="Sport slug for discovery.")
    live.add_argument("--event-concurrency", type=int, default=None, help="Optional concurrent event hydration limit.")
    live.add_argument("--audit-db", action="store_true", help="Run a post-hydration database audit and fail if raw/durable rows are missing.")

    scheduled = subparsers.add_parser("scheduled", help="Discover scheduled events for a sport/date and hydrate them.")
    scheduled.add_argument("--sport-slug", required=True, help="Sport slug for discovery.")
    scheduled.add_argument("--date", required=True, help="Date in YYYY-MM-DD format.")
    scheduled.add_argument("--event-concurrency", type=int, default=None, help="Optional concurrent event hydration limit.")
    scheduled.add_argument("--audit-db", action="store_true", help="Run a post-hydration database audit and fail if raw/durable rows are missing.")

    backfill = subparsers.add_parser("full-backfill", help="Hydrate explicit or database-seeded events through the hybrid backbone.")
    backfill.add_argument("--sport-slug", default=None, help="Optional sport filter when selecting from PostgreSQL.")
    backfill.add_argument("--event-id", type=int, action="append", default=[], help="Optional explicit event id.")
    backfill.add_argument("--limit", type=int, default=None, help="Optional event limit when selecting from PostgreSQL.")
    backfill.add_argument("--offset", type=int, default=0, help="Optional event offset when selecting from PostgreSQL.")
    backfill.add_argument("--event-concurrency", type=int, default=None, help="Optional concurrent event hydration limit.")
    backfill.add_argument("--audit-db", action="store_true", help="Run a post-hydration database audit and fail if raw/durable rows are missing.")

    replay = subparsers.add_parser("replay", help="Replay one or more payload snapshots through durable sinks.")
    replay.add_argument("--snapshot-id", type=int, action="append", required=True, help="Repeatable payload snapshot id.")

    audit = subparsers.add_parser("audit-db", help="Print a compact durable/raw database audit for event ids.")
    audit.add_argument("--sport-slug", required=True, help="Sport slug for special-table routing.")
    audit.add_argument("--event-id", type=int, action="append", required=True, help="Repeatable hydrated event id.")

    subparsers.add_parser("health", help="Print a compact hybrid health summary.")
    subparsers.add_parser("recover-live-state", help="Rebuild Redis live-state indexes from PostgreSQL history.")

    planner_daemon = subparsers.add_parser("planner-daemon", help="Run the continuous planner loop.")
    planner_daemon.add_argument("--sport-slug", action="append", default=[], help="Optional repeatable sport slug. Defaults to all supported sports.")
    planner_daemon.add_argument("--scheduled-interval-seconds", type=float, default=300.0, help="Scheduled planning interval per sport.")
    planner_daemon.add_argument("--loop-interval-seconds", type=float, default=5.0, help="Daemon tick loop interval.")

    worker_discovery = subparsers.add_parser("worker-discovery", help="Run the discovery consumer group loop.")
    worker_discovery.add_argument("--consumer-name", default="worker-discovery-1", help="Redis consumer name for the discovery worker.")
    worker_discovery.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_hydrate = subparsers.add_parser("worker-hydrate", help="Run the hydrate consumer group loop.")
    worker_hydrate.add_argument("--consumer-name", default="worker-hydrate-1", help="Redis consumer name for the hydrate worker.")
    worker_hydrate.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_live_hot = subparsers.add_parser("worker-live-hot", help="Run the live-hot consumer group loop.")
    worker_live_hot.add_argument("--consumer-name", default="worker-live-hot-1", help="Redis consumer name for the live-hot worker.")
    worker_live_hot.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_live_warm = subparsers.add_parser("worker-live-warm", help="Run the live-warm consumer group loop.")
    worker_live_warm.add_argument("--consumer-name", default="worker-live-warm-1", help="Redis consumer name for the live-warm worker.")
    worker_live_warm.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_maintenance = subparsers.add_parser("worker-maintenance", help="Run the maintenance/recovery consumer group loop.")
    worker_maintenance.add_argument("--consumer-name", default="worker-maintenance-1", help="Redis consumer name for the maintenance worker.")
    worker_maintenance.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")
    return parser


def _print_batch_report(label: str, report: HydrationBatchReport) -> None:
    print(f"{label} events={len(report.processed_event_ids)} event_ids={','.join(str(item) for item in report.processed_event_ids)}")


def _print_db_audit_report(report) -> None:
    special = " ".join(f"{name}={count}" for name, count in sorted(report.special_counts.items()))
    suffix = f" {special}" if special else ""
    print(
        "db_audit "
        f"sport={report.sport_slug} "
        f"events={report.event_count} "
        f"requests={report.raw_requests} "
        f"snapshots={report.raw_snapshots} "
        f"event_rows={report.events} "
        f"statistics={report.statistics} "
        f"incidents={report.incidents} "
        f"lineup_sides={report.lineup_sides} "
        f"lineup_players={report.lineup_players}"
        f"{suffix}"
    )


def _configure_logging(level_name: str) -> None:
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stdout,
    )


def _load_redis_backend(redis_url: str | None, *, allow_memory_fallback: bool):
    env = _load_project_env()
    resolved_url = redis_url or env.get("REDIS_URL") or env.get("SOFASCORE_REDIS_URL")
    if not resolved_url:
        if allow_memory_fallback:
            logger.warning("Redis URL missing; falling back to in-memory backend because --allow-memory-redis is enabled.")
            return _MemoryRedisBackend()
        raise RuntimeError("Redis is required for production runs. Set REDIS_URL or SOFASCORE_REDIS_URL.")
    try:
        import redis  # type: ignore
    except ImportError as exc:
        if allow_memory_fallback:
            logger.warning("Python package `redis` is not installed; falling back to in-memory backend because --allow-memory-redis is enabled.")
            return _MemoryRedisBackend()
        raise RuntimeError("Redis is required for production runs. Install python package `redis`.") from exc
    backend = redis.Redis.from_url(resolved_url, decode_responses=True)
    backend.ping()
    return backend


def _load_project_env() -> dict[str, str]:
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


class _MemoryRedisBackend:
    def __init__(self) -> None:
        self.hashes = {}
        self.sorted_sets = {}
        self.streams = {}
        self.pending = {}
        self.pending_meta = {}
        self.claimed_by = {}
        self.group_offsets = {}
        self.counters = {}
        self.created_groups = []
        self.values = {}

    def set(self, key: str, value: str, *, nx: bool = False, px: int | None = None, now_ms: int | None = None) -> bool:
        observed_at = int(now_ms if now_ms is not None else 0)
        self._expire_scalar_key(key, now_ms=observed_at)
        if nx and key in self.values:
            return False
        self.values[key] = {
            "value": str(value),
            "expires_at": None if px is None else observed_at + int(px),
        }
        return True

    def exists(self, key: str, *, now_ms: int | None = None) -> bool:
        observed_at = int(now_ms if now_ms is not None else 0)
        self._expire_scalar_key(key, now_ms=observed_at)
        return key in self.values

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        bucket = self.hashes.setdefault(key, {})
        bucket.update(dict(mapping))
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))

    def hdel(self, key: str, *members: str) -> int:
        bucket = self.hashes.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.sorted_sets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[str(member)] = float(score)
        return len(mapping)

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.sorted_sets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    def zrangebyscore(self, key: str, min_score: float, max_score: float, *, start: int = 0, num: int | None = None, withscores: bool = False):
        items = [
            (member, score)
            for member, score in sorted(self.sorted_sets.get(key, {}).items(), key=lambda item: (item[1], item[0]))
            if min_score <= score <= max_score
        ]
        sliced = items[start : start + num if num is not None else None]
        if withscores:
            return sliced
        return [member for member, _ in sliced]

    def xadd(self, stream: str, fields: dict[str, str]) -> str:
        counter = self.counters.get(stream, 0) + 1
        self.counters[stream] = counter
        message_id = f"1-{counter}"
        self.streams.setdefault(stream, []).append((message_id, dict(fields)))
        return message_id

    def xgroup_create(self, stream: str, group: str, *, id: str = "0-0", mkstream: bool = False) -> bool:
        group_key = (stream, group)
        if group_key in self.group_offsets:
            raise RuntimeError("BUSYGROUP Consumer Group name already exists")
        if mkstream:
            self.streams.setdefault(stream, [])
        self.group_offsets[group_key] = 0
        self.created_groups.append((stream, group, id))
        return True

    def xreadgroup(self, group: str, consumer: str, streams: dict[str, str], *, count: int | None = None, block: int | None = None):
        del block
        results = []
        for stream in streams:
            offset = self.group_offsets.get((stream, group), 0)
            messages = self.streams.get(stream, [])[offset : offset + (count or len(self.streams.get(stream, [])))]
            if messages:
                self.group_offsets[(stream, group)] = offset + len(messages)
                self.pending.setdefault((stream, group), set()).update(message_id for message_id, _ in messages)
                pending_meta = self.pending_meta.setdefault((stream, group), {})
                claimed_by = self.claimed_by.setdefault((stream, group), {})
                for message_id, _ in messages:
                    pending_meta[message_id] = {
                        "consumer": consumer,
                        "idle": 0,
                        "deliveries": 1,
                    }
                    claimed_by[message_id] = consumer
                results.append((stream, messages))
        return results

    def xack(self, stream: str, group: str, *message_ids: str) -> int:
        pending = self.pending.setdefault((stream, group), set())
        pending_meta = self.pending_meta.setdefault((stream, group), {})
        claimed_by = self.claimed_by.setdefault((stream, group), {})
        acknowledged = 0
        for message_id in message_ids:
            if message_id in pending:
                pending.remove(message_id)
                pending_meta.pop(message_id, None)
                claimed_by.pop(message_id, None)
                acknowledged += 1
        return acknowledged

    def xpending(self, stream: str, group: str) -> dict[str, object]:
        pending_meta = self.pending_meta.setdefault((stream, group), {})
        consumers = {}
        for metadata in pending_meta.values():
            consumer = str(metadata["consumer"])
            consumers[consumer] = consumers.get(consumer, 0) + 1
        message_ids = sorted(pending_meta)
        return {
            "pending": len(pending_meta),
            "min": message_ids[0] if message_ids else None,
            "max": message_ids[-1] if message_ids else None,
            "consumers": consumers,
        }

    def xpending_range(self, stream: str, group: str, min: str, max: str, count: int, consumername: str | None = None):
        del min, max
        pending_meta = self.pending_meta.setdefault((stream, group), {})
        rows = []
        for message_id in sorted(pending_meta):
            metadata = pending_meta[message_id]
            if consumername is not None and metadata["consumer"] != consumername:
                continue
            rows.append(
                {
                    "message_id": message_id,
                    "consumer": metadata["consumer"],
                    "time_since_delivered": metadata["idle"],
                    "times_delivered": metadata["deliveries"],
                }
            )
            if len(rows) >= count:
                break
        return rows

    def xautoclaim(self, stream: str, group: str, consumer: str, min_idle_time: int, start_id: str, *, count: int = 100):
        del start_id
        pending_meta = self.pending_meta.setdefault((stream, group), {})
        claimed_by = self.claimed_by.setdefault((stream, group), {})
        claimed = []
        for message_id in sorted(pending_meta):
            metadata = pending_meta[message_id]
            if int(metadata["idle"]) < min_idle_time:
                continue
            metadata["consumer"] = consumer
            metadata["idle"] = 0
            metadata["deliveries"] = int(metadata["deliveries"]) + 1
            claimed_by[message_id] = consumer
            payload = self._lookup_message(stream, message_id)
            claimed.append((message_id, dict(payload)))
            if len(claimed) >= count:
                break
        return ("0-0", claimed, [])

    def _lookup_message(self, stream: str, message_id: str) -> dict[str, str]:
        for current_message_id, values in self.streams.get(stream, []):
            if current_message_id == message_id:
                return values
        raise KeyError(message_id)

    def _expire_scalar_key(self, key: str, *, now_ms: int) -> None:
        current = self.values.get(key)
        if current is None:
            return
        expires_at = current.get("expires_at")
        if expires_at is not None and int(expires_at) <= int(now_ms):
            del self.values[key]


if __name__ == "__main__":
    raise SystemExit(main())
