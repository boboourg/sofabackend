"""Unified cutover CLI for the hybrid ETL backbone."""

from __future__ import annotations

import argparse
import asyncio
from collections import deque
import inspect
import logging
import os
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path

from .db import AsyncpgDatabase, load_database_config
from .coverage_policy import lineup_recheck_window_open
from .endpoints import hybrid_runtime_registry_entries_for_sport
from .event_endpoint_negative_cache import EventEndpointNegativeCache, load_event_negative_cache_settings
from .fetch_executor import FetchExecutor, PrefetchedFetchRecord, build_fetch_task_key
from .final_sweep_gate import FinalSweepGate
from .live_bootstrap import LiveBootstrapCoordinator
from .normalizers.sink import DurableNormalizeSink
from .normalizers.worker import NormalizeWorker
from .ops.db_audit import collect_db_audit, persist_audit_coverage
from .ops.health import collect_health_report
from .ops.recovery import rebuild_live_state_from_postgres
from .parsers.base import RawSnapshot
from .parsers.registry import ParserRegistry
from .pipeline.pilot_orchestrator import PilotOrchestrator, PilotRunReport
from .planner.planner import Planner
from .queue.live_state import LiveEventStateStore
from .queue.freshness import FreshnessStore
from .queue.proxy_state import ProxyStateStore
from .queue.streams import RedisStreamQueue
from .runtime import (
    HISTORICAL_PROXY_ENV_KEY,
    _load_project_env,
    load_historical_runtime_config,
    load_runtime_config,
    load_structure_runtime_config,
)
from .season_widget_negative_cache import SeasonWidgetNegativeCache, load_negative_cache_settings
from .services.historical_archive_service import (
    run_historical_tournament_archive as run_historical_tournament_archive_service,
)
from .services.historical_archive_service import (
    run_historical_tournament_entities_batch as run_historical_tournament_entities_batch_service,
)
from .services.historical_archive_service import (
    run_historical_tournament_event_detail_batch as run_historical_tournament_event_detail_batch_service,
)
from .services.historical_archive_service import (
    run_historical_tournament_enrichment as run_historical_tournament_enrichment_service,
)
from .services.proxy_health_monitor import ProxyHealthMonitor, ProxyHealthMonitorConfig
from .services.stage_audit_logger import StageAuditLogger
from .services.structure_sync_service import (
    run_structure_sync_for_tournament as run_structure_sync_service,
)
from .services.service_app import ServiceApp
from .sofascore_client import SofascoreClient
from .sources import build_source_adapter
from .storage.capability_repository import CapabilityRepository
from .storage.coverage_repository import CoverageRepository
from .storage.endpoint_negative_cache_repository import EndpointNegativeCacheRepository
from .storage.event_endpoint_negative_cache_repository import EventEndpointNegativeCacheRepository
from .storage.live_state_repository import LiveStateRepository
from .storage.normalize_repository import NormalizeRepository
from .storage.proxy_health_repository import ProxyHealthRepository
from .storage.raw_repository import RawRepository
from .transport import InspectorTransport

logger = logging.getLogger(__name__)

DEFAULT_EVENT_COVERAGE_SURFACES = ("event_core", "statistics", "incidents", "lineups")


@dataclass(frozen=True)
class HydrationBatchReport:
    processed_event_ids: tuple[int, ...]
    results: tuple[object, ...]


@dataclass(frozen=True)
class ReplayBatchReport:
    snapshot_ids: tuple[int, ...]
    parser_families: tuple[str, ...]


@dataclass(frozen=True)
class PrefetchedRun:
    event_id: int
    sport_slug: str
    fetch_records: tuple[PrefetchedFetchRecord, ...]
    snapshot_store: "HybridSnapshotStore"
    initial_capability_rollup: dict[str, str]
    widget_negative_cache_events: tuple[object, ...] = ()
    replay_widget_gate: object | None = None
    event_negative_cache_events: tuple[object, ...] = ()
    replay_event_endpoint_gate: object | None = None

    @property
    def total_payload_size_bytes(self) -> int:
        return sum(
            int(record.payload_snapshot.payload_size_bytes or 0)
            for record in self.fetch_records
            if record.payload_snapshot is not None
        )

    @property
    def endpoint_count(self) -> int:
        return len(self.fetch_records)


class HybridSnapshotStore:
    def __init__(self, repository: RawRepository, sql_executor) -> None:
        self.repository = repository
        self.sql_executor = sql_executor
        self._cache: dict[int, RawSnapshot] = {}
        self._next_prefetched_snapshot_id = -1

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

    def stage_snapshot(self, record) -> int:
        snapshot_id = int(self._next_prefetched_snapshot_id)
        self._next_prefetched_snapshot_id -= 1
        self._cache[snapshot_id] = RawSnapshot(
            snapshot_id=snapshot_id,
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

    def remap_snapshot_id(self, old_snapshot_id: int, new_snapshot_id: int) -> None:
        snapshot = self._cache.pop(int(old_snapshot_id), None)
        if snapshot is None:
            return
        if int(new_snapshot_id) not in self._cache:
            self._cache[int(new_snapshot_id)] = replace(snapshot, snapshot_id=int(new_snapshot_id))

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
        if self.sql_executor is None:
            raise KeyError(snapshot_id)
        snapshot = await self.repository.fetch_payload_snapshot(self.sql_executor, snapshot_id)
        self._cache[snapshot_id] = snapshot
        return snapshot


class ReplayFetchExecutor:
    def __init__(self, prefetched_run: PrefetchedRun) -> None:
        self._records_by_key: dict[tuple[object, ...], deque[PrefetchedFetchRecord]] = {}
        for record in prefetched_run.fetch_records:
            self._records_by_key.setdefault(build_fetch_task_key(record.task), deque()).append(record)

    async def execute(self, task) -> object:
        key = build_fetch_task_key(task)
        queued = self._records_by_key.get(key)
        if not queued:
            raise RuntimeError(f"No prefetched fetch outcome available for task={key!r}")
        return queued.popleft().outcome


class HybridApp:
    def __init__(self, *, database: AsyncpgDatabase, runtime_config, redis_backend) -> None:
        self.database = database
        self.runtime_config = runtime_config
        self.redis_backend = redis_backend
        self.transport = InspectorTransport(runtime_config)
        self.raw_repository = RawRepository()
        self.capability_repository = CapabilityRepository()
        self.live_state_repository = LiveStateRepository()
        self.normalize_repository = NormalizeRepository(redis_backend=redis_backend)
        self.endpoint_negative_cache_repository = EndpointNegativeCacheRepository()
        self.negative_cache_settings = load_negative_cache_settings()
        self.event_endpoint_negative_cache_repository = EventEndpointNegativeCacheRepository()
        self.event_negative_cache_settings = load_event_negative_cache_settings()
        self.stage_audit_logger = StageAuditLogger(database=database)
        self.live_state_store = LiveEventStateStore(redis_backend) if redis_backend is not None else None
        self.stream_queue = RedisStreamQueue(redis_backend) if redis_backend is not None else None
        self.freshness_store = FreshnessStore(redis_backend) if redis_backend is not None else None
        self.live_bootstrap_coordinator = (
            LiveBootstrapCoordinator(redis_backend=redis_backend, worker_id="hybrid-app")
            if redis_backend is not None
            else None
        )
        self.final_sweep_gate = FinalSweepGate()
        self.capability_rollup: dict[str, str] = {}
        self._seeded_endpoint_registry_sports: set[str] = set()
        # Structural sync contour uses a separate non-residential proxy pool.
        # Both are lazily initialised (so unrelated CLI flows don't require
        # SCHEMA_INSPECTOR_STRUCTURE_PROXY_URLS to be set).
        self._structure_runtime_config = None
        self._structure_transport = None
        self._source_adapter = None
        self._event_list_job = None

    def ensure_structure_runtime(self):
        """Lazily build a non-residential RuntimeConfig + transport for the
        structural-sync contour. Raises if the operator forgot to configure
        ``SCHEMA_INSPECTOR_STRUCTURE_PROXY_URLS`` — structure-sync must never
        silently borrow the live/residential pool.
        """

        if self._structure_runtime_config is None:
            self._structure_runtime_config = load_structure_runtime_config()
            self._structure_transport = InspectorTransport(self._structure_runtime_config)
        return self._structure_runtime_config, self._structure_transport

    async def close(self) -> None:
        await self.transport.close()
        if self._structure_transport is not None:
            await self._structure_transport.close()
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
        requested_hydration_mode = str(hydration_mode or "full").strip().lower()
        effective_hydration_mode = requested_hydration_mode
        should_mark_live_bootstrap = False
        if requested_hydration_mode == "live_delta" and self.live_bootstrap_coordinator is not None:
            async with self.database.connection() as connection:
                is_bootstrapped = await self.live_bootstrap_coordinator.is_bootstrapped(connection, event_id=event_id)
            if not is_bootstrapped:
                if not self.live_bootstrap_coordinator.acquire_hydrate_lock(event_id=event_id):
                    return PilotRunReport(
                        sport_slug=str(resolved_sport_slug or "football"),
                        event_id=int(event_id),
                        fetch_outcomes=(),
                        parse_results=(),
                    )
                effective_hydration_mode = "full"
                should_mark_live_bootstrap = True
        prefetched_run = await self._prefetch_event_run(
            event_id=event_id,
            sport_slug=str(resolved_sport_slug or "football"),
            hydration_mode=effective_hydration_mode,
        )
        self._warn_if_prefetched_run_large(prefetched_run)
        committed_run = await self._commit_prefetched_run(prefetched_run)
        result = await self._persist_prefetched_run(
            committed_run,
            hydration_mode=effective_hydration_mode,
        )
        if requested_hydration_mode == "live_delta" and self.live_bootstrap_coordinator is not None:
            async with self.database.transaction() as connection:
                if getattr(result, "finalized", False):
                    await self.live_bootstrap_coordinator.reset_bootstrap(connection, event_id=event_id)
                elif should_mark_live_bootstrap:
                    await self.live_bootstrap_coordinator.mark_bootstrapped(connection, event_id=event_id)
        return result
    async def _prefetch_event_run(
        self,
        *,
        event_id: int,
        sport_slug: str,
        hydration_mode: str,
    ) -> PrefetchedRun:
        initial_capability_rollup = dict(self.capability_rollup)
        snapshot_store = HybridSnapshotStore(self.raw_repository, None)
        prefetch_executor = FetchExecutor(
            transport=self.transport,
            raw_repository=self.raw_repository,
            sql_executor=None,
            snapshot_store=snapshot_store,
            write_mode="deferred",
            freshness_store=self.freshness_store,
        )
        season_widget_gate = None
        event_endpoint_gate = None
        async with self.database.connection() as read_connection:
            if self.negative_cache_settings.enabled:
                season_widget_gate = SeasonWidgetNegativeCache(
                    repository=self.endpoint_negative_cache_repository,
                    sql_executor=read_connection,
                    settings=self.negative_cache_settings,
                )
            if self.event_negative_cache_settings.enabled:
                event_endpoint_gate = EventEndpointNegativeCache(
                    repository=self.event_endpoint_negative_cache_repository,
                    sql_executor=read_connection,
                    settings=self.event_negative_cache_settings,
                )
            orchestrator = PilotOrchestrator(
                fetch_executor=prefetch_executor,
                snapshot_store=snapshot_store,
                normalize_worker=NormalizeWorker(ParserRegistry.default(), result_sink=None),
                planner=Planner(capability_rollup=dict(initial_capability_rollup)),
                capability_repository=None,
                sql_executor=None,
                live_state_store=None,
                live_state_repository=None,
                stream_queue=None,
                season_widget_gate=season_widget_gate,
                event_endpoint_gate=event_endpoint_gate,
                final_sweep_gate=self.final_sweep_gate,
                freshness_store=self.freshness_store,
            )
            await orchestrator.run_event(
                event_id=event_id,
                sport_slug=sport_slug,
                hydration_mode=hydration_mode,
            )
        return PrefetchedRun(
            event_id=event_id,
            sport_slug=sport_slug,
            fetch_records=prefetch_executor.prefetched_records,
            snapshot_store=snapshot_store,
            initial_capability_rollup=initial_capability_rollup,
            widget_negative_cache_events=season_widget_gate.events if season_widget_gate is not None else (),
            replay_widget_gate=season_widget_gate.build_replay_gate() if season_widget_gate is not None else None,
            event_negative_cache_events=event_endpoint_gate.events if event_endpoint_gate is not None else (),
            replay_event_endpoint_gate=event_endpoint_gate.build_replay_gate() if event_endpoint_gate is not None else None,
        )

    async def _commit_prefetched_run(self, prefetched_run: PrefetchedRun) -> PrefetchedRun:
        async with self.database.transaction() as connection:
            raw_write_executor = FetchExecutor(
                transport=self.transport,
                raw_repository=self.raw_repository,
                sql_executor=connection,
            )
            committed_records: list[PrefetchedFetchRecord] = []
            for record in prefetched_run.fetch_records:
                committed_outcome = await raw_write_executor.commit_prefetched_record(record)
                if record.outcome.snapshot_id is not None and committed_outcome.snapshot_id is not None:
                    prefetched_run.snapshot_store.remap_snapshot_id(
                        int(record.outcome.snapshot_id),
                        int(committed_outcome.snapshot_id),
                    )
                committed_snapshot_head = record.snapshot_head
                if committed_outcome.snapshot_id is not None and record.snapshot_head is not None:
                    committed_snapshot_head = replace(
                        record.snapshot_head,
                        latest_snapshot_id=int(committed_outcome.snapshot_id),
                    )
                committed_records.append(
                    replace(
                        record,
                        outcome=committed_outcome,
                        snapshot_head=committed_snapshot_head,
                    )
                )
        return replace(prefetched_run, fetch_records=tuple(committed_records))

    async def _persist_prefetched_run(
        self,
        prefetched_run: PrefetchedRun,
        *,
        hydration_mode: str,
    ):
        async with self.database.transaction() as connection:
            skip_entity_parser_families = {"event_root"} if hydration_mode == "core" else set()
            planner = Planner(capability_rollup=dict(prefetched_run.initial_capability_rollup))
            orchestrator = PilotOrchestrator(
                fetch_executor=ReplayFetchExecutor(prefetched_run),
                snapshot_store=prefetched_run.snapshot_store,
                normalize_worker=NormalizeWorker(
                    ParserRegistry.default(),
                    result_sink=DurableNormalizeSink(
                        self.normalize_repository,
                        connection,
                        skip_entity_parser_families=skip_entity_parser_families,
                    ),
                ),
                planner=planner,
                capability_repository=self.capability_repository,
                sql_executor=connection,
                live_state_store=self.live_state_store,
                live_state_repository=self.live_state_repository,
                stream_queue=self.stream_queue,
                season_widget_gate=prefetched_run.replay_widget_gate,
                event_endpoint_gate=prefetched_run.replay_event_endpoint_gate,
                freshness_store=self.freshness_store,
            )
            result = await orchestrator.run_event(
                event_id=prefetched_run.event_id,
                sport_slug=prefetched_run.sport_slug,
                hydration_mode=hydration_mode,
            )
            if self.event_negative_cache_settings.enabled and prefetched_run.event_negative_cache_events:
                await self.event_endpoint_negative_cache_repository.apply_events(
                    connection,
                    prefetched_run.event_negative_cache_events,
                )
            if self.negative_cache_settings.enabled and prefetched_run.widget_negative_cache_events:
                await self.endpoint_negative_cache_repository.apply_events(
                    connection,
                    prefetched_run.widget_negative_cache_events,
                )
        self.capability_rollup.update(planner.capability_rollup)
        return result

    def _warn_if_prefetched_run_large(self, prefetched_run: PrefetchedRun) -> None:
        limit_bytes = _prefetched_run_size_limit_bytes()
        total_bytes = prefetched_run.total_payload_size_bytes
        if total_bytes <= limit_bytes:
            return
        logger.warning(
            "Prefetched run exceeded in-memory payload budget: event_id=%s endpoint_count=%s total_payload_size_bytes=%s limit_bytes=%s",
            prefetched_run.event_id,
            prefetched_run.endpoint_count,
            total_bytes,
            limit_bytes,
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

    async def write_db_audit_coverage(self, *, report):
        async with self.database.transaction() as connection:
            return await persist_audit_coverage(
                sql_executor=connection,
                source_slug=self.runtime_config.source_slug,
                report=report,
            )

    async def discover_live_event_ids(self, *, sport_slug: str, timeout: float) -> tuple[int, ...]:
        result = await self.discover_live_events(sport_slug=sport_slug, timeout=timeout)
        return tuple(int(item.id) for item in result.parsed.events)

    async def discover_scheduled_event_ids(self, *, sport_slug: str, date: str, timeout: float) -> tuple[int, ...]:
        result = await self.discover_scheduled_events(sport_slug=sport_slug, date=date, timeout=timeout)
        return tuple(int(item.id) for item in result.parsed.events)

    async def discover_live_events(self, *, sport_slug: str, timeout: float):
        return await self._get_event_list_job().run_live(sport_slug=sport_slug, timeout=timeout)

    async def discover_scheduled_events(self, *, sport_slug: str, date: str, timeout: float):
        return await self._get_event_list_job().run_scheduled(date, sport_slug=sport_slug, timeout=timeout)

    def _get_source_adapter(self):
        if self._source_adapter is None:
            self._source_adapter = build_source_adapter(
                self.runtime_config.source_slug,
                runtime_config=self.runtime_config,
                transport=self.transport,
            )
        return self._source_adapter

    def _get_event_list_job(self):
        if self._event_list_job is not None:
            return self._event_list_job
        self._event_list_job = self._get_source_adapter().build_event_list_job(self.database)
        return self._event_list_job

    async def select_unique_tournament_ids_after_cursor(
        self,
        *,
        sport_slug: str,
        after_unique_tournament_id: int,
        limit: int,
        allowed_unique_tournament_ids: tuple[int, ...] = (),
    ) -> tuple[int, ...]:
        normalized_allowed_ids = tuple(
            int(item) for item in allowed_unique_tournament_ids if int(item) > 0
        )
        async with self.database.connection() as connection:
            rows = await connection.fetch(
                """
                SELECT ut.id
                FROM unique_tournament AS ut
                JOIN category AS c ON c.id = ut.category_id
                JOIN sport AS s ON s.id = c.sport_id
                WHERE s.slug = $1
                  AND ut.id > $2
                  AND (
                    cardinality($3::bigint[]) = 0
                    OR ut.id = ANY($3::bigint[])
                  )
                ORDER BY ut.id
                LIMIT $4
                """,
                sport_slug,
                int(after_unique_tournament_id),
                list(normalized_allowed_ids),
                max(1, int(limit)),
            )
        return tuple(int(row["id"]) for row in rows if row["id"] is not None)

    async def run_historical_tournament_archive(self, *, unique_tournament_id: int, sport_slug: str):
        return await run_historical_tournament_archive_service(
            self,
            unique_tournament_id=unique_tournament_id,
            sport_slug=sport_slug,
        )

    async def run_historical_tournament_enrichment(
        self,
        *,
        unique_tournament_id: int,
        sport_slug: str,
        season_ids: tuple[int, ...] = (),
    ):
        return await run_historical_tournament_enrichment_service(
            self,
            unique_tournament_id=unique_tournament_id,
            sport_slug=sport_slug,
            season_ids=season_ids,
        )

    async def run_historical_tournament_event_detail_batch(
        self,
        *,
        unique_tournament_id: int,
        sport_slug: str,
        season_ids: tuple[int, ...] = (),
    ):
        return await run_historical_tournament_event_detail_batch_service(
            self,
            unique_tournament_id=unique_tournament_id,
            sport_slug=sport_slug,
            season_ids=season_ids,
        )

    async def run_historical_tournament_entities_batch(
        self,
        *,
        unique_tournament_id: int,
        sport_slug: str,
        season_ids: tuple[int, ...] = (),
    ):
        return await run_historical_tournament_entities_batch_service(
            self,
            unique_tournament_id=unique_tournament_id,
            sport_slug=sport_slug,
            season_ids=season_ids,
        )

    async def run_structure_sync_for_tournament(
        self,
        *,
        unique_tournament_id: int,
        sport_slug: str,
    ):
        """Entry point used by ``StructureSyncWorker``.

        Always runs through the non-residential proxy pool — that's enforced
        inside ``ensure_structure_runtime`` (fail-fast if the env key is missing).
        """

        runtime_config, transport = self.ensure_structure_runtime()
        return await run_structure_sync_service(
            self,
            unique_tournament_id=int(unique_tournament_id),
            sport_slug=sport_slug,
            runtime_config=runtime_config,
            transport=transport,
        )

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

    async def select_event_ids_for_missing_coverage(
        self,
        *,
        limit: int | None,
        offset: int,
        sport_slug: str | None,
        surface_names: tuple[str, ...],
    ) -> tuple[int, ...]:
        normalized_surfaces = tuple(str(item) for item in surface_names if str(item)) or DEFAULT_EVENT_COVERAGE_SURFACES
        repository = CoverageRepository()
        async with self.database.connection() as connection:
            rows = await repository.fetch_event_scope_statuses(
                connection,
                source_slug=self.runtime_config.source_slug,
                surface_names=normalized_surfaces,
                freshness_statuses=("missing", "partial", "possible"),
                sport_slug=sport_slug,
            )
        now_timestamp = int(time.time())
        selected_event_ids: list[int] = []
        seen_event_ids: set[int] = set()
        skip_count = max(0, int(offset or 0))
        for row in rows:
            if row.scope_id in seen_event_ids:
                continue
            if not _coverage_scope_needs_refill(row, now_timestamp=now_timestamp):
                continue
            seen_event_ids.add(row.scope_id)
            if skip_count > 0:
                skip_count -= 1
                continue
            selected_event_ids.append(int(row.scope_id))
            if limit is not None and len(selected_event_ids) >= max(0, int(limit)):
                break
        return tuple(selected_event_ids)

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
                stream_queue=self.stream_queue,
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


def _coverage_scope_needs_refill(row, *, now_timestamp: int) -> bool:
    surface_name = str(getattr(row, "surface_name", "") or "")
    freshness_status = str(getattr(row, "freshness_status", "") or "")
    if surface_name != "lineups":
        return freshness_status in {"missing", "partial"}
    if freshness_status not in {"missing", "partial", "possible"}:
        return False
    return lineup_recheck_window_open(
        start_timestamp=getattr(row, "start_timestamp", None),
        now_timestamp=now_timestamp,
    )


async def run_full_backfill_command(args, *, orchestrator, event_selector) -> HydrationBatchReport:
    explicit_event_ids = tuple(int(item) for item in getattr(args, "event_id", ()) or ())
    if explicit_event_ids:
        event_ids = explicit_event_ids
    elif bool(getattr(args, "coverage_missing", False)):
        surface_names = tuple(str(item) for item in (getattr(args, "coverage_surface", ()) or ()) if str(item))
        event_ids = tuple(
            await event_selector.select_event_ids_for_missing_coverage(
                limit=args.limit,
                offset=args.offset,
                sport_slug=getattr(args, "sport_slug", None),
                surface_names=surface_names or DEFAULT_EVENT_COVERAGE_SURFACES,
            )
        )
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
    write_db_audit_coverage = getattr(app, "write_db_audit_coverage", None)
    if callable(write_db_audit_coverage):
        await write_db_audit_coverage(report=audit_report)
    return audit_report


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    parser = _build_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.log_level)
    return asyncio.run(_dispatch(args))


_HISTORICAL_COMMANDS = frozenset({
    "worker-historical-hydrate",
    "worker-historical-discovery",
    "worker-historical-tournament",
    "worker-historical-enrichment",
    "worker-historical-maintenance",
    "historical-planner-daemon",
    "historical-tournament-planner-daemon",
})


async def _dispatch(args) -> int:
    command = getattr(args, "command", None)
    if command in _HISTORICAL_COMMANDS:
        historical_env = None
        if args.proxy:
            historical_env = _load_project_env()
            historical_env[HISTORICAL_PROXY_ENV_KEY] = ",".join(args.proxy)
            singular_key = HISTORICAL_PROXY_ENV_KEY[: -len("_PROXY_URLS")] + "_PROXY_URL"
            historical_env.pop(singular_key, None)
        runtime_config = load_historical_runtime_config(
            env=historical_env,
            user_agent=args.user_agent,
            max_attempts=args.max_attempts,
        )
    else:
        runtime_config = load_runtime_config(
            proxy_urls=args.proxy or None,
            user_agent=args.user_agent,
            max_attempts=args.max_attempts,
        )
    if _normalized_source_slug(getattr(args, "source", None)) is not None:
        runtime_config = replace(runtime_config, source_slug=_normalized_source_slug(args.source))
    database_config = load_database_config(
        dsn=args.database_url,
        min_size=args.db_min_size,
        max_size=args.db_max_size,
        command_timeout=args.db_timeout,
    )
    async with AsyncpgDatabase(database_config) as database:
        redis_backend = _load_redis_backend(
            args.redis_url,
            allow_memory_fallback=bool(getattr(args, "allow_memory_redis", False)),
        )
        if args.command == "proxy-health-monitor":
            monitor = ProxyHealthMonitor(
                repository=ProxyHealthRepository(),
                state_store=ProxyStateStore(redis_backend),
                config=ProxyHealthMonitorConfig.from_env(_load_project_env()),
            )
            await monitor.run_forever(database.connection)
            return 0
        app = HybridApp(
            database=database,
            runtime_config=runtime_config,
            redis_backend=redis_backend,
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
                coverage_alert_summary = getattr(report, "coverage_alert_summary", None)
                reconcile_policy_summary = getattr(report, "reconcile_policy_summary", None)
                coverage_alert_count = int(
                    getattr(coverage_alert_summary, "flag_count", 0)
                    if coverage_alert_summary is not None
                    else int(getattr(report.coverage_summary, "stale_scope_count", 0) > 0)
                )
                reconcile_source_count = int(
                    getattr(reconcile_policy_summary, "source_count", 0)
                    if reconcile_policy_summary is not None
                    else 0
                )
                primary_source_slug = (
                    getattr(reconcile_policy_summary, "primary_source_slug", None)
                    if reconcile_policy_summary is not None
                    else None
                ) or "none"
                go_live = getattr(report, "go_live", None)
                go_live_ready = bool(getattr(go_live, "ready", False))
                go_live_flag_count = int(getattr(go_live, "flag_count", 0) or 0)
                print(
                    "health "
                    f"db_ok={int(report.database_ok)} "
                    f"redis_ok={int(report.redis_ok)} "
                    f"redis_backend={report.redis_backend_kind} "
                    f"snapshots={report.snapshot_count} "
                    f"rollups={report.capability_rollup_count} "
                    f"live_hot={report.live_hot_count} "
                    f"live_warm={report.live_warm_count} "
                    f"live_cold={report.live_cold_count} "
                    f"coverage_tracked={report.coverage_summary.tracked_scope_count} "
                    f"coverage_stale={report.coverage_summary.stale_scope_count} "
                    f"coverage_alerts={coverage_alert_count} "
                    f"reconcile_sources={reconcile_source_count} "
                    f"primary_source={primary_source_slug} "
                    f"drift_flags={report.drift_summary.flag_count} "
                    f"go_live={int(go_live_ready)} "
                    f"gate_flags={go_live_flag_count}"
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
            if args.command == "live-discovery-planner-daemon":
                service_app = ServiceApp(app)
                await service_app.run_live_discovery_planner_daemon(
                    sport_slugs=tuple(args.sport_slug or ()),
                    loop_interval_seconds=args.loop_interval_seconds,
                )
                return 0
            if args.command == "historical-planner-daemon":
                service_app = ServiceApp(app)
                await service_app.run_historical_planner_daemon(
                    sport_slugs=tuple(args.sport_slug or ()),
                    date_from=args.date_from,
                    date_to=args.date_to,
                    dates_per_tick=args.dates_per_tick,
                    loop_interval_seconds=args.loop_interval_seconds,
                )
                return 0
            if args.command == "historical-tournament-planner-daemon":
                service_app = ServiceApp(app)
                await service_app.run_historical_tournament_planner_daemon(
                    sport_slugs=tuple(args.sport_slug or ()),
                    tournaments_per_tick=args.tournaments_per_tick,
                    loop_interval_seconds=args.loop_interval_seconds,
                )
                return 0
            if args.command == "structure-planner-daemon":
                service_app = ServiceApp(app)
                await service_app.run_structure_planner_daemon(
                    sport_slugs=tuple(args.sport_slug or ()),
                    loop_interval_seconds=args.loop_interval_seconds,
                )
                return 0
            if args.command == "tournament-registry-refresh-daemon":
                service_app = ServiceApp(app)
                await service_app.run_tournament_registry_refresh_daemon(
                    sport_slugs=tuple(args.sport_slug or ()),
                    refresh_interval_seconds=args.refresh_interval_seconds,
                    loop_interval_seconds=args.loop_interval_seconds,
                    sports_per_tick=args.sports_per_tick,
                    timeout_s=args.timeout,
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
            if args.command == "worker-live-discovery":
                service_app = ServiceApp(app)
                await service_app.run_live_discovery_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                    timeout_s=args.timeout,
                )
                return 0
            if args.command == "worker-historical-discovery":
                service_app = ServiceApp(app)
                await service_app.run_historical_discovery_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                    timeout_s=args.timeout,
                )
                return 0
            if args.command == "worker-historical-tournament":
                service_app = ServiceApp(app)
                await service_app.run_historical_tournament_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
            if args.command == "worker-structure-sync":
                service_app = ServiceApp(app)
                await service_app.run_structure_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
            if args.command == "worker-historical-enrichment":
                service_app = ServiceApp(app)
                await service_app.run_historical_enrichment_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
            if args.command == "worker-hydrate":
                service_app = ServiceApp(app)
                await service_app.run_hydrate_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
            if args.command == "worker-historical-hydrate":
                service_app = ServiceApp(app)
                await service_app.run_historical_hydrate_worker(
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
            if args.command == "worker-live-tier-1":
                service_app = ServiceApp(app)
                await service_app.run_live_worker(
                    lane="tier_1",
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
            if args.command == "worker-live-tier-2":
                service_app = ServiceApp(app)
                await service_app.run_live_worker(
                    lane="tier_2",
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
            if args.command == "worker-live-tier-3":
                service_app = ServiceApp(app)
                await service_app.run_live_worker(
                    lane="tier_3",
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
            if args.command == "worker-historical-maintenance":
                service_app = ServiceApp(app)
                await service_app.run_historical_maintenance_worker(
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
    parser.add_argument("--source", default=None, help="Optional upstream source slug override.")
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
    backfill.add_argument(
        "--coverage-missing",
        action="store_true",
        help="Select tracked event ids from coverage_ledger where requested SofaScore surfaces are missing or partial.",
    )
    backfill.add_argument(
        "--coverage-surface",
        action="append",
        choices=DEFAULT_EVENT_COVERAGE_SURFACES,
        default=[],
        help="Repeatable coverage surface used with --coverage-missing. Defaults to all tracked event surfaces.",
    )
    backfill.add_argument("--event-concurrency", type=int, default=None, help="Optional concurrent event hydration limit.")
    backfill.add_argument("--audit-db", action="store_true", help="Run a post-hydration database audit and fail if raw/durable rows are missing.")

    replay = subparsers.add_parser("replay", help="Replay one or more payload snapshots through durable sinks.")
    replay.add_argument("--snapshot-id", type=int, action="append", required=True, help="Repeatable payload snapshot id.")

    audit = subparsers.add_parser("audit-db", help="Print a compact durable/raw database audit for event ids.")
    audit.add_argument("--sport-slug", required=True, help="Sport slug for special-table routing.")
    audit.add_argument("--event-id", type=int, action="append", required=True, help="Repeatable hydrated event id.")

    subparsers.add_parser("health", help="Print a compact hybrid health summary.")
    subparsers.add_parser("proxy-health-monitor", help="Continuously mark unhealthy proxy endpoints from api_request_log traffic.")
    subparsers.add_parser("recover-live-state", help="Rebuild Redis live-state indexes from PostgreSQL history.")

    planner_daemon = subparsers.add_parser("planner-daemon", help="Run the continuous planner loop.")
    planner_daemon.add_argument("--sport-slug", action="append", default=[], help="Optional repeatable sport slug. Defaults to all supported sports.")
    planner_daemon.add_argument("--scheduled-interval-seconds", type=float, default=3600.0, help="Scheduled planning interval per sport (default: 3600 = once per hour).")
    planner_daemon.add_argument("--loop-interval-seconds", type=float, default=5.0, help="Daemon tick loop interval.")

    live_discovery_planner_daemon = subparsers.add_parser("live-discovery-planner-daemon", help="Run the continuous live sport-surface planner loop.")
    live_discovery_planner_daemon.add_argument("--sport-slug", action="append", default=[], help="Optional repeatable sport slug. Defaults to all supported sports.")
    live_discovery_planner_daemon.add_argument("--loop-interval-seconds", type=float, default=5.0, help="Daemon tick loop interval.")

    historical_planner_daemon = subparsers.add_parser("historical-planner-daemon", help="Run the rolling historical planner loop.")
    historical_planner_daemon.add_argument("--sport-slug", action="append", default=[], help="Optional repeatable sport slug. Defaults to all supported sports.")
    historical_planner_daemon.add_argument("--date-from", help="Optional inclusive start date in YYYY-MM-DD format for manual override mode.")
    historical_planner_daemon.add_argument("--date-to", help="Optional inclusive end date in YYYY-MM-DD format for manual override mode.")
    historical_planner_daemon.add_argument("--dates-per-tick", type=int, default=1, help="Maximum archival dates to publish per sport on each planner tick.")
    historical_planner_daemon.add_argument("--loop-interval-seconds", type=float, default=5.0, help="Daemon tick loop interval.")

    historical_tournament_planner_daemon = subparsers.add_parser("historical-tournament-planner-daemon", help="Run the historical tournament/season planner loop.")
    historical_tournament_planner_daemon.add_argument("--sport-slug", action="append", default=[], help="Optional repeatable sport slug. Defaults to all supported sports.")
    historical_tournament_planner_daemon.add_argument("--consumer-name", default="historical-tournament-planner-1", help="Opaque planner instance label for logs and tmux naming.")
    historical_tournament_planner_daemon.add_argument("--tournaments-per-tick", type=int, default=10, help="Maximum archival tournaments to publish per sport on each planner tick.")
    historical_tournament_planner_daemon.add_argument("--loop-interval-seconds", type=float, default=10.0, help="Daemon tick loop interval.")

    structure_planner_daemon = subparsers.add_parser(
        "structure-planner-daemon",
        help="Run the skeleton-only tournament/season structure planner loop (non-residential proxies).",
    )
    structure_planner_daemon.add_argument(
        "--sport-slug",
        action="append",
        default=[],
        help="Optional repeatable sport slug. Defaults to all sports with structure_sync_mode != disabled.",
    )
    structure_planner_daemon.add_argument(
        "--consumer-name",
        default="structure-planner-1",
        help="Opaque planner instance label for logs and tmux naming.",
    )
    structure_planner_daemon.add_argument(
        "--loop-interval-seconds",
        type=float,
        default=30.0,
        help="Daemon tick loop interval (default: 30s).",
    )

    tournament_registry_refresh_daemon = subparsers.add_parser(
        "tournament-registry-refresh-daemon",
        help="Run the periodic tournament_registry refresh loop from category discovery.",
    )
    tournament_registry_refresh_daemon.add_argument(
        "--sport-slug",
        action="append",
        default=[],
        help="Optional repeatable sport slug. Defaults to all supported sports.",
    )
    tournament_registry_refresh_daemon.add_argument(
        "--refresh-interval-seconds",
        type=float,
        default=86400.0,
        help="Minimum refresh interval per sport in seconds (default: 86400 = once per day).",
    )
    tournament_registry_refresh_daemon.add_argument(
        "--loop-interval-seconds",
        type=float,
        default=300.0,
        help="Daemon tick loop interval (default: 300s).",
    )
    tournament_registry_refresh_daemon.add_argument(
        "--sports-per-tick",
        type=int,
        default=1,
        help="Maximum sports to refresh on one daemon tick (default: 1).",
    )

    worker_discovery = subparsers.add_parser("worker-discovery", help="Run the discovery consumer group loop.")
    worker_discovery.add_argument("--consumer-name", default="worker-discovery-1", help="Redis consumer name for the discovery worker.")
    worker_discovery.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_live_discovery = subparsers.add_parser("worker-live-discovery", help="Run the live discovery consumer group loop.")
    worker_live_discovery.add_argument("--consumer-name", default="worker-live-discovery-1", help="Redis consumer name for the live discovery worker.")
    worker_live_discovery.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_historical_discovery = subparsers.add_parser("worker-historical-discovery", help="Run the archival discovery consumer group loop.")
    worker_historical_discovery.add_argument("--consumer-name", default="worker-historical-discovery-1", help="Redis consumer name for the archival discovery worker.")
    worker_historical_discovery.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_historical_tournament = subparsers.add_parser("worker-historical-tournament", help="Run the archival tournament/season consumer group loop.")
    worker_historical_tournament.add_argument("--consumer-name", default="worker-historical-tournament-1", help="Redis consumer name for the archival tournament worker.")
    worker_historical_tournament.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_structure_sync = subparsers.add_parser(
        "worker-structure-sync",
        help="Run the skeleton-only structural-sync consumer group loop (non-residential proxies).",
    )
    worker_structure_sync.add_argument(
        "--consumer-name",
        default="worker-structure-sync-1",
        help="Redis consumer name for the structure-sync worker.",
    )
    worker_structure_sync.add_argument(
        "--block-ms",
        type=int,
        default=5000,
        help="XREADGROUP block timeout in milliseconds.",
    )

    worker_historical_enrichment = subparsers.add_parser("worker-historical-enrichment", help="Run the archival enrichment consumer group loop.")
    worker_historical_enrichment.add_argument("--consumer-name", default="worker-historical-enrichment-1", help="Redis consumer name for the archival enrichment worker.")
    worker_historical_enrichment.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_hydrate = subparsers.add_parser("worker-hydrate", help="Run the hydrate consumer group loop.")
    worker_hydrate.add_argument("--consumer-name", default="worker-hydrate-1", help="Redis consumer name for the hydrate worker.")
    worker_hydrate.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_historical_hydrate = subparsers.add_parser("worker-historical-hydrate", help="Run the archival hydrate consumer group loop.")
    worker_historical_hydrate.add_argument("--consumer-name", default="worker-historical-hydrate-1", help="Redis consumer name for the archival hydrate worker.")
    worker_historical_hydrate.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_live_hot = subparsers.add_parser("worker-live-hot", help="Run the live-hot consumer group loop.")
    worker_live_hot.add_argument("--consumer-name", default="worker-live-hot-1", help="Redis consumer name for the live-hot worker.")
    worker_live_hot.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_live_tier_1 = subparsers.add_parser("worker-live-tier-1", help="Run the live tier-1 consumer group loop.")
    worker_live_tier_1.add_argument("--consumer-name", default="worker-live-tier-1-1", help="Redis consumer name for the live tier-1 worker.")
    worker_live_tier_1.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_live_tier_2 = subparsers.add_parser("worker-live-tier-2", help="Run the live tier-2 consumer group loop.")
    worker_live_tier_2.add_argument("--consumer-name", default="worker-live-tier-2-1", help="Redis consumer name for the live tier-2 worker.")
    worker_live_tier_2.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_live_tier_3 = subparsers.add_parser("worker-live-tier-3", help="Run the live tier-3 consumer group loop.")
    worker_live_tier_3.add_argument("--consumer-name", default="worker-live-tier-3-1", help="Redis consumer name for the live tier-3 worker.")
    worker_live_tier_3.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_live_warm = subparsers.add_parser("worker-live-warm", help="Run the live-warm consumer group loop.")
    worker_live_warm.add_argument("--consumer-name", default="worker-live-warm-1", help="Redis consumer name for the live-warm worker.")
    worker_live_warm.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_maintenance = subparsers.add_parser("worker-maintenance", help="Run the maintenance/recovery consumer group loop.")
    worker_maintenance.add_argument("--consumer-name", default="worker-maintenance-1", help="Redis consumer name for the maintenance worker.")
    worker_maintenance.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_historical_maintenance = subparsers.add_parser("worker-historical-maintenance", help="Run the archival maintenance/recovery consumer group loop.")
    worker_historical_maintenance.add_argument("--consumer-name", default="worker-historical-maintenance-1", help="Redis consumer name for the archival maintenance worker.")
    worker_historical_maintenance.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")
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
    # The explicit memory fallback flag is a development/testing escape hatch.
    # When it is enabled and no URL was passed on the command line, prefer the
    # in-memory backend instead of implicitly binding to a machine-local REDIS_URL.
    if allow_memory_fallback and not redis_url:
        logger.warning("Using in-memory Redis backend because --allow-memory-redis is enabled.")
        return _MemoryRedisBackend()
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


def _normalized_source_slug(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    return normalized or None


def _prefetched_run_size_limit_bytes() -> int:
    raw = str(os.getenv("BP_PREFETCH_MEMORY_WARN_MB", "") or "").strip()
    if not raw:
        return 50 * 1024 * 1024
    try:
        return max(1, int(raw)) * 1024 * 1024
    except ValueError:
        logger.warning("Invalid BP_PREFETCH_MEMORY_WARN_MB=%r; falling back to 50MB default.", raw)
        return 50 * 1024 * 1024


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

    def xlen(self, stream: str) -> int:
        return len(self.streams.get(stream, ()))

    def xinfo_groups(self, stream: str) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for current_stream, group in sorted(self.group_offsets):
            if current_stream != stream:
                continue
            pending_meta = self.pending_meta.setdefault((stream, group), {})
            rows.append(
                {
                    "name": group,
                    "consumers": len({str(item["consumer"]) for item in pending_meta.values()}),
                    "pending": len(pending_meta),
                    "last-delivered-id": None if not self.streams.get(stream) else self.streams[stream][-1][0],
                    "entries-read": int(self.group_offsets.get((stream, group), 0)),
                    "lag": max(0, len(self.streams.get(stream, ())) - int(self.group_offsets.get((stream, group), 0))),
                }
            )
        return rows

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
