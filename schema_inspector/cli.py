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
from .monitoring import (
    MonitoringConfig,
    MonitoringDaemon,
    NullAlertSink,
    NullDedupeStore,
    RedisDedupeStore,
    TelegramAlertSink,
    fetch_all_signals_from_api,
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


def _live_fanout_max_inflight_from_env(hydration_mode: str) -> int:
    # live_delta uses SOFASCORE_LIVE_FANOUT_MAX_INFLIGHT (existing knob,
    # default 1). Historical/regular hydration uses
    # SOFASCORE_HYDRATE_FANOUT_MAX_INFLIGHT (also default 1 — sequential
    # — to preserve existing behaviour unless explicitly enabled).
    mode = str(hydration_mode or "").strip().lower()
    env_name = "SOFASCORE_LIVE_FANOUT_MAX_INFLIGHT" if mode == "live_delta" else "SOFASCORE_HYDRATE_FANOUT_MAX_INFLIGHT"
    raw_value = os.getenv(env_name, "1")
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        logger.warning(
            "Invalid %s=%r; falling back to sequential fan-out",
            env_name,
            raw_value,
        )
        return 1
    return max(1, value)


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
    freshness_skip_keys: frozenset[str] = frozenset()

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


class _ReplayFreshnessStore:
    """Replays prefetch's freshness-skip decisions deterministically.

    The live FreshnessStore is backed by Redis with a TTL. Between prefetch
    and replay phases the TTL can expire, which would cause replay to attempt
    a fetch that prefetch had skipped — and ReplayFetchExecutor would raise
    "No prefetched fetch outcome available". Snapshotting prefetch's skip set
    and consulting it from replay decouples replay from live TTL state.
    """

    def __init__(self, fresh_keys: frozenset[str]) -> None:
        self._fresh_keys = frozenset(fresh_keys or ())

    def is_fresh(self, key: str) -> bool:
        return key in self._fresh_keys


class ReplayFetchExecutor:
    """Replays committed prefetch outcomes by task key.

    For ``hydrate_special_route`` reasons (player heatmap / rating-breakdown /
    statistics / shotmap-derived followups), the prefetch and replay sides
    occasionally schedule slightly different sets of player IDs because the
    decision depends on rollup state that mutates across the run. When that
    happens we soft-skip with a synthetic ``replay_skipped_missing`` outcome
    instead of cascading into a full replay failure that would re-trigger a
    bootstrap upgrade and another full-mode burst on the next tick. Other
    reasons (root, edges, finalize_event) still raise — they are mandatory.
    """

    _SOFT_SKIP_REASONS = frozenset({"hydrate_special_route", "hydrate_entity_profile"})

    def __init__(self, prefetched_run: PrefetchedRun) -> None:
        self._records_by_key: dict[tuple[object, ...], deque[PrefetchedFetchRecord]] = {}
        for record in prefetched_run.fetch_records:
            self._records_by_key.setdefault(build_fetch_task_key(record.task), deque()).append(record)

    async def execute(self, task) -> object:
        key = build_fetch_task_key(task)
        queued = self._records_by_key.get(key)
        if queued:
            return queued.popleft().outcome
        fetch_reason = getattr(task, "fetch_reason", None)
        if fetch_reason in self._SOFT_SKIP_REASONS:
            from .fetch_models import FetchOutcomeEnvelope

            logger.warning(
                "ReplayFetchExecutor soft-skipping %s task without prefetched record: %s",
                fetch_reason,
                key,
            )
            return FetchOutcomeEnvelope(
                trace_id=getattr(task, "trace_id", None),
                job_id=getattr(task, "job_id", None),
                endpoint_pattern=getattr(task, "endpoint_pattern", ""),
                source_url=getattr(task, "source_url", ""),
                resolved_url=None,
                http_status=None,
                classification="replay_skipped_missing",
                proxy_id=None,
                challenge_reason=None,
                snapshot_id=None,
                payload_hash=None,
            )
        raise RuntimeError(f"No prefetched fetch outcome available for task={key!r}")


class _PrefetchFailedError(Exception):
    """Carries the prefetch executor reference so the caller can persist
    its accumulated request_log records before re-raising the original
    exception.

    P3 audit-trail fix: ``HybridApp.run_event`` uses a 3-stage pattern
    (prefetch in deferred-mode FetchExecutor → commit → persist). When
    Stage 1 raises (e.g. ``RetryableJobError`` from transport timeout),
    Stage 2 (the only place that writes ``api_request_log`` rows) is
    skipped and the buffered records die with garbage collection. This
    exception class wraps the original exception while exposing the
    executor so the caller can selectively commit ONLY request_log
    rows (not snapshots, not capability state) for forensic visibility.
    See ``HybridApp._commit_request_logs_only`` for the partial-commit
    semantics.
    """

    def __init__(self, *, executor: "FetchExecutor", original: BaseException) -> None:
        super().__init__(str(original))
        self.executor = executor
        self.original = original


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
        # A3 Phase 0 (2026-05-16): lazily-loaded per-UT live-tier override
        # registry. Stays ``None`` until ``ensure_tier_override_registry``
        # is awaited (planner-daemon / worker entrypoints do this once on
        # startup), which keeps unit tests and synchronous one-shot CLIs
        # free of DB roundtrips when the override layer is unused.
        self.tier_override_registry = None
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
        await _close_redis_backend(self.redis_backend)

    async def ensure_endpoint_registry(self, sport_slug: str) -> None:
        normalized_sport_slug = str(sport_slug or "").strip().lower() or "football"
        if normalized_sport_slug in self._seeded_endpoint_registry_sports:
            return
        registry_entries = hybrid_runtime_registry_entries_for_sport(normalized_sport_slug)
        async with self.database.transaction() as connection:
            await self.raw_repository.upsert_endpoint_registry_entries(connection, registry_entries)
        self._seeded_endpoint_registry_sports.add(normalized_sport_slug)

    async def ensure_tier_override_registry(self):
        """A3 Phase 0: lazily construct and load the per-UT live-tier
        override registry. Called once per process from planner / worker
        entrypoints. Subsequent calls return the cached registry without
        re-querying. Failures during ``load()`` are swallowed inside the
        registry itself (it preserves the previous snapshot — empty on
        the very first load), so a transient DB hiccup at boot does NOT
        block worker startup; live-tier dispatch falls through to the
        existing detail_id / user_count heuristic in that case.
        """

        if self.tier_override_registry is not None:
            return self.tier_override_registry
        try:
            from .live_tier_override import LiveTierOverrideRegistry
        except ImportError:  # safety net for partial deploys
            logger.warning("live_tier_override module unavailable; skipping registry load")
            return None
        async with self.database.connection() as connection:
            registry = LiveTierOverrideRegistry(sql_executor=connection)
            try:
                loaded = await registry.load()
                logger.info("tier_override_registry loaded: entries=%d", int(loaded or 0))
            except Exception:  # never block startup on this best-effort load
                logger.exception("tier_override_registry initial load failed; using empty registry")
        self.tier_override_registry = registry
        return registry

    async def run_event(
        self,
        *,
        event_id: int,
        sport_slug: str | None,
        hydration_mode: str = "full",
        scope: str | None = None,
    ):
        # Task 2 Phase B (2026-05-20): the ``scope`` kwarg is propagated
        # from the hydrate worker (set to ``"final_sync"`` by the
        # FinalSyncPlannerDaemon) all the way down to the inner
        # ``PilotOrchestrator.run_event`` where the success-path
        # ``set_event_locked`` stamp lives. Adding it here was the
        # missing wire that caused the 2026-05-20 hydrate worker
        # crashloop ("TypeError: HybridApp.run_event() got an
        # unexpected keyword argument 'scope'").
        resolved_sport_slug = sport_slug or await self.resolve_event_sport_slug(event_id)
        await self.ensure_endpoint_registry(str(resolved_sport_slug or "football"))
        requested_hydration_mode = str(hydration_mode or "full").strip().lower()
        effective_hydration_mode = requested_hydration_mode
        should_mark_live_bootstrap = False
        # Phase 2.2 (2026-05-20 perf audit): track explicit ownership so
        # the finally block can release the hydrate_lock only when this
        # invocation actually acquired it. Without this flag the CLI
        # path could leak the lock for the full 60s TTL on crashes in
        # the prefetch / commit / persist chain — every subsequent live
        # poll for that event would silently early-return.
        acquired_hydrate_lock = False
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
                acquired_hydrate_lock = True
                effective_hydration_mode = "full"
                should_mark_live_bootstrap = True
        try:
            try:
                prefetched_run = await self._prefetch_event_run(
                    event_id=event_id,
                    sport_slug=str(resolved_sport_slug or "football"),
                    hydration_mode=effective_hydration_mode,
                )
            except _PrefetchFailedError as wrap:
                # P3 audit-trail fix: persist whatever request_log records the
                # prefetch executor accumulated (proxy_address, attempts_json,
                # latency_ms, error_message per attempt) before re-raising.
                # Snapshots are deliberately skipped — they represent partial
                # state that the normalize pipeline would never re-process,
                # and persisting them would leak inconsistent rows that read-
                # side queries cannot detect.  Without this best-effort
                # commit, the forensics records die with garbage collection
                # when ``prefetch_executor`` goes out of scope at the next
                # stack frame, and api_request_log has zero rows for failed
                # root fetches on the live tier_1/tier_2/tier_3/warm paths.
                #
                # ``_commit_request_logs_only`` is wrapped in its own
                # try/except so its failure cannot mask the original
                # exception — at worst we log a warning and lose the audit
                # trail for that one prefetch.
                await self._commit_request_logs_only(wrap.executor)
                raise wrap.original
            self._warn_if_prefetched_run_large(prefetched_run)
            committed_run = await self._commit_prefetched_run(prefetched_run)
            result = await self._persist_prefetched_run(
                committed_run,
                hydration_mode=effective_hydration_mode,
                scope=scope,
            )
            if requested_hydration_mode == "live_delta" and self.live_bootstrap_coordinator is not None:
                async with self.database.transaction() as connection:
                    if getattr(result, "finalized", False):
                        await self.live_bootstrap_coordinator.reset_bootstrap(connection, event_id=event_id)
                    elif should_mark_live_bootstrap:
                        await self.live_bootstrap_coordinator.mark_bootstrapped(connection, event_id=event_id)
            return result
        finally:
            # Phase 2.2 (2026-05-20 perf audit): release the bootstrap
            # hydrate_lock unconditionally if this invocation acquired
            # it — covers both success and exception paths. Without
            # this, a crash in any of _prefetch / _commit / _persist
            # would leave the lock set for its full TTL, dropping
            # every subsequent live poll for the event.
            if (
                acquired_hydrate_lock
                and self.live_bootstrap_coordinator is not None
            ):
                try:
                    self.live_bootstrap_coordinator.release_hydrate_lock(
                        event_id=event_id
                    )
                except Exception as exc:  # noqa: BLE001 — defensive
                    logger.warning(
                        "App.run_event: release_hydrate_lock failed event_id=%s: %r",
                        event_id,
                        exc,
                    )

    async def run_event_details(
        self,
        *,
        event_id: int,
        sport_slug: str | None,
        context: dict,
    ):
        """Run only the per-event detail fanout phase (P0(a) split-details).

        Consumed by ``LiveDetailsWorkerService`` from
        ``stream:etl:live_details``. Uses a *direct* (immediate) write
        ``FetchExecutor`` rather than the prefetch+commit+persist replay
        pattern that ``run_event`` uses. The replay pattern was attempted
        in canary v2 and failed: ``_persist_prefetched_run`` invokes
        ``orchestrator.run_event(...)`` which tries to replay the ROOT
        ``/event`` fetch via the replay-only executor. Since the prefetch
        for details fanout never ran ROOT, the replay raised
        ``RuntimeError: No prefetched fetch outcome available for
        task=('hydrate_event_root', ...)``. Direct/immediate writes side-
        step replay entirely — each detail fetch persists raw + normalized
        rows inline.

        Trade-off vs ``run_event``: details writes lose batch-atomicity
        (each fetch commits independently within a single transaction
        block) and lose negative-cache replay snapshots (cache state
        mutates as fetches happen, not deterministically applied at end).
        Both are acceptable for details fanout because:

        * detail endpoints are independent (one /heatmap/{team_id} write
          does not depend on another /player/{player_id}/statistics
          write being durable);
        * negative-cache decisions for detail endpoints are read-only
          during a single details run — there is no replay phase to
          conflict with.

        Failure semantics — permissive: any exception bubbles out of
        this method but is caught by ``LiveDetailsWorkerService.handle``
        which translates it into ``completed_with_errors``. Details
        failure MUST NOT retry the parent ``refresh_live_event`` job.
        """

        resolved_sport_slug = sport_slug or await self.resolve_event_sport_slug(event_id)
        await self.ensure_endpoint_registry(str(resolved_sport_slug or "football"))

        planner = Planner(capability_rollup=dict(self.capability_rollup))
        async with self.database.transaction() as connection:
            snapshot_store = HybridSnapshotStore(self.raw_repository, connection)
            executor = FetchExecutor(
                transport=self.transport,
                raw_repository=self.raw_repository,
                sql_executor=connection,
                snapshot_store=snapshot_store,
                freshness_store=self.freshness_store,
            )
            season_widget_gate = None
            event_endpoint_gate = None
            if self.negative_cache_settings.enabled:
                season_widget_gate = SeasonWidgetNegativeCache(
                    repository=self.endpoint_negative_cache_repository,
                    sql_executor=connection,
                    settings=self.negative_cache_settings,
                )
            if self.event_negative_cache_settings.enabled:
                event_endpoint_gate = EventEndpointNegativeCache(
                    repository=self.event_endpoint_negative_cache_repository,
                    sql_executor=connection,
                    settings=self.event_negative_cache_settings,
                )
            orchestrator = PilotOrchestrator(
                fetch_executor=executor,
                snapshot_store=snapshot_store,
                normalize_worker=NormalizeWorker(
                    ParserRegistry.default(),
                    result_sink=DurableNormalizeSink(
                        self.normalize_repository,
                        connection,
                    ),
                ),
                planner=planner,
                capability_repository=self.capability_repository,
                sql_executor=connection,
                live_state_store=self.live_state_store,
                live_state_repository=self.live_state_repository,
                stream_queue=self.stream_queue,
                season_widget_gate=season_widget_gate,
                event_endpoint_gate=event_endpoint_gate,
                final_sweep_gate=self.final_sweep_gate,
                freshness_store=self.freshness_store,
                fanout_max_inflight=_live_fanout_max_inflight_from_env("live_delta"),
                tier_override_registry=self.tier_override_registry,
            )
            result = await orchestrator.run_event_details(
                event_id=event_id,
                sport_slug=str(resolved_sport_slug or "football"),
                context=dict(context or {}),
            )
            if (
                self.event_negative_cache_settings.enabled
                and event_endpoint_gate is not None
                and event_endpoint_gate.events
            ):
                await self.event_endpoint_negative_cache_repository.apply_events(
                    connection,
                    event_endpoint_gate.events,
                )
            if (
                self.negative_cache_settings.enabled
                and season_widget_gate is not None
                and season_widget_gate.events
            ):
                await self.endpoint_negative_cache_repository.apply_events(
                    connection,
                    season_widget_gate.events,
                )
        self.capability_rollup.update(planner.capability_rollup)
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
                fanout_max_inflight=_live_fanout_max_inflight_from_env(hydration_mode),
                tier_override_registry=self.tier_override_registry,
            )
            try:
                await orchestrator.run_event(
                    event_id=event_id,
                    sport_slug=sport_slug,
                    hydration_mode=hydration_mode,
                )
            except Exception as exc:
                # P3 audit-trail fix: wrap the original exception with the
                # prefetch_executor reference so ``run_event`` can persist
                # the accumulated request_log records (forensics: proxy,
                # latency, error per-attempt) before re-raising. Without
                # this wrap the records live only in
                # ``prefetch_executor._prefetched_records`` and die with
                # the function's local scope at the next stack frame.
                raise _PrefetchFailedError(executor=prefetch_executor, original=exc) from exc
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
            freshness_skip_keys=orchestrator.freshness_skip_keys,
        )

    async def _commit_request_logs_only(self, executor: "FetchExecutor") -> None:
        """P3 audit-trail fix — partial commit of ``api_request_log``
        rows accumulated in a failed-prefetch ``FetchExecutor``.

        Called from ``run_event`` when ``_prefetch_event_run`` raises
        ``_PrefetchFailedError``. Persists ONLY request_log records
        (the forensics: proxy_address, attempts_json, latency_ms,
        error_message per-attempt). Snapshot rows are deliberately
        skipped — they represent partial state that the downstream
        normalize pipeline would never re-process, and persisting them
        would leak inconsistent rows.

        The entire operation is wrapped in its own try/except so any
        DB error here (network, transaction abort, duplicate-key) is
        logged at WARNING and DOES NOT mask the original prefetch
        exception. Per-record inserts also each have their own
        try/except so a single bad record (e.g. row collision via
        worker retry) cannot prevent other records from landing.

        Idempotency: each ``ApiRequestLogRecord`` has a fresh
        ``started_at`` timestamp (set inside ``FetchExecutor.execute``
        at attempt time). A worker_runtime retry produces NEW attempts
        with NEW timestamps, so re-execution does not generate
        duplicate-id conflicts. The persisted row appears as one
        forensics entry per attempt regardless of how many retries the
        underlying job survives.
        """
        records = executor.prefetched_records
        if not records:
            return
        try:
            async with self.database.transaction() as connection:
                for record in records:
                    try:
                        await self.raw_repository.insert_request_log(
                            connection, record.request_log
                        )
                    except Exception as record_exc:  # pragma: no cover - defensive
                        logger.debug(
                            "P3 partial-commit insert_request_log skipped (likely "
                            "duplicate or transient): %s",
                            record_exc,
                        )
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "P3 partial-commit transaction failed for %d records: %s — "
                "audit trail lost for this prefetch, original exception will "
                "still surface",
                len(records),
                exc,
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
        scope: str | None = None,
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
                freshness_store=_ReplayFreshnessStore(prefetched_run.freshness_skip_keys),
                fanout_max_inflight=1,
                tier_override_registry=self.tier_override_registry,
            )
            result = await orchestrator.run_event(
                event_id=prefetched_run.event_id,
                sport_slug=prefetched_run.sport_slug,
                hydration_mode=hydration_mode,
                scope=scope,
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
        # A1 (2026-05-16): priority-aware backfill order. Previously the
        # ORDER BY was just ``ut.id`` (effectively random by insertion
        # time), so Macao Amateur Division and the Premier League got
        # the same probability of being picked on any given tick. With
        # this change Sofascore's own ``category.priority`` (Europe=19,
        # England=10, Macao=0) drives the order, with ``user_count``
        # (Premier League = 1.2M) as a tie-breaker between equal-priority
        # categories. The ``ut.id`` ASC at the end is the cursor-stable
        # tiebreaker — the cursor itself stays ``ut.id``-based so a full
        # cycle still wraps correctly; what changes is which UTs land
        # in each batch (top priority first per cycle).
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
                ORDER BY
                  c.priority DESC NULLS LAST,
                  ut.user_count DESC NULLS LAST,
                  ut.id
                LIMIT $4
                """,
                sport_slug,
                int(after_unique_tournament_id),
                list(normalized_allowed_ids),
                max(1, int(limit)),
            )
        return tuple(int(row["id"]) for row in rows if row["id"] is not None)

    async def run_historical_tournament_archive(
        self,
        *,
        unique_tournament_id: int,
        sport_slug: str,
        target_season_id: int | None = None,
        bootstrap_mode: bool = False,
    ):
        # Phase 3 (2026-05-22): forward bootstrap_mode kwarg to the
        # service so the lightweight-event-list-only path can be
        # selected per-job by the worker dispatcher. Without this
        # wire, the worker's catalog-state-driven dispatch silently
        # falls back to the full archive path.
        return await run_historical_tournament_archive_service(
            self,
            unique_tournament_id=unique_tournament_id,
            sport_slug=sport_slug,
            target_season_id=target_season_id,
            bootstrap_mode=bootstrap_mode,
        )

    async def advance_backfill_cursor(
        self,
        *,
        sport_slug: str,
        unique_tournament_id: int,
        completed_season_id: int,
    ) -> int | None:
        """Phase 1 (2026-05-16): called by HistoricalTournamentWorker after
        a successful per-season run. Walks tournament_registry cursor
        from ``completed_season_id`` down to the next-older season for
        this UT (or to 0 when nothing older exists). Best-effort — the
        worker swallows exceptions from this call so a transient DB
        error never fails the underlying job.
        """
        from .storage.tournament_registry_repository import TournamentRegistryRepository

        repo = TournamentRegistryRepository()
        async with self.database.connection() as connection:
            return await repo.advance_backfill_cursor(
                connection,
                sport_slug=sport_slug,
                unique_tournament_id=unique_tournament_id,
                completed_season_id=completed_season_id,
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

    async def rebuild_capability_rollup(
        self,
        *,
        sport_slug: str | None = None,
        lookback_days: int | None = None,
    ) -> int:
        """Out-of-band batch rebuilder for ``endpoint_capability_rollup``.

        Aggregates per-(sport_slug, endpoint_pattern) counts directly from
        ``endpoint_capability_observation`` and replaces rollup state in a
        single ON CONFLICT DO UPDATE per key. The inline live/hydrate hot
        path is gated off by default since 2026-05-13 (see
        ``SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED``); this command is the
        canonical way to refresh rollup data without participating in the
        live deadlock storm.
        """

        async with self.database.connection() as connection:
            return await self.capability_repository.rebuild_rollups_from_observations(
                connection,
                sport_slug=sport_slug,
                lookback_days=lookback_days,
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
    # Stage 3.4 (2026-05-20): one-shot backfill for a single (UT, season).
    # Membership here routes the dispatcher through
    # ``load_historical_runtime_config`` so backfill consumes the
    # non-residential historical proxy pool, not residential proxies
    # reserved for live polling.
    "historical-backfill",
})


async def _dispatch(args) -> int:
    command = getattr(args, "command", None)
    if command == "monitoring-daemon":
        return await _run_monitoring_daemon(args)
    if command == "api-cache-warmer":
        return await _run_cache_warmer(args)
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
        # P0(b)-followup: per-lane scoped Smartproxy session multiplier.
        # Workers may opt into a different multiplier than the global
        # SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER without affecting other
        # processes. The fallback chain always ends in the global env
        # key — if no scoped key is set, the global value (default 1,
        # no-op) is used.
        #
        # Scoped lanes (in priority of expected ROOT-fetch volume —
        # see /event traffic analysis: hydrate@8 + live-tier-3@9 +
        # live-warm@3 dominate, tier_1 / tier_2 are minor):
        #   * worker-hydrate         -> SCHEMA_INSPECTOR_HYDRATE_PROXY_SESSION_MULTIPLIER
        #   * worker-live-tier-3     -> SCHEMA_INSPECTOR_LIVE_TIER_3_PROXY_SESSION_MULTIPLIER
        #   * worker-live-warm       -> SCHEMA_INSPECTOR_LIVE_WARM_PROXY_SESSION_MULTIPLIER
        #   * worker-live-tier-2     -> SCHEMA_INSPECTOR_LIVE_TIER_2_PROXY_SESSION_MULTIPLIER
        #   * worker-live-tier-1     -> SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_SESSION_MULTIPLIER
        # All other CLI subcommands fall back to the global key only.
        _SCOPED_MULTIPLIER_KEY_BY_COMMAND = {
            "worker-hydrate": "SCHEMA_INSPECTOR_HYDRATE_PROXY_SESSION_MULTIPLIER",
            "worker-live-tier-1": "SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_SESSION_MULTIPLIER",
            "worker-live-tier-2": "SCHEMA_INSPECTOR_LIVE_TIER_2_PROXY_SESSION_MULTIPLIER",
            "worker-live-tier-3": "SCHEMA_INSPECTOR_LIVE_TIER_3_PROXY_SESSION_MULTIPLIER",
            "worker-live-warm": "SCHEMA_INSPECTOR_LIVE_WARM_PROXY_SESSION_MULTIPLIER",
        }
        # Variant A-lite (per-endpoint concurrent-lease cap). Same lane
        # mapping as session-multiplier above so operators can opt in
        # any single lane independently. Both env knobs may coexist
        # (multiplier inflates the slot count, max_in_use multiplies
        # leases per slot — they multiply). On prod today: Variant B
        # multiplier knobs are NOT set (HTTP 612 from Smartproxy
        # rejected sessions); Variant A-lite max_in_use knobs ship
        # default-unset by this commit, no runtime change.
        _SCOPED_MAX_IN_USE_KEY_BY_COMMAND = {
            "worker-hydrate": "SCHEMA_INSPECTOR_HYDRATE_PROXY_MAX_IN_USE_PER_ENDPOINT",
            "worker-live-tier-1": "SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_MAX_IN_USE_PER_ENDPOINT",
            "worker-live-tier-2": "SCHEMA_INSPECTOR_LIVE_TIER_2_PROXY_MAX_IN_USE_PER_ENDPOINT",
            "worker-live-tier-3": "SCHEMA_INSPECTOR_LIVE_TIER_3_PROXY_MAX_IN_USE_PER_ENDPOINT",
            "worker-live-warm": "SCHEMA_INSPECTOR_LIVE_WARM_PROXY_MAX_IN_USE_PER_ENDPOINT",
        }
        # X' patch: same lane-scoping pattern for the InspectorTransport
        # session-cache LRU cap. Ships default-unset by this commit — env
        # unset on every lane means `_resolve_session_cache_max_entries`
        # returns 0 → unbounded cache → behaviour identical to pre-patch.
        # Phase 1 canary will enable a single scoped key (e.g.
        # SCHEMA_INSPECTOR_LIVE_TIER_1_SESSION_CACHE_MAX_ENTRIES=5) on one
        # worker via a systemd drop-in, without touching this dispatch table.
        _SCOPED_SESSION_CACHE_KEY_BY_COMMAND = {
            "worker-hydrate": "SCHEMA_INSPECTOR_HYDRATE_SESSION_CACHE_MAX_ENTRIES",
            "worker-live-tier-1": "SCHEMA_INSPECTOR_LIVE_TIER_1_SESSION_CACHE_MAX_ENTRIES",
            "worker-live-tier-2": "SCHEMA_INSPECTOR_LIVE_TIER_2_SESSION_CACHE_MAX_ENTRIES",
            "worker-live-tier-3": "SCHEMA_INSPECTOR_LIVE_TIER_3_SESSION_CACHE_MAX_ENTRIES",
            "worker-live-warm": "SCHEMA_INSPECTOR_LIVE_WARM_SESSION_CACHE_MAX_ENTRIES",
            "worker-live-hot": "SCHEMA_INSPECTOR_LIVE_HOT_SESSION_CACHE_MAX_ENTRIES",
            "worker-historical-hydrate": "SCHEMA_INSPECTOR_HISTORICAL_HYDRATE_SESSION_CACHE_MAX_ENTRIES",
            "worker-historical-discovery": "SCHEMA_INSPECTOR_HISTORICAL_DISCOVERY_SESSION_CACHE_MAX_ENTRIES",
            "worker-historical-tournament": "SCHEMA_INSPECTOR_HISTORICAL_TOURNAMENT_SESSION_CACHE_MAX_ENTRIES",
            "worker-historical-enrichment": "SCHEMA_INSPECTOR_HISTORICAL_ENRICHMENT_SESSION_CACHE_MAX_ENTRIES",
            "worker-resource-refresh": "SCHEMA_INSPECTOR_RESOURCE_REFRESH_SESSION_CACHE_MAX_ENTRIES",
            "worker-discovery": "SCHEMA_INSPECTOR_DISCOVERY_SESSION_CACHE_MAX_ENTRIES",
            "worker-live-discovery": "SCHEMA_INSPECTOR_LIVE_DISCOVERY_SESSION_CACHE_MAX_ENTRIES",
        }
        scoped_multiplier_keys: tuple[str, ...] = ()
        scoped_key = _SCOPED_MULTIPLIER_KEY_BY_COMMAND.get(command)
        if scoped_key is not None:
            scoped_multiplier_keys = (scoped_key,)
        scoped_max_in_use_keys: tuple[str, ...] = ()
        scoped_max_in_use_key = _SCOPED_MAX_IN_USE_KEY_BY_COMMAND.get(command)
        if scoped_max_in_use_key is not None:
            scoped_max_in_use_keys = (scoped_max_in_use_key,)
        scoped_session_cache_keys: tuple[str, ...] = ()
        scoped_session_cache_key = _SCOPED_SESSION_CACHE_KEY_BY_COMMAND.get(command)
        if scoped_session_cache_key is not None:
            scoped_session_cache_keys = (scoped_session_cache_key,)
        runtime_config = load_runtime_config(
            proxy_urls=args.proxy or None,
            user_agent=args.user_agent,
            max_attempts=args.max_attempts,
            proxy_session_multiplier_env_keys=scoped_multiplier_keys,
            proxy_max_in_use_per_endpoint_env_keys=scoped_max_in_use_keys,
            session_cache_max_entries_env_keys=scoped_session_cache_keys,
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
            try:
                monitor = ProxyHealthMonitor(
                    repository=ProxyHealthRepository(),
                    state_store=ProxyStateStore(redis_backend),
                    config=ProxyHealthMonitorConfig.from_env(_load_project_env()),
                )
                await monitor.run_forever(database.connection)
                return 0
            finally:
                await _close_redis_backend(redis_backend)
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
            if args.command == "coverage-refresh":
                # Task 4 (2026-05-15): refresh mv_season_coverage. Should
                # be run on a 15-30 min cadence via systemd timer (or
                # manually after a big structure-sync sweep).
                import time as _coverage_refresh_time
                started = _coverage_refresh_time.perf_counter()
                async with app.database.connection() as conn:
                    await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_season_coverage")
                elapsed_ms = int((_coverage_refresh_time.perf_counter() - started) * 1000)
                print(f"coverage_refresh ok elapsed_ms={elapsed_ms}")
                return 0
            if args.command == "rebuild-capability-rollup":
                sport_slug = getattr(args, "sport_slug", None) or None
                lookback_days = getattr(args, "lookback_days", None)
                rebuilt = await app.rebuild_capability_rollup(
                    sport_slug=sport_slug,
                    lookback_days=lookback_days,
                )
                print(
                    "rebuild_capability_rollup "
                    f"rebuilt_rollup_rows={rebuilt} "
                    f"sport_slug={sport_slug or 'all'} "
                    f"lookback_days={lookback_days if lookback_days is not None else 'all'}"
                )
                return 0
            if args.command == "stale-live-events":
                from .ops.stale_live_events import (
                    collect_stale_live_events_summary,
                    collect_top_stale_live_events,
                    format_stale_live_events_report,
                )
                async with database.connection() as connection:
                    summary = await collect_stale_live_events_summary(connection)
                    top = await collect_top_stale_live_events(
                        connection,
                        threshold_seconds=int(args.threshold_seconds),
                        limit=int(args.top),
                        inprogress_only=not bool(args.all_statuses),
                    )
                print(
                    format_stale_live_events_report(
                        summary,
                        top,
                        threshold_seconds=int(args.threshold_seconds),
                        inprogress_only=not bool(args.all_statuses),
                    )
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
            if args.command == "backfill-cursor":
                from .storage.tournament_registry_repository import (
                    TournamentRegistryRepository,
                )

                action = getattr(args, "action", "show")
                repo = TournamentRegistryRepository()

                if action == "reseed-stuck":
                    cat_min = int(getattr(args, "min_cat_priority", 0))
                    async with app.database.connection() as connection:
                        affected = await repo.re_seed_stuck_cursors_to_newest_finished_season(
                            connection, cat_priority_min=cat_min,
                        )
                    print(f"re-seed: {affected} rows updated (cat_priority_min={cat_min})")
                    return 0

                if action == "show":
                    sport = getattr(args, "sport_slug", None)
                    async with app.database.connection() as connection:
                        rows = await repo.list_pending_backfill_cursors(
                            connection, sport_slug=sport, limit=200,
                        )
                    import json as _json
                    print(_json.dumps([dict(r) for r in rows], indent=2, default=str))
                    return 0

                print(f"Unknown action: {action}")
                return 2
            if args.command == "backfill-priorities":
                from .services.backfill_priority_config import (
                    BackfillPriorityConfig,
                    ConfigValidationError,
                )
                from pathlib import Path
                import json as _json

                cfg_path = Path(
                    getattr(args, "config_path", None)
                    or os.environ.get("SOFASCORE_BACKFILL_PRIORITIES_PATH")
                    or "/opt/sofascore/config/backfill_priorities.yaml"
                )
                action = getattr(args, "action", "show")

                if action == "show":
                    try:
                        cfg = BackfillPriorityConfig.load(cfg_path)
                    except ConfigValidationError as exc:
                        print(f"INVALID: {exc}")
                        return 2
                    print(_json.dumps({
                        "path": str(cfg_path),
                        "exists": cfg_path.exists(),
                        "sport_weights": dict(cfg.sport_weights),
                        "ut_boost": dict(cfg.ut_boost),
                        "sport_concurrency_caps": dict(cfg.sport_concurrency_caps),
                    }, indent=2))
                    return 0

                if action == "reload":
                    import subprocess
                    unit = "sofascore-historical-tournament-planner.service"
                    proc = subprocess.run(
                        ["sudo", "systemctl", "kill", "-s", "HUP", unit],
                        capture_output=True, text=True,
                    )
                    if proc.returncode != 0:
                        print(f"FAIL: {proc.stderr.strip()}")
                        return proc.returncode
                    print(f"OK: SIGHUP sent to {unit}")
                    print("Check 'journalctl -u sofascore-historical-tournament-planner -n 5' "
                          "for the reload log line.")
                    return 0

                if action == "dry-run":
                    # Just validate parse + dump effective ratios.
                    try:
                        cfg = BackfillPriorityConfig.load(cfg_path)
                    except ConfigValidationError as exc:
                        print(f"INVALID: {exc}")
                        return 2
                    total = sum(cfg.sport_weights.values()) or 1.0
                    print(f"Loaded {cfg_path}")
                    print(f"Effective shares (per planner tick of N slots):")
                    for sport, weight in sorted(cfg.sport_weights.items(), key=lambda kv: -kv[1]):
                        pct = 100.0 * weight / total
                        print(f"  {sport:25s} weight={weight:>6.1f}  {pct:>5.1f} %")
                    if cfg.ut_boost:
                        print(f"\nUT boost (multiplier on top of sport weight):")
                        for ut_id, mult in cfg.ut_boost.items():
                            print(f"  ut_id={ut_id:<8d}  multiplier={mult}")
                    if cfg.sport_concurrency_caps:
                        print(f"\nConcurrency caps:")
                        for sport, cap in cfg.sport_concurrency_caps.items():
                            print(f"  {sport:25s} max in-flight = {cap}")
                    return 0

                print(f"Unknown action: {action}")
                return 2
            if args.command == "historical-tournament-planner-daemon":
                service_app = ServiceApp(app)
                await service_app.run_historical_tournament_planner_daemon(
                    sport_slugs=tuple(args.sport_slug or ()),
                    tournaments_per_tick=args.tournaments_per_tick,
                    loop_interval_seconds=args.loop_interval_seconds,
                )
                return 0
            if args.command == "db-migrate":
                # Stage 3.5 (2026-05-20): migration runner CLI.
                from .storage.migration_runner import (
                    apply_pending_migrations,
                    default_migrations_dir,
                    list_applied_migrations,
                    mark_migration_applied,
                )
                migrations_dir = default_migrations_dir()
                async with app.database.connection() as connection:
                    if args.action == "apply":
                        applied = await apply_pending_migrations(connection, migrations_dir)
                        if applied:
                            print(f"Applied {len(applied)} migration(s):")
                            for name in applied:
                                print(f"  + {name}")
                        else:
                            print("No pending migrations.")
                        return 0
                    if args.action == "status":
                        applied_set = await list_applied_migrations(connection)
                        all_files = sorted(p.name for p in migrations_dir.glob("*.sql"))
                        pending = [name for name in all_files if name not in applied_set]
                        print(f"Applied: {len(applied_set)}")
                        for name in sorted(applied_set):
                            print(f"  ✓ {name}")
                        print(f"Pending: {len(pending)}")
                        for name in pending:
                            print(f"  · {name}")
                        return 0
                    if args.action == "mark-applied":
                        if not args.filename:
                            print("--filename is required for mark-applied")
                            return 2
                        await mark_migration_applied(connection, args.filename)
                        print(f"Marked applied (without running): {args.filename}")
                        return 0
                    print(f"Unknown db-migrate action: {args.action}")
                    return 2
            if args.command == "historical-backfill":
                # Stage 3.4 (2026-05-20): one-shot backfill of a single
                # (UT, season). Delegate to the existing helper so we
                # do not duplicate the multi-stage ingest. target_season_id
                # scopes the run to exactly the requested season —
                # without it the helper would loop over the UT's
                # backfill cursor.
                #
                # Stage 4.1 (2026-05-20 match-center fix): override the
                # hardcoded skip_event_detail=True / skip_entities=True
                # defaults so the operator-driven CLI actually pulls
                # the per-match content (lineups, incidents, statistics,
                # best-players, player-stats). Worker-historical-
                # tournament keeps the defaults because it publishes
                # enrichment child-jobs afterwards — the CLI handler
                # bypasses that publish path and must do the per-event
                # fan-out synchronously.
                from .services.historical_archive_service import (
                    run_historical_tournament_archive,
                )
                report = await run_historical_tournament_archive(
                    app,
                    unique_tournament_id=int(args.unique_tournament_id),
                    sport_slug=str(args.sport_slug),
                    seasons_per_tournament=0,
                    event_concurrency=int(args.event_concurrency),
                    timeout=float(args.timeout),
                    target_season_id=int(args.season_id),
                    skip_event_detail=False,
                    skip_entities=False,
                )
                logger.info(
                    "historical-backfill: ut=%s season=%s sport=%s report=%s",
                    args.unique_tournament_id,
                    args.season_id,
                    args.sport_slug,
                    report,
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
            if args.command == "resource-planner-daemon":
                service_app = ServiceApp(app)
                await service_app.run_resource_planner_daemon(
                    loop_interval_seconds=args.loop_interval_seconds,
                    publish_per_tick_cap=args.publish_per_tick_cap,
                    lag_threshold=args.lag_threshold,
                )
                return 0
            if args.command == "final-sync-planner-daemon":
                # Task 2 Phase B (2026-05-20): publishes one-shot
                # hydrate jobs with scope="final_sync" for events
                # finalised more than SOFASCORE_FINAL_SYNC_DELAY_SECONDS
                # ago. The orchestrator stamps locked_at on success.
                service_app = ServiceApp(app)
                await service_app.run_final_sync_planner_daemon()
                return 0
            if args.command == "unlock-event":
                # Task 2 Phase B operator escape hatch: clear
                # event_terminal_state.locked_at so a frozen event
                # can re-enter the normal flow.
                async with app.database.connection() as connection:
                    cleared = await app.live_state_repository.clear_event_lock(
                        connection, event_id=int(args.event_id)
                    )
                if cleared:
                    print(f"unlock-event: cleared locked_at for event_id={args.event_id}")
                else:
                    print(
                        f"unlock-event: no-op for event_id={args.event_id} "
                        f"(no terminal row, or already unlocked)"
                    )
                return 0
            if args.command == "worker-resource-refresh":
                service_app = ServiceApp(app)
                await service_app.run_resource_refresh_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
            if args.command == "worker-normalize":
                service_app = ServiceApp(app)
                await service_app.run_normalize_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
            if args.command == "backfill-leaderboards":
                from .services.leaderboards_backfill import run_leaderboards_backfill

                report = await run_leaderboards_backfill(
                    database=app.database,
                    stream_queue=app.stream_queue,
                    sport_slug=args.sport_slug,
                    priority_rank_threshold=args.priority_rank,
                    completed_gap_days=args.completed_gap_days,
                    history_window_days=args.history_window_days,
                )
                print(
                    "leaderboards_backfill "
                    f"pos={report.pos_published} "
                    f"top_players_overall={report.top_players_overall_published} "
                    f"top_teams_overall={report.top_teams_overall_published} "
                    f"top_players_regular={report.top_players_regular_season_published} "
                    f"top_teams_regular={report.top_teams_regular_season_published} "
                    f"total={report.total}"
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
            if args.command == "worker-live-details":
                service_app = ServiceApp(app)
                await service_app.run_live_details_worker(
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
            if args.command == "live-rescue-daemon":
                service_app = ServiceApp(app)
                await service_app.run_live_rescue_daemon()
                return 0
            if args.command == "ws-server":
                from .services.ws_server_service import build_app
                import uvicorn
                from redis.asyncio import Redis as AsyncRedis  # type: ignore

                # The shared sync ``redis_backend`` is not safe to use
                # from the asyncio pubsub loop (await on int return).
                # We open a small, dedicated **async** client just for
                # the WS server's pub/sub listener.
                env = _load_project_env()
                redis_url = (
                    args.redis_url
                    or env.get("REDIS_URL")
                    or env.get("SOFASCORE_REDIS_URL")
                )
                if not redis_url:
                    logger.error("ws-server requires REDIS_URL; aborting.")
                    return 2
                async_redis = AsyncRedis.from_url(redis_url, decode_responses=True)
                fastapi_app = build_app(async_redis)
                config = uvicorn.Config(
                    fastapi_app,
                    host=getattr(args, "host", "127.0.0.1"),
                    port=int(getattr(args, "port", 8001)),
                    log_level="info",
                    ws_ping_interval=20.0,
                    ws_ping_timeout=30.0,
                )
                server = uvicorn.Server(config)
                logger.info(
                    "ws-server starting on %s:%d (lifespan attached)",
                    config.host, config.port,
                )
                await server.serve()
                return 0
            if args.command == "ws-consumer":
                from .services.ws_consumer_service import (
                    DEFAULT_SPORTS,
                    WSConsumerService,
                )
                from .ws_fanout_publisher import RedisFanoutPublisher

                sports_arg = getattr(args, "sports", None) or ",".join(DEFAULT_SPORTS)
                sports = tuple(s.strip() for s in sports_arg.split(",") if s.strip())
                # Mirror WS server fanout: republish every delta on
                # ``ws:fanout:*`` redis pub/sub channels so the
                # sofascore-ws-server.service can broadcast to clients.
                fanout_publisher = None
                if not getattr(args, "no_fanout", False):
                    redis_client = getattr(app, "redis_backend", None)
                    if redis_client is not None:
                        fanout_publisher = RedisFanoutPublisher(redis_client)
                consumer = WSConsumerService(
                    pool=app.database._pool,
                    sports=sports,
                    include_odds=not getattr(args, "no_odds", False),
                    reconnect_delay_seconds=float(getattr(args, "reconnect_delay", 10.0)),
                    fanout_publisher=fanout_publisher,
                )
                logger.info(
                    "ws-consumer starting (sports=%s, include_odds=%s, fanout=%s)",
                    ",".join(sports), consumer.include_odds,
                    fanout_publisher is not None,
                )
                await consumer.run_forever()
                return 0
            if args.command == "backfill-cursor-bootstrap":
                from .storage.tournament_registry_repository import TournamentRegistryRepository

                repo = TournamentRegistryRepository()
                async with app.database.connection() as connection:
                    seeded = await repo.seed_backfill_cursors(
                        connection,
                        sport_slug=args.sport_slug,
                        only_uninitialised=not args.reseed,
                    )
                logger.info(
                    "backfill-cursor-bootstrap: seeded=%d sport=%s reseed=%s",
                    seeded,
                    args.sport_slug or "<all>",
                    args.reseed,
                )
                print(f"seeded={seeded} sport={args.sport_slug or '<all>'} reseed={args.reseed}")
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
    subparsers.add_parser("coverage-refresh", help="Refresh mv_season_coverage materialized view (Task 4 ledger).")

    rebuild_capability_rollup_parser = subparsers.add_parser(
        "rebuild-capability-rollup",
        help=(
            "Rebuild endpoint_capability_rollup from endpoint_capability_observation "
            "aggregates. Use after enabling the firebreak (inline rollup OFF) to "
            "refresh rollup data out-of-band without participating in the live "
            "deadlock storm."
        ),
    )
    rebuild_capability_rollup_parser.add_argument(
        "--sport-slug",
        default=None,
        help="Optional: restrict rebuild to one sport slug (e.g. 'football').",
    )
    rebuild_capability_rollup_parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Optional: only aggregate observations within the last N days (default: full history).",
    )

    stale_live = subparsers.add_parser(
        "stale-live-events",
        help=(
            "Read-only detector for inprogress live events whose /event "
            "snapshot has not advanced past a threshold. Surfaces "
            "transport / proxy regressions hidden behind FreshnessStore "
            "dedupe. Returns counts at multiple staleness thresholds plus "
            "a top-N detail list with success/retry/fail counters."
        ),
    )
    stale_live.add_argument(
        "--threshold-seconds",
        type=int,
        default=300,
        help="Top-N detail filter: only events with snapshot age > this. Default: 300.",
    )
    stale_live.add_argument(
        "--top",
        type=int,
        default=20,
        help="Top-N detail list size. Default: 20.",
    )
    stale_live.add_argument(
        "--all-statuses",
        action="store_true",
        help=(
            "Include events with terminal status_code (100/110/120/60/70). "
            "By default only inprogress codes (6/7/8/31/91/92/93) are listed "
            "in the detail section because terminal events are expected to "
            "have stale snapshots (upstream payload is frozen post-finalize)."
        ),
    )

    planner_daemon = subparsers.add_parser("planner-daemon", help="Run the continuous planner loop.")
    planner_daemon.add_argument("--sport-slug", action="append", default=[], help="Optional repeatable sport slug. Defaults to all supported sports.")
    planner_daemon.add_argument("--scheduled-interval-seconds", type=float, default=3600.0, help="Scheduled planning interval per sport (default: 3600 = once per hour).")
    planner_daemon.add_argument("--loop-interval-seconds", type=float, default=5.0, help="Daemon tick loop interval.")

    live_discovery_planner_daemon = subparsers.add_parser("live-discovery-planner-daemon", help="Run the continuous live sport-surface planner loop.")
    live_discovery_planner_daemon.add_argument("--sport-slug", action="append", default=[], help="Optional repeatable sport slug. Defaults to all supported sports.")
    live_discovery_planner_daemon.add_argument("--loop-interval-seconds", type=float, default=5.0, help="Daemon tick loop interval.")

    final_sync_planner_daemon = subparsers.add_parser(
        "final-sync-planner-daemon",
        help=(
            "Run the FinalSyncPlannerDaemon. Publishes one-shot hydrate jobs "
            "(scope=final_sync) for events finalised more than "
            "SOFASCORE_FINAL_SYNC_DELAY_SECONDS ago. The orchestrator stamps "
            "event_terminal_state.locked_at on success, freezing the event "
            "from further processing."
        ),
    )
    # daemon takes no extra args — config via env vars.

    unlock_event = subparsers.add_parser(
        "unlock-event",
        help=(
            "Operator escape hatch: clear event_terminal_state.locked_at "
            "so a frozen event can re-enter the FinalSyncPlanner queue."
        ),
    )
    unlock_event.add_argument(
        "--event-id", required=True, type=int, help="Event ID to unlock."
    )

    historical_planner_daemon = subparsers.add_parser("historical-planner-daemon", help="Run the rolling historical planner loop.")
    historical_planner_daemon.add_argument("--sport-slug", action="append", default=[], help="Optional repeatable sport slug. Defaults to all supported sports.")
    historical_planner_daemon.add_argument("--date-from", help="Optional inclusive start date in YYYY-MM-DD format for manual override mode.")
    historical_planner_daemon.add_argument("--date-to", help="Optional inclusive end date in YYYY-MM-DD format for manual override mode.")
    historical_planner_daemon.add_argument("--dates-per-tick", type=int, default=1, help="Maximum archival dates to publish per sport on each planner tick.")
    historical_planner_daemon.add_argument("--loop-interval-seconds", type=float, default=5.0, help="Daemon tick loop interval.")

    backfill_cursor = subparsers.add_parser(
        "backfill-cursor",
        help=(
            "Manage tournament_registry backfill cursors. "
            "``reseed-stuck`` retargets UTs whose cursor sits on a "
            "season with zero finished events to the newest finished "
            "season for that UT."
        ),
    )
    backfill_cursor.add_argument(
        "action", choices=["show", "reseed-stuck"], help="Cursor action.",
    )
    backfill_cursor.add_argument(
        "--min-cat-priority", type=int, default=0,
        help="Lower bound on category.priority to consider (0 = all, "
             "6 = top-5 European leagues + international).",
    )
    backfill_cursor.add_argument(
        "--sport-slug", default=None, help="Optional sport filter for ``show``.",
    )

    backfill_priorities = subparsers.add_parser(
        "backfill-priorities",
        help=(
            "Inspect or reload backfill priority config "
            "(/opt/sofascore/config/backfill_priorities.yaml). See "
            "docs/BACKFILL_PRIORITIES.md."
        ),
    )
    backfill_priorities.add_argument(
        "action",
        choices=["show", "reload", "dry-run"],
        help=(
            "show: parse the file + print effective config; "
            "reload: SIGHUP the historical-tournament-planner unit; "
            "dry-run: parse + print per-sport share ratios."
        ),
    )
    backfill_priorities.add_argument(
        "--config-path",
        default=None,
        help="Override the default config path.",
    )

    historical_tournament_planner_daemon = subparsers.add_parser("historical-tournament-planner-daemon", help="Run the historical tournament/season planner loop.")
    historical_tournament_planner_daemon.add_argument("--sport-slug", action="append", default=[], help="Optional repeatable sport slug. Defaults to all supported sports.")
    historical_tournament_planner_daemon.add_argument("--consumer-name", default="historical-tournament-planner-1", help="Opaque planner instance label for logs and tmux naming.")
    historical_tournament_planner_daemon.add_argument("--tournaments-per-tick", type=int, default=10, help="Maximum archival tournaments to publish per sport on each planner tick.")
    historical_tournament_planner_daemon.add_argument("--loop-interval-seconds", type=float, default=10.0, help="Daemon tick loop interval.")

    # Stage 3.4 (2026-05-20 historical layer): one-shot synchronous
    # backfill for a single (unique_tournament_id, season_id) pair.
    # Walks the same pipeline as the historical tournament worker
    # (services.historical_archive_service.run_historical_tournament_archive)
    # but exits when the requested season completes — ideal for
    # operator-driven archive runs of named seasons (e.g. EPL 1999,
    # Champions League 2009/10). Uses the non-residential historical
    # proxy pool via _HISTORICAL_COMMANDS membership above.
    # Stage 3.5 (2026-05-20): SQL migration runner. Avoids the manual
    # ``psql -f migrations/<file>.sql`` ops step. Subactions:
    #   apply         — apply every pending .sql in migrations/
    #   status        — show applied + pending
    #   mark-applied  — bootstrap helper, mark a file as applied
    #                   without running it (used once on prod to seed
    #                   the ledger with the back-catalog).
    db_migrate = subparsers.add_parser(
        "db-migrate",
        help=(
            "Apply or inspect SQL migrations from migrations/. "
            "Uses schema_migrations ledger + Postgres advisory lock."
        ),
    )
    db_migrate.add_argument(
        "action",
        choices=["apply", "status", "mark-applied"],
        help=(
            "apply: run all pending migrations. "
            "status: show applied + pending. "
            "mark-applied: record a file as applied without running it "
            "(bootstrap helper)."
        ),
    )
    db_migrate.add_argument(
        "--filename",
        default=None,
        help="For mark-applied: the migration filename (e.g. 2026-04-16_initial.sql).",
    )

    historical_backfill = subparsers.add_parser(
        "historical-backfill",
        help=(
            "One-shot synchronous backfill for a specific "
            "(unique_tournament_id, season_id). Reuses the historical "
            "tournament archive pipeline and exits on completion."
        ),
    )
    historical_backfill.add_argument(
        "--unique-tournament-id", type=int, required=True,
        help="Sofascore unique_tournament id (e.g. 7 = UEFA Champions League, 17 = English Premier League).",
    )
    historical_backfill.add_argument(
        "--season-id", type=int, required=True,
        help="Sofascore season id for the season to backfill (e.g. 41897 for EPL 2024/25).",
    )
    historical_backfill.add_argument(
        "--sport-slug", default="football",
        help="Sport slug for sport_profile resolution. Defaults to 'football'.",
    )
    historical_backfill.add_argument(
        "--event-concurrency", type=int, default=4,
        help="Concurrent /event fetches inside the archive pipeline. Default 4.",
    )
    historical_backfill.add_argument(
        "--timeout", type=float, default=20.0,
        help="Per-request timeout in seconds. Default 20.0.",
    )

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

    resource_planner_daemon = subparsers.add_parser(
        "resource-planner-daemon",
        help="Run the generic resource-refresh planner (publishes JOB_REFRESH_RESOURCE for opted-in endpoints).",
    )
    resource_planner_daemon.add_argument(
        "--loop-interval-seconds",
        type=float,
        default=30.0,
        help="Tick interval in seconds.",
    )
    resource_planner_daemon.add_argument(
        "--publish-per-tick-cap",
        type=int,
        default=20,
        help="Maximum number of jobs published per tick (cold-start safety).",
    )
    resource_planner_daemon.add_argument(
        "--lag-threshold",
        type=int,
        default=5000,
        help="Skip the tick when stream:etl:resource_refresh length is at or above this value.",
    )

    worker_resource_refresh = subparsers.add_parser(
        "worker-resource-refresh",
        help="Run the generic resource-refresh consumer group loop.",
    )
    worker_resource_refresh.add_argument(
        "--consumer-name",
        default="worker-resource-refresh-1",
        help="Redis consumer name for the resource-refresh worker.",
    )
    worker_resource_refresh.add_argument(
        "--block-ms",
        type=int,
        default=5000,
        help="XREADGROUP block timeout in milliseconds.",
    )

    worker_normalize = subparsers.add_parser(
        "worker-normalize",
        help="Run the normalize consumer group loop (stream:etl:normalize).",
    )
    worker_normalize.add_argument(
        "--consumer-name",
        default="worker-normalize-1",
        help="Redis consumer name for the normalize worker.",
    )
    worker_normalize.add_argument(
        "--block-ms",
        type=int,
        default=5000,
        help="XREADGROUP block timeout in milliseconds.",
    )

    backfill_leaderboards = subparsers.add_parser(
        "backfill-leaderboards",
        help=(
            "P0.2 one-shot: scan completed top-tier (ut, season) pairs missing "
            "POS / top-players / top-teams snapshots locally and publish "
            "JOB_REFRESH_RESOURCE envelopes to stream:etl:resource_refresh."
        ),
    )
    backfill_leaderboards.add_argument(
        "--sport-slug", default="football",
        help="Sport slug for top-tier scan (default football).",
    )
    backfill_leaderboards.add_argument(
        "--priority-rank", type=int, default=30,
        help="Tournament_registry.priority_rank threshold (default 30).",
    )
    backfill_leaderboards.add_argument(
        "--completed-gap-days", type=int, default=7,
        help="Days since last event for a season to be considered completed (default 7).",
    )
    backfill_leaderboards.add_argument(
        "--history-window-days", type=int, default=365,
        help="How far back to scan completed pairs (default 365).",
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

    worker_live_details = subparsers.add_parser(
        "worker-live-details",
        help=(
            "Run the live-details consumer group loop (P0(a) split-details). "
            "Consumes ``stream:etl:live_details`` published by live-tier "
            "workers when LIVE_SPLIT_DETAILS_FANOUT=1. Independent capacity "
            "from tier_1/2/3."
        ),
    )
    worker_live_details.add_argument(
        "--consumer-name",
        default="worker-live-details-1",
        help="Redis consumer name for the live-details worker.",
    )
    worker_live_details.add_argument(
        "--block-ms",
        type=int,
        default=5000,
        help="XREADGROUP block timeout in milliseconds.",
    )
    worker_live_warm.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_maintenance = subparsers.add_parser("worker-maintenance", help="Run the maintenance/recovery consumer group loop.")
    worker_maintenance.add_argument("--consumer-name", default="worker-maintenance-1", help="Redis consumer name for the maintenance worker.")
    worker_maintenance.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    worker_historical_maintenance = subparsers.add_parser("worker-historical-maintenance", help="Run the archival maintenance/recovery consumer group loop.")
    worker_historical_maintenance.add_argument("--consumer-name", default="worker-historical-maintenance-1", help="Redis consumer name for the archival maintenance worker.")
    worker_historical_maintenance.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")

    live_rescue_daemon = subparsers.add_parser(
        "live-rescue-daemon",
        help=(
            "A2 Phase 0: periodic force-hydrate for stuck live events. "
            "Scans /events/live and re-publishes hydrate jobs for events "
            "whose Redis live_state.last_ingested_at is stale "
            "(SOFASCORE_LIVE_RESCUE_STALE_MINUTES, default 5). "
            "Disabled by default — enable with SOFASCORE_LIVE_RESCUE_ENABLED=true."
        ),
    )
    # No CLI arguments — config is fully env-driven (mirrors housekeeping)
    # so operators can toggle behaviour via .env + systemctl restart without
    # editing unit files.

    ws_consumer = subparsers.add_parser(
        "ws-consumer",
        help=(
            "Stream Sofascore WS deltas (wss://ws.sofascore.com:9222) into "
            "the normalized event tables and invalidate event_payload_cache. "
            "Replaces the latency floor of the 5s polling path with ~100ms "
            "push-based updates for live state."
        ),
    )
    ws_consumer.add_argument(
        "--sports",
        default=None,
        help=(
            "Comma-separated sport slugs to subscribe to. "
            "Default: all 13 (football,basketball,tennis,table-tennis,"
            "volleyball,handball,ice-hockey,baseball,american-football,"
            "rugby,cricket,futsal,esports)."
        ),
    )
    ws_consumer.add_argument(
        "--no-odds",
        action="store_true",
        help="Skip odds.* subscriptions (cuts WS volume ~65%%).",
    )
    ws_consumer.add_argument(
        "--reconnect-delay",
        type=float,
        default=10.0,
        help="Seconds to wait before reconnecting after a transport error.",
    )
    ws_consumer.add_argument(
        "--no-fanout",
        action="store_true",
        help=(
            "Disable redis pub/sub fanout to the mirror WS server. "
            "Use when running consumer-only without the mirror layer."
        ),
    )

    ws_server = subparsers.add_parser(
        "ws-server",
        help=(
            "Mirror Sofascore WebSocket server: speaks the same NATS "
            "subset that wss://ws.sofascore.com:9222 does. Mobile clients "
            "SUB sport.{slug} / odds.{slug}.1 / event.{id} and receive "
            "deltas pushed in real time from the consumer-side fanout. "
            "Fire-and-forget; no replay buffer (clients refresh REST on "
            "reconnect)."
        ),
    )
    ws_server.add_argument("--host", default="127.0.0.1", help="Bind host.")
    ws_server.add_argument("--port", type=int, default=8001, help="Bind port.")

    backfill_cursor_bootstrap = subparsers.add_parser(
        "backfill-cursor-bootstrap",
        help=(
            "Phase 1 (2026-05-16): seed tournament_registry.next_season_backfill_id "
            "to each active UT's most recent season. One-shot; the historical "
            "tournament planner walks from there to older seasons one job at a time. "
            "Re-run safely: only NULL cursors get seeded unless --reseed is given."
        ),
    )
    backfill_cursor_bootstrap.add_argument(
        "--sport-slug",
        default=None,
        help="Limit seeding to one sport (default: all sports with historical_enabled).",
    )
    backfill_cursor_bootstrap.add_argument(
        "--reseed",
        action="store_true",
        help="Also overwrite existing non-NULL cursors. Use with caution.",
    )

    cache_warmer = subparsers.add_parser(
        "api-cache-warmer",
        help=(
            "Run the N4 Layer C cache warmer: periodically fetches hot "
            "sport-level API endpoints so the Redis response cache stays "
            "populated. See docs/N4_API_PERFORMANCE_PLAN.md."
        ),
    )
    cache_warmer.add_argument(
        "--consumer-name",
        default="api-cache-warmer-1",
        help="Opaque daemon instance label for logs and systemd naming.",
    )

    monitoring_daemon = subparsers.add_parser(
        "monitoring-daemon",
        help=(
            "Run the N1 monitoring daemon: poll /ops/* SLO endpoints, "
            "classify against env-tunable thresholds, send deduplicated "
            "Telegram alerts. See docs/N1_MONITORING_PLAN.md."
        ),
    )
    monitoring_daemon.add_argument(
        "--consumer-name",
        default="monitoring-daemon-1",
        help="Opaque daemon instance label for logs and systemd naming.",
    )
    monitoring_daemon.add_argument(
        "--base-url",
        default=None,
        help=(
            "Override the local API base URL (default uses env "
            "SOFASCORE_MONITORING_BASE_URL or http://127.0.0.1:8000)."
        ),
    )
    monitoring_daemon.add_argument(
        "--interval-seconds",
        type=float,
        default=None,
        help=(
            "Override poll interval in seconds (default uses env "
            "SOFASCORE_MONITORING_INTERVAL_SECONDS or 60.0)."
        ),
    )
    monitoring_daemon.add_argument(
        "--smoke-test",
        action="store_true",
        help=(
            "Send a synthetic CRIT alert immediately and exit. Used to "
            "verify the Telegram channel is wired up after initial deploy."
        ),
    )
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
    # Stage 1.5 (2026-05-20 stability re-audit, Constraint #2b):
    # explicit socket-level timeouts + retry on packet-level timeout.
    # Default redis-py: socket_timeout=None → a dead TCP socket keeps
    # XREADGROUP blocked past its BLOCK budget because BLOCK governs
    # only the Redis-side wait, not the client-side read. Without
    # health_check_interval an idle connection that has been silently
    # killed by a stateful firewall is only discovered on the next
    # XREADGROUP. retry_on_timeout=True papers over a single packet
    # glitch before the client gives up; combined with Fix #2b in
    # retry_policy.py (RedisError → retryable), the worker survives
    # transient Redis hiccups.
    backend = redis.Redis.from_url(
        resolved_url,
        decode_responses=True,
        socket_timeout=15.0,
        socket_connect_timeout=10.0,
        retry_on_timeout=True,
        health_check_interval=30,
    )
    backend.ping()
    return backend


async def _close_redis_backend(backend) -> None:
    close_backend = getattr(backend, "close", None)
    if not callable(close_backend):
        return
    maybe_awaitable = close_backend()
    if inspect.isawaitable(maybe_awaitable):
        await maybe_awaitable


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


async def _run_cache_warmer(args) -> int:
    """Standalone entry point for the N4 Layer C cache-warmer daemon."""

    import httpx

    from .cache_warmer import CacheWarmerConfig, CacheWarmerDaemon

    del args  # consumer-name is purely for logs/systemd labeling
    env = _load_project_env()
    config = CacheWarmerConfig.from_env(env)
    if not config.enabled:
        logger.info(
            "api-cache-warmer: SOFASCORE_CACHE_WARMER_ENABLED=0, exiting."
        )
        return 0

    http_client = httpx.AsyncClient(timeout=config.request_timeout_seconds)
    daemon = CacheWarmerDaemon(
        config=config,
        http_client=http_client,
    )
    try:
        await daemon.run_forever()
    finally:
        try:
            await http_client.aclose()
        except Exception:  # noqa: BLE001
            pass
    return 0


async def _run_monitoring_daemon(args) -> int:
    """Standalone entry point for the N1 monitoring daemon.

    Does not open the asyncpg pool — the daemon only reads /ops/*
    endpoints over HTTP and writes dedupe state to Redis. Decoupling
    from the DB pool keeps the daemon from interacting with planner
    transactions or sharing /ops/health's connection.
    """

    import httpx

    env = _load_project_env()
    config = MonitoringConfig.from_env(env)
    if args.base_url:
        from dataclasses import replace as _replace

        config = _replace(config, base_url=str(args.base_url))
    if args.interval_seconds is not None:
        from dataclasses import replace as _replace

        config = _replace(config, interval_seconds=float(args.interval_seconds))
    if not config.enabled and not args.smoke_test:
        logger.info(
            "monitoring-daemon: SOFASCORE_MONITORING_ENABLED=0, exiting."
        )
        return 0

    if config.has_telegram():
        sink = TelegramAlertSink(
            bot_token=config.telegram_bot_token or "",
            chat_id=config.telegram_chat_id or "",
            timeout_seconds=config.telegram_timeout_seconds,
        )
    else:
        logger.warning(
            "monitoring-daemon: Telegram credentials missing — using NullAlertSink "
            "(set SOFASCORE_MONITORING_TELEGRAM_BOT_TOKEN and "
            "SOFASCORE_MONITORING_TELEGRAM_CHAT_ID)."
        )
        sink = NullAlertSink()

    # Smoke-test mode: send one synthetic message and exit. Used right
    # after a fresh deploy to confirm the Telegram channel works without
    # waiting for a real SLO breach.
    if args.smoke_test:
        from datetime import datetime, timezone

        message = (
            f"[SMOKE] sofascore monitoring-daemon online\n"
            f"Time: {datetime.now(timezone.utc).isoformat()}\n"
            f"Host: {config.host_label}\n"
            f"Consumer: {args.consumer_name}\n"
            f"Note: this is a synthetic smoke-test alert (--smoke-test)."
        )
        ok = await sink.send(message)
        aclose = getattr(sink, "aclose", None)
        if callable(aclose):
            await aclose()
        return 0 if ok else 1

    # Dedupe store: Redis when available, NullDedupeStore (fail-open) when
    # we cannot reach Redis. The fail-open dedupe is intentional — better
    # to risk a duplicate alert than to suppress a real breach silently.
    dedupe_store = NullDedupeStore()
    redis_backend = None
    try:
        redis_backend = _load_redis_backend(
            getattr(args, "redis_url", None),
            allow_memory_fallback=bool(getattr(args, "allow_memory_redis", False)),
        )
        dedupe_store = RedisDedupeStore(redis_backend)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "monitoring-daemon: Redis unavailable, using NullDedupeStore "
            "(duplicates may slip through): %r",
            exc,
        )

    http_client = httpx.AsyncClient(timeout=config.http_request_timeout_seconds)

    thresholds_overrides = {
        "oldest_hot_score_age_seconds": {
            "warn": config.oldest_hot_age_warn_seconds,
            "crit": config.oldest_hot_age_crit_seconds,
        },
        "tier_1_blocked_rate_cumulative": {
            "warn": config.tier_1_blocked_warn_rate,
            "crit": config.tier_1_blocked_crit_rate,
        },
        "refresh_live_event_success_rate_5min": {
            "warn": config.refresh_success_warn_rate,
            "crit": config.refresh_success_crit_rate,
        },
        # Task 2 (2026-05-15) — tier_1 P5b quarantine threshold.
        "tier_1_quarantined_events": {
            "warn": config.tier_1_quarantined_warn,
            "crit": config.tier_1_quarantined_crit,
        },
        # Phase 2 — queue XLEN thresholds.
        "hydrate_xlen": {
            "warn": config.hydrate_xlen_warn,
            "crit": config.hydrate_xlen_crit,
        },
        "live_hot_xlen": {
            "warn": config.live_hot_xlen_warn,
            "crit": config.live_hot_xlen_crit,
        },
        "live_warm_xlen": {
            "warn": config.live_warm_xlen_warn,
            "crit": config.live_warm_xlen_crit,
        },
        "live_discovery_xlen": {
            "warn": config.live_discovery_xlen_warn,
            "crit": config.live_discovery_xlen_crit,
        },
        "discovery_xlen": {
            "warn": config.discovery_xlen_warn,
            "crit": config.discovery_xlen_crit,
        },
        # Phase 3 — job signal thresholds.
        "failed_jobs_15min": {
            "warn": config.failed_jobs_warn,
            "crit": config.failed_jobs_crit,
        },
        "retry_rate_15min": {
            "warn": config.retry_rate_warn,
            "crit": config.retry_rate_crit,
        },
        "no_recent_jobs_age_seconds": {
            "warn": config.no_recent_jobs_warn_seconds,
            "crit": config.no_recent_jobs_crit_seconds,
        },
    }

    async def _signal_source() -> list:
        return await fetch_all_signals_from_api(
            base_url=config.base_url,
            http_client=http_client,
            timeout_seconds=config.http_request_timeout_seconds,
            overrides=thresholds_overrides,
            include_job_signals=config.job_signals_enabled,
        )

    daemon = MonitoringDaemon(
        config=config,
        signal_source=_signal_source,
        sink=sink,
        dedupe=dedupe_store,
    )
    try:
        await daemon.run_forever()
    finally:
        aclose = getattr(sink, "aclose", None)
        if callable(aclose):
            try:
                await aclose()
            except Exception:  # noqa: BLE001
                pass
        try:
            await http_client.aclose()
        except Exception:  # noqa: BLE001
            pass
        if redis_backend is not None:
            try:
                await _close_redis_backend(redis_backend)
            except Exception:  # noqa: BLE001
                pass
    return 0


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

    def get(self, key: str, *, now_ms: int | None = None) -> str | None:
        observed_at = int(now_ms if now_ms is not None else 0)
        self._expire_scalar_key(key, now_ms=observed_at)
        current = self.values.get(key)
        if current is None:
            return None
        return str(current.get("value"))

    def delete(self, key: str) -> int:
        if key not in self.values:
            return 0
        del self.values[key]
        return 1

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
