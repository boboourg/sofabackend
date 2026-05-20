"""Bootstrap helpers for long-running planner and worker services."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass
from typing import Any

from ..jobs.types import JOB_REPLAY_FAILED_JOB
from ..queue.delayed import DelayedJobScheduler
from ..queue.backfill_governor import (
    BackfillGovernor,
    BackfillGovernorThresholds,
    CompositeBackpressure,
)
from ..queue.dedupe import DedupeStore
from ..queue.event_circuit_breaker import EventCircuitBreaker
from ..queue.live_details_throttle import LiveDetailsThrottle
from ..queue.live_edges_throttle import LiveEdgesThrottle
from ..queue.live_tier_1_quarantine import LiveTier1RetryQuarantineStore
from ..queue.live_inflight import (
    HydrateInFlightStore,
    LiveEventDetailsInFlightStore,
    LiveEventInFlightStore,
    LiveEventRootInFlightStore,
)
from ..sources import build_source_adapter
from ..storage.planner_cursor_repository import PlannerCursorRepository
from ..storage.tournament_registry_repository import TournamentRegistryRepository
from ..queue.streams import (
    GROUP_DISCOVERY,
    GROUP_HYDRATE,
    GROUP_HISTORICAL_DISCOVERY,
    GROUP_HISTORICAL_ENRICHMENT,
    GROUP_HISTORICAL_HYDRATE,
    GROUP_HISTORICAL_MAINTENANCE,
    GROUP_HISTORICAL_TOURNAMENT,
    GROUP_LIVE_HOT,
    GROUP_LIVE_TIER_1,
    GROUP_LIVE_TIER_2,
    GROUP_LIVE_TIER_3,
    GROUP_LIVE_WARM,
    GROUP_NORMALIZE,
    GROUP_RESOURCE_REFRESH,
    GROUP_STRUCTURE_SYNC,
    HISTORICAL_CONSUMER_GROUPS,
    NORMALIZE_CONSUMER_GROUPS,
    OPERATIONAL_CONSUMER_GROUPS,
    RESOURCE_REFRESH_CONSUMER_GROUPS,
    STREAM_DISCOVERY,
    STREAM_HISTORICAL_DISCOVERY,
    STREAM_HISTORICAL_ENRICHMENT,
    STREAM_HISTORICAL_HYDRATE,
    STREAM_HISTORICAL_MAINTENANCE,
    STREAM_HISTORICAL_TOURNAMENT,
    STREAM_HYDRATE,
    STREAM_LIVE_DISCOVERY,
    STREAM_LIVE_HOT,
    STREAM_LIVE_TIER_1,
    STREAM_LIVE_TIER_2,
    STREAM_LIVE_TIER_3,
    STREAM_LIVE_WARM,
    STREAM_MAINTENANCE,
    STREAM_NORMALIZE,
    STREAM_RESOURCE_REFRESH,
    STREAM_STRUCTURE_SYNC,
    STRUCTURE_CONSUMER_GROUPS,
    RedisStreamQueue,
    StreamEntry,
)
from ..queue.empty_data import EmptyDataStore
from ..queue.pagination_done import PaginationDoneStore
from ..queue.resource_cursor import ResourceCursorStore
from ..queue.resource_negative_cache import ResourceNegativeCache
from ..queue.totw_404_store import ToTW404Store
from ..sport_profiles import SUPPORTED_SPORT_SLUGS, resolve_sport_profile
from ..ops.health import fetch_live_snapshot_repair_reasons
from .backpressure import BackpressureLimit, QueueBackpressure
from .backpressure_config import (
    HISTORICAL_DISCOVERY_MAX_LAG,
    HISTORICAL_ENRICHMENT_MAX_LAG,
    HISTORICAL_HYDRATE_MAX_LAG,
    HISTORICAL_TOURNAMENT_MAX_LAG,
    HYDRATE_MAX_LAG,
    LIVE_HOT_MAX_LAG,
    LIVE_TIER_1_MAX_LAG,
    LIVE_TIER_2_MAX_LAG,
    LIVE_TIER_3_MAX_LAG,
    LIVE_WARM_MAX_LAG,
    SCHEDULED_DISCOVERY_MAX_LAG,
    STRUCTURE_SYNC_MAX_LAG,
)
from .freshness_policy import FreshnessPolicy
from .housekeeping import HousekeepingConfig, HousekeepingLoop
from .live_discovery_planner import LiveDiscoveryPlannerDaemon, LiveDiscoveryPlanningTarget
from .historical_planner import (
    HistoricalCursorStore,
    HistoricalPlannerDaemon,
    HistoricalPlanningTarget,
    PostgresHistoricalCursorStore,
    build_historical_planning_targets,
    compute_historical_horizon,
)
from .backfill_priority_config import (
    BackfillPriorityConfig,
    ConfigValidationError,
)
from .historical_tournament_planner import (
    HistoricalTournamentCursorStore,
    HistoricalTournamentPlannerDaemon,
    HistoricalTournamentPlanningTarget,
)


# Default path for the operator-tunable backfill priority config.
# Override via env SOFASCORE_BACKFILL_PRIORITIES_PATH if you keep
# the file elsewhere (e.g. for tests or multi-tenant deployments).
_BACKFILL_PRIORITIES_DEFAULT_PATH = "/opt/sofascore/config/backfill_priorities.yaml"


def _load_backfill_priority_config_or_warn() -> BackfillPriorityConfig | None:
    """Load the priority config at planner startup. ``None`` keeps
    the legacy uniform-rotation behaviour — log + carry on when:
      * the file is absent (typical fresh deploy);
      * the YAML is malformed (operator typo; the planner shouldn't
        fail to start over a config glitch).
    """
    from pathlib import Path

    path = Path(os.environ.get("SOFASCORE_BACKFILL_PRIORITIES_PATH") or _BACKFILL_PRIORITIES_DEFAULT_PATH)
    try:
        cfg = BackfillPriorityConfig.load(path)
    except ConfigValidationError as exc:
        logger.warning(
            "backfill priorities at %s are malformed (%s) — running with no priority config",
            path, exc,
        )
        return None
    if not cfg.sport_weights and not cfg.ut_boost:
        # Empty config (file missing or all sections empty) — None keeps
        # the planner in legacy mode so we don't pay the per-sport budget
        # split overhead for free.
        return None
    return cfg
from .resource_planner import ResourcePlannerDaemon
from .resource_scope import (
    CustomIdOfManagedEventsResolver,
    CustomIdOfRegistryEventsResolver,
    EventOfFinishedBaseballResolver,
    ManagedScopeResolver,
    PeriodOfManagedPairsResolver,
    PeriodOfRegistryFootballResolver,
    PlayerOfActiveSquadFirstPageResolver,
    PlayerOfActiveSquadResolver,
    PlayerOfNationalTeamHistoryResolver,
    RoundOfManagedPairsResolver,
    RoundOfRegistryFootballResolver,
    SeasonOfActiveUTBaseResolver,
    SeasonOfActiveUTEventsResolver,
    SeasonOfActiveUTStandingsResolver,
    SeasonOfRegistryUTCuptreesHistoricalResolver,
    SeasonOfRegistryUTResolver,
    SeasonOfRegistryUTRoundsHistoricalResolver,
    TeamOfActiveUTFirstPageResolver,
    TeamOfActiveUTResolver,
    TeamOfActiveUTSeasonResolver,
    TeamOfRegistryUTResolver,
)
from .structure_planner import (
    StructureCursorStore,
    StructurePlannerDaemon,
    StructurePlanningTarget,
    load_managed_tournaments,
)
from .tournament_registry_refresh import (
    TournamentRegistryRefreshCursorStore,
    TournamentRegistryRefreshDaemon,
    refresh_tournament_registry_for_sport,
)
from .job_audit_logger import JobAuditLogger
from .planner_daemon import PlannerDaemon, ScheduledPlanningTarget
from ..workers._stream_jobs import decode_stream_payload
from ..workers.discovery_worker import DiscoveryWorker
from ..workers.hydrate_worker import HydrateWorker
from ..workers.historical_archive_worker import HistoricalEnrichmentWorker, HistoricalTournamentWorker
from ..workers.live_worker_service import LiveWorkerService
from ..workers.maintenance_worker import MaintenanceWorker
from ..workers.maintenance_worker import ReclaimTarget
from ..workers.normalize_stream_worker import NormalizeStreamWorker
from ..workers.resource_refresh_worker import ResourceRefreshWorker
from ..workers.structure_worker import StructureSyncWorker

logger = logging.getLogger(__name__)


def _env_int_local(name: str, default: int) -> int:
    """Local copy of the ``_env_int`` pattern. Keeps this module's env reads
    independent from ``backpressure_config`` so callers can inspect a single
    canonical source for live-state sweep tuning.
    """

    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default

DELAYED_ENVELOPE_HASH = "hash:etl:delayed_envelopes"
DEFAULT_SERVICE_SPORT_SLUGS = SUPPORTED_SPORT_SLUGS


def _historical_tournament_targets_from_registry(
    registry_targets,
    *,
    sport_slugs: tuple[str, ...] | None = None,
) -> tuple[HistoricalTournamentPlanningTarget, ...]:
    allowed_sports = set(sport_slugs or ())
    grouped_ids: dict[str, list[int]] = {}
    for registry_target in registry_targets:
        sport_slug = str(registry_target.sport_slug).strip().lower()
        if allowed_sports and sport_slug not in allowed_sports:
            continue
        unique_tournament_id = int(registry_target.unique_tournament_id)
        sport_ids = grouped_ids.setdefault(sport_slug, [])
        if unique_tournament_id in sport_ids:
            continue
        sport_ids.append(unique_tournament_id)
    return tuple(
        HistoricalTournamentPlanningTarget(
            sport_slug=sport_slug,
            allowed_unique_tournament_ids=tuple(unique_tournament_ids),
        )
        for sport_slug, unique_tournament_ids in grouped_ids.items()
    )

class DelayedEnvelopeStore:
    """Stores serialized job payloads so delayed jobs can be reconstructed later."""

    def __init__(self, backend: Any, *, hash_key: str = DELAYED_ENVELOPE_HASH) -> None:
        self.backend = backend
        self.hash_key = hash_key

    def save_entry(self, entry: StreamEntry, *, attempt_increment: int = 1) -> None:
        self.save_payload(entry.values, fallback_job_id=entry.message_id, attempt_increment=attempt_increment)

    def save_payload(
        self,
        payload: dict[str, object] | dict[str, str],
        *,
        fallback_job_id: str | None = None,
        attempt_increment: int = 0,
    ) -> None:
        job_id = str(payload.get("job_id") or fallback_job_id or "")
        if not job_id:
            raise RuntimeError("Delayed envelope store requires a job_id.")
        values = {str(key): value for key, value in payload.items()}
        if attempt_increment:
            raw_attempt = values.get("attempt")
            try:
                next_attempt = int(raw_attempt) + int(attempt_increment)
            except (TypeError, ValueError):
                next_attempt = 1 + int(attempt_increment)
            values["attempt"] = next_attempt
        encoded = json.dumps(values, ensure_ascii=True, sort_keys=True)
        try:
            self.backend.hset(self.hash_key, mapping={job_id: encoded})
        except TypeError:
            self.backend.hset(self.hash_key, {job_id: encoded})

    def load(self, job_id: str):
        raw_payload = self.backend.hgetall(self.hash_key).get(str(job_id))
        if raw_payload in (None, ""):
            return None
        if isinstance(raw_payload, bytes):
            raw_payload = raw_payload.decode("utf-8", errors="ignore")
        values = json.loads(str(raw_payload))
        job = decode_stream_payload(values, fallback_job_id=str(job_id))
        delete = getattr(self.backend, "hdel", None)
        if callable(delete):
            delete(self.hash_key, str(job_id))
        return job


@dataclass
class ServiceApp:
    app: Any

    def __post_init__(self) -> None:
        if self.app.redis_backend is None:
            raise RuntimeError("Redis backend is required for continuous services.")
        self.stream_queue = self.app.stream_queue or RedisStreamQueue(self.app.redis_backend)
        self.live_state_store = self.app.live_state_store
        self.delayed_scheduler = DelayedJobScheduler(self.app.redis_backend)
        self.delayed_envelope_store = DelayedEnvelopeStore(self.app.redis_backend)
        self.completion_store = DedupeStore(self.app.redis_backend)
        self.live_event_inflight_store = LiveEventInFlightStore(self.app.redis_backend)
        # Phase 2.1 (2026-05-20 perf audit): cross-consumer single-flight
        # guard for HydrateWorker. Prevents two parallel
        # sofascore-hydrate@N processes from racing on the same event_id.
        self.hydrate_inflight_store = HydrateInFlightStore(self.app.redis_backend)
        # P0(a) split-details rollout primitives. Constructed unconditionally
        # so the dependency wiring is stable regardless of
        # ``LIVE_SPLIT_DETAILS_FANOUT`` env state — only the live-tier
        # worker's ``handle()`` checks the flag at run time.
        self.live_event_details_inflight_store = LiveEventDetailsInFlightStore(self.app.redis_backend)
        self.live_details_throttle = LiveDetailsThrottle(self.app.redis_backend)
        # P0(b) tier_1 root-only rollout primitives. Constructed
        # unconditionally so the dependency wiring is stable regardless
        # of ``LIVE_TIER_1_ROOT_ONLY`` env state — only the live-tier
        # worker's ``handle()`` checks the flag at run time.
        self.live_event_root_inflight_store = LiveEventRootInFlightStore(self.app.redis_backend)
        self.live_edges_throttle = LiveEdgesThrottle(self.app.redis_backend)
        # P0(c) tier_1 root-only retry quarantine. Store is constructed
        # unconditionally so dependency wiring stays stable; the
        # build_live_worker wiring below only passes it to LiveWorkerService
        # when ``LIVE_TIER_1_QUARANTINE_ENABLED`` env flag is set. Default
        # behaviour (flag unset) → store is constructed but never consulted
        # at the handler layer, so prod sees no behavioural change until a
        # separate ACK enables the flag on a canary instance.
        self.live_tier_1_quarantine_store = LiveTier1RetryQuarantineStore(self.app.redis_backend)
        # P5b: generic per-event circuit breaker — extends P0(c).2 quarantine
        # to hydrate + discovery lanes. Same store class with lane-parameterized
        # Redis keys, so tier_1 quarantine and hydrate/discovery circuit
        # breakers operate on disjoint key namespaces and do not interfere.
        # Wiring is gated by per-lane env flags
        # (HYDRATE_EVENT_CIRCUIT_BREAKER_ENABLED,
        # DISCOVERY_EVENT_CIRCUIT_BREAKER_ENABLED) — see build_*_worker
        # methods below.
        self.hydrate_circuit_breaker = EventCircuitBreaker(self.app.redis_backend, lane="hydrate")
        # Note: discovery_worker failures from P3 data are at the bulk
        # /sport/{X}/events/live list endpoint (not per-event). A
        # per-event circuit breaker doesn't fit that failure shape, so
        # discovery is intentionally NOT wired into the circuit breaker
        # in this iteration. If empirical data after P5b rollout shows
        # discovery still publishing quarantined-by-hydrate event-ids
        # in tight loops, we can add ``EventCircuitBreaker(lane="discovery")``
        # at the per-event publish gate in a follow-up.
        self.freshness_policy = FreshnessPolicy(store=DedupeStore(self.app.redis_backend))
        self.historical_cursor_store = HistoricalCursorStore(self.app.redis_backend)
        self.historical_tournament_cursor_store = HistoricalTournamentCursorStore(self.app.redis_backend)
        self.structure_cursor_store = StructureCursorStore(self.app.redis_backend)
        self.resource_cursor_store = ResourceCursorStore(self.app.redis_backend)
        self.resource_negative_cache = ResourceNegativeCache(self.app.redis_backend)
        self.pagination_done_store = PaginationDoneStore(self.app.redis_backend)
        self.empty_data_store = EmptyDataStore(self.app.redis_backend)
        self.totw_404_store = ToTW404Store(self.app.redis_backend)
        database = getattr(self.app, "database", None)
        self.job_audit_logger = None if database is None else JobAuditLogger(database=database)

    def ensure_consumer_groups(self) -> None:
        for stream, group in OPERATIONAL_CONSUMER_GROUPS:
            self.stream_queue.ensure_group(stream, group)

    async def ensure_tier_override_registry_loaded(self):
        """A3 Phase 0 (2026-05-16): forward to ``HybridApp.ensure_tier_override_registry``
        so workers and planner daemons get a populated registry at boot.
        Best-effort — never raises; on DB error the registry stays empty
        and live-tier dispatch falls back to the legacy heuristic."""
        ensure = getattr(self.app, "ensure_tier_override_registry", None)
        if not callable(ensure):
            return None
        try:
            return await ensure()
        except Exception:  # noqa: BLE001 — never block worker startup
            logger.exception("ensure_tier_override_registry failed; continuing with empty registry")
            return None

    def ensure_historical_consumer_groups(self) -> None:
        for stream, group in HISTORICAL_CONSUMER_GROUPS:
            self.stream_queue.ensure_group(stream, group)

    def ensure_structure_consumer_groups(self) -> None:
        for stream, group in STRUCTURE_CONSUMER_GROUPS:
            self.stream_queue.ensure_group(stream, group)

    def ensure_resource_refresh_consumer_groups(self) -> None:
        for stream, group in RESOURCE_REFRESH_CONSUMER_GROUPS:
            self.stream_queue.ensure_group(stream, group)

    def ensure_normalize_consumer_groups(self) -> None:
        for stream, group in NORMALIZE_CONSUMER_GROUPS:
            self.stream_queue.ensure_group(stream, group)

    def build_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        scheduled_interval_seconds: float = 3600.0,  # changed from 300 → 3600 (once per hour)
        loop_interval_seconds: float = 5.0,
    ) -> PlannerDaemon:
        normalized_sports = tuple(sport_slugs or DEFAULT_SERVICE_SPORT_SLUGS)
        targets = tuple(
            ScheduledPlanningTarget(
                sport_slug=sport_slug,
                interval_ms=int(scheduled_interval_seconds * 1000),
                date_factory=self._date_factory,
            )
            for sport_slug in normalized_sports
        )
        sweep_callback, sweep_interval_ms = self._build_live_state_sweep_wiring()
        return PlannerDaemon(
            queue=self.stream_queue,
            delayed_scheduler=self.delayed_scheduler,
            delayed_job_loader=self.delayed_envelope_store.load,
            live_state_store=self.live_state_store,
            scheduled_targets=targets,
            loop_interval_s=loop_interval_seconds,
            scheduled_backpressure=QueueBackpressure(
                queue=self.stream_queue,
                limits=(
                    BackpressureLimit(stream=STREAM_DISCOVERY, group=GROUP_DISCOVERY, max_lag=SCHEDULED_DISCOVERY_MAX_LAG),
                    BackpressureLimit(stream=STREAM_HYDRATE, group=GROUP_HYDRATE, max_lag=HYDRATE_MAX_LAG),
                ),
            ),
            live_backpressure=QueueBackpressure(
                queue=self.stream_queue,
                limits=(
                    BackpressureLimit(stream=STREAM_LIVE_TIER_1, group=GROUP_LIVE_TIER_1, max_lag=LIVE_TIER_1_MAX_LAG),
                    BackpressureLimit(stream=STREAM_LIVE_TIER_2, group=GROUP_LIVE_TIER_2, max_lag=LIVE_TIER_2_MAX_LAG),
                    BackpressureLimit(stream=STREAM_LIVE_TIER_3, group=GROUP_LIVE_TIER_3, max_lag=LIVE_TIER_3_MAX_LAG),
                    BackpressureLimit(stream=STREAM_LIVE_WARM, group=GROUP_LIVE_WARM, max_lag=LIVE_WARM_MAX_LAG),
                ),
            ),
            live_state_sweep_callback=sweep_callback,
            live_state_sweep_interval_ms=sweep_interval_ms,
        )

    def _build_live_state_sweep_wiring(self):
        """Construct the LiveStateSweeper + callback closure (P0.B 2026-05-14).

        Returns ``(callback, interval_ms)``. Callback is an awaitable
        ``async f(now_ms)`` that acquires a Postgres connection from the
        shared pool and runs one sweep cycle. The PlannerDaemon stores
        this callback and calls it on its own schedule so the daemon
        does not need direct database access. Interval defaults to 60s
        and can be tuned via ``SOFASCORE_LIVE_STATE_SWEEP_INTERVAL_MS``.
        """

        from .live_state_sweeper import LiveStateSweeper

        interval_ms = _env_int_local("SOFASCORE_LIVE_STATE_SWEEP_INTERVAL_MS", 60_000)
        grace_seconds = _env_int_local(
            "SOFASCORE_LIVE_STATE_SWEEP_GRACE_SECONDS", 300
        )
        max_remove = _env_int_local(
            "SOFASCORE_LIVE_STATE_SWEEP_MAX_REMOVE_PER_TICK", 5000
        )
        if interval_ms <= 0:
            return None, 0
        sweeper = LiveStateSweeper(
            live_state_store=self.live_state_store,
            grace_seconds=grace_seconds,
            max_remove_per_tick=max_remove,
        )

        async def _sweep(now_ms: int) -> None:
            from datetime import datetime, timezone as _tz
            now_dt = datetime.fromtimestamp(now_ms / 1000, tz=_tz.utc)
            # 2026-05-14 fix: was `self.database.connection()` which raised
            # AttributeError on prod every minute since the P0.B deploy —
            # ServiceApp holds the database on `self.app.database`, not
            # directly. See service_app.py lines 789/929 for the pattern
            # the rest of the module uses.
            async with self.app.database.connection() as conn:
                report = await sweeper.run_once(sql_executor=conn, now=now_dt)
            if report.removed_event_count > 0 or report.error is not None:
                logger.info(
                    "live_state_sweep done: scanned=%d removed=%d "
                    "lane_hot=%d lane_warm=%d lane_cold=%d elapsed_ms=%d error=%s",
                    report.scanned_finalized_count,
                    report.removed_event_count,
                    report.lane_removed_counts.get("hot", 0),
                    report.lane_removed_counts.get("warm", 0),
                    report.lane_removed_counts.get("cold", 0),
                    report.elapsed_ms,
                    report.error,
                )

        return _sweep, interval_ms

    def build_live_discovery_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        loop_interval_seconds: float = 5.0,
    ) -> LiveDiscoveryPlannerDaemon:
        normalized_sports = tuple(sport_slugs or DEFAULT_SERVICE_SPORT_SLUGS)
        targets = tuple(
            LiveDiscoveryPlanningTarget(
                sport_slug=sport_slug,
                interval_ms=int(resolve_sport_profile(sport_slug).live_discovery_interval_seconds * 1000),
            )
            for sport_slug in normalized_sports
        )
        database = getattr(self.app, "database", None)
        connection_factory = getattr(database, "connection", None) if database is not None else None
        drifted_sports_loader = None
        if callable(connection_factory):
            async def _load_drifted_sports(*, now_ms: int) -> dict[str, str]:
                async with connection_factory() as connection:
                    return await fetch_live_snapshot_repair_reasons(
                        connection,
                        sport_slugs=normalized_sports,
                        now=self._datetime_from_epoch_ms(now_ms),
                    )

            drifted_sports_loader = _load_drifted_sports
        return LiveDiscoveryPlannerDaemon(
            queue=self.stream_queue,
            targets=targets,
            loop_interval_s=loop_interval_seconds,
            backpressure=QueueBackpressure(
                queue=self.stream_queue,
                limits=(
                    BackpressureLimit(stream=STREAM_LIVE_TIER_1, group=GROUP_LIVE_TIER_1, max_lag=LIVE_TIER_1_MAX_LAG),
                    BackpressureLimit(stream=STREAM_LIVE_TIER_2, group=GROUP_LIVE_TIER_2, max_lag=LIVE_TIER_2_MAX_LAG),
                    BackpressureLimit(stream=STREAM_LIVE_TIER_3, group=GROUP_LIVE_TIER_3, max_lag=LIVE_TIER_3_MAX_LAG),
                    BackpressureLimit(stream=STREAM_LIVE_WARM, group=GROUP_LIVE_WARM, max_lag=LIVE_WARM_MAX_LAG),
                ),
            ),
            drifted_sports_loader=drifted_sports_loader,
        )

    def build_historical_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        dates_per_tick: int = 1,
        loop_interval_seconds: float = 5.0,
        today_factory=None,
    ) -> HistoricalPlannerDaemon:
        normalized_sports = tuple(sport_slugs or DEFAULT_SERVICE_SPORT_SLUGS)
        if bool(date_from) != bool(date_to):
            raise RuntimeError("Historical planner requires both date_from and date_to together.")
        database = getattr(self.app, "database", None)
        connection_factory = getattr(database, "connection", None) if database is not None else None
        runtime_config = getattr(self.app, "runtime_config", None)
        today_factory = today_factory or self._today_factory
        if callable(connection_factory) and runtime_config is not None:
            cursor_store = PostgresHistoricalCursorStore(
                repository=PlannerCursorRepository(),
                connection_factory=connection_factory,
                default_source_slug=runtime_config.source_slug,
            )
        else:
            cursor_store = self.historical_cursor_store

        if date_from and date_to:
            cursor_store = self.historical_cursor_store
            targets = tuple(
                HistoricalPlanningTarget(
                    source_slug=(
                        str(getattr(runtime_config, "source_slug", "sofascore")).strip().lower()
                    ),
                    sport_slug=sport_slug,
                    date_from=date_from,
                    date_to=date_to,
                    scope="historical",
                )
                for sport_slug in normalized_sports
            )
            target_loader = None
        else:
            if callable(connection_factory):
                repository = TournamentRegistryRepository()

                async def _target_loader():
                    async with connection_factory() as connection:
                        policies = await repository.list_historical_planning_policies(
                            connection,
                            sport_slugs=normalized_sports,
                        )
                    policies_by_sport = {policy.sport_slug: policy for policy in policies}
                    today = today_factory()
                    computed_targets: list[HistoricalPlanningTarget] = []
                    default_source_slug = str(getattr(runtime_config, "source_slug", "sofascore")).strip().lower()
                    for sport_slug in normalized_sports:
                        policy = policies_by_sport.get(sport_slug)
                        horizon = compute_historical_horizon(
                            today=today,
                            sport_slug=sport_slug,
                            start_override=None if policy is None else policy.historical_backfill_start_date,
                            end_override=None if policy is None else policy.historical_backfill_end_date,
                            recent_refresh_days=None if policy is None else policy.recent_refresh_days,
                        )
                        computed_targets.extend(
                            build_historical_planning_targets(
                                source_slug=default_source_slug if policy is None else policy.source_slug,
                                sport_slug=sport_slug,
                                horizon=horizon,
                            )
                        )
                    return tuple(computed_targets)

                targets = ()
                target_loader = _target_loader
            else:
                today = today_factory()
                computed_targets: list[HistoricalPlanningTarget] = []
                default_source_slug = str(getattr(runtime_config, "source_slug", "sofascore")).strip().lower()
                for sport_slug in normalized_sports:
                    computed_targets.extend(
                        build_historical_planning_targets(
                            source_slug=default_source_slug,
                            sport_slug=sport_slug,
                            horizon=compute_historical_horizon(today=today, sport_slug=sport_slug),
                        )
                    )
                targets = tuple(computed_targets)
                target_loader = None
        return HistoricalPlannerDaemon(
            queue=self.stream_queue,
            cursor_store=cursor_store,
            targets=targets,
            target_loader=target_loader,
            dates_per_tick=dates_per_tick,
            loop_interval_s=loop_interval_seconds,
            # Task 5 (2026-05-15): pair existing queue-lag backpressure
            # with a live-side governor — backfill pauses when live is
            # in CRIT regardless of stream lag.
            backpressure=CompositeBackpressure(
                checks=(
                    QueueBackpressure(
                        queue=self.stream_queue,
                        limits=(
                            BackpressureLimit(
                                stream=STREAM_HISTORICAL_DISCOVERY,
                                group=GROUP_HISTORICAL_DISCOVERY,
                                max_lag=HISTORICAL_DISCOVERY_MAX_LAG,
                            ),
                            BackpressureLimit(
                                stream=STREAM_HISTORICAL_HYDRATE,
                                group=GROUP_HISTORICAL_HYDRATE,
                                max_lag=HISTORICAL_HYDRATE_MAX_LAG,
                            ),
                        ),
                    ),
                    BackfillGovernor(
                        redis_backend=self.app.redis_backend,
                        thresholds=BackfillGovernorThresholds.from_env(),
                    ),
                ),
            ),
        )

    def build_historical_tournament_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        tournaments_per_tick: int = 10,
        loop_interval_seconds: float = 10.0,
    ) -> HistoricalTournamentPlannerDaemon:
        normalized_sports = tuple(sport_slugs or DEFAULT_SERVICE_SPORT_SLUGS)
        fallback_targets = tuple(
            HistoricalTournamentPlanningTarget(sport_slug=sport_slug)
            for sport_slug in normalized_sports
        )
        database = getattr(self.app, "database", None)
        connection_factory = getattr(database, "connection", None) if database is not None else None
        if callable(connection_factory):
            repository = TournamentRegistryRepository()

            async def _target_loader():
                try:
                    async with connection_factory() as connection:
                        registry_targets = await repository.list_active_targets(
                            connection,
                            sport_slugs=normalized_sports,
                            surface="historical",
                        )
                except Exception as exc:
                    logger.warning(
                        "historical tournament planner: tournament registry unavailable, falling back to sport-scoped targets: %s",
                        exc,
                    )
                    return fallback_targets
                return _historical_tournament_targets_from_registry(
                    registry_targets,
                    sport_slugs=normalized_sports,
                )

            computed_targets = ()
            target_loader = _target_loader
        else:
            computed_targets = fallback_targets
            target_loader = None
        # Phase 1 (2026-05-16): per-(UT, season) cursor selector. When
        # tournament_registry has cursors seeded (see
        # ``backfill-cursor-bootstrap`` CLI), the planner picks pending
        # rows in priority + UT-id order and publishes per-season jobs.
        # When the table is empty (fresh deploy, or only-non-football
        # sport not yet bootstrapped) it returns [] and the daemon falls
        # back to the legacy UT-only walk via ``selector`` above. So this
        # wiring is safe to ship even before every sport is seeded.
        if callable(connection_factory):
            cursor_repository = TournamentRegistryRepository()

            # D.1 (2026-05-18): strict category-priority barrier. When
            # SOFASCORE_BACKFILL_STRICT_CATEGORY_BARRIER is true (default),
            # the selector returns ONLY the highest pending cat.priority
            # bucket — so cat=20 (international) drains entirely before
            # cat=10 (England), and cat=0 (5,280 amateur UTs) only gets
            # touched at the very end. Env=false reverts to the legacy
            # priority_rank ASC walk for incident rollback.
            strict_barrier_enabled = (
                os.environ.get(
                    "SOFASCORE_BACKFILL_STRICT_CATEGORY_BARRIER", "true"
                ).strip().lower()
                not in ("0", "false", "no", "off")
            )

            async def _backfill_cursor_selector(*, sport_slug: str, limit: int):
                try:
                    async with connection_factory() as connection:
                        if strict_barrier_enabled:
                            return await cursor_repository.select_pending_cursors_by_top_category(
                                connection, sport_slug=sport_slug, limit=limit
                            )
                        return await cursor_repository.list_pending_backfill_cursors(
                            connection, sport_slug=sport_slug, limit=limit
                        )
                except Exception as exc:
                    logger.warning(
                        "historical tournament planner: cursor selector failed "
                        "(strict_barrier=%s), falling back to legacy walk: %s",
                        strict_barrier_enabled, exc,
                    )
                    return []

            backfill_cursor_selector = _backfill_cursor_selector
        else:
            backfill_cursor_selector = None

        return HistoricalTournamentPlannerDaemon(
            queue=self.stream_queue,
            cursor_store=self.historical_tournament_cursor_store,
            selector=self.app.select_unique_tournament_ids_after_cursor,
            targets=computed_targets,
            tournaments_per_tick=tournaments_per_tick,
            loop_interval_s=loop_interval_seconds,
            target_loader=target_loader,
            backfill_cursor_selector=backfill_cursor_selector,
            priority_config=_load_backfill_priority_config_or_warn(),
            # Task 5 (2026-05-15): live-side governor on top of stream
            # backpressure — see build_historical_planner_daemon comment.
            backpressure=CompositeBackpressure(
                checks=(
                    QueueBackpressure(
                        queue=self.stream_queue,
                        limits=(
                            BackpressureLimit(
                                stream=STREAM_HISTORICAL_TOURNAMENT,
                                group=GROUP_HISTORICAL_TOURNAMENT,
                                max_lag=HISTORICAL_TOURNAMENT_MAX_LAG,
                            ),
                            BackpressureLimit(
                                stream=STREAM_HISTORICAL_ENRICHMENT,
                                group=GROUP_HISTORICAL_ENRICHMENT,
                                max_lag=HISTORICAL_ENRICHMENT_MAX_LAG,
                            ),
                        ),
                    ),
                    BackfillGovernor(
                        redis_backend=self.app.redis_backend,
                        thresholds=BackfillGovernorThresholds.from_env(),
                    ),
                ),
            ),
        )

    def build_structure_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        loop_interval_seconds: float = 30.0,
        targets: tuple[StructurePlanningTarget, ...] | None = None,
    ) -> StructurePlannerDaemon:
        """Build a StructurePlannerDaemon.

        ``targets`` lets callers inject an explicit list (used by tests and by
        operators who want a hot-reloadable config). If None, we compute
        targets from env + profile at construction time.
        """

        if targets is None:
            normalized_sports = tuple(sport_slugs or DEFAULT_SERVICE_SPORT_SLUGS)
            fallback_targets = load_managed_tournaments(sport_slugs=normalized_sports)
            database = getattr(self.app, "database", None)
            connection_factory = getattr(database, "connection", None) if database is not None else None
            if callable(connection_factory):
                repository = TournamentRegistryRepository()

                async def _target_loader():
                    try:
                        async with connection_factory() as connection:
                            registry_targets = await repository.list_active_targets(
                                connection,
                                sport_slugs=normalized_sports,
                                surface="structure",
                            )
                    except Exception as exc:
                        logger.warning(
                            "structure planner: tournament registry unavailable, falling back to env/profile targets: %s",
                            exc,
                        )
                        return fallback_targets
                    return load_managed_tournaments(
                        sport_slugs=normalized_sports,
                        registry_targets=registry_targets,
                        registry_authoritative=True,
                    )

                computed_targets = ()
                target_loader = _target_loader
            else:
                computed_targets = fallback_targets
                target_loader = None
        else:
            computed_targets = tuple(targets)
            target_loader = None

        return StructurePlannerDaemon(
            queue=self.stream_queue,
            cursor_store=self.structure_cursor_store,
            targets=computed_targets,
            loop_interval_s=loop_interval_seconds,
            target_loader=target_loader,
            # Task 5 (2026-05-15): live-side governor + the existing
            # structure_sync lag guard. Same pattern as the historical
            # planners — see build_historical_planner_daemon.
            backpressure=CompositeBackpressure(
                checks=(
                    QueueBackpressure(
                        queue=self.stream_queue,
                        limits=(
                            BackpressureLimit(
                                stream=STREAM_STRUCTURE_SYNC,
                                group=GROUP_STRUCTURE_SYNC,
                                max_lag=STRUCTURE_SYNC_MAX_LAG,
                            ),
                        ),
                    ),
                    BackfillGovernor(
                        redis_backend=self.app.redis_backend,
                        thresholds=BackfillGovernorThresholds.from_env(),
                    ),
                ),
            ),
        )

    def build_tournament_registry_refresh_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        refresh_interval_seconds: float = 86_400.0,
        loop_interval_seconds: float = 300.0,
        sports_per_tick: int = 1,
        timeout_s: float = 20.0,
    ) -> TournamentRegistryRefreshDaemon:
        normalized_sports = tuple(sport_slugs or DEFAULT_SERVICE_SPORT_SLUGS)
        database = getattr(self.app, "database", None)
        connection_factory = getattr(database, "connection", None) if database is not None else None
        runtime_config = getattr(self.app, "runtime_config", None)
        if not callable(connection_factory) or runtime_config is None:
            raise RuntimeError("Tournament registry refresh daemon requires database and runtime_config.")

        repository = TournamentRegistryRepository()
        adapter = build_source_adapter(runtime_config.source_slug, runtime_config=runtime_config)
        category_job = adapter.build_category_tournaments_job(database)

        async def _has_registry_rows(sport_slug: str) -> bool:
            async with connection_factory() as connection:
                return await repository.has_rows(connection, sport_slug=sport_slug)

        async def _refresh_sport(sport_slug: str):
            return await refresh_tournament_registry_for_sport(
                category_job=category_job,
                sport_slug=sport_slug,
                timeout_s=timeout_s,
            )

        return TournamentRegistryRefreshDaemon(
            cursor_store=TournamentRegistryRefreshCursorStore(self.app.redis_backend),
            targets=normalized_sports,
            has_registry_rows=_has_registry_rows,
            refresh_sport=_refresh_sport,
            refresh_interval_seconds=refresh_interval_seconds,
            sports_per_tick=sports_per_tick,
            loop_interval_s=loop_interval_seconds,
        )

    def build_structure_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> StructureSyncWorker:
        self.ensure_structure_consumer_groups()
        return StructureSyncWorker(
            orchestrator=self.app,
            queue=self.stream_queue,
            consumer=consumer_name,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            block_ms=block_ms,
            job_audit_logger=self.job_audit_logger,
        )

    def build_resource_planner_daemon(
        self,
        *,
        loop_interval_seconds: float = 30.0,
        publish_per_tick_cap: int = 100,
        lag_threshold: int = 5000,
    ) -> ResourcePlannerDaemon:
        """Build the generic resource refresh planner.

        Stage A wired ``ManagedScopeResolver`` (env-driven pilot list).
        Stage B adds ``TeamOfActiveUTResolver`` (SQL: teams with recently-
        updated standings) so endpoints with ``scope_kind="team-of-active-ut"``
        cover all active leagues without per-team env config.

        Future stages add resolvers here (player-of-active-squad,
        season-of-active-ut) without touching the planner / worker code.
        """

        from ..endpoints import local_api_endpoints

        self.ensure_resource_refresh_consumer_groups()
        endpoints = local_api_endpoints()
        # Build the player-of-active-squad resolver once and share it with
        # the page=0 wrapper. That keeps both scope_kinds hitting the same
        # Redis cache + a single database query per refresh window.
        player_active_squad_resolver = PlayerOfActiveSquadResolver(
            database=self.app.database,
            redis_backend=self.app.redis_backend,
        )
        # Same trick for season-of-active-ut: one base resolver shared
        # between events and standings wrappers (one SQL query / cache hit
        # per tick covers both endpoint families).
        season_active_ut_base = SeasonOfActiveUTBaseResolver(
            database=self.app.database,
            redis_backend=self.app.redis_backend,
        )
        # Same trick for team-of-active-ut: one base resolver, plus a
        # first-page wrapper so /team/{id}/events/{last,next}/{page} can opt
        # in via a distinct scope_kind without duplicating the SQL.
        team_active_ut_resolver = TeamOfActiveUTResolver(
            database=self.app.database,
            redis_backend=self.app.redis_backend,
        )
        resolvers: dict[str, Any] = {
            ManagedScopeResolver.kind: ManagedScopeResolver(),
            TeamOfActiveUTResolver.kind: team_active_ut_resolver,
            TeamOfActiveUTFirstPageResolver.kind: TeamOfActiveUTFirstPageResolver(
                base=team_active_ut_resolver,
            ),
            TeamOfActiveUTSeasonResolver.kind: TeamOfActiveUTSeasonResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
            ),
            # D3: registry-driven scope. Registered side-by-side with the
            # standings-driven team-of-active-ut. Initially DORMANT — no
            # endpoint opts into "team-of-registry-ut" until the pilot
            # measurement step explicitly switches a single endpoint over.
            # Pilot scope gating is via SCHEMA_INSPECTOR_RESOURCE_REGISTRY_PILOT_UTS.
            TeamOfRegistryUTResolver.kind: TeamOfRegistryUTResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
            ),
            # D5: registry-driven (ut, season) scope. Used by /cuptrees
            # whose upstream gate is binary (404 for league tournaments,
            # 200 for cups). The 4xx is absorbed by ResourceNegativeCache
            # so league UTs only contribute one wasted request per 7 days.
            SeasonOfRegistryUTResolver.kind: SeasonOfRegistryUTResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
            ),
            # Item 1 (2026-05-19, UCL strategy C): pre-fetch ``/rounds``
            # for ANY (UT, season) where the catalog is still empty.
            # Decouples round structure ingestion from the cursor walk
            # so UCL/EURO/etc. (cat>=19, blocked by strict barrier)
            # still get Phase 4 round_slug routing once the catalog
            # lands. ``NOT EXISTS season_round`` filter makes the
            # resolver naturally drop pairs after first successful
            # fetch.
            SeasonOfRegistryUTRoundsHistoricalResolver.kind: SeasonOfRegistryUTRoundsHistoricalResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
            ),
            # Item 4 (2026-05-19): historical /cuptrees pre-fetch.
            # Same shape as the rounds-historical resolver but
            # filtered on ``season_cup_tree`` emptiness. ResourceNegativeCache
            # absorbs the 404s from league UTs after the first sweep.
            SeasonOfRegistryUTCuptreesHistoricalResolver.kind: SeasonOfRegistryUTCuptreesHistoricalResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
            ),
            # D12 parent→child fan-out for managed football leagues.
            # All three resolvers gate on the same managed env list
            # (SCHEMA_INSPECTOR_FORCE_TOP_FOOTBALL_LEAGUES). Empty env
            # = empty scope (safe default). Kept registered so an
            # operator can revert a single endpoint to the env-gated
            # variant via scope_kind without redeploying the planner.
            RoundOfManagedPairsResolver.kind: RoundOfManagedPairsResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
            ),
            PeriodOfManagedPairsResolver.kind: PeriodOfManagedPairsResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
            ),
            CustomIdOfManagedEventsResolver.kind: CustomIdOfManagedEventsResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
            ),
            # D13.2 globalised parent→child fan-out for ALL active
            # football leagues (registry-driven, no env gating). These
            # are the resolvers active endpoints actually point at as of
            # D13.2; the D12 managed variants above are kept registered
            # only as an emergency-revert lever.
            RoundOfRegistryFootballResolver.kind: RoundOfRegistryFootballResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
            ),
            PeriodOfRegistryFootballResolver.kind: PeriodOfRegistryFootballResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
                totw_404_store=self.totw_404_store,
            ),
            CustomIdOfRegistryEventsResolver.kind: CustomIdOfRegistryEventsResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
            ),
            PlayerOfActiveSquadResolver.kind: player_active_squad_resolver,
            PlayerOfActiveSquadFirstPageResolver.kind: PlayerOfActiveSquadFirstPageResolver(
                base=player_active_squad_resolver,
            ),
            PlayerOfNationalTeamHistoryResolver.kind: PlayerOfNationalTeamHistoryResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
            ),
            EventOfFinishedBaseballResolver.kind: EventOfFinishedBaseballResolver(
                database=self.app.database,
                redis_backend=self.app.redis_backend,
            ),
            SeasonOfActiveUTEventsResolver.kind: SeasonOfActiveUTEventsResolver(
                base=season_active_ut_base,
            ),
            SeasonOfActiveUTStandingsResolver.kind: SeasonOfActiveUTStandingsResolver(
                base=season_active_ut_base,
            ),
        }
        return ResourcePlannerDaemon(
            queue=self.stream_queue,
            cursor_store=self.resource_cursor_store,
            endpoints=endpoints,
            resolvers=resolvers,
            stream=STREAM_RESOURCE_REFRESH,
            group=GROUP_RESOURCE_REFRESH,
            tick_interval_seconds=loop_interval_seconds,
            publish_per_tick_cap=publish_per_tick_cap,
            lag_threshold=lag_threshold,
            negative_cache=self.resource_negative_cache,
            empty_data_store=self.empty_data_store,
        )

    def build_resource_refresh_worker(
        self,
        *,
        consumer_name: str,
        block_ms: int = 5_000,
    ) -> ResourceRefreshWorker:
        """Build the generic resource refresh worker.

        Wires a per-message FetchExecutor that borrows a fresh asyncpg
        connection from the pool, runs one fetch + snapshot insert, and
        releases the connection back. This mirrors how ``team_detail_cli``
        operates and keeps the worker independent of long-lived DB sessions.
        """

        from ..endpoints import local_api_endpoints

        self.ensure_resource_refresh_consumer_groups()
        executor = _PerConnectionFetchExecutor(
            transport=self.app.transport,
            raw_repository=self.app.raw_repository,
            database=self.app.database,
            freshness_store=self.app.freshness_store,
        )
        # snapshot_reader is async (snapshot_id) -> dict|None. Used by the
        # auto-pagination chain to inspect the just-inserted payload's
        # hasNextPage flag without re-doing the upstream fetch.
        async def _snapshot_reader(snapshot_id: int):
            async with self.app.database.connection() as connection:
                row = await connection.fetchrow(
                    "SELECT payload FROM api_payload_snapshot WHERE id = $1",
                    int(snapshot_id),
                )
            if row is None:
                return None
            payload = row["payload"]
            if isinstance(payload, str):
                import orjson
                try:
                    payload = orjson.loads(payload)
                except Exception:
                    return None
            return payload

        # D13.1: bridge fetch → normalize. The publisher pushes a
        # ``JOB_NORMALIZE_SNAPSHOT`` envelope onto ``stream:etl:normalize``
        # for every successfully inserted snapshot so the parser registry
        # can populate downstream durable tables (season_round, period,
        # ...). Best-effort: failures are logged inside the worker and
        # do not fail the underlying fetch job.
        from ..jobs.envelope import JobEnvelope
        from ..jobs.types import JOB_NORMALIZE_SNAPSHOT
        from ..workers._stream_jobs import encode_stream_job

        # Make sure the consumer group exists so freshly published
        # envelopes are not dropped on the floor before the first
        # ``worker-normalize`` instance starts.
        self.ensure_normalize_consumer_groups()

        def _normalize_publisher(
            *,
            snapshot_id: int,
            endpoint_pattern: str,
            sport_slug: str = "",
            trace_id: str | None = None,
            parent_job_id: str | None = None,
        ) -> None:
            envelope = JobEnvelope.create(
                job_type=JOB_NORMALIZE_SNAPSHOT,
                sport_slug=sport_slug,
                entity_type=None,
                entity_id=None,
                scope="normalize",
                params={
                    "snapshot_id": int(snapshot_id),
                    "endpoint_pattern": str(endpoint_pattern or ""),
                },
                priority=0,
                trace_id=trace_id,
                capability_hint="normalize",
                parent_job_id=parent_job_id,
            )
            self.stream_queue.publish(STREAM_NORMALIZE, encode_stream_job(envelope))

        return ResourceRefreshWorker(
            fetch_executor=executor,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            queue=self.stream_queue,
            consumer=consumer_name,
            block_ms=block_ms,
            endpoints=local_api_endpoints(),
            job_audit_logger=self.job_audit_logger,
            negative_cache=self.resource_negative_cache,
            pagination_done=self.pagination_done_store,
            empty_data_store=self.empty_data_store,
            snapshot_reader=_snapshot_reader,
            normalize_publisher=_normalize_publisher,
            totw_404_store=self.totw_404_store,
        )

    def build_normalize_worker(
        self,
        *,
        consumer_name: str,
        block_ms: int = 5_000,
    ) -> NormalizeStreamWorker:
        """Build the stream consumer that turns raw snapshots into normalized rows."""

        from ..parsers.registry import ParserRegistry

        self.ensure_normalize_consumer_groups()
        return NormalizeStreamWorker(
            database=self.app.database,
            raw_repository=self.app.raw_repository,
            normalize_repository=self.app.normalize_repository,
            parser_registry=ParserRegistry.default(),
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            queue=self.stream_queue,
            consumer=consumer_name,
            block_ms=block_ms,
            job_audit_logger=self.job_audit_logger,
        )

    def build_hydrate_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> HydrateWorker:
        self.ensure_consumer_groups()
        # P5b: pass circuit breaker through only when the rollout flag is
        # on. Default (flag unset) → store=None → legacy fast path,
        # branch-free at runtime. Matches LIVE_TIER_1_QUARANTINE_ENABLED
        # gating pattern.
        circuit_breaker = (
            self.hydrate_circuit_breaker
            if _service_app_env_flag_enabled("HYDRATE_EVENT_CIRCUIT_BREAKER_ENABLED")
            else None
        )
        return HydrateWorker(
            orchestrator=self.app,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            queue=self.stream_queue,
            consumer=consumer_name,
            block_ms=block_ms,
            job_audit_logger=self.job_audit_logger,
            circuit_breaker=circuit_breaker,
            # Phase 2.1 (2026-05-20 perf audit): cross-consumer in-flight
            # lock. Two sofascore-hydrate@N workers cannot now process
            # the same event_id concurrently.
            hydrate_inflight_store=self.hydrate_inflight_store,
        )

    def build_historical_hydrate_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> HydrateWorker:
        self.ensure_historical_consumer_groups()
        return HydrateWorker(
            orchestrator=self.app,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            queue=self.stream_queue,
            consumer=consumer_name,
            group="cg:historical_hydrate",
            stream=STREAM_HISTORICAL_HYDRATE,
            block_ms=block_ms,
            job_audit_logger=self.job_audit_logger,
        )

    def build_discovery_worker(
        self,
        *,
        consumer_name: str,
        block_ms: int = 5_000,
        timeout_s: float = 20.0,
    ) -> DiscoveryWorker:
        self.ensure_consumer_groups()
        return DiscoveryWorker(
            orchestrator=self.app,
            queue=self.stream_queue,
            consumer=consumer_name,
            block_ms=block_ms,
            timeout_s=timeout_s,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            freshness_policy=self.freshness_policy,
            hydrate_backpressure=QueueBackpressure(
                queue=self.stream_queue,
                limits=(
                    BackpressureLimit(stream=STREAM_HYDRATE, group=GROUP_HYDRATE, max_lag=HYDRATE_MAX_LAG),
                ),
            ),
            job_audit_logger=self.job_audit_logger,
            tier_override_registry=getattr(self.app, "tier_override_registry", None),
        )

    def build_historical_discovery_worker(
        self,
        *,
        consumer_name: str,
        block_ms: int = 5_000,
        timeout_s: float = 20.0,
    ) -> DiscoveryWorker:
        self.ensure_historical_consumer_groups()
        return DiscoveryWorker(
            orchestrator=self.app,
            queue=self.stream_queue,
            consumer=consumer_name,
            group="cg:historical_discovery",
            stream=STREAM_HISTORICAL_DISCOVERY,
            hydrate_stream=STREAM_HISTORICAL_HYDRATE,
            block_ms=block_ms,
            timeout_s=timeout_s,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            freshness_policy=self.freshness_policy,
            hydrate_backpressure=QueueBackpressure(
                queue=self.stream_queue,
                limits=(
                    BackpressureLimit(
                        stream=STREAM_HISTORICAL_HYDRATE,
                        group=GROUP_HISTORICAL_HYDRATE,
                        max_lag=HISTORICAL_HYDRATE_MAX_LAG,
                    ),
                ),
            ),
            defer_on_backpressure=True,
            admission_delay_ms=30_000,
            job_audit_logger=self.job_audit_logger,
            tier_override_registry=getattr(self.app, "tier_override_registry", None),
        )

    def build_live_discovery_worker(
        self,
        *,
        consumer_name: str,
        block_ms: int = 5_000,
        timeout_s: float = 20.0,
    ) -> DiscoveryWorker:
        self.ensure_consumer_groups()
        return DiscoveryWorker(
            orchestrator=self.app,
            queue=self.stream_queue,
            consumer=consumer_name,
            group="cg:live_discovery",
            stream=STREAM_LIVE_DISCOVERY,
            hydrate_stream=STREAM_HYDRATE,
            block_ms=block_ms,
            timeout_s=timeout_s,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            freshness_policy=self.freshness_policy,
            hydrate_backpressure=QueueBackpressure(
                queue=self.stream_queue,
                limits=(
                    BackpressureLimit(stream=STREAM_LIVE_TIER_1, group=GROUP_LIVE_TIER_1, max_lag=LIVE_TIER_1_MAX_LAG),
                    BackpressureLimit(stream=STREAM_LIVE_TIER_2, group=GROUP_LIVE_TIER_2, max_lag=LIVE_TIER_2_MAX_LAG),
                    BackpressureLimit(stream=STREAM_LIVE_TIER_3, group=GROUP_LIVE_TIER_3, max_lag=LIVE_TIER_3_MAX_LAG),
                ),
            ),
            job_audit_logger=self.job_audit_logger,
            tier_override_registry=getattr(self.app, "tier_override_registry", None),
        )

    def build_historical_tournament_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> HistoricalTournamentWorker:
        self.ensure_historical_consumer_groups()
        return HistoricalTournamentWorker(
            orchestrator=self.app,
            queue=self.stream_queue,
            consumer=consumer_name,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            block_ms=block_ms,
            job_audit_logger=self.job_audit_logger,
        )

    def build_historical_enrichment_worker(
        self,
        *,
        consumer_name: str,
        block_ms: int = 5_000,
    ) -> HistoricalEnrichmentWorker:
        self.ensure_historical_consumer_groups()
        return HistoricalEnrichmentWorker(
            orchestrator=self.app,
            queue=self.stream_queue,
            consumer=consumer_name,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            block_ms=block_ms,
            job_audit_logger=self.job_audit_logger,
        )

    def build_live_worker(self, *, lane: str, consumer_name: str, block_ms: int = 5_000) -> LiveWorkerService:
        self.ensure_consumer_groups()
        # P0(c) gate: pass quarantine store through ONLY when the rollout
        # flag is on. Keeping the store=None on the disabled path means the
        # worker's ``handle()`` short-circuits the entire quarantine branch
        # (cheaper than a per-call env lookup), and no quarantine counters
        # are written to Redis under default prod config. Flag lookup
        # happens once at service construction; toggling the flag requires
        # a worker restart, which is the same pattern as
        # ``LIVE_TIER_1_ROOT_ONLY``.
        quarantine_store = (
            self.live_tier_1_quarantine_store
            if _service_app_env_flag_enabled("LIVE_TIER_1_QUARANTINE_ENABLED")
            else None
        )
        return LiveWorkerService(
            orchestrator=self.app,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            queue=self.stream_queue,
            lane=lane,
            consumer=consumer_name,
            block_ms=block_ms,
            job_audit_logger=self.job_audit_logger,
            in_flight_store=self.live_event_inflight_store,
            details_throttle=self.live_details_throttle,
            root_in_flight_store=self.live_event_root_inflight_store,
            edges_throttle=self.live_edges_throttle,
            quarantine_store=quarantine_store,
        )

    def build_live_details_worker(self, *, consumer_name: str, block_ms: int = 5_000):
        """Construct the LiveDetailsWorkerService that consumes
        ``stream:etl:live_details``. Imported lazily so service_app.py
        does not have a hard dependency on the new module path during
        import-time class registration."""
        from ..workers.live_details_worker_service import LiveDetailsWorkerService

        self.ensure_consumer_groups()
        return LiveDetailsWorkerService(
            orchestrator=self.app,
            queue=self.stream_queue,
            consumer=consumer_name,
            block_ms=block_ms,
            job_audit_logger=self.job_audit_logger,
            details_in_flight_store=self.live_event_details_inflight_store,
        )

    def build_housekeeping_loop(self) -> HousekeepingLoop | None:
        database = getattr(self.app, "database", None)
        if database is None:
            return None
        connection_factory = getattr(database, "connection", None)
        if not callable(connection_factory):
            logger.warning("housekeeping loop disabled: app.database.connection is unavailable")
            return None
        return HousekeepingLoop(
            config=HousekeepingConfig.from_env(),
            connection_factory=connection_factory,
            live_state_store=self.live_state_store,
            redis_backend=self.app.redis_backend,
        )

    def build_live_rescue_loop(self):
        """A2 Phase 0 (2026-05-16): periodic force-hydrate for events
        that upstream still reports as live but our pipeline has not
        polled in `stale_minutes`. Returns ``None`` if Redis or the DB
        connection is unavailable, so the daemon entrypoint can warn
        and exit instead of crashing the systemd unit.
        """
        from .live_rescue import LiveRescueConfig, LiveRescueLoop

        database = getattr(self.app, "database", None)
        if database is None:
            logger.warning("live-rescue loop disabled: app.database is None")
            return None
        connection_factory = getattr(database, "connection", None)
        if not callable(connection_factory):
            logger.warning("live-rescue loop disabled: app.database.connection is unavailable")
            return None
        if self.app.redis_backend is None:
            logger.warning("live-rescue loop disabled: redis_backend is None")
            return None
        if self.live_state_store is None:
            logger.warning("live-rescue loop disabled: live_state_store is None")
            return None
        return LiveRescueLoop(
            config=LiveRescueConfig.from_env(),
            connection_factory=connection_factory,
            redis_backend=self.app.redis_backend,
            live_state_store=self.live_state_store,
            stream_queue=self.stream_queue,
            tier_override_registry=getattr(self.app, "tier_override_registry", None),
        )

    def build_maintenance_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> MaintenanceWorker:
        self.ensure_consumer_groups()
        return MaintenanceWorker(
            handler=self._handle_maintenance,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            queue=self.stream_queue,
            consumer=consumer_name,
            block_ms=block_ms,
            job_audit_logger=self.job_audit_logger,
            housekeeping_loop=self.build_housekeeping_loop(),
        )

    def build_historical_maintenance_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> MaintenanceWorker:
        self.ensure_historical_consumer_groups()
        return MaintenanceWorker(
            handler=self._handle_maintenance,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            queue=self.stream_queue,
            consumer=consumer_name,
            group=GROUP_HISTORICAL_MAINTENANCE,
            stream=STREAM_HISTORICAL_MAINTENANCE,
            block_ms=block_ms,
            reclaim_targets=(
                ReclaimTarget(stream=STREAM_HISTORICAL_DISCOVERY, group=GROUP_HISTORICAL_DISCOVERY),
                ReclaimTarget(stream=STREAM_HISTORICAL_TOURNAMENT, group=GROUP_HISTORICAL_TOURNAMENT),
                ReclaimTarget(stream=STREAM_HISTORICAL_ENRICHMENT, group=GROUP_HISTORICAL_ENRICHMENT),
                ReclaimTarget(stream=STREAM_HISTORICAL_HYDRATE, group=GROUP_HISTORICAL_HYDRATE),
            ),
            job_audit_logger=self.job_audit_logger,
        )

    async def run_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        scheduled_interval_seconds: float = 300.0,
        loop_interval_seconds: float = 5.0,
    ) -> None:
        self.ensure_consumer_groups()
        await self.ensure_tier_override_registry_loaded()
        await self._recover_live_state()
        daemon = self.build_planner_daemon(
            sport_slugs=sport_slugs,
            scheduled_interval_seconds=scheduled_interval_seconds,
            loop_interval_seconds=loop_interval_seconds,
        )
        await daemon.run_forever()

    async def run_live_discovery_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        loop_interval_seconds: float = 5.0,
    ) -> None:
        self.ensure_consumer_groups()
        await self.ensure_tier_override_registry_loaded()
        daemon = self.build_live_discovery_planner_daemon(
            sport_slugs=sport_slugs,
            loop_interval_seconds=loop_interval_seconds,
        )
        await daemon.run_forever()

    async def run_historical_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        dates_per_tick: int = 1,
        loop_interval_seconds: float = 5.0,
    ) -> None:
        self.ensure_historical_consumer_groups()
        daemon = self.build_historical_planner_daemon(
            sport_slugs=sport_slugs,
            date_from=date_from,
            date_to=date_to,
            dates_per_tick=dates_per_tick,
            loop_interval_seconds=loop_interval_seconds,
        )
        await daemon.run_forever()

    async def run_historical_tournament_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        tournaments_per_tick: int = 10,
        loop_interval_seconds: float = 10.0,
    ) -> None:
        self.ensure_historical_consumer_groups()
        daemon = self.build_historical_tournament_planner_daemon(
            sport_slugs=sport_slugs,
            tournaments_per_tick=tournaments_per_tick,
            loop_interval_seconds=loop_interval_seconds,
        )
        # SIGHUP → reload priorities from disk without restart.
        # Operator workflow:  `sudo systemctl kill -s HUP <unit>`.
        # On Windows asyncio has no add_signal_handler — guarded so
        # tests + dev shell never crash on import / startup.
        import signal as _signal
        try:
            loop = asyncio.get_running_loop()
            from pathlib import Path
            cfg_path = Path(
                os.environ.get("SOFASCORE_BACKFILL_PRIORITIES_PATH")
                or _BACKFILL_PRIORITIES_DEFAULT_PATH
            )
            loop.add_signal_handler(
                _signal.SIGHUP,
                lambda: daemon.reload_priority_config(cfg_path),
            )
            logger.info(
                "historical-tournament-planner: SIGHUP → reload from %s",
                cfg_path,
            )
        except (AttributeError, NotImplementedError, RuntimeError) as exc:
            logger.info(
                "SIGHUP signal handler not installed (%s) — reload only via restart",
                exc,
            )
        await daemon.run_forever()

    async def run_discovery_worker(
        self,
        *,
        consumer_name: str,
        block_ms: int = 5_000,
        timeout_s: float = 20.0,
    ) -> None:
        await self.ensure_tier_override_registry_loaded()
        worker = self.build_discovery_worker(
            consumer_name=consumer_name,
            block_ms=block_ms,
            timeout_s=timeout_s,
        )
        await worker.run_forever()

    async def run_historical_discovery_worker(
        self,
        *,
        consumer_name: str,
        block_ms: int = 5_000,
        timeout_s: float = 20.0,
    ) -> None:
        await self.ensure_tier_override_registry_loaded()
        worker = self.build_historical_discovery_worker(
            consumer_name=consumer_name,
            block_ms=block_ms,
            timeout_s=timeout_s,
        )
        await worker.run_forever()

    async def run_live_discovery_worker(
        self,
        *,
        consumer_name: str,
        block_ms: int = 5_000,
        timeout_s: float = 20.0,
    ) -> None:
        await self.ensure_tier_override_registry_loaded()
        worker = self.build_live_discovery_worker(
            consumer_name=consumer_name,
            block_ms=block_ms,
            timeout_s=timeout_s,
        )
        await worker.run_forever()

    async def run_historical_tournament_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> None:
        worker = self.build_historical_tournament_worker(consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()

    async def run_structure_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        loop_interval_seconds: float = 30.0,
        targets: tuple[StructurePlanningTarget, ...] | None = None,
    ) -> None:
        self.ensure_structure_consumer_groups()
        daemon = self.build_structure_planner_daemon(
            sport_slugs=sport_slugs,
            loop_interval_seconds=loop_interval_seconds,
            targets=targets,
        )
        await daemon.run_forever()

    async def run_tournament_registry_refresh_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        refresh_interval_seconds: float = 86_400.0,
        loop_interval_seconds: float = 300.0,
        sports_per_tick: int = 1,
        timeout_s: float = 20.0,
    ) -> None:
        daemon = self.build_tournament_registry_refresh_daemon(
            sport_slugs=sport_slugs,
            refresh_interval_seconds=refresh_interval_seconds,
            loop_interval_seconds=loop_interval_seconds,
            sports_per_tick=sports_per_tick,
            timeout_s=timeout_s,
        )
        await daemon.run_forever()

    async def run_structure_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> None:
        worker = self.build_structure_worker(consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()

    async def run_resource_planner_daemon(
        self,
        *,
        loop_interval_seconds: float = 30.0,
        publish_per_tick_cap: int = 100,
        lag_threshold: int = 5000,
    ) -> None:
        self.ensure_resource_refresh_consumer_groups()
        daemon = self.build_resource_planner_daemon(
            loop_interval_seconds=loop_interval_seconds,
            publish_per_tick_cap=publish_per_tick_cap,
            lag_threshold=lag_threshold,
        )
        await daemon.run_forever()

    async def run_resource_refresh_worker(
        self,
        *,
        consumer_name: str,
        block_ms: int = 5_000,
    ) -> None:
        worker = self.build_resource_refresh_worker(
            consumer_name=consumer_name,
            block_ms=block_ms,
        )
        await worker.run_forever()

    async def run_normalize_worker(
        self,
        *,
        consumer_name: str,
        block_ms: int = 5_000,
    ) -> None:
        worker = self.build_normalize_worker(
            consumer_name=consumer_name,
            block_ms=block_ms,
        )
        await worker.run_forever()

    async def run_historical_enrichment_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> None:
        worker = self.build_historical_enrichment_worker(consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()

    async def run_hydrate_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> None:
        await self.ensure_tier_override_registry_loaded()
        worker = self.build_hydrate_worker(consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()

    async def run_historical_hydrate_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> None:
        worker = self.build_historical_hydrate_worker(consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()

    async def run_live_worker(self, *, lane: str, consumer_name: str, block_ms: int = 5_000) -> None:
        await self.ensure_tier_override_registry_loaded()
        worker = self.build_live_worker(lane=lane, consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()

    async def run_live_details_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> None:
        """Run the dedicated details consumer (P0(a) split-details)."""
        worker = self.build_live_details_worker(consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()

    async def run_maintenance_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> None:
        worker = self.build_maintenance_worker(consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()

    async def run_live_rescue_daemon(self) -> None:
        """A2 Phase 0: forever-run the live-rescue loop. Loads the
        tier-override registry on boot (re-used by the resolver inside
        the rescuer) and starts the periodic scan. If the loop is
        disabled by config (``SOFASCORE_LIVE_RESCUE_ENABLED=false``),
        ``run_forever`` logs and returns immediately."""
        self.ensure_consumer_groups()
        await self.ensure_tier_override_registry_loaded()
        loop = self.build_live_rescue_loop()
        if loop is None:
            logger.warning("live-rescue daemon: build failed; exiting")
            return
        await loop.run_forever()

    async def run_historical_maintenance_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> None:
        worker = self.build_historical_maintenance_worker(consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()

    async def _handle_maintenance(self, entry: StreamEntry) -> str:
        job = decode_stream_payload(entry.values, fallback_job_id=entry.message_id)
        if job.job_type == JOB_REPLAY_FAILED_JOB:
            self.delayed_envelope_store.save_payload(
                dict(entry.values),
                fallback_job_id=entry.message_id,
            )
            self.delayed_scheduler.schedule(
                job.job_id,
                run_at_epoch_ms=int(job.params.get("run_at_epoch_ms") or 0),
            )
            return "scheduled"
        logger.warning("_handle_maintenance: unrecognised job_type=%r, skipping", job.job_type)
        return "skipped"

    @staticmethod
    def _date_factory(now_ms: int) -> str:
        from datetime import datetime, timezone

        return datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).date().isoformat()

    @staticmethod
    def _today_factory():
        from datetime import datetime, timezone

        return datetime.now(tz=timezone.utc).date()

    @staticmethod
    def _datetime_from_epoch_ms(now_ms: int):
        from datetime import datetime, timezone

        return datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)

    async def _recover_live_state(self) -> None:
        recover = getattr(self.app, "recover_live_state", None)
        if callable(recover):
            await recover()


class _PerConnectionFetchExecutor:
    """Adapter exposing ``execute(task)`` semantics over a per-call asyncpg
    connection borrowed from the pool.

    The base ``FetchExecutor`` requires a live ``sql_executor`` at
    construction time. Workers that consume Redis Streams cannot hold a
    single long-lived connection (it would pin DB resources), so we build
    a fresh executor for every job using a short-lived connection from
    ``database.connection()`` and release it as soon as the snapshot is
    written.
    """

    def __init__(
        self,
        *,
        transport,
        raw_repository,
        database,
        freshness_store=None,
    ) -> None:
        self.transport = transport
        self.raw_repository = raw_repository
        self.database = database
        self.freshness_store = freshness_store

    async def execute(self, task):
        from ..fetch_executor import FetchExecutor

        async with self.database.connection() as connection:
            # P0.2 — defence-in-depth lookup for the HEAD-probe: if HEAD
            # says 4xx we still issue GET when a real 200 exists for
            # this target within the last 30 days. Implemented as an
            # inline async closure over the per-call connection.
            async def _recent_200(t):
                window_seconds = 30 * 86400
                row = await connection.fetchrow(
                    """
                    SELECT 1 AS hit
                    FROM api_payload_snapshot
                    WHERE endpoint_pattern = $1
                      AND http_status = 200
                      AND coalesce(is_soft_error_payload, false) = false
                      AND fetched_at >= now() - make_interval(secs => $2::bigint)
                      AND (
                            ($3::bigint IS NULL OR context_event_id = $3::bigint)
                        AND ($4::bigint IS NULL OR context_unique_tournament_id = $4::bigint)
                        AND ($5::bigint IS NULL OR context_season_id = $5::bigint)
                      )
                    LIMIT 1
                    """,
                    str(t.endpoint_pattern or ""),
                    int(window_seconds),
                    int(t.context_event_id) if t.context_event_id is not None else None,
                    int(t.context_unique_tournament_id) if t.context_unique_tournament_id is not None else None,
                    int(t.context_season_id) if t.context_season_id is not None else None,
                )
                return row is not None

            executor = FetchExecutor(
                transport=self.transport,
                raw_repository=self.raw_repository,
                sql_executor=connection,
                freshness_store=self.freshness_store,
                recent_200_lookup=_recent_200,
            )
            return await executor.execute(task)


def _service_app_env_flag_enabled(name: str) -> bool:
    """Truthy-flag parser local to service_app.

    Kept in this module rather than imported from workers/live_worker_service
    to avoid the awkward shape of importing a private helper across module
    boundaries; ``LIVE_TIER_1_QUARANTINE_ENABLED`` is the only flag this
    bootstrap layer needs to read, and the parsing rule mirrors the worker
    layer (1/true/yes/on case-insensitive). Centralising flag parsing
    further can wait until a third call site appears.
    """
    raw = os.environ.get(name)
    if raw in (None, ""):
        return False
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}
