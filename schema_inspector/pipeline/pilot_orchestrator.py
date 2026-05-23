"""Pilot orchestrator wiring planner, fetch, raw snapshots, and normalization."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

from ..endpoints import (
    EVENT_BASEBALL_INNINGS_ENDPOINT,
    EVENT_BASEBALL_PITCHES_ENDPOINT,
    EVENT_BEST_PLAYERS_SUMMARY_ENDPOINT,
    EVENT_COMMENTS_ENDPOINT,
    EVENT_DETAIL_ENDPOINT,
    EVENT_H2H_ENDPOINT,
    EVENT_H2H_EVENTS_ENDPOINT,
    EVENT_MANAGERS_ENDPOINT,
    EVENT_GRAPH_ENDPOINT,
    EVENT_PLAYER_HEATMAP_ENDPOINT,
    EVENT_PLAYER_SHOTMAP_ENDPOINT,
    EVENT_GOALKEEPER_SHOTMAP_ENDPOINT,
    EVENT_INCIDENTS_ENDPOINT,
    EVENT_LINEUPS_ENDPOINT,
    EVENT_PLAYER_RATING_BREAKDOWN_ENDPOINT,
    EVENT_PLAYER_STATISTICS_ENDPOINT,
    EVENT_PREGAME_FORM_ENDPOINT,
    EVENT_SHOTMAP_ENDPOINT,
    EVENT_STATISTICS_ENDPOINT,
    MANAGER_ENDPOINT,
    PLAYER_ENDPOINT,
    SofascoreEndpoint,
    TEAM_ENDPOINT,
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT,
    unique_tournament_top_players_endpoint,
    unique_tournament_top_players_per_game_endpoint,
    unique_tournament_top_teams_endpoint,
)
from ..detail_resource_policy import build_event_detail_request_specs
from ..endpoint_ttl_policy import resolve_endpoint_ttl as _resolve_endpoint_ttl
from ..event_endpoint_static_denylist import is_static_dead_event_endpoint
from ..event_endpoint_negative_cache import normalize_event_status_phase
from ..fetch_classifier import (
    CLASSIFICATION_ACCESS_DENIED,
    CLASSIFICATION_CHALLENGE_DETECTED,
    CLASSIFICATION_NETWORK_ERROR,
    CLASSIFICATION_RATE_LIMITED,
)
from ..fetch_models import FetchOutcomeEnvelope, FetchTask
from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_FINALIZE_EVENT, JOB_HYDRATE_EVENT_ROOT, JOB_TRACK_LIVE_EVENT
from ..planner.live import TERMINAL_STATUS_TYPES
from ..match_center_policy import football_edge_allowed, football_special_allowed
from ..services.league_capabilities_registry import (
    is_league_capabilities_enabled,
    resolve_capability_verdict,
)
from ..parsers.sports import resolve_sport_adapter
from ..services.retry_policy import RetryableJobError
from ..storage.capability_repository import CapabilityObservationRecord, CapabilityRollupRecord
from ..storage.live_state_repository import EventLiveStateHistoryRecord, EventTerminalStateRecord
from ..workers.live_worker import LiveWorker

MISSING_ROOT_TERMINAL_STATUS = "not_found"

# Fix A (live freshness): a transient failure (network timeout, SSL,
# 403 access_denied, 429 rate_limited, bot challenge) on the ROOT
# `/api/v1/event/{event_id}` request must not be silently swallowed.
# Without raising, the orchestrator returns an empty PilotRunReport,
# the worker marks the job ``succeeded``, and the only retry path is
# the planner's next live cycle (~30-90s away). For live events that
# means the UI shows stale data while Sofascore advances. Raising
# RetryableJobError lets WorkerRuntime route the failure through
# ``retry_handler`` -> ``delayed_scheduler`` for an immediate,
# visible retry (and proper ``retry_scheduled`` audit row).
#
# Scope is the ROOT fetch only — fan-out detail endpoints (statistics,
# lineups, incidents, graph, ...) keep their current best-effort
# behaviour: a transient failure on one detail edge does not fail
# the whole event. The 404 ``not_found`` retire path is unchanged
# because not_found has retry_recommended=False and is excluded
# from this set.
_ROOT_FETCH_RETRYABLE_CLASSIFICATIONS = frozenset(
    {
        CLASSIFICATION_NETWORK_ERROR,
        CLASSIFICATION_ACCESS_DENIED,
        CLASSIFICATION_RATE_LIMITED,
        CLASSIFICATION_CHALLENGE_DETECTED,
    }
)
MISSING_ROOT_RETIRE_THRESHOLD = 3
MISSING_ROOT_RETIRE_LOOKBACK = 20

# Task 2 Phase A (2026-05-20): final-sync lifecycle delay. After an
# event finishes, the system holds it in the hot path for this many
# seconds so late-arrival statistics corrections can still land; then
# the FinalSyncPlannerDaemon publishes one final hydrate job
# (scope="final_sync") and the orchestrator stamps locked_at, freezing
# the event from further processing.
FINAL_SYNC_DELAY_SECONDS = int(
    os.environ.get("SOFASCORE_FINAL_SYNC_DELAY_SECONDS") or 7200
)
FINAL_SYNC_SCOPE = "final_sync"
PLAYER_PROFILE_FRESHNESS_TTL_SECONDS = 86_400
TEAM_PROFILE_FRESHNESS_TTL_SECONDS = 86_400
MANAGER_PROFILE_FRESHNESS_TTL_SECONDS = 604_800
EVENT_STATIC_DETAIL_FRESHNESS_TTL_SECONDS = 86_400
EVENT_PLAYER_DETAIL_FRESHNESS_TTL_SECONDS = 300

_EVENT_STATIC_DETAIL_FRESHNESS_PATTERNS = frozenset(
    {
        EVENT_MANAGERS_ENDPOINT.pattern,
        EVENT_H2H_ENDPOINT.pattern,
        EVENT_H2H_EVENTS_ENDPOINT.pattern,
    }
)
_EVENT_PLAYER_DETAIL_FRESHNESS_PATTERNS = frozenset(
    {
        EVENT_PLAYER_STATISTICS_ENDPOINT.pattern,
        EVENT_PLAYER_RATING_BREAKDOWN_ENDPOINT.pattern,
        EVENT_PLAYER_HEATMAP_ENDPOINT.pattern,
        EVENT_PLAYER_SHOTMAP_ENDPOINT.pattern,
        EVENT_GOALKEEPER_SHOTMAP_ENDPOINT.pattern,
    }
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PilotRunReport:
    sport_slug: str
    event_id: int
    fetch_outcomes: tuple[FetchOutcomeEnvelope, ...]
    parse_results: tuple[object, ...]
    live_lane: str | None = None
    live_stream: str | None = None
    finalized: bool = False
    # P0(a) split-details rollout. When ``LIVE_SPLIT_DETAILS_FANOUT=1`` and
    # the run was a live_delta refresh, the orchestrator returns *after*
    # the ROOT + edges critical phase and signals the worker to enqueue a
    # standalone ``refresh_live_event_details`` job onto
    # ``stream:etl:live_details`` for the dedicated details worker pool to
    # consume. ``details_context`` carries the runtime values needed by
    # ``build_event_detail_request_specs`` so the details worker can
    # rebuild the same fanout without re-parsing the root payload.
    # ``details_pending=False`` and ``details_context=None`` for the
    # legacy in-line behaviour.
    details_pending: bool = False
    details_context: dict[str, Any] | None = None
    # P0(b) tier_1 root-only rollout (``LIVE_TIER_1_ROOT_ONLY``). When the
    # worker requested ``hydration_mode="root_only"``, the orchestrator
    # returns *immediately after* ROOT (and inline finalisation if the
    # status is terminal) without running edges/details fan-out. The
    # worker layer reads ``edges_pending`` and enqueues a follow-up
    # ``refresh_live_event`` job (full hydration) onto
    # ``stream:etl:live_warm`` so edges/details still happen on the slow
    # lane. ``edges_pending=False`` for legacy modes (``full``, ``core``,
    # ``live_delta``) and for terminal ``root_only`` runs that have
    # already finalised.
    edges_pending: bool = False


@dataclass(frozen=True)
class _EventEndpointFetchSpec:
    endpoint: Any
    sport_slug: str
    path_params: dict[str, Any]
    context_entity_type: str | None
    context_entity_id: int | None
    context_event_id: int
    fetch_reason: str
    status_phase: str


@dataclass
class CapabilityRollupAccumulator:
    sport_slug: str
    endpoint_pattern: str
    success_count: int = 0
    not_found_count: int = 0
    soft_error_count: int = 0
    empty_count: int = 0
    last_success_at: str | None = None
    last_404_at: str | None = None
    last_soft_error_at: str | None = None

    def observe(self, outcome: FetchOutcomeEnvelope) -> CapabilityRollupRecord:
        if outcome.classification in {"success_json", "success_empty_json"}:
            self.success_count += 1
            self.last_success_at = outcome.fetched_at
            if outcome.is_empty_payload:
                self.empty_count += 1
        elif outcome.classification == "soft_error_json":
            self.soft_error_count += 1
            self.last_soft_error_at = outcome.fetched_at
        elif outcome.classification == "not_found":
            self.not_found_count += 1
            self.last_404_at = outcome.fetched_at

        total = self.success_count + self.not_found_count + self.soft_error_count
        if self.success_count > 0 and self.not_found_count == 0 and self.soft_error_count == 0:
            support_level = "supported"
        elif self.success_count == 0 and self.not_found_count > 0 and self.soft_error_count == 0:
            support_level = "unsupported"
        elif self.success_count > 0 or self.soft_error_count > 0:
            support_level = "conditionally_supported"
        else:
            support_level = "unknown"

        confidence = min(1.0, max(total, 1) / 3.0)
        return CapabilityRollupRecord(
            sport_slug=self.sport_slug,
            endpoint_pattern=self.endpoint_pattern,
            support_level=support_level,
            confidence=confidence,
            last_success_at=self.last_success_at,
            last_404_at=self.last_404_at,
            last_soft_error_at=self.last_soft_error_at,
            success_count=self.success_count,
            not_found_count=self.not_found_count,
            soft_error_count=self.soft_error_count,
            empty_count=self.empty_count,
            notes=None,
        )


@dataclass(frozen=True)
class DeferredCapabilityRecord:
    observation: CapabilityObservationRecord
    rollup: CapabilityRollupRecord


@dataclass(frozen=True)
class _SyntheticSpecialJob:
    special_kind: str
    player_id: int

    @property
    def job_type(self) -> str:
        return "hydrate_special_route"

    @property
    def params(self) -> dict[str, Any]:
        return {
            "special_kind": self.special_kind,
            "player_id": int(self.player_id),
        }


class PilotOrchestrator:
    def __init__(
        self,
        *,
        fetch_executor,
        snapshot_store,
        normalize_worker,
        planner,
        capability_repository,
        sql_executor,
        live_worker=None,
        live_state_store=None,
        live_state_repository=None,
        stream_queue=None,
        now_ms_factory=None,
        season_widget_gate=None,
        season_widget_structural_gate=None,
        event_endpoint_gate=None,
        live_bootstrap_coordinator=None,
        tier_override_registry=None,
        final_sweep_gate=None,
        freshness_store=None,
        player_profile_freshness_ttl_seconds: int = PLAYER_PROFILE_FRESHNESS_TTL_SECONDS,
        fanout_max_inflight: int = 1,
        # Phase 4.7 wire (2026-05-23): League Capabilities Registry.
        # ``None`` = pre-Phase-4.7 behaviour (orchestrator never
        # consults registry, gates fall back to legacy tier-based
        # logic). When non-None AND
        # ``is_league_capabilities_enabled()`` returns True, the
        # orchestrator looks up each endpoint pattern in the registry
        # before calling the gate function — measured verdicts win
        # over legacy policy.
        league_capabilities=None,
    ) -> None:
        self.fetch_executor = fetch_executor
        self.snapshot_store = snapshot_store
        self.normalize_worker = normalize_worker
        self.planner = planner
        self.capability_repository = capability_repository
        self.sql_executor = sql_executor
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        # A3 Phase 0 (2026-05-16): inject the per-UT tier-override registry
        # into the LiveWorker so dispatch_tier resolution short-circuits
        # to the operator-set value when one exists.
        self.tier_override_registry = tier_override_registry
        self.live_worker = live_worker or LiveWorker(
            now_ms_factory=self.now_ms_factory,
            tier_override_registry=tier_override_registry,
        )
        if hasattr(self.live_worker, "now_ms_factory"):
            self.live_worker.now_ms_factory = self.now_ms_factory
        self.live_state_store = live_state_store
        self.live_state_repository = live_state_repository
        self.stream_queue = stream_queue
        self.season_widget_gate = season_widget_gate
        # P0.2 — optional structural pre-filter for season widgets.
        # ``None`` = legacy behaviour (only negative-cache gating).
        self.season_widget_structural_gate = season_widget_structural_gate
        self.event_endpoint_gate = event_endpoint_gate
        self.live_bootstrap_coordinator = live_bootstrap_coordinator
        self.final_sweep_gate = final_sweep_gate
        self.freshness_store = freshness_store
        self.player_profile_freshness_ttl_seconds = int(player_profile_freshness_ttl_seconds)
        self._fanout_max_inflight = max(1, int(fanout_max_inflight or 1))
        # Phase 4.7 wire: registry instance (may be None).
        self.league_capabilities = league_capabilities
        self._event_endpoint_gate_decision_lock = asyncio.Lock()
        self._rollups: dict[tuple[str, str], CapabilityRollupAccumulator] = {}
        self._pending_capability_records: list[DeferredCapabilityRecord] = []
        # Task 6 (2026-05-15): track event id whose hydrate lock this
        # orchestrator instance is currently holding. Set by run_event
        # when ``acquire_hydrate_lock`` succeeds, cleared by
        # ``release_hydrate_lock_if_held`` which the worker layer calls
        # in its ``finally`` block regardless of run_event outcome.
        self._pending_hydrate_lock_event_id: int | None = None
        self._freshness_skip_keys: set[str] = set()

    async def _resolve_edge_capability_verdict(
        self,
        *,
        unique_tournament_id,
        season_id,
        status_type,
        edge_kind,
    ) -> str | None:
        """Phase 4.7 wire (2026-05-23): resolve capability verdict
        for one core edge.

        Maps ``edge_kind`` → endpoint pattern via
        ``_endpoint_for_edge_kind`` and asks the registry. Returns
        None when:
          * feature flag OFF
          * no registry instance attached to this orchestrator
          * edge_kind doesn't map to a known endpoint
          * registry has no row for the quad
          * registry raised (Redis / DB failure)
        Caller passes result to ``football_edge_allowed.capability_verdict``;
        None means fall back to legacy tier-based logic.
        """

        endpoint = _endpoint_for_edge_kind(str(edge_kind or ""))
        if endpoint is None:
            return None
        return await resolve_capability_verdict(
            registry=self.league_capabilities,
            enabled=is_league_capabilities_enabled(),
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            status_type=status_type,
            endpoint_pattern=endpoint.pattern,
        )

    async def _resolve_detail_capability_verdicts(
        self,
        *,
        unique_tournament_id,
        season_id,
        status_type,
    ) -> dict[str, str]:
        """Phase 4.7.3 wire (2026-05-23): resolve per-endpoint capability
        verdicts for the football detail-spec fanout.

        Queries the registry for every endpoint pattern in the status-
        specific allow set (managers, h2h, comments, shotmap, heatmap, …)
        and returns a ``{pattern: 'allowed'|'disabled'|'unknown'}`` dict
        that the caller passes to ``build_event_detail_request_specs(...,
        capability_verdicts=...)``. Patterns absent from the dict fall
        back to legacy tier-based gating inside
        ``football_detail_endpoint_allowed``.
        Returns ``{}`` (and skips all registry traffic) when:
          * feature flag OFF,
          * no registry instance attached to this orchestrator,
          * unique_tournament_id or status_type is None,
          * the status doesn't resolve to a known detail-pattern set.

        Each individual ``resolve_capability_verdict`` call is fail-safe;
        Redis / Postgres failures bubble up as ``None`` for that pattern,
        which the caller treats as "fall through to legacy" — never blocks
        the hot path.
        """

        if not is_league_capabilities_enabled() or self.league_capabilities is None:
            return {}
        if unique_tournament_id is None or status_type is None:
            return {}

        # Import is lazy to avoid pulling match_center_policy into the
        # orchestrator's import graph eagerly (it's already imported via
        # other paths but keeping this local mirrors _resolve_edge above).
        from ..match_center_policy import _allowed_detail_patterns_for_status

        normalized_status = str(status_type or "").strip().lower()
        patterns = _allowed_detail_patterns_for_status(normalized_status)
        if not patterns:
            return {}

        # Phase 4.7.4 (2026-05-23): single batched registry call replaces
        # the previous per-pattern loop. The earlier code issued 12
        # sequential ``await resolve_capability_verdict(...)`` calls per
        # match-center fetch; under the Phase 4.8 production flip that
        # exhausted the asyncpg connection pool with 70+ concurrent
        # workers, every job hit the 30 s statement-timeout, and live
        # throughput cratered 17-20×. The batch helper resolves the
        # whole quad in at most two DB roundtrips (season-specific +
        # UT-level fallback) and primes Redis along the way.
        try:
            batch = await self.league_capabilities.get_verdicts_batch(
                unique_tournament_id=int(unique_tournament_id),
                season_id=None if season_id is None else int(season_id),
                status_type=str(status_type),
                endpoint_patterns=tuple(str(p) for p in patterns),
            )
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning(
                "PilotOrchestrator detail verdicts batch failed "
                "ut=%s season=%s status=%s err=%r",
                unique_tournament_id, season_id, status_type, exc,
            )
            return {}
        return {
            str(pattern): verdict.cache_value
            for pattern, verdict in batch.items()
        }

    @property
    def freshness_skip_keys(self) -> frozenset[str]:
        return frozenset(self._freshness_skip_keys)

    async def run_event(
        self,
        *,
        event_id: int,
        sport_slug: str,
        hydration_mode: str = "full",
        scope: str | None = None,
    ) -> PilotRunReport:
        self._pending_capability_records.clear()
        if self.fetch_executor is None:
            raise RuntimeError("fetch_executor is required for run_event")

        # Task 2 Phase B (2026-05-20): scope marker. When the
        # FinalSyncPlannerDaemon enqueues a final-sync run we receive
        # ``scope="final_sync"`` here; after the pipeline completes
        # successfully we stamp ``event_terminal_state.locked_at`` to
        # freeze the event permanently.
        normalized_scope = (str(scope).strip().lower() if scope else "")

        requested_hydration_mode = str(hydration_mode or "full").strip().lower()
        effective_hydration_mode = requested_hydration_mode
        should_mark_live_bootstrap = False
        # Task 6 (2026-05-15): track explicit hydrate-lock ownership so
        # the ``finally`` block below releases it on every exit path
        # (success, exception, terminal early-return). Without this the
        # lock only released via TTL (60 s default), and any failure
        # mid-bootstrap blocked the next ~6-12 poll cycles for the same
        # event — root cause of the 49% never-bootstrapped events on
        # prod. See live_bootstrap.release_hydrate_lock docstring.
        if requested_hydration_mode == "live_delta" and self.live_bootstrap_coordinator is not None:
            is_bootstrapped = await self.live_bootstrap_coordinator.is_bootstrapped(self.sql_executor, event_id=event_id)
            if not is_bootstrapped:
                if not self.live_bootstrap_coordinator.acquire_hydrate_lock(event_id=event_id):
                    return PilotRunReport(
                        sport_slug=sport_slug,
                        event_id=event_id,
                        fetch_outcomes=(),
                        parse_results=(),
                    )
                # Task 6 fix: record the locked event_id so the worker
                # finally-block can always release the lock — even when
                # the bootstrap fan-out below raises mid-flight.
                self._pending_hydrate_lock_event_id = int(event_id)
                effective_hydration_mode = "full"
                should_mark_live_bootstrap = True
        core_only = effective_hydration_mode == "core"
        lightweight_only = effective_hydration_mode in {"core", "live_delta"}
        fetch_outcomes: list[FetchOutcomeEnvelope] = []
        parse_results: list[object] = []
        live_lane: str | None = None
        live_stream: str | None = None
        finalized = False
        baseball_seen_at_bats: set[int] = set()

        root_outcome, root_parse = await self._fetch_and_parse(
            endpoint=EVENT_DETAIL_ENDPOINT,
            sport_slug=sport_slug,
            path_params={"event_id": event_id},
            context_entity_type="event",
            context_entity_id=event_id,
            context_event_id=event_id,
            fetch_reason=JOB_HYDRATE_EVENT_ROOT,
        )
        fetch_outcomes.append(root_outcome)
        # Fix A: surface transient root-fetch failures so the worker
        # retries instead of marking the job ``succeeded`` with no data.
        # See ``_ROOT_FETCH_RETRYABLE_CLASSIFICATIONS`` for the included
        # set. ``not_found`` is not in this set (its classifier sets
        # retry_recommended=False) so the existing 404 retire path
        # below is preserved. We do NOT gate on ``snapshot_id is None``
        # — a 403 / 429 / soft-bot-challenge can land with a small JSON
        # body that does get persisted for forensics, but still means
        # "we did not get the data we asked for" and must retry.
        if (
            root_outcome.classification in _ROOT_FETCH_RETRYABLE_CLASSIFICATIONS
            and root_outcome.retry_recommended
        ):
            raise RetryableJobError(
                "Root /event fetch failed (event_id={event_id}, "
                "classification={classification}, http_status={http_status}): "
                "{error_message}".format(
                    event_id=event_id,
                    classification=root_outcome.classification,
                    http_status=root_outcome.http_status,
                    error_message=root_outcome.error_message or "",
                )
            )
        should_retire_missing_root = root_outcome.classification == "not_found" and await self._should_retire_missing_root_event(
            root_outcome=root_outcome
        )
        if should_retire_missing_root:
            finalized = True
            self.live_worker.finalize_event(
                sport_slug=sport_slug,
                event_id=event_id,
                status_type=MISSING_ROOT_TERMINAL_STATUS,
                live_state_store=self.live_state_store,
            )
            await self._record_live_state_history(
                event_id=event_id,
                status_type=MISSING_ROOT_TERMINAL_STATUS,
                poll_profile="terminal",
                observed_at=root_outcome.fetched_at,
            )
            await self._record_terminal_state(
                event_id=event_id,
                status_type=MISSING_ROOT_TERMINAL_STATUS,
                finalized_at=root_outcome.fetched_at,
                final_snapshot_id=root_outcome.snapshot_id,
            )
            await self._flush_capabilities()
            return PilotRunReport(
                sport_slug=sport_slug,
                event_id=event_id,
                fetch_outcomes=tuple(fetch_outcomes),
                parse_results=tuple(parse_results),
                live_lane=live_lane,
                live_stream=live_stream,
                finalized=finalized,
            )
        if root_parse is not None:
            parse_results.append(root_parse)

        # Mark bootstrap state as soon as the root response is in. Subsequent
        # poll ticks for the same event will then see is_bootstrapped=True
        # and run as live_delta instead of being promoted to full again,
        # even if the fan-out from THIS tick is still in flight.
        if (
            should_mark_live_bootstrap
            and self.live_bootstrap_coordinator is not None
            and root_outcome.classification in {"success_json", "success_empty_json"}
        ):
            await self.live_bootstrap_coordinator.mark_bootstrapped(
                self.sql_executor, event_id=event_id
            )
            should_mark_live_bootstrap = False

        status_type = None
        season_id = None
        unique_tournament_id = None
        start_timestamp = None
        home_team_id = None
        away_team_id = None
        detail_id = None
        custom_id = None
        has_event_player_statistics = None
        has_event_player_heat_map = None
        has_global_highlights = None
        has_xg = None
        # X'' matchcenter policy: ``is_editor`` lifted from the parsed root
        # event row so every downstream policy call can honour the HARD
        # BAN on SofaEditor (crowdsourcing-app) events. None → "unknown"
        # treated as non-banned to preserve legacy paths for events whose
        # root payload predates the field landing in the parser.
        is_editor = None
        tournament_tier = None
        tournament_user_count = None
        if root_parse is not None:
            event_rows = root_parse.entity_upserts.get("event", ())
            season_rows = root_parse.entity_upserts.get("season", ())
            unique_tournament_rows = root_parse.entity_upserts.get("unique_tournament", ())
            if event_rows:
                event_row = event_rows[0]
                status_type = event_row.get("status_type")
                start_timestamp = event_row.get("start_timestamp")
                home_team_id = _as_int(event_row.get("home_team_id"))
                away_team_id = _as_int(event_row.get("away_team_id"))
                detail_id = _as_int(event_row.get("detail_id"))
                custom_id = event_row.get("custom_id")
                has_event_player_statistics = event_row.get("has_event_player_statistics")
                has_event_player_heat_map = event_row.get("has_event_player_heat_map")
                has_global_highlights = event_row.get("has_global_highlights")
                has_xg = event_row.get("has_xg")
                is_editor = event_row.get("is_editor")
            if season_rows:
                season_id = season_rows[0].get("id")
            if unique_tournament_rows:
                unique_tournament_row = unique_tournament_rows[0]
                unique_tournament_id = unique_tournament_row.get("id")
                tournament_tier = _as_int(unique_tournament_row.get("tier"))
                tournament_user_count = _as_int(unique_tournament_row.get("user_count"))
        status_phase = normalize_event_status_phase(status_type)

        # P0(b) tier_1 root-only fast path (``LIVE_TIER_1_ROOT_ONLY``).
        # When the worker requested ``hydration_mode="root_only"``, return
        # IMMEDIATELY after ROOT (and inline finalisation if the status
        # is terminal). Edges/details fan-out is deferred — the worker
        # layer enqueues a follow-up ``refresh_live_event`` (full
        # hydration) job onto ``stream:etl:live_warm`` so the slow lane
        # picks them up. The point of this branch is to release the
        # tier_1 critical path within ~1-2 s wall-clock (single ROOT
        # fetch + parse + persist) instead of the 5-15 min legacy run.
        # For terminal status payloads (``finished`` / ``cancelled`` /
        # ``postponed`` / ``afterextra`` / ``afterpen``), inline
        # finalise + record_terminal_state mirror the existing 404
        # retire flow above so terminal events stop polling immediately
        # without waiting for the slow-lane follow-up.
        if effective_hydration_mode == "root_only":
            normalized_status_type = (
                str(status_type).strip().lower() if status_type is not None else ""
            )
            is_terminal_status = normalized_status_type in TERMINAL_STATUS_TYPES
            if is_terminal_status and not finalized:
                finalized = True
                self.live_worker.finalize_event(
                    sport_slug=sport_slug,
                    event_id=event_id,
                    status_type=status_type,
                    live_state_store=self.live_state_store,
                )
                await self._record_live_state_history(
                    event_id=event_id,
                    status_type=status_type,
                    poll_profile="terminal",
                    observed_at=root_outcome.fetched_at,
                )
                await self._record_terminal_state(
                    event_id=event_id,
                    status_type=status_type,
                    finalized_at=root_outcome.fetched_at,
                    final_snapshot_id=root_outcome.snapshot_id,
                )
                if self.live_bootstrap_coordinator is not None:
                    await self.live_bootstrap_coordinator.reset_bootstrap(
                        self.sql_executor, event_id=event_id
                    )
            await self._flush_capabilities()
            return PilotRunReport(
                sport_slug=sport_slug,
                event_id=event_id,
                fetch_outcomes=tuple(fetch_outcomes),
                parse_results=tuple(parse_results),
                live_lane=live_lane,
                live_stream=live_stream,
                finalized=finalized,
                # signal worker to enqueue edges follow-up only when not
                # finalised — terminal events have nothing left to refresh.
                edges_pending=not finalized,
            )

        root_job = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug=sport_slug,
            entity_type="event",
            entity_id=event_id,
            scope="pilot",
            params={
                "status_type": status_type,
                "hydration_mode": effective_hydration_mode,
                "detail_id": detail_id,
                "has_event_player_statistics": has_event_player_statistics,
                "has_event_player_heat_map": has_event_player_heat_map,
                "has_global_highlights": has_global_highlights,
                "has_xg": has_xg,
            },
            priority=0,
            trace_id=f"pilot:{sport_slug}:{event_id}",
        )
        planned_jobs = self.planner.expand(root_job)
        if effective_hydration_mode == "live_delta" and self._fanout_max_inflight > 1:
            edge_specs: list[_EventEndpointFetchSpec] = []
            for edge_job in planned_jobs:
                edge_kind = str(edge_job.params.get("edge_kind") or "")
                # Phase 4.7 wire: registry verdict short-circuits legacy.
                capability_verdict = await self._resolve_edge_capability_verdict(
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    status_type=status_type,
                    edge_kind=edge_kind,
                )
                if not football_edge_allowed(
                    sport_slug=sport_slug,
                    edge_kind=edge_kind,
                    detail_id=detail_id,
                    status_type=status_type,
                    has_xg=has_xg,
                    is_editor=is_editor,  # X'' matchcenter ban
                    capability_verdict=capability_verdict,
                ):
                    continue
                endpoint = _endpoint_for_edge_kind(edge_kind)
                if endpoint is None:
                    continue
                edge_specs.append(
                    _EventEndpointFetchSpec(
                        endpoint=endpoint,
                        sport_slug=sport_slug,
                        path_params={"event_id": event_id},
                        context_entity_type="event",
                        context_entity_id=event_id,
                        context_event_id=event_id,
                        fetch_reason=edge_job.job_type,
                        status_phase=status_phase,
                    )
                )
            for outcome, parsed in await self._fetch_event_endpoint_specs_bounded(edge_specs, phase_name="edges"):
                if outcome is None:
                    continue
                fetch_outcomes.append(outcome)
                if parsed is not None:
                    parse_results.append(parsed)
        else:
            for edge_job in planned_jobs:
                edge_kind = str(edge_job.params.get("edge_kind") or "")
                # Phase 4.7 wire.
                capability_verdict = await self._resolve_edge_capability_verdict(
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    status_type=status_type,
                    edge_kind=edge_kind,
                )
                if not football_edge_allowed(
                    sport_slug=sport_slug,
                    edge_kind=edge_kind,
                    detail_id=detail_id,
                    status_type=status_type,
                    has_xg=has_xg,
                    is_editor=is_editor,  # X'' matchcenter ban
                    capability_verdict=capability_verdict,
                ):
                    continue
                endpoint = _endpoint_for_edge_kind(edge_kind)
                if endpoint is None:
                    continue
                outcome, parsed = await self._fetch_gated_event_endpoint(
                    endpoint=endpoint,
                    sport_slug=sport_slug,
                    path_params={"event_id": event_id},
                    context_entity_type="event",
                    context_entity_id=event_id,
                    context_event_id=event_id,
                    fetch_reason=edge_job.job_type,
                    status_phase=status_phase,
                )
                if outcome is None:
                    continue
                fetch_outcomes.append(outcome)
                if parsed is not None:
                    parse_results.append(parsed)
                    if not lightweight_only:
                        followup_jobs = self.planner.plan_lineup_followups(edge_job, parsed)
                        for followup_job in followup_jobs:
                            if not football_special_allowed(
                                sport_slug=sport_slug,
                                special_kind=str(followup_job.params.get("special_kind") or ""),
                                detail_id=detail_id,
                                has_event_player_statistics=has_event_player_statistics,
                                has_event_player_heat_map=has_event_player_heat_map,
                                has_xg=has_xg,
                                is_editor=is_editor,  # X'' matchcenter ban
                            ):
                                continue
                            special_outcome, special_parse = await self._run_special_job(
                                job=followup_job,
                                sport_slug=sport_slug,
                                event_id=event_id,
                                status_phase=status_phase,
                            )
                            if special_outcome is None:
                                continue
                            fetch_outcomes.append(special_outcome)
                            if special_parse is not None:
                                parse_results.append(special_parse)
                child_outcomes, child_parses = await self._run_baseball_pitch_fanout(
                    sport_slug=sport_slug,
                    event_id=event_id,
                    parent_endpoint=endpoint,
                    parent_outcome=outcome,
                    seen_at_bats=baseball_seen_at_bats,
                )
                fetch_outcomes.extend(child_outcomes)
                parse_results.extend(child_parses)

        minutes_to_start = _minutes_to_start(
            start_timestamp=start_timestamp,
            now_ms=int(self.now_ms_factory()),
        )
        tracked_live = False
        for planned_job in planned_jobs:
            if planned_job.job_type == JOB_TRACK_LIVE_EVENT:
                track_result = self.live_worker.track_event(
                    sport_slug=sport_slug,
                    event_id=event_id,
                    status_type=status_type,
                    minutes_to_start=minutes_to_start,
                    trace_id=root_job.trace_id,
                    detail_id=detail_id,
                    tournament_tier=tournament_tier,
                    tournament_user_count=tournament_user_count,
                    live_state_store=self.live_state_store,
                    stream_queue=self.stream_queue,
                )
                live_lane = track_result.decision.lane
                live_stream = track_result.stream
                tracked_live = True
                await self._record_live_state_history(
                    event_id=event_id,
                    status_type=status_type,
                    poll_profile=track_result.decision.lane,
                    observed_at=root_outcome.fetched_at,
                )
            if planned_job.job_type == JOB_FINALIZE_EVENT:
                async def run_sweep():
                    return await self._run_final_sweep(
                        event_id=event_id,
                        sport_slug=sport_slug,
                        detail_id=detail_id,
                        status_type=status_type,
                        has_xg=has_xg,
                        status_phase=status_phase,
                        is_editor=is_editor,
                    )

                if self.final_sweep_gate is None:
                    final_outcomes, final_parses = await run_sweep()
                else:
                    final_outcomes, final_parses = await self.final_sweep_gate.run(run_sweep)
                fetch_outcomes.extend(final_outcomes)
                parse_results.extend(final_parses)
                self.live_worker.finalize_event(
                    sport_slug=sport_slug,
                    event_id=event_id,
                    status_type=status_type,
                    live_state_store=self.live_state_store,
                )
                final_snapshot_id = _latest_snapshot_id(final_outcomes) or root_outcome.snapshot_id
                await self._record_live_state_history(
                    event_id=event_id,
                    status_type=status_type,
                    poll_profile="terminal",
                    observed_at=_latest_fetched_at(final_outcomes) or root_outcome.fetched_at,
                )
                await self._record_terminal_state(
                    event_id=event_id,
                    status_type=status_type,
                    finalized_at=_latest_fetched_at(final_outcomes) or root_outcome.fetched_at,
                    final_snapshot_id=final_snapshot_id,
                )
                if self.live_bootstrap_coordinator is not None:
                    await self.live_bootstrap_coordinator.reset_bootstrap(self.sql_executor, event_id=event_id)
                finalized = True

        if (
            not tracked_live
            and not finalized
            and minutes_to_start is not None
            and minutes_to_start <= 30
        ):
            track_result = self.live_worker.track_event(
                sport_slug=sport_slug,
                event_id=event_id,
                status_type=status_type,
                minutes_to_start=minutes_to_start,
                trace_id=root_job.trace_id,
                detail_id=detail_id,
                tournament_tier=tournament_tier,
                tournament_user_count=tournament_user_count,
                live_state_store=self.live_state_store,
                stream_queue=self.stream_queue,
            )
            live_lane = track_result.decision.lane
            live_stream = track_result.stream
            await self._record_live_state_history(
                event_id=event_id,
                status_type=status_type,
                poll_profile=track_result.decision.lane,
                observed_at=root_outcome.fetched_at,
            )

        if not lightweight_only:
            hydrated_entities: set[tuple[str, int]] = set()
            while True:
                next_targets = _entity_profile_targets(
                    sport_slug,
                    parse_results,
                    seen=hydrated_entities,
                )
                if not next_targets:
                    break
                for entity_endpoint, entity_type, entity_id in next_targets:
                    hydrated_entities.add((entity_type, entity_id))
                    freshness_key, freshness_ttl_seconds = _entity_profile_freshness_fields(
                        entity_endpoint,
                        entity_type,
                        entity_id,
                        player_ttl_seconds=self.player_profile_freshness_ttl_seconds,
                    )
                    if freshness_key is not None and self._is_resource_fresh(freshness_key):
                        self._freshness_skip_keys.add(freshness_key)
                        logger.debug(
                            "Skipping fresh entity profile fan-out: entity_type=%s entity_id=%s endpoint=%s",
                            entity_type,
                            entity_id,
                            entity_endpoint.pattern,
                        )
                        continue
                    outcome, parsed = await self._fetch_and_parse(
                        endpoint=entity_endpoint,
                        sport_slug=sport_slug,
                        path_params={f"{entity_type}_id": entity_id},
                        context_entity_type=entity_type,
                        context_entity_id=entity_id,
                        context_event_id=event_id,
                        fetch_reason="hydrate_entity_profile",
                        freshness_key=freshness_key,
                        freshness_ttl_seconds=freshness_ttl_seconds,
                    )
                    fetch_outcomes.append(outcome)
                    if parsed is not None:
                        parse_results.append(parsed)

            if unique_tournament_id is not None and season_id is not None:
                blocked_widget_patterns: tuple[str, ...] = ()
                candidate_patterns = self.planner.plan_season_widget_patterns(
                    sport_slug,
                    unique_tournament_id=int(unique_tournament_id),
                    season_id=int(season_id),
                )
                if self.season_widget_gate is not None:
                    blocked_widget_patterns = await self.season_widget_gate.blocked_endpoint_patterns(
                        sport_slug=sport_slug,
                        unique_tournament_id=int(unique_tournament_id),
                        season_id=int(season_id),
                        endpoint_patterns=candidate_patterns,
                    )
                # P0.2 — structural pre-filter (POS completed-only,
                # regularSeason allowlist) merged in before negative
                # cache decisions. Cheap: at most one DB lookup for POS
                # plus an in-memory allowlist check for regularSeason.
                if self.season_widget_structural_gate is not None and candidate_patterns:
                    structural_blocks = await self.season_widget_structural_gate.blocked_patterns(
                        unique_tournament_id=int(unique_tournament_id),
                        season_id=int(season_id),
                        candidate_patterns=candidate_patterns,
                    )
                    if structural_blocks:
                        merged = set(blocked_widget_patterns)
                        merged.update(structural_blocks)
                        blocked_widget_patterns = tuple(sorted(merged))
                widget_jobs = self.planner.plan_season_widgets(
                    sport_slug,
                    unique_tournament_id=int(unique_tournament_id),
                    season_id=int(season_id),
                    blocked_endpoint_patterns=blocked_widget_patterns,
                )
                for widget_job in widget_jobs:
                    endpoint = _endpoint_for_widget_job(widget_job.params)
                    if endpoint is None:
                        continue
                    endpoint_pattern = endpoint.pattern
                    decision = None
                    if self.season_widget_gate is not None:
                        decision = await self.season_widget_gate.decide_widget_probe(
                            sport_slug=sport_slug,
                            unique_tournament_id=int(unique_tournament_id),
                            season_id=int(season_id),
                            widget_job=widget_job,
                            endpoint_pattern=endpoint_pattern,
                        )
                        if not getattr(decision, "should_fetch", True):
                            continue
                    outcome, parsed = await self._fetch_and_parse(
                        endpoint=endpoint,
                        sport_slug=sport_slug,
                        path_params={
                            "unique_tournament_id": int(unique_tournament_id),
                            "season_id": int(season_id),
                        },
                        context_entity_type="season",
                        context_entity_id=int(season_id),
                        context_unique_tournament_id=int(unique_tournament_id),
                        context_season_id=int(season_id),
                        fetch_reason=widget_job.job_type,
                    )
                    if self.season_widget_gate is not None and decision is not None:
                        await self.season_widget_gate.record_widget_outcome(
                            decision=decision,
                            endpoint_pattern=endpoint_pattern,
                            outcome=outcome,
                        )
                    fetch_outcomes.append(outcome)
                    if parsed is not None:
                        parse_results.append(parsed)

        # P0(a): when split-details is enabled and this is a live_delta tick
        # which has not just been finalized, skip the per-event detail fanout
        # in this worker. The live-tier worker layer will enqueue a standalone
        # ``refresh_live_event_details`` job onto ``stream:etl:live_details``
        # for the dedicated details worker pool to consume. Critical path
        # (ROOT + edges + track_live + final sweep on finalize) is unchanged
        # — it has already executed above before this gate.
        skip_details_in_run_event = (
            self._split_details_fanout_enabled()
            and effective_hydration_mode == "live_delta"
            and not finalized
        )
        details_context: dict[str, Any] | None = None
        if skip_details_in_run_event:
            details_context = {
                "status_type": status_type,
                "status_phase": status_phase,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "detail_id": detail_id,
                "custom_id": str(custom_id) if custom_id is not None else None,
                "start_timestamp": _as_int(start_timestamp),
                "has_event_player_statistics": has_event_player_statistics,
                "has_event_player_heat_map": has_event_player_heat_map,
                "has_global_highlights": has_global_highlights,
                "has_xg": has_xg,
                "effective_hydration_mode": effective_hydration_mode,
                "core_only": core_only,
                # Phase 4.7.3 (2026-05-23): forward UT/season so the
                # split-details worker can re-resolve capability verdicts
                # for the second build_event_detail_request_specs call
                # site inside run_event_details.
                "unique_tournament_id": unique_tournament_id,
                "season_id": season_id,
            }
            if should_mark_live_bootstrap and root_outcome.classification in {"success_json", "success_empty_json"}:
                await self.live_bootstrap_coordinator.mark_bootstrapped(self.sql_executor, event_id=event_id)
                should_mark_live_bootstrap = False
            await self._flush_capabilities()
            return PilotRunReport(
                sport_slug=sport_slug,
                event_id=event_id,
                fetch_outcomes=tuple(fetch_outcomes),
                parse_results=tuple(parse_results),
                live_lane=live_lane,
                live_stream=live_stream,
                finalized=finalized,
                details_pending=True,
                details_context=details_context,
            )

        # Phase 4.7.3 wire (2026-05-23): resolve per-endpoint capability
        # verdicts before fanout. Empty dict when flag OFF / no registry,
        # so legacy gating stays in effect by default.
        detail_capability_verdicts = await self._resolve_detail_capability_verdicts(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            status_type=status_type,
        )
        event_detail_request_specs = build_event_detail_request_specs(
            sport_slug=sport_slug,
            status_type=status_type,
            team_ids=tuple(team_id for team_id in (home_team_id, away_team_id) if isinstance(team_id, int)),
            provider_ids=(1,),
            has_event_player_statistics=has_event_player_statistics,
            has_event_player_heat_map=has_event_player_heat_map,
            has_global_highlights=has_global_highlights,
            has_xg=has_xg,
            detail_id=detail_id,
            custom_id=str(custom_id) if custom_id is not None else None,
            start_timestamp=_as_int(start_timestamp),
            now_timestamp=int(self.now_ms_factory()) // 1000,
            core_only=core_only,
            hydration_mode=effective_hydration_mode,
            is_editor=is_editor,  # X'' matchcenter ban
            capability_verdicts=detail_capability_verdicts,
        )
        if effective_hydration_mode == "live_delta" and self._fanout_max_inflight > 1:
            detail_specs: list[tuple[SofascoreEndpoint, _EventEndpointFetchSpec]] = []
            for request_spec in event_detail_request_specs:
                endpoint = request_spec.endpoint
                if (
                    sport_slug == "baseball"
                    and endpoint.pattern == "/api/v1/event/{event_id}/comments"
                    and baseball_seen_at_bats
                ):
                    continue
                detail_specs.append(
                    (
                        endpoint,
                        _EventEndpointFetchSpec(
                            endpoint=endpoint,
                            sport_slug=sport_slug,
                            path_params=request_spec.resolved_path_params(event_id=event_id),
                            context_entity_type="event",
                            context_entity_id=event_id,
                            context_event_id=event_id,
                            fetch_reason="hydrate_special_route",
                            status_phase=status_phase,
                        ),
                    )
                )
            detail_results = await self._fetch_event_endpoint_specs_bounded(
                [spec for _, spec in detail_specs],
                phase_name="details",
            )
            for (endpoint, _), (outcome, parsed) in zip(detail_specs, detail_results, strict=True):
                if outcome is None:
                    continue
                fetch_outcomes.append(outcome)
                if parsed is not None:
                    parse_results.append(parsed)
                child_outcomes, child_parses = await self._run_baseball_pitch_fanout(
                    sport_slug=sport_slug,
                    event_id=event_id,
                    parent_endpoint=endpoint,
                    parent_outcome=outcome,
                    seen_at_bats=baseball_seen_at_bats,
                )
                fetch_outcomes.extend(child_outcomes)
                parse_results.extend(child_parses)
        else:
            for request_spec in event_detail_request_specs:
                endpoint = request_spec.endpoint
                if (
                    sport_slug == "baseball"
                    and endpoint.pattern == "/api/v1/event/{event_id}/comments"
                    and baseball_seen_at_bats
                ):
                    continue
                outcome, parsed = await self._fetch_gated_event_endpoint(
                    endpoint=endpoint,
                    sport_slug=sport_slug,
                    path_params=request_spec.resolved_path_params(event_id=event_id),
                    context_entity_type="event",
                    context_entity_id=event_id,
                    context_event_id=event_id,
                    fetch_reason="hydrate_special_route",
                    status_phase=status_phase,
                )
                if outcome is None:
                    continue
                fetch_outcomes.append(outcome)
                if parsed is not None:
                    parse_results.append(parsed)
                child_outcomes, child_parses = await self._run_baseball_pitch_fanout(
                    sport_slug=sport_slug,
                    event_id=event_id,
                    parent_endpoint=endpoint,
                    parent_outcome=outcome,
                    seen_at_bats=baseball_seen_at_bats,
                )
                fetch_outcomes.extend(child_outcomes)
                parse_results.extend(child_parses)
                if not lightweight_only:
                    shotmap_outcomes, shotmap_parses = await self._run_football_shotmap_fanout(
                        sport_slug=sport_slug,
                        event_id=event_id,
                        detail_id=detail_id,
                        has_xg=has_xg,
                        status_phase=status_phase,
                        parent_endpoint=endpoint,
                        parent_outcome=outcome,
                        is_editor=is_editor,
                    )
                    fetch_outcomes.extend(shotmap_outcomes)
                    parse_results.extend(shotmap_parses)

        if should_mark_live_bootstrap and not finalized and root_outcome.classification in {"success_json", "success_empty_json"}:
            await self.live_bootstrap_coordinator.mark_bootstrapped(self.sql_executor, event_id=event_id)

        await self._flush_capabilities()

        # Task 2 Phase B (2026-05-20): stamp event_terminal_state.locked_at
        # on the success path of a scope="final_sync" run. The lock is
        # idempotent (no-op if already set) and persistent — subsequent
        # planners / workers / read-path will skip the event forever.
        # On exception this code is unreached, so the FinalSyncPlanner
        # will re-queue on the next tick.
        if normalized_scope == FINAL_SYNC_SCOPE and self.live_state_repository is not None:
            if root_outcome.classification in {"success_json", "success_empty_json"}:
                try:
                    await self.live_state_repository.set_event_locked(
                        self.sql_executor, event_id=event_id
                    )
                    logger.info(
                        "final_sync: stamped locked_at for event_id=%s",
                        event_id,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "final_sync: set_event_locked failed event_id=%s: %r",
                        event_id,
                        exc,
                    )

        return PilotRunReport(
            sport_slug=sport_slug,
            event_id=event_id,
            fetch_outcomes=tuple(fetch_outcomes),
            parse_results=tuple(parse_results),
            live_lane=live_lane,
            live_stream=live_stream,
            finalized=finalized,
        )

    def replay_snapshot(self, snapshot_id: int):
        snapshot = self.snapshot_store.load_snapshot(snapshot_id)
        return self.normalize_worker.handle(snapshot)

    def _split_details_fanout_enabled(self) -> bool:
        """True when ``LIVE_SPLIT_DETAILS_FANOUT=1`` is in the environment.

        Read at call time (not constructor) so a systemd drop-in
        environment edit + worker restart is the only step needed to
        flip the behaviour for a given tier.
        """
        raw = os.environ.get("LIVE_SPLIT_DETAILS_FANOUT", "0")
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    async def run_event_details(
        self,
        *,
        event_id: int,
        sport_slug: str,
        context: dict[str, Any],
    ) -> PilotRunReport:
        """Run ONLY the per-event detail fanout phase (P0(a) split-details).

        Consumed by ``LiveDetailsWorkerService`` from
        ``stream:etl:live_details``. ``context`` carries the values that
        are normally extracted from the freshly-parsed root payload
        during ``run_event`` (status_type, team_ids, has_* flags,
        start_timestamp, detail_id, custom_id, hydration_mode, core_only).
        Re-running the root parse here would defeat the purpose of the
        split — we trust the publisher (live-tier worker) to forward an
        accurate context dict captured at the time of root persist.

        Failure semantics — *intentionally permissive*. Network errors
        on detail endpoints record their outcome but DO NOT raise
        ``RetryableJobError``: a failed details fanout must not retry
        the parent ``refresh_live_event`` job nor block the next root
        poll. The returned report's ``fetch_outcomes`` carries the
        per-endpoint outcomes (including network_error rows) for
        forensics; the caller worker reports
        ``completed_with_errors`` if any outcome failed.
        """
        if self.fetch_executor is None:
            raise RuntimeError("fetch_executor is required for run_event_details")

        fetch_outcomes: list[FetchOutcomeEnvelope] = []
        parse_results: list[object] = []
        baseball_seen_at_bats: set[int] = set()

        status_type = context.get("status_type")
        status_phase = context.get("status_phase") or normalize_event_status_phase(status_type)
        home_team_id = _as_int(context.get("home_team_id"))
        away_team_id = _as_int(context.get("away_team_id"))
        detail_id = _as_int(context.get("detail_id"))
        custom_id_raw = context.get("custom_id")
        custom_id = str(custom_id_raw) if custom_id_raw is not None else None
        start_timestamp = _as_int(context.get("start_timestamp"))
        has_event_player_statistics = context.get("has_event_player_statistics")
        has_event_player_heat_map = context.get("has_event_player_heat_map")
        has_global_highlights = context.get("has_global_highlights")
        has_xg = context.get("has_xg")
        effective_hydration_mode = str(context.get("effective_hydration_mode") or "live_delta").strip().lower()
        core_only = bool(context.get("core_only", False))

        # Phase 4.7.3 wire (2026-05-23): resolve per-endpoint capability
        # verdicts before fanout in the split-details worker path. Empty
        # dict when flag OFF / no registry / UT not forwarded in context.
        detail_capability_verdicts = await self._resolve_detail_capability_verdicts(
            unique_tournament_id=context.get("unique_tournament_id"),
            season_id=context.get("season_id"),
            status_type=status_type,
        )
        event_detail_request_specs = build_event_detail_request_specs(
            sport_slug=sport_slug,
            status_type=status_type,
            team_ids=tuple(team_id for team_id in (home_team_id, away_team_id) if isinstance(team_id, int)),
            provider_ids=(1,),
            has_event_player_statistics=has_event_player_statistics,
            has_event_player_heat_map=has_event_player_heat_map,
            has_global_highlights=has_global_highlights,
            has_xg=has_xg,
            detail_id=detail_id,
            custom_id=custom_id,
            start_timestamp=start_timestamp,
            now_timestamp=int(self.now_ms_factory()) // 1000,
            core_only=core_only,
            hydration_mode=effective_hydration_mode,
            capability_verdicts=detail_capability_verdicts,
        )
        if effective_hydration_mode == "live_delta" and self._fanout_max_inflight > 1:
            detail_specs: list[tuple[Any, _EventEndpointFetchSpec]] = []
            for request_spec in event_detail_request_specs:
                endpoint = request_spec.endpoint
                if (
                    sport_slug == "baseball"
                    and endpoint.pattern == "/api/v1/event/{event_id}/comments"
                    and baseball_seen_at_bats
                ):
                    continue
                detail_specs.append(
                    (
                        endpoint,
                        _EventEndpointFetchSpec(
                            endpoint=endpoint,
                            sport_slug=sport_slug,
                            path_params=request_spec.resolved_path_params(event_id=event_id),
                            context_entity_type="event",
                            context_entity_id=event_id,
                            context_event_id=event_id,
                            fetch_reason="hydrate_special_route",
                            status_phase=status_phase,
                        ),
                    )
                )
            detail_results = await self._fetch_event_endpoint_specs_bounded(
                [spec for _, spec in detail_specs],
                phase_name="details_split",
            )
            for (endpoint, _), (outcome, parsed) in zip(detail_specs, detail_results, strict=True):
                if outcome is None:
                    continue
                fetch_outcomes.append(outcome)
                if parsed is not None:
                    parse_results.append(parsed)
                child_outcomes, child_parses = await self._run_baseball_pitch_fanout(
                    sport_slug=sport_slug,
                    event_id=event_id,
                    parent_endpoint=endpoint,
                    parent_outcome=outcome,
                    seen_at_bats=baseball_seen_at_bats,
                )
                fetch_outcomes.extend(child_outcomes)
                parse_results.extend(child_parses)
        else:
            for request_spec in event_detail_request_specs:
                endpoint = request_spec.endpoint
                if (
                    sport_slug == "baseball"
                    and endpoint.pattern == "/api/v1/event/{event_id}/comments"
                    and baseball_seen_at_bats
                ):
                    continue
                outcome, parsed = await self._fetch_gated_event_endpoint(
                    endpoint=endpoint,
                    sport_slug=sport_slug,
                    path_params=request_spec.resolved_path_params(event_id=event_id),
                    context_entity_type="event",
                    context_entity_id=event_id,
                    context_event_id=event_id,
                    fetch_reason="hydrate_special_route",
                    status_phase=status_phase,
                )
                if outcome is None:
                    continue
                fetch_outcomes.append(outcome)
                if parsed is not None:
                    parse_results.append(parsed)
                child_outcomes, child_parses = await self._run_baseball_pitch_fanout(
                    sport_slug=sport_slug,
                    event_id=event_id,
                    parent_endpoint=endpoint,
                    parent_outcome=outcome,
                    seen_at_bats=baseball_seen_at_bats,
                )
                fetch_outcomes.extend(child_outcomes)
                parse_results.extend(child_parses)

        await self._flush_capabilities()

        return PilotRunReport(
            sport_slug=sport_slug,
            event_id=event_id,
            fetch_outcomes=tuple(fetch_outcomes),
            parse_results=tuple(parse_results),
        )

    async def _fetch_and_parse(
        self,
        *,
        endpoint: SofascoreEndpoint,
        sport_slug: str,
        path_params: dict[str, Any],
        context_entity_type: str | None,
        context_entity_id: int | None,
        context_event_id: int | None = None,
        context_unique_tournament_id: int | None = None,
        context_season_id: int | None = None,
        fetch_reason: str,
        freshness_key: str | None = None,
        freshness_ttl_seconds: int | None = None,
    ) -> tuple[FetchOutcomeEnvelope, object | None]:
        task = FetchTask(
            trace_id=f"pilot:{sport_slug}:{context_entity_type}:{context_entity_id}",
            job_id=f"{fetch_reason}:{endpoint.pattern}:{context_entity_id}",
            sport_slug=sport_slug,
            endpoint_pattern=endpoint.pattern,
            source_url=endpoint.build_url(**path_params),
            timeout_profile="pilot",
            context_entity_type=context_entity_type,
            context_entity_id=context_entity_id,
            context_unique_tournament_id=context_unique_tournament_id,
            context_season_id=context_season_id,
            context_event_id=context_event_id,
            fetch_reason=fetch_reason,
            freshness_key=freshness_key,
            freshness_ttl_seconds=freshness_ttl_seconds,
            # P0.2 — propagate the endpoint's HEAD-probe preference for
            # the PilotOrchestrator path too (sync_season_widget,
            # season-detail / event-detail special routes). Without this
            # only the resource-refresh path gates by HEAD; the
            # higher-volume widget path keeps issuing full GETs.
            prefer_head_probe=bool(getattr(endpoint, "prefer_head_probe", False)),
        )
        outcome = await self.fetch_executor.execute(task)
        await self._record_capability(sport_slug=sport_slug, outcome=outcome, context_type=context_entity_type)

        parsed = None
        if outcome.snapshot_id is not None:
            snapshot = self.snapshot_store.load_snapshot(outcome.snapshot_id)
            handle_async = getattr(self.normalize_worker, "handle_async", None)
            if callable(handle_async):
                parsed = await handle_async(snapshot)
            else:
                parsed = self.normalize_worker.handle(snapshot)
        return outcome, parsed

    def _is_resource_fresh(self, freshness_key: str) -> bool:
        if self.freshness_store is None:
            return False
        try:
            return bool(self.freshness_store.is_fresh(freshness_key))
        except Exception as exc:
            logger.warning(
                "FreshnessStore.is_fresh failed in PilotOrchestrator (fail-open): %s",
                exc,
            )
            return False

    async def _fetch_gated_event_endpoint(
        self,
        *,
        endpoint: SofascoreEndpoint,
        sport_slug: str,
        path_params: dict[str, Any],
        context_entity_type: str | None,
        context_entity_id: int | None,
        context_event_id: int,
        fetch_reason: str,
        status_phase: str,
    ) -> tuple[FetchOutcomeEnvelope | None, object | None]:
        if is_static_dead_event_endpoint(sport_slug, endpoint.pattern):
            return None, None
        freshness_key, freshness_ttl_seconds = _event_detail_freshness_fields(
            endpoint,
            path_params,
            sport_slug=sport_slug,
            status_phase=status_phase,
        )
        if freshness_key is not None and self._is_resource_fresh(freshness_key):
            self._freshness_skip_keys.add(freshness_key)
            logger.debug(
                "Skipping fresh event detail resource: event_id=%s endpoint=%s",
                context_event_id,
                endpoint.pattern,
            )
            return None, None
        decision = None
        if self.event_endpoint_gate is not None:
            async with self._event_endpoint_gate_decision_lock:
                decision = await self.event_endpoint_gate.decide_event_probe(
                    event_id=int(context_event_id),
                    status_phase=status_phase,
                    endpoint_pattern=endpoint.pattern,
                    job_type=fetch_reason,
                )
            if not getattr(decision, "should_fetch", True):
                return None, None

        outcome, parsed = await self._fetch_and_parse(
            endpoint=endpoint,
            sport_slug=sport_slug,
            path_params=path_params,
            context_entity_type=context_entity_type,
            context_entity_id=context_entity_id,
            context_event_id=context_event_id,
            fetch_reason=fetch_reason,
            freshness_key=freshness_key,
            freshness_ttl_seconds=freshness_ttl_seconds,
        )
        if self.event_endpoint_gate is not None and decision is not None:
            await self.event_endpoint_gate.record_event_outcome(
                decision=decision,
                endpoint_pattern=endpoint.pattern,
                outcome=outcome,
                job_type=fetch_reason,
            )
        return outcome, parsed

    async def _fetch_event_endpoint_specs_bounded(
        self,
        specs: list[_EventEndpointFetchSpec] | tuple[_EventEndpointFetchSpec, ...],
        *,
        phase_name: str,
    ) -> list[tuple[FetchOutcomeEnvelope | None, object | None]]:
        # Fix #1 (2026-05-15, "tier 1 subs empty" boл):
        # Previously a single sub-endpoint exception would either break the
        # sequential loop or trigger ``raise errors[0]`` after the parallel
        # gather, aborting the whole event run. That meant: 24 of 25 subs
        # OK, 1 raises → whole event marked failed → quarantine 60s →
        # /lineups, /statistics, /incidents etc stayed empty in the DB.
        # Now individual sub failures are captured and converted into
        # ``(None, None)`` placeholders so the run continues for the rest.
        # Callers already skip ``None`` outcomes when appending to
        # ``fetch_outcomes`` / ``parse_results``, so partial success is
        # safe end-to-end.
        if not specs:
            return []
        if self._fanout_max_inflight <= 1:
            results: list[tuple[FetchOutcomeEnvelope | None, object | None]] = []
            sequential_errors = 0
            for spec in specs:
                try:
                    results.append(await self._fetch_event_endpoint_spec(spec))
                except Exception as exc:  # noqa: BLE001 — graceful per-sub failure
                    sequential_errors += 1
                    logger.warning(
                        "Event endpoint sequential fetch raised: phase=%s pattern=%s err=%r",
                        phase_name,
                        getattr(getattr(spec, "endpoint", None), "pattern", "?"),
                        exc,
                    )
                    results.append((None, None))
            if sequential_errors:
                logger.info(
                    "Event endpoint sequential fanout finished with errors: phase=%s "
                    "total=%s ok=%s errors=%s",
                    phase_name,
                    len(specs),
                    sum(1 for outcome, _ in results if outcome is not None),
                    sequential_errors,
                )
            return results

        started = time.monotonic()
        semaphore = asyncio.Semaphore(self._fanout_max_inflight)

        async def run_one(spec: _EventEndpointFetchSpec):
            async with semaphore:
                return await self._fetch_event_endpoint_spec(spec)

        gathered = await asyncio.gather(*(run_one(spec) for spec in specs), return_exceptions=True)
        results = []
        error_count = 0
        for item, spec in zip(gathered, specs):
            if isinstance(item, Exception):
                error_count += 1
                logger.warning(
                    "Event endpoint parallel fetch raised: phase=%s pattern=%s err=%r",
                    phase_name,
                    getattr(getattr(spec, "endpoint", None), "pattern", "?"),
                    item,
                )
                results.append((None, None))
            else:
                results.append(item)
        duration_ms = int((time.monotonic() - started) * 1000)
        ok_count = sum(1 for outcome, _ in results if outcome is not None)
        logger.info(
            "live_delta phase=%s endpoints=%s max_inflight=%s duration_ms=%s ok=%s errors=%s",
            phase_name,
            len(specs),
            self._fanout_max_inflight,
            duration_ms,
            ok_count,
            error_count,
        )
        # Fix #1: do NOT raise here. Partial results acceptable; the next
        # live tick will retry the missing subs naturally.
        return results

    async def _fetch_event_endpoint_spec(
        self, spec: _EventEndpointFetchSpec
    ) -> tuple[FetchOutcomeEnvelope | None, object | None]:
        return await self._fetch_gated_event_endpoint(
            endpoint=spec.endpoint,
            sport_slug=spec.sport_slug,
            path_params=spec.path_params,
            context_entity_type=spec.context_entity_type,
            context_entity_id=spec.context_entity_id,
            context_event_id=spec.context_event_id,
            fetch_reason=spec.fetch_reason,
            status_phase=spec.status_phase,
        )

    async def _run_final_sweep(
        self,
        *,
        event_id: int,
        sport_slug: str,
        detail_id: int | None = None,
        status_type: str | None = None,
        has_xg: bool | None = None,
        status_phase: str = "unknown",
        is_editor: bool | None = None,
    ) -> tuple[list[FetchOutcomeEnvelope], list[object]]:
        outcomes: list[FetchOutcomeEnvelope] = []
        parses: list[object] = []
        # X3 patch (2026-05-12): SofaEditor football events get NO final
        # sweep — neither core edges (already gated by ``football_edge_allowed``
        # below, which honours ``is_editor``) nor the Phase-2 ``final_only_edges``
        # set, which would otherwise run unconditionally. Guarding here is the
        # only way to keep Phase-2 honoured. Non-football sports are unaffected.
        if str(sport_slug or "").strip().lower() == "football" and is_editor is True:
            return outcomes, parses
        adapter = resolve_sport_adapter(sport_slug)
        # Phase 1: core edges, gated by football_edge_allowed (existing behaviour).
        # Phase 4.7 NOT wired here — _run_final_sweep doesn't carry
        # unique_tournament_id / season_id through its signature. The
        # other two football_edge_allowed call sites (live_delta /
        # full hydration paths) DO consult the registry; final sweep
        # falls back to legacy policy. Cost is bounded — final sweep
        # runs at most once per event after finalize.
        for edge_kind in adapter.core_event_edges:
            if not football_edge_allowed(
                sport_slug=sport_slug,
                edge_kind=edge_kind,
                detail_id=detail_id,
                status_type=status_type,
                has_xg=has_xg,
                is_editor=is_editor,
            ):
                continue
            endpoint = _endpoint_for_edge_kind(edge_kind)
            if endpoint is None:
                continue
            outcome, parsed = await self._fetch_gated_event_endpoint(
                endpoint=endpoint,
                sport_slug=sport_slug,
                path_params={"event_id": event_id},
                context_entity_type="event",
                context_entity_id=event_id,
                context_event_id=event_id,
                fetch_reason=JOB_FINALIZE_EVENT,
                status_phase=status_phase,
            )
            if outcome is None:
                continue
            outcomes.append(outcome)
            if parsed is not None:
                parses.append(parsed)
        # Phase 2 (F-3, 2026-05-08): final-only edges. NOT gated by
        # football_edge_allowed (whose allow-list only knows core edges) — we
        # explicitly want these to run unconditionally at finalize so the
        # finished match has the canonical post-match versions of /comments,
        # /best-players/summary, /h2h, /pregame-form. They are NOT in
        # live_delta_edge_kinds, so live polling cadence is unchanged.
        # X3 patch: ``is_editor=True`` short-circuits before reaching Phase 2
        # (see guard at top of method), so this loop only runs for non-editor
        # events.
        for edge_kind in adapter.final_only_edges:
            endpoint = _endpoint_for_final_only_edge(edge_kind)
            if endpoint is None:
                continue
            outcome, parsed = await self._fetch_gated_event_endpoint(
                endpoint=endpoint,
                sport_slug=sport_slug,
                path_params={"event_id": event_id},
                context_entity_type="event",
                context_entity_id=event_id,
                context_event_id=event_id,
                fetch_reason=JOB_FINALIZE_EVENT,
                status_phase=status_phase,
            )
            if outcome is None:
                continue
            outcomes.append(outcome)
            if parsed is not None:
                parses.append(parsed)
        return outcomes, parses

    async def _run_special_job(
        self,
        *,
        job,
        sport_slug: str,
        event_id: int,
        status_phase: str,
    ) -> tuple[FetchOutcomeEnvelope | None, object | None]:
        special_kind = str(job.params.get("special_kind") or "")
        endpoint = _endpoint_for_special_kind(special_kind)
        if endpoint is None:
            raise RuntimeError(f"Unsupported special_kind: {special_kind}")

        path_params: dict[str, Any] = {"event_id": event_id}
        context_entity_type = "event"
        context_entity_id = event_id
        if special_kind in {
            "event_player_statistics",
            "event_player_rating_breakdown",
            "event_player_heatmap",
            "event_player_shotmap",
            "event_goalkeeper_shotmap",
        }:
            player_id = int(job.params["player_id"])
            path_params["player_id"] = player_id
            context_entity_type = "player"
            context_entity_id = player_id

        return await self._fetch_gated_event_endpoint(
            endpoint=endpoint,
            sport_slug=sport_slug,
            path_params=path_params,
            context_entity_type=context_entity_type,
            context_entity_id=context_entity_id,
            context_event_id=event_id,
            fetch_reason=job.job_type,
            status_phase=status_phase,
        )

    async def _run_baseball_pitch_fanout(
        self,
        *,
        sport_slug: str,
        event_id: int,
        parent_endpoint: SofascoreEndpoint,
        parent_outcome: FetchOutcomeEnvelope,
        seen_at_bats: set[int],
    ) -> tuple[list[FetchOutcomeEnvelope], list[object]]:
        if (
            sport_slug != "baseball"
            or parent_outcome.snapshot_id is None
            or parent_endpoint.pattern not in {
                EVENT_INCIDENTS_ENDPOINT.pattern,
                EVENT_BASEBALL_INNINGS_ENDPOINT.pattern,
                EVENT_COMMENTS_ENDPOINT.pattern,
            }
        ):
            return [], []

        snapshot = self.snapshot_store.load_snapshot(parent_outcome.snapshot_id)
        discovered_at_bats = [
            at_bat_id
            for at_bat_id in _extract_baseball_at_bat_ids(snapshot.payload)
            if at_bat_id not in seen_at_bats
        ]
        if not discovered_at_bats:
            return [], []

        outcomes: list[FetchOutcomeEnvelope] = []
        parses: list[object] = []
        for at_bat_id in discovered_at_bats:
            seen_at_bats.add(at_bat_id)

        async def fetch_pitch(at_bat_id: int):
            return await self._fetch_and_parse(
                endpoint=EVENT_BASEBALL_PITCHES_ENDPOINT,
                sport_slug=sport_slug,
                path_params={"event_id": event_id, "at_bat_id": at_bat_id},
                context_entity_type="event",
                context_entity_id=event_id,
                context_event_id=event_id,
                fetch_reason="hydrate_special_route",
            )

        if self._fanout_max_inflight <= 1:
            pitch_results = [await fetch_pitch(at_bat_id) for at_bat_id in discovered_at_bats]
        else:
            started = time.monotonic()
            semaphore = asyncio.Semaphore(self._fanout_max_inflight)

            async def bounded_fetch(at_bat_id: int):
                async with semaphore:
                    return await fetch_pitch(at_bat_id)

            gathered = await asyncio.gather(
                *(bounded_fetch(at_bat_id) for at_bat_id in discovered_at_bats),
                return_exceptions=True,
            )
            # Fix #1 (2026-05-15): per-sub-endpoint resilience also for the
            # baseball pitch fanout. See _fetch_event_endpoint_specs_bounded
            # for the wider context.
            pitch_results = []
            error_count = 0
            for item in gathered:
                if isinstance(item, Exception):
                    error_count += 1
                    logger.warning(
                        "Baseball pitch fanout sub-fetch raised: err=%r", item
                    )
                else:
                    pitch_results.append(item)
            duration_ms = int((time.monotonic() - started) * 1000)
            logger.info(
                "live_delta phase=%s endpoints=%s max_inflight=%s duration_ms=%s ok=%s errors=%s",
                "baseball_pitches",
                len(discovered_at_bats),
                self._fanout_max_inflight,
                duration_ms,
                len(pitch_results),
                error_count,
            )

        for outcome, parsed in pitch_results:
            outcomes.append(outcome)
            if parsed is not None:
                parses.append(parsed)
        return outcomes, parses

    async def _run_football_shotmap_fanout(
        self,
        *,
        sport_slug: str,
        event_id: int,
        detail_id: int | None,
        has_xg: bool | None,
        status_phase: str,
        parent_endpoint: SofascoreEndpoint,
        parent_outcome: FetchOutcomeEnvelope,
        is_editor: bool | None = None,
    ) -> tuple[list[FetchOutcomeEnvelope], list[object]]:
        if (
            sport_slug != "football"
            or parent_outcome.snapshot_id is None
            or parent_endpoint.pattern != EVENT_SHOTMAP_ENDPOINT.pattern
        ):
            return [], []

        snapshot = self.snapshot_store.load_snapshot(parent_outcome.snapshot_id)
        player_ids = _extract_shotmap_player_ids(snapshot.payload)
        goalkeeper_ids = _extract_shotmap_goalkeeper_ids(snapshot.payload)
        outcomes: list[FetchOutcomeEnvelope] = []
        parses: list[object] = []

        for player_id in player_ids:
            if not football_special_allowed(
                sport_slug=sport_slug,
                special_kind="event_player_shotmap",
                detail_id=detail_id,
                has_event_player_statistics=None,
                has_event_player_heat_map=None,
                has_xg=has_xg,
                is_editor=is_editor,
            ):
                continue
            outcome, parsed = await self._run_special_job(
                job=_SyntheticSpecialJob("event_player_shotmap", player_id),
                sport_slug=sport_slug,
                event_id=event_id,
                status_phase=status_phase,
            )
            outcomes.append(outcome)
            if parsed is not None:
                parses.append(parsed)

        for player_id in goalkeeper_ids:
            if not football_special_allowed(
                sport_slug=sport_slug,
                special_kind="event_goalkeeper_shotmap",
                detail_id=detail_id,
                has_event_player_statistics=None,
                has_event_player_heat_map=None,
                has_xg=has_xg,
                is_editor=is_editor,
            ):
                continue
            outcome, parsed = await self._run_special_job(
                job=_SyntheticSpecialJob("event_goalkeeper_shotmap", player_id),
                sport_slug=sport_slug,
                event_id=event_id,
                status_phase=status_phase,
            )
            outcomes.append(outcome)
            if parsed is not None:
                parses.append(parsed)

        return outcomes, parses

    async def _record_capability(
        self,
        *,
        sport_slug: str,
        outcome: FetchOutcomeEnvelope,
        context_type: str | None,
    ) -> None:
        if outcome.classification == "replay_skipped_missing":
            return

        payload_validity = "json" if outcome.is_valid_json else "non_json"
        if outcome.is_soft_error_payload:
            payload_validity = "soft_error_json"

        observation = CapabilityObservationRecord(
            sport_slug=sport_slug,
            endpoint_pattern=outcome.endpoint_pattern,
            entity_scope=context_type,
            context_type=context_type,
            http_status=outcome.http_status,
            payload_validity=payload_validity,
            payload_root_keys=outcome.payload_root_keys,
            is_empty_payload=outcome.is_empty_payload,
            is_soft_error_payload=outcome.is_soft_error_payload,
            observed_at=outcome.fetched_at or "",
            sample_snapshot_id=outcome.snapshot_id,
        )

        key = (sport_slug, outcome.endpoint_pattern)
        accumulator = self._rollups.setdefault(
            key,
            CapabilityRollupAccumulator(sport_slug=sport_slug, endpoint_pattern=outcome.endpoint_pattern),
        )
        rollup = accumulator.observe(outcome)
        # NOTE: Intentionally do NOT mutate self.planner.capability_rollup here.
        # The planner's rollup is consumed by plan_lineup_followups /
        # _special_kind_supported / should_schedule_edge during this run.
        # If we mutated it live, prefetch and replay would diverge: prefetch
        # lands fetches in a non-deterministic order (parallel fan-out),
        # whereas replay sees all records pre-committed, so the rollup would
        # be in different states at the same decision point.
        # The accumulator above + _pending_capability_records below still
        # captures everything we need to persist into capability_repository.
        if self.capability_repository is None:
            return
        self._pending_capability_records.append(
            DeferredCapabilityRecord(
                observation=observation,
                rollup=rollup,
            )
        )

    def release_hydrate_lock_if_held(self) -> None:
        """Task 6 (2026-05-15): unconditionally release the bootstrap
        hydrate lock for the event whose orchestrator run just ended.

        Designed to be called from the worker layer's ``finally`` block
        so the lock never sits past the run, regardless of:
          - normal success (full fan-out completed + mark_bootstrapped),
          - bootstrap-but-no-marker (root succeeded, mark failed),
          - mid-flight exception (orchestrator raised).

        Idempotent. Safe to call when no lock was acquired (no-op).
        """

        event_id = self._pending_hydrate_lock_event_id
        if event_id is None:
            return
        # Clear state first so a second invocation is a true no-op even
        # if the release_hydrate_lock call raises (defensive — it should
        # not, but Redis hiccups exist).
        self._pending_hydrate_lock_event_id = None
        if self.live_bootstrap_coordinator is None:
            return
        try:
            self.live_bootstrap_coordinator.release_hydrate_lock(event_id=event_id)
        except Exception:  # noqa: BLE001 — defensive
            logger.exception(
                "release_hydrate_lock_if_held: unexpected error for event_id=%s",
                event_id,
            )

    async def _flush_capabilities(self) -> None:
        if self.capability_repository is None or not self._pending_capability_records:
            return

        observations = tuple(item.observation for item in self._pending_capability_records)
        latest_rollups: dict[tuple[str, str], CapabilityRollupRecord] = {}
        for item in self._pending_capability_records:
            latest_rollups[(item.rollup.sport_slug, item.rollup.endpoint_pattern)] = item.rollup

        # Observations are append-only and lock-free; always written inline.
        for observation in observations:
            await self.capability_repository.insert_observation(self.sql_executor, observation)

        # Firebreak (2026-05-13): the legacy `upsert_rollup` is a read-modify-
        # write race against a small 290-row PK space hit by ~20 concurrent
        # workers. It caused chronic PostgreSQL deadlocks and stalled live
        # ingestion (live-discovery worker retry loop, hot zset starvation,
        # missing matchcenter snapshots). The inline rollup path is now OFF by
        # default. Rollup state is rebuilt out-of-band from observations via
        # `rebuild-capability-rollup` CLI command. To restore the legacy
        # behaviour for debugging set `SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED=1`
        # in the prod .env; that branch sorts keys by (sport_slug,
        # endpoint_pattern) to avoid lock inversion but still has the underlying
        # race condition.
        if _inline_capability_rollup_enabled():
            for key in sorted(latest_rollups.keys()):
                await self.capability_repository.upsert_rollup(
                    self.sql_executor, latest_rollups[key]
                )

        self._pending_capability_records.clear()

    async def _record_live_state_history(
        self,
        *,
        event_id: int,
        status_type: str | None,
        poll_profile: str | None,
        observed_at: str | None,
    ) -> None:
        if self.live_state_repository is None:
            return
        await self.live_state_repository.insert_live_state_history(
            self.sql_executor,
            EventLiveStateHistoryRecord(
                event_id=event_id,
                observed_status_type=status_type,
                poll_profile=poll_profile,
                home_score=None,
                away_score=None,
                period_label=None,
                observed_at=observed_at or "",
            ),
        )

    async def _record_terminal_state(
        self,
        *,
        event_id: int,
        status_type: str | None,
        finalized_at: str | None,
        final_snapshot_id: int | None,
    ) -> None:
        if self.live_state_repository is None:
            return
        await self.live_state_repository.upsert_terminal_state(
            self.sql_executor,
            EventTerminalStateRecord(
                event_id=event_id,
                terminal_status=str(status_type or "finished"),
                finalized_at=finalized_at or "",
                final_snapshot_id=final_snapshot_id,
            ),
        )

    async def _should_retire_missing_root_event(
        self,
        *,
        root_outcome: FetchOutcomeEnvelope,
    ) -> bool:
        if root_outcome.classification != "not_found":
            return False
        fetch = getattr(self.sql_executor, "fetch", None)
        if not callable(fetch):
            return False
        rows = await fetch(
            """
            SELECT http_status
            FROM api_request_log
            WHERE source_url = $1
              AND endpoint_pattern = $2
              AND job_type = $3
            ORDER BY finished_at DESC
            LIMIT $4
            """,
            root_outcome.source_url,
            root_outcome.endpoint_pattern,
            JOB_HYDRATE_EVENT_ROOT,
            MISSING_ROOT_RETIRE_LOOKBACK,
        )
        statuses: list[int] = []
        for row in rows:
            http_status = row["http_status"]
            if http_status is None:
                continue
            statuses.append(int(http_status))
        if len(statuses) < MISSING_ROOT_RETIRE_THRESHOLD:
            return False
        if any(status != 404 for status in statuses[:MISSING_ROOT_RETIRE_THRESHOLD]):
            return False
        return any(200 <= status < 300 for status in statuses[MISSING_ROOT_RETIRE_THRESHOLD :])


def _endpoint_for_edge_kind(edge_kind: str) -> SofascoreEndpoint | None:
    # F-1 (2026-05-08): "graph" was historically mapped to None despite being
    # listed in LIVE_DELTA_EDGE_KINDS for football/basketball/handball. The
    # F-2 freshness audit confirmed /event/{id}/graph snapshots only ever
    # update at bootstrap — never during live polling. Wiring it to
    # EVENT_GRAPH_ENDPOINT closes that silent gap so momentum data refreshes
    # at the same cadence as statistics/lineups/incidents.
    mapping = {
        "meta": None,
        "statistics": EVENT_STATISTICS_ENDPOINT,
        "lineups": EVENT_LINEUPS_ENDPOINT,
        "incidents": EVENT_INCIDENTS_ENDPOINT,
        "graph": EVENT_GRAPH_ENDPOINT,
    }
    return mapping.get(edge_kind)


def _endpoint_for_final_only_edge(edge_kind: str) -> SofascoreEndpoint | None:
    # F-3 (2026-05-08): edges that are only re-fetched at finalize, not during
    # live polling. Kept in a separate mapping so adding entries here cannot
    # accidentally re-enable them inside the live tick (which would happen if
    # they shared `_endpoint_for_edge_kind` and someone added them to
    # LIVE_DELTA_EDGE_KINDS by mistake).
    mapping = {
        "comments": EVENT_COMMENTS_ENDPOINT,
        "best_players_summary": EVENT_BEST_PLAYERS_SUMMARY_ENDPOINT,
        "h2h": EVENT_H2H_ENDPOINT,
        "pregame_form": EVENT_PREGAME_FORM_ENDPOINT,
    }
    return mapping.get(edge_kind)


def _endpoint_for_special_kind(special_kind: str) -> SofascoreEndpoint | None:
    mapping = {
        "best_players_summary": EVENT_BEST_PLAYERS_SUMMARY_ENDPOINT,
        "event_player_heatmap": EVENT_PLAYER_HEATMAP_ENDPOINT,
        "event_player_statistics": EVENT_PLAYER_STATISTICS_ENDPOINT,
        "event_player_rating_breakdown": EVENT_PLAYER_RATING_BREAKDOWN_ENDPOINT,
        "event_player_shotmap": EVENT_PLAYER_SHOTMAP_ENDPOINT,
        "event_goalkeeper_shotmap": EVENT_GOALKEEPER_SHOTMAP_ENDPOINT,
    }
    return mapping.get(special_kind)


def _endpoint_for_widget_job(params: dict[str, Any]) -> SofascoreEndpoint | None:
    widget_kind = params.get("widget_kind")
    if widget_kind == "top_players":
        return unique_tournament_top_players_endpoint(str(params["suffix"]))
    if widget_kind == "top_players_per_game":
        return unique_tournament_top_players_per_game_endpoint(str(params["suffix"]))
    if widget_kind == "top_teams":
        return unique_tournament_top_teams_endpoint(str(params["suffix"]))
    if widget_kind == "player_of_the_season":
        return UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT
    return None


def _entity_profile_targets(
    sport_slug: str,
    parse_results: list[object],
    *,
    seen: set[tuple[str, int]],
) -> tuple[tuple[SofascoreEndpoint, str, int], ...]:
    adapter = resolve_sport_adapter(sport_slug)
    if not adapter.hydrate_entity_profiles:
        return ()
    planned: list[tuple[SofascoreEndpoint, str, int]] = []
    for result in parse_results:
        for team in result.entity_upserts.get("team", ()):
            team_id = team.get("id")
            if isinstance(team_id, int) and ("team", team_id) not in seen:
                seen.add(("team", team_id))
                planned.append((TEAM_ENDPOINT, "team", team_id))
        for player in result.entity_upserts.get("player", ()):
            player_id = player.get("id")
            if isinstance(player_id, int) and ("player", player_id) not in seen:
                seen.add(("player", player_id))
                planned.append((PLAYER_ENDPOINT, "player", player_id))
        for manager in result.entity_upserts.get("manager", ()):
            manager_id = manager.get("id")
            if isinstance(manager_id, int) and ("manager", manager_id) not in seen:
                seen.add(("manager", manager_id))
                planned.append((MANAGER_ENDPOINT, "manager", manager_id))
    return tuple(planned)


def _entity_profile_freshness_fields(
    endpoint: SofascoreEndpoint,
    entity_type: str,
    entity_id: int,
    *,
    player_ttl_seconds: int,
) -> tuple[str | None, int | None]:
    normalized_type = str(entity_type or "").strip().lower()
    if normalized_type == "team" and endpoint.pattern == TEAM_ENDPOINT.pattern:
        return f"freshness:team:{int(entity_id)}", TEAM_PROFILE_FRESHNESS_TTL_SECONDS
    if normalized_type == "player" and endpoint.pattern == PLAYER_ENDPOINT.pattern:
        return _player_profile_freshness_key(entity_id), int(player_ttl_seconds)
    if normalized_type == "manager" and endpoint.pattern == MANAGER_ENDPOINT.pattern:
        return f"freshness:manager:{int(entity_id)}", MANAGER_PROFILE_FRESHNESS_TTL_SECONDS
    return None, None


def _event_detail_freshness_fields(
    endpoint: SofascoreEndpoint,
    path_params: dict[str, Any],
    *,
    sport_slug: str | None = None,
    status_phase: str | None = None,
) -> tuple[str | None, int | None]:
    """Resolve (freshness_key, ttl_seconds) for an event sub-endpoint.

    B1 Phase 0 (2026-05-16) extension: when ``sport_slug`` and
    ``status_phase`` are provided AND the
    ``endpoint_ttl_policy.resolve_endpoint_ttl`` matrix has a per-status
    entry for this endpoint, that TTL wins over the legacy global
    defaults. The freshness *key* shape stays unchanged so old keys keep
    working through the cutover — only the TTL gets a more granular
    value.

    Legacy callers that omit ``sport_slug`` / ``status_phase`` (older
    tests, the prefetched-replay path) keep the previous behaviour
    exactly: global 24h for static event details and 5min for player
    event details.
    """

    pattern = endpoint.pattern
    # B1 — try the per-endpoint × per-status matrix first.
    per_status_ttl: int | None = None
    if sport_slug and status_phase:
        try:
            per_status_ttl = _resolve_endpoint_ttl(
                sport_slug=sport_slug,
                endpoint_pattern=pattern,
                status_phase=status_phase,
            )
        except Exception:  # noqa: BLE001 — never block fetch on policy lookup
            logger.exception(
                "endpoint_ttl_policy.resolve_endpoint_ttl failed for pattern=%s sport=%s phase=%s",
                pattern,
                sport_slug,
                status_phase,
            )
            per_status_ttl = None

    if pattern in _EVENT_PLAYER_DETAIL_FRESHNESS_PATTERNS:
        event_id = _as_int(path_params.get("event_id"))
        player_id = _as_int(path_params.get("player_id"))
        if event_id is None or player_id is None:
            return None, None
        ttl = per_status_ttl if per_status_ttl is not None else EVENT_PLAYER_DETAIL_FRESHNESS_TTL_SECONDS
        return (
            f"freshness:event-player:{event_id}:{player_id}:{pattern}",
            ttl,
        )
    if pattern in _EVENT_STATIC_DETAIL_FRESHNESS_PATTERNS:
        if pattern == EVENT_H2H_EVENTS_ENDPOINT.pattern:
            custom_id = str(path_params.get("custom_id") or "").strip()
            if not custom_id:
                return None, None
            ttl = per_status_ttl if per_status_ttl is not None else EVENT_STATIC_DETAIL_FRESHNESS_TTL_SECONDS
            return (
                f"freshness:event-detail-custom:{custom_id}:{pattern}",
                ttl,
            )
        event_id = _as_int(path_params.get("event_id"))
        if event_id is None:
            return None, None
        ttl = per_status_ttl if per_status_ttl is not None else EVENT_STATIC_DETAIL_FRESHNESS_TTL_SECONDS
        return (
            f"freshness:event-detail:{event_id}:{pattern}",
            ttl,
        )
    # B1 — endpoints not in the legacy whitelist sets but covered by the
    # new per-status matrix (e.g. comments, lineups, statistics, incidents)
    # now get a freshness gate too. Without this branch, the new TTL
    # entries for those endpoints would be silently ignored.
    if per_status_ttl is not None:
        event_id = _as_int(path_params.get("event_id"))
        if event_id is None:
            return None, None
        return (
            f"freshness:event-detail:{event_id}:{pattern}",
            per_status_ttl,
        )
    return None, None


def _player_profile_freshness_key(player_id: int) -> str:
    return f"freshness:player:{int(player_id)}"


def _minutes_to_start(*, start_timestamp: object, now_ms: int) -> int | None:
    if not isinstance(start_timestamp, int):
        return None
    delta_seconds = start_timestamp - (now_ms // 1000)
    return int(delta_seconds // 60)


def _latest_snapshot_id(outcomes: list[FetchOutcomeEnvelope]) -> int | None:
    for outcome in reversed(outcomes):
        if outcome.snapshot_id is not None:
            return outcome.snapshot_id
    return None


def _latest_fetched_at(outcomes: list[FetchOutcomeEnvelope]) -> str | None:
    for outcome in reversed(outcomes):
        if outcome.fetched_at:
            return outcome.fetched_at
    return None


def _extract_baseball_at_bat_ids(payload: object) -> tuple[int, ...]:
    found: set[int] = set()

    def walk(value: object) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                normalized_key = str(key).strip().lower()
                if normalized_key == "atbatid":
                    at_bat_id = _as_int(child)
                    if at_bat_id is not None:
                        found.add(at_bat_id)
                elif normalized_key == "atbat" and isinstance(child, dict):
                    at_bat_id = _as_int(child.get("id"))
                    if at_bat_id is not None:
                        found.add(at_bat_id)
                walk(child)
            return
        if isinstance(value, (list, tuple)):
            for item in value:
                walk(item)

    walk(payload)
    return tuple(sorted(found))


def _extract_shotmap_player_ids(payload: object) -> tuple[int, ...]:
    return _extract_nested_person_ids(payload, role_key="player")


def _extract_shotmap_goalkeeper_ids(payload: object) -> tuple[int, ...]:
    return _extract_nested_person_ids(payload, role_key="goalkeeper")


def _extract_nested_person_ids(payload: object, *, role_key: str) -> tuple[int, ...]:
    found: set[int] = set()

    def walk(value: object) -> None:
        if isinstance(value, dict):
            nested = value.get(role_key)
            if isinstance(nested, dict):
                nested_id = _as_int(nested.get("id"))
                if nested_id is not None:
                    found.add(nested_id)
            for child in value.values():
                walk(child)
            return
        if isinstance(value, (list, tuple)):
            for item in value:
                walk(item)

    walk(payload)
    return tuple(sorted(found))


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit() or (stripped.startswith("-") and stripped[1:].isdigit()):
            return int(stripped)
    return None


def _inline_capability_rollup_enabled() -> bool:
    """Whether to write ``endpoint_capability_rollup`` from the live/hydrate
    hot path (legacy behaviour).

    Default is **off** to avoid the chronic deadlock / lock-wait storm caused
    by ~20 concurrent workers all read-modify-writing the same ~290-row PK
    space. Rollup state is rebuilt out-of-band via the
    ``rebuild-capability-rollup`` CLI command.

    Set ``SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED=1`` (or ``true``/``yes``/
    ``on``) in prod ``.env`` to restore the legacy inline path for debugging.
    """

    raw_value = os.getenv("SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED", "0")
    return str(raw_value).strip().lower() in {"1", "true", "yes", "on"}
