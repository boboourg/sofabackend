"""Pilot orchestrator wiring planner, fetch, raw snapshots, and normalization."""

from __future__ import annotations

import logging
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
    EVENT_PLAYER_HEATMAP_ENDPOINT,
    EVENT_PLAYER_SHOTMAP_ENDPOINT,
    EVENT_GOALKEEPER_SHOTMAP_ENDPOINT,
    EVENT_INCIDENTS_ENDPOINT,
    EVENT_LINEUPS_ENDPOINT,
    EVENT_PLAYER_RATING_BREAKDOWN_ENDPOINT,
    EVENT_PLAYER_STATISTICS_ENDPOINT,
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
from ..event_endpoint_static_denylist import is_static_dead_event_endpoint
from ..event_endpoint_negative_cache import normalize_event_status_phase
from ..fetch_models import FetchOutcomeEnvelope, FetchTask
from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_FINALIZE_EVENT, JOB_HYDRATE_EVENT_ROOT, JOB_TRACK_LIVE_EVENT
from ..match_center_policy import football_edge_allowed, football_special_allowed
from ..parsers.sports import resolve_sport_adapter
from ..storage.capability_repository import CapabilityObservationRecord, CapabilityRollupRecord
from ..storage.live_state_repository import EventLiveStateHistoryRecord, EventTerminalStateRecord
from ..workers.live_worker import LiveWorker

MISSING_ROOT_TERMINAL_STATUS = "not_found"
MISSING_ROOT_RETIRE_THRESHOLD = 3
MISSING_ROOT_RETIRE_LOOKBACK = 20
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
        event_endpoint_gate=None,
        live_bootstrap_coordinator=None,
        final_sweep_gate=None,
        freshness_store=None,
        player_profile_freshness_ttl_seconds: int = PLAYER_PROFILE_FRESHNESS_TTL_SECONDS,
    ) -> None:
        self.fetch_executor = fetch_executor
        self.snapshot_store = snapshot_store
        self.normalize_worker = normalize_worker
        self.planner = planner
        self.capability_repository = capability_repository
        self.sql_executor = sql_executor
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.live_worker = live_worker or LiveWorker(now_ms_factory=self.now_ms_factory)
        if hasattr(self.live_worker, "now_ms_factory"):
            self.live_worker.now_ms_factory = self.now_ms_factory
        self.live_state_store = live_state_store
        self.live_state_repository = live_state_repository
        self.stream_queue = stream_queue
        self.season_widget_gate = season_widget_gate
        self.event_endpoint_gate = event_endpoint_gate
        self.live_bootstrap_coordinator = live_bootstrap_coordinator
        self.final_sweep_gate = final_sweep_gate
        self.freshness_store = freshness_store
        self.player_profile_freshness_ttl_seconds = int(player_profile_freshness_ttl_seconds)
        self._rollups: dict[tuple[str, str], CapabilityRollupAccumulator] = {}
        self._pending_capability_records: list[DeferredCapabilityRecord] = []

    async def run_event(
        self,
        *,
        event_id: int,
        sport_slug: str,
        hydration_mode: str = "full",
    ) -> PilotRunReport:
        self._pending_capability_records.clear()
        if self.fetch_executor is None:
            raise RuntimeError("fetch_executor is required for run_event")

        requested_hydration_mode = str(hydration_mode or "full").strip().lower()
        effective_hydration_mode = requested_hydration_mode
        should_mark_live_bootstrap = False
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
            if season_rows:
                season_id = season_rows[0].get("id")
            if unique_tournament_rows:
                unique_tournament_row = unique_tournament_rows[0]
                unique_tournament_id = unique_tournament_row.get("id")
                tournament_tier = _as_int(unique_tournament_row.get("tier"))
                tournament_user_count = _as_int(unique_tournament_row.get("user_count"))
        status_phase = normalize_event_status_phase(status_type)

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
        for edge_job in planned_jobs:
            edge_kind = str(edge_job.params.get("edge_kind") or "")
            if not football_edge_allowed(
                sport_slug=sport_slug,
                edge_kind=edge_kind,
                detail_id=detail_id,
                status_type=status_type,
                has_xg=has_xg,
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
                if self.season_widget_gate is not None:
                    candidate_patterns = self.planner.plan_season_widget_patterns(
                        sport_slug,
                        unique_tournament_id=int(unique_tournament_id),
                        season_id=int(season_id),
                    )
                    blocked_widget_patterns = await self.season_widget_gate.blocked_endpoint_patterns(
                        sport_slug=sport_slug,
                        unique_tournament_id=int(unique_tournament_id),
                        season_id=int(season_id),
                        endpoint_patterns=candidate_patterns,
                    )
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

        for request_spec in build_event_detail_request_specs(
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
        ):
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
                )
                fetch_outcomes.extend(shotmap_outcomes)
                parse_results.extend(shotmap_parses)

        if should_mark_live_bootstrap and not finalized and root_outcome.classification in {"success_json", "success_empty_json"}:
            await self.live_bootstrap_coordinator.mark_bootstrapped(self.sql_executor, event_id=event_id)

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

    def replay_snapshot(self, snapshot_id: int):
        snapshot = self.snapshot_store.load_snapshot(snapshot_id)
        return self.normalize_worker.handle(snapshot)

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
        freshness_key, freshness_ttl_seconds = _event_detail_freshness_fields(endpoint, path_params)
        if freshness_key is not None and self._is_resource_fresh(freshness_key):
            logger.debug(
                "Skipping fresh event detail resource: event_id=%s endpoint=%s",
                context_event_id,
                endpoint.pattern,
            )
            return None, None
        decision = None
        if self.event_endpoint_gate is not None:
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

    async def _run_final_sweep(
        self,
        *,
        event_id: int,
        sport_slug: str,
        detail_id: int | None = None,
        status_type: str | None = None,
        has_xg: bool | None = None,
        status_phase: str = "unknown",
    ) -> tuple[list[FetchOutcomeEnvelope], list[object]]:
        outcomes: list[FetchOutcomeEnvelope] = []
        parses: list[object] = []
        adapter = resolve_sport_adapter(sport_slug)
        for edge_kind in adapter.core_event_edges:
            if not football_edge_allowed(
                sport_slug=sport_slug,
                edge_kind=edge_kind,
                detail_id=detail_id,
                status_type=status_type,
                has_xg=has_xg,
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
            outcome, parsed = await self._fetch_and_parse(
                endpoint=EVENT_BASEBALL_PITCHES_ENDPOINT,
                sport_slug=sport_slug,
                path_params={"event_id": event_id, "at_bat_id": at_bat_id},
                context_entity_type="event",
                context_entity_id=event_id,
                context_event_id=event_id,
                fetch_reason="hydrate_special_route",
            )
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
        self.planner.capability_rollup[outcome.endpoint_pattern] = rollup.support_level
        if self.capability_repository is None:
            return
        self._pending_capability_records.append(
            DeferredCapabilityRecord(
                observation=observation,
                rollup=rollup,
            )
        )

    async def _flush_capabilities(self) -> None:
        if self.capability_repository is None or not self._pending_capability_records:
            return

        observations = tuple(item.observation for item in self._pending_capability_records)
        latest_rollups: dict[tuple[str, str], CapabilityRollupRecord] = {}
        for item in self._pending_capability_records:
            latest_rollups[(item.rollup.sport_slug, item.rollup.endpoint_pattern)] = item.rollup

        for observation in observations:
            await self.capability_repository.insert_observation(self.sql_executor, observation)
        for rollup in latest_rollups.values():
            await self.capability_repository.upsert_rollup(self.sql_executor, rollup)

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
    mapping = {
        "meta": None,
        "statistics": EVENT_STATISTICS_ENDPOINT,
        "lineups": EVENT_LINEUPS_ENDPOINT,
        "incidents": EVENT_INCIDENTS_ENDPOINT,
        "graph": None,
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
) -> tuple[str | None, int | None]:
    pattern = endpoint.pattern
    if pattern in _EVENT_PLAYER_DETAIL_FRESHNESS_PATTERNS:
        event_id = _as_int(path_params.get("event_id"))
        player_id = _as_int(path_params.get("player_id"))
        if event_id is None or player_id is None:
            return None, None
        return (
            f"freshness:event-player:{event_id}:{player_id}:{pattern}",
            EVENT_PLAYER_DETAIL_FRESHNESS_TTL_SECONDS,
        )
    if pattern in _EVENT_STATIC_DETAIL_FRESHNESS_PATTERNS:
        if pattern == EVENT_H2H_EVENTS_ENDPOINT.pattern:
            custom_id = str(path_params.get("custom_id") or "").strip()
            if not custom_id:
                return None, None
            return (
                f"freshness:event-detail-custom:{custom_id}:{pattern}",
                EVENT_STATIC_DETAIL_FRESHNESS_TTL_SECONDS,
            )
        event_id = _as_int(path_params.get("event_id"))
        if event_id is None:
            return None, None
        return (
            f"freshness:event-detail:{event_id}:{pattern}",
            EVENT_STATIC_DETAIL_FRESHNESS_TTL_SECONDS,
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
