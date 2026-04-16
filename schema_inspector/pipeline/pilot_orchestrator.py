"""Pilot orchestrator wiring planner, fetch, raw snapshots, and normalization."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from ..endpoints import (
    EVENT_BASEBALL_INNINGS_ENDPOINT,
    EVENT_DETAIL_ENDPOINT,
    EVENT_ESPORTS_GAMES_ENDPOINT,
    EVENT_INCIDENTS_ENDPOINT,
    EVENT_LINEUPS_ENDPOINT,
    EVENT_POINT_BY_POINT_ENDPOINT,
    EVENT_SHOTMAP_ENDPOINT,
    EVENT_TENNIS_POWER_ENDPOINT,
    MANAGER_ENDPOINT,
    PLAYER_ENDPOINT,
    SofascoreEndpoint,
    TEAM_ENDPOINT,
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT,
    unique_tournament_top_players_endpoint,
    unique_tournament_top_players_per_game_endpoint,
    unique_tournament_top_teams_endpoint,
)
from ..fetch_models import FetchOutcomeEnvelope, FetchTask
from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_FINALIZE_EVENT, JOB_HYDRATE_EVENT_ROOT, JOB_TRACK_LIVE_EVENT
from ..parsers.sports import resolve_sport_adapter
from ..storage.capability_repository import CapabilityObservationRecord, CapabilityRollupRecord
from ..workers.live_worker import LiveWorker


EVENT_STATISTICS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/statistics",
    envelope_key="statistics",
    target_table="event_statistic",
)


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
        stream_queue=None,
        now_ms_factory=None,
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
        self.stream_queue = stream_queue
        self._rollups: dict[tuple[str, str], CapabilityRollupAccumulator] = {}

    async def run_event(self, *, event_id: int, sport_slug: str) -> PilotRunReport:
        if self.fetch_executor is None:
            raise RuntimeError("fetch_executor is required for run_event")

        fetch_outcomes: list[FetchOutcomeEnvelope] = []
        parse_results: list[object] = []
        live_lane: str | None = None
        live_stream: str | None = None
        finalized = False

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
        if root_parse is not None:
            parse_results.append(root_parse)

        status_type = None
        season_id = None
        unique_tournament_id = None
        start_timestamp = None
        if root_parse is not None:
            event_rows = root_parse.entity_upserts.get("event", ())
            season_rows = root_parse.entity_upserts.get("season", ())
            unique_tournament_rows = root_parse.entity_upserts.get("unique_tournament", ())
            if event_rows:
                status_type = event_rows[0].get("status_type")
                start_timestamp = event_rows[0].get("start_timestamp")
            if season_rows:
                season_id = season_rows[0].get("id")
            if unique_tournament_rows:
                unique_tournament_id = unique_tournament_rows[0].get("id")

        root_job = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug=sport_slug,
            entity_type="event",
            entity_id=event_id,
            scope="pilot",
            params={"status_type": status_type},
            priority=0,
            trace_id=f"pilot:{sport_slug}:{event_id}",
        )
        planned_jobs = self.planner.expand(root_job)
        for edge_job in planned_jobs:
            edge_kind = str(edge_job.params.get("edge_kind") or "")
            endpoint = _endpoint_for_edge_kind(edge_kind)
            if endpoint is None:
                continue
            outcome, parsed = await self._fetch_and_parse(
                endpoint=endpoint,
                sport_slug=sport_slug,
                path_params={"event_id": event_id},
                context_entity_type="event",
                context_entity_id=event_id,
                context_event_id=event_id,
                fetch_reason=edge_job.job_type,
            )
            fetch_outcomes.append(outcome)
            if parsed is not None:
                parse_results.append(parsed)

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
                    live_state_store=self.live_state_store,
                    stream_queue=self.stream_queue,
                )
                live_lane = track_result.decision.lane
                live_stream = track_result.stream
                tracked_live = True
            if planned_job.job_type == JOB_FINALIZE_EVENT:
                final_outcomes, final_parses = await self._run_final_sweep(
                    event_id=event_id,
                    sport_slug=sport_slug,
                )
                fetch_outcomes.extend(final_outcomes)
                parse_results.extend(final_parses)
                self.live_worker.finalize_event(
                    sport_slug=sport_slug,
                    event_id=event_id,
                    status_type=status_type,
                    live_state_store=self.live_state_store,
                )
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
                live_state_store=self.live_state_store,
                stream_queue=self.stream_queue,
            )
            live_lane = track_result.decision.lane
            live_stream = track_result.stream

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
                outcome, parsed = await self._fetch_and_parse(
                    endpoint=entity_endpoint,
                    sport_slug=sport_slug,
                    path_params={f"{entity_type}_id": entity_id},
                    context_entity_type=entity_type,
                    context_entity_id=entity_id,
                    context_event_id=event_id,
                    fetch_reason="hydrate_entity_profile",
                )
                fetch_outcomes.append(outcome)
                if parsed is not None:
                    parse_results.append(parsed)

        for endpoint in _special_endpoints_for_sport(sport_slug):
            outcome, parsed = await self._fetch_and_parse(
                endpoint=endpoint,
                sport_slug=sport_slug,
                path_params={"event_id": event_id},
                context_entity_type="event",
                context_entity_id=event_id,
                context_event_id=event_id,
                fetch_reason="hydrate_special_route",
            )
            fetch_outcomes.append(outcome)
            if parsed is not None:
                parse_results.append(parsed)

        if unique_tournament_id is not None and season_id is not None:
            for widget_job in self.planner.plan_season_widgets(
                sport_slug,
                unique_tournament_id=int(unique_tournament_id),
                season_id=int(season_id),
            ):
                endpoint = _endpoint_for_widget_job(widget_job.params)
                if endpoint is None:
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
                fetch_outcomes.append(outcome)
                if parsed is not None:
                    parse_results.append(parsed)

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
        )
        outcome = await self.fetch_executor.execute(task)
        await self._record_capability(sport_slug=sport_slug, outcome=outcome, context_type=context_entity_type)

        parsed = None
        if outcome.snapshot_id is not None:
            snapshot = self.snapshot_store.load_snapshot(outcome.snapshot_id)
            parsed = self.normalize_worker.handle(snapshot)
        return outcome, parsed

    async def _run_final_sweep(
        self,
        *,
        event_id: int,
        sport_slug: str,
    ) -> tuple[list[FetchOutcomeEnvelope], list[object]]:
        outcomes: list[FetchOutcomeEnvelope] = []
        parses: list[object] = []
        adapter = resolve_sport_adapter(sport_slug)
        for edge_kind in adapter.core_event_edges:
            endpoint = _endpoint_for_edge_kind(edge_kind)
            if endpoint is None:
                continue
            outcome, parsed = await self._fetch_and_parse(
                endpoint=endpoint,
                sport_slug=sport_slug,
                path_params={"event_id": event_id},
                context_entity_type="event",
                context_entity_id=event_id,
                context_event_id=event_id,
                fetch_reason=JOB_FINALIZE_EVENT,
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
        if self.capability_repository is None:
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
        await self.capability_repository.insert_observation(self.sql_executor, observation)

        key = (sport_slug, outcome.endpoint_pattern)
        accumulator = self._rollups.setdefault(
            key,
            CapabilityRollupAccumulator(sport_slug=sport_slug, endpoint_pattern=outcome.endpoint_pattern),
        )
        rollup = accumulator.observe(outcome)
        await self.capability_repository.upsert_rollup(self.sql_executor, rollup)
        self.planner.capability_rollup[outcome.endpoint_pattern] = rollup.support_level


def _endpoint_for_edge_kind(edge_kind: str) -> SofascoreEndpoint | None:
    mapping = {
        "meta": None,
        "statistics": EVENT_STATISTICS_ENDPOINT,
        "lineups": EVENT_LINEUPS_ENDPOINT,
        "incidents": EVENT_INCIDENTS_ENDPOINT,
        "graph": None,
    }
    return mapping.get(edge_kind)


def _special_endpoints_for_sport(sport_slug: str) -> tuple[SofascoreEndpoint, ...]:
    adapter = resolve_sport_adapter(sport_slug)
    family_map = {
        "tennis_point_by_point": EVENT_POINT_BY_POINT_ENDPOINT,
        "tennis_power": EVENT_TENNIS_POWER_ENDPOINT,
        "baseball_innings": EVENT_BASEBALL_INNINGS_ENDPOINT,
        "shotmap": EVENT_SHOTMAP_ENDPOINT,
        "esports_games": EVENT_ESPORTS_GAMES_ENDPOINT,
    }
    return tuple(
        family_map[family]
        for family in adapter.special_families
        if family in family_map
    )


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


def _minutes_to_start(*, start_timestamp: object, now_ms: int) -> int | None:
    if not isinstance(start_timestamp, int):
        return None
    delta_seconds = start_timestamp - (now_ms // 1000)
    return int(delta_seconds // 60)
