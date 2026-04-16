"""Pilot orchestrator wiring planner, fetch, raw snapshots, and normalization."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..endpoints import (
    EVENT_DETAIL_ENDPOINT,
    EVENT_LINEUPS_ENDPOINT,
    EVENT_POINT_BY_POINT_ENDPOINT,
    EVENT_TENNIS_POWER_ENDPOINT,
    SofascoreEndpoint,
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT,
    unique_tournament_top_players_endpoint,
    unique_tournament_top_players_per_game_endpoint,
    unique_tournament_top_teams_endpoint,
)
from ..fetch_models import FetchOutcomeEnvelope, FetchTask
from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_HYDRATE_EVENT_ROOT
from ..storage.capability_repository import CapabilityObservationRecord, CapabilityRollupRecord


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
    ) -> None:
        self.fetch_executor = fetch_executor
        self.snapshot_store = snapshot_store
        self.normalize_worker = normalize_worker
        self.planner = planner
        self.capability_repository = capability_repository
        self.sql_executor = sql_executor
        self._rollups: dict[tuple[str, str], CapabilityRollupAccumulator] = {}

    async def run_event(self, *, event_id: int, sport_slug: str) -> PilotRunReport:
        if self.fetch_executor is None:
            raise RuntimeError("fetch_executor is required for run_event")

        fetch_outcomes: list[FetchOutcomeEnvelope] = []
        parse_results: list[object] = []

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
        if root_parse is not None:
            event_rows = root_parse.entity_upserts.get("event", ())
            season_rows = root_parse.entity_upserts.get("season", ())
            unique_tournament_rows = root_parse.entity_upserts.get("unique_tournament", ())
            if event_rows:
                status_type = event_rows[0].get("status_type")
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
        for edge_job in self.planner.expand(root_job):
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
        "incidents": None,
        "graph": None,
    }
    return mapping.get(edge_kind)


def _special_endpoints_for_sport(sport_slug: str) -> tuple[SofascoreEndpoint, ...]:
    if sport_slug == "tennis":
        return (EVENT_POINT_BY_POINT_ENDPOINT, EVENT_TENNIS_POWER_ENDPOINT)
    return ()


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
