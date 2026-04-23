"""Season-widget negative cache policy and gate helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import os
from typing import Any

from .endpoints import (
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT,
    unique_tournament_top_players_endpoint,
    unique_tournament_top_players_per_game_endpoint,
    unique_tournament_top_teams_endpoint,
)
from .runtime import _env_bool, _load_project_env


CLASSIFICATION_C_PROBATION = "c_probation"
CLASSIFICATION_B_STRUCTURAL = "b_structural"
CLASSIFICATION_MIXED_BY_SEASON = "mixed_by_season"
CLASSIFICATION_SUPPORTED_SEASON = "supported_season"

SCOPE_TOURNAMENT = "tournament"
SCOPE_SEASON = "season"

MODE_OFF = "off"
MODE_SHADOW = "shadow"
MODE_ENFORCE = "enforce"

SEASONAL_BYPASS_BEFORE_END = timedelta(days=3)
SEASONAL_BYPASS_AFTER_END = timedelta(days=14)

_C_PROBATION_BASE_INTERVALS = (
    timedelta(hours=6),
    timedelta(hours=24),
    timedelta(hours=72),
)
_B_STRUCTURAL_BASE_INTERVALS = (
    timedelta(days=7),
    timedelta(days=21),
    timedelta(days=60),
)


@dataclass(frozen=True)
class NegativeCacheSettings:
    mode: str = MODE_ENFORCE

    @property
    def enabled(self) -> bool:
        return self.mode != MODE_OFF

    @property
    def shadow(self) -> bool:
        return self.mode == MODE_SHADOW

    @property
    def enforce(self) -> bool:
        return self.mode == MODE_ENFORCE


@dataclass(frozen=True)
class NegativeCacheState:
    scope_kind: str
    unique_tournament_id: int
    season_id: int | None
    endpoint_pattern: str
    classification: str
    first_404_at: datetime | None
    last_404_at: datetime | None
    first_200_at: datetime | None
    last_200_at: datetime | None
    seen_404_season_ids: tuple[int, ...]
    seen_200_season_ids: tuple[int, ...]
    suppressed_hits_total: int
    actual_probe_total: int
    recheck_iteration: int
    next_probe_after: datetime | None
    probe_lease_until: datetime | None
    probe_lease_owner: str | None
    last_http_status: int | None
    last_job_type: str | None
    last_trace_id: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ProbeObservation:
    unique_tournament_id: int
    season_id: int | None
    endpoint_pattern: str
    observed_at: datetime
    http_status: int
    next_probe_after: datetime | None
    job_type: str = "sync_season_widget"
    trace_id: str | None = None


@dataclass(frozen=True)
class WidgetGateDecision:
    should_fetch: bool
    endpoint_pattern: str
    scope_kind: str
    decision_type: str
    classification_before: str | None
    unique_tournament_id: int
    season_id: int | None


@dataclass(frozen=True)
class WidgetAvailabilityEvent:
    unique_tournament_id: int
    season_id: int | None
    endpoint_pattern: str
    scope_kind: str
    decision: str
    observed_at: datetime
    job_type: str
    trace_id: str | None
    http_status: int | None = None
    proxy_id: str | None = None
    source_url: str | None = None
    classification_before: str | None = None
    probe_latency_ms: int | None = None
    note: str | None = None


def load_negative_cache_settings(env: dict[str, str] | None = None) -> NegativeCacheSettings:
    resolved_env = _load_project_env() if env is None else env
    if not _env_bool(resolved_env, "SCHEMA_INSPECTOR_NEGATIVE_CACHE_ENABLED", True):
        return NegativeCacheSettings(mode=MODE_OFF)
    raw_mode = str(resolved_env.get("SCHEMA_INSPECTOR_NEGATIVE_CACHE_MODE", MODE_ENFORCE)).strip().lower()
    if raw_mode not in {MODE_OFF, MODE_SHADOW, MODE_ENFORCE}:
        raw_mode = MODE_ENFORCE
    return NegativeCacheSettings(mode=raw_mode)


def widget_endpoint_pattern_from_params(params: dict[str, Any]) -> str | None:
    widget_kind = params.get("widget_kind")
    if widget_kind == "top_players":
        return unique_tournament_top_players_endpoint(str(params["suffix"])).pattern
    if widget_kind == "top_players_per_game":
        return unique_tournament_top_players_per_game_endpoint(str(params["suffix"])).pattern
    if widget_kind == "top_teams":
        return unique_tournament_top_teams_endpoint(str(params["suffix"])).pattern
    if widget_kind == "player_of_the_season":
        return UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT.pattern
    return None


def is_seasonal_widget_pattern(endpoint_pattern: str) -> bool:
    normalized = str(endpoint_pattern or "").strip().lower()
    return "top-players" in normalized or "top-teams" in normalized or "player-of-the-season" in normalized


def is_seasonal_widget_bypass_active(
    endpoint_pattern: str,
    *,
    now: datetime,
    season_end_at: datetime | None,
) -> bool:
    if season_end_at is None or not is_seasonal_widget_pattern(endpoint_pattern):
        return False
    return (season_end_at - SEASONAL_BYPASS_BEFORE_END) <= now <= (season_end_at + SEASONAL_BYPASS_AFTER_END)


def reduce_negative_cache_state(
    current: NegativeCacheState | None,
    observation: ProbeObservation,
) -> NegativeCacheState | tuple[NegativeCacheState, NegativeCacheState | None]:
    observed_at = observation.observed_at
    season_id = int(observation.season_id) if observation.season_id is not None else None
    if observation.http_status == 200:
        tournament_created_at = current.created_at if current is not None else observed_at
        tournament_404_seasons = current.seen_404_season_ids if current is not None else ()
        tournament_200_seasons = _merge_sorted_ids(
            current.seen_200_season_ids if current is not None else (),
            season_id,
        )
        tournament_state = NegativeCacheState(
            scope_kind=SCOPE_TOURNAMENT,
            unique_tournament_id=int(observation.unique_tournament_id),
            season_id=None,
            endpoint_pattern=observation.endpoint_pattern,
            classification=CLASSIFICATION_MIXED_BY_SEASON,
            first_404_at=current.first_404_at if current is not None else None,
            last_404_at=current.last_404_at if current is not None else None,
            first_200_at=current.first_200_at if current is not None else observed_at,
            last_200_at=observed_at,
            seen_404_season_ids=tournament_404_seasons,
            seen_200_season_ids=tournament_200_seasons,
            suppressed_hits_total=current.suppressed_hits_total if current is not None else 0,
            actual_probe_total=(current.actual_probe_total if current is not None else 0) + 1,
            recheck_iteration=0,
            next_probe_after=None,
            probe_lease_until=None,
            probe_lease_owner=None,
            last_http_status=200,
            last_job_type=observation.job_type,
            last_trace_id=observation.trace_id,
            created_at=tournament_created_at,
            updated_at=observed_at,
        )
        if season_id is None:
            return tournament_state, None
        season_state = NegativeCacheState(
            scope_kind=SCOPE_SEASON,
            unique_tournament_id=int(observation.unique_tournament_id),
            season_id=season_id,
            endpoint_pattern=observation.endpoint_pattern,
            classification=CLASSIFICATION_SUPPORTED_SEASON,
            first_404_at=None,
            last_404_at=None,
            first_200_at=observed_at,
            last_200_at=observed_at,
            seen_404_season_ids=(),
            seen_200_season_ids=(season_id,),
            suppressed_hits_total=0,
            actual_probe_total=1,
            recheck_iteration=0,
            next_probe_after=None,
            probe_lease_until=None,
            probe_lease_owner=None,
            last_http_status=200,
            last_job_type=observation.job_type,
            last_trace_id=observation.trace_id,
            created_at=observed_at,
            updated_at=observed_at,
        )
        return tournament_state, season_state

    prior_404_ids = current.seen_404_season_ids if current is not None else ()
    merged_404_ids = _merge_sorted_ids(prior_404_ids, season_id)
    merged_200_ids = current.seen_200_season_ids if current is not None else ()
    classification = CLASSIFICATION_C_PROBATION
    if current is not None and current.scope_kind == SCOPE_SEASON:
        classification = CLASSIFICATION_C_PROBATION
    elif len(merged_404_ids) >= 2 and not merged_200_ids:
        classification = CLASSIFICATION_B_STRUCTURAL

    return NegativeCacheState(
        scope_kind=current.scope_kind if current is not None else SCOPE_TOURNAMENT,
        unique_tournament_id=int(observation.unique_tournament_id),
        season_id=current.season_id if current is not None else None,
        endpoint_pattern=observation.endpoint_pattern,
        classification=classification,
        first_404_at=current.first_404_at if current is not None else observed_at,
        last_404_at=observed_at,
        first_200_at=current.first_200_at if current is not None else None,
        last_200_at=current.last_200_at if current is not None else None,
        seen_404_season_ids=merged_404_ids,
        seen_200_season_ids=merged_200_ids,
        suppressed_hits_total=current.suppressed_hits_total if current is not None else 0,
        actual_probe_total=(current.actual_probe_total if current is not None else 0) + 1,
        recheck_iteration=((current.recheck_iteration if current is not None else 0) + 1),
        next_probe_after=observation.next_probe_after,
        probe_lease_until=None,
        probe_lease_owner=None,
        last_http_status=observation.http_status,
        last_job_type=observation.job_type,
        last_trace_id=observation.trace_id,
        created_at=current.created_at if current is not None else observed_at,
        updated_at=observed_at,
    )


def next_probe_after_for_state(
    *,
    state: NegativeCacheState | None,
    next_classification: str,
    observed_at: datetime,
    unique_tournament_id: int,
    season_id: int | None,
    endpoint_pattern: str,
) -> datetime | None:
    if next_classification in {CLASSIFICATION_MIXED_BY_SEASON, CLASSIFICATION_SUPPORTED_SEASON}:
        return None
    previous_iteration = state.recheck_iteration if state is not None else 0
    if next_classification == CLASSIFICATION_B_STRUCTURAL:
        sequence = _B_STRUCTURAL_BASE_INTERVALS
        step = max(previous_iteration, 0)
    else:
        sequence = _C_PROBATION_BASE_INTERVALS
        step = max(previous_iteration, 0)
    interval = sequence[min(step, len(sequence) - 1)]
    multiplier = _deterministic_jitter_multiplier(
        unique_tournament_id=unique_tournament_id,
        season_id=season_id,
        endpoint_pattern=endpoint_pattern,
        step=step,
    )
    return observed_at + (interval * multiplier)


def parse_observed_at(value: str | None, *, fallback: datetime | None = None) -> datetime:
    if value:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass
    return fallback or datetime.now(UTC)


class SeasonWidgetNegativeCache:
    def __init__(
        self,
        *,
        repository,
        sql_executor,
        now_factory=None,
        settings: NegativeCacheSettings | None = None,
        lease_owner: str | None = None,
    ) -> None:
        self.repository = repository
        self.sql_executor = sql_executor
        self.now_factory = now_factory or (lambda: datetime.now(UTC))
        self.settings = settings or NegativeCacheSettings()
        self.lease_owner = lease_owner or f"widget-negcache:{os.getpid()}:{id(self)}"
        self._events: list[WidgetAvailabilityEvent] = []
        self._coarsely_blocked_patterns: set[str] = set()
        self._fetched_patterns: set[str] = set()
        self._tournament_states: dict[tuple[int, str], NegativeCacheState] = {}
        self._season_states: dict[tuple[int, int, str], NegativeCacheState] = {}
        self._season_end_at_by_key: dict[tuple[int, int], datetime | None] = {}

    @property
    def events(self) -> tuple[WidgetAvailabilityEvent, ...]:
        return tuple(self._events)

    async def blocked_endpoint_patterns(
        self,
        *,
        sport_slug: str,
        unique_tournament_id: int,
        season_id: int,
        endpoint_patterns,
    ) -> tuple[str, ...]:
        del sport_slug
        if not self.settings.enabled or not endpoint_patterns:
            return ()
        normalized_patterns = tuple(str(pattern) for pattern in endpoint_patterns if str(pattern))
        if not normalized_patterns:
            return ()
        await self._prime_tournament_states(
            unique_tournament_id=unique_tournament_id,
            endpoint_patterns=normalized_patterns,
        )
        blocked: list[str] = []
        now = self.now_factory()
        for endpoint_pattern in normalized_patterns:
            state = self._tournament_states.get((int(unique_tournament_id), endpoint_pattern))
            seasonal_bypass = await self._seasonal_bypass_active(
                unique_tournament_id=int(unique_tournament_id),
                season_id=int(season_id),
                endpoint_pattern=endpoint_pattern,
                now=now,
            )
            if not self._is_coarsely_suppressed(
                unique_tournament_id=int(unique_tournament_id),
                season_id=int(season_id),
                endpoint_pattern=endpoint_pattern,
                state=state,
                now=now,
                seasonal_bypass=seasonal_bypass,
            ):
                continue
            if state is not None:
                self._events.append(
                    WidgetAvailabilityEvent(
                        unique_tournament_id=int(unique_tournament_id),
                        season_id=int(season_id),
                        endpoint_pattern=endpoint_pattern,
                        scope_kind=SCOPE_TOURNAMENT,
                        decision="shadow_suppress" if self.settings.shadow else "suppressed",
                        observed_at=now,
                        job_type="sync_season_widget",
                        trace_id=None,
                        classification_before=state.classification,
                        note="coarse_structural_shadow" if self.settings.shadow else "coarse_structural_suppress",
                    )
                )
            if self.settings.enforce:
                self._coarsely_blocked_patterns.add(endpoint_pattern)
                blocked.append(endpoint_pattern)
        return tuple(sorted(set(blocked)))

    async def coarse_filter_widget_jobs(
        self,
        *,
        sport_slug: str,
        unique_tournament_id: int,
        season_id: int,
        jobs,
    ):
        if not self.settings.enabled or not jobs:
            return tuple(jobs)
        blocked = set(
            await self.blocked_endpoint_patterns(
                sport_slug=sport_slug,
                unique_tournament_id=int(unique_tournament_id),
                season_id=int(season_id),
                endpoint_patterns=tuple(
                    pattern
                    for job in jobs
                    for pattern in (str(job.params.get("_endpoint_pattern") or ""),)
                    if pattern
                ),
            )
        )
        if not blocked:
            return tuple(jobs)
        return tuple(job for job in jobs if str(job.params.get("_endpoint_pattern") or "") not in blocked)

    async def decide_widget_probe(
        self,
        *,
        sport_slug: str,
        unique_tournament_id: int,
        season_id: int,
        widget_job,
        endpoint_pattern: str,
    ) -> WidgetGateDecision:
        del sport_slug, widget_job
        if not self.settings.enabled:
            return WidgetGateDecision(
                should_fetch=True,
                endpoint_pattern=endpoint_pattern,
                scope_kind=SCOPE_TOURNAMENT,
                decision_type="probe",
                classification_before=None,
                unique_tournament_id=int(unique_tournament_id),
                season_id=int(season_id),
            )

        now = self.now_factory()
        await self._prime_tournament_states(
            unique_tournament_id=int(unique_tournament_id),
            endpoint_patterns=(endpoint_pattern,),
        )
        tournament_state = self._tournament_states.get((int(unique_tournament_id), endpoint_pattern))
        seasonal_bypass = await self._seasonal_bypass_active(
            unique_tournament_id=int(unique_tournament_id),
            season_id=int(season_id),
            endpoint_pattern=endpoint_pattern,
            now=now,
        )
        if tournament_state is None:
            self._fetched_patterns.add(endpoint_pattern)
            return WidgetGateDecision(
                should_fetch=True,
                endpoint_pattern=endpoint_pattern,
                scope_kind=SCOPE_TOURNAMENT,
                decision_type="bypass_probe" if seasonal_bypass else "probe",
                classification_before=None,
                unique_tournament_id=int(unique_tournament_id),
                season_id=int(season_id),
            )

        if tournament_state.classification == CLASSIFICATION_MIXED_BY_SEASON:
            await self._prime_season_states(
                unique_tournament_id=int(unique_tournament_id),
                season_id=int(season_id),
                endpoint_patterns=(endpoint_pattern,),
            )
            season_state = self._season_states.get((int(unique_tournament_id), int(season_id), endpoint_pattern))
            if season_state is not None and self._is_suppressed_by_state(
                state=season_state,
                endpoint_pattern=endpoint_pattern,
                now=now,
                seasonal_bypass=seasonal_bypass,
            ):
                if self.settings.shadow:
                    return self._shadow_probe_decision(
                        unique_tournament_id=int(unique_tournament_id),
                        season_id=int(season_id),
                        endpoint_pattern=endpoint_pattern,
                        scope_kind=SCOPE_SEASON,
                        classification_before=season_state.classification,
                        seasonal_bypass=seasonal_bypass,
                        note="season_scope_shadow_suppress",
                    )
                self._events.append(
                    WidgetAvailabilityEvent(
                        unique_tournament_id=int(unique_tournament_id),
                        season_id=int(season_id),
                        endpoint_pattern=endpoint_pattern,
                        scope_kind=SCOPE_SEASON,
                        decision="suppressed",
                        observed_at=now,
                        job_type="sync_season_widget",
                        trace_id=None,
                        classification_before=season_state.classification,
                        note="season_scope_cooldown",
                    )
                )
                return WidgetGateDecision(
                    should_fetch=False,
                    endpoint_pattern=endpoint_pattern,
                    scope_kind=SCOPE_SEASON,
                    decision_type="suppressed",
                    classification_before=season_state.classification,
                    unique_tournament_id=int(unique_tournament_id),
                    season_id=int(season_id),
                )
            if (
                season_state is not None
                and not seasonal_bypass
                and season_state.next_probe_after is not None
                and season_state.next_probe_after <= now
            ):
                acquired = await self.repository.try_acquire_probe_lease(
                    self.sql_executor,
                    scope_kind=SCOPE_SEASON,
                    unique_tournament_id=int(unique_tournament_id),
                    season_id=int(season_id),
                    endpoint_pattern=endpoint_pattern,
                    lease_owner=self.lease_owner,
                    now=now,
                )
                if not acquired:
                    if self.settings.shadow:
                        return self._shadow_probe_decision(
                            unique_tournament_id=int(unique_tournament_id),
                            season_id=int(season_id),
                            endpoint_pattern=endpoint_pattern,
                            scope_kind=SCOPE_SEASON,
                            classification_before=season_state.classification,
                            seasonal_bypass=seasonal_bypass,
                            note="season_scope_shadow_lease_blocked",
                        )
                    self._events.append(
                        WidgetAvailabilityEvent(
                            unique_tournament_id=int(unique_tournament_id),
                            season_id=int(season_id),
                            endpoint_pattern=endpoint_pattern,
                            scope_kind=SCOPE_SEASON,
                            decision="lease_blocked",
                            observed_at=now,
                            job_type="sync_season_widget",
                            trace_id=None,
                            classification_before=season_state.classification,
                            note="season_scope_lease_blocked",
                        )
                    )
                    return WidgetGateDecision(
                        should_fetch=False,
                        endpoint_pattern=endpoint_pattern,
                        scope_kind=SCOPE_SEASON,
                        decision_type="lease_blocked",
                        classification_before=season_state.classification,
                        unique_tournament_id=int(unique_tournament_id),
                        season_id=int(season_id),
                    )
            self._fetched_patterns.add(endpoint_pattern)
            return WidgetGateDecision(
                should_fetch=True,
                endpoint_pattern=endpoint_pattern,
                scope_kind=SCOPE_SEASON,
                decision_type="bypass_probe" if seasonal_bypass else "probe",
                classification_before=season_state.classification if season_state is not None else tournament_state.classification,
                unique_tournament_id=int(unique_tournament_id),
                season_id=int(season_id),
            )

        if self._is_suppressed_by_state(
            state=tournament_state,
            endpoint_pattern=endpoint_pattern,
            now=now,
            seasonal_bypass=seasonal_bypass,
        ):
            if self.settings.shadow:
                return self._shadow_probe_decision(
                    unique_tournament_id=int(unique_tournament_id),
                    season_id=int(season_id),
                    endpoint_pattern=endpoint_pattern,
                    scope_kind=SCOPE_TOURNAMENT,
                    classification_before=tournament_state.classification,
                    seasonal_bypass=seasonal_bypass,
                    note="tournament_scope_shadow_suppress",
                )
            self._events.append(
                WidgetAvailabilityEvent(
                    unique_tournament_id=int(unique_tournament_id),
                    season_id=int(season_id),
                    endpoint_pattern=endpoint_pattern,
                    scope_kind=SCOPE_TOURNAMENT,
                    decision="suppressed",
                    observed_at=now,
                    job_type="sync_season_widget",
                    trace_id=None,
                    classification_before=tournament_state.classification,
                    note="tournament_scope_cooldown",
                )
            )
            return WidgetGateDecision(
                should_fetch=False,
                endpoint_pattern=endpoint_pattern,
                scope_kind=SCOPE_TOURNAMENT,
                decision_type="suppressed",
                classification_before=tournament_state.classification,
                unique_tournament_id=int(unique_tournament_id),
                season_id=int(season_id),
            )

        if (
            not seasonal_bypass
            and tournament_state.next_probe_after is not None
            and tournament_state.next_probe_after <= now
        ):
            acquired = await self.repository.try_acquire_probe_lease(
                self.sql_executor,
                scope_kind=SCOPE_TOURNAMENT,
                unique_tournament_id=int(unique_tournament_id),
                season_id=None,
                endpoint_pattern=endpoint_pattern,
                lease_owner=self.lease_owner,
                now=now,
            )
            if not acquired:
                if self.settings.shadow:
                    return self._shadow_probe_decision(
                        unique_tournament_id=int(unique_tournament_id),
                        season_id=int(season_id),
                        endpoint_pattern=endpoint_pattern,
                        scope_kind=SCOPE_TOURNAMENT,
                        classification_before=tournament_state.classification,
                        seasonal_bypass=seasonal_bypass,
                        note="tournament_scope_shadow_lease_blocked",
                    )
                self._events.append(
                    WidgetAvailabilityEvent(
                        unique_tournament_id=int(unique_tournament_id),
                        season_id=int(season_id),
                        endpoint_pattern=endpoint_pattern,
                        scope_kind=SCOPE_TOURNAMENT,
                        decision="lease_blocked",
                        observed_at=now,
                        job_type="sync_season_widget",
                        trace_id=None,
                        classification_before=tournament_state.classification,
                        note="tournament_scope_lease_blocked",
                    )
                )
                return WidgetGateDecision(
                    should_fetch=False,
                    endpoint_pattern=endpoint_pattern,
                    scope_kind=SCOPE_TOURNAMENT,
                    decision_type="lease_blocked",
                    classification_before=tournament_state.classification,
                    unique_tournament_id=int(unique_tournament_id),
                    season_id=int(season_id),
                )

        self._fetched_patterns.add(endpoint_pattern)
        return WidgetGateDecision(
            should_fetch=True,
            endpoint_pattern=endpoint_pattern,
            scope_kind=SCOPE_TOURNAMENT,
            decision_type="bypass_probe" if seasonal_bypass else "probe",
            classification_before=tournament_state.classification,
            unique_tournament_id=int(unique_tournament_id),
            season_id=int(season_id),
        )

    async def record_widget_outcome(
        self,
        *,
        decision: WidgetGateDecision,
        endpoint_pattern: str,
        outcome,
    ) -> None:
        if not self.settings.enabled or not decision.should_fetch:
            return
        self._events.append(
            WidgetAvailabilityEvent(
                unique_tournament_id=decision.unique_tournament_id,
                season_id=decision.season_id,
                endpoint_pattern=endpoint_pattern,
                scope_kind=decision.scope_kind,
                decision=decision.decision_type,
                observed_at=parse_observed_at(getattr(outcome, "fetched_at", None), fallback=self.now_factory()),
                job_type="sync_season_widget",
                trace_id=getattr(outcome, "trace_id", None),
                http_status=getattr(outcome, "http_status", None),
                proxy_id=getattr(outcome, "proxy_id", None),
                source_url=getattr(outcome, "source_url", None),
                classification_before=decision.classification_before,
            )
        )

    def build_replay_gate(self):
        return ReplaySeasonWidgetNegativeCache(
            blocked_patterns=tuple(sorted(self._coarsely_blocked_patterns)),
            fetched_patterns=tuple(sorted(self._fetched_patterns)),
        )

    def _shadow_probe_decision(
        self,
        *,
        unique_tournament_id: int,
        season_id: int,
        endpoint_pattern: str,
        scope_kind: str,
        classification_before: str | None,
        seasonal_bypass: bool,
        note: str,
    ) -> WidgetGateDecision:
        self._events.append(
            WidgetAvailabilityEvent(
                unique_tournament_id=int(unique_tournament_id),
                season_id=int(season_id),
                endpoint_pattern=endpoint_pattern,
                scope_kind=scope_kind,
                decision="shadow_suppress",
                observed_at=self.now_factory(),
                job_type="sync_season_widget",
                trace_id=None,
                classification_before=classification_before,
                note=note,
            )
        )
        self._fetched_patterns.add(endpoint_pattern)
        return WidgetGateDecision(
            should_fetch=True,
            endpoint_pattern=endpoint_pattern,
            scope_kind=scope_kind,
            decision_type="bypass_probe" if seasonal_bypass else "probe",
            classification_before=classification_before,
            unique_tournament_id=int(unique_tournament_id),
            season_id=int(season_id),
        )

    async def _prime_tournament_states(self, *, unique_tournament_id: int, endpoint_patterns: tuple[str, ...]) -> None:
        missing_patterns = tuple(
            pattern for pattern in endpoint_patterns if (int(unique_tournament_id), pattern) not in self._tournament_states
        )
        if not missing_patterns:
            return
        loaded = await self.repository.list_states(
            self.sql_executor,
            scope_kind=SCOPE_TOURNAMENT,
            unique_tournament_id=int(unique_tournament_id),
            season_id=None,
            endpoint_patterns=missing_patterns,
        )
        self._tournament_states.update(
            {
                (int(unique_tournament_id), state.endpoint_pattern): state
                for state in loaded.values()
            }
        )

    async def _prime_season_states(
        self,
        *,
        unique_tournament_id: int,
        season_id: int,
        endpoint_patterns: tuple[str, ...],
    ) -> None:
        missing_patterns = tuple(
            pattern
            for pattern in endpoint_patterns
            if (int(unique_tournament_id), int(season_id), pattern) not in self._season_states
        )
        if not missing_patterns:
            return
        loaded = await self.repository.list_states(
            self.sql_executor,
            scope_kind=SCOPE_SEASON,
            unique_tournament_id=int(unique_tournament_id),
            season_id=int(season_id),
            endpoint_patterns=missing_patterns,
        )
        self._season_states.update(
            {
                (int(unique_tournament_id), int(season_id), state.endpoint_pattern): state
                for state in loaded.values()
            }
        )

    async def _seasonal_bypass_active(
        self,
        *,
        unique_tournament_id: int,
        season_id: int,
        endpoint_pattern: str,
        now: datetime,
    ) -> bool:
        cache_key = (int(unique_tournament_id), int(season_id))
        if cache_key not in self._season_end_at_by_key:
            self._season_end_at_by_key[cache_key] = await self.repository.fetch_season_end_at(
                self.sql_executor,
                unique_tournament_id=int(unique_tournament_id),
                season_id=int(season_id),
            )
        return is_seasonal_widget_bypass_active(
            endpoint_pattern,
            now=now,
            season_end_at=self._season_end_at_by_key[cache_key],
        )

    def _is_coarsely_suppressed(
        self,
        *,
        unique_tournament_id: int,
        season_id: int,
        endpoint_pattern: str,
        state: NegativeCacheState | None,
        now: datetime,
        seasonal_bypass: bool,
    ) -> bool:
        if state is None or state.classification != CLASSIFICATION_B_STRUCTURAL:
            return False
        if seasonal_bypass:
            return False
        if state.next_probe_after is None or state.next_probe_after <= now:
            return False
        return True

    def _is_suppressed_by_state(
        self,
        *,
        state: NegativeCacheState,
        endpoint_pattern: str,
        now: datetime,
        seasonal_bypass: bool,
    ) -> bool:
        if seasonal_bypass:
            return False
        if state.classification == CLASSIFICATION_SUPPORTED_SEASON:
            return False
        if state.next_probe_after is None:
            return False
        return state.next_probe_after > now


class ReplaySeasonWidgetNegativeCache:
    def __init__(self, *, blocked_patterns: tuple[str, ...], fetched_patterns: tuple[str, ...]) -> None:
        self._blocked_patterns = set(blocked_patterns)
        self._fetched_patterns = set(fetched_patterns)

    async def blocked_endpoint_patterns(
        self,
        *,
        sport_slug: str,
        unique_tournament_id: int,
        season_id: int,
        endpoint_patterns,
    ) -> tuple[str, ...]:
        del sport_slug, unique_tournament_id, season_id
        return tuple(
            pattern
            for pattern in endpoint_patterns
            if str(pattern) in self._blocked_patterns
        )

    async def coarse_filter_widget_jobs(self, *, sport_slug: str, unique_tournament_id: int, season_id: int, jobs):
        del sport_slug, unique_tournament_id, season_id
        return tuple(job for job in jobs if str(job.params.get("_endpoint_pattern") or "") not in self._blocked_patterns)

    async def decide_widget_probe(
        self,
        *,
        sport_slug: str,
        unique_tournament_id: int,
        season_id: int,
        widget_job,
        endpoint_pattern: str,
    ) -> WidgetGateDecision:
        del sport_slug, unique_tournament_id, season_id, widget_job
        return WidgetGateDecision(
            should_fetch=endpoint_pattern in self._fetched_patterns,
            endpoint_pattern=endpoint_pattern,
            scope_kind=SCOPE_TOURNAMENT,
            decision_type="replay_probe" if endpoint_pattern in self._fetched_patterns else "replay_suppressed",
            classification_before=None,
            unique_tournament_id=0,
            season_id=None,
        )

    async def record_widget_outcome(self, *, decision: WidgetGateDecision, endpoint_pattern: str, outcome) -> None:
        del decision, endpoint_pattern, outcome


def _merge_sorted_ids(existing: tuple[int, ...], season_id: int | None) -> tuple[int, ...]:
    if season_id is None:
        return tuple(existing)
    values = sorted({int(item) for item in existing} | {int(season_id)})
    return tuple(values)


def _deterministic_jitter_multiplier(
    *,
    unique_tournament_id: int,
    season_id: int | None,
    endpoint_pattern: str,
    step: int,
) -> float:
    seed = f"{unique_tournament_id}:{season_id or 0}:{endpoint_pattern}:{step}".encode("utf-8")
    digest = hashlib.sha1(seed).digest()
    fraction = int.from_bytes(digest[:8], "big") / float(2**64 - 1)
    return 0.75 + (fraction * 0.5)
