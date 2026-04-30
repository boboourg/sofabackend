"""Event-phase negative cache for match-center endpoint retries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import hashlib
import os

from .runtime import _env_bool, _load_project_env


CLASSIFICATION_C_PROBATION = "c_probation"
CLASSIFICATION_SUPPORTED = "supported"

MODE_OFF = "off"
MODE_SHADOW = "shadow"
MODE_ENFORCE = "enforce"

PHASE_NOTSTARTED = "notstarted"
PHASE_INPROGRESS = "inprogress"
PHASE_FINISHED = "finished"
PHASE_TERMINATED = "terminated"
PHASE_UNKNOWN = "unknown"

_KNOWN_PHASES = {
    PHASE_NOTSTARTED,
    PHASE_INPROGRESS,
    PHASE_FINISHED,
    PHASE_TERMINATED,
    PHASE_UNKNOWN,
}

_NEGATIVE_OUTCOME_CLASSIFICATIONS = {"not_found", "success_empty_json"}

_PHASE_INTERVALS: dict[str, tuple[timedelta, ...]] = {
    PHASE_NOTSTARTED: (
        timedelta(minutes=15),
        timedelta(hours=1),
        timedelta(hours=3),
    ),
    PHASE_INPROGRESS: (
        timedelta(minutes=2),
        timedelta(minutes=5),
        timedelta(minutes=10),
    ),
    PHASE_FINISHED: (
        timedelta(minutes=15),
        timedelta(hours=1),
        timedelta(hours=6),
    ),
    PHASE_TERMINATED: (
        timedelta(hours=1),
        timedelta(hours=6),
        timedelta(hours=24),
    ),
    PHASE_UNKNOWN: (
        timedelta(minutes=15),
        timedelta(hours=1),
        timedelta(hours=6),
    ),
}


@dataclass(frozen=True)
class EventEndpointNegativeCacheSettings:
    mode: str = MODE_OFF

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
class EventEndpointNegativeCacheState:
    event_id: int
    status_phase: str
    endpoint_pattern: str
    classification: str
    first_negative_at: datetime | None
    last_negative_at: datetime | None
    first_success_at: datetime | None
    last_success_at: datetime | None
    suppressed_hits_total: int
    actual_probe_total: int
    recheck_iteration: int
    next_probe_after: datetime | None
    probe_lease_until: datetime | None
    probe_lease_owner: str | None
    last_http_status: int | None
    last_outcome_classification: str | None
    last_job_type: str | None
    last_trace_id: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ProbeObservation:
    event_id: int
    status_phase: str
    endpoint_pattern: str
    observed_at: datetime
    http_status: int | None
    outcome_classification: str
    next_probe_after: datetime | None
    job_type: str
    trace_id: str | None = None


@dataclass(frozen=True)
class EventEndpointGateDecision:
    should_fetch: bool
    endpoint_pattern: str
    decision_type: str
    classification_before: str | None
    event_id: int
    status_phase: str


@dataclass(frozen=True)
class EventEndpointAvailabilityEvent:
    event_id: int
    status_phase: str
    endpoint_pattern: str
    decision: str
    observed_at: datetime
    job_type: str
    trace_id: str | None
    http_status: int | None = None
    outcome_classification: str | None = None
    proxy_id: str | None = None
    source_url: str | None = None
    classification_before: str | None = None
    note: str | None = None


def load_event_negative_cache_settings(env: dict[str, str] | None = None) -> EventEndpointNegativeCacheSettings:
    resolved_env = _load_project_env() if env is None else env
    if not _env_bool(resolved_env, "SCHEMA_INSPECTOR_EVENT_NEGATIVE_CACHE_ENABLED", False):
        return EventEndpointNegativeCacheSettings(mode=MODE_OFF)
    raw_mode = str(resolved_env.get("SCHEMA_INSPECTOR_EVENT_NEGATIVE_CACHE_MODE", MODE_ENFORCE)).strip().lower()
    if raw_mode not in {MODE_OFF, MODE_SHADOW, MODE_ENFORCE}:
        raw_mode = MODE_ENFORCE
    return EventEndpointNegativeCacheSettings(mode=raw_mode)


def normalize_event_status_phase(status_type: str | None) -> str:
    normalized = str(status_type or "").strip().lower()
    if normalized in _KNOWN_PHASES:
        return normalized
    if normalized in {"scheduled", "pre", "prematch"}:
        return PHASE_NOTSTARTED
    if normalized in {"cancelled", "postponed", "abandoned", "walkover"}:
        return PHASE_TERMINATED
    return PHASE_UNKNOWN


def reduce_event_negative_cache_state(
    current: EventEndpointNegativeCacheState | None,
    observation: ProbeObservation,
) -> EventEndpointNegativeCacheState:
    observed_at = observation.observed_at
    if observation.outcome_classification == "success_json":
        return EventEndpointNegativeCacheState(
            event_id=int(observation.event_id),
            status_phase=normalize_event_status_phase(observation.status_phase),
            endpoint_pattern=observation.endpoint_pattern,
            classification=CLASSIFICATION_SUPPORTED,
            first_negative_at=current.first_negative_at if current is not None else None,
            last_negative_at=current.last_negative_at if current is not None else None,
            first_success_at=current.first_success_at if current is not None else observed_at,
            last_success_at=observed_at,
            suppressed_hits_total=current.suppressed_hits_total if current is not None else 0,
            actual_probe_total=(current.actual_probe_total if current is not None else 0) + 1,
            recheck_iteration=0,
            next_probe_after=None,
            probe_lease_until=None,
            probe_lease_owner=None,
            last_http_status=observation.http_status,
            last_outcome_classification=observation.outcome_classification,
            last_job_type=observation.job_type,
            last_trace_id=observation.trace_id,
            created_at=current.created_at if current is not None else observed_at,
            updated_at=observed_at,
        )

    if observation.outcome_classification not in _NEGATIVE_OUTCOME_CLASSIFICATIONS:
        return current or EventEndpointNegativeCacheState(
            event_id=int(observation.event_id),
            status_phase=normalize_event_status_phase(observation.status_phase),
            endpoint_pattern=observation.endpoint_pattern,
            classification=CLASSIFICATION_SUPPORTED,
            first_negative_at=None,
            last_negative_at=None,
            first_success_at=None,
            last_success_at=None,
            suppressed_hits_total=0,
            actual_probe_total=0,
            recheck_iteration=0,
            next_probe_after=None,
            probe_lease_until=None,
            probe_lease_owner=None,
            last_http_status=observation.http_status,
            last_outcome_classification=observation.outcome_classification,
            last_job_type=observation.job_type,
            last_trace_id=observation.trace_id,
            created_at=observed_at,
            updated_at=observed_at,
        )

    return EventEndpointNegativeCacheState(
        event_id=int(observation.event_id),
        status_phase=normalize_event_status_phase(observation.status_phase),
        endpoint_pattern=observation.endpoint_pattern,
        classification=CLASSIFICATION_C_PROBATION,
        first_negative_at=current.first_negative_at if current is not None else observed_at,
        last_negative_at=observed_at,
        first_success_at=current.first_success_at if current is not None else None,
        last_success_at=current.last_success_at if current is not None else None,
        suppressed_hits_total=current.suppressed_hits_total if current is not None else 0,
        actual_probe_total=(current.actual_probe_total if current is not None else 0) + 1,
        recheck_iteration=(current.recheck_iteration if current is not None else 0) + 1,
        next_probe_after=observation.next_probe_after,
        probe_lease_until=None,
        probe_lease_owner=None,
        last_http_status=observation.http_status,
        last_outcome_classification=observation.outcome_classification,
        last_job_type=observation.job_type,
        last_trace_id=observation.trace_id,
        created_at=current.created_at if current is not None else observed_at,
        updated_at=observed_at,
    )


def next_probe_after_for_state(
    *,
    state: EventEndpointNegativeCacheState | None,
    status_phase: str,
    observed_at: datetime,
    event_id: int,
    endpoint_pattern: str,
) -> datetime:
    phase = normalize_event_status_phase(status_phase)
    sequence = _PHASE_INTERVALS.get(phase, _PHASE_INTERVALS[PHASE_UNKNOWN])
    previous_iteration = state.recheck_iteration if state is not None else 0
    step = max(previous_iteration, 0)
    interval = sequence[min(step, len(sequence) - 1)]
    multiplier = _deterministic_jitter_multiplier(
        event_id=int(event_id),
        status_phase=phase,
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


class EventEndpointNegativeCache:
    def __init__(
        self,
        *,
        repository,
        sql_executor,
        now_factory=None,
        settings: EventEndpointNegativeCacheSettings | None = None,
        lease_owner: str | None = None,
    ) -> None:
        self.repository = repository
        self.sql_executor = sql_executor
        self.now_factory = now_factory or (lambda: datetime.now(UTC))
        self.settings = settings or EventEndpointNegativeCacheSettings()
        self.lease_owner = lease_owner or f"event-negcache:{os.getpid()}:{id(self)}"
        self._events: list[EventEndpointAvailabilityEvent] = []
        self._fetched_patterns: set[str] = set()
        self._states: dict[tuple[int, str, str], EventEndpointNegativeCacheState] = {}

    @property
    def events(self) -> tuple[EventEndpointAvailabilityEvent, ...]:
        return tuple(self._events)

    async def decide_event_probe(
        self,
        *,
        event_id: int,
        status_phase: str,
        endpoint_pattern: str,
        job_type: str,
    ) -> EventEndpointGateDecision:
        normalized_phase = normalize_event_status_phase(status_phase)
        if not self.settings.enabled:
            self._fetched_patterns.add(endpoint_pattern)
            return EventEndpointGateDecision(
                should_fetch=True,
                endpoint_pattern=endpoint_pattern,
                decision_type="probe",
                classification_before=None,
                event_id=int(event_id),
                status_phase=normalized_phase,
            )

        await self._prime_states(
            event_id=int(event_id),
            status_phase=normalized_phase,
            endpoint_patterns=(endpoint_pattern,),
        )
        state = self._states.get((int(event_id), normalized_phase, endpoint_pattern))
        now = self.now_factory()

        if state is None or state.classification == CLASSIFICATION_SUPPORTED or state.next_probe_after is None:
            self._fetched_patterns.add(endpoint_pattern)
            return EventEndpointGateDecision(
                should_fetch=True,
                endpoint_pattern=endpoint_pattern,
                decision_type="probe",
                classification_before=state.classification if state is not None else None,
                event_id=int(event_id),
                status_phase=normalized_phase,
            )

        if state.next_probe_after > now:
            if self.settings.shadow:
                self._events.append(
                    EventEndpointAvailabilityEvent(
                        event_id=int(event_id),
                        status_phase=normalized_phase,
                        endpoint_pattern=endpoint_pattern,
                        decision="shadow_suppress",
                        observed_at=now,
                        job_type=job_type,
                        trace_id=None,
                        classification_before=state.classification,
                        note="cooldown_shadow",
                    )
                )
                self._fetched_patterns.add(endpoint_pattern)
                return EventEndpointGateDecision(
                    should_fetch=True,
                    endpoint_pattern=endpoint_pattern,
                    decision_type="probe",
                    classification_before=state.classification,
                    event_id=int(event_id),
                    status_phase=normalized_phase,
                )
            self._events.append(
                EventEndpointAvailabilityEvent(
                    event_id=int(event_id),
                    status_phase=normalized_phase,
                    endpoint_pattern=endpoint_pattern,
                    decision="suppressed",
                    observed_at=now,
                    job_type=job_type,
                    trace_id=None,
                    classification_before=state.classification,
                    note="cooldown_enforced",
                )
            )
            return EventEndpointGateDecision(
                should_fetch=False,
                endpoint_pattern=endpoint_pattern,
                decision_type="suppressed",
                classification_before=state.classification,
                event_id=int(event_id),
                status_phase=normalized_phase,
            )

        acquired = await self.repository.try_acquire_probe_lease(
            self.sql_executor,
            event_id=int(event_id),
            status_phase=normalized_phase,
            endpoint_pattern=endpoint_pattern,
            lease_owner=self.lease_owner,
            now=now,
        )
        if not acquired:
            if self.settings.shadow:
                self._events.append(
                    EventEndpointAvailabilityEvent(
                        event_id=int(event_id),
                        status_phase=normalized_phase,
                        endpoint_pattern=endpoint_pattern,
                        decision="shadow_suppress",
                        observed_at=now,
                        job_type=job_type,
                        trace_id=None,
                        classification_before=state.classification,
                        note="lease_blocked_shadow",
                    )
                )
                self._fetched_patterns.add(endpoint_pattern)
                return EventEndpointGateDecision(
                    should_fetch=True,
                    endpoint_pattern=endpoint_pattern,
                    decision_type="probe",
                    classification_before=state.classification,
                    event_id=int(event_id),
                    status_phase=normalized_phase,
                )
            self._events.append(
                EventEndpointAvailabilityEvent(
                    event_id=int(event_id),
                    status_phase=normalized_phase,
                    endpoint_pattern=endpoint_pattern,
                    decision="lease_blocked",
                    observed_at=now,
                    job_type=job_type,
                    trace_id=None,
                    classification_before=state.classification,
                    note="probe_lease_blocked",
                )
            )
            return EventEndpointGateDecision(
                should_fetch=False,
                endpoint_pattern=endpoint_pattern,
                decision_type="lease_blocked",
                classification_before=state.classification,
                event_id=int(event_id),
                status_phase=normalized_phase,
            )

        self._fetched_patterns.add(endpoint_pattern)
        return EventEndpointGateDecision(
            should_fetch=True,
            endpoint_pattern=endpoint_pattern,
            decision_type="probe",
            classification_before=state.classification,
            event_id=int(event_id),
            status_phase=normalized_phase,
        )

    async def record_event_outcome(
        self,
        *,
        decision: EventEndpointGateDecision,
        endpoint_pattern: str,
        outcome,
        job_type: str,
    ) -> None:
        if not self.settings.enabled or not decision.should_fetch:
            return
        outcome_classification = str(getattr(outcome, "classification", "") or "").strip().lower()
        if outcome_classification not in {"success_json", *tuple(_NEGATIVE_OUTCOME_CLASSIFICATIONS)}:
            return
        observed_at = parse_observed_at(getattr(outcome, "fetched_at", None), fallback=self.now_factory())
        state_key = (int(decision.event_id), decision.status_phase, endpoint_pattern)
        current = self._states.get(state_key)
        next_probe_after = None
        if outcome_classification != "success_json":
            next_probe_after = next_probe_after_for_state(
                state=current,
                status_phase=decision.status_phase,
                observed_at=observed_at,
                event_id=decision.event_id,
                endpoint_pattern=endpoint_pattern,
            )
        updated_state = reduce_event_negative_cache_state(
            current=current,
            observation=ProbeObservation(
                event_id=int(decision.event_id),
                status_phase=decision.status_phase,
                endpoint_pattern=endpoint_pattern,
                observed_at=observed_at,
                http_status=getattr(outcome, "http_status", None),
                outcome_classification=outcome_classification,
                next_probe_after=next_probe_after,
                job_type=job_type,
                trace_id=getattr(outcome, "trace_id", None),
            ),
        )
        self._states[state_key] = updated_state
        self._events.append(
            EventEndpointAvailabilityEvent(
                event_id=decision.event_id,
                status_phase=decision.status_phase,
                endpoint_pattern=endpoint_pattern,
                decision=decision.decision_type,
                observed_at=observed_at,
                job_type=job_type,
                trace_id=getattr(outcome, "trace_id", None),
                http_status=getattr(outcome, "http_status", None),
                outcome_classification=outcome_classification,
                proxy_id=getattr(outcome, "proxy_id", None),
                source_url=getattr(outcome, "source_url", None),
                classification_before=decision.classification_before,
            )
        )

    def build_replay_gate(self):
        return ReplayEventEndpointNegativeCache(fetched_patterns=tuple(sorted(self._fetched_patterns)))

    async def _prime_states(
        self,
        *,
        event_id: int,
        status_phase: str,
        endpoint_patterns: tuple[str, ...],
    ) -> None:
        missing_patterns = tuple(
            pattern
            for pattern in endpoint_patterns
            if (int(event_id), status_phase, pattern) not in self._states
        )
        if not missing_patterns:
            return
        loaded = await self.repository.list_states(
            self.sql_executor,
            event_id=int(event_id),
            status_phase=status_phase,
            endpoint_patterns=missing_patterns,
        )
        self._states.update(
            {
                (int(event_id), status_phase, state.endpoint_pattern): state
                for state in loaded.values()
            }
        )


class ReplayEventEndpointNegativeCache:
    def __init__(self, *, fetched_patterns: tuple[str, ...]) -> None:
        self._fetched_patterns = set(fetched_patterns)

    async def decide_event_probe(
        self,
        *,
        event_id: int,
        status_phase: str,
        endpoint_pattern: str,
        job_type: str,
    ) -> EventEndpointGateDecision:
        del event_id, status_phase, job_type
        return EventEndpointGateDecision(
            should_fetch=endpoint_pattern in self._fetched_patterns,
            endpoint_pattern=endpoint_pattern,
            decision_type="replay_probe" if endpoint_pattern in self._fetched_patterns else "replay_suppressed",
            classification_before=None,
            event_id=0,
            status_phase=PHASE_UNKNOWN,
        )

    async def record_event_outcome(self, *, decision: EventEndpointGateDecision, endpoint_pattern: str, outcome, job_type: str) -> None:
        del decision, endpoint_pattern, outcome, job_type


def _deterministic_jitter_multiplier(
    *,
    event_id: int,
    status_phase: str,
    endpoint_pattern: str,
    step: int,
) -> float:
    seed = f"{event_id}:{status_phase}:{endpoint_pattern}:{step}".encode("utf-8")
    digest = hashlib.sha1(seed).digest()
    fraction = int.from_bytes(digest[:8], "big") / float(2**64 - 1)
    return 0.75 + (fraction * 0.5)
