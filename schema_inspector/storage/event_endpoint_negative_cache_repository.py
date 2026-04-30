"""Persistent storage for event-level match-center negative cache state."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

from ..event_endpoint_negative_cache import (
    EventEndpointAvailabilityEvent,
    EventEndpointNegativeCacheState,
    ProbeObservation,
    next_probe_after_for_state,
    normalize_event_status_phase,
    parse_observed_at,
    reduce_event_negative_cache_state,
)
from ._temporal import coerce_timestamptz


class EventEndpointNegativeCacheRepository:
    async def list_states(
        self,
        executor,
        *,
        event_id: int,
        status_phase: str,
        endpoint_patterns: tuple[str, ...],
    ) -> dict[str, EventEndpointNegativeCacheState]:
        if not endpoint_patterns:
            return {}
        rows = await executor.fetch(
            """
            SELECT
                event_id,
                status_phase,
                endpoint_pattern,
                classification,
                first_negative_at,
                last_negative_at,
                first_success_at,
                last_success_at,
                suppressed_hits_total,
                actual_probe_total,
                recheck_iteration,
                next_probe_after,
                probe_lease_until,
                probe_lease_owner,
                last_http_status,
                last_outcome_classification,
                last_job_type,
                last_trace_id,
                created_at,
                updated_at
            FROM event_endpoint_negative_cache_state
            WHERE event_id = $1
              AND status_phase = $2
              AND endpoint_pattern = ANY($3::text[])
            """,
            int(event_id),
            normalize_event_status_phase(status_phase),
            list(endpoint_patterns),
        )
        return {str(row["endpoint_pattern"]): _state_from_row(row) for row in rows}

    async def try_acquire_probe_lease(
        self,
        executor,
        *,
        event_id: int,
        status_phase: str,
        endpoint_pattern: str,
        lease_owner: str,
        now: datetime,
        lease_seconds: int = 90,
    ) -> bool:
        row = await executor.fetchrow(
            """
            UPDATE event_endpoint_negative_cache_state
            SET probe_lease_until = $5,
                probe_lease_owner = $4,
                updated_at = $6
            WHERE event_id = $1
              AND status_phase = $2
              AND endpoint_pattern = $3
              AND (probe_lease_until IS NULL OR probe_lease_until < $6)
            RETURNING endpoint_pattern
            """,
            int(event_id),
            normalize_event_status_phase(status_phase),
            endpoint_pattern,
            lease_owner,
            coerce_timestamptz((now.replace(microsecond=0) + timedelta(seconds=lease_seconds)).isoformat()),
            coerce_timestamptz(now.isoformat()),
        )
        return row is not None

    async def apply_events(self, executor, events: Iterable[EventEndpointAvailabilityEvent]) -> None:
        for event in events:
            await self._apply_event(executor, event)

    async def _apply_event(self, executor, event: EventEndpointAvailabilityEvent) -> None:
        if event.decision == "probe" and event.outcome_classification is not None:
            await self._apply_probe_event(executor, event)
            return

        state = await self._load_state(
            executor,
            event_id=event.event_id,
            status_phase=event.status_phase,
            endpoint_pattern=event.endpoint_pattern,
        )
        if state is not None and event.decision in {"suppressed", "lease_blocked"}:
            updated = replace(
                state,
                suppressed_hits_total=state.suppressed_hits_total + 1,
                updated_at=event.observed_at,
                last_job_type=event.job_type,
                last_trace_id=event.trace_id,
            )
            await self._upsert_state(executor, updated)
        await self._insert_log(
            executor,
            event=event,
            classification_after=state.classification if state is not None else None,
            next_probe_after=state.next_probe_after if state is not None else None,
        )

    async def _apply_probe_event(self, executor, event: EventEndpointAvailabilityEvent) -> None:
        current = await self._load_state(
            executor,
            event_id=event.event_id,
            status_phase=event.status_phase,
            endpoint_pattern=event.endpoint_pattern,
        )
        observed_at = parse_observed_at(
            event.observed_at.isoformat() if hasattr(event.observed_at, "isoformat") else str(event.observed_at),
            fallback=event.observed_at,
        )
        next_probe_after = None
        if event.outcome_classification != "success_json":
            next_probe_after = next_probe_after_for_state(
                state=current,
                status_phase=event.status_phase,
                observed_at=observed_at,
                event_id=event.event_id,
                endpoint_pattern=event.endpoint_pattern,
            )
        updated = reduce_event_negative_cache_state(
            current=current,
            observation=ProbeObservation(
                event_id=int(event.event_id),
                status_phase=normalize_event_status_phase(event.status_phase),
                endpoint_pattern=event.endpoint_pattern,
                observed_at=observed_at,
                http_status=event.http_status,
                outcome_classification=str(event.outcome_classification or ""),
                next_probe_after=next_probe_after,
                job_type=event.job_type,
                trace_id=event.trace_id,
            ),
        )
        await self._upsert_state(executor, updated)
        await self._insert_log(
            executor,
            event=event,
            classification_after=updated.classification,
            next_probe_after=updated.next_probe_after,
        )

    async def _load_state(
        self,
        executor,
        *,
        event_id: int,
        status_phase: str,
        endpoint_pattern: str,
    ) -> EventEndpointNegativeCacheState | None:
        rows = await self.list_states(
            executor,
            event_id=int(event_id),
            status_phase=status_phase,
            endpoint_patterns=(endpoint_pattern,),
        )
        return rows.get(endpoint_pattern)

    async def _upsert_state(self, executor, state: EventEndpointNegativeCacheState) -> None:
        await executor.execute(
            """
            INSERT INTO event_endpoint_negative_cache_state (
                cache_key,
                event_id,
                status_phase,
                endpoint_pattern,
                classification,
                first_negative_at,
                last_negative_at,
                first_success_at,
                last_success_at,
                suppressed_hits_total,
                actual_probe_total,
                recheck_iteration,
                next_probe_after,
                probe_lease_until,
                probe_lease_owner,
                last_http_status,
                last_outcome_classification,
                last_job_type,
                last_trace_id,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
            )
            ON CONFLICT (cache_key) DO UPDATE SET
                classification = EXCLUDED.classification,
                first_negative_at = EXCLUDED.first_negative_at,
                last_negative_at = EXCLUDED.last_negative_at,
                first_success_at = EXCLUDED.first_success_at,
                last_success_at = EXCLUDED.last_success_at,
                suppressed_hits_total = EXCLUDED.suppressed_hits_total,
                actual_probe_total = EXCLUDED.actual_probe_total,
                recheck_iteration = EXCLUDED.recheck_iteration,
                next_probe_after = EXCLUDED.next_probe_after,
                probe_lease_until = EXCLUDED.probe_lease_until,
                probe_lease_owner = EXCLUDED.probe_lease_owner,
                last_http_status = EXCLUDED.last_http_status,
                last_outcome_classification = EXCLUDED.last_outcome_classification,
                last_job_type = EXCLUDED.last_job_type,
                last_trace_id = EXCLUDED.last_trace_id,
                updated_at = EXCLUDED.updated_at
            """,
            _cache_key(state.event_id, state.status_phase, state.endpoint_pattern),
            int(state.event_id),
            normalize_event_status_phase(state.status_phase),
            state.endpoint_pattern,
            state.classification,
            coerce_timestamptz(_dt_to_iso(state.first_negative_at)),
            coerce_timestamptz(_dt_to_iso(state.last_negative_at)),
            coerce_timestamptz(_dt_to_iso(state.first_success_at)),
            coerce_timestamptz(_dt_to_iso(state.last_success_at)),
            int(state.suppressed_hits_total),
            int(state.actual_probe_total),
            int(state.recheck_iteration),
            coerce_timestamptz(_dt_to_iso(state.next_probe_after)),
            coerce_timestamptz(_dt_to_iso(state.probe_lease_until)),
            state.probe_lease_owner,
            state.last_http_status,
            state.last_outcome_classification,
            state.last_job_type,
            state.last_trace_id,
            coerce_timestamptz(_dt_to_iso(state.created_at)),
            coerce_timestamptz(_dt_to_iso(state.updated_at)),
        )

    async def _insert_log(
        self,
        executor,
        *,
        event: EventEndpointAvailabilityEvent,
        classification_after: str | None,
        next_probe_after: datetime | None,
    ) -> None:
        await executor.execute(
            """
            INSERT INTO event_endpoint_availability_log (
                observed_at,
                event_id,
                status_phase,
                endpoint_pattern,
                job_type,
                trace_id,
                decision,
                http_status,
                outcome_classification,
                classification_before,
                classification_after,
                next_probe_after,
                proxy_id,
                source_url,
                note
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            """,
            coerce_timestamptz(_dt_to_iso(event.observed_at)),
            int(event.event_id),
            normalize_event_status_phase(event.status_phase),
            event.endpoint_pattern,
            event.job_type,
            event.trace_id,
            event.decision,
            event.http_status,
            event.outcome_classification,
            event.classification_before,
            classification_after,
            coerce_timestamptz(_dt_to_iso(next_probe_after)),
            event.proxy_id,
            event.source_url,
            event.note,
        )


def _state_from_row(row: Any) -> EventEndpointNegativeCacheState:
    return EventEndpointNegativeCacheState(
        event_id=int(row["event_id"]),
        status_phase=normalize_event_status_phase(str(row["status_phase"])),
        endpoint_pattern=str(row["endpoint_pattern"]),
        classification=str(row["classification"]),
        first_negative_at=_coerce_dt(row["first_negative_at"]),
        last_negative_at=_coerce_dt(row["last_negative_at"]),
        first_success_at=_coerce_dt(row["first_success_at"]),
        last_success_at=_coerce_dt(row["last_success_at"]),
        suppressed_hits_total=int(row["suppressed_hits_total"] or 0),
        actual_probe_total=int(row["actual_probe_total"] or 0),
        recheck_iteration=int(row["recheck_iteration"] or 0),
        next_probe_after=_coerce_dt(row["next_probe_after"]),
        probe_lease_until=_coerce_dt(row["probe_lease_until"]),
        probe_lease_owner=row["probe_lease_owner"],
        last_http_status=int(row["last_http_status"]) if row["last_http_status"] is not None else None,
        last_outcome_classification=row["last_outcome_classification"],
        last_job_type=row["last_job_type"],
        last_trace_id=row["last_trace_id"],
        created_at=_coerce_dt(row["created_at"]) or datetime.now(UTC),
        updated_at=_coerce_dt(row["updated_at"]) or datetime.now(UTC),
    )


def _cache_key(event_id: int, status_phase: str, endpoint_pattern: str) -> str:
    return f"{int(event_id)}:{normalize_event_status_phase(status_phase)}:{endpoint_pattern}"


def _dt_to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _coerce_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None
