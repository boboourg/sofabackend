"""Persistent storage for season-widget negative cache state."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable

import orjson

from ..season_widget_negative_cache import (
    CLASSIFICATION_B_STRUCTURAL,
    CLASSIFICATION_C_PROBATION,
    CLASSIFICATION_MIXED_BY_SEASON,
    CLASSIFICATION_SUPPORTED_SEASON,
    NegativeCacheState,
    ProbeObservation,
    WidgetAvailabilityEvent,
    next_probe_after_for_state,
    parse_observed_at,
    reduce_negative_cache_state,
)
from ._temporal import coerce_timestamptz


class EndpointNegativeCacheRepository:
    async def list_states(
        self,
        executor,
        *,
        scope_kind: str,
        unique_tournament_id: int,
        season_id: int | None,
        endpoint_patterns: tuple[str, ...],
    ) -> dict[str, NegativeCacheState]:
        if not endpoint_patterns:
            return {}
        rows = await executor.fetch(
            """
            SELECT
                scope_kind,
                unique_tournament_id,
                season_id,
                endpoint_pattern,
                classification,
                first_404_at,
                last_404_at,
                first_200_at,
                last_200_at,
                seen_404_season_ids_json,
                seen_200_season_ids_json,
                suppressed_hits_total,
                actual_probe_total,
                recheck_iteration,
                next_probe_after,
                probe_lease_until,
                probe_lease_owner,
                last_http_status,
                last_job_type,
                last_trace_id,
                created_at,
                updated_at
            FROM endpoint_negative_cache_state
            WHERE scope_kind = $1
              AND unique_tournament_id = $2
              AND (
                ($1 = 'tournament' AND season_id IS NULL)
                OR ($1 = 'season' AND season_id = $3)
              )
              AND endpoint_pattern = ANY($4::text[])
            """,
            scope_kind,
            int(unique_tournament_id),
            int(season_id) if season_id is not None else None,
            list(endpoint_patterns),
        )
        return {
            str(row["endpoint_pattern"]): _state_from_row(row)
            for row in rows
        }

    async def fetch_season_end_at(
        self,
        executor,
        *,
        unique_tournament_id: int,
        season_id: int,
    ) -> datetime | None:
        value = await executor.fetchval(
            """
            WITH scheduled_future AS (
                SELECT MAX(to_timestamp(event.start_timestamp)) AS season_end_at
                FROM event
                JOIN event_status
                  ON event_status.code = event.status_code
                WHERE event.unique_tournament_id = $1
                  AND event.season_id = $2
                  AND event.start_timestamp IS NOT NULL
                  AND lower(coalesce(event_status.type, '')) IN ('scheduled', 'notstarted')
            ),
            finished_fallback AS (
                SELECT MAX(to_timestamp(event.start_timestamp)) AS season_end_at
                FROM event
                JOIN event_status
                  ON event_status.code = event.status_code
                WHERE event.unique_tournament_id = $1
                  AND event.season_id = $2
                  AND event.start_timestamp IS NOT NULL
                  AND lower(coalesce(event_status.type, '')) = 'finished'
            )
            SELECT COALESCE(
                (SELECT season_end_at FROM scheduled_future),
                (SELECT season_end_at FROM finished_fallback)
            )
            """,
            int(unique_tournament_id),
            int(season_id),
        )
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return None

    async def try_acquire_probe_lease(
        self,
        executor,
        *,
        scope_kind: str,
        unique_tournament_id: int,
        season_id: int | None,
        endpoint_pattern: str,
        lease_owner: str,
        now: datetime,
        lease_seconds: int = 90,
    ) -> bool:
        row = await executor.fetchrow(
            """
            UPDATE endpoint_negative_cache_state
            SET probe_lease_until = $6,
                probe_lease_owner = $5,
                updated_at = $4
            WHERE scope_kind = $1
              AND unique_tournament_id = $2
              AND (
                ($1 = 'tournament' AND season_id IS NULL)
                OR ($1 = 'season' AND season_id = $3)
              )
              AND endpoint_pattern = $7
              AND (probe_lease_until IS NULL OR probe_lease_until < $4)
            RETURNING endpoint_pattern
            """,
            scope_kind,
            int(unique_tournament_id),
            int(season_id) if season_id is not None else None,
            coerce_timestamptz(now.isoformat()),
            lease_owner,
            coerce_timestamptz((now.replace(microsecond=0) + timedelta(seconds=lease_seconds)).isoformat()),
            endpoint_pattern,
        )
        return row is not None

    async def apply_events(self, executor, events: Iterable[WidgetAvailabilityEvent]) -> None:
        for event in events:
            await self._apply_event(executor, event)

    async def rebuild_state_from_log(self, executor) -> None:
        await executor.execute("TRUNCATE endpoint_negative_cache_state")
        rows = await executor.fetch(
            """
            SELECT
                unique_tournament_id,
                season_id,
                endpoint_pattern,
                scope_kind,
                decision,
                observed_at,
                job_type,
                trace_id,
                http_status
            FROM endpoint_availability_log
            WHERE decision IN ('probe', 'bypass_probe')
            ORDER BY observed_at ASC, id ASC
            """
        )
        for row in rows:
            await self._apply_probe_event(
                executor,
                WidgetAvailabilityEvent(
                    unique_tournament_id=int(row["unique_tournament_id"]),
                    season_id=int(row["season_id"]) if row["season_id"] is not None else None,
                    endpoint_pattern=str(row["endpoint_pattern"]),
                    scope_kind=str(row["scope_kind"]),
                    decision=str(row["decision"]),
                    observed_at=parse_observed_at(row["observed_at"].isoformat() if hasattr(row["observed_at"], "isoformat") else str(row["observed_at"])),
                    job_type=str(row["job_type"]),
                    trace_id=row["trace_id"],
                    http_status=int(row["http_status"]) if row["http_status"] is not None else None,
                ),
                persist_log=False,
            )

    async def _apply_event(self, executor, event: WidgetAvailabilityEvent) -> None:
        if event.decision in {"probe", "bypass_probe"} and event.http_status is not None:
            await self._apply_probe_event(executor, event, persist_log=True)
            return
        if event.decision == "shadow_suppress":
            state = await self._load_state(
                executor,
                scope_kind=event.scope_kind,
                unique_tournament_id=event.unique_tournament_id,
                season_id=event.season_id if event.scope_kind == "season" else None,
                endpoint_pattern=event.endpoint_pattern,
            )
            await self._insert_log(
                executor,
                event=event,
                classification_after=state.classification if state is not None else None,
                next_probe_after=state.next_probe_after if state is not None else None,
            )
            return
        state = await self._load_state(
            executor,
            scope_kind=event.scope_kind,
            unique_tournament_id=event.unique_tournament_id,
            season_id=event.season_id if event.scope_kind == "season" else None,
            endpoint_pattern=event.endpoint_pattern,
        )
        if state is not None:
            updated = replace(
                state,
                suppressed_hits_total=state.suppressed_hits_total + 1,
                updated_at=event.observed_at,
                last_job_type=event.job_type,
                last_trace_id=event.trace_id,
            )
            await self._upsert_state(executor, updated)
        if event.decision == "lease_blocked":
            await self._insert_log(
                executor,
                event=event,
                classification_after=state.classification if state is not None else None,
                next_probe_after=state.next_probe_after if state is not None else None,
            )

    async def _apply_probe_event(self, executor, event: WidgetAvailabilityEvent, *, persist_log: bool) -> None:
        observed_at = event.observed_at
        tournament_state = await self._load_state(
            executor,
            scope_kind="tournament",
            unique_tournament_id=event.unique_tournament_id,
            season_id=None,
            endpoint_pattern=event.endpoint_pattern,
        )
        classification_after: str | None = None
        next_probe_after = None
        if event.scope_kind == "season":
            season_state = await self._load_state(
                executor,
                scope_kind="season",
                unique_tournament_id=event.unique_tournament_id,
                season_id=event.season_id,
                endpoint_pattern=event.endpoint_pattern,
            )
            if event.http_status == 200:
                updated_tournament, season_supported = reduce_negative_cache_state(
                    tournament_state,
                    ProbeObservation(
                        unique_tournament_id=event.unique_tournament_id,
                        season_id=event.season_id,
                        endpoint_pattern=event.endpoint_pattern,
                        observed_at=observed_at,
                        http_status=200,
                        next_probe_after=None,
                        job_type=event.job_type,
                        trace_id=event.trace_id,
                    ),
                )
                await self._upsert_state(executor, updated_tournament)
                if season_supported is not None:
                    await self._upsert_state(executor, season_supported)
                classification_after = season_supported.classification if season_supported is not None else updated_tournament.classification
            else:
                next_probe_after = next_probe_after_for_state(
                    state=season_state,
                    next_classification=CLASSIFICATION_C_PROBATION,
                    observed_at=observed_at,
                    unique_tournament_id=event.unique_tournament_id,
                    season_id=event.season_id,
                    endpoint_pattern=event.endpoint_pattern,
                )
                season_updated = reduce_negative_cache_state(
                    season_state,
                    ProbeObservation(
                        unique_tournament_id=event.unique_tournament_id,
                        season_id=event.season_id,
                        endpoint_pattern=event.endpoint_pattern,
                        observed_at=observed_at,
                        http_status=404,
                        next_probe_after=next_probe_after,
                        job_type=event.job_type,
                        trace_id=event.trace_id,
                    ),
                )
                if isinstance(season_updated, tuple):
                    season_updated = season_updated[0]
                await self._upsert_state(executor, season_updated)
                classification_after = season_updated.classification
            if persist_log:
                await self._insert_log(
                    executor,
                    event=event,
                    classification_after=classification_after,
                    next_probe_after=next_probe_after,
                )
            return

        if event.http_status == 200:
            updated_tournament, season_supported = reduce_negative_cache_state(
                tournament_state,
                ProbeObservation(
                    unique_tournament_id=event.unique_tournament_id,
                    season_id=event.season_id,
                    endpoint_pattern=event.endpoint_pattern,
                    observed_at=observed_at,
                    http_status=200,
                    next_probe_after=None,
                    job_type=event.job_type,
                    trace_id=event.trace_id,
                ),
            )
            await self._upsert_state(executor, updated_tournament)
            if season_supported is not None:
                await self._upsert_state(executor, season_supported)
            classification_after = updated_tournament.classification
        else:
            next_classification = CLASSIFICATION_C_PROBATION
            projected_404_ids = set(tournament_state.seen_404_season_ids if tournament_state is not None else ())
            if event.season_id is not None:
                projected_404_ids.add(int(event.season_id))
            seen_200_ids = set(tournament_state.seen_200_season_ids if tournament_state is not None else ())
            if len(projected_404_ids) >= 2 and not seen_200_ids:
                next_classification = CLASSIFICATION_B_STRUCTURAL
            next_probe_after = next_probe_after_for_state(
                state=tournament_state,
                next_classification=next_classification,
                observed_at=observed_at,
                unique_tournament_id=event.unique_tournament_id,
                season_id=event.season_id,
                endpoint_pattern=event.endpoint_pattern,
            )
            tournament_updated = reduce_negative_cache_state(
                tournament_state,
                ProbeObservation(
                    unique_tournament_id=event.unique_tournament_id,
                    season_id=event.season_id,
                    endpoint_pattern=event.endpoint_pattern,
                    observed_at=observed_at,
                    http_status=404,
                    next_probe_after=next_probe_after,
                    job_type=event.job_type,
                    trace_id=event.trace_id,
                ),
            )
            if isinstance(tournament_updated, tuple):
                tournament_updated = tournament_updated[0]
            await self._upsert_state(executor, tournament_updated)
            classification_after = tournament_updated.classification

        if persist_log:
            await self._insert_log(
                executor,
                event=event,
                classification_after=classification_after,
                next_probe_after=next_probe_after,
            )

    async def _load_state(self, executor, *, scope_kind: str, unique_tournament_id: int, season_id: int | None, endpoint_pattern: str) -> NegativeCacheState | None:
        rows = await self.list_states(
            executor,
            scope_kind=scope_kind,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            endpoint_patterns=(endpoint_pattern,),
        )
        return rows.get(endpoint_pattern)

    async def _upsert_state(self, executor, state: NegativeCacheState) -> None:
        await executor.execute(
            """
            INSERT INTO endpoint_negative_cache_state (
                cache_key,
                scope_kind,
                unique_tournament_id,
                season_id,
                endpoint_pattern,
                classification,
                first_404_at,
                last_404_at,
                first_200_at,
                last_200_at,
                seen_404_season_ids_json,
                seen_200_season_ids_json,
                suppressed_hits_total,
                actual_probe_total,
                recheck_iteration,
                next_probe_after,
                probe_lease_until,
                probe_lease_owner,
                last_http_status,
                last_job_type,
                last_trace_id,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10::jsonb, $11::jsonb,
                $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
            )
            ON CONFLICT (cache_key) DO UPDATE SET
                classification = EXCLUDED.classification,
                first_404_at = EXCLUDED.first_404_at,
                last_404_at = EXCLUDED.last_404_at,
                first_200_at = EXCLUDED.first_200_at,
                last_200_at = EXCLUDED.last_200_at,
                seen_404_season_ids_json = EXCLUDED.seen_404_season_ids_json,
                seen_200_season_ids_json = EXCLUDED.seen_200_season_ids_json,
                suppressed_hits_total = EXCLUDED.suppressed_hits_total,
                actual_probe_total = EXCLUDED.actual_probe_total,
                recheck_iteration = EXCLUDED.recheck_iteration,
                next_probe_after = EXCLUDED.next_probe_after,
                probe_lease_until = EXCLUDED.probe_lease_until,
                probe_lease_owner = EXCLUDED.probe_lease_owner,
                last_http_status = EXCLUDED.last_http_status,
                last_job_type = EXCLUDED.last_job_type,
                last_trace_id = EXCLUDED.last_trace_id,
                updated_at = EXCLUDED.updated_at
            """,
            _cache_key(state.scope_kind, state.unique_tournament_id, state.season_id, state.endpoint_pattern),
            state.scope_kind,
            state.unique_tournament_id,
            state.season_id,
            state.endpoint_pattern,
            state.classification,
            coerce_timestamptz(state.first_404_at.isoformat() if state.first_404_at is not None else None),
            coerce_timestamptz(state.last_404_at.isoformat() if state.last_404_at is not None else None),
            coerce_timestamptz(state.first_200_at.isoformat() if state.first_200_at is not None else None),
            coerce_timestamptz(state.last_200_at.isoformat() if state.last_200_at is not None else None),
            orjson.dumps(list(state.seen_404_season_ids)).decode("utf-8"),
            orjson.dumps(list(state.seen_200_season_ids)).decode("utf-8"),
            state.suppressed_hits_total,
            state.actual_probe_total,
            state.recheck_iteration,
            coerce_timestamptz(state.next_probe_after.isoformat() if state.next_probe_after is not None else None),
            coerce_timestamptz(state.probe_lease_until.isoformat() if state.probe_lease_until is not None else None),
            state.probe_lease_owner,
            state.last_http_status,
            state.last_job_type,
            state.last_trace_id,
            coerce_timestamptz(state.created_at.isoformat()),
            coerce_timestamptz(state.updated_at.isoformat()),
        )

    async def _insert_log(
        self,
        executor,
        *,
        event: WidgetAvailabilityEvent,
        classification_after: str | None,
        next_probe_after: datetime | None,
    ) -> None:
        await executor.execute(
            """
            INSERT INTO endpoint_availability_log (
                observed_at,
                unique_tournament_id,
                season_id,
                endpoint_pattern,
                job_type,
                trace_id,
                worker_id,
                scope_kind,
                decision,
                http_status,
                probe_latency_ms,
                classification_before,
                classification_after,
                next_probe_after,
                proxy_id,
                source_url,
                note
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, NULL, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
            )
            """,
            coerce_timestamptz(event.observed_at.isoformat()),
            event.unique_tournament_id,
            event.season_id,
            event.endpoint_pattern,
            event.job_type,
            event.trace_id,
            event.scope_kind,
            event.decision,
            event.http_status,
            event.probe_latency_ms,
            event.classification_before,
            classification_after,
            coerce_timestamptz(next_probe_after.isoformat() if next_probe_after is not None else None),
            event.proxy_id,
            event.source_url,
            event.note,
        )


def _state_from_row(row: Any) -> NegativeCacheState:
    return NegativeCacheState(
        scope_kind=str(row["scope_kind"]),
        unique_tournament_id=int(row["unique_tournament_id"]),
        season_id=int(row["season_id"]) if row["season_id"] is not None else None,
        endpoint_pattern=str(row["endpoint_pattern"]),
        classification=str(row["classification"]),
        first_404_at=_as_datetime(row["first_404_at"]),
        last_404_at=_as_datetime(row["last_404_at"]),
        first_200_at=_as_datetime(row["first_200_at"]),
        last_200_at=_as_datetime(row["last_200_at"]),
        seen_404_season_ids=_as_id_tuple(row["seen_404_season_ids_json"]),
        seen_200_season_ids=_as_id_tuple(row["seen_200_season_ids_json"]),
        suppressed_hits_total=int(row["suppressed_hits_total"] or 0),
        actual_probe_total=int(row["actual_probe_total"] or 0),
        recheck_iteration=int(row["recheck_iteration"] or 0),
        next_probe_after=_as_datetime(row["next_probe_after"]),
        probe_lease_until=_as_datetime(row["probe_lease_until"]),
        probe_lease_owner=row["probe_lease_owner"],
        last_http_status=int(row["last_http_status"]) if row["last_http_status"] is not None else None,
        last_job_type=row["last_job_type"],
        last_trace_id=row["last_trace_id"],
        created_at=_as_datetime(row["created_at"]) or datetime.now(UTC),
        updated_at=_as_datetime(row["updated_at"]) or datetime.now(UTC),
    )


def _as_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return parse_observed_at(str(value))


def _as_id_tuple(value: Any) -> tuple[int, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        try:
            payload = orjson.loads(value)
        except orjson.JSONDecodeError:
            return ()
    else:
        payload = value
    if not isinstance(payload, list):
        return ()
    return tuple(sorted({int(item) for item in payload}))


def _cache_key(scope_kind: str, unique_tournament_id: int, season_id: int | None, endpoint_pattern: str) -> str:
    if scope_kind == "season" and season_id is not None:
        return f"season:{int(unique_tournament_id)}:{int(season_id)}:{endpoint_pattern}"
    return f"tournament:{int(unique_tournament_id)}:{endpoint_pattern}"
