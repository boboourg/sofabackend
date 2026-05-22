"""League Capabilities Probe Service (Phase 4.2, 2026-05-23).

Discovers which Sofascore endpoints are actually published per
``(unique_tournament_id, season_id, status_type)`` cohort. Pure-Python
verdict aggregator + orchestration shapes; the HTTP fetching layer is
injected by callers (so the unit tests don't need a live proxy stack).

Algorithm:

  1. Select up to N (default 5) recent events for the (UT, season,
     status) cohort.
  2. For each endpoint pattern in the configured probe set: issue a
     GET / HEAD against /api/v1/event/{event_id}/<endpoint> for each
     sample event.
  3. Classify the response per sample: 200 OK with non-empty payload
     = success; 404 = hard miss; 200 OK with empty payload = soft
     miss; other (5xx, timeout, proxy block) = error (transient).
  4. Aggregate per-endpoint verdict:
       success_rate (ok / total) >= 0.6  → allowed
       negative_rate (404 + empty) >= 0.6 → disabled
       otherwise                          → unknown
  5. Emit one ``CapabilityUpsert`` per endpoint to be persisted by the
     repository.

The thresholds match the Phase 4 design doc — 3-of-5 majority
verdicts. Errors (transient) push neither toward allowed nor toward
disabled — they leave the verdict 'unknown' so the next probe pass
re-evaluates.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping, Sequence

from ..storage.league_capabilities_repository import (
    STATE_ALLOWED,
    STATE_DISABLED,
    STATE_UNKNOWN,
    SOURCE_PROBE,
    CapabilityUpsert,
)


# 3-of-5 majority. Stored as float so the aggregator works for arbitrary
# sample sizes (e.g. cup-style competitions where only 2 recent events
# exist — 2/2 → 1.0 >= 0.6 → allowed).
_ALLOWED_SUCCESS_RATE = 0.6

# Same threshold for the negative-signal majority. 404 + empty payload
# both count toward "disabled" (empty payload means the endpoint exists
# but Sofascore returns no data for this league — same effect for us).
_DISABLED_NEGATIVE_RATE = 0.6

# Default TTL for probe verdicts. 14 days matches the Phase 4 design
# doc. Manual overrides override this; refresh daemon re-probes
# expired rows.
_DEFAULT_TTL = timedelta(days=14)


@dataclass(frozen=True)
class ProbeSampleOutcome:
    """One HTTP probe result against one event × one endpoint.

    Bool flags are derived from HTTP status + payload — the fetcher
    pre-classifies them so the aggregator doesn't need to know about
    payload shapes per endpoint."""

    event_id: int
    status: int
    payload_empty: bool
    is_error: bool = False  # transient: TLS, 5xx, proxy 403, rate-limit


@dataclass(frozen=True)
class VerdictResult:
    state: str  # 'allowed' | 'disabled' | 'unknown'
    confidence_score: float | None
    ok: int
    http_404: int
    empty: int
    error: int
    total: int


def aggregate_verdict(
    *,
    ok: int,
    http_404: int,
    empty: int,
    error: int,
    total: int,
) -> VerdictResult:
    """Pure aggregator — see module docstring for thresholds."""

    if total <= 0:
        return VerdictResult(
            state=STATE_UNKNOWN,
            confidence_score=None,
            ok=ok, http_404=http_404, empty=empty, error=error, total=total,
        )

    success_rate = ok / total
    negative_signals = http_404 + empty
    negative_rate = negative_signals / total

    if success_rate >= _ALLOWED_SUCCESS_RATE:
        state = STATE_ALLOWED
    elif negative_rate >= _DISABLED_NEGATIVE_RATE:
        state = STATE_DISABLED
    else:
        # Transient errors dominant OR ambiguous mix — leave for re-probe.
        state = STATE_UNKNOWN

    return VerdictResult(
        state=state,
        confidence_score=round(success_rate, 3),
        ok=ok, http_404=http_404, empty=empty, error=error, total=total,
    )


def _classify_outcome(outcome: ProbeSampleOutcome) -> str:
    """Bucket one outcome into one of {'ok','404','empty','error'}."""

    if outcome.is_error:
        return "error"
    if outcome.status == 404:
        return "404"
    if outcome.status >= 400:
        # Treat other 4xx/5xx as transient error rather than hard miss
        # (avoids classifying e.g. 502/503 as 'disabled' permanently).
        return "error"
    if outcome.payload_empty:
        return "empty"
    return "ok"


def build_capability_upserts(
    *,
    unique_tournament_id: int,
    season_id: int | None,
    status_type: str,
    sample_outcomes_by_endpoint: Mapping[str, Sequence[ProbeSampleOutcome]],
    expires_at: datetime | None = None,
    source: str = SOURCE_PROBE,
    notes: str | None = None,
    now_factory=None,
) -> list[CapabilityUpsert]:
    """Materialize one ``CapabilityUpsert`` per endpoint pattern from
    a batch of sample outcomes. Used by the probe service after it
    has fetched all probe samples for a (UT, season, status) cohort.
    """

    now_fn = now_factory or (lambda: datetime.now(timezone.utc))
    if expires_at is None:
        expires_at = now_fn() + _DEFAULT_TTL

    upserts: list[CapabilityUpsert] = []
    for endpoint_pattern, outcomes in sample_outcomes_by_endpoint.items():
        counters = {"ok": 0, "404": 0, "empty": 0, "error": 0}
        for outcome in outcomes:
            counters[_classify_outcome(outcome)] += 1
        total = sum(counters.values())
        verdict = aggregate_verdict(
            ok=counters["ok"],
            http_404=counters["404"],
            empty=counters["empty"],
            error=counters["error"],
            total=total,
        )
        upserts.append(
            CapabilityUpsert(
                unique_tournament_id=int(unique_tournament_id),
                season_id=season_id if season_id is None else int(season_id),
                status_type=str(status_type),
                endpoint_pattern=str(endpoint_pattern),
                state=verdict.state,
                probe_samples_total=verdict.total,
                probe_samples_ok=verdict.ok,
                probe_samples_http_404=verdict.http_404,
                probe_samples_empty=verdict.empty,
                probe_samples_error=verdict.error,
                confidence_score=verdict.confidence_score,
                expires_at=expires_at,
                source=source,
                notes=notes,
            )
        )
    return upserts


__all__ = [
    "ProbeSampleOutcome",
    "VerdictResult",
    "aggregate_verdict",
    "build_capability_upserts",
    "ProbeExecutor",
    "ProbeReport",
    "PROBE_ENDPOINTS_BY_STATUS",
]


# Endpoint sets per status_type. Each value is a tuple of endpoint
# patterns the probe service issues GETs against. These sets pin the
# scope of probing so it never grows uncontrolled — operator-tunable
# via env override (planned ops surface).
PROBE_ENDPOINTS_BY_STATUS: dict[str, tuple[str, ...]] = {
    "inprogress": (
        "/api/v1/event/{event_id}",
        "/api/v1/event/{event_id}/incidents",
        "/api/v1/event/{event_id}/statistics",
        "/api/v1/event/{event_id}/lineups",
        "/api/v1/event/{event_id}/best-players/summary",
        "/api/v1/event/{event_id}/comments",
        "/api/v1/event/{event_id}/graph",
        "/api/v1/event/{event_id}/votes",
        "/api/v1/event/{event_id}/shotmap",
        "/api/v1/event/{event_id}/h2h",
        "/api/v1/event/{event_id}/managers",
        "/api/v1/event/{event_id}/pregame-form",
    ),
    "finished": (
        # All inprogress endpoints continue to make sense post-match
        # PLUS post-match-only surfaces.
        "/api/v1/event/{event_id}",
        "/api/v1/event/{event_id}/incidents",
        "/api/v1/event/{event_id}/statistics",
        "/api/v1/event/{event_id}/lineups",
        "/api/v1/event/{event_id}/best-players/summary",
        "/api/v1/event/{event_id}/comments",
        "/api/v1/event/{event_id}/graph",
        "/api/v1/event/{event_id}/shotmap",
        "/api/v1/event/{event_id}/h2h",
        "/api/v1/event/{event_id}/managers",
        "/api/v1/event/{event_id}/pregame-form",
        "/api/v1/event/{event_id}/highlights",
    ),
    "notstarted": (
        "/api/v1/event/{event_id}",
        "/api/v1/event/{event_id}/h2h",
        "/api/v1/event/{event_id}/managers",
        "/api/v1/event/{event_id}/pregame-form",
        "/api/v1/event/{event_id}/votes",
        "/api/v1/event/{event_id}/team-streaks",
        "/api/v1/event/{event_id}/lineups",  # warmup probable lineups
    ),
}


@dataclass(frozen=True)
class ProbeReport:
    """Summary of one probe pass for (UT, season, status)."""

    unique_tournament_id: int
    season_id: int | None
    status_type: str
    samples_used: int
    upserts_count: int
    by_endpoint: dict[str, str]  # endpoint_pattern → state
    elapsed_seconds: float
    errors: list[str]


class ProbeExecutor:
    """Phase 4.4 (2026-05-23): orchestrate one probe pass.

      1. SELECT last N events for (UT, season, status) from DB.
      2. For each endpoint × event: GET, classify outcome.
      3. Aggregate verdicts and persist upserts via repository.
      4. Invalidate Redis cache so new verdicts become visible
         immediately to the orchestrator.

    The HTTP client interface is minimal — ``get_json(event_id,
    endpoint_pattern)`` returns ``{'status': int, 'payload': dict|None}``
    or raises on transient failure. Production wires
    ``SofascoreClient`` here; tests use a fake.
    """

    def __init__(
        self,
        *,
        database,
        client,
        repository,
        registry,
        samples_per_endpoint: int = 5,
    ) -> None:
        self.database = database
        self.client = client
        self.repository = repository
        self.registry = registry
        self.samples_per_endpoint = max(1, int(samples_per_endpoint))

    async def probe(
        self,
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
        endpoint_patterns: tuple[str, ...],
        samples_per_endpoint: int | None = None,
    ) -> ProbeReport:
        import time
        started = time.perf_counter()
        n_samples = int(samples_per_endpoint or self.samples_per_endpoint)
        errors: list[str] = []

        # Step 1: sample selection.
        event_ids = await self._select_sample_events(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            status_type=status_type,
            limit=n_samples,
        )

        # Step 2: probe (event × endpoint).
        sample_outcomes_by_endpoint: dict[str, list[ProbeSampleOutcome]] = {
            ep: [] for ep in endpoint_patterns
        }
        for endpoint_pattern in endpoint_patterns:
            for event_id in event_ids:
                outcome = await self._probe_one(event_id, endpoint_pattern, errors)
                sample_outcomes_by_endpoint[endpoint_pattern].append(outcome)

        # Step 3: aggregate + upsert.
        upserts = build_capability_upserts(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            status_type=status_type,
            sample_outcomes_by_endpoint=sample_outcomes_by_endpoint,
        )
        # Cover endpoints whose sample list was empty (no events
        # selected at all) so they still get a row in DB.
        if not event_ids:
            from datetime import datetime, timezone, timedelta
            upserts = [
                CapabilityUpsert(
                    unique_tournament_id=int(unique_tournament_id),
                    season_id=season_id if season_id is None else int(season_id),
                    status_type=str(status_type),
                    endpoint_pattern=ep,
                    state=STATE_UNKNOWN,
                    probe_samples_total=0,
                    probe_samples_ok=0,
                    probe_samples_http_404=0,
                    probe_samples_empty=0,
                    probe_samples_error=0,
                    confidence_score=None,
                    expires_at=datetime.now(timezone.utc) + timedelta(days=14),
                    source=SOURCE_PROBE,
                    notes="no sample events available",
                )
                for ep in endpoint_patterns
            ]

        async with self.database.connection() as connection:
            for row in upserts:
                await self.repository.upsert_capability(connection, row=row)

        # Step 4: invalidate Redis cache so the new verdicts win on
        # the next orchestrator read.
        try:
            await self.registry.invalidate_quad(
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                status_type=status_type,
                endpoint_patterns=endpoint_patterns,
            )
        except Exception as exc:  # pragma: no cover — defensive
            errors.append(f"invalidate_quad failed: {exc}")

        elapsed = time.perf_counter() - started
        return ProbeReport(
            unique_tournament_id=int(unique_tournament_id),
            season_id=season_id,
            status_type=str(status_type),
            samples_used=len(event_ids),
            upserts_count=len(upserts),
            by_endpoint={u.endpoint_pattern: u.state for u in upserts},
            elapsed_seconds=elapsed,
            errors=errors,
        )

    async def _select_sample_events(
        self,
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
        limit: int,
    ) -> list[int]:
        """Return list of event_ids for the (UT, season, status_type)
        cohort, newest first. Caller decides what to do if empty."""

        # event table stores status_code (int), not status_type (text).
        # JOIN event_status to translate status_type='finished' etc.
        # into the matching code set (Sofascore has multiple codes
        # per type: finished=100/110, afterextra/afterpen/etc).
        async with self.database.connection() as connection:
            rows = await connection.fetch(
                """
                SELECT e.id
                FROM event e
                JOIN event_status es ON es.code = e.status_code
                WHERE e.unique_tournament_id = $1
                  AND (e.season_id = $2 OR $2 IS NULL)
                  AND es.type = $3
                ORDER BY e.start_timestamp DESC NULLS LAST
                LIMIT $4
                """,
                int(unique_tournament_id),
                None if season_id is None else int(season_id),
                str(status_type),
                int(limit),
            )
        return [int(row["id"]) for row in rows]

    async def _probe_one(
        self,
        event_id: int,
        endpoint_pattern: str,
        errors: list,
    ) -> ProbeSampleOutcome:
        """One HTTP probe. Wraps client.get_json with classification
        of status / empty payload / transient exceptions."""

        try:
            outcome = await self.client.get_json(
                event_id=event_id, endpoint_pattern=endpoint_pattern
            )
        except Exception as exc:  # transient — TLS, 5xx, proxy, etc.
            errors.append(f"event={event_id} ep={endpoint_pattern}: {exc}")
            return ProbeSampleOutcome(
                event_id=event_id, status=0, payload_empty=False, is_error=True,
            )
        status = int(outcome.get("status") or 0)
        payload = outcome.get("payload")
        payload_empty = _is_payload_empty(payload)
        return ProbeSampleOutcome(
            event_id=event_id,
            status=status,
            payload_empty=payload_empty,
            is_error=False,
        )


def _is_payload_empty(payload) -> bool:
    """Heuristic: payload is considered empty when:
      * None
      * dict where every value is empty list / dict / None
      * dict with an explicit ``error`` envelope (Sofascore soft-error)
    Anything else counts as non-empty (parser will figure out shape)."""

    if payload is None:
        return True
    if not isinstance(payload, dict):
        return False
    if "error" in payload and isinstance(payload.get("error"), dict):
        return True
    if not payload:
        return True
    for value in payload.values():
        if value is None:
            continue
        if isinstance(value, (list, tuple)) and len(value) == 0:
            continue
        if isinstance(value, dict) and not value:
            continue
        return False
    return True
