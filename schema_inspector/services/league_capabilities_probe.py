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
]
