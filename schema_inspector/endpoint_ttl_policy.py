"""Per-endpoint × per-status freshness TTL config (B1 Phase 0, 2026-05-16).

This module is the *configuration source of truth* for "how stale can a
specific event sub-endpoint be before we re-fetch it" — per status phase
(not-started / live / finished). The TTL table itself is curated by hand
from ``docs/football-matrix.md`` section 7 (Bobur-desired cadence).

Resolution call site:
  ``schema_inspector/pipeline/pilot_orchestrator.py:_event_detail_freshness_fields``

The resolver returns a number-of-seconds. The FreshnessStore (Redis TTL
wrapper) will then:
  * before fetch:  ``is_fresh(key)`` → if True, skip the HTTP call;
  * after success: ``mark_fetched(key, ttl_seconds)``.

If this module returns ``None`` for a given (sport, endpoint, phase) we
**fall through to the legacy global TTLs** (24h for static event details,
5min for player-level event details), preserving the old behaviour for
endpoints not yet ported into the matrix. That makes the rollout
strictly additive — only endpoints explicitly listed below get the new
per-status cadence; everything else keeps shipping unchanged.

The matrix below is **football-only** today. ``resolve_endpoint_ttl``
exits early for any other ``sport_slug`` so other sports keep their
existing freshness behaviour.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Status phases the resolver knows about.
STATUS_NOT_STARTED = "not_started"
STATUS_LIVE = "live"
STATUS_FINISHED = "finished"


@dataclass(frozen=True)
class EndpointTtlSpec:
    """TTL (seconds) per status phase. ``None`` = no per-phase override.

    Note: ``None`` for a phase does NOT mean "do not fetch" — that
    decision belongs to ``match_center_policy`` (whitelist). It only
    means "no per-endpoint TTL to enforce; fall through to legacy
    globals."
    """

    not_started: int | None = None
    live: int | None = None
    finished: int | None = None

    def for_phase(self, phase: str) -> int | None:
        normalized = (phase or "").strip().lower()
        if normalized in {"not_started", "notstarted", "scheduled"}:
            return self.not_started
        if normalized in {"live", "inprogress"}:
            return self.live
        if normalized in {"finished", "afterextra", "afterpen", "aet", "apen"}:
            return self.finished
        return None


# ---------------------------------------------------------------------------
# Bobur-desired football matrix
#
# Source: docs/football-matrix.md §7. Update both files together — code
# below is the runtime side, the markdown is what humans read and edit.
# ---------------------------------------------------------------------------
_SECONDS_PER_HOUR = 3_600
_SECONDS_PER_DAY = 24 * _SECONDS_PER_HOUR

_FOOTBALL_TTL_MATRIX: dict[str, EndpointTtlSpec] = {
    # core edges
    "/api/v1/event/{event_id}/lineups": EndpointTtlSpec(
        not_started=5 * 60,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/incidents": EndpointTtlSpec(
        not_started=None,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/statistics": EndpointTtlSpec(
        not_started=None,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/graph": EndpointTtlSpec(
        not_started=None,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    # high-traffic talk streams
    "/api/v1/event/{event_id}/comments": EndpointTtlSpec(
        not_started=None,
        # Bobur (§7): "live раз в минуту, finished раз в день".
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/official-tweets": EndpointTtlSpec(
        not_started=None,
        # Bobur (§7): "20×60s" — refresh every 20 minutes during live.
        live=20 * 60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/shotmap": EndpointTtlSpec(
        not_started=None,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/heatmap/{team_id}": EndpointTtlSpec(
        not_started=None,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/best-players/summary": EndpointTtlSpec(
        not_started=None,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/average-positions": EndpointTtlSpec(
        not_started=None,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    # meta endpoints (slow churn pre-game; pinned for finished)
    "/api/v1/event/{event_id}/managers": EndpointTtlSpec(
        not_started=_SECONDS_PER_HOUR,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/h2h": EndpointTtlSpec(
        not_started=_SECONDS_PER_HOUR,
        live=None,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{custom_id}/h2h/events": EndpointTtlSpec(
        not_started=_SECONDS_PER_HOUR,
        live=None,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/pregame-form": EndpointTtlSpec(
        not_started=_SECONDS_PER_HOUR,
        live=None,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/votes": EndpointTtlSpec(
        not_started=5 * 60,
        live=5 * 60,
        finished=_SECONDS_PER_DAY,
    ),
    # odds — live churn dominates; ~5min keeps the market table fresh
    # without blowing through provider rate-limits.
    "/api/v1/event/{event_id}/odds/{provider_id}/all": EndpointTtlSpec(
        not_started=5 * 60,
        live=5 * 60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/odds/{provider_id}/featured": EndpointTtlSpec(
        not_started=5 * 60,
        live=5 * 60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/provider/{provider_id}/winning-odds": EndpointTtlSpec(
        not_started=5 * 60,
        live=5 * 60,
        finished=_SECONDS_PER_DAY,
    ),
    # streaks
    "/api/v1/event/{event_id}/team-streaks": EndpointTtlSpec(
        not_started=_SECONDS_PER_HOUR,
        live=5 * 60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/team-streaks/betting-odds/{provider_id}": EndpointTtlSpec(
        not_started=_SECONDS_PER_HOUR,
        live=5 * 60,
        finished=_SECONDS_PER_DAY,
    ),
    # post-finish content
    "/api/v1/event/{event_id}/highlights": EndpointTtlSpec(
        not_started=None,
        live=None,
        finished=_SECONDS_PER_DAY,
    ),
    # per-player event endpoints (fan-out from lineups)
    "/api/v1/event/{event_id}/player/{player_id}/statistics": EndpointTtlSpec(
        not_started=None,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/player/{player_id}/rating-breakdown": EndpointTtlSpec(
        not_started=None,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/player/{player_id}/heatmap": EndpointTtlSpec(
        not_started=None,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/shotmap/player/{player_id}": EndpointTtlSpec(
        not_started=None,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
    "/api/v1/event/{event_id}/goalkeeper-shotmap/player/{player_id}": EndpointTtlSpec(
        not_started=None,
        live=60,
        finished=_SECONDS_PER_DAY,
    ),
}


# Map of supported sports → matrix. Today football-only; structure leaves
# room for tennis/basketball/etc. to plug in their own per-status TTLs.
_TTL_MATRICES_BY_SPORT: dict[str, dict[str, EndpointTtlSpec]] = {
    "football": _FOOTBALL_TTL_MATRIX,
}


def resolve_endpoint_ttl(
    *,
    sport_slug: str,
    endpoint_pattern: str,
    status_phase: str,
) -> int | None:
    """Return per-endpoint × per-status TTL in seconds, or ``None``.

    ``None`` means "no matrix entry — fall through to legacy globals".
    A positive integer means "skip fetch if we already fetched within
    this many seconds." A ``None`` result for a known endpoint+phase
    (whitelist gap) ALSO falls through — the caller cannot distinguish
    "no matrix entry at all" from "matrix entry but None for this
    phase", which is intentional: both cases are "no opinion from B1,
    keep legacy behaviour."
    """
    if not endpoint_pattern:
        return None
    normalized_sport = (sport_slug or "").strip().lower()
    matrix = _TTL_MATRICES_BY_SPORT.get(normalized_sport)
    if matrix is None:
        return None
    spec = matrix.get(endpoint_pattern)
    if spec is None:
        return None
    return spec.for_phase(status_phase)


def normalize_status_phase(status_phase: str | None) -> str:
    """Normalize raw status strings into the three phases the matrix
    understands. Falls back to ``"live"`` for unknown values so we err
    on the side of fresher polling (lower TTL).
    """
    raw = (status_phase or "").strip().lower()
    if raw in {"not_started", "notstarted", "scheduled"}:
        return STATUS_NOT_STARTED
    if raw in {"live", "inprogress"}:
        return STATUS_LIVE
    if raw in {"finished", "afterextra", "afterpen", "aet", "apen"}:
        return STATUS_FINISHED
    return STATUS_LIVE


def known_football_endpoint_patterns() -> tuple[str, ...]:
    """Read-only view of the football matrix keys, for ops / tests."""
    return tuple(_FOOTBALL_TTL_MATRIX.keys())
