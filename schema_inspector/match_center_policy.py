"""Fetch gates for match-center routes with high 404 risk.

The rules in this module are intentionally narrow: they only gate football
event child/detail routes using root payload fields already parsed into event
rows. Other sports keep their existing behavior.
"""

from __future__ import annotations

from typing import Iterable, TypeVar


# Stage 5.2 (2026-05-21): highlights delay bumped from 150 → 180
# minutes. Empirical observation: upstream highlights bucket is still
# empty at 150 minutes for top-tier matches (longer post-match editorial
# pipeline). 180 minutes (3 h) hits the bucket on the first try.
FOOTBALL_HIGHLIGHTS_DELAY_SECONDS = 180 * 60

_TIER_1_DETAIL_IDS = frozenset({1})
_TIER_2_DETAIL_IDS = frozenset({4, 6})
_TIER_3_DETAIL_IDS = frozenset({2, 3, 5})

_NOTSTARTED_STATUS_TYPES = frozenset({"notstarted", "scheduled"})
_LIVE_STATUS_TYPES = frozenset({"inprogress", "live"})
_FINISHED_STATUS_TYPES = frozenset({"finished", "afterextra", "afterpen", "aet", "apen"})

_NOTSTARTED_CORE_EDGES = frozenset({"incidents", "lineups"})
_ACTIVE_CORE_EDGES = frozenset({"statistics", "lineups", "incidents", "graph"})

# Stage 5.2 (2026-05-21): split pre-match detail endpoints into a
# cold lane (static metadata, fetched 1-14 days before kickoff) and a
# hot lane (cold + dynamic editorial picks like odds and votes,
# activated within 24 h of kickoff). The split prevents wasting proxy
# budget on odds/votes for matches 10 days out — they rotate too
# frequently and the value of an early fetch is near zero.
FOOTBALL_PREMATCH_HOT_WINDOW_SECONDS = 24 * 3600

_FOOTBALL_PREMATCH_COLD_DETAIL_ENDPOINTS = frozenset(
    {
        "/api/v1/event/{event_id}/managers",
        "/api/v1/event/{event_id}/h2h",
        "/api/v1/event/{custom_id}/h2h/events",
        "/api/v1/event/{event_id}/pregame-form",
        "/api/v1/event/{event_id}/team-streaks",
    }
)
_FOOTBALL_PREMATCH_HOT_ONLY_DETAIL_ENDPOINTS = frozenset(
    {
        "/api/v1/event/{event_id}/votes",
        "/api/v1/event/{event_id}/odds/{provider_id}/all",
        "/api/v1/event/{event_id}/odds/{provider_id}/featured",
        "/api/v1/event/{event_id}/provider/{provider_id}/winning-odds",
        "/api/v1/event/{event_id}/team-streaks/betting-odds/{provider_id}",
    }
)
# Full pre-match envelope (hot lane). Kept as a single name so existing
# callers (e.g. tier_5 path below, _allowed_detail_patterns_for_status)
# continue to see the complete pre-match allowlist when caller does not
# pass start_timestamp / now_timestamp.
_FOOTBALL_NOTSTARTED_DETAIL_ENDPOINTS = (
    _FOOTBALL_PREMATCH_COLD_DETAIL_ENDPOINTS
    | _FOOTBALL_PREMATCH_HOT_ONLY_DETAIL_ENDPOINTS
)
_FOOTBALL_INPROGRESS_DETAIL_ENDPOINTS = _FOOTBALL_NOTSTARTED_DETAIL_ENDPOINTS | {
    "/api/v1/event/{event_id}/graph",
    "/api/v1/event/{event_id}/comments",
    "/api/v1/event/{event_id}/best-players/summary",
    "/api/v1/event/{event_id}/official-tweets",
    "/api/v1/event/{event_id}/heatmap/{team_id}",
    "/api/v1/event/{event_id}/shotmap",
    "/api/v1/event/{event_id}/average-positions",
}
_FOOTBALL_FINISHED_DETAIL_ENDPOINTS = _FOOTBALL_INPROGRESS_DETAIL_ENDPOINTS | {
    "/api/v1/event/{event_id}/highlights",
}

_TIER_2_DETAIL_ENDPOINTS = frozenset(
    {
        "/api/v1/event/{event_id}/managers",
        "/api/v1/event/{event_id}/h2h",
        "/api/v1/event/{event_id}/pregame-form",
        "/api/v1/event/{event_id}/votes",
        "/api/v1/event/{event_id}/odds/{provider_id}/all",
        "/api/v1/event/{event_id}/odds/{provider_id}/featured",
        "/api/v1/event/{event_id}/provider/{provider_id}/winning-odds",
        "/api/v1/event/{event_id}/team-streaks",
        "/api/v1/event/{event_id}/team-streaks/betting-odds/{provider_id}",
        "/api/v1/event/{custom_id}/h2h/events",
        "/api/v1/event/{event_id}/graph",
        "/api/v1/event/{event_id}/official-tweets",
        "/api/v1/event/{event_id}/average-positions",
    }
)

_HEATMAP_ENDPOINT = "/api/v1/event/{event_id}/heatmap/{team_id}"
_SHOTMAP_ENDPOINT = "/api/v1/event/{event_id}/shotmap"
_COMMENTS_ENDPOINT = "/api/v1/event/{event_id}/comments"
_HIGHLIGHTS_ENDPOINT = "/api/v1/event/{event_id}/highlights"
_PLAYER_HEATMAP_ENDPOINT = "/api/v1/event/{event_id}/player/{player_id}/heatmap"
_PLAYER_SHOTMAP_ENDPOINT = "/api/v1/event/{event_id}/shotmap/player/{player_id}"
_GOALKEEPER_SHOTMAP_ENDPOINT = "/api/v1/event/{event_id}/goalkeeper-shotmap/player/{player_id}"

_PLAYER_STAT_SPECIALS = frozenset({"best_players_summary", "event_player_statistics"})
_PLAYER_RATING_SPECIALS = frozenset({"event_player_rating_breakdown"})
_PLAYER_HEATMAP_SPECIALS = frozenset({"event_player_heatmap"})
_PLAYER_SHOTMAP_SPECIALS = frozenset({"event_player_shotmap", "event_goalkeeper_shotmap"})

T = TypeVar("T")


def football_detail_tier(detail_id: int | None) -> str:
    """Map SofaScore football detailId to a local coverage tier."""
    normalized_detail_id = _as_int(detail_id)
    if normalized_detail_id in _TIER_1_DETAIL_IDS:
        return "tier_1"
    if normalized_detail_id in _TIER_2_DETAIL_IDS:
        return "tier_2"
    if normalized_detail_id in _TIER_3_DETAIL_IDS:
        return "tier_3"
    return "tier_5"


def football_edge_allowed(
    *,
    sport_slug: str | None,
    edge_kind: str,
    detail_id: int | None,
    status_type: str | None,
    has_xg: bool | None,
    is_editor: bool | None = None,
    # Phase 4.7 (2026-05-23): League Capabilities Registry verdict for
    # this (UT, season, status, endpoint_pattern). None / 'unknown' →
    # caller did not consult the registry, or registry has no entry —
    # we fall back to the legacy tier-based logic below. 'allowed' /
    # 'disabled' SHORT-CIRCUIT the legacy gating (subject to the
    # is_editor HARD BAN which always wins).
    capability_verdict: str | None = None,
) -> bool:
    """Return whether a planned core edge should be fetched for football.

    X'' patch (2026-05-12) introduces two behaviour changes versus the
    legacy ``detailId``-only gate:

    * ``is_editor=True`` is a **HARD BAN** — events published via the
      SofaEditor crowdsourcing app are excluded from every matchcenter
      endpoint regardless of ``detailId``, ``status_type``, or capability
      flags. Empirical sample (67-event audit) showed 100% of
      ``isEditor=True`` events are amateur leagues with crowd-sourced
      coverage; the user policy is to never parse this data path.
    * ``edge_kind="incidents"`` is unlocked for non-isEditor football
      events even when ``detailId`` is missing. The 67-event audit
      confirmed 100% upstream availability of ``/incidents`` regardless
      of detailId — the legacy ``tier_5`` block was over-conservative.
    * ``edge_kind="lineups"`` is unlocked for non-isEditor football events
      regardless of ``detailId`` AND regardless of ``status_type``. The
      previous "block pre-match" heuristic was wrong: upstream returns
      ``/lineups`` HTTP 200 with ``confirmed=false`` (probable/predicted
      lineup) before kickoff and ``confirmed=true`` (official) once the
      sheet is published. ``confirmed=false`` is real data, not absence.

    All other edges keep the legacy tier-based gating exactly.
    """
    if _normalize_sport(sport_slug) != "football":
        return True

    normalized_edge = str(edge_kind or "").strip().lower()
    if normalized_edge == "meta":
        return True

    # X'' HARD BAN — SofaEditor app data is never fetched downstream of
    # ROOT. Must precede every gate decision so even bypass paths honour it.
    if is_editor is True:
        return False

    # Phase 4.7: League Capabilities Registry verdict (when present)
    # is authoritative — it's measured per-UT data instead of guesses
    # off detailId. Stays AFTER is_editor (the HARD BAN must precede
    # every other gate per X'' policy).
    if capability_verdict == "allowed":
        return True
    if capability_verdict == "disabled":
        return False
    # 'unknown' or None → fall through to legacy tier-based gating.

    normalized_status = _normalize_status(status_type)
    tier = football_detail_tier(detail_id)

    # X'' /incidents unlock — 100% upstream availability for non-isEditor
    # football. Bypasses the ``tier_5`` block that previously dropped 98%
    # of football events (detailId missing) from matchcenter coverage.
    if normalized_edge == "incidents":
        return True

    # X'' /lineups unlock for tier_5 (no detailId), all statuses.
    # Upstream returns 200 with ``confirmed=false`` (probable/predicted)
    # before kickoff and ``confirmed=true`` (official) once the sheet is
    # published — both are valid matchcenter data. tier_3 stays blocked
    # because the legacy ``problem`` cohort had genuinely bad coverage.
    if normalized_edge == "lineups":
        if tier in {"tier_1", "tier_2", "tier_5"}:
            return True
        # tier_3 stays blocked (legacy ``problem detail`` cohort).
        return False

    # All other edges: legacy tier-based gating, unchanged semantics.
    if normalized_status in _NOTSTARTED_STATUS_TYPES:
        if tier == "tier_1":
            return normalized_edge in _NOTSTARTED_CORE_EDGES
        if tier == "tier_2":
            return normalized_edge in _NOTSTARTED_CORE_EDGES
        return False
    if tier == "tier_1":
        return normalized_edge in _ACTIVE_CORE_EDGES
    if tier == "tier_2":
        return normalized_edge in _ACTIVE_CORE_EDGES
    if tier == "tier_3":
        return normalized_edge == "incidents"
    return False


def football_special_allowed(
    *,
    sport_slug: str | None,
    special_kind: str,
    detail_id: int | None,
    has_event_player_statistics: bool | None,
    has_event_player_heat_map: bool | None,
    has_xg: bool | None,
    is_editor: bool | None = None,
) -> bool:
    """Gate per-player/best-player followups generated from lineups.

    X'' patch: ``is_editor=True`` is a **HARD BAN** for all player
    followups — best-players summary, per-player statistics, rating
    breakdowns, heatmaps, and shotmaps are skipped for SofaEditor events.
    Existing tier-based gating is preserved for non-isEditor events.
    """
    if _normalize_sport(sport_slug) != "football":
        return True

    # X'' HARD BAN.
    if is_editor is True:
        return False

    normalized_special = str(special_kind or "").strip().lower()
    tier = football_detail_tier(detail_id)
    if normalized_special in _PLAYER_HEATMAP_SPECIALS:
        return has_event_player_heat_map is True and tier in {"tier_1", "tier_2"}
    if normalized_special in _PLAYER_SHOTMAP_SPECIALS:
        return has_xg is True and tier in {"tier_1", "tier_2"}
    if tier == "tier_1":
        return True
    if tier == "tier_2":
        return has_event_player_statistics is True and normalized_special in _PLAYER_STAT_SPECIALS
    if (
        normalized_special in _PLAYER_STAT_SPECIALS
        or normalized_special in _PLAYER_RATING_SPECIALS
        or normalized_special in _PLAYER_HEATMAP_SPECIALS
        or normalized_special in _PLAYER_SHOTMAP_SPECIALS
    ):
        return False
    return False


def football_detail_endpoint_allowed(
    *,
    sport_slug: str | None,
    endpoint_pattern: str,
    detail_id: int | None,
    status_type: str | None,
    has_xg: bool | None,
    has_event_player_heat_map: bool | None,
    has_event_player_statistics: bool | None,
    has_global_highlights: bool | None,
    start_timestamp: int | None,
    now_timestamp: int | None,
    is_editor: bool | None = None,
    # Phase 4.7 (2026-05-23): League Capabilities Registry verdict
    # (same semantics as football_edge_allowed.capability_verdict).
    capability_verdict: str | None = None,
) -> bool:
    """Return whether a non-core event detail endpoint should be fetched.

    X'' patch: ``is_editor=True`` is a **HARD BAN** for football. Every
    detail endpoint (managers, h2h, pregame-form, votes, odds, comments,
    best-players, shotmap, heatmap, average-positions, highlights, …) is
    skipped for SofaEditor events.
    """
    if _normalize_sport(sport_slug) != "football":
        return True

    # X'' HARD BAN.
    if is_editor is True:
        return False

    # Phase 4.7: League Capabilities Registry verdict short-circuits
    # the legacy detail-endpoint gating when present (always after
    # is_editor HARD BAN).
    if capability_verdict == "allowed":
        return True
    if capability_verdict == "disabled":
        return False
    # 'unknown' or None → fall through to legacy logic.

    normalized_pattern = str(endpoint_pattern or "").strip()
    normalized_status = _normalize_status(status_type)
    tier = football_detail_tier(detail_id)

    if normalized_pattern == _HIGHLIGHTS_ENDPOINT:
        return football_highlights_allowed(
            sport_slug=sport_slug,
            detail_id=detail_id,
            status_type=status_type,
            has_global_highlights=has_global_highlights,
            start_timestamp=start_timestamp,
            now_timestamp=now_timestamp,
            is_editor=is_editor,
        )
    allowed_patterns = _allowed_detail_patterns_for_status(normalized_status)
    if normalized_pattern not in allowed_patterns:
        return False
    # Stage 5.2 (2026-05-21): pre-match cold/hot split. For notstarted
    # events the dynamic editorial endpoints (odds, votes, winning-odds,
    # betting-odds team-streaks) are gated to the hot window
    # (<=24h until kickoff). Static endpoints (managers, h2h, pregame-
    # form, team-streaks, h2h/events) remain available across the full
    # 1-14 day cold window.
    if (
        normalized_status in _NOTSTARTED_STATUS_TYPES
        and normalized_pattern in _FOOTBALL_PREMATCH_HOT_ONLY_DETAIL_ENDPOINTS
        and not _is_within_prematch_hot_window(start_timestamp, now_timestamp)
    ):
        return False
    # X3 patch (2026-05-12): tier_5 (= detailId missing on root payload) is
    # the dominant cohort — empirical UI audit on Premier League pre-match
    # event 15999228 (uniqueTournament=17) showed `detailId` is simply not
    # set in the upstream root payload yet, but upstream nevertheless
    # returns 200 for the pre-match detail bundle (managers, h2h,
    # pregame-form, votes, odds, winning-odds, team-streaks, h2h/events).
    # The old "tier_3 OR tier_5 → block everything" rule was wrong: it
    # collapsed ~93% of football events into the "no coverage" cohort even
    # though upstream had data. We now allow ``_FOOTBALL_NOTSTARTED_DETAIL_ENDPOINTS``
    # for tier_5; premium endpoints (graph, comments, heatmap, shotmap,
    # best-players, official-tweets, average-positions, highlights) stay
    # blocked because those genuinely require tier_1/tier_2 coverage on
    # the root payload. tier_3 (= detailId ∈ {2, 3, 5}) stays fully blocked
    # because its empirical cohort is the legacy "problem detail" group
    # where upstream coverage is genuinely poor.
    if tier == "tier_5":
        if normalized_pattern in _FOOTBALL_NOTSTARTED_DETAIL_ENDPOINTS:
            return True
        return False
    if tier == "tier_3":
        return False
    if normalized_pattern == _COMMENTS_ENDPOINT and tier != "tier_1":
        return False
    if normalized_pattern == _HEATMAP_ENDPOINT:
        return has_event_player_heat_map is True and tier in {"tier_1", "tier_2"}
    if normalized_pattern == _SHOTMAP_ENDPOINT:
        return has_xg is True and tier in {"tier_1", "tier_2"}
    if tier == "tier_1":
        return True
    if tier == "tier_2":
        return normalized_pattern in _TIER_2_DETAIL_ENDPOINTS
    return False


def football_highlights_allowed(
    *,
    sport_slug: str | None,
    detail_id: int | None,
    status_type: str | None,
    has_global_highlights: bool | None,
    start_timestamp: int | None,
    now_timestamp: int | None,
    is_editor: bool | None = None,
) -> bool:
    """Allow highlights only after a finished football match has aged out.

    X'' patch: ``is_editor=True`` is a **HARD BAN** — highlights for
    SofaEditor events are never fetched.
    """
    if _normalize_sport(sport_slug) != "football":
        return False
    # X'' HARD BAN.
    if is_editor is True:
        return False
    if football_detail_tier(detail_id) not in {"tier_1", "tier_2"}:
        return False
    if has_global_highlights is not True:
        return False
    if _normalize_status(status_type) not in _FINISHED_STATUS_TYPES:
        return False
    start_ts = _as_int(start_timestamp)
    now_ts = _as_int(now_timestamp)
    if start_ts is None or now_ts is None:
        return False
    return now_ts - start_ts >= FOOTBALL_HIGHLIGHTS_DELAY_SECONDS


def filter_football_detail_specs(
    specs: Iterable[T],
    *,
    sport_slug: str | None,
    detail_id: int | None,
    status_type: str | None,
    has_xg: bool | None,
    has_event_player_heat_map: bool | None,
    has_event_player_statistics: bool | None,
    has_global_highlights: bool | None,
    start_timestamp: int | None,
    now_timestamp: int | None,
    is_editor: bool | None = None,
    # Phase 4.7.3 (2026-05-23): per-endpoint League Capabilities Registry
    # verdicts. The dict maps ``endpoint_pattern -> 'allowed'/'disabled'/
    # 'unknown'``; entries absent from the dict (or a None map) fall back
    # to legacy tier-based gating in ``football_detail_endpoint_allowed``.
    # Resolved by the orchestrator from the registry before calling
    # ``build_event_detail_request_specs``.
    capability_verdicts: dict[str, str] | None = None,
) -> tuple[T, ...]:
    """Filter EventDetailRequestSpec-like objects by endpoint.pattern.

    X'' patch: ``is_editor=True`` short-circuits the whole list to empty
    for football (HARD BAN). The per-spec gate via
    ``football_detail_endpoint_allowed`` also honours ``is_editor`` so
    this is belt-and-suspenders — short-circuit avoids per-spec overhead.
    """
    if _normalize_sport(sport_slug) != "football":
        return tuple(specs)

    # X'' HARD BAN — empty tuple.
    if is_editor is True:
        return ()

    filtered: list[T] = []
    for spec in specs:
        endpoint = getattr(spec, "endpoint", None)
        endpoint_pattern = getattr(endpoint, "pattern", None)
        if endpoint_pattern is None:
            continue
        pattern_str = str(endpoint_pattern)
        verdict_for_pattern = (
            capability_verdicts.get(pattern_str)
            if capability_verdicts
            else None
        )
        if football_detail_endpoint_allowed(
            sport_slug=sport_slug,
            endpoint_pattern=pattern_str,
            detail_id=detail_id,
            status_type=status_type,
            has_xg=has_xg,
            has_event_player_heat_map=has_event_player_heat_map,
            has_event_player_statistics=has_event_player_statistics,
            has_global_highlights=has_global_highlights,
            start_timestamp=start_timestamp,
            now_timestamp=now_timestamp,
            is_editor=is_editor,
            capability_verdict=verdict_for_pattern,
        ):
            filtered.append(spec)
    return tuple(filtered)


def _normalize_sport(sport_slug: str | None) -> str:
    return str(sport_slug or "").strip().lower()


def _normalize_status(status_type: str | None) -> str:
    return str(status_type or "").strip().lower()


def _allowed_detail_patterns_for_status(status_type: str) -> frozenset[str]:
    if status_type in _NOTSTARTED_STATUS_TYPES:
        return _FOOTBALL_NOTSTARTED_DETAIL_ENDPOINTS
    if status_type in _LIVE_STATUS_TYPES:
        return _FOOTBALL_INPROGRESS_DETAIL_ENDPOINTS
    if status_type in _FINISHED_STATUS_TYPES:
        return _FOOTBALL_FINISHED_DETAIL_ENDPOINTS
    return _FOOTBALL_NOTSTARTED_DETAIL_ENDPOINTS


def _as_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_within_prematch_hot_window(
    start_timestamp: int | None, now_timestamp: int | None
) -> bool:
    """True when kickoff is at most ``FOOTBALL_PREMATCH_HOT_WINDOW_SECONDS``
    away (or in the past — though notstarted + past kickoff is an odd
    state). Used to gate dynamic editorial endpoints in pre-match.

    Returns False conservatively when either timestamp is missing so a
    bare planner publish does not accidentally enable hot-lane fetches
    for matches whose start time hasn't been ingested yet.
    """

    start_ts = _as_int(start_timestamp)
    now_ts = _as_int(now_timestamp)
    if start_ts is None or now_ts is None:
        return False
    return (start_ts - now_ts) <= FOOTBALL_PREMATCH_HOT_WINDOW_SECONDS
