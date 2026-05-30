"""Routing policy for physically sharded live refresh streams.

Logical live tracking still uses the existing hot/warm/cold model, but active
matches are dispatched into one of three independent live streams:

* tier_1 — top coverage / top audience
* tier_2 — normal live coverage
* tier_3 — long-tail / niche live coverage
"""

from __future__ import annotations

import os

from .match_center_policy import football_detail_tier

LIVE_TIER_1 = "tier_1"
LIVE_TIER_2 = "tier_2"
LIVE_TIER_3 = "tier_3"
LIVE_DISPATCH_TIERS = (LIVE_TIER_1, LIVE_TIER_2, LIVE_TIER_3)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


# Based on the current production distribution snapshot for live tournaments on
# 2026-04-27: p75 ~ 959 user_count, p95 ~ 6627 user_count.
LIVE_TIER_1_MIN_USER_COUNT = _env_int("LIVE_TIER_1_MIN_USER_COUNT", 6500)
LIVE_TIER_2_MIN_USER_COUNT = _env_int("LIVE_TIER_2_MIN_USER_COUNT", 1000)

LIVE_TIER_1_POLL_SECONDS = _env_int("LIVE_TIER_1_POLL_SECONDS", 5)
LIVE_TIER_2_POLL_SECONDS = _env_int("LIVE_TIER_2_POLL_SECONDS", 30)
LIVE_TIER_3_POLL_SECONDS = _env_int("LIVE_TIER_3_POLL_SECONDS", 90)

# Phase1-A2 (2026-05-29): per-tier root/edge fetch timeout. Replaces the
# single global SOFASCORE_FETCH_TIMEOUT_SECONDS (bumped 10→20 on 2026-05-29
# as a stop-gap) with principled per-tier values:
#   * tier_1 (top matches, detailId=1) get the most headroom — the
#     2026-05-29 incident (event 15728277) showed top live matches losing
#     their entire match-center because a proxy-latency burst (probe saw up
#     to ~18.7s) blew the 10s timeout on the root /event fetch. 25s clears
#     those bursts so the match-center actually hydrates.
#   * tier_3 (long-tail / niche) fail fast (12s) so a dead niche fetch does
#     not pin a proxy slot for 20-25s and starve live.
# Falls back to the global SOFASCORE_FETCH_TIMEOUT_SECONDS for non-tier
# lanes / callers that pass no tier (hydrate, CLI one-shot).
LIVE_TIER_1_FETCH_TIMEOUT_SECONDS = _env_float("SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_1", 25.0)
LIVE_TIER_2_FETCH_TIMEOUT_SECONDS = _env_float("SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_2", 20.0)
LIVE_TIER_3_FETCH_TIMEOUT_SECONDS = _env_float("SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_3", 12.0)

# F-7: tier-aware dispatch claim lease (Redis SET NX PX TTL inside
# claim_dispatch). Until F-7 the planner used a single 90_000ms lease
# for every event regardless of tier, which capped tier_1 cadence at
# ~90s even with LIVE_TIER_1_POLL_SECONDS=5. Splitting the lease per
# tier lets ops dial each tier independently from the .env file.
#
# Defaults are intentionally identical to the old single-lease value
# (90_000ms): a code-only deploy is therefore a behaviour no-op. Lower
# values are rolled out staged via env vars (tier_3 → tier_2 → tier_1)
# behind a separate ACK so any 403 / timeout / lag regression is
# attributable to a specific lease change, not a code change.
LIVE_DISPATCH_LEASE_TIER_1_MS = _env_int("LIVE_DISPATCH_LEASE_TIER_1_MS", 90_000)
LIVE_DISPATCH_LEASE_TIER_2_MS = _env_int("LIVE_DISPATCH_LEASE_TIER_2_MS", 90_000)
LIVE_DISPATCH_LEASE_TIER_3_MS = _env_int("LIVE_DISPATCH_LEASE_TIER_3_MS", 90_000)


def resolve_live_dispatch_tier(
    *,
    sport_slug: str | None,
    detail_id: int | None,
    tournament_tier: int | None,
    tournament_user_count: int | None,
    unique_tournament_id: int | None = None,
    tier_override_registry: object = None,
) -> str:
    # A3 Phase 0 (2026-05-16): operator-controlled per-UT override.
    # If ``live_tier_override`` has a row for this UT, return its
    # tier immediately, bypassing the heuristics below. Backwards-
    # compatible: when no registry is provided (e.g. unit tests),
    # the override lookup is skipped.
    if tier_override_registry is not None and unique_tournament_id is not None:
        override_getter = getattr(tier_override_registry, "get", None)
        if callable(override_getter):
            override = override_getter(unique_tournament_id)
            if override:
                return str(override)

    normalized_sport = str(sport_slug or "").strip().lower()
    if normalized_sport == "football":
        football_tier = football_detail_tier(detail_id)
        if football_tier == "tier_1":
            return LIVE_TIER_1
        if football_tier == "tier_2":
            return LIVE_TIER_2
        if football_tier == "tier_3":
            return LIVE_TIER_3
        # Phase 0 (2026-05-30): football_detail_tier returns "tier_5" when the
        # root payload carries NO detailId. ~93% of football events fall here,
        # including popular EPL/UCL matches whose tournament_user_count is well
        # above LIVE_TIER_1_MIN_USER_COUNT. Hard-returning LIVE_TIER_3 for
        # tier_5 (the old rule) routed those into the long-tail tier_3 stream
        # and made the user_count / tournament_tier ladder below DEAD CODE for
        # football. We now let ONLY the tier_5 cohort (detailId ABSENT) fall
        # through to that ladder. The genuine tier_3 cohort (detailId in
        # {2,3,5}) keeps its hard LIVE_TIER_3 return above, and explicit
        # detailId tier_1/tier_2 are unchanged.

    normalized_user_count = _as_int(tournament_user_count)
    if normalized_user_count is not None:
        if normalized_user_count >= LIVE_TIER_1_MIN_USER_COUNT:
            return LIVE_TIER_1
        if normalized_user_count >= LIVE_TIER_2_MIN_USER_COUNT:
            return LIVE_TIER_2
        return LIVE_TIER_3

    normalized_tournament_tier = _as_int(tournament_tier)
    if normalized_tournament_tier is not None:
        if normalized_tournament_tier <= 1:
            return LIVE_TIER_1
        if normalized_tournament_tier <= 3:
            return LIVE_TIER_2
        return LIVE_TIER_3

    return LIVE_TIER_3


def poll_seconds_for_live_dispatch_tier(dispatch_tier: str | None, *, default_seconds: int | None) -> int | None:
    normalized = normalize_live_dispatch_tier(dispatch_tier)
    if normalized == LIVE_TIER_1:
        return LIVE_TIER_1_POLL_SECONDS
    if normalized == LIVE_TIER_2:
        return LIVE_TIER_2_POLL_SECONDS
    if normalized == LIVE_TIER_3:
        return LIVE_TIER_3_POLL_SECONDS
    return default_seconds


def fetch_timeout_for_dispatch_tier(
    dispatch_tier: str | None, *, default_seconds: float | None = None
) -> float | None:
    """Phase1-A2: per-tier HTTP fetch timeout (seconds) for the live path.

    ``dispatch_tier`` may be a tier string (``tier_1``/``tier_2``/``tier_3``)
    OR a lane string (``hot``/``warm``/``cold``) — ``normalize_live_dispatch_tier``
    maps hot→tier_1, warm→tier_2, cold/unknown→tier_3, so passing the live
    worker's ``self.lane`` works directly. Returns ``default_seconds`` when
    ``dispatch_tier`` is None/empty (no tier info) so non-tier callers
    (hydrate, CLI) keep the global ``SOFASCORE_FETCH_TIMEOUT_SECONDS`` default.
    A non-empty but unrecognised value follows ``normalize_live_dispatch_tier``
    (→ fail-fast tier_3).
    """
    if dispatch_tier is None or not str(dispatch_tier).strip():
        return default_seconds
    normalized = normalize_live_dispatch_tier(dispatch_tier)
    if normalized == LIVE_TIER_1:
        return LIVE_TIER_1_FETCH_TIMEOUT_SECONDS
    if normalized == LIVE_TIER_2:
        return LIVE_TIER_2_FETCH_TIMEOUT_SECONDS
    if normalized == LIVE_TIER_3:
        return LIVE_TIER_3_FETCH_TIMEOUT_SECONDS
    return default_seconds


def lease_ms_for_dispatch_tier(dispatch_tier: str | None, *, default_ms: int) -> int:
    """Return the dispatch-claim lease (ms) for the given tier.

    F-7: lets the planner pick a per-tier TTL for the Redis SET NX PX
    that gates re-publish, instead of the legacy single 90s value.
    Falls back to ``default_ms`` for unrecognised tier strings so
    legacy callers without tier context keep their original behaviour.
    """
    normalized = normalize_live_dispatch_tier(dispatch_tier)
    if normalized == LIVE_TIER_1:
        return LIVE_DISPATCH_LEASE_TIER_1_MS
    if normalized == LIVE_TIER_2:
        return LIVE_DISPATCH_LEASE_TIER_2_MS
    if normalized == LIVE_TIER_3:
        return LIVE_DISPATCH_LEASE_TIER_3_MS
    return default_ms


def normalize_live_dispatch_tier(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in LIVE_DISPATCH_TIERS:
        return normalized
    if normalized == "hot":
        return LIVE_TIER_1
    if normalized in {"", "warm"}:
        return LIVE_TIER_2
    return LIVE_TIER_3


def live_priority_for_dispatch_tier(dispatch_tier: str | None) -> int:
    normalized = normalize_live_dispatch_tier(dispatch_tier)
    if normalized == LIVE_TIER_1:
        return 0
    if normalized == LIVE_TIER_2:
        return 1
    return 2


def _as_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
