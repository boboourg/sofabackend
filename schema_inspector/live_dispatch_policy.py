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


# Based on the current production distribution snapshot for live tournaments on
# 2026-04-27: p75 ~ 959 user_count, p95 ~ 6627 user_count.
LIVE_TIER_1_MIN_USER_COUNT = _env_int("LIVE_TIER_1_MIN_USER_COUNT", 6500)
LIVE_TIER_2_MIN_USER_COUNT = _env_int("LIVE_TIER_2_MIN_USER_COUNT", 1000)

LIVE_TIER_1_POLL_SECONDS = _env_int("LIVE_TIER_1_POLL_SECONDS", 5)
LIVE_TIER_2_POLL_SECONDS = _env_int("LIVE_TIER_2_POLL_SECONDS", 30)
LIVE_TIER_3_POLL_SECONDS = _env_int("LIVE_TIER_3_POLL_SECONDS", 90)


def resolve_live_dispatch_tier(
    *,
    sport_slug: str | None,
    detail_id: int | None,
    tournament_tier: int | None,
    tournament_user_count: int | None,
) -> str:
    normalized_sport = str(sport_slug or "").strip().lower()
    if normalized_sport == "football":
        football_tier = football_detail_tier(detail_id)
        if football_tier == "tier_1":
            return LIVE_TIER_1
        if football_tier == "tier_2":
            return LIVE_TIER_2
        if football_tier in {"tier_3", "tier_5"}:
            return LIVE_TIER_3

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
