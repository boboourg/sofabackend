"""Resolve live hydration rollout flags consistently across workers."""

from __future__ import annotations

import os

_LIVE_SCOPES = {"live", "hot", "warm", "cold"}


def resolve_live_hydration_mode(
    *,
    requested_mode: object,
    sport_slug: object,
    scope: object,
) -> str:
    """Apply rollout flags only to live refresh/hydrate jobs.

    Non-live jobs keep their requested mode so rollback cannot accidentally
    expand scheduled or historical work from core/delta into full hydration.
    """

    job_mode = _normalize_text(requested_mode) or "live_delta"
    normalized_scope = _normalize_text(scope)
    if normalized_scope not in _LIVE_SCOPES and job_mode != "live_delta":
        return job_mode

    configured_mode = _normalize_text(os.environ.get("SOFASCORE_LIVE_HYDRATION_MODE")) or "delta"
    if configured_mode == "full":
        return "full"
    if configured_mode == "auto":
        return job_mode if _is_canary_sport(sport_slug) else "full"
    return job_mode


def _is_canary_sport(sport_slug: object) -> bool:
    canary_sports = _configured_canary_sports()
    if not canary_sports:
        return False
    return (_normalize_text(sport_slug) or "") in canary_sports


def _configured_canary_sports() -> set[str]:
    raw = os.environ.get("SOFASCORE_LIVE_HYDRATION_CANARY_SPORTS")
    if raw is None:
        raw = os.environ.get("CANARY_SPORTS")
    return {
        item.strip().lower()
        for item in str(raw or "").split(",")
        if item.strip()
    }


def _normalize_text(value: object) -> str:
    return str(value or "").strip().lower()
