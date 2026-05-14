"""Env-backed configuration for the cache-warmer daemon."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


_DEFAULT_SPORTS = (
    "football",
    "basketball",
    "tennis",
    "ice-hockey",
    "volleyball",
    "handball",
    "baseball",
    "american-football",
    "rugby",
    "cricket",
    "mma",
    "esports",
    "table-tennis",
    "futsal",
)


def _env_int(env: dict[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_float(env: dict[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_bool(env: dict[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_str_tuple(env: dict[str, str], name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = env.get(name)
    if not raw:
        return default
    parsed = tuple(item.strip() for item in raw.split(",") if item.strip())
    return parsed or default


@dataclass(frozen=True)
class CacheWarmerConfig:
    """All env knobs of the cache warmer, frozen."""

    enabled: bool = True
    base_url: str = "http://127.0.0.1:8000"
    tick_interval_seconds: float = 1.0
    request_timeout_seconds: float = 60.0
    sports: tuple[str, ...] = _DEFAULT_SPORTS
    # Per-target cadences. Default matches Layer B cache TTL policy.
    live_interval_seconds: int = 5
    scheduled_today_interval_seconds: int = 60
    scheduled_tomorrow_interval_seconds: int = 300
    scheduled_tournaments_interval_seconds: int = 60
    # Optional toggle for tomorrow's scheduled-events warming. Off by
    # default because today's data is the priority and tomorrow's is
    # less heavily trafficked.
    warm_tomorrow_scheduled: bool = False

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "CacheWarmerConfig":
        resolved = dict(os.environ) if env is None else dict(env)
        return cls(
            enabled=_env_bool(resolved, "SOFASCORE_CACHE_WARMER_ENABLED", True),
            base_url=resolved.get("SOFASCORE_CACHE_WARMER_BASE_URL")
            or "http://127.0.0.1:8000",
            tick_interval_seconds=_env_float(
                resolved, "SOFASCORE_CACHE_WARMER_TICK_INTERVAL_SECONDS", 1.0
            ),
            request_timeout_seconds=_env_float(
                resolved, "SOFASCORE_CACHE_WARMER_REQUEST_TIMEOUT_SECONDS", 60.0
            ),
            sports=_env_str_tuple(resolved, "SOFASCORE_CACHE_WARMER_SPORTS", _DEFAULT_SPORTS),
            live_interval_seconds=_env_int(
                resolved, "SOFASCORE_CACHE_WARMER_LIVE_INTERVAL_SECONDS", 5
            ),
            scheduled_today_interval_seconds=_env_int(
                resolved, "SOFASCORE_CACHE_WARMER_SCHEDULED_TODAY_INTERVAL_SECONDS", 60
            ),
            scheduled_tomorrow_interval_seconds=_env_int(
                resolved, "SOFASCORE_CACHE_WARMER_SCHEDULED_TOMORROW_INTERVAL_SECONDS", 300
            ),
            scheduled_tournaments_interval_seconds=_env_int(
                resolved,
                "SOFASCORE_CACHE_WARMER_SCHEDULED_TOURNAMENTS_INTERVAL_SECONDS",
                60,
            ),
            warm_tomorrow_scheduled=_env_bool(
                resolved, "SOFASCORE_CACHE_WARMER_WARM_TOMORROW_SCHEDULED", False
            ),
        )
