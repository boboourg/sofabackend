"""Environment-overridable queue lag thresholds for service backpressure."""

from __future__ import annotations

import os


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


SCHEDULED_DISCOVERY_MAX_LAG = _env_int("SCHEDULED_DISCOVERY_MAX_LAG", 100)
HYDRATE_MAX_LAG = _env_int("HYDRATE_MAX_LAG", 800)
LIVE_DISCOVERY_MAX_LAG = _env_int("LIVE_DISCOVERY_MAX_LAG", 50)
LIVE_DISCOVERY_HYDRATE_MAX_LAG = _env_int("LIVE_DISCOVERY_HYDRATE_MAX_LAG", 1000)
LIVE_HOT_MAX_LAG = _env_int("LIVE_HOT_MAX_LAG", 200)
LIVE_WARM_MAX_LAG = _env_int("LIVE_WARM_MAX_LAG", 800)
HISTORICAL_DISCOVERY_MAX_LAG = _env_int("HISTORICAL_DISCOVERY_MAX_LAG", 100)
HISTORICAL_HYDRATE_MAX_LAG = _env_int("HISTORICAL_HYDRATE_MAX_LAG", 500)
HISTORICAL_TOURNAMENT_MAX_LAG = _env_int("HISTORICAL_TOURNAMENT_MAX_LAG", 2500)
HISTORICAL_ENRICHMENT_MAX_LAG = _env_int("HISTORICAL_ENRICHMENT_MAX_LAG", 15000)


__all__ = [
    "SCHEDULED_DISCOVERY_MAX_LAG",
    "HYDRATE_MAX_LAG",
    "LIVE_DISCOVERY_MAX_LAG",
    "LIVE_DISCOVERY_HYDRATE_MAX_LAG",
    "LIVE_HOT_MAX_LAG",
    "LIVE_WARM_MAX_LAG",
    "HISTORICAL_DISCOVERY_MAX_LAG",
    "HISTORICAL_HYDRATE_MAX_LAG",
    "HISTORICAL_TOURNAMENT_MAX_LAG",
    "HISTORICAL_ENRICHMENT_MAX_LAG",
]
