"""Cache warmer daemon (N4 Layer C, 2026-05-14).

Periodically fetches hot sport-level aggregation URLs via local API so
the Redis response cache stays populated. Without warming, first-user
hit to /api/v1/sport/{slug}/scheduled-events/{date} takes 5-30s.
With warming, every user request hits warm Redis cache and returns in
2-5ms.

See docs/N4_API_PERFORMANCE_PLAN.md for design.
"""

from __future__ import annotations

from .config import CacheWarmerConfig
from .daemon import CacheWarmerDaemon
from .url_targets import UrlTarget, build_url_targets, default_sports

__all__ = [
    "CacheWarmerConfig",
    "CacheWarmerDaemon",
    "UrlTarget",
    "build_url_targets",
    "default_sports",
]
