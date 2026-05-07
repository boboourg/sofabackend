"""Shared parser for the managed (ut, season) pair list used by D12 resolvers.

The D12 parent→child resolvers (round-of-managed, period-of-managed,
custom-id-of-managed) all need the same opt-in list of football leagues
to walk. Centralising the env-parser here keeps the three child
resolvers consistent and lets the operations layer flip a single env
var to control all three at once.

Env format::

    SCHEMA_INSPECTOR_FORCE_TOP_FOOTBALL_LEAGUES=679:76984,17:76986

Empty/unset env disables all three child resolvers (they yield no
targets) - safe default so production never publishes parent→child
fan-out jobs unless explicitly enabled.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

ENV_KEY = "SCHEMA_INSPECTOR_FORCE_TOP_FOOTBALL_LEAGUES"


def parse_managed_pairs(raw: str | None) -> tuple[tuple[int, int], ...]:
    """Parse ``ut1:season1,ut2:season2`` env value.

    Drops malformed tokens with a warning so a typo never silently
    masks the entire managed list.
    """

    if not raw:
        return ()
    seen: set[tuple[int, int]] = set()
    out: list[tuple[int, int]] = []
    for chunk in str(raw).split(","):
        token = chunk.strip()
        if not token:
            continue
        if ":" not in token:
            logger.warning("managed-football-pairs: ignoring %r (missing ':')", token)
            continue
        left, right = token.split(":", 1)
        try:
            ut = int(left.strip())
            season = int(right.strip())
        except ValueError:
            logger.warning(
                "managed-football-pairs: ignoring non-integer pair %r", token
            )
            continue
        if ut <= 0 or season <= 0:
            continue
        pair = (ut, season)
        if pair in seen:
            continue
        seen.add(pair)
        out.append(pair)
    return tuple(out)


def load_managed_pairs(env: dict[str, str] | None = None) -> tuple[tuple[int, int], ...]:
    """Read env once. Empty/unset → empty tuple (resolvers yield nothing)."""

    resolved = env if env is not None else os.environ
    return parse_managed_pairs(resolved.get(ENV_KEY))


__all__ = ["ENV_KEY", "parse_managed_pairs", "load_managed_pairs"]
