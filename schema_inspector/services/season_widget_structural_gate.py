"""Structural pre-filter for ``planner.plan_season_widgets`` (P0.2).

The :class:`SeasonWidgetNegativeCache` (in
``season_widget_negative_cache.py``) handles per-(ut, season, endpoint)
404 caching with hours-to-days re-probe intervals. That works well as
defence-in-depth, but it cannot avoid the FIRST 4xx for every new
(ut, season) pair — and on prod we have ~50k such pairs which yields
~60k 4xx /hour for endpoints like ``/top-teams/overall``.

This gate adds a *structural* layer on top of the cache: rather than
"we just learned this 404s, back off", the gate says "this combination
is structurally not applicable, never publish in the first place".
Concretely:

* ``/api/v1/.../player-of-the-season`` — Sofascore returns 200 only
  AFTER the season ends (live probe 2026-05-07: PL 25/26 active → 404,
  PL 24/25 completed → 200). Filter:
    a. last event in season fetched > 7 days ago
    b. no upcoming event for the (ut, season) pair
    c. ``season_player_of_the_season`` table has no row yet (one-shot)
  Together: probe POS exactly once per season, after it ends, and
  never again once we have the row.

* ``/api/v1/.../top-players/regularSeason`` (and related ``regularSeason``
  variants) — only NBA, MLB, NHL, NFL and a few basketball top-leagues
  return 200 (live probe + DB confirmed). For every other UT this is
  a structural 404. Filter to an explicit allowlist; auto-grow via a
  daily background sweep (separate concern).

The gate is consumed by the orchestrator wiring as
``blocked_endpoint_patterns`` passed into
:func:`schema_inspector.planner.rules.season_widget_jobs`. When the
gate decides "skip", the pattern goes into ``blocked_endpoint_patterns``
and the planner never spawns the child job. No upstream call, no worker
job, no negative-cache mutation.

Configuration::

    SCHEMA_INSPECTOR_POS_COMPLETED_GAP_DAYS=7
    SCHEMA_INSPECTOR_REGULAR_SEASON_UTS=132,11205,234,9464,262,138
"""

from __future__ import annotations

import logging
import os
from typing import Any, Mapping

from ..endpoints import (
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT,
)

logger = logging.getLogger(__name__)


# Live probe (2026-05-07): only these UTs return 200 on regularSeason
# top-N endpoints. Football and other sports never use regularSeason.
DEFAULT_REGULAR_SEASON_ALLOWLIST = (
    132,    # NBA
    11205,  # MLB
    234,    # NHL
    9464,   # NFL
    262,    # Italian Serie A (basketball — Lega Basket)
    138,    # Euroleague basketball
)

DEFAULT_POS_COMPLETED_GAP_DAYS = 7


class SeasonWidgetStructuralGate:
    """Pre-filter for plan_season_widgets that returns blocked patterns."""

    def __init__(
        self,
        *,
        database: Any,
        env: dict[str, str] | None = None,
    ) -> None:
        self.database = database
        self._env = env if env is not None else dict(os.environ)

    @property
    def pos_completed_gap_days(self) -> int:
        raw = self._env.get("SCHEMA_INSPECTOR_POS_COMPLETED_GAP_DAYS")
        if raw in (None, ""):
            return DEFAULT_POS_COMPLETED_GAP_DAYS
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return DEFAULT_POS_COMPLETED_GAP_DAYS
        return value if value > 0 else DEFAULT_POS_COMPLETED_GAP_DAYS

    @property
    def regular_season_allowlist(self) -> tuple[int, ...]:
        raw = self._env.get("SCHEMA_INSPECTOR_REGULAR_SEASON_UTS")
        if raw in (None, ""):
            return DEFAULT_REGULAR_SEASON_ALLOWLIST
        seen: set[int] = set()
        out: list[int] = []
        for chunk in str(raw).split(","):
            token = chunk.strip()
            if not token:
                continue
            try:
                value = int(token)
            except ValueError:
                continue
            if value <= 0 or value in seen:
                continue
            seen.add(value)
            out.append(value)
        return tuple(out) if out else DEFAULT_REGULAR_SEASON_ALLOWLIST

    async def blocked_patterns(
        self,
        *,
        unique_tournament_id: int,
        season_id: int,
        candidate_patterns: tuple[str, ...],
    ) -> tuple[str, ...]:
        """Return the subset of ``candidate_patterns`` to skip.

        Caller passes the patterns that the planner would otherwise
        publish; this returns the structural-block list which the
        caller merges with the negative-cache block list before
        invoking :func:`season_widget_jobs`.
        """

        if not candidate_patterns:
            return ()

        pos_pattern = UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT.pattern
        blocked: list[str] = []

        # 1) regularSeason allowlist — sport-agnostic, fast in-memory.
        allow = set(self.regular_season_allowlist)
        if int(unique_tournament_id) not in allow:
            for pattern in candidate_patterns:
                if pattern.endswith("/regularSeason") and pattern not in blocked:
                    blocked.append(pattern)

        # 2) POS structural filter — completed-only + one-shot.
        if pos_pattern in candidate_patterns and pos_pattern not in blocked:
            should_skip = await self._should_skip_pos(
                unique_tournament_id=int(unique_tournament_id),
                season_id=int(season_id),
            )
            if should_skip:
                blocked.append(pos_pattern)

        return tuple(blocked)

    async def _should_skip_pos(self, *, unique_tournament_id: int, season_id: int) -> bool:
        """True iff POS endpoint shouldn't be probed for this (ut, season)."""

        gap_seconds = self.pos_completed_gap_days * 86400
        try:
            async with self.database.connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT
                        EXISTS (
                            SELECT 1 FROM event
                            WHERE unique_tournament_id = $1 AND season_id = $2
                              AND start_timestamp >
                                  EXTRACT(EPOCH FROM now())::bigint - $3::bigint
                        ) AS has_recent_or_upcoming,
                        EXISTS (
                            SELECT 1 FROM season_player_of_the_season
                            WHERE unique_tournament_id = $1 AND season_id = $2
                        ) AS already_stored
                    """,
                    int(unique_tournament_id),
                    int(season_id),
                    int(gap_seconds),
                )
        except Exception as exc:
            logger.warning(
                "SeasonWidgetStructuralGate: POS gate query failed (fail-open): %s",
                exc,
            )
            return False

        if row is None:
            return False
        already_stored = bool(row["already_stored"])
        if already_stored:
            return True  # one-shot: we already have it, never refetch
        # If the season still has events within the gap window (recent
        # match or upcoming match), POS is not yet computed upstream.
        has_active = bool(row["has_recent_or_upcoming"])
        return has_active


__all__ = [
    "SeasonWidgetStructuralGate",
    "DEFAULT_REGULAR_SEASON_ALLOWLIST",
    "DEFAULT_POS_COMPLETED_GAP_DAYS",
]
