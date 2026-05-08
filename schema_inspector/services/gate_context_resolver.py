"""GateContextResolver — extracts gate-decision context from a FetchTask.

Component for ``SportradarCoverageGate`` (P3.2). Separates concerns: the gate
decides yes/no, the resolver figures out *which* (sport, ut_id, season_id,
is_editor) tuple to evaluate. Single resolver class handles all path-template
shapes in our endpoint registry.

Resolution rules (path-template-driven):

* ``/api/v1/event/{event_id}/...``                         → event_id available
  in path or task; DB lookup yields sport_slug + unique_tournament_id +
  is_editor.
* ``/api/v1/event/{event_id}`` (root)                      → same as above.
* ``/api/v1/unique-tournament/{ut_id}/season/{s_id}/...``  → ut_id + season_id
  from path params; sport via UT → category → sport JOIN.
* ``/api/v1/team/{team_id}/...``                           → no canonical UT
  context (a team plays in many tournaments). Returns missing context →
  gate fails open in Phase 1. Phase 2 may add a heuristic via
  ``team.primary_unique_tournament_id``.
* ``/api/v1/player/{player_id}/...``                       → same — no canonical
  UT. Phase 1 fail-open.
* ``/api/v1/sport/{sport}/...``                            → sport_slug only
  from path; no ut/event → missing context.
* anything else                                            → missing context.

Caching: simple bounded dict (FIFO eviction). Hit rate at 100k decisions/hour
should exceed 99% because event_ids repeat across many endpoints in one polling
cycle. Cache hits are O(1); cache misses do one indexed Postgres lookup
(~1-3 ms p99).

The gate enforces ``has_full_context == False`` → fail open. This document
ensures team and player endpoints are never accidentally hard-blocked.

See ``docs/superpowers/plans/2026-05-08-p3-2-sportradar-coverage-gate.md`` §6.3.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping, Protocol

logger = logging.getLogger(__name__)


_EVENT_CACHE_MAX_SIZE = 10_000
_EVENT_CACHE_EVICT_BATCH = 1_000


@dataclass(frozen=True)
class GateContext:
    """Context resolved from a FetchTask, used by SportradarCoverageGate."""

    sport_slug: str | None
    is_editor: bool
    unique_tournament_id: int | None
    season_id: int | None
    event_id: int | None
    has_full_context: bool
    missing_reason: str = ""


class _DatabaseLike(Protocol):
    """Subset of asyncpg-style Database used by the resolver.

    Minimal shape for testability. Real Database in
    ``schema_inspector.db`` satisfies this via duck typing.
    """

    async def fetchrow(self, sql: str, *args: Any) -> Mapping[str, Any] | None: ...


class GateContextResolver:
    """Resolves :class:`GateContext` from a FetchTask.

    Implementation notes:
    * Method ``resolve`` is async because event/UT lookups touch the database
      when context is not already populated on the task.
    * The resolver does NOT depend on the gate; it can be reused for other
      decision pipelines (e.g., per-endpoint cost estimation, future tier
      analytics).
    """

    _UT_CONTEXT_PREFIX = "/api/v1/unique-tournament/"
    _EVENT_CONTEXT_PREFIX = "/api/v1/event/"
    _TEAM_CONTEXT_PREFIX = "/api/v1/team/"
    _PLAYER_CONTEXT_PREFIX = "/api/v1/player/"
    _SPORT_LISTING_PREFIX = "/api/v1/sport/"

    def __init__(self, *, database: _DatabaseLike | None = None) -> None:
        self._database = database
        self._event_cache: dict[int, tuple[str | None, int | None, bool]] = {}

    async def resolve(self, task: Any) -> GateContext:
        """Resolve gate context from a fetch task.

        ``task`` is expected to expose at least:
          - ``endpoint_pattern: str``
          - ``sport_slug: str | None``
          - ``context_event_id: int | None``
          - ``context_unique_tournament_id: int | None``
          - ``context_season_id: int | None``
        """
        path = getattr(task, "endpoint_pattern", None)
        if not isinstance(path, str) or not path:
            return GateContext(
                None, False, None, None, None, False,
                missing_reason="task has no endpoint_pattern",
            )

        # Short-circuit on team/player paths: Phase 1 policy = no canonical
        # UT context, fail-open in gate. We could try team.primary_ut later
        # but for now we explicitly return missing context.
        if path.startswith(self._TEAM_CONTEXT_PREFIX):
            return GateContext(
                getattr(task, "sport_slug", None),
                False, None, None, None, False,
                missing_reason="team-level path has no canonical UT context (Phase 1)",
            )
        if path.startswith(self._PLAYER_CONTEXT_PREFIX):
            return GateContext(
                getattr(task, "sport_slug", None),
                False, None, None, None, False,
                missing_reason="player-level path has no canonical UT context (Phase 1)",
            )
        if path.startswith(self._SPORT_LISTING_PREFIX):
            return GateContext(
                getattr(task, "sport_slug", None),
                False, None, None, None, False,
                missing_reason="sport-listing path has no UT/event context",
            )

        # UT/season-level path: ut_id + season_id from task or path params.
        if path.startswith(self._UT_CONTEXT_PREFIX):
            return await self._resolve_ut_path(task, path)

        # Event-level path: lookup via event_id.
        if path.startswith(self._EVENT_CONTEXT_PREFIX):
            return await self._resolve_event_path(task, path)

        return GateContext(
            getattr(task, "sport_slug", None),
            False, None, None, None, False,
            missing_reason=f"unsupported path_template prefix: {path!r}",
        )

    async def _resolve_ut_path(self, task: Any, path: str) -> GateContext:
        ut_id = getattr(task, "context_unique_tournament_id", None)
        season_id = getattr(task, "context_season_id", None)
        sport_slug = getattr(task, "sport_slug", None)

        if ut_id is None:
            return GateContext(
                sport_slug, False, None, season_id, None, False,
                missing_reason=f"unique_tournament_id missing on task for {path}",
            )

        # If sport_slug missing, look it up.
        if sport_slug is None and self._database is not None:
            sport_slug = await self._lookup_sport_for_ut(int(ut_id))

        return GateContext(
            sport_slug=sport_slug,
            is_editor=False,
            unique_tournament_id=int(ut_id),
            season_id=int(season_id) if season_id is not None else None,
            event_id=None,
            has_full_context=sport_slug is not None,
            missing_reason="" if sport_slug is not None else f"could not resolve sport for ut={ut_id}",
        )

    async def _resolve_event_path(self, task: Any, path: str) -> GateContext:
        event_id = getattr(task, "context_event_id", None)
        sport_slug = getattr(task, "sport_slug", None)
        ut_id = getattr(task, "context_unique_tournament_id", None)

        if event_id is None:
            return GateContext(
                sport_slug, False, ut_id, None, None, False,
                missing_reason="context_event_id missing on event-level task",
            )

        eid = int(event_id)

        # Need DB lookup for is_editor regardless (not in task).
        if self._database is None:
            # No DB available — assume non-editor and use whatever we have.
            return GateContext(
                sport_slug=sport_slug,
                is_editor=False,
                unique_tournament_id=int(ut_id) if ut_id is not None else None,
                season_id=None,
                event_id=eid,
                has_full_context=(sport_slug is not None and ut_id is not None),
                missing_reason="no database available to resolve is_editor",
            )

        sport_from_db, ut_from_db, is_editor = await self._lookup_event(eid)
        # Prefer task-supplied values when present (planner already resolved them);
        # fall back to DB lookup.
        resolved_sport = sport_slug or sport_from_db
        resolved_ut = int(ut_id) if ut_id is not None else ut_from_db

        if resolved_sport is None or resolved_ut is None:
            return GateContext(
                resolved_sport, is_editor, resolved_ut, None, eid, False,
                missing_reason=f"event {eid} not found or missing tournament context",
            )

        return GateContext(
            sport_slug=resolved_sport,
            is_editor=is_editor,
            unique_tournament_id=resolved_ut,
            season_id=None,
            event_id=eid,
            has_full_context=True,
        )

    async def _lookup_event(self, event_id: int) -> tuple[str | None, int | None, bool]:
        """Returns (sport_slug, unique_tournament_id, is_editor) from event row.

        Cached. ``unique_tournament_id`` may be NULL in DB if the tournament
        isn't linked to a unique_tournament — the resolver returns None in
        that case and the gate falls open.
        """
        if event_id in self._event_cache:
            return self._event_cache[event_id]
        try:
            row = await self._database.fetchrow(
                """
                SELECT s.slug AS sport_slug,
                       ut.id AS ut_id,
                       COALESCE(ev.is_editor, FALSE) AS is_editor
                FROM event ev
                JOIN tournament t ON t.id = ev.tournament_id
                LEFT JOIN unique_tournament ut ON ut.id = t.unique_tournament_id
                JOIN category c ON c.id = COALESCE(ut.category_id, t.category_id)
                JOIN sport s ON s.id = c.sport_id
                WHERE ev.id = $1
                """,
                event_id,
            )
        except Exception as exc:
            logger.warning(
                "GateContextResolver event lookup failed: event_id=%s err=%s",
                event_id, exc,
            )
            row = None
        if row is None:
            result: tuple[str | None, int | None, bool] = (None, None, False)
        else:
            result = (
                row["sport_slug"] if row["sport_slug"] else None,
                int(row["ut_id"]) if row["ut_id"] is not None else None,
                bool(row["is_editor"]),
            )
        self._event_cache[event_id] = result
        if len(self._event_cache) > _EVENT_CACHE_MAX_SIZE:
            evict = list(self._event_cache.keys())[:_EVENT_CACHE_EVICT_BATCH]
            for k in evict:
                self._event_cache.pop(k, None)
        return result

    async def _lookup_sport_for_ut(self, ut_id: int) -> str | None:
        try:
            row = await self._database.fetchrow(
                """
                SELECT s.slug FROM unique_tournament ut
                JOIN category c ON c.id = ut.category_id
                JOIN sport s ON s.id = c.sport_id
                WHERE ut.id = $1
                """,
                ut_id,
            )
        except Exception as exc:
            logger.warning(
                "GateContextResolver UT sport lookup failed: ut_id=%s err=%s",
                ut_id, exc,
            )
            return None
        return row["slug"] if row and row["slug"] else None

    def cache_size(self) -> int:
        """Diagnostic helper: current event cache size."""
        return len(self._event_cache)


__all__ = ["GateContext", "GateContextResolver"]
