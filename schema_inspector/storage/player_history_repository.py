"""Read-only helpers reconstructing the player → team timeline from
existing per-event tables.

The hard column ``player.team_id`` is overwritten on every upsert
(see ``normalize_repository._upsert_child_pass``), so it always reflects
the CURRENT club. Any read-path query that needs the player's club
at a point in time MUST use this module — both to answer
``/api/v1/player/{id}`` historically and to drive future
career-timeline endpoints.

Storage layout the helper relies on:

* ``event_lineup_player(event_id, side, player_id, team_id)`` — per-match
  snapshot; team_id reflects the team the player ACTUALLY played for in
  that specific match. Never overwritten between matches (Fix in
  Stage 1.2 made identical re-ingests no-ops, so even a re-fetch of
  the same match leaves the row alone).
* ``event(id, home_team_id, away_team_id, start_timestamp)`` — provides
  the timestamp axis and a side-based team fallback when ``elp.team_id``
  is NULL (legacy ingests prior to 2026-04).

Existing index ``idx_event_lineup_player_player_id_event_id``
(``migrations/2026-05-18_event_lineup_player_player_idx.sql``) makes
the lookup an index scan on a star player's ~500 lineup rows + nested
loop into ``event`` PK → 1-5 ms cold cache, sub-ms warm.

Stage 2.2 (2026-05-20 architecture rework). Read-only; no schema
changes required.
"""

from __future__ import annotations

from typing import Any, Protocol


class SqlExecutor(Protocol):
    async def fetchrow(self, query: str, *args: object) -> Any: ...


async def lookup_player_team_at(
    executor: SqlExecutor,
    *,
    player_id: int,
    at_timestamp: int | None = None,
) -> int | None:
    """Return the team_id the player belonged to at the requested time.

    If ``at_timestamp`` is None, returns the team from the most recent
    lineup (any time). If ``at_timestamp`` is provided (UNIX epoch
    seconds), returns the team from the latest event whose
    ``start_timestamp`` is ``<= at_timestamp``.

    Returns ``None`` when the player has no lineup rows at all (or no
    rows within the timestamp window). The caller decides whether to
    fall back to ``player.team_id`` or omit the team binding entirely.

    Fallback chain when ``event_lineup_player.team_id`` is NULL:
    use ``event.home_team_id`` if ``elp.side='home'``, else
    ``event.away_team_id``. This covers legacy lineup rows where
    ``team_id`` was not yet populated.
    """

    if at_timestamp is None:
        query = """
            SELECT
                elp.team_id AS team_id,
                elp.side AS side,
                e.home_team_id AS home_team_id,
                e.away_team_id AS away_team_id
            FROM event_lineup_player elp
            JOIN event e ON e.id = elp.event_id
            WHERE elp.player_id = $1
            ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
            LIMIT 1
        """
        row = await executor.fetchrow(query, int(player_id))
    else:
        query = """
            SELECT
                elp.team_id AS team_id,
                elp.side AS side,
                e.home_team_id AS home_team_id,
                e.away_team_id AS away_team_id
            FROM event_lineup_player elp
            JOIN event e ON e.id = elp.event_id
            WHERE elp.player_id = $1
              AND e.start_timestamp <= $2
            ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
            LIMIT 1
        """
        row = await executor.fetchrow(query, int(player_id), int(at_timestamp))

    if row is None:
        return None

    team_id = row["team_id"]
    if team_id is not None:
        return int(team_id)

    # Fallback path: lineup row had NULL team_id (legacy ingest). Derive
    # from event.home_team_id / away_team_id via the side column.
    side = str(row["side"] or "").strip().lower()
    if side == "home":
        home_team_id = row["home_team_id"]
        return int(home_team_id) if home_team_id is not None else None
    if side == "away":
        away_team_id = row["away_team_id"]
        return int(away_team_id) if away_team_id is not None else None
    return None
