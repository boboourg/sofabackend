"""One-shot leaderboards backfill CLI helper (P0.2 / Layer 4).

Audit on prod (2026-05-07) showed a massive coverage gap on top-tier
football leagues: 3,249 / 3,278 (99.1%) of completed top-tier (ut,
season) pairs had no ``/player-of-the-season`` snapshot locally even
though Sofascore returns 200. ``LeaderboardsBackfillJob`` exists but
is not on a systemd timer; ``sync_season_widget`` only runs as part of
the per-event PilotOrchestrator path which doesn't sweep prior
seasons. This subcommand fills the gap by publishing
``JOB_REFRESH_RESOURCE`` envelopes onto ``stream:etl:resource_refresh``
for each missing target — the existing worker pool then processes
them with all the usual safety nets (HEAD probe, negative cache,
freshness gate).

Targets:

1. ``/api/v1/.../player-of-the-season`` for each completed (ut,
   season) where the ``season_player_of_the_season`` table has no
   row. "Completed" = max(event.start_timestamp) > 7d ago AND no
   upcoming event for that pair.

2. ``/api/v1/.../top-players/overall`` and
   ``/api/v1/.../top-teams/overall`` for completed top-tier pairs
   missing a real 200 in ``api_payload_snapshot``.

3. ``/api/v1/.../top-players/regularSeason`` and
   ``/api/v1/.../top-teams/regularSeason`` for the explicit allowlist
   UTs (NBA, MLB, NHL, NFL, basketball top-leagues). Only seasons
   missing a real 200.

The subcommand is intentionally idempotent and conservative: it relies
on the worker's HEAD-probe + negative-cache layers to absorb stale
publications cheaply. Re-running is safe.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from ..endpoints import (
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT,
    unique_tournament_top_players_endpoint,
    unique_tournament_top_teams_endpoint,
)
from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_REFRESH_RESOURCE
from ..queue.streams import STREAM_RESOURCE_REFRESH
from ..workers._stream_jobs import encode_stream_job
from .season_widget_structural_gate import DEFAULT_REGULAR_SEASON_ALLOWLIST

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LeaderboardsBackfillReport:
    pos_published: int
    top_players_overall_published: int
    top_teams_overall_published: int
    top_players_regular_season_published: int
    top_teams_regular_season_published: int

    @property
    def total(self) -> int:
        return (
            self.pos_published
            + self.top_players_overall_published
            + self.top_teams_overall_published
            + self.top_players_regular_season_published
            + self.top_teams_regular_season_published
        )


async def run_leaderboards_backfill(
    *,
    database: Any,
    stream_queue: Any,
    sport_slug: str = "football",
    priority_rank_threshold: int = 30,
    completed_gap_days: int = 7,
    history_window_days: int = 365,
    regular_season_uts: tuple[int, ...] = DEFAULT_REGULAR_SEASON_ALLOWLIST,
) -> LeaderboardsBackfillReport:
    """Find and publish missing leaderboard targets to stream:etl:resource_refresh.

    Returns counters for each endpoint family.
    """

    pos_pattern = UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT.pattern
    top_players_overall = unique_tournament_top_players_endpoint("overall")
    top_teams_overall = unique_tournament_top_teams_endpoint("overall")
    top_players_reg = unique_tournament_top_players_endpoint("regularSeason")
    top_teams_reg = unique_tournament_top_teams_endpoint("regularSeason")

    pos_pairs = await _find_pos_missing_pairs(
        database=database,
        sport_slug=sport_slug,
        priority_rank_threshold=priority_rank_threshold,
        completed_gap_days=completed_gap_days,
    )
    overall_pairs = await _find_overall_missing_pairs(
        database=database,
        sport_slug=sport_slug,
        priority_rank_threshold=priority_rank_threshold,
        completed_gap_days=completed_gap_days,
        history_window_days=history_window_days,
    )
    reg_pairs = await _find_regular_season_pairs(
        database=database,
        regular_season_uts=regular_season_uts,
        history_window_days=history_window_days,
    )

    pos_count = _publish_targets(
        stream_queue=stream_queue,
        pattern=pos_pattern,
        pairs=pos_pairs,
        sport_slug=sport_slug,
        scope="leaderboards_backfill",
    )
    top_players_overall_count = _publish_targets(
        stream_queue=stream_queue,
        pattern=top_players_overall.pattern,
        pairs=overall_pairs,
        sport_slug=sport_slug,
        scope="leaderboards_backfill",
    )
    top_teams_overall_count = _publish_targets(
        stream_queue=stream_queue,
        pattern=top_teams_overall.pattern,
        pairs=overall_pairs,
        sport_slug=sport_slug,
        scope="leaderboards_backfill",
    )
    top_players_reg_count = _publish_targets(
        stream_queue=stream_queue,
        pattern=top_players_reg.pattern,
        pairs=reg_pairs,
        sport_slug=None,  # cross-sport
        scope="leaderboards_backfill",
    )
    top_teams_reg_count = _publish_targets(
        stream_queue=stream_queue,
        pattern=top_teams_reg.pattern,
        pairs=reg_pairs,
        sport_slug=None,
        scope="leaderboards_backfill",
    )

    report = LeaderboardsBackfillReport(
        pos_published=pos_count,
        top_players_overall_published=top_players_overall_count,
        top_teams_overall_published=top_teams_overall_count,
        top_players_regular_season_published=top_players_reg_count,
        top_teams_regular_season_published=top_teams_reg_count,
    )
    logger.info(
        "leaderboards_backfill: published %s envelopes (pos=%s, overall=%s+%s, regular=%s+%s)",
        report.total,
        report.pos_published,
        report.top_players_overall_published,
        report.top_teams_overall_published,
        report.top_players_regular_season_published,
        report.top_teams_regular_season_published,
    )
    return report


async def _find_pos_missing_pairs(
    *,
    database: Any,
    sport_slug: str,
    priority_rank_threshold: int,
    completed_gap_days: int,
) -> tuple[tuple[int, int], ...]:
    """Top-tier completed (ut, season) pairs without a season_player_of_the_season row."""

    sql = """
    WITH top_uts AS (
        SELECT DISTINCT unique_tournament_id FROM tournament_registry
        WHERE sport_slug = $1 AND is_active AND priority_rank <= $2
    ),
    completed_pairs AS (
        SELECT e.unique_tournament_id AS ut, e.season_id AS s
        FROM event e
        WHERE e.unique_tournament_id IN (SELECT unique_tournament_id FROM top_uts)
          AND e.start_timestamp IS NOT NULL
        GROUP BY 1, 2
        HAVING max(e.start_timestamp) <
                 EXTRACT(EPOCH FROM now())::bigint - $3::bigint
           AND max(e.start_timestamp) >
                 EXTRACT(EPOCH FROM now())::bigint - 5 * 365 * 86400
    )
    SELECT c.ut, c.s
    FROM completed_pairs c
    WHERE NOT EXISTS (
        SELECT 1 FROM season_player_of_the_season p
        WHERE p.unique_tournament_id = c.ut AND p.season_id = c.s
    )
    ORDER BY c.ut, c.s
    """
    async with database.connection() as conn:
        rows = await conn.fetch(
            sql,
            str(sport_slug),
            int(priority_rank_threshold),
            int(completed_gap_days * 86400),
        )
    return tuple((int(r["ut"]), int(r["s"])) for r in rows)


async def _find_overall_missing_pairs(
    *,
    database: Any,
    sport_slug: str,
    priority_rank_threshold: int,
    completed_gap_days: int,
    history_window_days: int,
) -> tuple[tuple[int, int], ...]:
    """Top-tier completed (ut, season) pairs missing a real 200 on top-players/overall."""

    pattern = unique_tournament_top_players_endpoint("overall").pattern
    sql = """
    WITH top_uts AS (
        SELECT DISTINCT unique_tournament_id FROM tournament_registry
        WHERE sport_slug = $1 AND is_active AND priority_rank <= $2
    ),
    completed_pairs AS (
        SELECT e.unique_tournament_id AS ut, e.season_id AS s
        FROM event e
        WHERE e.unique_tournament_id IN (SELECT unique_tournament_id FROM top_uts)
          AND e.start_timestamp IS NOT NULL
        GROUP BY 1, 2
        HAVING max(e.start_timestamp) <
                 EXTRACT(EPOCH FROM now())::bigint - $3::bigint
           AND max(e.start_timestamp) >
                 EXTRACT(EPOCH FROM now())::bigint - $4::bigint
    )
    SELECT c.ut, c.s
    FROM completed_pairs c
    WHERE NOT EXISTS (
        SELECT 1 FROM api_payload_snapshot
        WHERE endpoint_pattern = $5
          AND context_unique_tournament_id = c.ut
          AND context_season_id = c.s
          AND http_status = 200
          AND coalesce(is_soft_error_payload, false) = false
    )
    ORDER BY c.ut, c.s
    """
    async with database.connection() as conn:
        rows = await conn.fetch(
            sql,
            str(sport_slug),
            int(priority_rank_threshold),
            int(completed_gap_days * 86400),
            int(history_window_days * 86400),
            pattern,
        )
    return tuple((int(r["ut"]), int(r["s"])) for r in rows)


async def _find_regular_season_pairs(
    *,
    database: Any,
    regular_season_uts: tuple[int, ...],
    history_window_days: int,
) -> tuple[tuple[int, int], ...]:
    """All seasons (within history window) for the regularSeason allowlist UTs missing a real 200."""

    if not regular_season_uts:
        return ()
    pattern = unique_tournament_top_players_endpoint("regularSeason").pattern
    sql = """
    WITH ut_seasons AS (
        SELECT DISTINCT unique_tournament_id AS ut, season_id AS s
        FROM event
        WHERE unique_tournament_id = ANY($1::bigint[])
          AND season_id IS NOT NULL
          AND start_timestamp IS NOT NULL
          AND start_timestamp > EXTRACT(EPOCH FROM now())::bigint - $2::bigint
    )
    SELECT us.ut, us.s
    FROM ut_seasons us
    WHERE NOT EXISTS (
        SELECT 1 FROM api_payload_snapshot
        WHERE endpoint_pattern = $3
          AND context_unique_tournament_id = us.ut
          AND context_season_id = us.s
          AND http_status = 200
          AND coalesce(is_soft_error_payload, false) = false
    )
    ORDER BY us.ut, us.s
    """
    async with database.connection() as conn:
        rows = await conn.fetch(
            sql,
            list(regular_season_uts),
            int(history_window_days * 86400),
            pattern,
        )
    return tuple((int(r["ut"]), int(r["s"])) for r in rows)


def _publish_targets(
    *,
    stream_queue: Any,
    pattern: str,
    pairs: tuple[tuple[int, int], ...],
    sport_slug: str | None,
    scope: str,
) -> int:
    if not pairs or stream_queue is None:
        return 0
    count = 0
    for ut, season in pairs:
        envelope = JobEnvelope.create(
            job_type=JOB_REFRESH_RESOURCE,
            sport_slug=sport_slug,
            entity_type="season",
            entity_id=int(season),
            scope=scope,
            params={
                "endpoint_pattern": pattern,
                "path_params": {
                    "unique_tournament_id": int(ut),
                    "season_id": int(season),
                },
                "context_unique_tournament_id": int(ut),
                "context_season_id": int(season),
            },
            priority=10,  # below normal refresh priority
            trace_id=None,
            capability_hint="leaderboards_backfill",
        )
        try:
            stream_queue.publish(STREAM_RESOURCE_REFRESH, encode_stream_job(envelope))
        except Exception as exc:
            logger.warning(
                "leaderboards_backfill: publish failed pattern=%s ut=%s season=%s: %s",
                pattern, ut, season, exc,
            )
            continue
        count += 1
    return count


__all__ = [
    "run_leaderboards_backfill",
    "LeaderboardsBackfillReport",
]
