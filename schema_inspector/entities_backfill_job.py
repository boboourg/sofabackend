"""Backfill job for entity/enrichment endpoints using seeds already stored in PostgreSQL."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .db import AsyncpgDatabase
from .endpoints import (
    PLAYER_ENDPOINT,
    PLAYER_SEASON_HEATMAP_OVERALL_ENDPOINT,
    PLAYER_SEASON_OVERALL_STATISTICS_ENDPOINT,
    PLAYER_STATISTICS_ENDPOINT,
    TEAM_ENDPOINT,
    TEAM_PERFORMANCE_GRAPH_ENDPOINT,
    TEAM_SEASON_OVERALL_STATISTICS_ENDPOINT,
)
from .entities_job import EntitiesIngestJob, EntitiesIngestResult
from .limit_utils import normalize_limit
from .entities_parser import (
    PlayerHeatmapRequest,
    PlayerOverallRequest,
    TeamOverallRequest,
    TeamPerformanceGraphRequest,
)


@dataclass(frozen=True)
class EntitiesBackfillResult:
    player_ids: tuple[int, ...]
    player_statistics_ids: tuple[int, ...]
    team_ids: tuple[int, ...]
    player_overall_requests: tuple[PlayerOverallRequest, ...]
    team_overall_requests: tuple[TeamOverallRequest, ...]
    player_heatmap_requests: tuple[PlayerHeatmapRequest, ...]
    team_performance_graph_requests: tuple[TeamPerformanceGraphRequest, ...]
    ingest: EntitiesIngestResult


class EntitiesBackfillJob:
    """Loads entity seeds from PostgreSQL and hydrates entity endpoints in one transactional batch."""

    def __init__(
        self,
        ingest_job: EntitiesIngestJob,
        database: AsyncpgDatabase,
        *,
        logger: logging.Logger | None = None,
        now_factory=None,
    ) -> None:
        self.ingest_job = ingest_job
        self.database = database
        self.logger = logger or logging.getLogger(__name__)
        self.now_factory = now_factory or _default_now_utc

    async def run(
        self,
        *,
        player_limit: int | None = None,
        player_offset: int = 0,
        team_limit: int | None = None,
        team_offset: int = 0,
        player_request_limit: int | None = None,
        player_request_offset: int = 0,
        team_request_limit: int | None = None,
        team_request_offset: int = 0,
        only_missing: bool = True,
        include_player_statistics: bool = True,
        include_player_overall: bool = True,
        include_team_overall: bool = True,
        include_player_heatmaps: bool = True,
        include_team_performance_graphs: bool = True,
        include_player_statistics_seasons: bool = True,
        include_player_transfer_history: bool = True,
        include_team_statistics_seasons: bool = True,
        include_team_player_statistics_seasons: bool = True,
        event_timestamp_from: int | None = None,
        unique_tournament_ids: tuple[int, ...] | None = None,
        event_timestamp_to: int | None = None,
        timeout: float = 20.0,
    ) -> EntitiesBackfillResult:
        resolved_unique_tournament_ids = tuple(
            dict.fromkeys(int(item) for item in (unique_tournament_ids or ()) if item is not None)
        ) or None
        event_timestamp_from, event_timestamp_to = _resolve_default_event_window(
            only_missing=only_missing,
            unique_tournament_ids=resolved_unique_tournament_ids,
            event_timestamp_from=event_timestamp_from,
            event_timestamp_to=event_timestamp_to,
            now_factory=self.now_factory,
        )
        async with self.database.connection() as connection:
            player_ids = await self._load_player_ids(
                connection,
                endpoint=PLAYER_ENDPOINT,
                unique_tournament_ids=resolved_unique_tournament_ids,
                limit=player_limit,
                offset=player_offset,
                only_missing=only_missing,
                event_timestamp_from=event_timestamp_from,
                event_timestamp_to=event_timestamp_to,
            )
            player_statistics_ids = await self._load_player_ids(
                connection,
                endpoint=PLAYER_STATISTICS_ENDPOINT,
                unique_tournament_ids=resolved_unique_tournament_ids,
                limit=player_limit,
                offset=player_offset,
                only_missing=only_missing,
                event_timestamp_from=event_timestamp_from,
                event_timestamp_to=event_timestamp_to,
            )
            team_ids = await self._load_team_ids(
                connection,
                unique_tournament_ids=resolved_unique_tournament_ids,
                limit=team_limit,
                offset=team_offset,
                only_missing=only_missing,
                event_timestamp_from=event_timestamp_from,
                event_timestamp_to=event_timestamp_to,
            )
            player_overall_requests = await self._load_player_requests(
                connection,
                endpoint=PLAYER_SEASON_OVERALL_STATISTICS_ENDPOINT,
                request_type=PlayerOverallRequest,
                unique_tournament_ids=resolved_unique_tournament_ids,
                limit=player_request_limit,
                offset=player_request_offset,
                only_missing=only_missing,
                event_timestamp_from=event_timestamp_from,
                event_timestamp_to=event_timestamp_to,
            )
            player_heatmap_requests = await self._load_player_requests(
                connection,
                endpoint=PLAYER_SEASON_HEATMAP_OVERALL_ENDPOINT,
                request_type=PlayerHeatmapRequest,
                unique_tournament_ids=resolved_unique_tournament_ids,
                limit=player_request_limit,
                offset=player_request_offset,
                only_missing=only_missing,
                event_timestamp_from=event_timestamp_from,
                event_timestamp_to=event_timestamp_to,
            )
            team_overall_requests = await self._load_team_requests(
                connection,
                endpoint=TEAM_SEASON_OVERALL_STATISTICS_ENDPOINT,
                request_type=TeamOverallRequest,
                unique_tournament_ids=resolved_unique_tournament_ids,
                limit=team_request_limit,
                offset=team_request_offset,
                only_missing=only_missing,
                event_timestamp_from=event_timestamp_from,
                event_timestamp_to=event_timestamp_to,
            )
            team_performance_graph_requests = await self._load_team_requests(
                connection,
                endpoint=TEAM_PERFORMANCE_GRAPH_ENDPOINT,
                request_type=TeamPerformanceGraphRequest,
                unique_tournament_ids=resolved_unique_tournament_ids,
                limit=team_request_limit,
                offset=team_request_offset,
                only_missing=only_missing,
                event_timestamp_from=event_timestamp_from,
                event_timestamp_to=event_timestamp_to,
            )

        self.logger.info(
            "Entities backfill seeds loaded: players=%s player_statistics=%s teams=%s player_overall=%s team_overall=%s player_heatmaps=%s team_graphs=%s",
            len(player_ids),
            len(player_statistics_ids),
            len(team_ids),
            len(player_overall_requests),
            len(team_overall_requests),
            len(player_heatmap_requests),
            len(team_performance_graph_requests),
        )

        ingest = await self.ingest_job.run(
            player_ids=player_ids,
            player_statistics_ids=player_statistics_ids if include_player_statistics else (),
            team_ids=team_ids,
            player_overall_requests=player_overall_requests if include_player_overall else (),
            team_overall_requests=team_overall_requests if include_team_overall else (),
            player_heatmap_requests=player_heatmap_requests if include_player_heatmaps else (),
            team_performance_graph_requests=(
                team_performance_graph_requests if include_team_performance_graphs else ()
            ),
            include_player_statistics=include_player_statistics,
            include_player_statistics_seasons=include_player_statistics_seasons,
            include_player_transfer_history=include_player_transfer_history,
            include_team_statistics_seasons=include_team_statistics_seasons,
            include_team_player_statistics_seasons=include_team_player_statistics_seasons,
            timeout=timeout,
        )
        self.logger.info(
            "Entities backfill completed: players=%s player_statistics=%s teams=%s player_overall=%s team_overall=%s heatmaps=%s team_graphs=%s",
            len(player_ids),
            len(player_statistics_ids if include_player_statistics else ()),
            len(team_ids),
            len(player_overall_requests if include_player_overall else ()),
            len(team_overall_requests if include_team_overall else ()),
            len(player_heatmap_requests if include_player_heatmaps else ()),
            len(team_performance_graph_requests if include_team_performance_graphs else ()),
        )
        return EntitiesBackfillResult(
            player_ids=player_ids,
            player_statistics_ids=player_statistics_ids if include_player_statistics else (),
            team_ids=team_ids,
            player_overall_requests=player_overall_requests if include_player_overall else (),
            team_overall_requests=team_overall_requests if include_team_overall else (),
            player_heatmap_requests=player_heatmap_requests if include_player_heatmaps else (),
            team_performance_graph_requests=(
                team_performance_graph_requests if include_team_performance_graphs else ()
            ),
            ingest=ingest,
        )

    async def _load_player_ids(
        self,
        connection,
        *,
        endpoint,
        unique_tournament_ids: tuple[int, ...] | None,
        limit: int | None,
        offset: int,
        only_missing: bool,
        event_timestamp_from: int | None,
        event_timestamp_to: int | None,
    ) -> tuple[int, ...]:
        resolved_limit = normalize_limit(limit)
        if event_timestamp_from is None and event_timestamp_to is None and not unique_tournament_ids:
            sql = """
                SELECT p.id
                FROM player AS p
                WHERE p.id IS NOT NULL
                ORDER BY p.id
                OFFSET $1
            """
            if resolved_limit is None:
                rows = await connection.fetch(sql, offset)
            else:
                rows = await connection.fetch(f"{sql}\n            LIMIT $2", offset, resolved_limit)
        else:
            sql = """
                SELECT seed.player_id AS id
                FROM (
                    SELECT DISTINCT elp.player_id
                    FROM event_lineup_player AS elp
                    JOIN event AS e ON e.id = elp.event_id
                    WHERE elp.player_id IS NOT NULL
                      AND ($1::bigint IS NULL OR e.start_timestamp >= $1)
                      AND ($2::bigint IS NULL OR e.start_timestamp <= $2)
                      AND ($3::bigint[] IS NULL OR e.unique_tournament_id = ANY($3))
                    UNION
                    SELECT DISTINCT em.player_id
                    FROM event_lineup_missing_player AS em
                    JOIN event AS e ON e.id = em.event_id
                    WHERE em.player_id IS NOT NULL
                      AND ($1::bigint IS NULL OR e.start_timestamp >= $1)
                      AND ($2::bigint IS NULL OR e.start_timestamp <= $2)
                      AND ($3::bigint[] IS NULL OR e.unique_tournament_id = ANY($3))
                ) AS seed
                ORDER BY seed.player_id
                OFFSET $4
            """
            if resolved_limit is None:
                rows = await connection.fetch(sql, event_timestamp_from, event_timestamp_to, list(unique_tournament_ids) if unique_tournament_ids else None, offset)
            else:
                rows = await connection.fetch(
                    f"{sql}\n            LIMIT $5",
                    event_timestamp_from,
                    event_timestamp_to,
                    list(unique_tournament_ids) if unique_tournament_ids else None,
                    offset,
                    resolved_limit,
                )
        player_ids = tuple(int(row["id"]) for row in rows if row["id"] is not None)
        if not only_missing or not player_ids:
            return player_ids
        urls = tuple(endpoint.build_url(player_id=player_id) for player_id in player_ids)
        existing = await self._load_existing_source_urls(
            connection,
            endpoint_pattern=endpoint.pattern,
            urls=urls,
        )
        return tuple(
            player_id
            for player_id in player_ids
            if endpoint.build_url(player_id=player_id) not in existing
        )

    async def _load_team_ids(
        self,
        connection,
        *,
        unique_tournament_ids: tuple[int, ...] | None,
        limit: int | None,
        offset: int,
        only_missing: bool,
        event_timestamp_from: int | None,
        event_timestamp_to: int | None,
    ) -> tuple[int, ...]:
        resolved_limit = normalize_limit(limit)
        if event_timestamp_from is None and event_timestamp_to is None and not unique_tournament_ids:
            sql = """
                SELECT t.id
                FROM team AS t
                WHERE t.id IS NOT NULL
                ORDER BY t.id
                OFFSET $1
            """
            if resolved_limit is None:
                rows = await connection.fetch(sql, offset)
            else:
                rows = await connection.fetch(f"{sql}\n            LIMIT $2", offset, resolved_limit)
        else:
            sql = """
                SELECT seed.team_id AS id
                FROM (
                    SELECT DISTINCT e.home_team_id AS team_id
                    FROM event AS e
                    WHERE e.home_team_id IS NOT NULL
                      AND ($1::bigint IS NULL OR e.start_timestamp >= $1)
                      AND ($2::bigint IS NULL OR e.start_timestamp <= $2)
                      AND ($3::bigint[] IS NULL OR e.unique_tournament_id = ANY($3))
                    UNION
                    SELECT DISTINCT e.away_team_id AS team_id
                    FROM event AS e
                    WHERE e.away_team_id IS NOT NULL
                      AND ($1::bigint IS NULL OR e.start_timestamp >= $1)
                      AND ($2::bigint IS NULL OR e.start_timestamp <= $2)
                      AND ($3::bigint[] IS NULL OR e.unique_tournament_id = ANY($3))
                ) AS seed
                ORDER BY seed.team_id
                OFFSET $4
            """
            if resolved_limit is None:
                rows = await connection.fetch(sql, event_timestamp_from, event_timestamp_to, list(unique_tournament_ids) if unique_tournament_ids else None, offset)
            else:
                rows = await connection.fetch(
                    f"{sql}\n            LIMIT $5",
                    event_timestamp_from,
                    event_timestamp_to,
                    list(unique_tournament_ids) if unique_tournament_ids else None,
                    offset,
                    resolved_limit,
                )
        team_ids = tuple(int(row["id"]) for row in rows if row["id"] is not None)
        if not only_missing or not team_ids:
            return team_ids
        urls = tuple(TEAM_ENDPOINT.build_url(team_id=team_id) for team_id in team_ids)
        existing = await self._load_existing_source_urls(
            connection,
            endpoint_pattern=TEAM_ENDPOINT.pattern,
            urls=urls,
        )
        return tuple(
            team_id
            for team_id in team_ids
            if TEAM_ENDPOINT.build_url(team_id=team_id) not in existing
        )

    async def _load_player_requests(
        self,
        connection,
        *,
        endpoint,
        request_type,
        unique_tournament_ids: tuple[int, ...] | None,
        limit: int | None,
        offset: int,
        only_missing: bool,
        event_timestamp_from: int | None,
        event_timestamp_to: int | None,
    ) -> tuple[PlayerOverallRequest | PlayerHeatmapRequest, ...]:
        resolved_limit = normalize_limit(limit)
        if event_timestamp_from is None and event_timestamp_to is None and not unique_tournament_ids:
            sql = """
                SELECT DISTINCT
                    r.player_id,
                    s.unique_tournament_id,
                    s.season_id
                FROM season_statistics_result AS r
                JOIN season_statistics_snapshot AS s
                    ON s.id = r.snapshot_id
                WHERE r.player_id IS NOT NULL
                ORDER BY s.season_id DESC, s.unique_tournament_id, r.player_id
                OFFSET $1
            """
            if resolved_limit is None:
                rows = await connection.fetch(sql, offset)
            else:
                rows = await connection.fetch(f"{sql}\n            LIMIT $2", offset, resolved_limit)
        else:
            sql = """
                SELECT seed.player_id, seed.unique_tournament_id, seed.season_id
                FROM (
                    SELECT DISTINCT
                        elp.player_id,
                        e.unique_tournament_id,
                        e.season_id
                    FROM event_lineup_player AS elp
                    JOIN event AS e ON e.id = elp.event_id
                    WHERE elp.player_id IS NOT NULL
                      AND e.unique_tournament_id IS NOT NULL
                      AND e.season_id IS NOT NULL
                      AND ($1::bigint IS NULL OR e.start_timestamp >= $1)
                      AND ($2::bigint IS NULL OR e.start_timestamp <= $2)
                      AND ($3::bigint[] IS NULL OR e.unique_tournament_id = ANY($3))
                    UNION
                    SELECT DISTINCT
                        em.player_id,
                        e.unique_tournament_id,
                        e.season_id
                    FROM event_lineup_missing_player AS em
                    JOIN event AS e ON e.id = em.event_id
                    WHERE em.player_id IS NOT NULL
                      AND e.unique_tournament_id IS NOT NULL
                      AND e.season_id IS NOT NULL
                      AND ($1::bigint IS NULL OR e.start_timestamp >= $1)
                      AND ($2::bigint IS NULL OR e.start_timestamp <= $2)
                      AND ($3::bigint[] IS NULL OR e.unique_tournament_id = ANY($3))
                ) AS seed
                ORDER BY seed.season_id DESC, seed.unique_tournament_id, seed.player_id
                OFFSET $4
            """
            if resolved_limit is None:
                rows = await connection.fetch(sql, event_timestamp_from, event_timestamp_to, list(unique_tournament_ids) if unique_tournament_ids else None, offset)
            else:
                rows = await connection.fetch(
                    f"{sql}\n            LIMIT $5",
                    event_timestamp_from,
                    event_timestamp_to,
                    list(unique_tournament_ids) if unique_tournament_ids else None,
                    offset,
                    resolved_limit,
                )
        requests = tuple(
            request_type(
                player_id=int(row["player_id"]),
                unique_tournament_id=int(row["unique_tournament_id"]),
                season_id=int(row["season_id"]),
            )
            for row in rows
            if row["player_id"] is not None
            and row["unique_tournament_id"] is not None
            and row["season_id"] is not None
        )
        if not only_missing or not requests:
            return requests
        urls = tuple(
            endpoint.build_url(
                player_id=request.player_id,
                unique_tournament_id=request.unique_tournament_id,
                season_id=request.season_id,
            )
            for request in requests
        )
        existing = await self._load_existing_source_urls(connection, endpoint_pattern=endpoint.pattern, urls=urls)
        return tuple(
            request
            for request in requests
            if endpoint.build_url(
                player_id=request.player_id,
                unique_tournament_id=request.unique_tournament_id,
                season_id=request.season_id,
            )
            not in existing
        )

    async def _load_team_requests(
        self,
        connection,
        *,
        endpoint,
        request_type,
        unique_tournament_ids: tuple[int, ...] | None,
        limit: int | None,
        offset: int,
        only_missing: bool,
        event_timestamp_from: int | None,
        event_timestamp_to: int | None,
    ) -> tuple[TeamOverallRequest | TeamPerformanceGraphRequest, ...]:
        resolved_limit = normalize_limit(limit)
        sql = """
            SELECT seed.team_id, seed.unique_tournament_id, seed.season_id
            FROM (
                SELECT DISTINCT
                    e.home_team_id AS team_id,
                    e.unique_tournament_id,
                    e.season_id
                FROM event AS e
                WHERE e.home_team_id IS NOT NULL
                    AND e.unique_tournament_id IS NOT NULL
                    AND e.season_id IS NOT NULL
                    AND ($1::bigint IS NULL OR e.start_timestamp >= $1)
                    AND ($2::bigint IS NULL OR e.start_timestamp <= $2)
                    AND ($3::bigint[] IS NULL OR e.unique_tournament_id = ANY($3))
                UNION
                SELECT DISTINCT
                    e.away_team_id AS team_id,
                    e.unique_tournament_id,
                    e.season_id
                FROM event AS e
                WHERE e.away_team_id IS NOT NULL
                    AND e.unique_tournament_id IS NOT NULL
                    AND e.season_id IS NOT NULL
                    AND ($1::bigint IS NULL OR e.start_timestamp >= $1)
                    AND ($2::bigint IS NULL OR e.start_timestamp <= $2)
                    AND ($3::bigint[] IS NULL OR e.unique_tournament_id = ANY($3))
            ) AS seed
            ORDER BY seed.season_id DESC, seed.unique_tournament_id, seed.team_id
            OFFSET $4
        """
        if resolved_limit is None:
            rows = await connection.fetch(sql, event_timestamp_from, event_timestamp_to, list(unique_tournament_ids) if unique_tournament_ids else None, offset)
        else:
            rows = await connection.fetch(
                f"{sql}\n            LIMIT $5",
                event_timestamp_from,
                event_timestamp_to,
                list(unique_tournament_ids) if unique_tournament_ids else None,
                offset,
                resolved_limit,
            )
        requests = tuple(
            request_type(
                team_id=int(row["team_id"]),
                unique_tournament_id=int(row["unique_tournament_id"]),
                season_id=int(row["season_id"]),
            )
            for row in rows
            if row["team_id"] is not None
            and row["unique_tournament_id"] is not None
            and row["season_id"] is not None
        )
        if not only_missing or not requests:
            return requests
        urls = tuple(
            endpoint.build_url(
                team_id=request.team_id,
                unique_tournament_id=request.unique_tournament_id,
                season_id=request.season_id,
            )
            for request in requests
        )
        existing = await self._load_existing_source_urls(connection, endpoint_pattern=endpoint.pattern, urls=urls)
        return tuple(
            request
            for request in requests
            if endpoint.build_url(
                team_id=request.team_id,
                unique_tournament_id=request.unique_tournament_id,
                season_id=request.season_id,
            )
            not in existing
        )

    async def _load_existing_source_urls(
        self,
        connection,
        *,
        endpoint_pattern: str,
        urls: tuple[str, ...],
    ) -> set[str]:
        if not urls:
            return set()
        rows = await connection.fetch(
            """
            SELECT source_url
            FROM api_payload_snapshot
            WHERE endpoint_pattern = $1
              AND source_url = ANY($2::text[])
            """,
            endpoint_pattern,
            list(urls),
        )
        return {str(row["source_url"]) for row in rows if row["source_url"] is not None}


def _resolve_default_event_window(
    *,
    only_missing: bool,
    unique_tournament_ids: tuple[int, ...] | None,
    event_timestamp_from: int | None,
    event_timestamp_to: int | None,
    now_factory,
) -> tuple[int | None, int | None]:
    if (
        not only_missing
        or unique_tournament_ids
        or event_timestamp_from is not None
        or event_timestamp_to is not None
    ):
        return event_timestamp_from, event_timestamp_to
    resolved_now = now_factory()
    return (
        int((resolved_now - timedelta(days=180)).timestamp()),
        int((resolved_now + timedelta(days=7)).timestamp()),
    )


def _default_now_utc() -> datetime:
    return datetime.now(timezone.utc)
