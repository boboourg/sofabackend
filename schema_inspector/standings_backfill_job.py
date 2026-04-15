"""Backfill job for standings endpoints using seasons already stored in PostgreSQL."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .db import AsyncpgDatabase
from .endpoints import TOURNAMENT_STANDINGS_ENDPOINT, UNIQUE_TOURNAMENT_STANDINGS_ENDPOINT
from .limit_utils import normalize_limit
from .standings_job import StandingsIngestJob, StandingsIngestResult


@dataclass(frozen=True)
class StandingsBackfillItem:
    source_kind: str
    source_id: int
    season_id: int
    scopes: tuple[str, ...]
    success: bool
    error: str | None = None
    result: StandingsIngestResult | None = None


@dataclass(frozen=True)
class StandingsBackfillResult:
    total_candidates: int
    processed: int
    succeeded: int
    failed: int
    items: tuple[StandingsBackfillItem, ...]


@dataclass(frozen=True)
class _StandingsSeed:
    source_kind: str
    source_id: int
    season_id: int


class StandingsBackfillJob:
    """Loads standings seeds from PostgreSQL and hydrates standings endpoints in batches."""

    def __init__(
        self,
        ingest_job: StandingsIngestJob,
        database: AsyncpgDatabase,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.ingest_job = ingest_job
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    async def run(
        self,
        *,
        season_limit: int | None = None,
        season_offset: int = 0,
        only_missing: bool = True,
        scopes: tuple[str, ...] = ("total",),
        concurrency: int = 2,
        timeout: float = 20.0,
    ) -> StandingsBackfillResult:
        resolved_scopes = tuple(dict.fromkeys(scopes or ("total",)))
        seeds = await self._load_seeds(
            limit=season_limit,
            offset=season_offset,
            only_missing=only_missing,
            scopes=resolved_scopes,
        )
        semaphore = asyncio.Semaphore(max(concurrency, 1))

        async def _run_one(seed: _StandingsSeed) -> StandingsBackfillItem:
            async with semaphore:
                try:
                    if seed.source_kind == "unique_tournament":
                        result = await self.ingest_job.run_for_unique_tournament(
                            seed.source_id,
                            seed.season_id,
                            scopes=resolved_scopes,
                            timeout=timeout,
                        )
                    else:
                        result = await self.ingest_job.run_for_tournament(
                            seed.source_id,
                            seed.season_id,
                            scopes=resolved_scopes,
                            timeout=timeout,
                        )
                except Exception as exc:
                    self.logger.warning(
                        "Standings backfill failed for source_kind=%s source_id=%s season_id=%s: %s",
                        seed.source_kind,
                        seed.source_id,
                        seed.season_id,
                        exc,
                    )
                    return StandingsBackfillItem(
                        source_kind=seed.source_kind,
                        source_id=seed.source_id,
                        season_id=seed.season_id,
                        scopes=resolved_scopes,
                        success=False,
                        error=str(exc),
                    )
                return StandingsBackfillItem(
                    source_kind=seed.source_kind,
                    source_id=seed.source_id,
                    season_id=seed.season_id,
                    scopes=resolved_scopes,
                    success=True,
                    result=result,
                )

        items = tuple(await asyncio.gather(*(_run_one(seed) for seed in seeds)))
        succeeded = sum(1 for item in items if item.success)
        failed = len(items) - succeeded
        self.logger.info(
            "Standings backfill completed: candidates=%s succeeded=%s failed=%s",
            len(seeds),
            succeeded,
            failed,
        )
        return StandingsBackfillResult(
            total_candidates=len(seeds),
            processed=len(items),
            succeeded=succeeded,
            failed=failed,
            items=items,
        )

    async def _load_seeds(
        self,
        *,
        limit: int | None,
        offset: int,
        only_missing: bool,
        scopes: tuple[str, ...],
    ) -> tuple[_StandingsSeed, ...]:
        resolved_limit = normalize_limit(limit)
        async with self.database.connection() as connection:
            sql = """
                SELECT DISTINCT e.unique_tournament_id, e.tournament_id, e.season_id
                FROM event AS e
                WHERE e.season_id IS NOT NULL
                    AND (e.unique_tournament_id IS NOT NULL OR e.tournament_id IS NOT NULL)
                ORDER BY e.season_id DESC, e.unique_tournament_id NULLS LAST, e.tournament_id NULLS LAST
                OFFSET $1
            """
            if resolved_limit is None:
                rows = await connection.fetch(sql, offset)
            else:
                rows = await connection.fetch(f"{sql}\n                LIMIT $2", offset, resolved_limit)
            seeds = tuple(
                _StandingsSeed(
                    source_kind="unique_tournament" if row["unique_tournament_id"] is not None else "tournament",
                    source_id=int(
                        row["unique_tournament_id"]
                        if row["unique_tournament_id"] is not None
                        else row["tournament_id"]
                    ),
                    season_id=int(row["season_id"]),
                )
                for row in rows
                if row["season_id"] is not None
                and (row["unique_tournament_id"] is not None or row["tournament_id"] is not None)
            )
            if not only_missing or not seeds:
                return seeds

            urls_by_seed: dict[_StandingsSeed, tuple[str, ...]] = {}
            all_urls: list[str] = []
            for seed in seeds:
                if seed.source_kind == "unique_tournament":
                    seed_urls = tuple(
                        UNIQUE_TOURNAMENT_STANDINGS_ENDPOINT.build_url(
                            unique_tournament_id=seed.source_id,
                            season_id=seed.season_id,
                            scope=scope,
                        )
                        for scope in scopes
                    )
                else:
                    seed_urls = tuple(
                        TOURNAMENT_STANDINGS_ENDPOINT.build_url(
                            tournament_id=seed.source_id,
                            season_id=seed.season_id,
                            scope=scope,
                        )
                        for scope in scopes
                    )
                urls_by_seed[seed] = seed_urls
                all_urls.extend(seed_urls)

            existing_rows = await connection.fetch(
                """
                SELECT source_url
                FROM api_payload_snapshot
                WHERE source_url = ANY($1::text[])
                """,
                list(all_urls),
            )
            existing = {str(row["source_url"]) for row in existing_rows if row["source_url"] is not None}

        return tuple(seed for seed in seeds if not all(url in existing for url in urls_by_seed[seed]))
