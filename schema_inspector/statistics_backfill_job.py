"""Backfill job for season-statistics endpoints using seeds already stored in PostgreSQL."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Iterable

from .db import AsyncpgDatabase
from .endpoints import UNIQUE_TOURNAMENT_STATISTICS_INFO_ENDPOINT
from .limit_utils import normalize_limit
from .statistics_job import StatisticsIngestJob, StatisticsIngestResult
from .statistics_parser import StatisticsQuery


@dataclass(frozen=True)
class StatisticsBackfillItem:
    unique_tournament_id: int
    season_id: int
    queries: tuple[StatisticsQuery, ...]
    include_info: bool
    success: bool
    error: str | None = None
    result: StatisticsIngestResult | None = None


@dataclass(frozen=True)
class StatisticsBackfillResult:
    total_candidates: int
    processed: int
    succeeded: int
    failed: int
    items: tuple[StatisticsBackfillItem, ...]


class StatisticsBackfillJob:
    """Loads season ids from PostgreSQL and hydrates statistics endpoints in batches."""

    def __init__(
        self,
        ingest_job: StatisticsIngestJob,
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
        queries: Iterable[StatisticsQuery] | None = None,
        include_info: bool = True,
        concurrency: int = 2,
        timeout: float = 20.0,
    ) -> StatisticsBackfillResult:
        resolved_queries = (
            (StatisticsQuery(limit=20, offset=0),) if queries is None else tuple(dict.fromkeys(tuple(queries)))
        )
        season_pairs = await self._load_season_pairs(
            limit=season_limit,
            offset=season_offset,
            only_missing=only_missing,
            queries=resolved_queries,
            include_info=include_info,
        )
        semaphore = asyncio.Semaphore(max(concurrency, 1))

        async def _run_one(unique_tournament_id: int, season_id: int) -> StatisticsBackfillItem:
            async with semaphore:
                try:
                    result = await self.ingest_job.run(
                        unique_tournament_id,
                        season_id,
                        queries=resolved_queries,
                        include_info=include_info,
                        timeout=timeout,
                    )
                except Exception as exc:
                    self.logger.warning(
                        "Statistics backfill failed for unique_tournament_id=%s season_id=%s: %s",
                        unique_tournament_id,
                        season_id,
                        exc,
                    )
                    return StatisticsBackfillItem(
                        unique_tournament_id=unique_tournament_id,
                        season_id=season_id,
                        queries=resolved_queries,
                        include_info=include_info,
                        success=False,
                        error=str(exc),
                    )
                return StatisticsBackfillItem(
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    queries=resolved_queries,
                    include_info=include_info,
                    success=True,
                    result=result,
                )

        items = tuple(
            await asyncio.gather(
                *(_run_one(unique_tournament_id, season_id) for unique_tournament_id, season_id in season_pairs)
            )
        )
        succeeded = sum(1 for item in items if item.success)
        failed = len(items) - succeeded
        self.logger.info(
            "Statistics backfill completed: candidates=%s succeeded=%s failed=%s",
            len(season_pairs),
            succeeded,
            failed,
        )
        return StatisticsBackfillResult(
            total_candidates=len(season_pairs),
            processed=len(items),
            succeeded=succeeded,
            failed=failed,
            items=items,
        )

    async def _load_season_pairs(
        self,
        *,
        limit: int | None,
        offset: int,
        only_missing: bool,
        queries: tuple[StatisticsQuery, ...],
        include_info: bool,
    ) -> tuple[tuple[int, int], ...]:
        resolved_limit = normalize_limit(limit)
        async with self.database.connection() as connection:
            sql = """
                SELECT DISTINCT e.unique_tournament_id, e.season_id
                FROM event AS e
                WHERE e.unique_tournament_id IS NOT NULL
                    AND e.season_id IS NOT NULL
                ORDER BY e.season_id DESC, e.unique_tournament_id
                OFFSET $1
            """
            if resolved_limit is None:
                rows = await connection.fetch(sql, offset)
            else:
                rows = await connection.fetch(f"{sql}\n                LIMIT $2", offset, resolved_limit)
            season_pairs = tuple(
                (int(row["unique_tournament_id"]), int(row["season_id"]))
                for row in rows
                if row["unique_tournament_id"] is not None and row["season_id"] is not None
            )
            if not only_missing or not season_pairs:
                return season_pairs

            info_urls_by_pair: dict[tuple[int, int], str] = {}
            all_info_urls: list[str] = []
            for unique_tournament_id, season_id in season_pairs:
                if include_info:
                    info_url = UNIQUE_TOURNAMENT_STATISTICS_INFO_ENDPOINT.build_url(
                        unique_tournament_id=unique_tournament_id,
                        season_id=season_id,
                    )
                    info_urls_by_pair[(unique_tournament_id, season_id)] = info_url
                    all_info_urls.append(info_url)

            existing_info: set[str] = set()
            if all_info_urls:
                existing_rows = await connection.fetch(
                    """
                    SELECT source_url
                    FROM api_payload_snapshot
                    WHERE source_url = ANY($1::text[])
                    """,
                    list(all_info_urls),
                )
                existing_info = {str(row["source_url"]) for row in existing_rows if row["source_url"] is not None}

            snapshot_rows = await connection.fetch(
                """
                SELECT
                    unique_tournament_id,
                    season_id,
                    pages,
                    limit_value,
                    offset_value,
                    order_code,
                    accumulation,
                    group_code,
                    fields,
                    filters
                FROM season_statistics_snapshot
                WHERE unique_tournament_id = ANY($1::bigint[])
                  AND season_id = ANY($2::bigint[])
                """,
                list({pair[0] for pair in season_pairs}),
                list({pair[1] for pair in season_pairs}),
            )

        snapshot_rows_by_pair: dict[tuple[int, int], list[dict[str, object]]] = {}
        season_pair_set = set(season_pairs)
        for row in snapshot_rows:
            unique_tournament_id = row["unique_tournament_id"]
            season_id = row["season_id"]
            if unique_tournament_id is None or season_id is None:
                continue
            pair = (int(unique_tournament_id), int(season_id))
            if pair not in season_pair_set:
                continue
            snapshot_rows_by_pair.setdefault(pair, []).append(
                {
                    "pages": row["pages"],
                    "limit_value": row["limit_value"],
                    "offset_value": row["offset_value"],
                    "order_code": row["order_code"],
                    "accumulation": row["accumulation"],
                    "group_code": row["group_code"],
                    "fields": row["fields"],
                    "filters": row["filters"],
                }
            )

        filtered_pairs: list[tuple[int, int]] = []
        for pair in season_pairs:
            if include_info and info_urls_by_pair.get(pair) not in existing_info:
                filtered_pairs.append(pair)
                continue

            pair_snapshot_rows = snapshot_rows_by_pair.get(pair, [])
            if not self._has_complete_statistics_pages(pair_snapshot_rows, queries):
                filtered_pairs.append(pair)

        return tuple(filtered_pairs)

    @staticmethod
    def _has_complete_statistics_pages(
        rows: list[dict[str, object]],
        queries: tuple[StatisticsQuery, ...],
    ) -> bool:
        if not queries:
            return True

        for query in queries:
            matching_rows = [row for row in rows if StatisticsBackfillJob._snapshot_matches_query(row, query)]
            if not matching_rows:
                return False

            base_offset = query.offset or 0
            seen_offsets = {
                int(offset)
                for offset in (row["offset_value"] for row in matching_rows)
                if isinstance(offset, int)
            }
            if any(row["offset_value"] is None for row in matching_rows):
                seen_offsets.add(base_offset)
            if base_offset not in seen_offsets:
                return False

            expected_pages = max(
                int(row["pages"]) if isinstance(row["pages"], int) and row["pages"] > 0 else 1
                for row in matching_rows
            )
            if len(seen_offsets) < expected_pages:
                return False

        return True

    @staticmethod
    def _snapshot_matches_query(row: dict[str, object], query: StatisticsQuery) -> bool:
        return (
            row["limit_value"] == query.limit
            and row["order_code"] == query.order
            and row["accumulation"] == query.accumulation
            and row["group_code"] == query.group
            and StatisticsBackfillJob._normalize_json_value(row["fields"])
            == StatisticsBackfillJob._normalize_json_value(query.parsed_fields())
            and StatisticsBackfillJob._normalize_json_value(row["filters"])
            == StatisticsBackfillJob._normalize_json_value(query.parsed_filters())
        )

    @staticmethod
    def _normalize_json_value(value: object) -> str | None:
        if value is None:
            return None
        return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
