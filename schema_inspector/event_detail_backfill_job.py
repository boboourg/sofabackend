"""Batch backfill job for event-detail endpoints."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Iterable

from .db import AsyncpgDatabase
from .event_detail_job import EventDetailIngestJob, EventDetailIngestResult
from .limit_utils import normalize_limit


@dataclass(frozen=True)
class EventDetailBackfillItem:
    event_id: int
    success: bool
    error: str | None = None
    result: EventDetailIngestResult | None = None


@dataclass(frozen=True)
class EventDetailBackfillResult:
    total_candidates: int
    processed: int
    succeeded: int
    failed: int
    items: tuple[EventDetailBackfillItem, ...]


class EventDetailBackfillJob:
    """Loads event ids from PostgreSQL and hydrates event-detail endpoints in batches."""

    def __init__(
        self,
        detail_job: EventDetailIngestJob,
        database: AsyncpgDatabase,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.detail_job = detail_job
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    async def run(
        self,
        *,
        limit: int | None = None,
        offset: int = 0,
        only_missing: bool = True,
        unique_tournament_ids: Iterable[int] | None = None,
        unique_tournament_id: int | None = None,
        start_timestamp_from: int | None = None,
        start_timestamp_to: int | None = None,
        provider_ids: Iterable[int] = (1,),
        concurrency: int = 3,
        timeout: float = 20.0,
    ) -> EventDetailBackfillResult:
        resolved_unique_tournament_ids = tuple(
            dict.fromkeys(int(item) for item in (unique_tournament_ids or ()) if item is not None)
        )
        event_ids = await self._load_event_ids(
            limit=limit,
            offset=offset,
            only_missing=only_missing,
            unique_tournament_id=unique_tournament_id,
            unique_tournament_ids=resolved_unique_tournament_ids or None,
            start_timestamp_from=start_timestamp_from,
            start_timestamp_to=start_timestamp_to,
        )
        semaphore = asyncio.Semaphore(max(concurrency, 1))
        resolved_provider_ids = tuple(dict.fromkeys(provider_ids))
        total_candidates = len(event_ids)
        progress_lock = asyncio.Lock()
        progress = {"processed": 0, "succeeded": 0, "failed": 0}

        self.logger.info(
            "Event-detail backfill loaded candidates=%s only_missing=%s unique_tournament_id=%s unique_tournament_ids=%s concurrency=%s",
            total_candidates,
            only_missing,
            unique_tournament_id,
            len(resolved_unique_tournament_ids),
            max(concurrency, 1),
        )

        async def _run_one(event_id: int) -> EventDetailBackfillItem:
            async with semaphore:
                try:
                    result = await self.detail_job.run(
                        event_id,
                        provider_ids=resolved_provider_ids,
                        timeout=timeout,
                    )
                except Exception as exc:
                    self.logger.warning("Event-detail backfill failed for event_id=%s: %s", event_id, exc)
                    item = EventDetailBackfillItem(event_id=event_id, success=False, error=str(exc))
                else:
                    item = EventDetailBackfillItem(event_id=event_id, success=True, result=result)

                async with progress_lock:
                    progress["processed"] += 1
                    if item.success:
                        progress["succeeded"] += 1
                    else:
                        progress["failed"] += 1
                    self.logger.info(
                        "Event-detail progress: processed=%s/%s succeeded=%s failed=%s event_id=%s",
                        progress["processed"],
                        total_candidates,
                        progress["succeeded"],
                        progress["failed"],
                        event_id,
                    )
                return item

        items = tuple(await asyncio.gather(*(_run_one(event_id) for event_id in event_ids)))
        succeeded = sum(1 for item in items if item.success)
        failed = len(items) - succeeded
        self.logger.info(
            "Event-detail backfill completed: candidates=%s succeeded=%s failed=%s",
            len(event_ids),
            succeeded,
            failed,
        )
        return EventDetailBackfillResult(
            total_candidates=len(event_ids),
            processed=len(items),
            succeeded=succeeded,
            failed=failed,
            items=items,
        )

    async def _load_event_ids(
        self,
        *,
        limit: int | None,
        offset: int,
        only_missing: bool,
        unique_tournament_id: int | None,
        unique_tournament_ids: tuple[int, ...] | None,
        start_timestamp_from: int | None,
        start_timestamp_to: int | None,
    ) -> tuple[int, ...]:
        resolved_limit = normalize_limit(limit)
        sql = """
            SELECT e.id
            FROM event AS e
            WHERE (
                $1::boolean = FALSE OR
                NOT EXISTS (
                    SELECT 1
                    FROM api_payload_snapshot AS s
                    WHERE s.endpoint_pattern = '/api/v1/event/{event_id}'
                      AND s.context_entity_type = 'event'
                      AND s.context_entity_id = e.id
                )
            )
              AND ($2::bigint IS NULL OR e.unique_tournament_id = $2)
              AND ($3::bigint[] IS NULL OR e.unique_tournament_id = ANY($3))
              AND ($4::bigint IS NULL OR e.start_timestamp >= $4)
              AND ($5::bigint IS NULL OR e.start_timestamp <= $5)
            ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
            OFFSET $6
        """
        async with self.database.connection() as connection:
            if resolved_limit is None:
                rows = await connection.fetch(
                    sql,
                    only_missing,
                    unique_tournament_id,
                    list(unique_tournament_ids) if unique_tournament_ids else None,
                    start_timestamp_from,
                    start_timestamp_to,
                    offset,
                )
            else:
                rows = await connection.fetch(
                    f"{sql}\n            LIMIT $7",
                    only_missing,
                    unique_tournament_id,
                    list(unique_tournament_ids) if unique_tournament_ids else None,
                    start_timestamp_from,
                    start_timestamp_to,
                    offset,
                    resolved_limit,
                )
        return tuple(int(row["id"]) for row in rows if row["id"] is not None)
