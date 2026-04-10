"""Batch backfill job for event-detail endpoints."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Iterable

from .db import AsyncpgDatabase
from .event_detail_job import EventDetailIngestJob, EventDetailIngestResult


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
        limit: int = 100,
        offset: int = 0,
        only_missing: bool = True,
        provider_ids: Iterable[int] = (1,),
        concurrency: int = 3,
        timeout: float = 20.0,
    ) -> EventDetailBackfillResult:
        event_ids = await self._load_event_ids(limit=limit, offset=offset, only_missing=only_missing)
        semaphore = asyncio.Semaphore(max(concurrency, 1))
        resolved_provider_ids = tuple(dict.fromkeys(provider_ids))

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
                    return EventDetailBackfillItem(event_id=event_id, success=False, error=str(exc))
                return EventDetailBackfillItem(event_id=event_id, success=True, result=result)

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

    async def _load_event_ids(self, *, limit: int, offset: int, only_missing: bool) -> tuple[int, ...]:
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
            ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
            OFFSET $2
            LIMIT $3
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(sql, only_missing, offset, limit)
        return tuple(int(row["id"]) for row in rows if row["id"] is not None)
