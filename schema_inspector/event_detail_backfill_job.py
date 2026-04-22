"""Batch backfill job for event-detail endpoints."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
        now_factory=None,
    ) -> None:
        self.detail_job = detail_job
        self.database = database
        self.logger = logger or logging.getLogger(__name__)
        self.now_factory = now_factory or _default_now_utc

    async def run(
        self,
        *,
        limit: int | None = None,
        offset: int = 0,
        only_missing: bool = True,
        season_ids: Iterable[int] | None = None,
        unique_tournament_ids: Iterable[int] | None = None,
        unique_tournament_id: int | None = None,
        start_timestamp_from: int | None = None,
        start_timestamp_to: int | None = None,
        provider_ids: Iterable[int] = (1,),
        concurrency: int = 3,
        timeout: float = 20.0,
    ) -> EventDetailBackfillResult:
        resolved_season_ids = tuple(dict.fromkeys(int(item) for item in (season_ids or ()) if item is not None))
        resolved_unique_tournament_ids = tuple(
            dict.fromkeys(int(item) for item in (unique_tournament_ids or ()) if item is not None)
        )
        start_timestamp_from, start_timestamp_to = _resolve_default_window(
            only_missing=only_missing,
            unique_tournament_id=unique_tournament_id,
            unique_tournament_ids=resolved_unique_tournament_ids or None,
            start_timestamp_from=start_timestamp_from,
            start_timestamp_to=start_timestamp_to,
            now_factory=self.now_factory,
        )
        event_ids = await self._load_event_ids(
            limit=limit,
            offset=offset,
            only_missing=only_missing,
            season_ids=resolved_season_ids or None,
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
            "Event-detail backfill loaded candidates=%s only_missing=%s unique_tournament_id=%s unique_tournament_ids=%s season_ids=%s concurrency=%s",
            total_candidates,
            only_missing,
            unique_tournament_id,
            len(resolved_unique_tournament_ids),
            len(resolved_season_ids),
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
        season_ids: tuple[int, ...] | None,
        unique_tournament_id: int | None,
        unique_tournament_ids: tuple[int, ...] | None,
        start_timestamp_from: int | None,
        start_timestamp_to: int | None,
    ) -> tuple[int, ...]:
        resolved_limit = normalize_limit(limit)
        resolved_offset = max(int(offset), 0)
        page_size = _candidate_page_size(resolved_limit)
        collected_event_ids: list[int] = []
        scanned_candidates = 0

        async with self.database.connection() as connection:
            while True:
                remaining = None if resolved_limit is None else max(resolved_limit - len(collected_event_ids), 0)
                if remaining == 0:
                    break

                fetch_limit = page_size if remaining is None else max(page_size, remaining)
                candidate_event_ids = await self._load_candidate_event_ids_page(
                    connection,
                    limit=fetch_limit,
                    offset=resolved_offset,
                    season_ids=season_ids,
                    unique_tournament_id=unique_tournament_id,
                    unique_tournament_ids=unique_tournament_ids,
                    start_timestamp_from=start_timestamp_from,
                    start_timestamp_to=start_timestamp_to,
                )
                if not candidate_event_ids:
                    break

                scanned_candidates += len(candidate_event_ids)
                resolved_offset += len(candidate_event_ids)
                filtered_event_ids = candidate_event_ids
                if only_missing:
                    existing_event_ids = await self._load_existing_event_detail_ids(connection, candidate_event_ids)
                    filtered_event_ids = tuple(
                        event_id for event_id in candidate_event_ids if event_id not in existing_event_ids
                    )
                collected_event_ids.extend(filtered_event_ids)

                if resolved_limit is not None and len(collected_event_ids) >= resolved_limit:
                    break
                if len(candidate_event_ids) < fetch_limit:
                    break

        resolved_event_ids = tuple(
            collected_event_ids if resolved_limit is None else collected_event_ids[:resolved_limit]
        )
        self.logger.info(
            "Event-detail candidate scan complete: scanned=%s returned=%s only_missing=%s page_size=%s",
            scanned_candidates,
            len(resolved_event_ids),
            only_missing,
            page_size,
        )
        return resolved_event_ids

    async def _load_candidate_event_ids_page(
        self,
        connection,
        *,
        limit: int | None,
        offset: int,
        season_ids: tuple[int, ...] | None,
        unique_tournament_id: int | None,
        unique_tournament_ids: tuple[int, ...] | None,
        start_timestamp_from: int | None,
        start_timestamp_to: int | None,
    ) -> tuple[int, ...]:
        sql = """
            SELECT e.id
            FROM event AS e
            WHERE ($1::bigint IS NULL OR e.unique_tournament_id = $1)
              AND ($2::bigint[] IS NULL OR e.unique_tournament_id = ANY($2))
              AND ($3::bigint[] IS NULL OR e.season_id = ANY($3))
              AND ($4::bigint IS NULL OR e.start_timestamp >= $4)
              AND ($5::bigint IS NULL OR e.start_timestamp <= $5)
            ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
            OFFSET $6
        """
        args = (
            unique_tournament_id,
            list(unique_tournament_ids) if unique_tournament_ids else None,
            list(season_ids) if season_ids else None,
            start_timestamp_from,
            start_timestamp_to,
            offset,
        )
        if limit is None:
            rows = await connection.fetch(sql, *args)
        else:
            rows = await connection.fetch(f"{sql}\n            LIMIT $7", *args, limit)
        return tuple(int(row["id"]) for row in rows if row["id"] is not None)

    async def _load_existing_event_detail_ids(self, connection, event_ids: tuple[int, ...]) -> frozenset[int]:
        if not event_ids:
            return frozenset()
        scope_key_to_event_id = {_event_detail_scope_key(event_id): event_id for event_id in event_ids}
        rows = await connection.fetch(
            """
            SELECT scope_key
            FROM api_snapshot_head
            WHERE scope_key = ANY($1::text[])
            """,
            list(scope_key_to_event_id),
        )
        return frozenset(
            scope_key_to_event_id[str(row["scope_key"])]
            for row in rows
            if row["scope_key"] is not None and str(row["scope_key"]) in scope_key_to_event_id
        )


def _resolve_default_window(
    *,
    only_missing: bool,
    unique_tournament_id: int | None,
    unique_tournament_ids: tuple[int, ...] | None,
    start_timestamp_from: int | None,
    start_timestamp_to: int | None,
    now_factory,
) -> tuple[int | None, int | None]:
    if (
        not only_missing
        or unique_tournament_id is not None
        or unique_tournament_ids
        or start_timestamp_from is not None
        or start_timestamp_to is not None
    ):
        return start_timestamp_from, start_timestamp_to
    resolved_now = now_factory()
    return (
        int((resolved_now - timedelta(days=180)).timestamp()),
        int((resolved_now + timedelta(days=7)).timestamp()),
    )


def _default_now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _candidate_page_size(limit: int | None) -> int:
    if limit is None:
        return 1000
    return min(max(int(limit) * 4, 1000), 5000)


def _event_detail_scope_key(event_id: int) -> str:
    return f"event:{int(event_id)}:/api/v1/event/{{event_id}}"
