"""Async ETL jobs for event-detail endpoints."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Iterable

from .db import AsyncpgDatabase
from .event_detail_parser import EventDetailBundle, EventDetailParser
from .event_detail_repository import EventDetailRepository, EventDetailWriteResult


@dataclass
class EventDetailIngestProfile:
    upstream_fetch_ms: int = 0
    parse_ms: int = 0
    registry_sync_ms: int = 0
    db_persist_ms: int = 0


@dataclass(frozen=True)
class EventDetailIngestResult:
    event_id: int
    provider_ids: tuple[int, ...]
    parsed: EventDetailBundle
    profile: EventDetailIngestProfile
    written: EventDetailWriteResult


class EventDetailIngestJob:
    """Runs one event-detail parser flow and persists it transactionally."""

    def __init__(
        self,
        parser: EventDetailParser,
        repository: EventDetailRepository,
        database: AsyncpgDatabase,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.parser = parser
        self.repository = repository
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    async def run(
        self,
        event_id: int,
        *,
        provider_ids: Iterable[int] = (1,),
        timeout: float = 20.0,
    ) -> EventDetailIngestResult:
        resolved_provider_ids = tuple(dict.fromkeys(provider_ids))
        profile = EventDetailIngestProfile()
        parse_started = time.perf_counter()
        bundle = await self.parser.fetch_bundle(
            event_id,
            provider_ids=resolved_provider_ids,
            timeout=timeout,
            profile=profile,
        )
        parse_elapsed_ms = round((time.perf_counter() - parse_started) * 1000)
        profile.parse_ms = max(parse_elapsed_ms - profile.upstream_fetch_ms, 0)
        persist_started = time.perf_counter()
        async with self.database.transaction() as connection:
            try:
                write_result = await self.repository.upsert_bundle(connection, bundle, profile=profile)
            except Exception as exc:
                if _is_undefined_table_error(exc):
                    raise RuntimeError(
                        "Database schema is out of date for event-detail ingestion. "
                        "Run `.\\.venv311\\Scripts\\python.exe -m schema_inspector.db_setup_cli` "
                        "to apply the latest migrations, including "
                        "`2026-04-17_event_player_analytics.sql`."
                    ) from exc
                raise
        profile.db_persist_ms = round((time.perf_counter() - persist_started) * 1000)
        self.logger.info(
            "Event-detail ingest completed: event_id=%s players=%s markets=%s",
            event_id,
            write_result.player_rows,
            write_result.event_market_rows,
        )
        return EventDetailIngestResult(
            event_id=event_id,
            provider_ids=resolved_provider_ids,
            parsed=bundle,
            profile=profile,
            written=write_result,
        )


def _is_undefined_table_error(exc: Exception) -> bool:
    return exc.__class__.__name__ == "UndefinedTableError"
