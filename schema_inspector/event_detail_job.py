"""Async ETL jobs for event-detail endpoints."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Iterable

from .db import AsyncpgDatabase
from .endpoints import EVENT_INCIDENTS_ENDPOINT, EVENT_STATISTICS_ENDPOINT
from .event_detail_parser import EventDetailBundle, EventDetailParser
from .event_detail_repository import EventDetailRepository, EventDetailWriteResult
from .parsers.base import RawSnapshot
from .parsers.registry import ParserRegistry
from .storage.normalize_repository import NormalizeRepository


# Stage 4.4 (2026-05-21): which snapshots from the event-detail bundle
# need a second pass through ``ParserRegistry`` so the normalized rows
# land in ``event_incident`` / ``event_statistic``.
#
# Why these two patterns: Stage 4.2 added their fetch to
# ``EventDetailParser.fetch_bundle`` but stopped at writing the raw
# snapshot. ``EventDetailRepository.upsert_bundle`` has no
# ``_upsert_event_incidents`` / ``_upsert_event_statistics`` — it only
# walks the typed accumulator (lineups, managers, markets, …). Without
# this hook, historical-backfill leaves both tables empty for archive
# matches even though the raw payload sits in ``api_payload_snapshot``.
#
# Other event-detail snapshots (lineups, managers, h2h, …) already
# have dedicated upsert methods on the repository — putting them
# through the parser registry too would cause duplicate DELETE+INSERT
# work in the same transaction.
_MATCH_CENTER_NORMALIZE_PATTERNS = frozenset(
    {EVENT_INCIDENTS_ENDPOINT.pattern, EVENT_STATISTICS_ENDPOINT.pattern}
)


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
        normalize_repository: NormalizeRepository | None = None,
        parser_registry: ParserRegistry | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.parser = parser
        self.repository = repository
        self.database = database
        # Stage 4.4 (2026-05-21): optional ParserRegistry +
        # NormalizeRepository pair. When both are present the job will
        # push /incidents and /statistics snapshots through the
        # registry inside the same transaction as ``upsert_bundle`` so
        # ``event_incident`` / ``event_statistic`` land for archive
        # matches. Default ``None`` keeps every legacy caller binary
        # compatible — they continue to write snapshots without
        # normalising the match-center tables, which matches the
        # pre-Stage 4.4 behaviour.
        self.normalize_repository = normalize_repository
        self.parser_registry = parser_registry
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
            # Stage 4.4: after the repository has anchored the parent
            # event / team / player rows, replay the captured
            # /incidents and /statistics snapshots through the parser
            # registry so event_incident / event_statistic populate
            # inside the same transaction. The Stage 4.2 commit body
            # promised this wire ("normalisation is left to the
            # standard ParserRegistry") but it was never plugged in,
            # leaving both tables empty for every match exercised by
            # the historical-backfill CLI path.
            if self.normalize_repository is not None:
                await self._normalize_match_center_snapshots(connection, bundle)
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


    async def _normalize_match_center_snapshots(
        self,
        connection,
        bundle: EventDetailBundle,
    ) -> None:
        """Stage 4.4 (2026-05-21): forward /incidents + /statistics
        snapshots to ``NormalizeRepository`` via the standard parser
        registry. Runs inside the open transaction so any failure rolls
        back together with ``upsert_bundle``."""

        registry = self.parser_registry or ParserRegistry.default()
        for snapshot_record in bundle.payload_snapshots:
            if snapshot_record.endpoint_pattern not in _MATCH_CENTER_NORMALIZE_PATTERNS:
                continue
            raw_snapshot = RawSnapshot(
                snapshot_id=None,
                endpoint_pattern=snapshot_record.endpoint_pattern,
                sport_slug=None,
                source_url=snapshot_record.source_url,
                resolved_url=snapshot_record.source_url,
                envelope_key=snapshot_record.envelope_key,
                http_status=None,
                payload=snapshot_record.payload,
                fetched_at=snapshot_record.fetched_at,
                context_entity_type=snapshot_record.context_entity_type,
                context_entity_id=snapshot_record.context_entity_id,
                context_event_id=(
                    snapshot_record.context_entity_id
                    if snapshot_record.context_entity_type == "event"
                    else None
                ),
            )
            parse_result = registry.parse(raw_snapshot)
            await self.normalize_repository.persist_parse_result(
                connection, parse_result, skip_entity_upserts=False
            )


def _is_undefined_table_error(exc: Exception) -> bool:
    return exc.__class__.__name__ == "UndefinedTableError"
