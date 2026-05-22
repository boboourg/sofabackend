"""Historical tournament and enrichment workers."""

from __future__ import annotations

import time

from ..jobs.types import (
    JOB_ENRICH_TOURNAMENT_ARCHIVE,
    JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH,
    JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH,
    JOB_SYNC_TOURNAMENT_ARCHIVE,
)
from ..queue.streams import (
    GROUP_HISTORICAL_ENRICHMENT,
    GROUP_HISTORICAL_TOURNAMENT,
    STREAM_HISTORICAL_ENRICHMENT,
    STREAM_HISTORICAL_TOURNAMENT,
    StreamEntry,
)
from ..services.season_capabilities import (
    EVENTS,
    required_capabilities_for_cursor_advance,
)
from ..services.worker_runtime import WorkerRuntime
from ._stream_jobs import decode_stream_job, encode_stream_job


def _coerce_optional_int(value: object) -> int | None:
    """Read int from job.params dict cleanly. Job params come from a JSON
    blob — values may already be int, or could be string like ``"52186"``,
    or absent entirely. Returns None for any unparseable input."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_bootstrap_mode_from_catalog_state(state: str | None) -> bool:
    """Phase 3 (2026-05-22): two-phase archive backfill dispatch.

    ``pending``         → bootstrap mode (lightweight event-list-only walk;
                           after success the state transitions to
                           ``events_loaded`` and the next cursor pass
                           picks the season up in full mode).
    ``events_loaded``   → full archive mode (match-center + entities).
    ``fully_processed`` → full mode (defensive — already-processed
                           seasons that get re-published should NOT
                           regress to bootstrap; the re-pass is
                           idempotent in full mode).
    ``None`` / missing  → full mode (legacy seasons that pre-date the
                           catalog table; default to non-bootstrap
                           behaviour so nothing is silently downgraded).
    """

    normalized = (state or "").strip().lower()
    return normalized == "pending"


async def _fetch_catalog_state(
    orchestrator, *, unique_tournament_id: int, season_id: int | None
) -> str | None:
    """Best-effort lookup of catalog bootstrap_state for (UT, season).
    Returns None for missing rows / DB errors so the worker falls back
    to the safe full-archive path (rather than bootstrap-mode-by-mistake
    on a legacy season)."""

    if season_id is None:
        return None
    database = getattr(orchestrator, "database", None)
    if database is None:
        return None
    try:
        async with database.connection() as connection:
            row = await connection.fetchrow(
                """
                SELECT bootstrap_state
                FROM tournament_season_upstream_catalog
                WHERE unique_tournament_id = $1 AND season_id = $2
                """,
                int(unique_tournament_id),
                int(season_id),
            )
            if row is None:
                return None
            return str(row["bootstrap_state"]) if row["bootstrap_state"] else None
    except Exception:  # noqa: BLE001 — defensive: any DB hiccup → full mode
        return None


class HistoricalTournamentWorker:
    def __init__(
        self,
        *,
        orchestrator,
        queue,
        consumer: str,
        group: str = GROUP_HISTORICAL_TOURNAMENT,
        stream: str = STREAM_HISTORICAL_TOURNAMENT,
        enrichment_stream: str = STREAM_HISTORICAL_ENRICHMENT,
        block_ms: int = 5_000,
        delayed_scheduler=None,
        delayed_payload_store=None,
        completion_store=None,
        now_ms_factory=None,
        job_audit_logger=None,
    ) -> None:
        self.orchestrator = orchestrator
        self.queue = queue
        self.consumer = consumer
        self.group = group
        self.stream = stream
        self.enrichment_stream = enrichment_stream
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.runtime = WorkerRuntime(
            name="historical-tournament-worker",
            queue=queue,
            stream=stream,
            group=group,
            consumer=consumer,
            handler=self.handle,
            retry_handler=self.retry_later if delayed_scheduler is not None else None,
            completion_store=completion_store,
            block_ms=block_ms,
            now_ms_factory=self.now_ms_factory,
            job_audit_logger=job_audit_logger,
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        if job.job_type != JOB_SYNC_TOURNAMENT_ARCHIVE:
            return "ignored"
        if job.entity_id is None:
            raise RuntimeError("Historical tournament worker requires unique_tournament entity_id.")
        sport_slug = str(job.sport_slug or "").strip().lower()
        ut_id = int(job.entity_id)
        # Phase 1 cursor mode (2026-05-16): when the planner publishes
        # a per-(UT, season) job it includes the target season in params.
        # Legacy "process every season" jobs leave the param absent and
        # we fall back to the historical multi-season walk.
        target_season_id = _coerce_optional_int(job.params.get("target_season_id"))
        # Phase 3 (2026-05-22): two-phase dispatch.
        # Lookup catalog row for (UT, season) — if 'pending' → bootstrap.
        # Bootstrap path skips macro stages + event-detail + entities;
        # only fetches event-list. After success the
        # ``_run_tournament_worker`` events_loaded transition (Phase 2.A.4)
        # flips the catalog state, so the next cursor walk picks the
        # season up in full mode automatically. Missing row / DB error
        # → defensive full mode (no silent downgrade).
        catalog_state = await _fetch_catalog_state(
            self.orchestrator,
            unique_tournament_id=ut_id,
            season_id=target_season_id,
        )
        bootstrap_mode = _resolve_bootstrap_mode_from_catalog_state(catalog_state)
        result = await self.orchestrator.run_historical_tournament_archive(
            unique_tournament_id=ut_id,
            sport_slug=sport_slug,
            target_season_id=target_season_id,
            bootstrap_mode=bootstrap_mode,
        )
        season_ids = tuple(int(item) for item in result.get("season_ids", ()) or ())

        # Advance the registry cursor when the orchestrator reports that
        # all *required* season-level capabilities completed.
        #
        # Evolution of this gate:
        #   1) 2026-05-16: ``stage_failures == 0``. Broke for cup-style
        #      competitions (FIFA WC, EURO) that 404 on /standings/home and
        #      similar optional endpoints — UT=16 season=41087 stayed
        #      stuck for 3h+ with 64/64 events already collected.
        #   2) 2026-05-18 (hack-fix): ``discovered_event_ids > 0``. Fixed
        #      the symptom but didn't express intent. Stage failures were
        #      silently ignored.
        #   3) 2026-05-19 (this code): capability dependency graph. The
        #      orchestrator reports ``capabilities_completed`` (the set of
        #      season-level capabilities it produced — events, rounds,
        #      standings, leaderboards, etc.). The worker advances when
        #      the *required* subset (defined in
        #      ``schema_inspector.services.season_capabilities``) is a
        #      subset of the *completed* set. Adding a new required
        #      capability later is a 1-line change in that module.
        #
        # Backward compatibility: when ``capabilities_completed`` is
        # entirely absent from ``result`` (legacy orchestrators or test
        # fakes still being updated), we fall back to the
        # ``discovered_event_ids > 0`` proxy so we don't regress the
        # 2026-05-18 fix while the new gate rolls out. An empty
        # ``capabilities_completed`` (key present, value ``()``) is a
        # deliberate "nothing useful completed" signal — NO fallback.
        should_advance = False
        if "capabilities_completed" in result:
            completed = frozenset(
                str(item) for item in (result.get("capabilities_completed") or ())
            )
            required = required_capabilities_for_cursor_advance()
            should_advance = required.issubset(completed)
        else:
            # Legacy path — orchestrator pre-dates the capability model.
            discovered_event_ids = int(result.get("discovered_event_ids", 0) or 0)
            should_advance = discovered_event_ids > 0

        if (
            target_season_id is not None
            and season_ids
            and should_advance
        ):
            advance = getattr(self.orchestrator, "advance_backfill_cursor", None)
            if callable(advance):
                try:
                    await advance(
                        sport_slug=sport_slug,
                        unique_tournament_id=ut_id,
                        completed_season_id=int(target_season_id),
                    )
                except Exception:  # noqa: BLE001 — never fail the job on cursor accounting
                    import logging
                    logging.getLogger(__name__).exception(
                        "advance_backfill_cursor failed: ut=%s season=%s",
                        ut_id,
                        target_season_id,
                    )

        for child_job_type in (
            JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH,
            JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH,
        ):
            enrich_job = job.spawn_child(
                job_type=child_job_type,
                entity_type="unique_tournament",
                entity_id=ut_id,
                scope="historical",
                params={"season_ids": list(season_ids)},
                priority=job.priority,
            )
            self.queue.publish(self.enrichment_stream, encode_stream_job(enrich_job))
        return "completed"

    async def retry_later(self, entry: StreamEntry, exc: Exception, *, delay_ms: int) -> str:
        del exc
        job = decode_stream_job(entry)
        if self.delayed_payload_store is not None:
            self.delayed_payload_store.save_entry(entry)
        self.delayed_scheduler.schedule(job.job_id, run_at_epoch_ms=int(self.now_ms_factory()) + int(delay_ms))
        return "requeued"

    async def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        await self.runtime.run_forever(install_signal_handlers=install_signal_handlers)


class HistoricalEnrichmentWorker:
    def __init__(
        self,
        *,
        orchestrator,
        queue,
        consumer: str,
        group: str = GROUP_HISTORICAL_ENRICHMENT,
        stream: str = STREAM_HISTORICAL_ENRICHMENT,
        block_ms: int = 5_000,
        delayed_scheduler=None,
        delayed_payload_store=None,
        completion_store=None,
        now_ms_factory=None,
        job_audit_logger=None,
    ) -> None:
        self.orchestrator = orchestrator
        self.queue = queue
        self.consumer = consumer
        self.group = group
        self.stream = stream
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.runtime = WorkerRuntime(
            name="historical-enrichment-worker",
            queue=queue,
            stream=stream,
            group=group,
            consumer=consumer,
            handler=self.handle,
            retry_handler=self.retry_later if delayed_scheduler is not None else None,
            completion_store=completion_store,
            block_ms=block_ms,
            now_ms_factory=self.now_ms_factory,
            job_audit_logger=job_audit_logger,
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        if job.job_type not in {
            JOB_ENRICH_TOURNAMENT_ARCHIVE,
            JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH,
            JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH,
        }:
            return "ignored"
        if job.entity_id is None:
            raise RuntimeError("Historical enrichment worker requires unique_tournament entity_id.")
        sport_slug = str(job.sport_slug or "").strip().lower()
        season_ids = tuple(int(item) for item in job.params.get("season_ids", ()) or ())
        if job.job_type == JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH:
            await self.orchestrator.run_historical_tournament_event_detail_batch(
                unique_tournament_id=int(job.entity_id),
                sport_slug=sport_slug,
                season_ids=season_ids,
            )
            return "completed"
        if job.job_type == JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH:
            await self.orchestrator.run_historical_tournament_entities_batch(
                unique_tournament_id=int(job.entity_id),
                sport_slug=sport_slug,
                season_ids=season_ids,
            )
            return "completed"
        for child_job_type in (
            JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH,
            JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH,
        ):
            child_job = job.spawn_child(
                job_type=child_job_type,
                entity_type="unique_tournament",
                entity_id=int(job.entity_id),
                scope="historical",
                params={"season_ids": list(season_ids)},
                priority=job.priority,
            )
            self.queue.publish(self.stream, encode_stream_job(child_job))
        return "completed"

    async def retry_later(self, entry: StreamEntry, exc: Exception, *, delay_ms: int) -> str:
        del exc
        job = decode_stream_job(entry)
        if self.delayed_payload_store is not None:
            self.delayed_payload_store.save_entry(entry)
        self.delayed_scheduler.schedule(job.job_id, run_at_epoch_ms=int(self.now_ms_factory()) + int(delay_ms))
        return "requeued"

    async def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        await self.runtime.run_forever(install_signal_handlers=install_signal_handlers)
