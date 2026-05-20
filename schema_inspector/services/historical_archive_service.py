"""Helpers for historical tournament/archive workers."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

from ..default_tournaments_pipeline_cli import _run_tournament_worker
from ..entities_backfill_job import EntitiesBackfillJob
from ..event_detail_backfill_job import EventDetailBackfillJob
from ..sources import build_source_adapter
from .historical_planner import (
    choose_event_detail_budget,
    choose_recent_history_window,
    choose_saturation_budget,
)
from ..sport_profiles import resolve_sport_profile
from ..statistics_parser import StatisticsQuery

async def run_historical_tournament_archive(
    app,
    *,
    unique_tournament_id: int,
    sport_slug: str,
    seasons_per_tournament: int = 0,
    event_concurrency: int = 4,
    timeout: float = 20.0,
    target_season_id: int | None = None,
    # Stage 4.1 (2026-05-20 match-center fix): expose the previously
    # hardcoded skip-flags so the operator-driven CLI handler can opt
    # INTO per-event fan-out without having to fork the helper.
    # Defaults preserve the worker-historical-tournament flow:
    # that path keeps skip=True here and runs the per-event work
    # afterwards via enrichment child-jobs published to
    # STREAM_HISTORICAL_ENRICHMENT (historical_archive_worker.py:160-172).
    # The CLI handler (cli.py historical-backfill) bypasses the worker
    # path entirely, so it must pass skip_event_detail=False and
    # skip_entities=False explicitly to get lineups/incidents/
    # statistics/player-stats for archive matches.
    skip_event_detail: bool = True,
    skip_entities: bool = True,
) -> dict[str, object]:
    adapter = build_source_adapter(
        app.runtime_config.source_slug,
        runtime_config=app.runtime_config,
        transport=app.transport,
    )
    sport_profile = resolve_sport_profile(sport_slug)
    competition_job = adapter.build_competition_job(app.database)
    event_list_job = adapter.build_event_list_job(app.database)
    statistics_job = adapter.build_statistics_job(app.database)
    standings_job = adapter.build_standings_job(app.database)
    leaderboards_job = adapter.build_leaderboards_job(app.database)
    event_detail_job = adapter.build_event_detail_job(app.database)
    entities_job = adapter.build_entities_job(app.database)
    result = await _run_tournament_worker(
        app.database,
        competition_job=competition_job,
        event_list_job=event_list_job,
        statistics_job=statistics_job,
        standings_job=standings_job,
        leaderboards_job=leaderboards_job,
        event_detail_job=event_detail_job,
        entities_job=entities_job,
        sport_slug=sport_slug,
        sport_profile=sport_profile,
        unique_tournament_id=int(unique_tournament_id),
        standings_scopes=tuple(sport_profile.standings_scopes),
        provider_ids=(1,),
        stats_query=StatisticsQuery(
            limit=20,
            offset=0,
            order="-rating",
            accumulation="total",
            group="summary",
            fields=(),
            filters=(),
        ),
        seasons_per_tournament=seasons_per_tournament,
        event_concurrency=max(1, int(event_concurrency)),
        skip_featured_events=False,
        skip_round_events=False,
        skip_event_detail=skip_event_detail,
        skip_entities=skip_entities,
        skip_statistics=False,
        skip_standings=False,
        skip_leaderboards=False,
        timeout=timeout,
        target_season_id=target_season_id,
    )
    # 2026-05-19 (fix 1): pass through the capability set so the
    # ``HistoricalTournamentWorker`` advance gate can use the capability
    # subset check. ``getattr`` with default protects against legacy
    # SimpleNamespace stubs in tests that don't set the field — the
    # worker treats absent ``capabilities_completed`` as the
    # backward-compat fallback to ``discovered_event_ids > 0``.
    capabilities = getattr(result, "capabilities_completed", frozenset())
    return {
        "season_ids": tuple(int(item) for item in result.season_ids),
        "completed_seasons": int(result.completed_seasons),
        "discovered_event_ids": int(result.discovered_event_ids),
        "stage_failures": int(result.stage_failures),
        "success": bool(result.success),
        "capabilities_completed": tuple(sorted(capabilities)),
    }


async def run_historical_tournament_enrichment(
    app,
    *,
    unique_tournament_id: int,
    sport_slug: str,
    season_ids: tuple[int, ...] = (),
    event_detail_concurrency: int = 6,
    timeout: float = 20.0,
    now_factory=None,
) -> dict[str, object]:
    adapter = _build_historical_enrichment_adapter(app)
    inputs = _resolve_historical_enrichment_inputs(sport_slug, now_factory=now_factory)
    event_detail_payload = await _run_historical_tournament_event_detail_batch(
        app,
        adapter=adapter,
        unique_tournament_id=unique_tournament_id,
        sport_slug=sport_slug,
        season_ids=season_ids,
        recent_window_start=inputs["recent_window_start"],
        recent_window_end=inputs["recent_window_end"],
        event_detail_limit=inputs["event_detail_limit"],
        event_detail_concurrency=max(1, int(event_detail_concurrency)),
        timeout=timeout,
    )
    entities_payload = await _run_historical_tournament_entities_batch(
        app,
        adapter=adapter,
        unique_tournament_id=unique_tournament_id,
        sport_slug=sport_slug,
        season_ids=season_ids,
        recent_window_start=inputs["recent_window_start"],
        recent_window_end=inputs["recent_window_end"],
        saturation_budget=inputs["saturation_budget"],
        timeout=timeout,
    )
    return {
        **event_detail_payload,
        **entities_payload,
    }


async def run_historical_tournament_event_detail_batch(
    app,
    *,
    unique_tournament_id: int,
    sport_slug: str,
    season_ids: tuple[int, ...] = (),
    event_detail_concurrency: int = 6,
    timeout: float = 20.0,
    now_factory=None,
) -> dict[str, object]:
    adapter = _build_historical_enrichment_adapter(app)
    inputs = _resolve_historical_enrichment_inputs(sport_slug, now_factory=now_factory)
    return await _run_historical_tournament_event_detail_batch(
        app,
        adapter=adapter,
        unique_tournament_id=unique_tournament_id,
        sport_slug=sport_slug,
        season_ids=season_ids,
        recent_window_start=inputs["recent_window_start"],
        recent_window_end=inputs["recent_window_end"],
        event_detail_limit=inputs["event_detail_limit"],
        event_detail_concurrency=max(1, int(event_detail_concurrency)),
        timeout=timeout,
    )


async def run_historical_tournament_entities_batch(
    app,
    *,
    unique_tournament_id: int,
    sport_slug: str,
    season_ids: tuple[int, ...] = (),
    timeout: float = 20.0,
    now_factory=None,
) -> dict[str, object]:
    adapter = _build_historical_enrichment_adapter(app)
    inputs = _resolve_historical_enrichment_inputs(sport_slug, now_factory=now_factory)
    return await _run_historical_tournament_entities_batch(
        app,
        adapter=adapter,
        unique_tournament_id=unique_tournament_id,
        sport_slug=sport_slug,
        season_ids=season_ids,
        recent_window_start=inputs["recent_window_start"],
        recent_window_end=inputs["recent_window_end"],
        saturation_budget=inputs["saturation_budget"],
        timeout=timeout,
    )


def _build_historical_enrichment_adapter(app):
    adapter = getattr(app, "_historical_enrichment_adapter", None)
    if adapter is None:
        adapter = build_source_adapter(
            app.runtime_config.source_slug,
            runtime_config=app.runtime_config,
            transport=app.transport,
        )
        setattr(app, "_historical_enrichment_adapter", adapter)
    return adapter


def _get_historical_enrichment_event_detail_job(app, adapter):
    event_detail_job = getattr(app, "_historical_enrichment_event_detail_job", None)
    if event_detail_job is None:
        event_detail_job = adapter.build_event_detail_job(app.database)
        setattr(app, "_historical_enrichment_event_detail_job", event_detail_job)
    return event_detail_job


def _get_historical_enrichment_entities_job(app, adapter):
    entities_job = getattr(app, "_historical_enrichment_entities_job", None)
    if entities_job is None:
        entities_job = adapter.build_entities_job(app.database)
        setattr(app, "_historical_enrichment_entities_job", entities_job)
    return entities_job


def _resolve_historical_enrichment_inputs(sport_slug: str, *, now_factory=None) -> dict[str, object]:
    resolved_now = (now_factory or _default_now_utc)()
    recent_window_days = choose_recent_history_window(sport_slug)
    return {
        "recent_window_start": int((resolved_now - timedelta(days=recent_window_days)).timestamp()),
        "recent_window_end": int(resolved_now.timestamp()),
        "saturation_budget": choose_saturation_budget(sport_slug),
        "event_detail_limit": choose_event_detail_budget(sport_slug),
    }


async def _run_historical_tournament_event_detail_batch(
    app,
    *,
    adapter,
    unique_tournament_id: int,
    sport_slug: str,
    season_ids: tuple[int, ...],
    recent_window_start: int,
    recent_window_end: int,
    event_detail_limit: int,
    event_detail_concurrency: int,
    timeout: float,
) -> dict[str, object]:
    event_detail_backfill_job = EventDetailBackfillJob(
        _get_historical_enrichment_event_detail_job(app, adapter),
        app.database,
    )
    async with _stage_scope(
        app,
        stage_name="historical.enrichment.event_detail",
        meta={
            "unique_tournament_id": int(unique_tournament_id),
            "sport_slug": sport_slug,
            "season_ids": [int(item) for item in season_ids],
            "window_start": recent_window_start,
            "window_end": recent_window_end,
            "event_detail_limit": event_detail_limit,
            "event_detail_concurrency": max(1, int(event_detail_concurrency)),
        },
    ):
        event_detail_result = await event_detail_backfill_job.run(
            limit=event_detail_limit,
            only_missing=True,
            unique_tournament_ids=(int(unique_tournament_id),),
            start_timestamp_from=recent_window_start,
            start_timestamp_to=recent_window_end,
            concurrency=max(1, int(event_detail_concurrency)),
            timeout=timeout,
        )
    return {
        "event_detail_candidates": int(event_detail_result.total_candidates),
        "event_detail_succeeded": int(event_detail_result.succeeded),
        "event_detail_failed": int(event_detail_result.failed),
    }


async def _run_historical_tournament_entities_batch(
    app,
    *,
    adapter,
    unique_tournament_id: int,
    sport_slug: str,
    season_ids: tuple[int, ...],
    recent_window_start: int,
    recent_window_end: int,
    saturation_budget,
    timeout: float,
) -> dict[str, object]:
    entities_backfill_job = EntitiesBackfillJob(
        _get_historical_enrichment_entities_job(app, adapter),
        app.database,
    )
    async with _stage_scope(
        app,
        stage_name="historical.enrichment.entities",
        meta={
            "unique_tournament_id": int(unique_tournament_id),
            "sport_slug": sport_slug,
            "season_ids": [int(item) for item in season_ids],
            "window_start": recent_window_start,
            "window_end": recent_window_end,
            "player_limit": saturation_budget.player_limit,
            "team_limit": saturation_budget.team_limit,
            "player_request_limit": saturation_budget.player_request_limit,
            "team_request_limit": saturation_budget.team_request_limit,
        },
    ):
        entities_result = await entities_backfill_job.run(
            only_missing=True,
            player_limit=saturation_budget.player_limit,
            team_limit=saturation_budget.team_limit,
            player_request_limit=saturation_budget.player_request_limit,
            team_request_limit=saturation_budget.team_request_limit,
            unique_tournament_ids=(int(unique_tournament_id),),
            event_timestamp_from=recent_window_start,
            event_timestamp_to=recent_window_end,
            timeout=timeout,
        )
    return {
        "entity_players": len(entities_result.player_ids),
        "entity_teams": len(entities_result.team_ids),
        "entity_snapshots": int(entities_result.ingest.written.payload_snapshot_rows),
    }


def _default_now_utc() -> datetime:
    return datetime.now(timezone.utc)


@asynccontextmanager
async def _stage_scope(app, **kwargs):
    stage_audit_logger = getattr(app, "stage_audit_logger", None)
    if stage_audit_logger is None:
        yield
        return
    async with stage_audit_logger.stage(**kwargs):
        yield
