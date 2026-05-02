"""Structural sync service — skeleton-only tournament/season/event scaffolding.

This service owns the *shape* of the competition graph: sport, unique_tournament,
season, and event skeleton (id, teams, round/date/status, slug). It deliberately
does NOT run statistics, standings, leaderboards, entity-detail, or any hydrate
surface — those live in live/historical/hydrate workers.

The service supports two strategies, chosen by sport profile and capability:

- ``rounds``   — probe round_number=1..N via the round-events endpoint; halts
                 when a round returns empty. Good for league formats (EPL, NBA
                 regular season, rugby, futsal).
- ``calendar`` — query featured-events + per-date unique-tournament scheduled
                 events over the next N days. Good for tennis / esports /
                 table-tennis / playoffs where rounds are absent.

When the profile is ``auto``, the service inspects ``UniqueTournamentRecord.
has_rounds`` from a freshly-fetched CompetitionBundle to pick a path. If the
field is missing, it falls back to calendar mode (safer — produces at least
some skeleton no matter the structure).
"""

from __future__ import annotations

import logging
import inspect
import time
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any, Mapping

from ..competition_parser import UniqueTournamentRecord
from ..event_list_job import EventListIngestJob
from ..sources import build_source_adapter
from ..sport_profiles import SportProfile, resolve_sport_profile

logger = logging.getLogger(__name__)

_MAX_CONSECUTIVE_MISSING_ROUNDS = 2
_MAX_CONSECUTIVE_EMPTY_CALENDAR_DAYS = 3
_MAX_SEASON_EVENT_PAGES = 50
_SEASON_EVENT_SURFACE_REFRESH_TTL_SECONDS = 24 * 3600


@dataclass(frozen=True)
class StructureSyncResult:
    unique_tournament_id: int
    sport_slug: str
    mode: str
    season_ids: tuple[int, ...] = ()
    rounds_probed: int = 0
    rounds_with_events: int = 0
    calendar_dates_probed: int = 0
    event_ids: tuple[int, ...] = field(default_factory=tuple)
    success: bool = True
    reason: str | None = None


@dataclass(frozen=True)
class _SeasonSurfaceSyncResult:
    event_ids: tuple[int, ...]
    complete: bool


async def run_structure_sync_for_tournament(
    app,
    *,
    unique_tournament_id: int,
    sport_slug: str,
    runtime_config=None,
    transport=None,
    now_factory=None,
    max_rounds_probe: int = 60,
    timeout: float = 20.0,
) -> StructureSyncResult:
    """Run one skeleton-only structural sync pass for a single unique tournament.

    ``runtime_config`` and ``transport`` override the orchestrator's default
    residential pool — they are REQUIRED when this is called from the structure
    worker (the worker builds them from the non-residential pool). Passing None
    is allowed only for unit tests that stub the parsers.
    """

    normalized_sport = str(sport_slug or "").strip().lower() or "football"
    profile = resolve_sport_profile(normalized_sport)
    if profile.structure_sync_mode == "disabled":
        return StructureSyncResult(
            unique_tournament_id=int(unique_tournament_id),
            sport_slug=normalized_sport,
            mode="disabled",
            success=True,
            reason="structure_sync_mode=disabled for this sport",
        )

    client_runtime = runtime_config if runtime_config is not None else app.runtime_config
    client_transport = transport if transport is not None else getattr(app, "transport", None)
    adapter = build_source_adapter(
        client_runtime.source_slug,
        runtime_config=client_runtime,
        transport=client_transport,
    )
    competition_job = adapter.build_competition_job(app.database)
    event_list_job = adapter.build_event_list_job(app.database)

    # Phase 1 — pull tournament + seasons caps (cheap, 2 endpoints).
    competition_result = await competition_job.run(
        unique_tournament_id=int(unique_tournament_id),
        include_seasons=True,
        timeout=timeout,
    )
    tournament_record = _pick_tournament(competition_result.parsed.unique_tournaments, unique_tournament_id)
    season_ids = tuple(sorted({int(s.id) for s in competition_result.parsed.seasons}, reverse=True))

    if not season_ids:
        return StructureSyncResult(
            unique_tournament_id=int(unique_tournament_id),
            sport_slug=normalized_sport,
            mode=_resolve_mode(profile, tournament_record),
            success=True,
            reason="no seasons reported by Sofascore — skeleton not produced",
        )

    # Phase 2 — pull season_info for the current (largest-id) season so we
    # normalise season metadata before we touch event-list surfaces.
    current_season_id = int(season_ids[0])
    await competition_job.run(
        unique_tournament_id=int(unique_tournament_id),
        season_id=current_season_id,
        include_seasons=False,
        timeout=timeout,
    )

    mode = _resolve_mode(profile, tournament_record)

    if mode == "rounds":
        result = await _run_rounds_mode(
            unique_tournament_id=int(unique_tournament_id),
            sport_slug=normalized_sport,
            profile=profile,
            tournament=tournament_record,
            season_ids=season_ids,
            current_season_id=current_season_id,
            event_list_job=event_list_job,
            now_factory=now_factory,
            max_rounds_probe=max_rounds_probe,
            timeout=timeout,
        )
    elif mode == "brackets":
        result = await _run_brackets_mode(
            unique_tournament_id=int(unique_tournament_id),
            sport_slug=normalized_sport,
            profile=profile,
            season_ids=season_ids,
            current_season_id=current_season_id,
            event_list_job=event_list_job,
            now_factory=now_factory,
            timeout=timeout,
        )
    else:
        # calendar mode (either explicit or auto fallback)
        result = await _run_calendar_mode(
            unique_tournament_id=int(unique_tournament_id),
            sport_slug=normalized_sport,
            profile=profile,
            season_ids=season_ids,
            event_list_job=event_list_job,
            now_factory=now_factory,
            timeout=timeout,
        )

    return await _append_current_season_event_surfaces(
        result,
        app=app,
        unique_tournament_id=int(unique_tournament_id),
        sport_slug=normalized_sport,
        current_season_id=current_season_id,
        event_list_job=event_list_job,
        timeout=timeout,
    )


def _pick_tournament(
    records: tuple[UniqueTournamentRecord, ...],
    unique_tournament_id: int,
) -> UniqueTournamentRecord | None:
    for record in records:
        if int(record.id) == int(unique_tournament_id):
            return record
    return records[0] if records else None


def _resolve_mode(profile: SportProfile, tournament: UniqueTournamentRecord | None) -> str:
    mode = profile.structure_sync_mode
    if mode in ("rounds", "brackets", "calendar"):
        return mode
    if mode == "disabled":
        return "disabled"
    # auto: decide per-tournament based on has_rounds
    has_rounds = None if tournament is None else tournament.has_rounds
    if has_rounds is True:
        return "rounds"
    has_playoff_series = None if tournament is None else tournament.has_playoff_series
    if has_playoff_series is True:
        return "brackets"
    # Unknown / missing — fall back to calendar (always produces *some* skeleton).
    return "calendar"


def _http_status_code(exc: Exception) -> int | None:
    status = getattr(exc, "status_code", None)
    if isinstance(status, int):
        return status
    transport_result = getattr(exc, "transport_result", None)
    status = getattr(transport_result, "status_code", None)
    if isinstance(status, int):
        return status
    return None


async def _run_rounds_mode(
    *,
    unique_tournament_id: int,
    sport_slug: str,
    profile: SportProfile,
    tournament: UniqueTournamentRecord | None,
    season_ids: tuple[int, ...],
    current_season_id: int,
    event_list_job: EventListIngestJob,
    now_factory,
    max_rounds_probe: int,
    timeout: float,
) -> StructureSyncResult:
    rounds_probed = 0
    rounds_with_events = 0
    collected_event_ids: list[int] = []
    missing_round_streak = 0

    for round_number in range(1, max_rounds_probe + 1):
        rounds_probed += 1
        try:
            result = await event_list_job.run_round(
                unique_tournament_id=unique_tournament_id,
                season_id=current_season_id,
                round_number=round_number,
                sport_slug=sport_slug,
                timeout=timeout,
            )
        except Exception as exc:
            logger.warning(
                "structure-sync rounds probe failed: tournament=%s season=%s round=%s: %s",
                unique_tournament_id,
                current_season_id,
                round_number,
                exc,
            )
            # Don't escalate per-round failures — structure sync is best-effort.
            # If every round fails and we accumulated nothing, we'll downgrade.
            if _http_status_code(exc) == 404:
                missing_round_streak += 1
                if rounds_with_events > 0 and missing_round_streak >= _MAX_CONSECUTIVE_MISSING_ROUNDS:
                    break
                if rounds_with_events == 0 and missing_round_streak >= _MAX_CONSECUTIVE_MISSING_ROUNDS:
                    break
            else:
                missing_round_streak = 0
            continue
        event_count = len(result.parsed.events)
        if event_count == 0:
            missing_round_streak = 0
            # First empty round after we had at least one populated round — stop.
            if rounds_with_events > 0:
                break
            # Still zero-zero: give it 2 more chances, then give up on rounds mode.
            if round_number >= 3:
                break
            continue
        missing_round_streak = 0
        rounds_with_events += 1
        collected_event_ids.extend(int(event.id) for event in result.parsed.events)

    if rounds_with_events == 0 and profile.structure_rounds_fallback_calendar:
        if _has_bracket_capability(tournament):
            logger.info(
                "structure-sync rounds mode empty for tournament=%s; trying brackets before calendar",
                unique_tournament_id,
            )
            fallback = await _run_brackets_mode(
                unique_tournament_id=unique_tournament_id,
                sport_slug=sport_slug,
                profile=profile,
                season_ids=season_ids,
                current_season_id=current_season_id,
                event_list_job=event_list_job,
                now_factory=now_factory,
                timeout=timeout,
            )
            if fallback.mode == "brackets":
                return StructureSyncResult(
                    unique_tournament_id=unique_tournament_id,
                    sport_slug=sport_slug,
                    mode="rounds->brackets",
                    season_ids=season_ids,
                    rounds_probed=rounds_probed,
                    rounds_with_events=0,
                    event_ids=fallback.event_ids,
                    success=fallback.success,
                    reason="rounds empty; fell back to brackets",
                )
            if fallback.mode == "brackets->calendar":
                return StructureSyncResult(
                    unique_tournament_id=unique_tournament_id,
                    sport_slug=sport_slug,
                    mode="rounds->brackets->calendar",
                    season_ids=season_ids,
                    rounds_probed=rounds_probed,
                    rounds_with_events=0,
                    calendar_dates_probed=fallback.calendar_dates_probed,
                    event_ids=fallback.event_ids,
                    success=fallback.success,
                    reason="rounds empty; fell back to brackets then calendar",
                )
        logger.info(
            "structure-sync rounds mode empty for tournament=%s; falling back to calendar",
            unique_tournament_id,
        )
        fallback = await _run_calendar_mode(
            unique_tournament_id=unique_tournament_id,
            sport_slug=sport_slug,
            profile=profile,
            season_ids=season_ids,
            event_list_job=event_list_job,
            now_factory=now_factory,
            timeout=timeout,
        )
        return StructureSyncResult(
            unique_tournament_id=unique_tournament_id,
            sport_slug=sport_slug,
            mode="rounds->calendar",
            season_ids=season_ids,
            rounds_probed=rounds_probed,
            rounds_with_events=0,
            calendar_dates_probed=fallback.calendar_dates_probed,
            event_ids=fallback.event_ids,
            success=fallback.success,
            reason="rounds empty; fell back to calendar",
        )

    return StructureSyncResult(
        unique_tournament_id=unique_tournament_id,
        sport_slug=sport_slug,
        mode="rounds",
        season_ids=season_ids,
        rounds_probed=rounds_probed,
        rounds_with_events=rounds_with_events,
        event_ids=tuple(collected_event_ids),
        success=True,
    )


async def _run_calendar_mode(
    *,
    unique_tournament_id: int,
    sport_slug: str,
    profile: SportProfile,
    season_ids: tuple[int, ...],
    event_list_job: EventListIngestJob,
    now_factory,
    timeout: float,
) -> StructureSyncResult:
    forward_months = max(1, int(profile.structure_calendar_forward_months))
    resolved_now = (now_factory or _default_now_utc)()
    start_date = resolved_now.date()
    total_days = forward_months * 30

    collected_event_ids: list[int] = []
    dates_probed = 0

    # Featured-events first — usually gives a good anchor of upcoming / live.
    try:
        featured = await event_list_job.run_featured(
            unique_tournament_id=unique_tournament_id,
            sport_slug=sport_slug,
            timeout=timeout,
        )
        collected_event_ids.extend(int(event.id) for event in featured.parsed.events)
    except Exception as exc:
        logger.warning(
            "structure-sync featured-events failed tournament=%s: %s",
            unique_tournament_id,
            exc,
        )

    # Daily scheduled scan — capped forward window, skip consecutive empty days
    # after a reasonable warm-up to save API calls on quiet tournaments.
    empty_streak = 0
    for offset in range(total_days):
        probe_date = start_date + timedelta(days=offset)
        iso = probe_date.isoformat()
        dates_probed += 1
        try:
            daily = await event_list_job.run_unique_tournament_scheduled(
                unique_tournament_id=unique_tournament_id,
                date=iso,
                sport_slug=sport_slug,
                timeout=timeout,
            )
        except Exception as exc:
            logger.warning(
                "structure-sync scheduled failed tournament=%s date=%s: %s",
                unique_tournament_id,
                iso,
                exc,
            )
            empty_streak += 1
            if empty_streak >= _MAX_CONSECUTIVE_EMPTY_CALENDAR_DAYS:
                break
            continue
        event_count = len(daily.parsed.events)
        if event_count == 0:
            empty_streak += 1
            if empty_streak >= _MAX_CONSECUTIVE_EMPTY_CALENDAR_DAYS:
                break
            continue
        empty_streak = 0
        collected_event_ids.extend(int(event.id) for event in daily.parsed.events)

    return StructureSyncResult(
        unique_tournament_id=unique_tournament_id,
        sport_slug=sport_slug,
        mode="calendar",
        season_ids=season_ids,
        calendar_dates_probed=dates_probed,
        event_ids=tuple(collected_event_ids),
        success=True,
    )


async def _run_brackets_mode(
    *,
    unique_tournament_id: int,
    sport_slug: str,
    profile: SportProfile,
    season_ids: tuple[int, ...],
    current_season_id: int,
    event_list_job: EventListIngestJob,
    now_factory,
    timeout: float,
) -> StructureSyncResult:
    try:
        result = await event_list_job.run_brackets(
            unique_tournament_id=unique_tournament_id,
            season_id=current_season_id,
            sport_slug=sport_slug,
            timeout=timeout,
        )
    except Exception as exc:
        logger.warning(
            "structure-sync brackets failed tournament=%s season=%s: %s",
            unique_tournament_id,
            current_season_id,
            exc,
        )
        fallback = await _run_calendar_mode(
            unique_tournament_id=unique_tournament_id,
            sport_slug=sport_slug,
            profile=profile,
            season_ids=season_ids,
            event_list_job=event_list_job,
            now_factory=now_factory,
            timeout=timeout,
        )
        return StructureSyncResult(
            unique_tournament_id=unique_tournament_id,
            sport_slug=sport_slug,
            mode="brackets->calendar",
            season_ids=season_ids,
            calendar_dates_probed=fallback.calendar_dates_probed,
            event_ids=fallback.event_ids,
            success=True,
            reason=f"brackets failed; fell back to calendar: {exc}",
        )

    event_ids = tuple(int(event.id) for event in result.parsed.events)
    if event_ids:
        return StructureSyncResult(
            unique_tournament_id=unique_tournament_id,
            sport_slug=sport_slug,
            mode="brackets",
            season_ids=season_ids,
            event_ids=event_ids,
            success=True,
        )

    fallback = await _run_calendar_mode(
        unique_tournament_id=unique_tournament_id,
        sport_slug=sport_slug,
        profile=profile,
        season_ids=season_ids,
        event_list_job=event_list_job,
        now_factory=now_factory,
        timeout=timeout,
    )
    return StructureSyncResult(
        unique_tournament_id=unique_tournament_id,
        sport_slug=sport_slug,
        mode="brackets->calendar",
        season_ids=season_ids,
        calendar_dates_probed=fallback.calendar_dates_probed,
        event_ids=fallback.event_ids,
        success=True,
        reason="brackets empty; fell back to calendar",
    )


async def _append_current_season_event_surfaces(
    result: StructureSyncResult,
    *,
    app,
    unique_tournament_id: int,
    sport_slug: str,
    current_season_id: int,
    event_list_job: EventListIngestJob,
    timeout: float,
) -> StructureSyncResult:
    if not result.success:
        return result
    if not _has_season_event_surface_runners(event_list_job):
        return result

    freshness_key = _season_event_surface_freshness_key(sport_slug, unique_tournament_id, current_season_id)
    redis_backend = getattr(app, "redis_backend", None)
    if _is_season_event_surface_fresh(redis_backend, freshness_key):
        return result

    surface_result = await _sync_current_season_event_surfaces(
        unique_tournament_id=unique_tournament_id,
        season_id=current_season_id,
        sport_slug=sport_slug,
        event_list_job=event_list_job,
        timeout=timeout,
    )
    if surface_result.complete:
        _mark_season_event_surface_fresh(redis_backend, freshness_key)
    if not surface_result.event_ids:
        return result

    return replace(result, event_ids=_dedupe_event_ids((*result.event_ids, *surface_result.event_ids)))


async def _sync_current_season_event_surfaces(
    *,
    unique_tournament_id: int,
    season_id: int,
    sport_slug: str,
    event_list_job: EventListIngestJob,
    timeout: float,
) -> _SeasonSurfaceSyncResult:
    last_result = await _run_paginated_season_surface(
        event_list_job.run_season_last,
        surface_name="last",
        unique_tournament_id=unique_tournament_id,
        season_id=season_id,
        sport_slug=sport_slug,
        timeout=timeout,
    )
    next_result = await _run_paginated_season_surface(
        event_list_job.run_season_next,
        surface_name="next",
        unique_tournament_id=unique_tournament_id,
        season_id=season_id,
        sport_slug=sport_slug,
        timeout=timeout,
    )
    return _SeasonSurfaceSyncResult(
        event_ids=_dedupe_event_ids((*last_result.event_ids, *next_result.event_ids)),
        complete=last_result.complete and next_result.complete,
    )


async def _run_paginated_season_surface(
    runner,
    *,
    surface_name: str,
    unique_tournament_id: int,
    season_id: int,
    sport_slug: str,
    timeout: float,
) -> _SeasonSurfaceSyncResult:
    collected: list[int] = []
    for page in range(_MAX_SEASON_EVENT_PAGES):
        try:
            result = await runner(
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                page=page,
                sport_slug=sport_slug,
                timeout=timeout,
            )
        except Exception as exc:
            logger.warning(
                "structure-sync season %s-events failed tournament=%s season=%s page=%s: %s",
                surface_name,
                unique_tournament_id,
                season_id,
                page,
                exc,
            )
            return _SeasonSurfaceSyncResult(event_ids=_dedupe_event_ids(collected), complete=False)
        collected.extend(int(event.id) for event in getattr(result.parsed, "events", ()))
        if not _result_has_next_page(result):
            return _SeasonSurfaceSyncResult(event_ids=_dedupe_event_ids(collected), complete=True)
    logger.warning(
        "structure-sync season %s-events reached max page limit tournament=%s season=%s max_pages=%s",
        surface_name,
        unique_tournament_id,
        season_id,
        _MAX_SEASON_EVENT_PAGES,
    )
    return _SeasonSurfaceSyncResult(event_ids=_dedupe_event_ids(collected), complete=False)


def _result_has_next_page(result) -> bool:
    snapshots = getattr(getattr(result, "parsed", None), "payload_snapshots", ())
    for snapshot in snapshots:
        payload = getattr(snapshot, "payload", None)
        if isinstance(payload, Mapping):
            return bool(payload.get("hasNextPage"))
    return False


def _has_season_event_surface_runners(event_list_job: EventListIngestJob) -> bool:
    return _is_async_callable(getattr(event_list_job, "run_season_last", None)) and _is_async_callable(
        getattr(event_list_job, "run_season_next", None)
    )


def _is_async_callable(candidate: Any) -> bool:
    return bool(candidate is not None and inspect.iscoroutinefunction(candidate))


def _dedupe_event_ids(event_ids) -> tuple[int, ...]:
    seen: set[int] = set()
    output: list[int] = []
    for raw_event_id in event_ids:
        event_id = int(raw_event_id)
        if event_id in seen:
            continue
        seen.add(event_id)
        output.append(event_id)
    return tuple(output)


def _season_event_surface_freshness_key(sport_slug: str, unique_tournament_id: int, season_id: int) -> str:
    return f"freshness:season-events:{sport_slug}:{int(unique_tournament_id)}:{int(season_id)}"


def _is_season_event_surface_fresh(redis_backend, key: str) -> bool:
    if redis_backend is None:
        return False
    try:
        return bool(_call_redis(redis_backend.exists, key, now_ms=_now_ms()))
    except Exception as exc:
        logger.warning("structure-sync season event freshness check failed key=%s: %s", key, exc)
        return False


def _mark_season_event_surface_fresh(redis_backend, key: str) -> None:
    if redis_backend is None:
        return
    try:
        _call_redis(
            redis_backend.set,
            key,
            "1",
            px=_SEASON_EVENT_SURFACE_REFRESH_TTL_SECONDS * 1000,
            now_ms=_now_ms(),
        )
    except Exception as exc:
        logger.warning("structure-sync season event freshness mark failed key=%s: %s", key, exc)


def _call_redis(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except TypeError:
        filtered_kwargs = {key: value for key, value in kwargs.items() if key != "now_ms"}
        return method(*args, **filtered_kwargs)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _has_bracket_capability(tournament: UniqueTournamentRecord | None) -> bool:
    return bool(tournament is not None and tournament.has_playoff_series is True)


def _default_now_utc() -> datetime:
    return datetime.now(timezone.utc)


__all__ = [
    "StructureSyncResult",
    "run_structure_sync_for_tournament",
]
