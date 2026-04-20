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
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from ..competition_job import CompetitionIngestJob
from ..competition_parser import CompetitionParser, UniqueTournamentRecord
from ..competition_repository import CompetitionRepository
from ..event_list_job import EventListIngestJob
from ..event_list_parser import EventListParser
from ..event_list_repository import EventListRepository
from ..sofascore_client import SofascoreClient
from ..sport_profiles import SportProfile, resolve_sport_profile

logger = logging.getLogger(__name__)


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
    client = SofascoreClient(client_runtime, transport=client_transport)

    competition_job = CompetitionIngestJob(
        CompetitionParser(client),
        CompetitionRepository(),
        app.database,
    )
    event_list_job = EventListIngestJob(
        EventListParser(client),
        EventListRepository(),
        app.database,
    )

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
        return await _run_rounds_mode(
            unique_tournament_id=int(unique_tournament_id),
            sport_slug=normalized_sport,
            profile=profile,
            season_ids=season_ids,
            current_season_id=current_season_id,
            event_list_job=event_list_job,
            now_factory=now_factory,
            max_rounds_probe=max_rounds_probe,
            timeout=timeout,
        )

    # calendar mode (either explicit or auto fallback)
    return await _run_calendar_mode(
        unique_tournament_id=int(unique_tournament_id),
        sport_slug=normalized_sport,
        profile=profile,
        season_ids=season_ids,
        event_list_job=event_list_job,
        now_factory=now_factory,
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
    if mode in ("rounds", "calendar"):
        return mode
    if mode == "disabled":
        return "disabled"
    # auto: decide per-tournament based on has_rounds
    has_rounds = None if tournament is None else tournament.has_rounds
    if has_rounds is True:
        return "rounds"
    if has_rounds is False:
        return "calendar"
    # Unknown / missing — fall back to calendar (always produces *some* skeleton).
    return "calendar"


async def _run_rounds_mode(
    *,
    unique_tournament_id: int,
    sport_slug: str,
    profile: SportProfile,
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
            continue
        event_count = len(result.parsed.events)
        if event_count == 0:
            # First empty round after we had at least one populated round — stop.
            if rounds_with_events > 0:
                break
            # Still zero-zero: give it 2 more chances, then give up on rounds mode.
            if round_number >= 3:
                break
            continue
        rounds_with_events += 1
        collected_event_ids.extend(int(event.id) for event in result.parsed.events)

    if rounds_with_events == 0 and profile.structure_rounds_fallback_calendar:
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
            if empty_streak >= 10:
                break
            continue
        event_count = len(daily.parsed.events)
        if event_count == 0:
            empty_streak += 1
            # Abort early if we've crossed ~14 consecutive empty days.
            if empty_streak >= 14:
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


def _default_now_utc() -> datetime:
    return datetime.now(timezone.utc)


__all__ = [
    "StructureSyncResult",
    "run_structure_sync_for_tournament",
]
