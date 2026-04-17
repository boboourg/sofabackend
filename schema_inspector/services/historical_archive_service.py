"""Helpers for historical tournament/archive workers."""

from __future__ import annotations

from ..competition_job import CompetitionIngestJob
from ..competition_parser import CompetitionParser
from ..competition_repository import CompetitionRepository
from ..default_tournaments_pipeline_cli import _run_tournament_worker
from ..entities_backfill_job import EntitiesBackfillJob
from ..entities_job import EntitiesIngestJob
from ..entities_parser import EntitiesParser
from ..entities_repository import EntitiesRepository
from ..event_detail_backfill_job import EventDetailBackfillJob
from ..event_detail_job import EventDetailIngestJob
from ..event_detail_parser import EventDetailParser
from ..event_detail_repository import EventDetailRepository
from ..event_list_job import EventListIngestJob
from ..event_list_parser import EventListParser
from ..event_list_repository import EventListRepository
from ..leaderboards_job import LeaderboardsIngestJob
from ..leaderboards_parser import LeaderboardsParser
from ..leaderboards_repository import LeaderboardsRepository
from ..sofascore_client import SofascoreClient
from ..sport_profiles import resolve_sport_profile
from ..standings_job import StandingsIngestJob
from ..standings_parser import StandingsParser
from ..standings_repository import StandingsRepository
from ..statistics_job import StatisticsIngestJob
from ..statistics_parser import StatisticsParser, StatisticsQuery
from ..statistics_repository import StatisticsRepository


async def run_historical_tournament_archive(
    app,
    *,
    unique_tournament_id: int,
    sport_slug: str,
    seasons_per_tournament: int = 0,
    event_concurrency: int = 4,
    timeout: float = 20.0,
) -> dict[str, object]:
    client = SofascoreClient(app.runtime_config, transport=app.transport)
    sport_profile = resolve_sport_profile(sport_slug)
    competition_job = CompetitionIngestJob(CompetitionParser(client), CompetitionRepository(), app.database)
    event_list_job = EventListIngestJob(EventListParser(client), EventListRepository(), app.database)
    statistics_job = StatisticsIngestJob(StatisticsParser(client), StatisticsRepository(), app.database)
    standings_job = StandingsIngestJob(StandingsParser(client), StandingsRepository(), app.database)
    leaderboards_job = LeaderboardsIngestJob(LeaderboardsParser(client), LeaderboardsRepository(), app.database)
    event_detail_job = EventDetailIngestJob(EventDetailParser(client), EventDetailRepository(), app.database)
    entities_job = EntitiesIngestJob(EntitiesParser(client), EntitiesRepository(), app.database)
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
        skip_event_detail=True,
        skip_entities=True,
        skip_statistics=False,
        skip_standings=False,
        skip_leaderboards=False,
        timeout=timeout,
    )
    return {
        "season_ids": tuple(int(item) for item in result.season_ids),
        "completed_seasons": int(result.completed_seasons),
        "discovered_event_ids": int(result.discovered_event_ids),
        "stage_failures": int(result.stage_failures),
        "success": bool(result.success),
    }


async def run_historical_tournament_enrichment(
    app,
    *,
    unique_tournament_id: int,
    sport_slug: str,
    season_ids: tuple[int, ...] = (),
    event_detail_concurrency: int = 6,
    timeout: float = 20.0,
) -> dict[str, object]:
    del sport_slug, season_ids
    client = SofascoreClient(app.runtime_config, transport=app.transport)
    event_detail_backfill_job = EventDetailBackfillJob(
        EventDetailIngestJob(EventDetailParser(client), EventDetailRepository(), app.database),
        app.database,
    )
    entities_backfill_job = EntitiesBackfillJob(
        EntitiesIngestJob(EntitiesParser(client), EntitiesRepository(), app.database),
        app.database,
    )
    event_detail_result = await event_detail_backfill_job.run(
        only_missing=True,
        unique_tournament_ids=(int(unique_tournament_id),),
        concurrency=max(1, int(event_detail_concurrency)),
        timeout=timeout,
    )
    entities_result = await entities_backfill_job.run(
        only_missing=True,
        unique_tournament_ids=(int(unique_tournament_id),),
        timeout=timeout,
    )
    return {
        "event_detail_candidates": int(event_detail_result.total_candidates),
        "event_detail_succeeded": int(event_detail_result.succeeded),
        "event_detail_failed": int(event_detail_result.failed),
        "entity_players": len(entities_result.player_ids),
        "entity_teams": len(entities_result.team_ids),
        "entity_snapshots": int(entities_result.ingest.written.payload_snapshot_rows),
    }
