from __future__ import annotations

from ..categories_seed_job import CategoriesSeedIngestJob
from ..categories_seed_parser import CategoriesSeedParser
from ..categories_seed_repository import CategoriesSeedRepository
from ..competition_job import CompetitionIngestJob
from ..competition_parser import CompetitionParser
from ..competition_repository import CompetitionRepository
from ..default_tournaments_parser import DefaultTournamentListParser
from ..entities_job import EntitiesIngestJob
from ..entities_parser import EntitiesParser
from ..entities_repository import EntitiesRepository
from ..event_detail_job import EventDetailIngestJob
from ..event_detail_parser import EventDetailParser
from ..event_detail_repository import EventDetailRepository
from ..event_list_job import EventListIngestJob
from ..event_list_parser import EventListParser
from ..event_list_repository import EventListRepository
from ..leaderboards_job import LeaderboardsIngestJob
from ..leaderboards_parser import LeaderboardsParser
from ..leaderboards_repository import LeaderboardsRepository
from ..runtime import RuntimeConfig
from ..sofascore_client import SofascoreClient
from ..standings_job import StandingsIngestJob
from ..standings_parser import StandingsParser
from ..standings_repository import StandingsRepository
from ..statistics_job import StatisticsIngestJob
from ..statistics_parser import StatisticsParser
from ..statistics_repository import StatisticsRepository
from ..transport import InspectorTransport
from .base import SourceAdapter, SourceFetchRequest, SourceFetchResponse


class SofascoreSourceAdapter(SourceAdapter):
    source_slug = "sofascore"

    def __init__(
        self,
        *,
        runtime_config: RuntimeConfig,
        transport: InspectorTransport | None = None,
        client: SofascoreClient | None = None,
    ) -> None:
        self.runtime_config = runtime_config
        self.client = client or SofascoreClient(runtime_config, transport=transport)

    async def get_json(self, request: SourceFetchRequest) -> SourceFetchResponse:
        response = await self.client.get_json(
            request.url,
            headers=request.headers,
            timeout=float(request.timeout),
        )
        return SourceFetchResponse(
            source_slug=self.source_slug,
            source_url=response.source_url,
            resolved_url=response.resolved_url,
            fetched_at=response.fetched_at,
            status_code=int(response.status_code),
            headers=response.headers,
            body_bytes=response.body_bytes,
            payload=response.payload,
            attempts=response.attempts,
            final_proxy_name=response.final_proxy_name,
            challenge_reason=response.challenge_reason,
        )

    def build_categories_seed_job(self, database):
        return CategoriesSeedIngestJob(
            CategoriesSeedParser(self.client),
            CategoriesSeedRepository(),
            database,
        )

    def build_default_tournament_list_parser(self):
        return DefaultTournamentListParser(self.client)

    def build_event_list_job(self, database):
        return EventListIngestJob(
            EventListParser(self.client),
            EventListRepository(),
            database,
        )

    def build_competition_job(self, database):
        return CompetitionIngestJob(
            CompetitionParser(self.client),
            CompetitionRepository(),
            database,
        )

    def build_event_detail_job(self, database):
        return EventDetailIngestJob(
            EventDetailParser(self.client),
            EventDetailRepository(),
            database,
        )

    def build_entities_job(self, database):
        return EntitiesIngestJob(
            EntitiesParser(self.client),
            EntitiesRepository(),
            database,
        )

    def build_statistics_job(self, database):
        return StatisticsIngestJob(
            StatisticsParser(self.client),
            StatisticsRepository(),
            database,
        )

    def build_standings_job(self, database):
        return StandingsIngestJob(
            StandingsParser(self.client),
            StandingsRepository(),
            database,
        )

    def build_leaderboards_job(self, database):
        return LeaderboardsIngestJob(
            LeaderboardsParser(self.client),
            LeaderboardsRepository(),
            database,
        )
