"""Schema inspection tools for JSON API responses."""

from .categories_seed_job import CategoriesSeedIngestJob, CategoriesSeedIngestResult
from .categories_seed_parser import (
    CategoriesSeedBundle,
    CategoriesSeedParser,
    CategoriesSeedParserError,
    CategoryDailySummaryRecord,
    CategoryDailyTeamRecord,
    CategoryDailyUniqueTournamentRecord,
)
from .categories_seed_repository import CategoriesSeedRepository, CategoriesSeedWriteResult
from .competition_job import CompetitionIngestJob, CompetitionIngestResult
from .competition_parser import CompetitionBundle, CompetitionParser, CompetitionParserError
from .competition_repository import CompetitionRepository, CompetitionWriteResult
from .db import AsyncpgDatabase, DatabaseConfig, load_database_config
from .entities_backfill_job import EntitiesBackfillJob, EntitiesBackfillResult
from .entities_job import EntitiesIngestJob, EntitiesIngestResult
from .entities_parser import (
    EntitiesBundle,
    EntitiesParser,
    EntitiesParserError,
    PlayerHeatmapRequest,
    PlayerOverallRequest,
    PlayerSeasonStatisticsRecord,
    TeamOverallRequest,
    TeamPerformanceGraphRequest,
)
from .entities_repository import EntitiesRepository, EntitiesWriteResult
from .endpoints import (
    CATEGORIES_SEED_ENDPOINTS,
    ENTITIES_ENDPOINTS,
    EVENT_DETAIL_ENDPOINTS,
    EVENT_LIST_ENDPOINTS,
    LEADERBOARDS_ENDPOINTS,
    STANDINGS_ENDPOINTS,
    STATISTICS_ENDPOINTS,
    EndpointRegistryEntry,
    SofascoreEndpoint,
    categories_seed_registry_entries,
    competition_registry_entries,
    entities_registry_entries,
    event_detail_registry_entries,
    event_list_registry_entries,
    leaderboards_registry_entries,
    standings_registry_entries,
    statistics_registry_entries,
)
from .event_detail_backfill_job import EventDetailBackfillJob, EventDetailBackfillResult
from .event_detail_job import EventDetailIngestJob, EventDetailIngestResult
from .event_detail_parser import (
    EventDetailBundle,
    EventDetailParser,
    EventDetailParserError,
    ProviderConfigurationRecord,
)
from .event_detail_repository import EventDetailRepository, EventDetailWriteResult
from .event_list_job import EventListIngestJob, EventListIngestResult
from .event_list_parser import EventListBundle, EventListParser, EventListParserError
from .event_list_repository import EventListRepository, EventListWriteResult
from .leaderboards_backfill_job import (
    LeaderboardsBackfillItem,
    LeaderboardsBackfillJob,
    LeaderboardsBackfillResult,
)
from .leaderboards_job import LeaderboardsIngestJob, LeaderboardsIngestResult
from .leaderboards_parser import (
    LeaderboardsBundle,
    LeaderboardsParser,
    LeaderboardsParserError,
    PeriodRecord,
    SeasonGroupRecord,
    SeasonPlayerOfTheSeasonRecord,
    TeamOfTheWeekPlayerRecord,
    TeamOfTheWeekRecord,
    TopPlayerEntryRecord,
    TopPlayerSnapshotRecord,
    TopTeamEntryRecord,
    TopTeamSnapshotRecord,
    TournamentTeamEventBucketRecord,
    TournamentTeamEventSnapshotRecord,
)
from .leaderboards_repository import LeaderboardsRepository, LeaderboardsWriteResult
from .service import inspect_url_to_markdown
from .runtime import RuntimeConfig, load_runtime_config
from .sofascore_client import (
    SofascoreAccessDeniedError,
    SofascoreClient,
    SofascoreClientError,
    SofascoreHttpError,
    SofascoreJsonDecodeError,
    SofascoreRateLimitError,
)
from .standings_backfill_job import StandingsBackfillItem, StandingsBackfillJob, StandingsBackfillResult
from .statistics_backfill_job import StatisticsBackfillItem, StatisticsBackfillJob, StatisticsBackfillResult
from .statistics_job import StatisticsIngestJob, StatisticsIngestResult
from .statistics_parser import StatisticsBundle, StatisticsParser, StatisticsParserError, StatisticsQuery
from .statistics_repository import StatisticsRepository, StatisticsWriteResult
from .standings_job import StandingsIngestJob, StandingsIngestResult
from .standings_parser import StandingsBundle, StandingsParser, StandingsParserError
from .standings_repository import StandingsRepository, StandingsWriteResult
from .timezone_utils import resolve_timezone_offset_seconds

__all__ = [
    "inspect_url_to_markdown",
    "RuntimeConfig",
    "load_runtime_config",
    "DatabaseConfig",
    "load_database_config",
    "AsyncpgDatabase",
    "EndpointRegistryEntry",
    "SofascoreEndpoint",
    "CATEGORIES_SEED_ENDPOINTS",
    "ENTITIES_ENDPOINTS",
    "EVENT_DETAIL_ENDPOINTS",
    "EVENT_LIST_ENDPOINTS",
    "LEADERBOARDS_ENDPOINTS",
    "STANDINGS_ENDPOINTS",
    "STATISTICS_ENDPOINTS",
    "categories_seed_registry_entries",
    "competition_registry_entries",
    "entities_registry_entries",
    "event_detail_registry_entries",
    "event_list_registry_entries",
    "leaderboards_registry_entries",
    "standings_registry_entries",
    "statistics_registry_entries",
    "SofascoreClient",
    "SofascoreClientError",
    "SofascoreHttpError",
    "SofascoreRateLimitError",
    "SofascoreAccessDeniedError",
    "SofascoreJsonDecodeError",
    "CategoriesSeedBundle",
    "CategoryDailySummaryRecord",
    "CategoryDailyUniqueTournamentRecord",
    "CategoryDailyTeamRecord",
    "CategoriesSeedParser",
    "CategoriesSeedParserError",
    "CategoriesSeedRepository",
    "CategoriesSeedWriteResult",
    "CategoriesSeedIngestJob",
    "CategoriesSeedIngestResult",
    "CompetitionBundle",
    "CompetitionParser",
    "CompetitionParserError",
    "CompetitionRepository",
    "CompetitionWriteResult",
    "CompetitionIngestJob",
    "CompetitionIngestResult",
    "EntitiesBundle",
    "EntitiesParser",
    "EntitiesParserError",
    "PlayerOverallRequest",
    "PlayerSeasonStatisticsRecord",
    "TeamOverallRequest",
    "PlayerHeatmapRequest",
    "TeamPerformanceGraphRequest",
    "EntitiesRepository",
    "EntitiesWriteResult",
    "EntitiesIngestJob",
    "EntitiesIngestResult",
    "EntitiesBackfillJob",
    "EntitiesBackfillResult",
    "EventDetailBundle",
    "EventDetailParser",
    "EventDetailParserError",
    "ProviderConfigurationRecord",
    "EventDetailRepository",
    "EventDetailWriteResult",
    "EventDetailIngestJob",
    "EventDetailIngestResult",
    "EventDetailBackfillJob",
    "EventDetailBackfillResult",
    "EventListBundle",
    "EventListParser",
    "EventListParserError",
    "EventListRepository",
    "EventListWriteResult",
    "EventListIngestJob",
    "EventListIngestResult",
    "LeaderboardsBundle",
    "TopPlayerEntryRecord",
    "TopPlayerSnapshotRecord",
    "TopTeamEntryRecord",
    "TopTeamSnapshotRecord",
    "TournamentTeamEventBucketRecord",
    "TournamentTeamEventSnapshotRecord",
    "PeriodRecord",
    "SeasonGroupRecord",
    "SeasonPlayerOfTheSeasonRecord",
    "TeamOfTheWeekRecord",
    "TeamOfTheWeekPlayerRecord",
    "LeaderboardsParser",
    "LeaderboardsParserError",
    "LeaderboardsRepository",
    "LeaderboardsWriteResult",
    "LeaderboardsIngestJob",
    "LeaderboardsIngestResult",
    "LeaderboardsBackfillItem",
    "LeaderboardsBackfillJob",
    "LeaderboardsBackfillResult",
    "StatisticsBundle",
    "StatisticsQuery",
    "StatisticsParser",
    "StatisticsParserError",
    "StatisticsRepository",
    "StatisticsWriteResult",
    "StatisticsIngestJob",
    "StatisticsIngestResult",
    "StatisticsBackfillItem",
    "StatisticsBackfillJob",
    "StatisticsBackfillResult",
    "StandingsBundle",
    "StandingsParser",
    "StandingsParserError",
    "StandingsRepository",
    "StandingsWriteResult",
    "StandingsIngestJob",
    "StandingsIngestResult",
    "StandingsBackfillItem",
    "StandingsBackfillJob",
    "StandingsBackfillResult",
    "resolve_timezone_offset_seconds",
]
