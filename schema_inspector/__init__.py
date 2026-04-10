"""Schema inspection tools for JSON API responses."""

from .competition_job import CompetitionIngestJob, CompetitionIngestResult
from .competition_parser import CompetitionBundle, CompetitionParser, CompetitionParserError
from .competition_repository import CompetitionRepository, CompetitionWriteResult
from .db import AsyncpgDatabase, DatabaseConfig, load_database_config
from .endpoints import (
    EVENT_DETAIL_ENDPOINTS,
    EVENT_LIST_ENDPOINTS,
    EndpointRegistryEntry,
    SofascoreEndpoint,
    competition_registry_entries,
    event_detail_registry_entries,
    event_list_registry_entries,
)
from .event_detail_job import EventDetailIngestJob, EventDetailIngestResult
from .event_detail_parser import EventDetailBundle, EventDetailParser, EventDetailParserError
from .event_detail_repository import EventDetailRepository, EventDetailWriteResult
from .event_list_job import EventListIngestJob, EventListIngestResult
from .event_list_parser import EventListBundle, EventListParser, EventListParserError
from .event_list_repository import EventListRepository, EventListWriteResult
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

__all__ = [
    "inspect_url_to_markdown",
    "RuntimeConfig",
    "load_runtime_config",
    "DatabaseConfig",
    "load_database_config",
    "AsyncpgDatabase",
    "EndpointRegistryEntry",
    "SofascoreEndpoint",
    "EVENT_DETAIL_ENDPOINTS",
    "EVENT_LIST_ENDPOINTS",
    "competition_registry_entries",
    "event_detail_registry_entries",
    "event_list_registry_entries",
    "SofascoreClient",
    "SofascoreClientError",
    "SofascoreHttpError",
    "SofascoreRateLimitError",
    "SofascoreAccessDeniedError",
    "SofascoreJsonDecodeError",
    "CompetitionBundle",
    "CompetitionParser",
    "CompetitionParserError",
    "CompetitionRepository",
    "CompetitionWriteResult",
    "CompetitionIngestJob",
    "CompetitionIngestResult",
    "EventDetailBundle",
    "EventDetailParser",
    "EventDetailParserError",
    "EventDetailRepository",
    "EventDetailWriteResult",
    "EventDetailIngestJob",
    "EventDetailIngestResult",
    "EventListBundle",
    "EventListParser",
    "EventListParserError",
    "EventListRepository",
    "EventListWriteResult",
    "EventListIngestJob",
    "EventListIngestResult",
]
