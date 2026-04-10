"""Schema inspection tools for JSON API responses."""

from .competition_job import CompetitionIngestJob, CompetitionIngestResult
from .competition_parser import CompetitionBundle, CompetitionParser, CompetitionParserError
from .competition_repository import CompetitionRepository, CompetitionWriteResult
from .db import AsyncpgDatabase, DatabaseConfig, load_database_config
from .endpoints import (
    EVENT_LIST_ENDPOINTS,
    EndpointRegistryEntry,
    SofascoreEndpoint,
    competition_registry_entries,
    event_list_registry_entries,
)
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
    "EVENT_LIST_ENDPOINTS",
    "competition_registry_entries",
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
    "EventListBundle",
    "EventListParser",
    "EventListParserError",
    "EventListRepository",
    "EventListWriteResult",
    "EventListIngestJob",
    "EventListIngestResult",
]
