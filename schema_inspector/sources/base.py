from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Mapping

if False:
    from ..db import AsyncpgDatabase


@dataclass(frozen=True)
class SourceFetchRequest:
    url: str
    timeout: float = 20.0
    headers: Mapping[str, str] | None = None


@dataclass(frozen=True)
class SourceFetchResponse:
    source_slug: str
    source_url: str
    resolved_url: str
    fetched_at: str
    status_code: int
    headers: Mapping[str, str]
    body_bytes: bytes
    payload: object


class SourceAdapter(ABC):
    source_slug = ""
    is_enabled = True

    @abstractmethod
    async def get_json(self, request: SourceFetchRequest) -> SourceFetchResponse:
        raise NotImplementedError

    @abstractmethod
    def build_event_list_job(self, database: "AsyncpgDatabase"):
        raise NotImplementedError
