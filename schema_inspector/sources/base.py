from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Mapping

from ..runtime import TransportAttempt

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
    attempts: tuple[TransportAttempt, ...]
    final_proxy_name: str | None
    challenge_reason: str | None


class SourceAdapterError(RuntimeError):
    """Base adapter-boundary error."""


class UnknownSourceAdapterError(SourceAdapterError, ValueError):
    """Raised when no adapter is registered for a source slug."""


class DisabledSourceAdapterError(SourceAdapterError, RuntimeError):
    """Raised when an adapter exists but is intentionally disabled."""


class UnsupportedSourceAdapterError(SourceAdapterError, RuntimeError):
    """Raised when a source exists but a specific operation is not wired yet."""


class SourceAdapter(ABC):
    source_slug = ""
    is_enabled = True

    @abstractmethod
    async def get_json(self, request: SourceFetchRequest) -> SourceFetchResponse:
        raise NotImplementedError

    @abstractmethod
    def build_event_list_job(self, database: "AsyncpgDatabase"):
        raise NotImplementedError
