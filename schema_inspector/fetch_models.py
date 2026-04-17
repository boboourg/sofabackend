"""Typed models for the shared hybrid ETL fetch executor."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from .runtime import TransportAttempt


@dataclass(frozen=True)
class FetchTask:
    trace_id: str | None
    job_id: str | None
    sport_slug: str | None
    endpoint_pattern: str
    source_url: str
    timeout_profile: str
    timeout_seconds: float = 20.0
    method: str = "GET"
    request_headers: Mapping[str, str] | None = None
    query_params: Mapping[str, object] | None = None
    context_entity_type: str | None = None
    context_entity_id: int | None = None
    context_unique_tournament_id: int | None = None
    context_season_id: int | None = None
    context_event_id: int | None = None
    expected_content_type: str | None = "application/json"
    fetch_reason: str | None = None


@dataclass(frozen=True)
class ClassifiedFetchResult:
    classification: str
    payload: object | None
    is_valid_json: bool
    is_empty_payload: bool
    is_soft_error_payload: bool
    payload_root_keys: tuple[str, ...]
    retry_recommended: bool
    capability_signal: str
    content_type: str | None


@dataclass(frozen=True)
class FetchOutcomeEnvelope:
    trace_id: str | None
    job_id: str | None
    endpoint_pattern: str
    source_url: str
    resolved_url: str | None
    http_status: int | None
    classification: str
    proxy_id: str | None
    challenge_reason: str | None
    snapshot_id: int | None
    payload_hash: str | None
    payload_root_keys: tuple[str, ...] = field(default_factory=tuple)
    is_valid_json: bool = False
    is_empty_payload: bool = False
    is_soft_error_payload: bool = False
    retry_recommended: bool = False
    capability_signal: str = "unknown"
    attempts: tuple[TransportAttempt, ...] = field(default_factory=tuple)
    fetched_at: str | None = None
    error_message: str | None = None
