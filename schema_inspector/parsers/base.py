"""Core raw-snapshot parser models for the hybrid ETL backbone."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol


PARSE_STATUS_PARSED = "parsed"
PARSE_STATUS_PARSED_EMPTY = "parsed_empty"
PARSE_STATUS_PARTIALLY_PARSED = "partially_parsed"
PARSE_STATUS_SOFT_ERROR = "soft_error_payload"
PARSE_STATUS_UNSUPPORTED = "unsupported_shape"
PARSE_STATUS_FAILED = "failed"


@dataclass(frozen=True)
class RawSnapshot:
    snapshot_id: int | None
    endpoint_pattern: str
    sport_slug: str | None
    source_url: str
    resolved_url: str | None
    envelope_key: str
    http_status: int | None
    payload: object
    fetched_at: str | None
    context_entity_type: str | None = None
    context_entity_id: int | None = None
    context_unique_tournament_id: int | None = None
    context_season_id: int | None = None
    context_event_id: int | None = None

    @property
    def observed_root_keys(self) -> tuple[str, ...]:
        if isinstance(self.payload, Mapping):
            return tuple(sorted(str(key) for key in self.payload.keys()))
        return ()


@dataclass(frozen=True)
class ParseResult:
    snapshot_id: int | None
    parser_family: str
    parser_version: str
    status: str
    entity_upserts: dict[str, tuple[Mapping[str, object], ...]] = field(default_factory=dict)
    relation_upserts: dict[str, tuple[Mapping[str, object], ...]] = field(default_factory=dict)
    metric_rows: dict[str, tuple[Mapping[str, object], ...]] = field(default_factory=dict)
    warnings: tuple[str, ...] = field(default_factory=tuple)
    unsupported_fragments: tuple[str, ...] = field(default_factory=tuple)
    observed_root_keys: tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def empty(
        cls,
        *,
        snapshot: RawSnapshot,
        parser_family: str,
        parser_version: str,
        status: str,
        warnings: tuple[str, ...] = (),
        unsupported_fragments: tuple[str, ...] = (),
    ) -> "ParseResult":
        return cls(
            snapshot_id=snapshot.snapshot_id,
            parser_family=parser_family,
            parser_version=parser_version,
            status=status,
            warnings=warnings,
            unsupported_fragments=unsupported_fragments,
            observed_root_keys=snapshot.observed_root_keys,
        )


class SnapshotParser(Protocol):
    parser_family: str
    parser_version: str

    def parse(self, snapshot: RawSnapshot) -> ParseResult: ...
