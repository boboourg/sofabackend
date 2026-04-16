"""Family parser for `/event/{id}/incidents` payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot
from ..entities import extract_entities


class EventIncidentsParser:
    parser_family = "event_incidents"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        incidents = payload.get("incidents")
        rows: list[Mapping[str, object]] = []
        if isinstance(incidents, (list, tuple)):
            for ordinal, item in enumerate(incidents):
                incident = _as_mapping(item)
                if incident is None:
                    continue
                rows.append(
                    {
                        "event_id": snapshot.context_event_id or snapshot.context_entity_id,
                        "ordinal": ordinal,
                        "incident_id": _as_int(incident.get("id")),
                        "incident_type": _as_str(incident.get("incidentType")) or _as_str(incident.get("type")),
                        "time": _as_int(incident.get("time")),
                        "home_score": incident.get("homeScore"),
                        "away_score": incident.get("awayScore"),
                        "text": _as_str(incident.get("text")),
                    }
                )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if rows else PARSE_STATUS_PARSED_EMPTY,
            entity_upserts=extract_entities(snapshot.payload),
            metric_rows={"event_incident": tuple(rows)} if rows else {},
            observed_root_keys=snapshot.observed_root_keys,
        )


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None
