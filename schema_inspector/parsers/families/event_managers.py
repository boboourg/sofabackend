"""Family parser for `/event/{id}/managers` payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot
from ..entities import extract_entities


class EventManagersParser:
    parser_family = "event_managers"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        event_id = snapshot.context_event_id or snapshot.context_entity_id
        assignment_rows: list[Mapping[str, object]] = []
        performance_rows: list[Mapping[str, object]] = []

        for side, key in (("home", "homeManager"), ("away", "awayManager")):
            manager = _as_mapping(payload.get(key))
            manager_id = _as_int(manager.get("id")) if manager is not None else None
            if manager_id is None or event_id is None:
                continue
            assignment_rows.append({"event_id": event_id, "side": side, "manager_id": manager_id})
            performance = _as_mapping(manager.get("performance")) if manager is not None else None
            if performance is None:
                continue
            performance_rows.append(
                {
                    "manager_id": manager_id,
                    "total": _as_int(performance.get("total")),
                    "wins": _as_int(performance.get("wins")),
                    "draws": _as_int(performance.get("draws")),
                    "losses": _as_int(performance.get("losses")),
                    "goals_scored": _as_int(performance.get("goalsScored")),
                    "goals_conceded": _as_int(performance.get("goalsConceded")),
                    "total_points": _as_int(performance.get("totalPoints")),
                }
            )

        metric_rows: dict[str, tuple[Mapping[str, object], ...]] = {}
        if assignment_rows:
            metric_rows["event_manager_assignment"] = tuple(assignment_rows)
        if performance_rows:
            metric_rows["manager_performance"] = tuple(performance_rows)

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if metric_rows else PARSE_STATUS_PARSED_EMPTY,
            entity_upserts=extract_entities(snapshot.payload),
            metric_rows=metric_rows,
            observed_root_keys=snapshot.observed_root_keys,
        )


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit() or (stripped.startswith("-") and stripped[1:].isdigit()):
            return int(stripped)
    return None
