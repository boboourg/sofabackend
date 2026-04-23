"""Family parser for `/event/{id}/pregame-form` payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class EventPregameFormParser:
    parser_family = "event_pregame_form"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        event_id = snapshot.context_event_id or snapshot.context_entity_id
        if event_id is None:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_PARSED_EMPTY,
            )

        form_rows = [{"event_id": event_id, "label": _as_str(payload.get("label"))}]
        side_rows: list[Mapping[str, object]] = []
        item_rows: list[Mapping[str, object]] = []

        for side, key in (("home", "homeTeam"), ("away", "awayTeam")):
            item = _as_mapping(payload.get(key))
            if item is None:
                continue
            side_rows.append(
                {
                    "event_id": event_id,
                    "side": side,
                    "avg_rating": _as_scalar_text(item.get("avgRating")),
                    "position": _as_int(item.get("position")),
                    "value": _as_scalar_text(item.get("value")),
                }
            )
            form_items = item.get("form")
            if not isinstance(form_items, (list, tuple)):
                continue
            for ordinal, form_item in enumerate(form_items):
                text_value = _as_scalar_text(form_item)
                if text_value is None:
                    continue
                item_rows.append(
                    {
                        "event_id": event_id,
                        "side": side,
                        "ordinal": ordinal,
                        "form_value": text_value,
                    }
                )

        metric_rows: dict[str, tuple[Mapping[str, object], ...]] = {"event_pregame_form": tuple(form_rows)}
        if side_rows:
            metric_rows["event_pregame_form_side"] = tuple(side_rows)
        if item_rows:
            metric_rows["event_pregame_form_item"] = tuple(item_rows)
        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if side_rows or item_rows or form_rows else PARSE_STATUS_PARSED_EMPTY,
            metric_rows=metric_rows,
            observed_root_keys=snapshot.observed_root_keys,
        )


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_scalar_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return None


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
