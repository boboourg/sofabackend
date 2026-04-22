"""Family parser for `/event/{id}/comments` payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot
from ..entities import extract_entities


class EventCommentsParser:
    parser_family = "event_comments"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        event_id = snapshot.context_event_id or snapshot.context_entity_id
        feed_rows: list[Mapping[str, object]] = []
        comment_rows: list[Mapping[str, object]] = []

        home = _as_mapping(payload.get("home"))
        away = _as_mapping(payload.get("away"))
        if event_id is not None and (home is not None or away is not None):
            feed_rows.append(
                {
                    "event_id": event_id,
                    "home_player_color": home.get("playerColor") if home is not None else None,
                    "home_goalkeeper_color": home.get("goalkeeperColor") if home is not None else None,
                    "away_player_color": away.get("playerColor") if away is not None else None,
                    "away_goalkeeper_color": away.get("goalkeeperColor") if away is not None else None,
                }
            )

        comments = payload.get("comments")
        if isinstance(comments, (list, tuple)):
            for item in comments:
                comment = _as_mapping(item)
                if comment is None or event_id is None:
                    continue
                player = _as_mapping(comment.get("player"))
                comment_rows.append(
                    {
                        "event_id": event_id,
                        "comment_id": _as_int(comment.get("id")),
                        "sequence": _as_int(comment.get("sequence")),
                        "period_name": _as_str(comment.get("periodName")),
                        "is_home": _as_bool(comment.get("isHome")),
                        "player_id": _as_int(player.get("id")) if player is not None else None,
                        "text": _as_str(comment.get("text")),
                        "match_time": _as_float(comment.get("time")),
                        "comment_type": _as_str(comment.get("type")),
                    }
                )

        metric_rows: dict[str, tuple[Mapping[str, object], ...]] = {}
        if feed_rows:
            metric_rows["event_comment_feed"] = tuple(feed_rows)
        if comment_rows:
            metric_rows["event_comment"] = tuple(comment_rows)

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


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


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


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None
