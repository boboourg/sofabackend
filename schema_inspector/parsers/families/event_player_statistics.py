"""Family parser for `/event/{id}/player/{player_id}/statistics` payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_UNSUPPORTED, ParseResult, RawSnapshot
from ..entities import extract_entities


class EventPlayerStatisticsParser:
    parser_family = "event_player_statistics"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        player = _as_mapping(payload.get("player"))
        statistics = _as_mapping(payload.get("statistics"))
        if player is None or statistics is None:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_UNSUPPORTED,
                warnings=("Missing player/statistics envelopes.",),
            )

        event_id = snapshot.context_event_id
        player_id = _as_int(player.get("id")) or snapshot.context_entity_id
        team = _as_mapping(payload.get("team"))
        team_id = _as_int(team.get("id")) if team is not None else None
        rating_versions = _as_mapping(statistics.get("ratingVersions"))
        statistics_type = _as_mapping(statistics.get("statisticsType"))

        summary_row = {
            "event_id": event_id,
            "player_id": player_id,
            "team_id": team_id,
            "position": _as_str(payload.get("position")),
            "rating": _as_float(statistics.get("rating")),
            "rating_original": _as_float(rating_versions.get("original")) if rating_versions is not None else None,
            "rating_alternative": _as_float(rating_versions.get("alternative")) if rating_versions is not None else None,
            "statistics_type": _as_str(statistics_type.get("statisticsType")) if statistics_type is not None else None,
            "sport_slug": _as_str(statistics_type.get("sportSlug")) if statistics_type is not None else None,
            "extra_json": _as_mapping(payload.get("extra")),
        }

        stat_rows: list[Mapping[str, object]] = []
        for stat_name, stat_value in statistics.items():
            if stat_name in {"rating", "ratingVersions", "statisticsType"}:
                continue
            numeric_value = _as_float(stat_value)
            text_value = _as_scalar_text(stat_value)
            json_value = _as_mapping(stat_value) if isinstance(stat_value, Mapping) else None
            if numeric_value is None and text_value is None and json_value is None:
                continue
            stat_rows.append(
                {
                    "event_id": event_id,
                    "player_id": player_id,
                    "stat_name": stat_name,
                    "stat_value_numeric": numeric_value,
                    "stat_value_text": text_value,
                    "stat_value_json": json_value,
                }
            )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED,
            entity_upserts=extract_entities(snapshot.payload),
            metric_rows={
                "event_player_statistics": (summary_row,),
                "event_player_stat_value": tuple(stat_rows),
            },
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


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_scalar_text(value: object) -> str | None:
    if value is None or isinstance(value, (list, tuple, dict, Mapping)):
        return None
    return str(value)
