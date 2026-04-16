"""Family parser for `/event/{id}/statistics` payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot
from ..entities import extract_entities


class EventStatisticsParser:
    parser_family = "event_statistics"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        statistics_blocks = payload.get("statistics")
        rows: list[Mapping[str, object]] = []
        if isinstance(statistics_blocks, (list, tuple)):
            for block in statistics_blocks:
                block_mapping = _as_mapping(block)
                if block_mapping is None:
                    continue
                period = _as_str(block_mapping.get("period")) or _as_str(block_mapping.get("periodName"))
                groups = block_mapping.get("groups")
                if not isinstance(groups, (list, tuple)):
                    continue
                for group in groups:
                    group_mapping = _as_mapping(group)
                    if group_mapping is None:
                        continue
                    group_name = _as_str(group_mapping.get("groupName")) or _as_str(group_mapping.get("name"))
                    items = group_mapping.get("statisticsItems")
                    if not isinstance(items, (list, tuple)):
                        continue
                    for item in items:
                        item_mapping = _as_mapping(item)
                        if item_mapping is None:
                            continue
                        rows.append(
                            {
                                "event_id": snapshot.context_event_id or snapshot.context_entity_id,
                                "period": period,
                                "group_name": group_name,
                                "name": _as_str(item_mapping.get("name")),
                                "home_value": item_mapping.get("home"),
                                "away_value": item_mapping.get("away"),
                                "compare_code": item_mapping.get("compareCode"),
                                "statistics_type": _as_str(item_mapping.get("statisticsType")),
                            }
                        )

        status = PARSE_STATUS_PARSED if rows else PARSE_STATUS_PARSED_EMPTY
        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=status,
            entity_upserts=extract_entities(snapshot.payload),
            metric_rows={"event_statistic": tuple(rows)} if rows else {},
            observed_root_keys=snapshot.observed_root_keys,
        )


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None
