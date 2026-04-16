"""Parser for tennis power snapshots."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class TennisPowerParser:
    parser_family = "tennis_power"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        rankings = _as_mapping(payload.get("tennisPowerRankings")) or {}
        rows: list[Mapping[str, object]] = []
        for side_name in ("home", "away"):
            side = _as_mapping(rankings.get(side_name))
            if side is None:
                continue
            rows.append(
                {
                    "event_id": snapshot.context_event_id or snapshot.context_entity_id,
                    "side": side_name,
                    "current": side.get("current"),
                    "delta": side.get("delta"),
                }
            )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if rows else PARSE_STATUS_PARSED_EMPTY,
            metric_rows={"tennis_power": tuple(rows)} if rows else {},
            observed_root_keys=snapshot.observed_root_keys,
        )


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None
