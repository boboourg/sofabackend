"""Family parser for `/event/{id}/votes` payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class EventVotesParser:
    parser_family = "event_votes"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        event_id = snapshot.context_event_id or snapshot.context_entity_id
        rows: list[Mapping[str, object]] = []

        for vote_type, vote_payload in payload.items():
            vote_mapping = _as_mapping(vote_payload)
            if vote_mapping is None or event_id is None:
                continue
            for option_name, vote_count in vote_mapping.items():
                count = _as_int(vote_count)
                if count is None:
                    continue
                rows.append(
                    {
                        "event_id": event_id,
                        "vote_type": str(vote_type),
                        "option_name": str(option_name),
                        "vote_count": count,
                    }
                )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if rows else PARSE_STATUS_PARSED_EMPTY,
            metric_rows={"event_vote_option": tuple(rows)} if rows else {},
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
