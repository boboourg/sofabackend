"""Family parser for season cup tree snapshots."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot
from ..entities import extract_entities


class SeasonCupTreesParser:
    parser_family = "season_cuptrees"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        unique_tournament_id = _as_int(snapshot.context_unique_tournament_id)
        season_id = _as_int(snapshot.context_season_id or snapshot.context_entity_id)
        if unique_tournament_id is None or season_id is None:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_PARSED_EMPTY,
                warnings=("Missing unique_tournament_id or season_id context for season cuptrees snapshot.",),
            )

        cup_tree_rows: list[Mapping[str, object]] = []
        round_rows: list[Mapping[str, object]] = []
        block_rows: list[Mapping[str, object]] = []
        participant_rows: list[Mapping[str, object]] = []

        for tree in _iter_mappings(payload.get("cupTrees")):
            cup_tree_id = _as_int(tree.get("id"))
            if cup_tree_id is None:
                continue
            tournament = _as_mapping(tree.get("tournament"))
            tournament_id = _as_int(tournament.get("id")) if tournament is not None else None
            cup_tree_rows.append(
                {
                    "cup_tree_id": cup_tree_id,
                    "unique_tournament_id": unique_tournament_id,
                    "season_id": season_id,
                    "tournament_id": tournament_id,
                    "name": _as_str(tree.get("name")),
                    "current_round": _as_int(tree.get("currentRound")),
                }
            )

            for round_item in _iter_mappings(tree.get("rounds")):
                round_order = _as_int(round_item.get("order"))
                if round_order is None:
                    continue
                round_rows.append(
                    {
                        "cup_tree_id": cup_tree_id,
                        "round_order": round_order,
                        "round_type": _as_int(round_item.get("type")),
                        "description": _as_str(round_item.get("description")),
                    }
                )

                for block in _iter_mappings(round_item.get("blocks")):
                    entry_id = _as_int(block.get("id")) or _as_int(block.get("blockId"))
                    if entry_id is None:
                        continue
                    event_ids = sorted(
                        {
                            event_id
                            for event_id in (_as_int(item) for item in _iter_scalars(block.get("events")))
                            if event_id is not None
                        }
                    )
                    block_rows.append(
                        {
                            "entry_id": entry_id,
                            "cup_tree_id": cup_tree_id,
                            "round_order": round_order,
                            "block_id": _as_int(block.get("blockId")),
                            "block_order": _as_int(block.get("order")),
                            "finished": _as_bool(block.get("finished")),
                            "matches_in_round": _as_int(block.get("matchesInRound")),
                            "result": _as_str(block.get("result")),
                            "home_team_score": _as_str(block.get("homeTeamScore")),
                            "away_team_score": _as_str(block.get("awayTeamScore")),
                            "has_next_round_link": _as_bool(block.get("hasNextRoundLink")),
                            "series_start_date_timestamp": _as_int(block.get("seriesStartDateTimestamp")),
                            "automatic_progression": _as_bool(block.get("automaticProgression")),
                            "event_ids_json": event_ids,
                        }
                    )
                    for participant in _iter_mappings(block.get("participants")):
                        participant_id = _as_int(participant.get("id"))
                        if participant_id is None:
                            continue
                        team = _as_mapping(participant.get("team"))
                        participant_rows.append(
                            {
                                "participant_id": participant_id,
                                "entry_id": entry_id,
                                "team_id": _as_int(team.get("id")) if team is not None else None,
                                "order_value": _as_int(participant.get("order")),
                                "winner": _as_bool(participant.get("winner")),
                            }
                        )

        metric_rows: dict[str, tuple[Mapping[str, object], ...]] = {}
        if cup_tree_rows:
            metric_rows["season_cup_tree"] = tuple(sorted(cup_tree_rows, key=lambda row: int(row["cup_tree_id"])))
        if round_rows:
            metric_rows["season_cup_tree_round"] = tuple(
                sorted(round_rows, key=lambda row: (int(row["cup_tree_id"]), int(row["round_order"])))
            )
        if block_rows:
            metric_rows["season_cup_tree_block"] = tuple(
                sorted(block_rows, key=lambda row: (int(row["cup_tree_id"]), int(row["round_order"]), int(row["entry_id"])))
            )
        if participant_rows:
            metric_rows["season_cup_tree_participant"] = tuple(
                sorted(participant_rows, key=lambda row: (int(row["entry_id"]), int(row["participant_id"])))
            )

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


def _iter_mappings(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _iter_scalars(value: object) -> tuple[object, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if not isinstance(item, Mapping))


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
