"""Family parser for ``/unique-tournament/{ut}/season/{s}/standings/{scope}``.

Stage 3.2 (2026-05-20 historical layer). Until this commit the
classifier returned ``"season_standings"`` for the pattern but the
registry had no parser, so every snapshot fell into
``PARSE_STATUS_UNSUPPORTED`` and the bytes were dropped on the floor.

The parser is narrow-purpose by design: it extracts the champion (the
first row of the ``total`` standings) and emits a single metric row
keyed by ``(unique_tournament_id, season_id)``. The full
``standing`` + ``standing_row`` ingest path remains the batch
``standings_repository`` stack — duplicating it here would re-implement
several hundred lines of upsert logic. The champion row alone closes
the audit-B gap that ``unique_tournament.title_holder_team_id`` is
overwritten on every upsert and the per-season chain of past champions
was nowhere persisted.

Output schema:
    metric_rows["unique_tournament_season_champion"] = (
        {
            "unique_tournament_id": int,
            "season_id": int,
            "team_id": int,
            "ordinal": 1,
            "source": "standings",
        },
    )
"""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class SeasonStandingsParser:
    parser_family = "season_standings"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        unique_tournament_id = _as_int(snapshot.context_unique_tournament_id)
        season_id = _as_int(snapshot.context_season_id)
        if unique_tournament_id is None or season_id is None:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_PARSED_EMPTY,
                warnings=(
                    "season_standings: missing unique_tournament_id or "
                    "season_id context — cannot key the champion row.",
                ),
            )

        champion_team_id = _extract_total_standings_champion(payload)
        if champion_team_id is None:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_PARSED_EMPTY,
            )

        row: dict[str, object] = {
            "unique_tournament_id": unique_tournament_id,
            "season_id": season_id,
            "team_id": champion_team_id,
            "ordinal": 1,
            "source": "standings",
        }
        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED,
            metric_rows={"unique_tournament_season_champion": (row,)},
            observed_root_keys=snapshot.observed_root_keys,
        )


def _extract_total_standings_champion(payload: Mapping[str, Any]) -> int | None:
    standings = payload.get("standings")
    if not isinstance(standings, list):
        return None
    for block in standings:
        if not isinstance(block, Mapping):
            continue
        if str(block.get("type") or "").strip().lower() != "total":
            continue
        rows = block.get("rows")
        if not isinstance(rows, list) or not rows:
            continue
        first = rows[0]
        if not isinstance(first, Mapping):
            continue
        # Position is informational only — Sofascore sorts the rows.
        # We anchor on rows[0] explicitly so a payload that omits the
        # ``position`` field still resolves.
        team = first.get("team")
        if not isinstance(team, Mapping):
            continue
        team_id = _as_int(team.get("id"))
        if team_id is not None:
            return team_id
    return None


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
