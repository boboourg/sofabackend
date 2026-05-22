"""Family parser for ``/unique-tournament/{ut}/seasons`` payloads.

Emits one ``tournament_season_upstream_catalog`` metric row per season
in the upstream response. Position 0 = newest (matches Sofascore's
natural ``/seasons`` ordering). The catalog is the source of truth for
the historical-backfill cursor walk; see Phase 2.A migration
``2026-05-22_tournament_season_upstream_catalog.sql``.

The parser DOES NOT touch ``bootstrap_state``. State transitions are
driven by separate writers (event-list-job → ``events_loaded``,
cursor advance → ``fully_processed``). Re-ingest of the same /seasons
snapshot must not regress an already-processed row, so the persist
layer uses ``INSERT … ON CONFLICT … DO UPDATE`` with the state column
explicitly excluded.
"""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class UpstreamSeasonCatalogParser:
    parser_family = "tournament_season_upstream_catalog"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = snapshot.payload if isinstance(snapshot.payload, Mapping) else {}
        seasons = payload.get("seasons")
        rows: list[Mapping[str, object]] = []
        ut_id = snapshot.context_entity_id or snapshot.context_unique_tournament_id
        if isinstance(seasons, (list, tuple)) and ut_id is not None:
            for position, item in enumerate(seasons):
                if not isinstance(item, Mapping):
                    continue
                season_id = item.get("id")
                if not isinstance(season_id, int):
                    continue
                rows.append(
                    {
                        "unique_tournament_id": int(ut_id),
                        "season_id": int(season_id),
                        "season_name": _as_str(item.get("name")),
                        "season_year": _as_str(item.get("year")),
                        "upstream_position": position,
                    }
                )
        status = PARSE_STATUS_PARSED if rows else PARSE_STATUS_PARSED_EMPTY
        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=status,
            entity_upserts={},
            metric_rows=(
                {"tournament_season_upstream_catalog": tuple(rows)} if rows else {}
            ),
            observed_root_keys=snapshot.observed_root_keys,
        )


def _as_str(value: object) -> str | None:
    if isinstance(value, str):
        return value or None
    if value is None:
        return None
    return str(value)
