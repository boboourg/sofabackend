"""Durable sink repository for normalized hybrid parse results."""

from __future__ import annotations

import json
from typing import Any, Mapping, Protocol

from ..parsers.base import ParseResult
from .bulk_write_helpers import RowsBatch, sorted_delete_insert, sorted_upsert

_EXECUTEMANY_BATCH_SIZE = 100
_CACHEABLE_MINIMAL_ENTITY_KINDS = (
    "sport",
    "country",
    "category",
    "unique_tournament",
    "tournament",
    "season",
    "venue",
    "manager",
    "team",
)


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...

    async def executemany(self, query: str, args: list[tuple[object, ...]]) -> Any: ...


class NormalizeRepository:
    """Persists normalized parser output into PostgreSQL fact tables."""

    def __init__(self) -> None:
        self._known_minimal_entities: dict[str, set[object]] = {
            entity_kind: set() for entity_kind in _CACHEABLE_MINIMAL_ENTITY_KINDS
        }

    async def persist_parse_result(
        self,
        executor: SqlExecutor,
        result: ParseResult,
        *,
        skip_entity_upserts: bool = False,
    ) -> None:
        inserted = _empty_inserted_tracker()
        if not skip_entity_upserts:
            inserted = await self._upsert_minimal_entities(executor, result.entity_upserts)
        await self._persist_lineups(executor, result, inserted)
        await self._persist_event_statistics(executor, result.metric_rows.get("event_statistic", ()))
        await self._persist_event_incidents(executor, result.metric_rows.get("event_incident", ()))
        await self._persist_event_graph(executor, result.metric_rows)
        await self._persist_best_players(executor, result.metric_rows.get("event_best_player_entry", ()))
        await self._persist_event_player_statistics(executor, result.metric_rows.get("event_player_statistics", ()))
        await self._persist_event_player_stat_values(executor, result.metric_rows.get("event_player_stat_value", ()))
        await self._persist_event_player_rating_breakdown(
            executor,
            result.metric_rows.get("event_player_rating_breakdown_action", ()),
        )
        await self._replace_event_rows(
            executor,
            "tennis_point_by_point",
            result.metric_rows.get("tennis_point_by_point", ()),
            (
                "event_id",
                "ordinal",
                "point_id",
                "set_number",
                "game_number",
                "server",
                "home_score",
                "away_score",
            ),
            sort_keys=("event_id", "ordinal"),
        )
        await self._replace_event_rows(
            executor,
            "tennis_power",
            result.metric_rows.get("tennis_power", ()),
            ("event_id", "ordinal", "set_number", "game_number", "value_numeric", "value_text", "break_occurred"),
            row_mapper=lambda row: (
                row.get("event_id"),
                row.get("ordinal"),
                row.get("set_number"),
                row.get("game_number"),
                _as_float(row.get("value", row.get("current"))),
                _as_scalar_text(row.get("value", row.get("current"))),
                row.get("break_occurred"),
            ),
            sort_keys=("event_id", "ordinal"),
        )
        await self._replace_event_rows(
            executor,
            "baseball_inning",
            result.metric_rows.get("baseball_inning", ()),
            ("event_id", "ordinal", "inning", "home_score", "away_score"),
            sort_keys=("event_id", "ordinal"),
        )
        await self._persist_baseball_pitches(executor, result.metric_rows.get("baseball_pitch", ()))
        await self._replace_event_rows(
            executor,
            "shotmap_point",
            result.metric_rows.get("shotmap_point", ()),
            ("event_id", "ordinal", "x", "y", "shot_type"),
            sort_keys=("event_id", "ordinal"),
        )
        await self._replace_event_rows(
            executor,
            "esports_game",
            result.metric_rows.get("esports_game", ()),
            ("event_id", "ordinal", "game_id", "status", "map_name"),
            sort_keys=("event_id", "ordinal"),
        )

    async def _upsert_minimal_entities(
        self,
        executor: SqlExecutor,
        entity_upserts: dict[str, tuple[Mapping[str, object], ...]],
    ) -> dict[str, set[int]]:
        inserted: dict[str, set[int]] = {
            "sport": set(),
            "country": set(),
            "category": set(),
            "unique_tournament": set(),
            "tournament": set(),
            "season": set(),
            "venue": set(),
            "manager": set(),
            "team": set(),
            "player": set(),
            "event": set(),
        }
        for entity_kind, known_entities in self._known_minimal_entities.items():
            inserted[entity_kind].update(known_entities)

        sport_rows = [
            (row.get("id"), row.get("slug"), row.get("name"))
            for row in entity_upserts.get("sport", ())
            if (
                row.get("id") is not None
                and row.get("slug")
                and row.get("name")
                and row.get("id") not in self._known_minimal_entities["sport"]
            )
        ]
        if sport_rows:
            await sorted_upsert(
                executor,
                "sport",
                RowsBatch(
                    columns=("id", "slug", "name"),
                    values=tuple(sport_rows),
                ),
                sort_keys=("id",),
                conflict_target="id",
                update_cols=(),
            )
            sport_ids = {int(row[0]) for row in sport_rows if row[0] is not None}
            inserted["sport"].update(sport_ids)
            self._known_minimal_entities["sport"].update(sport_ids)

        country_rows = [
            (row.get("alpha2"), row.get("alpha3"), row.get("slug"), row.get("name"))
            for row in entity_upserts.get("country", ())
            if (
                row.get("alpha2") is not None
                and row.get("name")
                and row.get("alpha2") not in self._known_minimal_entities["country"]
            )
        ]
        if country_rows:
            await sorted_upsert(
                executor,
                "country",
                RowsBatch(
                    columns=("alpha2", "alpha3", "slug", "name"),
                    values=tuple(country_rows),
                ),
                sort_keys=("alpha2",),
                conflict_target="alpha2",
                update_cols=(),
            )
            country_codes = {str(row[0]) for row in country_rows if row[0] is not None}
            inserted["country"].update(country_codes)
            self._known_minimal_entities["country"].update(country_codes)

        category_rows = []
        for row in entity_upserts.get("category", ()):
            category_id = row.get("id")
            sport_id = row.get("sport_id")
            if category_id is None or not row.get("slug") or not row.get("name") or sport_id not in inserted["sport"]:
                continue
            if category_id in self._known_minimal_entities["category"]:
                continue
            country_alpha2 = row.get("country_alpha2")
            if country_alpha2 not in inserted["country"]:
                country_alpha2 = None
            category_rows.append(
                (
                    category_id,
                    row.get("slug"),
                    row.get("name"),
                    sport_id,
                    country_alpha2,
                )
            )
        if category_rows:
            await sorted_upsert(
                executor,
                "category",
                RowsBatch(
                    columns=("id", "slug", "name", "sport_id", "country_alpha2"),
                    values=tuple(category_rows),
                ),
                sort_keys=("id",),
                conflict_target="id",
                update_cols=(),
            )
            category_ids = {int(row[0]) for row in category_rows if row[0] is not None}
            inserted["category"].update(category_ids)
            self._known_minimal_entities["category"].update(category_ids)

        unique_tournament_rows = []
        for row in entity_upserts.get("unique_tournament", ()):
            unique_tournament_id = row.get("id")
            category_id = row.get("category_id")
            if (
                unique_tournament_id is None
                or not row.get("slug")
                or not row.get("name")
                or category_id not in inserted["category"]
            ):
                continue
            if unique_tournament_id in self._known_minimal_entities["unique_tournament"]:
                continue
            country_alpha2 = row.get("country_alpha2")
            if country_alpha2 not in inserted["country"]:
                country_alpha2 = None
            unique_tournament_rows.append(
                (
                    unique_tournament_id,
                    row.get("slug"),
                    row.get("name"),
                    category_id,
                    country_alpha2,
                )
            )
        if unique_tournament_rows:
            await sorted_upsert(
                executor,
                "unique_tournament",
                RowsBatch(
                    columns=("id", "slug", "name", "category_id", "country_alpha2"),
                    values=tuple(unique_tournament_rows),
                ),
                sort_keys=("id",),
                conflict_target="id",
                update_cols=(),
            )
            unique_tournament_ids = {int(row[0]) for row in unique_tournament_rows if row[0] is not None}
            inserted["unique_tournament"].update(unique_tournament_ids)
            self._known_minimal_entities["unique_tournament"].update(unique_tournament_ids)

        season_rows = [
            (row.get("id"), row.get("name"), row.get("year"), row.get("editor"))
            for row in entity_upserts.get("season", ())
            if row.get("id") is not None and row.get("id") not in self._known_minimal_entities["season"]
        ]
        if season_rows:
            await sorted_upsert(
                executor,
                "season",
                RowsBatch(
                    columns=("id", "name", "year", "editor"),
                    values=tuple(season_rows),
                ),
                sort_keys=("id",),
                conflict_target="id",
                update_cols=(),
            )
            season_ids = {int(row[0]) for row in season_rows if row[0] is not None}
            inserted["season"].update(season_ids)
            self._known_minimal_entities["season"].update(season_ids)

        tournament_rows = []
        for row in entity_upserts.get("tournament", ()):
            tournament_id = row.get("id")
            category_id = row.get("category_id")
            unique_tournament_id = row.get("unique_tournament_id")
            if tournament_id is None or not row.get("name"):
                continue
            if tournament_id in self._known_minimal_entities["tournament"]:
                continue
            if category_id not in inserted["category"]:
                continue
            if unique_tournament_id is not None and unique_tournament_id not in inserted["unique_tournament"]:
                unique_tournament_id = None
            tournament_rows.append(
                (
                    tournament_id,
                    row.get("slug"),
                    row.get("name"),
                    category_id,
                    unique_tournament_id,
                )
            )
        if tournament_rows:
            await sorted_upsert(
                executor,
                "tournament",
                RowsBatch(
                    columns=("id", "slug", "name", "category_id", "unique_tournament_id"),
                    values=tuple(tournament_rows),
                ),
                sort_keys=("id",),
                conflict_target="id",
                update_cols=(),
            )
            tournament_ids = {int(row[0]) for row in tournament_rows if row[0] is not None}
            inserted["tournament"].update(tournament_ids)
            self._known_minimal_entities["tournament"].update(tournament_ids)

        venue_rows = [
            (
                row.get("id"),
                row.get("slug"),
                row.get("name"),
                row.get("country_alpha2") if row.get("country_alpha2") in inserted["country"] else None,
            )
            for row in entity_upserts.get("venue", ())
            if (
                row.get("id") is not None
                and row.get("name")
                and row.get("id") not in self._known_minimal_entities["venue"]
            )
        ]
        if venue_rows:
            await sorted_upsert(
                executor,
                "venue",
                RowsBatch(
                    columns=("id", "slug", "name", "country_alpha2"),
                    values=tuple(venue_rows),
                ),
                sort_keys=("id",),
                conflict_target="id",
                update_cols=(),
            )
            venue_ids = {int(row[0]) for row in venue_rows if row[0] is not None}
            inserted["venue"].update(venue_ids)
            self._known_minimal_entities["venue"].update(venue_ids)

        manager_rows = [
            (row.get("id"), row.get("slug"), row.get("name"), row.get("short_name"), None)
            for row in entity_upserts.get("manager", ())
            if (
                row.get("id") is not None
                and row.get("name")
                and row.get("id") not in self._known_minimal_entities["manager"]
            )
        ]
        if manager_rows:
            await sorted_upsert(
                executor,
                "manager",
                RowsBatch(
                    columns=("id", "slug", "name", "short_name", "team_id"),
                    values=tuple(manager_rows),
                ),
                sort_keys=("id",),
                conflict_target="id",
                update_cols=("slug", "name", "short_name", "team_id"),
            )
            manager_ids = {int(row[0]) for row in manager_rows if row[0] is not None}
            inserted["manager"].update(manager_ids)
            self._known_minimal_entities["manager"].update(manager_ids)

        team_rows = []
        for row in entity_upserts.get("team", ()):
            team_id = row.get("id")
            if team_id is None or not row.get("slug") or not row.get("name"):
                continue
            if team_id in self._known_minimal_entities["team"]:
                continue
            sport_id = row.get("sport_id")
            if sport_id not in inserted["sport"]:
                sport_id = None
            category_id = row.get("category_id")
            if category_id not in inserted["category"]:
                category_id = None
            manager_id = row.get("manager_id")
            if manager_id not in inserted["manager"]:
                manager_id = None
            venue_id = row.get("venue_id")
            if venue_id not in inserted["venue"]:
                venue_id = None
            tournament_id = row.get("tournament_id")
            if tournament_id not in inserted["tournament"]:
                tournament_id = None
            primary_unique_tournament_id = row.get("primary_unique_tournament_id")
            if primary_unique_tournament_id not in inserted["unique_tournament"]:
                primary_unique_tournament_id = None
            parent_team_id = row.get("parent_team_id")
            team_rows.append(
                (
                    team_id,
                    row.get("slug"),
                    row.get("name"),
                    row.get("short_name"),
                    sport_id,
                    category_id,
                    row.get("country_alpha2"),
                    manager_id,
                    venue_id,
                    tournament_id,
                    primary_unique_tournament_id,
                    parent_team_id,
                )
            )
        team_rows.sort(key=lambda row: (row[11] is not None, row[11] or 0, row[0]))
        if team_rows:
            await _executemany(
                executor,
                """
                INSERT INTO team (
                    id, slug, name, short_name, sport_id, category_id, country_alpha2,
                    manager_id, venue_id, tournament_id, primary_unique_tournament_id, parent_team_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (id) DO UPDATE SET
                    slug = EXCLUDED.slug,
                    name = EXCLUDED.name,
                    short_name = COALESCE(EXCLUDED.short_name, team.short_name),
                    sport_id = COALESCE(EXCLUDED.sport_id, team.sport_id),
                    category_id = COALESCE(EXCLUDED.category_id, team.category_id),
                    country_alpha2 = COALESCE(EXCLUDED.country_alpha2, team.country_alpha2),
                    manager_id = COALESCE(EXCLUDED.manager_id, team.manager_id),
                    venue_id = COALESCE(EXCLUDED.venue_id, team.venue_id),
                    tournament_id = COALESCE(EXCLUDED.tournament_id, team.tournament_id),
                    primary_unique_tournament_id = COALESCE(
                        EXCLUDED.primary_unique_tournament_id,
                        team.primary_unique_tournament_id
                    ),
                    parent_team_id = COALESCE(EXCLUDED.parent_team_id, team.parent_team_id)
                """,
                team_rows,
            )
            team_ids = {int(row[0]) for row in team_rows if row[0] is not None}
            inserted["team"].update(team_ids)
            self._known_minimal_entities["team"].update(team_ids)

        player_rows = []
        for row in entity_upserts.get("player", ()):
            player_id = row.get("id")
            if player_id is None or not row.get("name"):
                continue
            team_id = row.get("team_id")
            if team_id not in inserted["team"]:
                team_id = None
            player_rows.append((player_id, row.get("slug"), row.get("name"), row.get("short_name"), team_id))
        if player_rows:
            await sorted_upsert(
                executor,
                "player",
                RowsBatch(
                    columns=("id", "slug", "name", "short_name", "team_id"),
                    values=tuple(player_rows),
                ),
                sort_keys=("id",),
                conflict_target="id",
                update_cols=("slug", "name", "short_name", "team_id"),
            )
            inserted["player"].update(int(row[0]) for row in player_rows if row[0] is not None)

        event_rows = []
        for row in entity_upserts.get("event", ()):
            event_id = row.get("id")
            if event_id is None:
                continue
            tournament_id = row.get("tournament_id")
            if tournament_id not in inserted["tournament"]:
                tournament_id = None
            unique_tournament_id = row.get("unique_tournament_id")
            if unique_tournament_id not in inserted["unique_tournament"]:
                unique_tournament_id = None
            season_id = row.get("season_id")
            if season_id not in inserted["season"]:
                season_id = None
            home_team_id = row.get("home_team_id")
            if home_team_id not in inserted["team"]:
                home_team_id = None
            away_team_id = row.get("away_team_id")
            if away_team_id not in inserted["team"]:
                away_team_id = None
            venue_id = row.get("venue_id")
            if venue_id not in inserted["venue"]:
                venue_id = None
            event_rows.append(
                (
                    event_id,
                    row.get("slug"),
                    tournament_id,
                    unique_tournament_id,
                    season_id,
                    home_team_id,
                    away_team_id,
                    venue_id,
                    row.get("start_timestamp"),
                )
            )
        if event_rows:
            await sorted_upsert(
                executor,
                "event",
                RowsBatch(
                    columns=(
                        "id",
                        "slug",
                        "tournament_id",
                        "unique_tournament_id",
                        "season_id",
                        "home_team_id",
                        "away_team_id",
                        "venue_id",
                        "start_timestamp",
                    ),
                    values=tuple(event_rows),
                ),
                sort_keys=("id",),
                conflict_target="id",
                update_cols={
                    "slug": 'COALESCE(EXCLUDED."slug", event."slug")',
                    "tournament_id": 'COALESCE(EXCLUDED."tournament_id", event."tournament_id")',
                    "unique_tournament_id": 'COALESCE(EXCLUDED."unique_tournament_id", event."unique_tournament_id")',
                    "season_id": 'COALESCE(EXCLUDED."season_id", event."season_id")',
                    "home_team_id": 'COALESCE(EXCLUDED."home_team_id", event."home_team_id")',
                    "away_team_id": 'COALESCE(EXCLUDED."away_team_id", event."away_team_id")',
                    "venue_id": 'COALESCE(EXCLUDED."venue_id", event."venue_id")',
                    "start_timestamp": 'EXCLUDED."start_timestamp"',
                },
            )
            inserted["event"].update(int(row[0]) for row in event_rows if row[0] is not None)

        return inserted

    async def _persist_lineups(
        self,
        executor: SqlExecutor,
        result: ParseResult,
        inserted: dict[str, set[int]],
    ) -> None:
        side_rows = result.metric_rows.get("event_lineup_side", ())
        player_rows = result.relation_upserts.get("event_lineup_player", ())
        if not side_rows and not player_rows:
            return
        event_id = _event_id_from_rows(side_rows) or _event_id_from_rows(player_rows)
        if event_id is None:
            return
        if side_rows:
            await sorted_delete_insert(
                executor,
                "event_lineup",
                RowsBatch(
                    columns=("event_id", "side", "formation"),
                    values=tuple(
                        (row.get("event_id"), row.get("side"), row.get("formation"))
                        for row in side_rows
                    ),
                ),
                delete_key="event_id",
                sort_keys=("event_id", "side"),
            )
        else:
            await executor.execute("DELETE FROM event_lineup WHERE event_id = $1", event_id)
        normalized_players = []
        for row in player_rows:
            team_id = row.get("team_id")
            if team_id not in inserted["team"]:
                team_id = None
            normalized_players.append(
                (
                    row.get("event_id"),
                    row.get("side"),
                    row.get("player_id"),
                    team_id,
                    row.get("position"),
                    row.get("substitute"),
                    _as_int(row.get("order_value")),
                    row.get("jersey_number"),
                    _as_float(row.get("avg_rating")),
                )
            )
        await _executemany(
            executor,
            """
            INSERT INTO event_lineup_player (
                event_id, side, player_id, team_id, position,
                substitute, shirt_number, jersey_number, avg_rating
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            normalized_players,
        )

    async def _persist_event_statistics(self, executor: SqlExecutor, rows: tuple[Mapping[str, object], ...]) -> None:
        if not rows:
            return
        event_id = _event_id_from_rows(rows)
        if event_id is None:
            return
        normalized_rows = []
        for row in rows:
            normalized_rows.append(
                (
                    row.get("event_id"),
                    _as_scalar_text(row.get("period")),
                    _as_scalar_text(row.get("group_name")),
                    _as_scalar_text(row.get("name")),
                    _as_float(row.get("home_value")),
                    _as_scalar_text(row.get("home_value")),
                    _jsonb(_as_mapping(row.get("home_value")) if isinstance(row.get("home_value"), Mapping) else None),
                    _as_float(row.get("away_value")),
                    _as_scalar_text(row.get("away_value")),
                    _jsonb(_as_mapping(row.get("away_value")) if isinstance(row.get("away_value"), Mapping) else None),
                    _as_scalar_text(row.get("compare_code")),
                    _as_scalar_text(row.get("statistics_type")),
                )
            )
        await sorted_delete_insert(
            executor,
            "event_statistic",
            RowsBatch(
                columns=(
                    "event_id",
                    "period",
                    "group_name",
                    "stat_name",
                    "home_value_numeric",
                    "home_value_text",
                    "home_value_json",
                    "away_value_numeric",
                    "away_value_text",
                    "away_value_json",
                    "compare_code",
                    "statistics_type",
                ),
                values=tuple(normalized_rows),
            ),
            delete_key="event_id",
            sort_keys=("event_id", "period", "group_name", "stat_name"),
        )

    async def _persist_event_incidents(self, executor: SqlExecutor, rows: tuple[Mapping[str, object], ...]) -> None:
        if not rows:
            return
        event_id = _event_id_from_rows(rows)
        if event_id is None:
            return
        await sorted_delete_insert(
            executor,
            "event_incident",
            RowsBatch(
                columns=(
                    "event_id",
                    "ordinal",
                    "incident_id",
                    "incident_type",
                    "minute",
                    "home_score_text",
                    "away_score_text",
                    "text_value",
                ),
                values=tuple(
                    (
                        row.get("event_id"),
                        row.get("ordinal"),
                        row.get("incident_id"),
                        row.get("incident_type"),
                        row.get("time"),
                        _as_scalar_text(row.get("home_score")),
                        _as_scalar_text(row.get("away_score")),
                        row.get("text"),
                    )
                    for row in rows
                ),
            ),
            delete_key="event_id",
            sort_keys=("event_id", "ordinal"),
        )

    async def _persist_event_graph(
        self,
        executor: SqlExecutor,
        metric_rows: dict[str, tuple[Mapping[str, object], ...]],
    ) -> None:
        graph_rows = metric_rows.get("event_graph", ())
        point_rows = metric_rows.get("event_graph_point", ())
        if not graph_rows and not point_rows:
            return
        event_id = _event_id_from_rows(graph_rows) or _event_id_from_rows(point_rows)
        if event_id is None:
            return
        if graph_rows:
            row = graph_rows[0]
            await sorted_upsert(
                executor,
                "event_graph",
                RowsBatch(
                    columns=("event_id", "period_time", "period_count", "overtime_length"),
                    values=(
                        (
                            row.get("event_id"),
                            row.get("period_time"),
                            row.get("period_count"),
                            row.get("overtime_length"),
                        ),
                    ),
                ),
                sort_keys=("event_id",),
                conflict_target="event_id",
                update_cols=("period_time", "period_count", "overtime_length"),
            )
        if point_rows:
            await sorted_delete_insert(
                executor,
                "event_graph_point",
                RowsBatch(
                    columns=("event_id", "ordinal", "minute", "value"),
                    values=tuple(
                        (row.get("event_id"), row.get("ordinal"), row.get("minute"), row.get("value"))
                        for row in point_rows
                    ),
                ),
                delete_key="event_id",
                sort_keys=("event_id", "ordinal"),
            )
        else:
            await executor.execute("DELETE FROM event_graph_point WHERE event_id = $1", event_id)

    async def _persist_best_players(self, executor: SqlExecutor, rows: tuple[Mapping[str, object], ...]) -> None:
        if not rows:
            return
        event_id = _event_id_from_rows(rows)
        if event_id is None:
            return
        await sorted_delete_insert(
            executor,
            "event_best_player_entry",
            RowsBatch(
                columns=(
                    "event_id",
                    "bucket",
                    "ordinal",
                    "player_id",
                    "label",
                    "value_text",
                    "value_numeric",
                    "is_player_of_the_match",
                ),
                values=tuple(
                    (
                        row.get("event_id"),
                        row.get("bucket"),
                        row.get("ordinal"),
                        row.get("player_id"),
                        row.get("label"),
                        row.get("value_text"),
                        row.get("value_numeric"),
                        row.get("is_player_of_the_match"),
                    )
                    for row in rows
                ),
            ),
            delete_key="event_id",
            sort_keys=("event_id", "bucket", "ordinal"),
        )

    async def _persist_event_player_statistics(
        self,
        executor: SqlExecutor,
        rows: tuple[Mapping[str, object], ...],
    ) -> None:
        if not rows:
            return
        await sorted_upsert(
            executor,
            "event_player_statistics",
            RowsBatch(
                columns=(
                    "event_id",
                    "player_id",
                    "team_id",
                    "position",
                    "rating",
                    "rating_original",
                    "rating_alternative",
                    "statistics_type",
                    "sport_slug",
                    "extra_json",
                ),
                values=tuple(
                    (
                        row.get("event_id"),
                        row.get("player_id"),
                        row.get("team_id"),
                        row.get("position"),
                        row.get("rating"),
                        row.get("rating_original"),
                        row.get("rating_alternative"),
                        row.get("statistics_type"),
                        row.get("sport_slug"),
                        _jsonb(row.get("extra_json")),
                    )
                    for row in rows
                ),
            ),
            sort_keys=("event_id", "player_id"),
            conflict_target=("event_id", "player_id"),
            update_cols=(
                "team_id",
                "position",
                "rating",
                "rating_original",
                "rating_alternative",
                "statistics_type",
                "sport_slug",
                "extra_json",
            ),
        )

    async def _persist_event_player_stat_values(
        self,
        executor: SqlExecutor,
        rows: tuple[Mapping[str, object], ...],
    ) -> None:
        if not rows:
            return
        await sorted_delete_insert(
            executor,
            "event_player_stat_value",
            RowsBatch(
                columns=(
                    "event_id",
                    "player_id",
                    "stat_name",
                    "stat_value_numeric",
                    "stat_value_text",
                    "stat_value_json",
                ),
                values=tuple(
                    (
                        row.get("event_id"),
                        row.get("player_id"),
                        row.get("stat_name"),
                        row.get("stat_value_numeric"),
                        row.get("stat_value_text"),
                        _jsonb(row.get("stat_value_json")),
                    )
                    for row in rows
                ),
            ),
            delete_key=("event_id", "player_id"),
            sort_keys=("event_id", "player_id", "stat_name"),
        )

    async def _persist_event_player_rating_breakdown(
        self,
        executor: SqlExecutor,
        rows: tuple[Mapping[str, object], ...],
    ) -> None:
        if not rows:
            return
        await sorted_delete_insert(
            executor,
            "event_player_rating_breakdown_action",
            RowsBatch(
                columns=(
                    "event_id",
                    "player_id",
                    "action_group",
                    "ordinal",
                    "event_action_type",
                    "is_home",
                    "keypass",
                    "outcome",
                    "start_x",
                    "start_y",
                    "end_x",
                    "end_y",
                ),
                values=tuple(
                    (
                        row.get("event_id"),
                        row.get("player_id"),
                        row.get("action_group"),
                        row.get("ordinal"),
                        row.get("event_action_type"),
                        row.get("is_home"),
                        row.get("keypass"),
                        row.get("outcome"),
                        row.get("start_x"),
                        row.get("start_y"),
                        row.get("end_x"),
                        row.get("end_y"),
                    )
                    for row in rows
                ),
            ),
            delete_key=("event_id", "player_id"),
            sort_keys=("event_id", "player_id", "action_group", "ordinal"),
        )

    async def _persist_baseball_pitches(
        self,
        executor: SqlExecutor,
        rows: tuple[Mapping[str, object], ...],
    ) -> None:
        if not rows:
            return
        await sorted_delete_insert(
            executor,
            "baseball_pitch",
            RowsBatch(
                columns=(
                    "event_id",
                    "at_bat_id",
                    "ordinal",
                    "pitch_id",
                    "pitch_speed",
                    "pitch_type",
                    "pitch_zone",
                    "pitch_x",
                    "pitch_y",
                    "mlb_x",
                    "mlb_y",
                    "outcome",
                    "pitcher_id",
                    "hitter_id",
                ),
                values=tuple(
                    (
                        row.get("event_id"),
                        row.get("at_bat_id"),
                        row.get("ordinal"),
                        row.get("pitch_id"),
                        _as_float(row.get("pitch_speed")),
                        _as_scalar_text(row.get("pitch_type")),
                        _as_scalar_text(row.get("pitch_zone")),
                        _as_float(row.get("pitch_x")),
                        _as_float(row.get("pitch_y")),
                        _as_float(row.get("mlb_x")),
                        _as_float(row.get("mlb_y")),
                        _as_scalar_text(row.get("outcome")),
                        row.get("pitcher_id"),
                        row.get("hitter_id"),
                    )
                    for row in rows
                ),
            ),
            delete_key=("event_id", "at_bat_id"),
            sort_keys=("event_id", "at_bat_id", "ordinal"),
        )

    async def _replace_event_rows(
        self,
        executor: SqlExecutor,
        table_name: str,
        rows: tuple[Mapping[str, object], ...],
        columns: tuple[str, ...],
        *,
        row_mapper=None,
        sort_keys: tuple[str, ...],
    ) -> None:
        if not rows:
            return
        event_id = _event_id_from_rows(rows)
        if event_id is None:
            return
        mapper = row_mapper or (lambda row: tuple(row.get(column) for column in columns))
        await sorted_delete_insert(
            executor,
            table_name,
            RowsBatch(
                columns=columns,
                values=tuple(tuple(mapper(row)) for row in rows),
            ),
            delete_key="event_id",
            sort_keys=sort_keys,
        )


def _event_id_from_rows(rows: tuple[Mapping[str, object], ...]) -> int | None:
    for row in rows:
        value = _as_int(row.get("event_id"))
        if value is not None:
            return value
    return None


async def _executemany(executor: SqlExecutor, sql: str, rows: list[tuple[object, ...]]) -> None:
    if not rows:
        return
    for start in range(0, len(rows), _EXECUTEMANY_BATCH_SIZE):
        await executor.executemany(sql, rows[start : start + _EXECUTEMANY_BATCH_SIZE])


def _jsonb(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _as_mapping(value: object) -> Mapping[str, object] | None:
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


def _as_scalar_text(value: object) -> str | None:
    if value is None or isinstance(value, (dict, Mapping, list, tuple)):
        return None
    return str(value)


def _empty_inserted_tracker() -> dict[str, set[int]]:
    return {
        "sport": set(),
        "country": set(),
        "category": set(),
        "unique_tournament": set(),
        "tournament": set(),
        "season": set(),
        "venue": set(),
        "manager": set(),
        "team": set(),
        "player": set(),
        "event": set(),
    }
