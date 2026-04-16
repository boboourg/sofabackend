"""Durable sink repository for normalized hybrid parse results."""

from __future__ import annotations

import json
from typing import Any, Mapping, Protocol

from ..parsers.base import ParseResult


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...

    async def executemany(self, query: str, args: list[tuple[object, ...]]) -> Any: ...


class NormalizeRepository:
    """Persists normalized parser output into PostgreSQL fact tables."""

    async def persist_parse_result(self, executor: SqlExecutor, result: ParseResult) -> None:
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
        )
        await self._replace_event_rows(
            executor,
            "tennis_power",
            result.metric_rows.get("tennis_power", ()),
            ("event_id", "side", "current_value_numeric", "current_value_text", "delta_value_numeric", "delta_value_text"),
            row_mapper=lambda row: (
                row.get("event_id"),
                row.get("side"),
                _as_float(row.get("current")),
                _as_scalar_text(row.get("current")),
                _as_float(row.get("delta")),
                _as_scalar_text(row.get("delta")),
            ),
        )
        await self._replace_event_rows(
            executor,
            "baseball_inning",
            result.metric_rows.get("baseball_inning", ()),
            ("event_id", "ordinal", "inning", "home_score", "away_score"),
        )
        await self._replace_event_rows(
            executor,
            "shotmap_point",
            result.metric_rows.get("shotmap_point", ()),
            ("event_id", "ordinal", "x", "y", "shot_type"),
        )
        await self._replace_event_rows(
            executor,
            "esports_game",
            result.metric_rows.get("esports_game", ()),
            ("event_id", "ordinal", "game_id", "status", "map_name"),
        )

    async def _upsert_minimal_entities(
        self,
        executor: SqlExecutor,
        entity_upserts: dict[str, tuple[Mapping[str, object], ...]],
    ) -> dict[str, set[int]]:
        inserted: dict[str, set[int]] = {
            "season": set(),
            "venue": set(),
            "manager": set(),
            "team": set(),
            "player": set(),
            "event": set(),
        }

        season_rows = [
            (row.get("id"), row.get("name"), row.get("year"), row.get("editor"))
            for row in entity_upserts.get("season", ())
            if row.get("id") is not None
        ]
        if season_rows:
            await _executemany(
                executor,
                """
                INSERT INTO season (id, name, year, editor)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    year = EXCLUDED.year,
                    editor = EXCLUDED.editor
                """,
                season_rows,
            )
            inserted["season"].update(int(row[0]) for row in season_rows if row[0] is not None)

        venue_rows = [
            (row.get("id"), row.get("slug"), row.get("name"))
            for row in entity_upserts.get("venue", ())
            if row.get("id") is not None and row.get("name")
        ]
        if venue_rows:
            await _executemany(
                executor,
                """
                INSERT INTO venue (id, slug, name)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO UPDATE SET
                    slug = EXCLUDED.slug,
                    name = EXCLUDED.name
                """,
                venue_rows,
            )
            inserted["venue"].update(int(row[0]) for row in venue_rows if row[0] is not None)

        manager_rows = [
            (row.get("id"), row.get("slug"), row.get("name"), row.get("short_name"), None)
            for row in entity_upserts.get("manager", ())
            if row.get("id") is not None and row.get("name")
        ]
        if manager_rows:
            await _executemany(
                executor,
                """
                INSERT INTO manager (id, slug, name, short_name, team_id)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (id) DO UPDATE SET
                    slug = EXCLUDED.slug,
                    name = EXCLUDED.name,
                    short_name = EXCLUDED.short_name,
                    team_id = EXCLUDED.team_id
                """,
                manager_rows,
            )
            inserted["manager"].update(int(row[0]) for row in manager_rows if row[0] is not None)

        team_rows = []
        for row in entity_upserts.get("team", ()):
            team_id = row.get("id")
            if team_id is None or not row.get("slug") or not row.get("name"):
                continue
            manager_id = row.get("manager_id")
            if manager_id not in inserted["manager"]:
                manager_id = None
            venue_id = row.get("venue_id")
            if venue_id not in inserted["venue"]:
                venue_id = None
            team_rows.append((team_id, row.get("slug"), row.get("name"), row.get("short_name"), manager_id, venue_id))
        if team_rows:
            await _executemany(
                executor,
                """
                INSERT INTO team (id, slug, name, short_name, manager_id, venue_id)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (id) DO UPDATE SET
                    slug = EXCLUDED.slug,
                    name = EXCLUDED.name,
                    short_name = EXCLUDED.short_name,
                    manager_id = EXCLUDED.manager_id,
                    venue_id = EXCLUDED.venue_id
                """,
                team_rows,
            )
            inserted["team"].update(int(row[0]) for row in team_rows if row[0] is not None)

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
            await _executemany(
                executor,
                """
                INSERT INTO player (id, slug, name, short_name, team_id)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (id) DO UPDATE SET
                    slug = EXCLUDED.slug,
                    name = EXCLUDED.name,
                    short_name = EXCLUDED.short_name,
                    team_id = EXCLUDED.team_id
                """,
                player_rows,
            )
            inserted["player"].update(int(row[0]) for row in player_rows if row[0] is not None)

        event_rows = []
        for row in entity_upserts.get("event", ()):
            event_id = row.get("id")
            if event_id is None:
                continue
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
            event_rows.append((event_id, row.get("slug"), season_id, home_team_id, away_team_id, venue_id, row.get("start_timestamp")))
        if event_rows:
            await _executemany(
                executor,
                """
                INSERT INTO event (id, slug, season_id, home_team_id, away_team_id, venue_id, start_timestamp)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (id) DO UPDATE SET
                    slug = EXCLUDED.slug,
                    season_id = EXCLUDED.season_id,
                    home_team_id = EXCLUDED.home_team_id,
                    away_team_id = EXCLUDED.away_team_id,
                    venue_id = EXCLUDED.venue_id,
                    start_timestamp = EXCLUDED.start_timestamp
                """,
                event_rows,
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
        await executor.execute("DELETE FROM event_lineup WHERE event_id = $1", event_id)
        await _executemany(
            executor,
            """
            INSERT INTO event_lineup (event_id, side, formation)
            VALUES ($1, $2, $3)
            """,
            [(row.get("event_id"), row.get("side"), row.get("formation")) for row in side_rows],
        )
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
        await executor.execute("DELETE FROM event_statistic WHERE event_id = $1", event_id)
        await _executemany(
            executor,
            """
            INSERT INTO event_statistic (
                event_id, period, group_name, stat_name,
                home_value_numeric, home_value_text, home_value_json,
                away_value_numeric, away_value_text, away_value_json,
                compare_code, statistics_type
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10::jsonb, $11, $12)
            """,
            [
                (
                    row.get("event_id"),
                    row.get("period"),
                    row.get("group_name"),
                    row.get("name"),
                    _as_float(row.get("home_value")),
                    _as_scalar_text(row.get("home_value")),
                    _jsonb(_as_mapping(row.get("home_value")) if isinstance(row.get("home_value"), Mapping) else None),
                    _as_float(row.get("away_value")),
                    _as_scalar_text(row.get("away_value")),
                    _jsonb(_as_mapping(row.get("away_value")) if isinstance(row.get("away_value"), Mapping) else None),
                    row.get("compare_code"),
                    row.get("statistics_type"),
                )
                for row in rows
            ],
        )

    async def _persist_event_incidents(self, executor: SqlExecutor, rows: tuple[Mapping[str, object], ...]) -> None:
        if not rows:
            return
        event_id = _event_id_from_rows(rows)
        if event_id is None:
            return
        await executor.execute("DELETE FROM event_incident WHERE event_id = $1", event_id)
        await _executemany(
            executor,
            """
            INSERT INTO event_incident (
                event_id, ordinal, incident_id, incident_type,
                minute, home_score_text, away_score_text, text_value
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            [
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
            ],
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
            await executor.execute(
                """
                INSERT INTO event_graph (event_id, period_time, period_count, overtime_length)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (event_id) DO UPDATE SET
                    period_time = EXCLUDED.period_time,
                    period_count = EXCLUDED.period_count,
                    overtime_length = EXCLUDED.overtime_length
                """,
                row.get("event_id"),
                row.get("period_time"),
                row.get("period_count"),
                row.get("overtime_length"),
            )
        await executor.execute("DELETE FROM event_graph_point WHERE event_id = $1", event_id)
        await _executemany(
            executor,
            """
            INSERT INTO event_graph_point (event_id, ordinal, minute, value)
            VALUES ($1, $2, $3, $4)
            """,
            [(row.get("event_id"), row.get("ordinal"), row.get("minute"), row.get("value")) for row in point_rows],
        )

    async def _persist_best_players(self, executor: SqlExecutor, rows: tuple[Mapping[str, object], ...]) -> None:
        if not rows:
            return
        event_id = _event_id_from_rows(rows)
        if event_id is None:
            return
        await executor.execute("DELETE FROM event_best_player_entry WHERE event_id = $1", event_id)
        await _executemany(
            executor,
            """
            INSERT INTO event_best_player_entry (
                event_id, bucket, ordinal, player_id, label, value_text, value_numeric, is_player_of_the_match
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            [
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
            ],
        )

    async def _persist_event_player_statistics(
        self,
        executor: SqlExecutor,
        rows: tuple[Mapping[str, object], ...],
    ) -> None:
        if not rows:
            return
        await _executemany(
            executor,
            """
            INSERT INTO event_player_statistics (
                event_id, player_id, team_id, position, rating,
                rating_original, rating_alternative, statistics_type, sport_slug, extra_json
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
            ON CONFLICT (event_id, player_id) DO UPDATE SET
                team_id = EXCLUDED.team_id,
                position = EXCLUDED.position,
                rating = EXCLUDED.rating,
                rating_original = EXCLUDED.rating_original,
                rating_alternative = EXCLUDED.rating_alternative,
                statistics_type = EXCLUDED.statistics_type,
                sport_slug = EXCLUDED.sport_slug,
                extra_json = EXCLUDED.extra_json
            """,
            [
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
            ],
        )

    async def _persist_event_player_stat_values(
        self,
        executor: SqlExecutor,
        rows: tuple[Mapping[str, object], ...],
    ) -> None:
        if not rows:
            return
        seen_pairs = sorted(
            {
                (row.get("event_id"), row.get("player_id"))
                for row in rows
                if row.get("event_id") is not None and row.get("player_id") is not None
            }
        )
        for event_id, player_id in seen_pairs:
            await executor.execute(
                "DELETE FROM event_player_stat_value WHERE event_id = $1 AND player_id = $2",
                event_id,
                player_id,
            )
        await _executemany(
            executor,
            """
            INSERT INTO event_player_stat_value (
                event_id, player_id, stat_name, stat_value_numeric, stat_value_text, stat_value_json
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            """,
            [
                (
                    row.get("event_id"),
                    row.get("player_id"),
                    row.get("stat_name"),
                    row.get("stat_value_numeric"),
                    row.get("stat_value_text"),
                    _jsonb(row.get("stat_value_json")),
                )
                for row in rows
            ],
        )

    async def _persist_event_player_rating_breakdown(
        self,
        executor: SqlExecutor,
        rows: tuple[Mapping[str, object], ...],
    ) -> None:
        if not rows:
            return
        seen_pairs = sorted(
            {
                (row.get("event_id"), row.get("player_id"))
                for row in rows
                if row.get("event_id") is not None and row.get("player_id") is not None
            }
        )
        for event_id, player_id in seen_pairs:
            await executor.execute(
                "DELETE FROM event_player_rating_breakdown_action WHERE event_id = $1 AND player_id = $2",
                event_id,
                player_id,
            )
        await _executemany(
            executor,
            """
            INSERT INTO event_player_rating_breakdown_action (
                event_id, player_id, action_group, ordinal, event_action_type,
                is_home, keypass, outcome, start_x, start_y, end_x, end_y
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
            [
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
            ],
        )

    async def _replace_event_rows(
        self,
        executor: SqlExecutor,
        table_name: str,
        rows: tuple[Mapping[str, object], ...],
        columns: tuple[str, ...],
        *,
        row_mapper=None,
    ) -> None:
        if not rows:
            return
        event_id = _event_id_from_rows(rows)
        if event_id is None:
            return
        await executor.execute(f"DELETE FROM {table_name} WHERE event_id = $1", event_id)
        mapper = row_mapper or (lambda row: tuple(row.get(column) for column in columns))
        placeholders = ", ".join(f"${index}" for index in range(1, len(columns) + 1))
        await _executemany(
            executor,
            f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
            [tuple(mapper(row)) for row in rows],
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
    await executor.executemany(sql, rows)


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
