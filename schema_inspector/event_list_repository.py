"""PostgreSQL repository for event-list ETL bundles."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Protocol

from .event_list_parser import EventListBundle
from .services.surface_correction_detector import SurfaceEventState
from .storage.bulk_write_helpers import RowsBatch, sorted_upsert


class SqlExecutor(Protocol):
    async def executemany(self, command: str, args: Iterable[tuple[Any, ...]]) -> Any: ...
    async def fetch(self, command: str, *args) -> Any: ...


@dataclass(frozen=True)
class EventListWriteResult:
    endpoint_registry_rows: int
    payload_snapshot_rows: int
    sport_rows: int
    country_rows: int
    category_rows: int
    team_rows: int
    unique_tournament_rows: int
    season_rows: int
    tournament_rows: int
    event_status_rows: int
    event_rows: int
    event_round_info_rows: int
    event_status_time_rows: int
    event_time_rows: int
    event_var_in_progress_rows: int
    event_score_rows: int
    event_filter_value_rows: int
    event_change_item_rows: int


class EventListRepository:
    """Writes normalized event-list data into PostgreSQL."""

    async def load_surface_states(self, executor: SqlExecutor, event_ids: Iterable[int]) -> dict[int, SurfaceEventState]:
        resolved_event_ids = tuple(sorted({int(item) for item in event_ids}))
        if not resolved_event_ids:
            return {}

        rows = await executor.fetch(
            """
            SELECT id, status_code, winner_code, aggregated_winner_code
            FROM event
            WHERE id = ANY($1::bigint[])
            """,
            resolved_event_ids,
        )
        if not rows:
            return {}

        states: dict[int, dict[str, Any]] = {
            int(row["id"]): {
                "event_id": int(row["id"]),
                "status_code": row["status_code"],
                "winner_code": row["winner_code"],
                "aggregated_winner_code": row["aggregated_winner_code"],
                "home_score": None,
                "away_score": None,
                "var_home": None,
                "var_away": None,
                "change_timestamp": None,
                "changes": (),
            }
            for row in rows
        }

        score_rows = await executor.fetch(
            """
            SELECT event_id, side, current
            FROM event_score
            WHERE event_id = ANY($1::bigint[])
            """,
            resolved_event_ids,
        )
        for row in score_rows:
            state = states.get(int(row["event_id"]))
            if state is None:
                continue
            if str(row["side"]) == "home":
                state["home_score"] = row["current"]
            elif str(row["side"]) == "away":
                state["away_score"] = row["current"]

        var_rows = await executor.fetch(
            """
            SELECT event_id, home_team, away_team
            FROM event_var_in_progress
            WHERE event_id = ANY($1::bigint[])
            """,
            resolved_event_ids,
        )
        for row in var_rows:
            state = states.get(int(row["event_id"]))
            if state is None:
                continue
            state["var_home"] = row["home_team"]
            state["var_away"] = row["away_team"]

        change_rows = await executor.fetch(
            """
            SELECT event_id, change_timestamp, ordinal, change_value
            FROM event_change_item
            WHERE event_id = ANY($1::bigint[])
            ORDER BY event_id, ordinal
            """,
            resolved_event_ids,
        )
        grouped_changes: dict[int, list[tuple[int, str, int | None]]] = {}
        for row in change_rows:
            grouped_changes.setdefault(int(row["event_id"]), []).append(
                (int(row["ordinal"]), str(row["change_value"]), row["change_timestamp"])
            )
        for event_id, values in grouped_changes.items():
            state = states.get(int(event_id))
            if state is None:
                continue
            state["change_timestamp"] = next((timestamp for _, _, timestamp in values if timestamp is not None), None)
            state["changes"] = tuple(change_value for _, change_value, _ in values)

        return {event_id: SurfaceEventState(**state) for event_id, state in states.items()}

    async def upsert_bundle(self, executor: SqlExecutor, bundle: EventListBundle) -> EventListWriteResult:
        await self._upsert_endpoint_registry(executor, bundle)
        await self._upsert_sports(executor, bundle)
        await self._upsert_countries(executor, bundle)
        await self._upsert_categories(executor, bundle)
        await self._upsert_unique_tournaments(executor, bundle)
        await self._upsert_seasons(executor, bundle)
        await self._upsert_tournaments(executor, bundle)
        await self._upsert_teams(executor, bundle)
        await self._upsert_event_statuses(executor, bundle)
        await self._upsert_events(executor, bundle)
        await self._upsert_event_round_infos(executor, bundle)
        await self._upsert_event_status_times(executor, bundle)
        await self._upsert_event_times(executor, bundle)
        await self._upsert_event_var_in_progress(executor, bundle)
        await self._upsert_event_scores(executor, bundle)
        await self._upsert_event_filter_values(executor, bundle)
        await self._upsert_event_change_items(executor, bundle)
        await self._insert_payload_snapshots(executor, bundle)

        return EventListWriteResult(
            endpoint_registry_rows=len(bundle.registry_entries),
            payload_snapshot_rows=len(bundle.payload_snapshots),
            sport_rows=len(bundle.sports),
            country_rows=len(bundle.countries),
            category_rows=len(bundle.categories),
            team_rows=len(bundle.teams),
            unique_tournament_rows=len(bundle.unique_tournaments),
            season_rows=len(bundle.seasons),
            tournament_rows=len(bundle.tournaments),
            event_status_rows=len(bundle.event_statuses),
            event_rows=len(bundle.events),
            event_round_info_rows=len(bundle.event_round_infos),
            event_status_time_rows=len(bundle.event_status_times),
            event_time_rows=len(bundle.event_times),
            event_var_in_progress_rows=len(bundle.event_var_in_progress_items),
            event_score_rows=len(bundle.event_scores),
            event_filter_value_rows=len(bundle.event_filter_values),
            event_change_item_rows=len(bundle.event_change_items),
        )

    async def _upsert_endpoint_registry(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [
            (
                item.pattern,
                item.path_template,
                item.query_template,
                item.envelope_key,
                item.target_table,
                item.notes,
            )
            for item in bundle.registry_entries
        ]
        await sorted_upsert(
            executor,
            "endpoint_registry",
            RowsBatch(
                columns=("pattern", "path_template", "query_template", "envelope_key", "target_table", "notes"),
                values=tuple(rows),
            ),
            sort_keys=("pattern",),
            conflict_target="pattern",
            update_cols=("path_template", "query_template", "envelope_key", "target_table", "notes"),
        )

    async def _upsert_sports(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.id, item.slug, item.name) for item in bundle.sports]
        await sorted_upsert(
            executor,
            "sport",
            RowsBatch(columns=("id", "slug", "name"), values=tuple(rows)),
            sort_keys=("id",),
            conflict_target="id",
            update_cols=("slug", "name"),
        )

    async def _upsert_countries(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.alpha2, item.alpha3, item.slug, item.name) for item in bundle.countries]
        await sorted_upsert(
            executor,
            "country",
            RowsBatch(columns=("alpha2", "alpha3", "slug", "name"), values=tuple(rows)),
            sort_keys=("alpha2",),
            conflict_target="alpha2",
            update_cols=("alpha3", "slug", "name"),
        )

    async def _upsert_categories(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.flag,
                item.alpha2,
                item.priority,
                item.sport_id,
                item.country_alpha2,
                _jsonb(item.field_translations),
            )
            for item in bundle.categories
        ]
        await sorted_upsert(
            executor,
            "category",
            RowsBatch(
                columns=("id", "slug", "name", "flag", "alpha2", "priority", "sport_id", "country_alpha2", "field_translations"),
                values=tuple(rows),
            ),
            sort_keys=("id",),
            conflict_target="id",
            update_cols=("slug", "name", "flag", "alpha2", "priority", "sport_id", "country_alpha2", "field_translations"),
        )

    async def _upsert_unique_tournaments(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.category_id,
                item.country_alpha2,
                item.gender,
                item.primary_color_hex,
                item.secondary_color_hex,
                item.user_count,
                item.has_event_player_statistics,
                item.display_inverse_home_away_teams,
                _jsonb(item.field_translations),
                _jsonb(item.period_length),
            )
            for item in bundle.unique_tournaments
        ]
        await sorted_upsert(
            executor,
            "unique_tournament",
            RowsBatch(
                columns=(
                    "id",
                    "slug",
                    "name",
                    "category_id",
                    "country_alpha2",
                    "gender",
                    "primary_color_hex",
                    "secondary_color_hex",
                    "user_count",
                    "has_event_player_statistics",
                    "display_inverse_home_away_teams",
                    "field_translations",
                    "period_length",
                ),
                values=tuple(rows),
            ),
            sort_keys=("id",),
            conflict_target="id",
            update_cols=(
                "slug",
                "name",
                "category_id",
                "country_alpha2",
                "gender",
                "primary_color_hex",
                "secondary_color_hex",
                "user_count",
                "has_event_player_statistics",
                "display_inverse_home_away_teams",
                "field_translations",
                "period_length",
            ),
        )

    async def _upsert_seasons(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [
            (item.id, item.name, item.year, item.editor, _jsonb(item.season_coverage_info))
            for item in bundle.seasons
        ]
        await sorted_upsert(
            executor,
            "season",
            RowsBatch(
                columns=("id", "name", "year", "editor", "season_coverage_info"),
                values=tuple(rows),
            ),
            sort_keys=("id",),
            conflict_target="id",
            update_cols=("name", "year", "editor", "season_coverage_info"),
        )

    async def _upsert_tournaments(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.category_id,
                item.unique_tournament_id,
                item.group_name,
                item.group_sign,
                item.is_group,
                item.is_live,
                item.priority,
                _jsonb(item.field_translations),
            )
            for item in bundle.tournaments
        ]
        await sorted_upsert(
            executor,
            "tournament",
            RowsBatch(
                columns=(
                    "id",
                    "slug",
                    "name",
                    "category_id",
                    "unique_tournament_id",
                    "group_name",
                    "group_sign",
                    "is_group",
                    "is_live",
                    "priority",
                    "field_translations",
                ),
                values=tuple(rows),
            ),
            sort_keys=("id",),
            conflict_target="id",
            update_cols=(
                "slug",
                "name",
                "category_id",
                "unique_tournament_id",
                "group_name",
                "group_sign",
                "is_group",
                "is_live",
                "priority",
                "field_translations",
            ),
        )

    async def _upsert_teams(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        ordered_teams = sorted(bundle.teams, key=lambda item: (item.parent_team_id is not None, item.id))
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.short_name,
                item.name_code,
                item.sport_id,
                item.country_alpha2,
                item.parent_team_id,
                item.gender,
                item.type,
                item.national,
                item.disabled,
                item.user_count,
                _jsonb(item.team_colors),
                _jsonb(item.field_translations),
            )
            for item in ordered_teams
        ]
        await _executemany(
            executor,
            """
            INSERT INTO team (
                id, slug, name, short_name, name_code, sport_id, country_alpha2,
                parent_team_id, gender, type, national, disabled, user_count,
                team_colors, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb, $15::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                short_name = EXCLUDED.short_name,
                name_code = EXCLUDED.name_code,
                sport_id = EXCLUDED.sport_id,
                country_alpha2 = EXCLUDED.country_alpha2,
                parent_team_id = EXCLUDED.parent_team_id,
                gender = EXCLUDED.gender,
                type = EXCLUDED.type,
                national = EXCLUDED.national,
                disabled = EXCLUDED.disabled,
                user_count = EXCLUDED.user_count,
                team_colors = EXCLUDED.team_colors,
                field_translations = EXCLUDED.field_translations
            """,
            rows,
        )

    async def _upsert_event_statuses(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.code, item.description, item.type) for item in bundle.event_statuses]
        await sorted_upsert(
            executor,
            "event_status",
            RowsBatch(columns=("code", "description", "type"), values=tuple(rows)),
            sort_keys=("code",),
            conflict_target="code",
            update_cols=("description", "type"),
        )

    async def _upsert_events(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.custom_id,
                item.detail_id,
                item.tournament_id,
                item.unique_tournament_id,
                item.season_id,
                item.home_team_id,
                item.away_team_id,
                item.status_code,
                item.season_statistics_type,
                item.start_timestamp,
                item.coverage,
                item.winner_code,
                item.aggregated_winner_code,
                item.home_red_cards,
                item.away_red_cards,
                item.previous_leg_event_id,
                item.cup_matches_in_round,
                item.default_period_count,
                item.default_period_length,
                item.default_overtime_length,
                item.last_period,
                item.correct_ai_insight,
                item.correct_halftime_ai_insight,
                item.feed_locked,
                item.is_editor,
                item.show_toto_promo,
                item.crowdsourcing_enabled,
                item.crowdsourcing_data_display_enabled,
                item.final_result_only,
                item.has_event_player_statistics,
                item.has_event_player_heat_map,
                item.has_global_highlights,
                item.has_xg,
            )
            for item in bundle.events
        ]
        await sorted_upsert(
            executor,
            "event",
            RowsBatch(
                columns=(
                    "id",
                    "slug",
                    "custom_id",
                    "detail_id",
                    "tournament_id",
                    "unique_tournament_id",
                    "season_id",
                    "home_team_id",
                    "away_team_id",
                    "status_code",
                    "season_statistics_type",
                    "start_timestamp",
                    "coverage",
                    "winner_code",
                    "aggregated_winner_code",
                    "home_red_cards",
                    "away_red_cards",
                    "previous_leg_event_id",
                    "cup_matches_in_round",
                    "default_period_count",
                    "default_period_length",
                    "default_overtime_length",
                    "last_period",
                    "correct_ai_insight",
                    "correct_halftime_ai_insight",
                    "feed_locked",
                    "is_editor",
                    "show_toto_promo",
                    "crowdsourcing_enabled",
                    "crowdsourcing_data_display_enabled",
                    "final_result_only",
                    "has_event_player_statistics",
                    "has_event_player_heat_map",
                    "has_global_highlights",
                    "has_xg",
                ),
                values=tuple(rows),
            ),
            sort_keys=("id",),
            conflict_target="id",
            update_cols=(
                "slug",
                "custom_id",
                "detail_id",
                "tournament_id",
                "unique_tournament_id",
                "season_id",
                "home_team_id",
                "away_team_id",
                "status_code",
                "season_statistics_type",
                "start_timestamp",
                "coverage",
                "winner_code",
                "aggregated_winner_code",
                "home_red_cards",
                "away_red_cards",
                "previous_leg_event_id",
                "cup_matches_in_round",
                "default_period_count",
                "default_period_length",
                "default_overtime_length",
                "last_period",
                "correct_ai_insight",
                "correct_halftime_ai_insight",
                "feed_locked",
                "is_editor",
                "show_toto_promo",
                "crowdsourcing_enabled",
                "crowdsourcing_data_display_enabled",
                "final_result_only",
                "has_event_player_statistics",
                "has_event_player_heat_map",
                "has_global_highlights",
                "has_xg",
            ),
        )

    async def _upsert_event_round_infos(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.event_id, item.round_number, item.slug, item.name, item.cup_round_type) for item in bundle.event_round_infos]
        await sorted_upsert(
            executor,
            "event_round_info",
            RowsBatch(
                columns=("event_id", "round_number", "slug", "name", "cup_round_type"),
                values=tuple(rows),
            ),
            sort_keys=("event_id",),
            conflict_target="event_id",
            update_cols=("round_number", "slug", "name", "cup_round_type"),
        )

    async def _upsert_event_status_times(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.event_id, item.prefix, item.timestamp, item.initial, item.max, item.extra) for item in bundle.event_status_times]
        await sorted_upsert(
            executor,
            "event_status_time",
            RowsBatch(
                columns=("event_id", "prefix", "timestamp", "initial", "max", "extra"),
                values=tuple(rows),
            ),
            sort_keys=("event_id",),
            conflict_target="event_id",
            update_cols=("prefix", "timestamp", "initial", "max", "extra"),
        )

    async def _upsert_event_times(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [
            (
                item.event_id,
                item.current_period_start_timestamp,
                item.initial,
                item.max,
                item.extra,
                item.injury_time1,
                item.injury_time2,
                item.injury_time3,
                item.injury_time4,
                item.overtime_length,
                item.period_length,
                item.total_period_count,
            )
            for item in bundle.event_times
        ]
        await sorted_upsert(
            executor,
            "event_time",
            RowsBatch(
                columns=(
                    "event_id",
                    "current_period_start_timestamp",
                    "initial",
                    "max",
                    "extra",
                    "injury_time1",
                    "injury_time2",
                    "injury_time3",
                    "injury_time4",
                    "overtime_length",
                    "period_length",
                    "total_period_count",
                ),
                values=tuple(rows),
            ),
            sort_keys=("event_id",),
            conflict_target="event_id",
            update_cols=(
                "current_period_start_timestamp",
                "initial",
                "max",
                "extra",
                "injury_time1",
                "injury_time2",
                "injury_time3",
                "injury_time4",
                "overtime_length",
                "period_length",
                "total_period_count",
            ),
        )

    async def _upsert_event_var_in_progress(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.event_id, item.home_team, item.away_team) for item in bundle.event_var_in_progress_items]
        await sorted_upsert(
            executor,
            "event_var_in_progress",
            RowsBatch(columns=("event_id", "home_team", "away_team"), values=tuple(rows)),
            sort_keys=("event_id",),
            conflict_target="event_id",
            update_cols=("home_team", "away_team"),
        )

    async def _upsert_event_scores(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [
            (
                item.event_id, item.side, item.current, item.display, item.aggregated,
                item.normaltime, item.overtime, item.penalties, item.period1, item.period2,
                item.period3, item.period4, item.extra1, item.extra2, item.series,
            )
            for item in bundle.event_scores
        ]
        await sorted_upsert(
            executor,
            "event_score",
            RowsBatch(
                columns=(
                    "event_id",
                    "side",
                    "current",
                    "display",
                    "aggregated",
                    "normaltime",
                    "overtime",
                    "penalties",
                    "period1",
                    "period2",
                    "period3",
                    "period4",
                    "extra1",
                    "extra2",
                    "series",
                ),
                values=tuple(rows),
            ),
            sort_keys=("event_id", "side"),
            conflict_target=("event_id", "side"),
            update_cols=(
                "current",
                "display",
                "aggregated",
                "normaltime",
                "overtime",
                "penalties",
                "period1",
                "period2",
                "period3",
                "period4",
                "extra1",
                "extra2",
                "series",
            ),
        )

    async def _upsert_event_filter_values(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.event_id, item.filter_name, item.ordinal, item.filter_value) for item in bundle.event_filter_values]
        await sorted_upsert(
            executor,
            "event_filter_value",
            RowsBatch(
                columns=("event_id", "filter_name", "ordinal", "filter_value"),
                values=tuple(rows),
            ),
            sort_keys=("event_id", "filter_name", "ordinal"),
            conflict_target=("event_id", "filter_name", "ordinal"),
            update_cols=("filter_value",),
        )

    async def _upsert_event_change_items(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.event_id, item.change_timestamp, item.ordinal, item.change_value) for item in bundle.event_change_items]
        await sorted_upsert(
            executor,
            "event_change_item",
            RowsBatch(
                columns=("event_id", "change_timestamp", "ordinal", "change_value"),
                values=tuple(rows),
            ),
            sort_keys=("event_id", "ordinal"),
            conflict_target=("event_id", "ordinal"),
            update_cols=("change_timestamp", "change_value"),
        )

    async def _insert_payload_snapshots(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [
            (
                item.endpoint_pattern,
                item.source_url,
                item.envelope_key,
                item.context_entity_type,
                item.context_entity_id,
                _jsonb(item.payload),
                _timestamp(item.fetched_at),
            )
            for item in bundle.payload_snapshots
        ]
        await _executemany(
            executor,
            """
            INSERT INTO api_payload_snapshot (
                endpoint_pattern, source_url, envelope_key, context_entity_type, context_entity_id, payload, fetched_at
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::timestamptz)
            """,
            rows,
        )


async def _executemany(executor: SqlExecutor, sql: str, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return
    await executor.executemany(sql, rows)


def _jsonb(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _timestamp(value: str | datetime | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)
