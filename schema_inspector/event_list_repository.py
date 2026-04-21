"""PostgreSQL repository for event-list ETL bundles."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Protocol

from .event_list_parser import EventListBundle
from .services.surface_correction_detector import SurfaceEventState
from .storage.raw_repository import RawRepository


_RAW_REPOSITORY = RawRepository()


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

    async def load_surface_states_for_bundle(
        self,
        executor: SqlExecutor,
        bundle: EventListBundle,
    ) -> dict[int, SurfaceEventState]:
        incoming_events = tuple(bundle.events)
        exact_states = await self.load_surface_states(executor, (int(item.id) for item in incoming_events))
        resolved_states = {
            int(item.id): exact_states[int(item.id)]
            for item in incoming_events
            if int(item.id) in exact_states
        }
        unmatched_events = tuple(
            item for item in incoming_events if int(item.id) not in resolved_states and item.custom_id is not None
        )
        if not unmatched_events:
            return resolved_states

        candidate_rows = await self._load_identity_candidate_rows(executor, unmatched_events)
        if not candidate_rows:
            return resolved_states

        candidates_by_custom_id: dict[str, list[dict[str, Any]]] = {}
        for row in candidate_rows:
            custom_id = row.get("custom_id")
            if custom_id is None:
                continue
            candidates_by_custom_id.setdefault(str(custom_id), []).append(dict(row))

        bundle_sport_ids = self._bundle_event_sport_ids(bundle)
        matched_state_ids: set[int] = set()
        matched_previous_ids: dict[int, int] = {}
        for item in unmatched_events:
            matched_row = self._select_identity_candidate(
                item=item,
                bundle_sport_id=bundle_sport_ids.get(int(item.id)),
                candidates=candidates_by_custom_id.get(str(item.custom_id), ()),
            )
            if matched_row is None:
                continue
            matched_event_id = int(matched_row["id"])
            matched_previous_ids[int(item.id)] = matched_event_id
            matched_state_ids.add(matched_event_id)

        if not matched_state_ids:
            return resolved_states

        candidate_states = await self.load_surface_states(executor, tuple(sorted(matched_state_ids)))
        for incoming_event_id, matched_event_id in matched_previous_ids.items():
            state = candidate_states.get(matched_event_id)
            if state is not None:
                resolved_states[incoming_event_id] = state
        return resolved_states

    async def _load_identity_candidate_rows(
        self,
        executor: SqlExecutor,
        events: Iterable[Any],
    ) -> tuple[dict[str, Any], ...]:
        custom_ids = tuple(sorted({str(item.custom_id) for item in events if item.custom_id is not None}))
        if not custom_ids:
            return ()
        rows = await executor.fetch(
            """
            SELECT
                e.id,
                e.custom_id,
                e.start_timestamp,
                e.home_team_id,
                e.away_team_id,
                s.id AS sport_id
            FROM event AS e
            LEFT JOIN unique_tournament AS ut ON ut.id = e.unique_tournament_id
            LEFT JOIN category AS c ON c.id = ut.category_id
            LEFT JOIN sport AS s ON s.id = c.sport_id
            WHERE e.custom_id = ANY($1::text[])
            """,
            custom_ids,
        )
        return tuple(dict(row) for row in rows)

    def _bundle_event_sport_ids(self, bundle: EventListBundle) -> dict[int, int]:
        category_sport_ids = {
            int(item.id): int(item.sport_id)
            for item in bundle.categories
            if item.id is not None and item.sport_id is not None
        }
        unique_tournament_category_ids = {
            int(item.id): int(item.category_id)
            for item in bundle.unique_tournaments
            if item.id is not None and item.category_id is not None
        }
        tournament_category_ids = {
            int(item.id): int(item.category_id)
            for item in bundle.tournaments
            if item.id is not None and item.category_id is not None
        }
        event_sport_ids: dict[int, int] = {}
        for item in bundle.events:
            category_id = None
            if item.unique_tournament_id is not None:
                category_id = unique_tournament_category_ids.get(int(item.unique_tournament_id))
            if category_id is None and item.tournament_id is not None:
                category_id = tournament_category_ids.get(int(item.tournament_id))
            sport_id = None if category_id is None else category_sport_ids.get(int(category_id))
            if sport_id is not None:
                event_sport_ids[int(item.id)] = int(sport_id)
        return event_sport_ids

    def _select_identity_candidate(
        self,
        *,
        item: Any,
        bundle_sport_id: int | None,
        candidates: Iterable[dict[str, Any]],
    ) -> dict[str, Any] | None:
        best_row: dict[str, Any] | None = None
        best_key: tuple[int, int] | None = None
        for row in candidates:
            if not self._identity_candidate_matches(item=item, bundle_sport_id=bundle_sport_id, row=row):
                continue
            score = self._identity_candidate_score(item=item, bundle_sport_id=bundle_sport_id, row=row)
            candidate_key = (score, int(row["id"]))
            if best_key is None or candidate_key > best_key:
                best_key = candidate_key
                best_row = row
        return best_row

    def _identity_candidate_matches(
        self,
        *,
        item: Any,
        bundle_sport_id: int | None,
        row: dict[str, Any],
    ) -> bool:
        if item.custom_id is None or str(row.get("custom_id")) != str(item.custom_id):
            return False
        row_sport_id = row.get("sport_id")
        if bundle_sport_id is not None and row_sport_id is not None and int(row_sport_id) != int(bundle_sport_id):
            return False
        for field_name in ("start_timestamp", "home_team_id", "away_team_id"):
            incoming_value = getattr(item, field_name, None)
            existing_value = row.get(field_name)
            if incoming_value is not None and existing_value is not None and int(existing_value) != int(incoming_value):
                return False
        return True

    def _identity_candidate_score(
        self,
        *,
        item: Any,
        bundle_sport_id: int | None,
        row: dict[str, Any],
    ) -> int:
        score = 0
        row_sport_id = row.get("sport_id")
        if bundle_sport_id is not None and row_sport_id is not None and int(row_sport_id) == int(bundle_sport_id):
            score += 1
        for field_name in ("start_timestamp", "home_team_id", "away_team_id"):
            incoming_value = getattr(item, field_name, None)
            existing_value = row.get(field_name)
            if incoming_value is not None and existing_value is not None and int(existing_value) == int(incoming_value):
                score += 1
        return score

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
        await _RAW_REPOSITORY.upsert_endpoint_registry_entries(executor, bundle.registry_entries)

    async def _upsert_sports(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.id, item.slug, item.name) for item in bundle.sports]
        await _executemany(
            executor,
            """
            INSERT INTO sport (id, slug, name)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO UPDATE SET slug = EXCLUDED.slug, name = EXCLUDED.name
            """,
            rows,
        )

    async def _upsert_countries(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.alpha2, item.alpha3, item.slug, item.name) for item in bundle.countries]
        await _executemany(
            executor,
            """
            INSERT INTO country (alpha2, alpha3, slug, name)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (alpha2) DO UPDATE SET
                alpha3 = EXCLUDED.alpha3,
                slug = EXCLUDED.slug,
                name = EXCLUDED.name
            """,
            rows,
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
        await _executemany(
            executor,
            """
            INSERT INTO category (
                id, slug, name, flag, alpha2, priority, sport_id, country_alpha2, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                flag = EXCLUDED.flag,
                alpha2 = EXCLUDED.alpha2,
                priority = EXCLUDED.priority,
                sport_id = EXCLUDED.sport_id,
                country_alpha2 = EXCLUDED.country_alpha2,
                field_translations = EXCLUDED.field_translations
            """,
            rows,
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
        await _executemany(
            executor,
            """
            INSERT INTO unique_tournament (
                id, slug, name, category_id, country_alpha2, gender,
                primary_color_hex, secondary_color_hex, user_count, has_event_player_statistics,
                display_inverse_home_away_teams, field_translations, period_length
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                category_id = EXCLUDED.category_id,
                country_alpha2 = EXCLUDED.country_alpha2,
                gender = EXCLUDED.gender,
                primary_color_hex = EXCLUDED.primary_color_hex,
                secondary_color_hex = EXCLUDED.secondary_color_hex,
                user_count = EXCLUDED.user_count,
                has_event_player_statistics = EXCLUDED.has_event_player_statistics,
                display_inverse_home_away_teams = EXCLUDED.display_inverse_home_away_teams,
                field_translations = EXCLUDED.field_translations,
                period_length = EXCLUDED.period_length
            """,
            rows,
        )

    async def _upsert_seasons(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [
            (item.id, item.name, item.year, item.editor, _jsonb(item.season_coverage_info))
            for item in bundle.seasons
        ]
        await _executemany(
            executor,
            """
            INSERT INTO season (id, name, year, editor, season_coverage_info)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                year = EXCLUDED.year,
                editor = EXCLUDED.editor,
                season_coverage_info = EXCLUDED.season_coverage_info
            """,
            rows,
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
        await _executemany(
            executor,
            """
            INSERT INTO tournament (
                id, slug, name, category_id, unique_tournament_id,
                group_name, group_sign, is_group, is_live, priority, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                category_id = EXCLUDED.category_id,
                unique_tournament_id = EXCLUDED.unique_tournament_id,
                group_name = EXCLUDED.group_name,
                group_sign = EXCLUDED.group_sign,
                is_group = EXCLUDED.is_group,
                is_live = EXCLUDED.is_live,
                priority = EXCLUDED.priority,
                field_translations = EXCLUDED.field_translations
            """,
            rows,
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
        await _executemany(
            executor,
            """
            INSERT INTO event_status (code, description, type)
            VALUES ($1, $2, $3)
            ON CONFLICT (code) DO UPDATE SET
                description = EXCLUDED.description,
                type = EXCLUDED.type
            """,
            rows,
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
        await _executemany(
            executor,
            """
            INSERT INTO event (
                id, slug, custom_id, detail_id, tournament_id, unique_tournament_id, season_id,
                home_team_id, away_team_id, status_code, season_statistics_type, start_timestamp,
                coverage, winner_code, aggregated_winner_code, home_red_cards, away_red_cards,
                previous_leg_event_id, cup_matches_in_round, default_period_count,
                default_period_length, default_overtime_length, last_period,
                correct_ai_insight, correct_halftime_ai_insight, feed_locked, is_editor,
                show_toto_promo, crowdsourcing_enabled, crowdsourcing_data_display_enabled,
                final_result_only, has_event_player_statistics, has_event_player_heat_map,
                has_global_highlights, has_xg
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35
            )
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                custom_id = EXCLUDED.custom_id,
                detail_id = EXCLUDED.detail_id,
                tournament_id = EXCLUDED.tournament_id,
                unique_tournament_id = EXCLUDED.unique_tournament_id,
                season_id = EXCLUDED.season_id,
                home_team_id = EXCLUDED.home_team_id,
                away_team_id = EXCLUDED.away_team_id,
                status_code = EXCLUDED.status_code,
                season_statistics_type = EXCLUDED.season_statistics_type,
                start_timestamp = EXCLUDED.start_timestamp,
                coverage = EXCLUDED.coverage,
                winner_code = EXCLUDED.winner_code,
                aggregated_winner_code = EXCLUDED.aggregated_winner_code,
                home_red_cards = EXCLUDED.home_red_cards,
                away_red_cards = EXCLUDED.away_red_cards,
                previous_leg_event_id = EXCLUDED.previous_leg_event_id,
                cup_matches_in_round = EXCLUDED.cup_matches_in_round,
                default_period_count = EXCLUDED.default_period_count,
                default_period_length = EXCLUDED.default_period_length,
                default_overtime_length = EXCLUDED.default_overtime_length,
                last_period = EXCLUDED.last_period,
                correct_ai_insight = EXCLUDED.correct_ai_insight,
                correct_halftime_ai_insight = EXCLUDED.correct_halftime_ai_insight,
                feed_locked = EXCLUDED.feed_locked,
                is_editor = EXCLUDED.is_editor,
                show_toto_promo = EXCLUDED.show_toto_promo,
                crowdsourcing_enabled = EXCLUDED.crowdsourcing_enabled,
                crowdsourcing_data_display_enabled = EXCLUDED.crowdsourcing_data_display_enabled,
                final_result_only = EXCLUDED.final_result_only,
                has_event_player_statistics = EXCLUDED.has_event_player_statistics,
                has_event_player_heat_map = EXCLUDED.has_event_player_heat_map,
                has_global_highlights = EXCLUDED.has_global_highlights,
                has_xg = EXCLUDED.has_xg
            """,
            rows,
        )

    async def _upsert_event_round_infos(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.event_id, item.round_number, item.slug, item.name, item.cup_round_type) for item in bundle.event_round_infos]
        await _executemany(
            executor,
            """
            INSERT INTO event_round_info (event_id, round_number, slug, name, cup_round_type)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (event_id) DO UPDATE SET
                round_number = EXCLUDED.round_number,
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                cup_round_type = EXCLUDED.cup_round_type
            """,
            rows,
        )

    async def _upsert_event_status_times(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.event_id, item.prefix, item.timestamp, item.initial, item.max, item.extra) for item in bundle.event_status_times]
        await _executemany(
            executor,
            """
            INSERT INTO event_status_time (event_id, prefix, timestamp, initial, max, extra)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (event_id) DO UPDATE SET
                prefix = EXCLUDED.prefix,
                timestamp = EXCLUDED.timestamp,
                initial = EXCLUDED.initial,
                max = EXCLUDED.max,
                extra = EXCLUDED.extra
            """,
            rows,
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
        await _executemany(
            executor,
            """
            INSERT INTO event_time (
                event_id, current_period_start_timestamp, initial, max, extra,
                injury_time1, injury_time2, injury_time3, injury_time4,
                overtime_length, period_length, total_period_count
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (event_id) DO UPDATE SET
                current_period_start_timestamp = EXCLUDED.current_period_start_timestamp,
                initial = EXCLUDED.initial,
                max = EXCLUDED.max,
                extra = EXCLUDED.extra,
                injury_time1 = EXCLUDED.injury_time1,
                injury_time2 = EXCLUDED.injury_time2,
                injury_time3 = EXCLUDED.injury_time3,
                injury_time4 = EXCLUDED.injury_time4,
                overtime_length = EXCLUDED.overtime_length,
                period_length = EXCLUDED.period_length,
                total_period_count = EXCLUDED.total_period_count
            """,
            rows,
        )

    async def _upsert_event_var_in_progress(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.event_id, item.home_team, item.away_team) for item in bundle.event_var_in_progress_items]
        await _executemany(
            executor,
            """
            INSERT INTO event_var_in_progress (event_id, home_team, away_team)
            VALUES ($1, $2, $3)
            ON CONFLICT (event_id) DO UPDATE SET
                home_team = EXCLUDED.home_team,
                away_team = EXCLUDED.away_team
            """,
            rows,
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
        await _executemany(
            executor,
            """
            INSERT INTO event_score (
                event_id, side, current, display, aggregated, normaltime, overtime,
                penalties, period1, period2, period3, period4, extra1, extra2, series
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (event_id, side) DO UPDATE SET
                current = EXCLUDED.current,
                display = EXCLUDED.display,
                aggregated = EXCLUDED.aggregated,
                normaltime = EXCLUDED.normaltime,
                overtime = EXCLUDED.overtime,
                penalties = EXCLUDED.penalties,
                period1 = EXCLUDED.period1,
                period2 = EXCLUDED.period2,
                period3 = EXCLUDED.period3,
                period4 = EXCLUDED.period4,
                extra1 = EXCLUDED.extra1,
                extra2 = EXCLUDED.extra2,
                series = EXCLUDED.series
            """,
            rows,
        )

    async def _upsert_event_filter_values(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.event_id, item.filter_name, item.ordinal, item.filter_value) for item in bundle.event_filter_values]
        await _executemany(
            executor,
            """
            INSERT INTO event_filter_value (event_id, filter_name, ordinal, filter_value)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (event_id, filter_name, ordinal) DO UPDATE SET
                filter_value = EXCLUDED.filter_value
            """,
            rows,
        )

    async def _upsert_event_change_items(self, executor: SqlExecutor, bundle: EventListBundle) -> None:
        rows = [(item.event_id, item.change_timestamp, item.ordinal, item.change_value) for item in bundle.event_change_items]
        await _executemany(
            executor,
            """
            INSERT INTO event_change_item (event_id, change_timestamp, ordinal, change_value)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (event_id, ordinal) DO UPDATE SET
                change_timestamp = EXCLUDED.change_timestamp,
                change_value = EXCLUDED.change_value
            """,
            rows,
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
