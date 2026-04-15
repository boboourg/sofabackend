"""PostgreSQL repository for event-detail ETL bundles."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .event_detail_parser import EventDetailBundle
from .event_list_repository import EventListRepository, SqlExecutor, _executemany, _jsonb


@dataclass(frozen=True)
class EventDetailWriteResult:
    endpoint_registry_rows: int
    payload_snapshot_rows: int
    sport_rows: int
    country_rows: int
    category_rows: int
    unique_tournament_rows: int
    season_rows: int
    tournament_rows: int
    team_rows: int
    venue_rows: int
    referee_rows: int
    manager_rows: int
    manager_performance_rows: int
    manager_team_membership_rows: int
    player_rows: int
    event_status_rows: int
    event_rows: int
    event_round_info_rows: int
    event_status_time_rows: int
    event_time_rows: int
    event_var_in_progress_rows: int
    event_score_rows: int
    event_filter_value_rows: int
    event_change_item_rows: int
    event_manager_assignment_rows: int
    event_duel_rows: int
    event_pregame_form_rows: int
    event_pregame_form_side_rows: int
    event_pregame_form_item_rows: int
    event_vote_option_rows: int
    event_comment_feed_rows: int
    event_comment_rows: int
    event_graph_rows: int
    event_graph_point_rows: int
    event_team_heatmap_rows: int
    event_team_heatmap_point_rows: int
    provider_rows: int
    provider_configuration_rows: int
    event_market_rows: int
    event_market_choice_rows: int
    event_winning_odds_rows: int
    event_lineup_rows: int
    event_lineup_player_rows: int
    event_lineup_missing_player_rows: int


class EventDetailRepository(EventListRepository):
    """Writes normalized event-detail data into PostgreSQL."""

    async def upsert_bundle(self, executor: SqlExecutor, bundle: EventDetailBundle) -> EventDetailWriteResult:
        await self._upsert_endpoint_registry(executor, bundle)
        await self._upsert_sports(executor, bundle)
        await self._upsert_countries(executor, bundle)
        await self._upsert_categories(executor, bundle)
        await self._upsert_unique_tournaments(executor, bundle)
        await self._upsert_seasons(executor, bundle)
        await self._upsert_tournaments(executor, bundle)
        await self._upsert_venues(executor, bundle)
        await self._upsert_referees(executor, bundle)
        await self._upsert_teams_base(executor, bundle)
        await self._upsert_managers(executor, bundle)
        await self._upsert_team_manager_links(executor, bundle)
        await self._upsert_manager_performances(executor, bundle)
        await self._upsert_manager_team_memberships(executor, bundle)
        await self._upsert_players(executor, bundle)
        await self._upsert_event_statuses(executor, bundle)
        await self._upsert_events(executor, bundle)
        await self._upsert_event_round_infos(executor, bundle)
        await self._upsert_event_status_times(executor, bundle)
        await self._upsert_event_times(executor, bundle)
        await self._upsert_event_var_in_progress(executor, bundle)
        await self._upsert_event_scores(executor, bundle)
        await self._upsert_event_filter_values(executor, bundle)
        await self._upsert_event_change_items(executor, bundle)
        await self._upsert_event_manager_assignments(executor, bundle)
        await self._upsert_event_duels(executor, bundle)
        await self._upsert_event_pregame_forms(executor, bundle)
        await self._upsert_event_pregame_form_sides(executor, bundle)
        await self._upsert_event_pregame_form_items(executor, bundle)
        await self._upsert_event_vote_options(executor, bundle)
        await self._upsert_event_comment_feeds(executor, bundle)
        await self._upsert_event_comments(executor, bundle)
        await self._upsert_event_graphs(executor, bundle)
        await self._upsert_event_graph_points(executor, bundle)
        await self._upsert_event_team_heatmaps(executor, bundle)
        await self._upsert_event_team_heatmap_points(executor, bundle)
        await self._upsert_providers(executor, bundle)
        await self._upsert_provider_configurations(executor, bundle)
        await self._upsert_event_markets(executor, bundle)
        await self._upsert_event_market_choices(executor, bundle)
        await self._upsert_event_winning_odds(executor, bundle)
        await self._upsert_event_lineups(executor, bundle)
        await self._upsert_event_lineup_players(executor, bundle)
        await self._upsert_event_lineup_missing_players(executor, bundle)
        await self._insert_payload_snapshots(executor, bundle)

        return EventDetailWriteResult(
            endpoint_registry_rows=len(bundle.registry_entries),
            payload_snapshot_rows=len(bundle.payload_snapshots),
            sport_rows=len(bundle.sports),
            country_rows=len(bundle.countries),
            category_rows=len(bundle.categories),
            unique_tournament_rows=len(bundle.unique_tournaments),
            season_rows=len(bundle.seasons),
            tournament_rows=len(bundle.tournaments),
            team_rows=len(bundle.teams),
            venue_rows=len(bundle.venues),
            referee_rows=len(bundle.referees),
            manager_rows=len(bundle.managers),
            manager_performance_rows=len(bundle.manager_performances),
            manager_team_membership_rows=len(bundle.manager_team_memberships),
            player_rows=len(bundle.players),
            event_status_rows=len(bundle.event_statuses),
            event_rows=len(bundle.events),
            event_round_info_rows=len(bundle.event_round_infos),
            event_status_time_rows=len(bundle.event_status_times),
            event_time_rows=len(bundle.event_times),
            event_var_in_progress_rows=len(bundle.event_var_in_progress_items),
            event_score_rows=len(bundle.event_scores),
            event_filter_value_rows=len(bundle.event_filter_values),
            event_change_item_rows=len(bundle.event_change_items),
            event_manager_assignment_rows=len(bundle.event_manager_assignments),
            event_duel_rows=len(bundle.event_duels),
            event_pregame_form_rows=len(bundle.event_pregame_forms),
            event_pregame_form_side_rows=len(bundle.event_pregame_form_sides),
            event_pregame_form_item_rows=len(bundle.event_pregame_form_items),
            event_vote_option_rows=len(bundle.event_vote_options),
            event_comment_feed_rows=len(bundle.event_comment_feeds),
            event_comment_rows=len(bundle.event_comments),
            event_graph_rows=len(bundle.event_graphs),
            event_graph_point_rows=len(bundle.event_graph_points),
            event_team_heatmap_rows=len(bundle.event_team_heatmaps),
            event_team_heatmap_point_rows=len(bundle.event_team_heatmap_points),
            provider_rows=len(bundle.providers),
            provider_configuration_rows=len(bundle.provider_configurations),
            event_market_rows=len(bundle.event_markets),
            event_market_choice_rows=len(bundle.event_market_choices),
            event_winning_odds_rows=len(bundle.event_winning_odds),
            event_lineup_rows=len(bundle.event_lineups),
            event_lineup_player_rows=len(bundle.event_lineup_players),
            event_lineup_missing_player_rows=len(bundle.event_lineup_missing_players),
        )

    async def _upsert_tournaments(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.category_id,
                item.unique_tournament_id,
                item.competition_type,
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
                id, slug, name, category_id, unique_tournament_id, competition_type,
                group_name, group_sign, is_group, is_live, priority, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                category_id = EXCLUDED.category_id,
                unique_tournament_id = EXCLUDED.unique_tournament_id,
                competition_type = EXCLUDED.competition_type,
                group_name = EXCLUDED.group_name,
                group_sign = EXCLUDED.group_sign,
                is_group = EXCLUDED.is_group,
                is_live = EXCLUDED.is_live,
                priority = EXCLUDED.priority,
                field_translations = EXCLUDED.field_translations
            """,
            rows,
        )

    async def _upsert_venues(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.capacity,
                item.hidden,
                item.country_alpha2,
                item.city_name,
                item.stadium_name,
                item.stadium_capacity,
                item.latitude,
                item.longitude,
                _jsonb(item.field_translations),
            )
            for item in bundle.venues
        ]
        await _executemany(
            executor,
            """
            INSERT INTO venue (
                id, slug, name, capacity, hidden, country_alpha2, city_name,
                stadium_name, stadium_capacity, latitude, longitude, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                capacity = EXCLUDED.capacity,
                hidden = EXCLUDED.hidden,
                country_alpha2 = EXCLUDED.country_alpha2,
                city_name = EXCLUDED.city_name,
                stadium_name = EXCLUDED.stadium_name,
                stadium_capacity = EXCLUDED.stadium_capacity,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                field_translations = EXCLUDED.field_translations
            """,
            rows,
        )

    async def _upsert_referees(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.sport_id,
                item.country_alpha2,
                item.games,
                item.yellow_cards,
                item.yellow_red_cards,
                item.red_cards,
                _jsonb(item.field_translations),
            )
            for item in bundle.referees
        ]
        await _executemany(
            executor,
            """
            INSERT INTO referee (
                id, slug, name, sport_id, country_alpha2, games,
                yellow_cards, yellow_red_cards, red_cards, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                sport_id = EXCLUDED.sport_id,
                country_alpha2 = EXCLUDED.country_alpha2,
                games = EXCLUDED.games,
                yellow_cards = EXCLUDED.yellow_cards,
                yellow_red_cards = EXCLUDED.yellow_red_cards,
                red_cards = EXCLUDED.red_cards,
                field_translations = EXCLUDED.field_translations
            """,
            rows,
        )

    async def _upsert_teams_base(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        ordered_teams = sorted(bundle.teams, key=lambda item: (item.parent_team_id is not None, item.id))
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.short_name,
                item.full_name,
                item.name_code,
                item.sport_id,
                item.category_id,
                item.country_alpha2,
                item.venue_id,
                item.tournament_id,
                item.primary_unique_tournament_id,
                item.parent_team_id,
                item.gender,
                item.type,
                item.class_value,
                item.ranking,
                item.national,
                item.disabled,
                item.foundation_date_timestamp,
                item.user_count,
                _jsonb(item.team_colors),
                _jsonb(item.field_translations),
                _jsonb(item.time_active),
            )
            for item in ordered_teams
        ]
        await _executemany(
            executor,
            """
            INSERT INTO team (
                id, slug, name, short_name, full_name, name_code, sport_id, category_id,
                country_alpha2, venue_id, tournament_id, primary_unique_tournament_id, parent_team_id,
                gender, type, class, ranking, national, disabled, foundation_date_timestamp,
                user_count, team_colors, field_translations, time_active
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22::jsonb, $23::jsonb, $24::jsonb
            )
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                short_name = EXCLUDED.short_name,
                full_name = EXCLUDED.full_name,
                name_code = EXCLUDED.name_code,
                sport_id = EXCLUDED.sport_id,
                category_id = EXCLUDED.category_id,
                country_alpha2 = EXCLUDED.country_alpha2,
                venue_id = EXCLUDED.venue_id,
                tournament_id = EXCLUDED.tournament_id,
                primary_unique_tournament_id = EXCLUDED.primary_unique_tournament_id,
                parent_team_id = EXCLUDED.parent_team_id,
                gender = EXCLUDED.gender,
                type = EXCLUDED.type,
                class = EXCLUDED.class,
                ranking = EXCLUDED.ranking,
                national = EXCLUDED.national,
                disabled = EXCLUDED.disabled,
                foundation_date_timestamp = EXCLUDED.foundation_date_timestamp,
                user_count = EXCLUDED.user_count,
                team_colors = EXCLUDED.team_colors,
                field_translations = EXCLUDED.field_translations,
                time_active = EXCLUDED.time_active
            """,
            rows,
        )

    async def _upsert_managers(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.short_name,
                item.sport_id,
                item.country_alpha2,
                item.team_id,
                item.former_player_id,
                item.nationality,
                item.nationality_iso2,
                item.date_of_birth_timestamp,
                item.deceased,
                item.preferred_formation,
                _jsonb(item.field_translations),
            )
            for item in bundle.managers
        ]
        await _executemany(
            executor,
            """
            INSERT INTO manager (
                id, slug, name, short_name, sport_id, country_alpha2, team_id,
                former_player_id, nationality, nationality_iso2, date_of_birth_timestamp,
                deceased, preferred_formation, field_translations
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb)
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                short_name = EXCLUDED.short_name,
                sport_id = EXCLUDED.sport_id,
                country_alpha2 = EXCLUDED.country_alpha2,
                team_id = EXCLUDED.team_id,
                former_player_id = EXCLUDED.former_player_id,
                nationality = EXCLUDED.nationality,
                nationality_iso2 = EXCLUDED.nationality_iso2,
                date_of_birth_timestamp = EXCLUDED.date_of_birth_timestamp,
                deceased = EXCLUDED.deceased,
                preferred_formation = EXCLUDED.preferred_formation,
                field_translations = EXCLUDED.field_translations
            """,
            rows,
        )

    async def _upsert_team_manager_links(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [(item.manager_id, item.id) for item in bundle.teams if item.manager_id is not None]
        await _executemany(
            executor,
            """
            UPDATE team
            SET manager_id = $1
            WHERE id = $2
            """,
            rows,
        )

    async def _upsert_manager_performances(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.manager_id,
                item.total,
                item.wins,
                item.draws,
                item.losses,
                item.goals_scored,
                item.goals_conceded,
                item.total_points,
            )
            for item in bundle.manager_performances
        ]
        await _executemany(
            executor,
            """
            INSERT INTO manager_performance (
                manager_id, total, wins, draws, losses, goals_scored, goals_conceded, total_points
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (manager_id) DO UPDATE SET
                total = EXCLUDED.total,
                wins = EXCLUDED.wins,
                draws = EXCLUDED.draws,
                losses = EXCLUDED.losses,
                goals_scored = EXCLUDED.goals_scored,
                goals_conceded = EXCLUDED.goals_conceded,
                total_points = EXCLUDED.total_points
            """,
            rows,
        )

    async def _upsert_manager_team_memberships(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [(item.manager_id, item.team_id) for item in bundle.manager_team_memberships]
        await _executemany(
            executor,
            """
            INSERT INTO manager_team_membership (manager_id, team_id)
            VALUES ($1, $2)
            ON CONFLICT (manager_id, team_id) DO NOTHING
            """,
            rows,
        )

    async def _upsert_players(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.short_name,
                item.first_name,
                item.last_name,
                item.team_id,
                item.country_alpha2,
                item.manager_id,
                item.gender,
                item.position,
                list(item.positions_detailed) if item.positions_detailed else None,
                item.preferred_foot,
                item.jersey_number,
                item.sofascore_id,
                item.date_of_birth,
                item.date_of_birth_timestamp,
                item.height,
                item.weight,
                item.market_value_currency,
                _jsonb(item.proposed_market_value_raw),
                item.rating,
                item.retired,
                item.deceased,
                item.user_count,
                item.order_value,
                _jsonb(item.field_translations),
            )
            for item in bundle.players
        ]
        await _executemany(
            executor,
            """
            INSERT INTO player (
                id, slug, name, short_name, first_name, last_name, team_id, country_alpha2,
                manager_id, gender, position, positions_detailed, preferred_foot, jersey_number,
                sofascore_id, date_of_birth, date_of_birth_timestamp, height, weight,
                market_value_currency, proposed_market_value_raw, rating, retired, deceased,
                user_count, order_value, field_translations
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12::text[], $13, $14, $15, $16, $17, $18, $19, $20,
                $21::jsonb, $22, $23, $24, $25, $26, $27::jsonb
            )
            ON CONFLICT (id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                short_name = EXCLUDED.short_name,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                team_id = EXCLUDED.team_id,
                country_alpha2 = EXCLUDED.country_alpha2,
                manager_id = EXCLUDED.manager_id,
                gender = EXCLUDED.gender,
                position = EXCLUDED.position,
                positions_detailed = EXCLUDED.positions_detailed,
                preferred_foot = EXCLUDED.preferred_foot,
                jersey_number = EXCLUDED.jersey_number,
                sofascore_id = EXCLUDED.sofascore_id,
                date_of_birth = EXCLUDED.date_of_birth,
                date_of_birth_timestamp = EXCLUDED.date_of_birth_timestamp,
                height = EXCLUDED.height,
                weight = EXCLUDED.weight,
                market_value_currency = EXCLUDED.market_value_currency,
                proposed_market_value_raw = EXCLUDED.proposed_market_value_raw,
                rating = EXCLUDED.rating,
                retired = EXCLUDED.retired,
                deceased = EXCLUDED.deceased,
                user_count = EXCLUDED.user_count,
                order_value = EXCLUDED.order_value,
                field_translations = EXCLUDED.field_translations
            """,
            rows,
        )

    async def _upsert_events(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
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
                item.venue_id,
                item.referee_id,
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
                home_team_id, away_team_id, venue_id, referee_id, status_code, season_statistics_type,
                start_timestamp, coverage, winner_code, aggregated_winner_code, home_red_cards,
                away_red_cards, previous_leg_event_id, cup_matches_in_round, default_period_count,
                default_period_length, default_overtime_length, last_period, correct_ai_insight,
                correct_halftime_ai_insight, feed_locked, is_editor, show_toto_promo,
                crowdsourcing_enabled, crowdsourcing_data_display_enabled, final_result_only,
                has_event_player_statistics, has_event_player_heat_map, has_global_highlights, has_xg
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35, $36, $37
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
                venue_id = EXCLUDED.venue_id,
                referee_id = EXCLUDED.referee_id,
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

    async def _upsert_event_manager_assignments(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [(item.event_id, item.side, item.manager_id) for item in bundle.event_manager_assignments]
        await _executemany(
            executor,
            """
            INSERT INTO event_manager_assignment (event_id, side, manager_id)
            VALUES ($1, $2, $3)
            ON CONFLICT (event_id, side) DO UPDATE SET
                manager_id = EXCLUDED.manager_id
            """,
            rows,
        )

    async def _upsert_event_duels(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [(item.event_id, item.duel_type, item.home_wins, item.away_wins, item.draws) for item in bundle.event_duels]
        await _executemany(
            executor,
            """
            INSERT INTO event_duel (event_id, duel_type, home_wins, away_wins, draws)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (event_id, duel_type) DO UPDATE SET
                home_wins = EXCLUDED.home_wins,
                away_wins = EXCLUDED.away_wins,
                draws = EXCLUDED.draws
            """,
            rows,
        )

    async def _upsert_event_pregame_forms(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [(item.event_id, item.label) for item in bundle.event_pregame_forms]
        await _executemany(
            executor,
            """
            INSERT INTO event_pregame_form (event_id, label)
            VALUES ($1, $2)
            ON CONFLICT (event_id) DO UPDATE SET
                label = EXCLUDED.label
            """,
            rows,
        )

    async def _upsert_event_pregame_form_sides(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [(item.event_id, item.side, item.avg_rating, item.position, item.value) for item in bundle.event_pregame_form_sides]
        await _executemany(
            executor,
            """
            INSERT INTO event_pregame_form_side (event_id, side, avg_rating, position, value)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (event_id, side) DO UPDATE SET
                avg_rating = EXCLUDED.avg_rating,
                position = EXCLUDED.position,
                value = EXCLUDED.value
            """,
            rows,
        )

    async def _upsert_event_pregame_form_items(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [(item.event_id, item.side, item.ordinal, item.form_value) for item in bundle.event_pregame_form_items]
        await _executemany(
            executor,
            """
            INSERT INTO event_pregame_form_item (event_id, side, ordinal, form_value)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (event_id, side, ordinal) DO UPDATE SET
                form_value = EXCLUDED.form_value
            """,
            rows,
        )

    async def _upsert_event_vote_options(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [(item.event_id, item.vote_type, item.option_name, item.vote_count) for item in bundle.event_vote_options]
        await _executemany(
            executor,
            """
            INSERT INTO event_vote_option (event_id, vote_type, option_name, vote_count)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (event_id, vote_type, option_name) DO UPDATE SET
                vote_count = EXCLUDED.vote_count
            """,
            rows,
        )

    async def _upsert_event_comment_feeds(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.event_id,
                _jsonb(item.home_player_color),
                _jsonb(item.home_goalkeeper_color),
                _jsonb(item.away_player_color),
                _jsonb(item.away_goalkeeper_color),
            )
            for item in bundle.event_comment_feeds
        ]
        await _executemany(
            executor,
            """
            INSERT INTO event_comment_feed (
                event_id, home_player_color, home_goalkeeper_color, away_player_color, away_goalkeeper_color
            )
            VALUES ($1, $2::jsonb, $3::jsonb, $4::jsonb, $5::jsonb)
            ON CONFLICT (event_id) DO UPDATE SET
                home_player_color = EXCLUDED.home_player_color,
                home_goalkeeper_color = EXCLUDED.home_goalkeeper_color,
                away_player_color = EXCLUDED.away_player_color,
                away_goalkeeper_color = EXCLUDED.away_goalkeeper_color
            """,
            rows,
        )

    async def _upsert_event_comments(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.event_id,
                item.comment_id,
                item.sequence,
                item.period_name,
                item.is_home,
                item.player_id,
                item.text,
                item.match_time,
                item.comment_type,
            )
            for item in bundle.event_comments
        ]
        await _executemany(
            executor,
            """
            INSERT INTO event_comment (
                event_id, comment_id, sequence, period_name, is_home, player_id, text, match_time, comment_type
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (event_id, comment_id) DO UPDATE SET
                sequence = EXCLUDED.sequence,
                period_name = EXCLUDED.period_name,
                is_home = EXCLUDED.is_home,
                player_id = EXCLUDED.player_id,
                text = EXCLUDED.text,
                match_time = EXCLUDED.match_time,
                comment_type = EXCLUDED.comment_type
            """,
            rows,
        )

    async def _upsert_event_graphs(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (item.event_id, item.period_time, item.period_count, item.overtime_length)
            for item in bundle.event_graphs
        ]
        await _executemany(
            executor,
            """
            INSERT INTO event_graph (event_id, period_time, period_count, overtime_length)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (event_id) DO UPDATE SET
                period_time = EXCLUDED.period_time,
                period_count = EXCLUDED.period_count,
                overtime_length = EXCLUDED.overtime_length
            """,
            rows,
        )

    async def _upsert_event_graph_points(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [(item.event_id, item.ordinal, item.minute, item.value) for item in bundle.event_graph_points]
        await _executemany(
            executor,
            """
            INSERT INTO event_graph_point (event_id, ordinal, minute, value)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (event_id, ordinal) DO UPDATE SET
                minute = EXCLUDED.minute,
                value = EXCLUDED.value
            """,
            rows,
        )

    async def _upsert_event_team_heatmaps(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [(item.event_id, item.team_id) for item in bundle.event_team_heatmaps]
        await _executemany(
            executor,
            """
            INSERT INTO event_team_heatmap (event_id, team_id)
            VALUES ($1, $2)
            ON CONFLICT (event_id, team_id) DO NOTHING
            """,
            rows,
        )

    async def _upsert_event_team_heatmap_points(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (item.event_id, item.team_id, item.point_type, item.ordinal, item.x, item.y)
            for item in bundle.event_team_heatmap_points
        ]
        await _executemany(
            executor,
            """
            INSERT INTO event_team_heatmap_point (event_id, team_id, point_type, ordinal, x, y)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (event_id, team_id, point_type, ordinal) DO UPDATE SET
                x = EXCLUDED.x,
                y = EXCLUDED.y
            """,
            rows,
        )

    async def _upsert_providers(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.id,
                item.slug,
                item.name,
                item.country,
                item.default_bet_slip_link,
                _jsonb(item.colors),
                item.odds_from_provider_id,
                item.live_odds_from_provider_id,
            )
            for item in bundle.providers
        ]
        await _executemany(
            executor,
            """
            INSERT INTO provider (
                id, slug, name, country, default_bet_slip_link, colors,
                odds_from_provider_id, live_odds_from_provider_id
            )
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
            ON CONFLICT (id) DO UPDATE SET
                slug = COALESCE(EXCLUDED.slug, provider.slug),
                name = COALESCE(EXCLUDED.name, provider.name),
                country = COALESCE(EXCLUDED.country, provider.country),
                default_bet_slip_link = COALESCE(EXCLUDED.default_bet_slip_link, provider.default_bet_slip_link),
                colors = COALESCE(EXCLUDED.colors, provider.colors),
                odds_from_provider_id = COALESCE(EXCLUDED.odds_from_provider_id, provider.odds_from_provider_id),
                live_odds_from_provider_id = COALESCE(EXCLUDED.live_odds_from_provider_id, provider.live_odds_from_provider_id)
            """,
            rows,
        )

    async def _upsert_provider_configurations(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.id,
                item.campaign_id,
                item.provider_id,
                item.fallback_provider_id,
                item.type,
                item.weight,
                item.branded,
                item.featured_odds_type,
                item.bet_slip_link,
                item.default_bet_slip_link,
                item.impression_cost_encrypted,
            )
            for item in bundle.provider_configurations
        ]
        await _executemany(
            executor,
            """
            INSERT INTO provider_configuration (
                id, campaign_id, provider_id, fallback_provider_id, type, weight,
                branded, featured_odds_type, bet_slip_link, default_bet_slip_link,
                impression_cost_encrypted
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (id) DO UPDATE SET
                campaign_id = EXCLUDED.campaign_id,
                provider_id = EXCLUDED.provider_id,
                fallback_provider_id = EXCLUDED.fallback_provider_id,
                type = EXCLUDED.type,
                weight = EXCLUDED.weight,
                branded = EXCLUDED.branded,
                featured_odds_type = EXCLUDED.featured_odds_type,
                bet_slip_link = EXCLUDED.bet_slip_link,
                default_bet_slip_link = EXCLUDED.default_bet_slip_link,
                impression_cost_encrypted = EXCLUDED.impression_cost_encrypted
            """,
            rows,
        )

    async def _upsert_event_markets(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.id,
                item.event_id,
                item.provider_id,
                item.fid,
                item.market_id,
                item.source_id,
                item.market_group,
                item.market_name,
                item.market_period,
                item.structure_type,
                item.choice_group,
                item.is_live,
                item.suspended,
            )
            for item in bundle.event_markets
        ]
        await _executemany(
            executor,
            """
            INSERT INTO event_market (
                id, event_id, provider_id, fid, market_id, source_id, market_group,
                market_name, market_period, structure_type, choice_group, is_live, suspended
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (id) DO UPDATE SET
                event_id = EXCLUDED.event_id,
                provider_id = EXCLUDED.provider_id,
                fid = EXCLUDED.fid,
                market_id = EXCLUDED.market_id,
                source_id = EXCLUDED.source_id,
                market_group = EXCLUDED.market_group,
                market_name = EXCLUDED.market_name,
                market_period = EXCLUDED.market_period,
                structure_type = EXCLUDED.structure_type,
                choice_group = EXCLUDED.choice_group,
                is_live = EXCLUDED.is_live,
                suspended = EXCLUDED.suspended
            """,
            rows,
        )

    async def _upsert_event_market_choices(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.source_id,
                item.event_market_id,
                item.name,
                item.change_value,
                item.fractional_value,
                item.initial_fractional_value,
            )
            for item in bundle.event_market_choices
        ]
        await _executemany(
            executor,
            """
            INSERT INTO event_market_choice (
                source_id, event_market_id, name, change_value, fractional_value, initial_fractional_value
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (source_id) DO UPDATE SET
                event_market_id = EXCLUDED.event_market_id,
                name = EXCLUDED.name,
                change_value = EXCLUDED.change_value,
                fractional_value = EXCLUDED.fractional_value,
                initial_fractional_value = EXCLUDED.initial_fractional_value
            """,
            rows,
        )

    async def _upsert_event_winning_odds(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.event_id,
                item.provider_id,
                item.side,
                item.odds_id,
                item.actual,
                item.expected,
                item.fractional_value,
            )
            for item in bundle.event_winning_odds
        ]
        await _executemany(
            executor,
            """
            INSERT INTO event_winning_odds (
                event_id, provider_id, side, odds_id, actual, expected, fractional_value
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (event_id, provider_id, side) DO UPDATE SET
                odds_id = EXCLUDED.odds_id,
                actual = EXCLUDED.actual,
                expected = EXCLUDED.expected,
                fractional_value = EXCLUDED.fractional_value
            """,
            rows,
        )

    async def _upsert_event_lineups(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.event_id,
                item.side,
                item.formation,
                _jsonb(item.player_color),
                _jsonb(item.goalkeeper_color),
                _jsonb(item.support_staff),
            )
            for item in bundle.event_lineups
        ]
        await _executemany(
            executor,
            """
            INSERT INTO event_lineup (
                event_id, side, formation, player_color, goalkeeper_color, support_staff
            )
            VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb)
            ON CONFLICT (event_id, side) DO UPDATE SET
                formation = EXCLUDED.formation,
                player_color = EXCLUDED.player_color,
                goalkeeper_color = EXCLUDED.goalkeeper_color,
                support_staff = EXCLUDED.support_staff
            """,
            rows,
        )

    async def _upsert_event_lineup_players(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.event_id,
                item.side,
                item.player_id,
                item.team_id,
                item.position,
                item.substitute,
                item.shirt_number,
                item.jersey_number,
                item.avg_rating,
            )
            for item in bundle.event_lineup_players
        ]
        await _executemany(
            executor,
            """
            INSERT INTO event_lineup_player (
                event_id, side, player_id, team_id, position, substitute,
                shirt_number, jersey_number, avg_rating
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (event_id, side, player_id) DO UPDATE SET
                team_id = EXCLUDED.team_id,
                position = EXCLUDED.position,
                substitute = EXCLUDED.substitute,
                shirt_number = EXCLUDED.shirt_number,
                jersey_number = EXCLUDED.jersey_number,
                avg_rating = EXCLUDED.avg_rating
            """,
            rows,
        )

    async def _upsert_event_lineup_missing_players(self, executor: SqlExecutor, bundle: EventDetailBundle) -> None:
        rows = [
            (
                item.event_id,
                item.side,
                item.player_id,
                item.description,
                item.expected_end_date,
                item.external_type,
                item.reason,
                item.type,
            )
            for item in bundle.event_lineup_missing_players
        ]
        await _executemany(
            executor,
            """
            INSERT INTO event_lineup_missing_player (
                event_id, side, player_id, description, expected_end_date, external_type, reason, type
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (event_id, side, player_id) DO UPDATE SET
                description = EXCLUDED.description,
                expected_end_date = EXCLUDED.expected_end_date,
                external_type = EXCLUDED.external_type,
                reason = EXCLUDED.reason,
                type = EXCLUDED.type
            """,
            rows,
        )
