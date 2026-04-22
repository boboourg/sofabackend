from __future__ import annotations

import unittest
from unittest.mock import patch

from schema_inspector.competition_parser import ApiPayloadSnapshotRecord, CategoryRecord, CountryRecord, SportRecord, UniqueTournamentRecord
from schema_inspector.endpoints import event_detail_registry_entries
from schema_inspector.event_detail_job import EventDetailIngestJob, EventDetailIngestProfile
from schema_inspector.event_detail_parser import (
    EventCommentFeedRecord,
    EventCommentRecord,
    EventDetailBundle,
    EventDetailEventRecord,
    EventDetailTeamRecord,
    EventDetailTournamentRecord,
    EventDuelRecord,
    EventGraphPointRecord,
    EventGraphRecord,
    EventBestPlayerEntryRecord,
    EventLineupMissingPlayerRecord,
    EventLineupPlayerRecord,
    EventLineupRecord,
    EventManagerAssignmentRecord,
    EventMarketChoiceRecord,
    EventMarketRecord,
    EventPlayerRatingBreakdownActionRecord,
    EventPlayerStatisticsRecord,
    EventPlayerStatValueRecord,
    EventPregameFormItemRecord,
    EventPregameFormRecord,
    EventPregameFormSideRecord,
    EventTeamHeatmapPointRecord,
    EventTeamHeatmapRecord,
    EventVoteOptionRecord,
    EventWinningOddsRecord,
    ManagerPerformanceRecord,
    ManagerRecord,
    ManagerTeamMembershipRecord,
    PlayerRecord,
    ProviderConfigurationRecord,
    ProviderRecord,
    RefereeRecord,
    VenueRecord,
)
from schema_inspector.event_detail_repository import EventDetailRepository, EventDetailWriteResult
from schema_inspector.event_list_parser import (
    EventChangeItemRecord,
    EventFilterValueRecord,
    EventRoundInfoRecord,
    EventScoreRecord,
    EventSeasonRecord,
    EventStatusRecord,
    EventStatusTimeRecord,
    EventTimeRecord,
    EventVarInProgressRecord,
)


class _FakeExecutor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    async def executemany(self, command: str, args):
        self.executemany_calls.append((command, list(args)))
        return None


class _FakeParser:
    def __init__(self, bundle: EventDetailBundle) -> None:
        self.bundle = bundle
        self.calls: list[tuple[int, tuple[int, ...], float]] = []

    async def fetch_bundle(
        self,
        event_id: int,
        *,
        provider_ids=(1,),
        timeout: float = 20.0,
        profile: EventDetailIngestProfile | None = None,
    ) -> EventDetailBundle:
        self.calls.append((event_id, tuple(provider_ids), timeout))
        if profile is not None:
            object.__setattr__(profile, "upstream_fetch_ms", 17)
        return self.bundle


class _FakeRepository:
    def __init__(self, result: EventDetailWriteResult | None, *, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.calls: list[tuple[object, EventDetailBundle]] = []

    async def upsert_bundle(self, executor, bundle: EventDetailBundle) -> EventDetailWriteResult:
        self.calls.append((executor, bundle))
        if self.error is not None:
            raise self.error
        if self.result is None:
            raise RuntimeError("result is required when error is not set")
        return self.result


class _FakeTransaction:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def __aenter__(self) -> object:
        return self.connection

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        return None


class _FakeDatabase:
    def __init__(self, connection: object) -> None:
        self.connection = connection
        self.transaction_calls = 0

    def transaction(self) -> _FakeTransaction:
        self.transaction_calls += 1
        return _FakeTransaction(self.connection)


def _build_bundle() -> EventDetailBundle:
    return EventDetailBundle(
        registry_entries=event_detail_registry_entries(sport_slug="football"),
        payload_snapshots=(
            ApiPayloadSnapshotRecord(
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/14083191",
                envelope_key="event",
                context_entity_type="event",
                context_entity_id=14083191,
                payload={"id": 14083191},
                fetched_at="2026-04-10T10:00:00+00:00",
            ),
        ),
        sports=(SportRecord(id=1, slug="football", name="Football"),),
        countries=(CountryRecord(alpha2="EN", alpha3="ENG", slug="england", name="England"),),
        categories=(CategoryRecord(id=1, slug="england", name="England", sport_id=1, country_alpha2="EN"),),
        unique_tournaments=(UniqueTournamentRecord(id=17, slug="premier-league", name="Premier League", category_id=1),),
        seasons=(EventSeasonRecord(id=76986, name="Premier League 25/26", year="25/26", editor=False),),
        tournaments=(
            EventDetailTournamentRecord(
                id=1,
                slug="premier-league",
                name="Premier League",
                category_id=1,
                unique_tournament_id=17,
                competition_type=1,
            ),
        ),
        teams=(
            EventDetailTeamRecord(id=42, slug="arsenal", name="Arsenal", sport_id=1, country_alpha2="EN", manager_id=500),
            EventDetailTeamRecord(id=43, slug="chelsea", name="Chelsea", sport_id=1, country_alpha2="EN", manager_id=501),
        ),
        venues=(VenueRecord(id=55, slug="emirates", name="Emirates Stadium", country_alpha2="EN"),),
        referees=(RefereeRecord(id=99, slug="michael-oliver", name="Michael Oliver", sport_id=1, country_alpha2="EN"),),
        managers=(
            ManagerRecord(id=500, slug="arteta", name="Mikel Arteta", team_id=42),
            ManagerRecord(id=501, slug="maresca", name="Enzo Maresca", team_id=43),
        ),
        manager_performances=(ManagerPerformanceRecord(manager_id=500, total=10, wins=7, draws=2, losses=1),),
        manager_team_memberships=(
            ManagerTeamMembershipRecord(manager_id=500, team_id=42),
            ManagerTeamMembershipRecord(manager_id=501, team_id=43),
        ),
        players=(PlayerRecord(id=700, slug="saka", name="Bukayo Saka", team_id=42, country_alpha2="EN", position="F"),),
        event_statuses=(EventStatusRecord(code=100, description="1st half", type="inprogress"),),
        events=(
            EventDetailEventRecord(
                id=14083191,
                slug="arsenal-chelsea",
                custom_id="abc123",
                detail_id=9,
                tournament_id=1,
                unique_tournament_id=17,
                season_id=76986,
                home_team_id=42,
                away_team_id=43,
                venue_id=55,
                referee_id=99,
                status_code=100,
                season_statistics_type="overall",
                start_timestamp=1775779200,
                coverage=1,
                winner_code=1,
                aggregated_winner_code=1,
                home_red_cards=0,
                away_red_cards=0,
                previous_leg_event_id=None,
                cup_matches_in_round=1,
                default_period_count=2,
                default_period_length=45,
                default_overtime_length=15,
                last_period="2nd",
                correct_ai_insight=False,
                correct_halftime_ai_insight=False,
                feed_locked=False,
                is_editor=False,
                show_toto_promo=False,
                crowdsourcing_enabled=True,
                crowdsourcing_data_display_enabled=True,
                final_result_only=False,
                has_event_player_statistics=True,
                has_event_player_heat_map=True,
                has_global_highlights=False,
                has_xg=True,
            ),
        ),
        event_round_infos=(EventRoundInfoRecord(event_id=14083191, round_number=32, name="Round 32"),),
        event_status_times=(EventStatusTimeRecord(event_id=14083191, prefix="'", timestamp=1712745600),),
        event_times=(EventTimeRecord(event_id=14083191, current_period_start_timestamp=1712742000, period_length=45),),
        event_var_in_progress_items=(EventVarInProgressRecord(event_id=14083191, home_team=True, away_team=False),),
        event_scores=(
            EventScoreRecord(event_id=14083191, side="home", current=1, display=1),
            EventScoreRecord(event_id=14083191, side="away", current=0, display=0),
        ),
        event_filter_values=(EventFilterValueRecord(event_id=14083191, filter_name="category", ordinal=0, filter_value="live"),),
        event_change_items=(EventChangeItemRecord(event_id=14083191, change_timestamp=1712745600, ordinal=0, change_value="status"),),
        event_manager_assignments=(
            EventManagerAssignmentRecord(event_id=14083191, side="home", manager_id=500),
            EventManagerAssignmentRecord(event_id=14083191, side="away", manager_id=501),
        ),
        event_duels=(EventDuelRecord(event_id=14083191, duel_type="team", home_wins=5, away_wins=3, draws=2),),
        event_pregame_forms=(EventPregameFormRecord(event_id=14083191, label="Pts"),),
        event_pregame_form_sides=(
            EventPregameFormSideRecord(event_id=14083191, side="home", avg_rating="6.7", position=2, value="70"),
            EventPregameFormSideRecord(event_id=14083191, side="away", avg_rating="6.6", position=5, value="60"),
        ),
        event_pregame_form_items=(
            EventPregameFormItemRecord(event_id=14083191, side="home", ordinal=0, form_value="W"),
            EventPregameFormItemRecord(event_id=14083191, side="away", ordinal=0, form_value="L"),
        ),
        event_vote_options=(EventVoteOptionRecord(event_id=14083191, vote_type="vote", option_name="vote1", vote_count=10),),
        event_comment_feeds=(
            EventCommentFeedRecord(
                event_id=14083191,
                home_player_color={"primary": "#ffffff"},
                away_player_color={"primary": "#0000ff"},
            ),
        ),
        event_comments=(
            EventCommentRecord(
                event_id=14083191,
                comment_id=37184719,
                sequence=0,
                period_name="2ND",
                is_home=False,
                player_id=700,
                text="Second Half begins.",
                match_time=46,
                comment_type="matchStarted",
            ),
        ),
        event_graphs=(EventGraphRecord(event_id=14083191, period_time=45, period_count=2, overtime_length=15),),
        event_graph_points=(
            EventGraphPointRecord(event_id=14083191, ordinal=0, minute=1, value=-4),
            EventGraphPointRecord(event_id=14083191, ordinal=1, minute=2, value=12),
        ),
        event_team_heatmaps=(
            EventTeamHeatmapRecord(event_id=14083191, team_id=42),
            EventTeamHeatmapRecord(event_id=14083191, team_id=43),
        ),
        event_team_heatmap_points=(
            EventTeamHeatmapPointRecord(event_id=14083191, team_id=42, point_type="player", ordinal=0, x=50.2, y=43.1),
            EventTeamHeatmapPointRecord(event_id=14083191, team_id=42, point_type="goalkeeper", ordinal=0, x=11.1, y=50.0),
        ),
        providers=(ProviderRecord(id=1),),
        provider_configurations=(
            ProviderConfigurationRecord(
                id=91,
                provider_id=1,
                campaign_id=12,
                fallback_provider_id=None,
                type="featured",
                weight=10,
                branded=True,
                featured_odds_type="pre",
                bet_slip_link="https://example.com/slip",
                default_bet_slip_link="https://example.com/default-slip",
                impression_cost_encrypted="ciphertext",
            ),
        ),
        event_markets=(
            EventMarketRecord(
                id=289779151,
                event_id=14083191,
                provider_id=1,
                fid=191744484,
                market_id=1,
                source_id=191744484,
                market_group="1X2",
                market_name="Full time",
                market_period="Full-time",
                structure_type=1,
                choice_group="default",
                is_live=False,
                suspended=False,
            ),
        ),
        event_market_choices=(
            EventMarketChoiceRecord(
                source_id=11,
                event_market_id=289779151,
                name="1",
                change_value=0,
                fractional_value="1/1",
                initial_fractional_value="11/10",
            ),
        ),
        event_winning_odds=(EventWinningOddsRecord(event_id=14083191, provider_id=1, side="home", odds_id=1, actual=52),),
        event_lineups=(
            EventLineupRecord(event_id=14083191, side="home", formation="4-3-3", support_staff=()),
            EventLineupRecord(event_id=14083191, side="away", formation="4-2-3-1", support_staff=()),
        ),
        event_lineup_players=(EventLineupPlayerRecord(event_id=14083191, side="home", player_id=700, team_id=42, position="F", substitute=False, shirt_number=7, jersey_number="7", avg_rating=7.1),),
        event_lineup_missing_players=(EventLineupMissingPlayerRecord(event_id=14083191, side="away", player_id=700, description="Hamstring", expected_end_date="2026-05-01", external_type=72, reason=1, type="missing"),),
        event_best_player_entries=(
            EventBestPlayerEntryRecord(event_id=14083191, bucket="best_home", ordinal=0, player_id=700, label="rating", value_text="7.9", value_numeric=7.9, is_player_of_the_match=False),
            EventBestPlayerEntryRecord(event_id=14083191, bucket="player_of_the_match", ordinal=0, player_id=700, label="rating", value_text="7.9", value_numeric=7.9, is_player_of_the_match=True),
        ),
        event_player_statistics=(
            EventPlayerStatisticsRecord(
                event_id=14083191,
                player_id=700,
                team_id=42,
                position="F",
                rating=7.9,
                rating_original=7.8,
                rating_alternative=7.6,
                statistics_type="player",
                sport_slug="football",
                extra_json={"captain": True},
            ),
        ),
        event_player_stat_values=(
            EventPlayerStatValueRecord(event_id=14083191, player_id=700, stat_name="minutesPlayed", stat_value_numeric=90.0, stat_value_text="90", stat_value_json=None),
            EventPlayerStatValueRecord(event_id=14083191, player_id=700, stat_name="goals", stat_value_numeric=1.0, stat_value_text="1", stat_value_json=None),
        ),
        event_player_rating_breakdown_actions=(
            EventPlayerRatingBreakdownActionRecord(
                event_id=14083191,
                player_id=700,
                action_group="passes",
                ordinal=0,
                event_action_type="pass",
                is_home=True,
                keypass=False,
                outcome=True,
                start_x=80.5,
                start_y=58.9,
                end_x=96.9,
                end_y=60.3,
            ),
        ),
    )


class EventDetailStorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_event_detail_repository_writes_expected_tables(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EventDetailRepository()

        result = await repository.upsert_bundle(executor, bundle)

        self.assertEqual(result.event_rows, 1)
        self.assertEqual(result.player_rows, 1)
        self.assertEqual(result.event_market_rows, 1)
        self.assertEqual(result.event_player_statistics_rows, 1)
        statements = [sql for sql, _ in executor.executemany_calls]
        endpoint_registry_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO endpoint_registry" in sql)
        self.assertEqual(endpoint_registry_rows[0][6], "sofascore")
        self.assertEqual(endpoint_registry_rows[0][7], "v1")
        self.assertTrue(any("INSERT INTO venue" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO referee" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO manager " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO player " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event (" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_lineup " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_comment_feed " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_comment " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_graph " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_team_heatmap " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO provider_configuration " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_market " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_vote_option" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_best_player_entry " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_player_statistics " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_player_stat_value " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_player_rating_breakdown_action " in sql for sql in statements))

    async def test_event_detail_repository_drops_orphan_player_team_foreign_keys(self) -> None:
        bundle = _build_bundle()
        bundle = EventDetailBundle(
            **{
                **bundle.__dict__,
                "players": (
                    PlayerRecord(
                        id=700,
                        slug="saka",
                        name="Bukayo Saka",
                        team_id=1073591,
                        country_alpha2="EN",
                        position="F",
                    ),
                ),
            }
        )
        executor = _FakeExecutor()
        repository = EventDetailRepository()

        await repository.upsert_bundle(executor, bundle)

        player_insert = next(
            args
            for sql, rows in executor.executemany_calls
            if "INSERT INTO player " in sql
            for args in rows
        )
        self.assertIsNone(player_insert[6])

    async def test_event_detail_repository_drops_orphan_lineup_player_team_foreign_keys(self) -> None:
        bundle = _build_bundle()
        bundle = EventDetailBundle(
            **{
                **bundle.__dict__,
                "event_lineup_players": (
                    EventLineupPlayerRecord(
                        event_id=14083191,
                        side="home",
                        player_id=700,
                        team_id=1073591,
                        position="F",
                        substitute=False,
                        shirt_number=7,
                        jersey_number="7",
                        avg_rating=7.1,
                    ),
                ),
            }
        )
        executor = _FakeExecutor()
        repository = EventDetailRepository()

        await repository.upsert_bundle(executor, bundle)

        lineup_player_insert = next(
            args
            for sql, rows in executor.executemany_calls
            if "INSERT INTO event_lineup_player (" in sql
            for args in rows
        )
        self.assertIsNone(lineup_player_insert[3])

    async def test_event_detail_ingest_job_uses_transaction_and_repository(self) -> None:
        bundle = _build_bundle()
        parser = _FakeParser(bundle)
        repository_result = EventDetailWriteResult(
            endpoint_registry_rows=12,
            payload_snapshot_rows=1,
            sport_rows=1,
            country_rows=1,
            category_rows=1,
            unique_tournament_rows=1,
            season_rows=1,
            tournament_rows=1,
            team_rows=2,
            venue_rows=1,
            referee_rows=1,
            manager_rows=2,
            manager_performance_rows=1,
            manager_team_membership_rows=2,
            player_rows=1,
            event_status_rows=1,
            event_rows=1,
            event_round_info_rows=1,
            event_status_time_rows=1,
            event_time_rows=1,
            event_var_in_progress_rows=1,
            event_score_rows=2,
            event_filter_value_rows=1,
            event_change_item_rows=1,
            event_manager_assignment_rows=2,
            event_duel_rows=1,
            event_pregame_form_rows=1,
            event_pregame_form_side_rows=2,
            event_pregame_form_item_rows=2,
            event_vote_option_rows=1,
            event_comment_feed_rows=1,
            event_comment_rows=1,
            event_graph_rows=1,
            event_graph_point_rows=2,
            event_team_heatmap_rows=2,
            event_team_heatmap_point_rows=2,
            provider_rows=1,
            provider_configuration_rows=1,
            event_market_rows=1,
            event_market_choice_rows=1,
            event_winning_odds_rows=1,
            event_lineup_rows=2,
            event_lineup_player_rows=1,
            event_lineup_missing_player_rows=1,
            event_best_player_entry_rows=2,
            event_player_statistics_rows=1,
            event_player_stat_value_rows=2,
            event_player_rating_breakdown_action_rows=1,
        )
        repository = _FakeRepository(repository_result)
        connection = object()
        database = _FakeDatabase(connection)
        job = EventDetailIngestJob(parser, repository, database)

        result = await job.run(14083191, provider_ids=(1, 2, 1), timeout=12.5)

        self.assertEqual(parser.calls, [(14083191, (1, 2), 12.5)])
        self.assertEqual(repository.calls, [(connection, bundle)])
        self.assertEqual(database.transaction_calls, 1)
        self.assertEqual(result.event_id, 14083191)
        self.assertEqual(result.provider_ids, (1, 2))
        self.assertEqual(result.written.event_market_rows, 1)

    async def test_event_detail_ingest_job_raises_actionable_error_for_missing_tables(self) -> None:
        bundle = _build_bundle()
        parser = _FakeParser(bundle)
        undefined_table_error = type("UndefinedTableError", (RuntimeError,), {})(
            'relation "event_best_player_entry" does not exist'
        )
        repository = _FakeRepository(None, error=undefined_table_error)
        database = _FakeDatabase(object())
        job = EventDetailIngestJob(parser, repository, database)

        with self.assertRaisesRegex(RuntimeError, "db_setup_cli"):
            await job.run(14083191, provider_ids=(1,), timeout=12.5)

    async def test_event_detail_ingest_job_records_stage_profile_breakdown(self) -> None:
        bundle = _build_bundle()
        parser = _FakeParser(bundle)
        repository = _FakeRepository(
            EventDetailWriteResult(
                endpoint_registry_rows=1,
                payload_snapshot_rows=1,
                sport_rows=1,
                country_rows=1,
                category_rows=1,
                unique_tournament_rows=1,
                season_rows=1,
                tournament_rows=1,
                team_rows=1,
                venue_rows=1,
                referee_rows=1,
                manager_rows=1,
                manager_performance_rows=1,
                manager_team_membership_rows=1,
                player_rows=1,
                event_status_rows=1,
                event_rows=1,
                event_round_info_rows=1,
                event_status_time_rows=1,
                event_time_rows=1,
                event_var_in_progress_rows=1,
                event_score_rows=1,
                event_filter_value_rows=1,
                event_change_item_rows=1,
                event_manager_assignment_rows=1,
                event_duel_rows=1,
                event_pregame_form_rows=1,
                event_pregame_form_side_rows=1,
                event_pregame_form_item_rows=1,
                event_vote_option_rows=1,
                event_comment_feed_rows=1,
                event_comment_rows=1,
                event_graph_rows=1,
                event_graph_point_rows=1,
                event_team_heatmap_rows=1,
                event_team_heatmap_point_rows=1,
                provider_rows=1,
                provider_configuration_rows=1,
                event_market_rows=1,
                event_market_choice_rows=1,
                event_winning_odds_rows=1,
                event_lineup_rows=1,
                event_lineup_player_rows=1,
                event_lineup_missing_player_rows=1,
                event_best_player_entry_rows=1,
                event_player_statistics_rows=1,
                event_player_stat_value_rows=1,
                event_player_rating_breakdown_action_rows=1,
            )
        )
        database = _FakeDatabase(object())
        job = EventDetailIngestJob(parser, repository, database)

        with patch(
            "schema_inspector.event_detail_job.time.perf_counter",
            side_effect=[0.0, 0.050, 0.050, 0.090],
        ):
            result = await job.run(14083191)

        self.assertEqual(result.profile.upstream_fetch_ms, 17)
        self.assertEqual(result.profile.parse_ms, 33)
        self.assertEqual(result.profile.db_persist_ms, 40)


if __name__ == "__main__":
    unittest.main()
