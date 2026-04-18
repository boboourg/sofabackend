from __future__ import annotations

import unittest


class SurfaceCorrectionDetectorTests(unittest.TestCase):
    def test_detects_score_regression_and_var_flip_as_force_rehydrate(self) -> None:
        from schema_inspector.event_list_parser import (
            EventChangeItemRecord,
            EventListBundle,
            EventRecord,
            EventScoreRecord,
            EventVarInProgressRecord,
        )
        from schema_inspector.services.surface_correction_detector import (
            SurfaceCorrectionDetector,
            SurfaceEventState,
        )

        detector = SurfaceCorrectionDetector()
        bundle = EventListBundle(
            registry_entries=(),
            payload_snapshots=(),
            sports=(),
            countries=(),
            categories=(),
            teams=(),
            unique_tournaments=(),
            seasons=(),
            tournaments=(),
            event_statuses=(),
            events=(
                EventRecord(
                    id=901,
                    slug="event-901",
                    custom_id=None,
                    detail_id=None,
                    tournament_id=None,
                    unique_tournament_id=None,
                    season_id=None,
                    home_team_id=None,
                    away_team_id=None,
                    status_code=100,
                    season_statistics_type=None,
                    start_timestamp=None,
                    coverage=None,
                    winner_code=None,
                    aggregated_winner_code=None,
                    home_red_cards=None,
                    away_red_cards=None,
                    previous_leg_event_id=None,
                    cup_matches_in_round=None,
                    default_period_count=None,
                    default_period_length=None,
                    default_overtime_length=None,
                    last_period=None,
                    correct_ai_insight=None,
                    correct_halftime_ai_insight=None,
                    feed_locked=None,
                    is_editor=None,
                    show_toto_promo=None,
                    crowdsourcing_enabled=None,
                    crowdsourcing_data_display_enabled=None,
                    final_result_only=None,
                    has_event_player_statistics=None,
                    has_event_player_heat_map=None,
                    has_global_highlights=None,
                    has_xg=None,
                ),
            ),
            event_round_infos=(),
            event_status_times=(),
            event_times=(),
            event_var_in_progress_items=(EventVarInProgressRecord(event_id=901, home_team=True, away_team=False),),
            event_scores=(
                EventScoreRecord(event_id=901, side="home", current=0),
                EventScoreRecord(event_id=901, side="away", current=0),
            ),
            event_filter_values=(),
            event_change_items=(EventChangeItemRecord(event_id=901, change_timestamp=123, ordinal=0, change_value="score"),),
        )

        corrections = detector.detect(
            bundle=bundle,
            previous_states={
                901: SurfaceEventState(
                    event_id=901,
                    status_code=100,
                    winner_code=None,
                    aggregated_winner_code=None,
                    home_score=1,
                    away_score=0,
                    var_home=False,
                    var_away=False,
                    change_timestamp=111,
                    changes=("score",),
                )
            },
        )

        self.assertEqual(len(corrections), 1)
        self.assertEqual(corrections[0].event_id, 901)
        self.assertEqual(corrections[0].reason, "score_changed")

    def test_ignores_new_events_without_previous_surface_state(self) -> None:
        from schema_inspector.event_list_parser import EventListBundle
        from schema_inspector.services.surface_correction_detector import SurfaceCorrectionDetector

        detector = SurfaceCorrectionDetector()
        corrections = detector.detect(
            bundle=EventListBundle(
                registry_entries=(),
                payload_snapshots=(),
                sports=(),
                countries=(),
                categories=(),
                teams=(),
                unique_tournaments=(),
                seasons=(),
                tournaments=(),
                event_statuses=(),
                events=(),
                event_round_infos=(),
                event_status_times=(),
                event_times=(),
                event_var_in_progress_items=(),
                event_scores=(),
                event_filter_values=(),
                event_change_items=(),
            ),
            previous_states={},
        )

        self.assertEqual(corrections, ())


if __name__ == "__main__":
    unittest.main()
