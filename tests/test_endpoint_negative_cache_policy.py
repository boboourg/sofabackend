from __future__ import annotations

from datetime import UTC, datetime
import unittest


class EndpointNegativeCachePolicyTests(unittest.TestCase):
    def test_feature_flag_can_disable_negative_cache_without_schema_rollback(self) -> None:
        from schema_inspector.season_widget_negative_cache import load_negative_cache_settings

        settings = load_negative_cache_settings(env={"SCHEMA_INSPECTOR_NEGATIVE_CACHE_ENABLED": "0"})

        self.assertFalse(settings.enabled)
        self.assertEqual(settings.mode, "off")

    def test_shadow_mode_can_be_enabled_without_enforcement(self) -> None:
        from schema_inspector.season_widget_negative_cache import load_negative_cache_settings

        settings = load_negative_cache_settings(env={"SCHEMA_INSPECTOR_NEGATIVE_CACHE_MODE": "shadow"})

        self.assertTrue(settings.enabled)
        self.assertTrue(settings.shadow)
        self.assertFalse(settings.enforce)

    def test_first_404_creates_probation_state(self) -> None:
        from schema_inspector.season_widget_negative_cache import NegativeCacheState, ProbeObservation, reduce_negative_cache_state

        now = datetime(2026, 4, 23, 12, 0, tzinfo=UTC)
        next_probe_after = datetime(2026, 4, 23, 18, 0, tzinfo=UTC)

        updated = reduce_negative_cache_state(
            current=None,
            observation=ProbeObservation(
                unique_tournament_id=32784,
                season_id=91078,
                endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season",
                observed_at=now,
                http_status=404,
                next_probe_after=next_probe_after,
            ),
        )

        self.assertEqual(updated.classification, "c_probation")
        self.assertEqual(updated.seen_404_season_ids, (91078,))
        self.assertEqual(updated.seen_200_season_ids, ())
        self.assertEqual(updated.next_probe_after, next_probe_after)

    def test_second_distinct_404_season_promotes_probation_to_structural(self) -> None:
        from schema_inspector.season_widget_negative_cache import NegativeCacheState, ProbeObservation, reduce_negative_cache_state

        current = NegativeCacheState(
            scope_kind="tournament",
            unique_tournament_id=32784,
            season_id=None,
            endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall",
            classification="c_probation",
            first_404_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
            last_404_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
            first_200_at=None,
            last_200_at=None,
            seen_404_season_ids=(91078,),
            seen_200_season_ids=(),
            suppressed_hits_total=0,
            actual_probe_total=1,
            recheck_iteration=0,
            next_probe_after=datetime(2026, 4, 23, 12, 0, tzinfo=UTC),
            probe_lease_until=None,
            probe_lease_owner=None,
            last_http_status=404,
            last_job_type="sync_season_widget",
            last_trace_id="trace-1",
            created_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
            updated_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
        )

        updated = reduce_negative_cache_state(
            current=current,
            observation=ProbeObservation(
                unique_tournament_id=32784,
                season_id=92743,
                endpoint_pattern=current.endpoint_pattern,
                observed_at=datetime(2026, 4, 23, 12, 0, tzinfo=UTC),
                http_status=404,
                next_probe_after=datetime(2026, 4, 30, 12, 0, tzinfo=UTC),
            ),
        )

        self.assertEqual(updated.classification, "b_structural")
        self.assertEqual(updated.seen_404_season_ids, (91078, 92743))
        self.assertEqual(updated.seen_200_season_ids, ())

    def test_success_promotes_tournament_scope_to_mixed_and_marks_supported_season(self) -> None:
        from schema_inspector.season_widget_negative_cache import NegativeCacheState, ProbeObservation, reduce_negative_cache_state

        current = NegativeCacheState(
            scope_kind="tournament",
            unique_tournament_id=15006,
            season_id=None,
            endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/overall",
            classification="c_probation",
            first_404_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
            last_404_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
            first_200_at=None,
            last_200_at=None,
            seen_404_season_ids=(88262,),
            seen_200_season_ids=(),
            suppressed_hits_total=0,
            actual_probe_total=1,
            recheck_iteration=0,
            next_probe_after=datetime(2026, 4, 23, 12, 0, tzinfo=UTC),
            probe_lease_until=None,
            probe_lease_owner=None,
            last_http_status=404,
            last_job_type="sync_season_widget",
            last_trace_id="trace-1",
            created_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
            updated_at=datetime(2026, 4, 22, 12, 0, tzinfo=UTC),
        )

        tournament_state, season_state = reduce_negative_cache_state(
            current=current,
            observation=ProbeObservation(
                unique_tournament_id=15006,
                season_id=87327,
                endpoint_pattern=current.endpoint_pattern,
                observed_at=datetime(2026, 4, 23, 12, 0, tzinfo=UTC),
                http_status=200,
                next_probe_after=None,
            ),
        )

        self.assertEqual(tournament_state.classification, "mixed_by_season")
        self.assertEqual(tournament_state.seen_200_season_ids, (87327,))
        self.assertIsNotNone(season_state)
        self.assertEqual(season_state.scope_kind, "season")
        self.assertEqual(season_state.classification, "supported_season")
        self.assertEqual(season_state.season_id, 87327)

    def test_seasonal_window_is_active_three_days_before_and_fourteen_days_after_season_end(self) -> None:
        from schema_inspector.season_widget_negative_cache import is_seasonal_widget_bypass_active

        season_end_at = datetime(2026, 5, 10, 12, 0, tzinfo=UTC)
        inside_before = datetime(2026, 5, 7, 12, 0, tzinfo=UTC)
        inside_after = datetime(2026, 5, 24, 11, 59, tzinfo=UTC)
        outside_before = datetime(2026, 5, 7, 11, 59, tzinfo=UTC)
        outside_after = datetime(2026, 5, 24, 12, 1, tzinfo=UTC)

        pattern = "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season"
        self.assertTrue(is_seasonal_widget_bypass_active(pattern, now=inside_before, season_end_at=season_end_at))
        self.assertTrue(is_seasonal_widget_bypass_active(pattern, now=inside_after, season_end_at=season_end_at))
        self.assertFalse(is_seasonal_widget_bypass_active(pattern, now=outside_before, season_end_at=season_end_at))
        self.assertFalse(is_seasonal_widget_bypass_active(pattern, now=outside_after, season_end_at=season_end_at))


if __name__ == "__main__":
    unittest.main()
