from __future__ import annotations

from datetime import UTC, datetime, timedelta
import unittest


class _CapturingExecutor:
    def __init__(self, value=None) -> None:
        self.value = value
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, sql: str, *args):
        self.calls.append((sql, args))
        return self.value

    async def execute(self, sql: str, *args):
        self.calls.append((sql, args))
        return "OK"


class _StaticNegativeCacheRepository:
    def __init__(self, state) -> None:
        self.state = state

    async def list_states(self, executor, *, scope_kind: str, unique_tournament_id: int, season_id: int | None, endpoint_patterns: tuple[str, ...]):
        del executor, scope_kind, unique_tournament_id, season_id
        if self.state is None:
            return {}
        return {
            pattern: self.state
            for pattern in endpoint_patterns
            if pattern == self.state.endpoint_pattern
        }

    async def fetch_season_end_at(self, executor, *, unique_tournament_id: int, season_id: int):
        del executor, unique_tournament_id, season_id
        return None


class EndpointNegativeCacheRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_season_end_uses_scheduled_notstarted_primary_with_finished_fallback(self) -> None:
        from schema_inspector.storage.endpoint_negative_cache_repository import EndpointNegativeCacheRepository

        repository = EndpointNegativeCacheRepository()
        executor = _CapturingExecutor(value=datetime(2026, 5, 20, 12, 0, tzinfo=UTC))

        value = await repository.fetch_season_end_at(executor, unique_tournament_id=132, season_id=84695)

        self.assertEqual(value, datetime(2026, 5, 20, 12, 0, tzinfo=UTC))
        self.assertEqual(len(executor.calls), 1)
        sql, args = executor.calls[0]
        self.assertIn("IN ('scheduled', 'notstarted')", sql)
        self.assertIn("= 'finished'", sql)
        self.assertIn("COALESCE", sql)
        self.assertEqual(args, (132, 84695))

    async def test_shadow_mode_logs_shadow_suppress_but_does_not_block_planner_candidates(self) -> None:
        from schema_inspector.season_widget_negative_cache import (
            CLASSIFICATION_B_STRUCTURAL,
            MODE_SHADOW,
            NegativeCacheSettings,
            NegativeCacheState,
            SeasonWidgetNegativeCache,
        )

        now = datetime(2026, 4, 23, 12, 0, tzinfo=UTC)
        state = NegativeCacheState(
            scope_kind="tournament",
            unique_tournament_id=32784,
            season_id=None,
            endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season",
            classification=CLASSIFICATION_B_STRUCTURAL,
            first_404_at=now - timedelta(days=5),
            last_404_at=now - timedelta(days=1),
            first_200_at=None,
            last_200_at=None,
            seen_404_season_ids=(91078, 92743),
            seen_200_season_ids=(),
            suppressed_hits_total=0,
            actual_probe_total=2,
            recheck_iteration=1,
            next_probe_after=now + timedelta(days=7),
            probe_lease_until=None,
            probe_lease_owner=None,
            last_http_status=404,
            last_job_type="sync_season_widget",
            last_trace_id="trace-1",
            created_at=now - timedelta(days=5),
            updated_at=now - timedelta(days=1),
        )
        gate = SeasonWidgetNegativeCache(
            repository=_StaticNegativeCacheRepository(state),
            sql_executor=object(),
            now_factory=lambda: now,
            settings=NegativeCacheSettings(mode=MODE_SHADOW),
        )

        blocked = await gate.blocked_endpoint_patterns(
            sport_slug="basketball",
            unique_tournament_id=32784,
            season_id=91078,
            endpoint_patterns=(state.endpoint_pattern,),
        )

        self.assertEqual(blocked, ())
        self.assertEqual(len(gate.events), 1)
        self.assertEqual(gate.events[0].decision, "shadow_suppress")

    async def test_upsert_state_keeps_last_200_at_separate_from_jsonb_columns(self) -> None:
        from schema_inspector.season_widget_negative_cache import NegativeCacheState
        from schema_inspector.storage.endpoint_negative_cache_repository import EndpointNegativeCacheRepository

        now = datetime(2026, 4, 23, 12, 0, tzinfo=UTC)
        state = NegativeCacheState(
            scope_kind="tournament",
            unique_tournament_id=15006,
            season_id=None,
            endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/overall",
            classification="mixed_by_season",
            first_404_at=now - timedelta(days=5),
            last_404_at=now - timedelta(days=2),
            first_200_at=now - timedelta(days=1),
            last_200_at=now,
            seen_404_season_ids=(88262,),
            seen_200_season_ids=(87327,),
            suppressed_hits_total=3,
            actual_probe_total=4,
            recheck_iteration=0,
            next_probe_after=None,
            probe_lease_until=None,
            probe_lease_owner=None,
            last_http_status=200,
            last_job_type="sync_season_widget",
            last_trace_id="trace-200",
            created_at=now - timedelta(days=5),
            updated_at=now,
        )
        repository = EndpointNegativeCacheRepository()
        executor = _CapturingExecutor()

        await repository._upsert_state(executor, state)

        sql, args = executor.calls[0]
        self.assertIn("$7, $8, $9, $10, $11::jsonb, $12::jsonb", sql)
        self.assertEqual(len(args), 23)
        self.assertEqual(str(args[9]), "2026-04-23 12:00:00+00:00")
        self.assertEqual(args[10], "[88262]")
        self.assertEqual(args[11], "[87327]")

    async def test_shadow_and_enforce_make_same_coarse_suppress_decision_for_structural_state(self) -> None:
        from schema_inspector.season_widget_negative_cache import (
            CLASSIFICATION_B_STRUCTURAL,
            MODE_ENFORCE,
            MODE_SHADOW,
            NegativeCacheSettings,
            NegativeCacheState,
            SeasonWidgetNegativeCache,
        )

        now = datetime(2026, 4, 23, 12, 0, tzinfo=UTC)
        state = NegativeCacheState(
            scope_kind="tournament",
            unique_tournament_id=32784,
            season_id=None,
            endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season",
            classification=CLASSIFICATION_B_STRUCTURAL,
            first_404_at=now - timedelta(days=5),
            last_404_at=now - timedelta(days=1),
            first_200_at=None,
            last_200_at=None,
            seen_404_season_ids=(91078, 92743),
            seen_200_season_ids=(),
            suppressed_hits_total=0,
            actual_probe_total=2,
            recheck_iteration=1,
            next_probe_after=now + timedelta(days=7),
            probe_lease_until=None,
            probe_lease_owner=None,
            last_http_status=404,
            last_job_type="sync_season_widget",
            last_trace_id="trace-1",
            created_at=now - timedelta(days=5),
            updated_at=now - timedelta(days=1),
        )
        shadow_gate = SeasonWidgetNegativeCache(
            repository=_StaticNegativeCacheRepository(state),
            sql_executor=object(),
            now_factory=lambda: now,
            settings=NegativeCacheSettings(mode=MODE_SHADOW),
        )
        enforce_gate = SeasonWidgetNegativeCache(
            repository=_StaticNegativeCacheRepository(state),
            sql_executor=object(),
            now_factory=lambda: now,
            settings=NegativeCacheSettings(mode=MODE_ENFORCE),
        )

        shadow_blocked = await shadow_gate.blocked_endpoint_patterns(
            sport_slug="basketball",
            unique_tournament_id=32784,
            season_id=91078,
            endpoint_patterns=(state.endpoint_pattern,),
        )
        enforce_blocked = await enforce_gate.blocked_endpoint_patterns(
            sport_slug="basketball",
            unique_tournament_id=32784,
            season_id=91078,
            endpoint_patterns=(state.endpoint_pattern,),
        )

        self.assertEqual(shadow_blocked, ())
        self.assertEqual(enforce_blocked, (state.endpoint_pattern,))
        self.assertEqual(len(shadow_gate.events), 1)
        self.assertEqual(len(enforce_gate.events), 1)
        self.assertEqual(shadow_gate.events[0].endpoint_pattern, enforce_gate.events[0].endpoint_pattern)
        self.assertEqual(shadow_gate.events[0].scope_kind, enforce_gate.events[0].scope_kind)
        self.assertEqual(shadow_gate.events[0].classification_before, enforce_gate.events[0].classification_before)
        self.assertEqual(shadow_gate.events[0].decision, "shadow_suppress")
        self.assertEqual(enforce_gate.events[0].decision, "suppressed")


if __name__ == "__main__":
    unittest.main()
