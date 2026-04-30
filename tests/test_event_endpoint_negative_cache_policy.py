from __future__ import annotations

from datetime import UTC, datetime, timedelta
import unittest


class _StaticEventNegativeCacheRepository:
    def __init__(self, state_by_key) -> None:
        self.state_by_key = state_by_key

    async def list_states(self, executor, *, event_id: int, status_phase: str, endpoint_patterns: tuple[str, ...]):
        del executor
        resolved = {}
        for pattern in endpoint_patterns:
            state = self.state_by_key.get((int(event_id), str(status_phase), pattern))
            if state is not None:
                resolved[pattern] = state
        return resolved

    async def try_acquire_probe_lease(self, executor, *, event_id: int, status_phase: str, endpoint_pattern: str, lease_owner: str, now, lease_seconds: int = 90):
        del executor, event_id, status_phase, endpoint_pattern, lease_owner, now, lease_seconds
        return True


class EventEndpointNegativeCachePolicyTests(unittest.TestCase):
    def test_feature_flag_defaults_to_off_until_explicitly_enabled(self) -> None:
        from schema_inspector.event_endpoint_negative_cache import load_event_negative_cache_settings

        settings = load_event_negative_cache_settings(env={})

        self.assertFalse(settings.enabled)
        self.assertEqual(settings.mode, "off")

    def test_shadow_mode_can_be_enabled_for_event_negative_cache(self) -> None:
        from schema_inspector.event_endpoint_negative_cache import load_event_negative_cache_settings

        settings = load_event_negative_cache_settings(
            env={
                "SCHEMA_INSPECTOR_EVENT_NEGATIVE_CACHE_ENABLED": "1",
                "SCHEMA_INSPECTOR_EVENT_NEGATIVE_CACHE_MODE": "shadow",
            }
        )

        self.assertTrue(settings.enabled)
        self.assertTrue(settings.shadow)
        self.assertFalse(settings.enforce)

    def test_first_not_found_creates_probation_state(self) -> None:
        from schema_inspector.event_endpoint_negative_cache import ProbeObservation, reduce_event_negative_cache_state

        now = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
        next_probe_after = now + timedelta(minutes=5)

        updated = reduce_event_negative_cache_state(
            current=None,
            observation=ProbeObservation(
                event_id=15235535,
                status_phase="inprogress",
                endpoint_pattern="/api/v1/event/{event_id}/statistics",
                observed_at=now,
                http_status=404,
                outcome_classification="not_found",
                next_probe_after=next_probe_after,
                job_type="hydrate_event_edge",
            ),
        )

        self.assertEqual(updated.classification, "c_probation")
        self.assertEqual(updated.next_probe_after, next_probe_after)
        self.assertEqual(updated.last_http_status, 404)

    def test_success_json_clears_probation_and_marks_supported(self) -> None:
        from schema_inspector.event_endpoint_negative_cache import (
            EventEndpointNegativeCacheState,
            ProbeObservation,
            reduce_event_negative_cache_state,
        )

        now = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
        current = EventEndpointNegativeCacheState(
            event_id=15235535,
            status_phase="finished",
            endpoint_pattern="/api/v1/event/{event_id}/highlights",
            classification="c_probation",
            first_negative_at=now - timedelta(hours=2),
            last_negative_at=now - timedelta(hours=1),
            first_success_at=None,
            last_success_at=None,
            suppressed_hits_total=2,
            actual_probe_total=2,
            recheck_iteration=2,
            next_probe_after=now + timedelta(hours=1),
            probe_lease_until=None,
            probe_lease_owner=None,
            last_http_status=404,
            last_outcome_classification="not_found",
            last_job_type="hydrate_special_route",
            last_trace_id="trace-1",
            created_at=now - timedelta(hours=2),
            updated_at=now - timedelta(hours=1),
        )

        updated = reduce_event_negative_cache_state(
            current=current,
            observation=ProbeObservation(
                event_id=15235535,
                status_phase="finished",
                endpoint_pattern=current.endpoint_pattern,
                observed_at=now,
                http_status=200,
                outcome_classification="success_json",
                next_probe_after=None,
                job_type="hydrate_special_route",
            ),
        )

        self.assertEqual(updated.classification, "supported")
        self.assertIsNone(updated.next_probe_after)
        self.assertEqual(updated.last_http_status, 200)
        self.assertEqual(updated.last_outcome_classification, "success_json")

    def test_phase_scoping_does_not_block_live_probe_after_notstarted_404(self) -> None:
        from schema_inspector.event_endpoint_negative_cache import (
            EventEndpointNegativeCache,
            EventEndpointNegativeCacheSettings,
            EventEndpointNegativeCacheState,
        )

        now = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
        pattern = "/api/v1/event/{event_id}/statistics"
        state = EventEndpointNegativeCacheState(
            event_id=15235535,
            status_phase="notstarted",
            endpoint_pattern=pattern,
            classification="c_probation",
            first_negative_at=now - timedelta(minutes=30),
            last_negative_at=now - timedelta(minutes=5),
            first_success_at=None,
            last_success_at=None,
            suppressed_hits_total=1,
            actual_probe_total=1,
            recheck_iteration=1,
            next_probe_after=now + timedelta(minutes=10),
            probe_lease_until=None,
            probe_lease_owner=None,
            last_http_status=404,
            last_outcome_classification="not_found",
            last_job_type="hydrate_event_edge",
            last_trace_id="trace-1",
            created_at=now - timedelta(minutes=30),
            updated_at=now - timedelta(minutes=5),
        )
        gate = EventEndpointNegativeCache(
            repository=_StaticEventNegativeCacheRepository({(15235535, "notstarted", pattern): state}),
            sql_executor=object(),
            now_factory=lambda: now,
            settings=EventEndpointNegativeCacheSettings(mode="enforce"),
        )

        decision = self.run_async(
            gate.decide_event_probe(
                event_id=15235535,
                status_phase="inprogress",
                endpoint_pattern=pattern,
                job_type="hydrate_event_edge",
            )
        )

        self.assertTrue(decision.should_fetch)

    def test_replay_gate_is_scoped_by_job_type(self) -> None:
        from schema_inspector.event_endpoint_negative_cache import EventEndpointNegativeCache, EventEndpointNegativeCacheSettings

        now = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
        pattern = "/api/v1/event/{event_id}/lineups"
        gate = EventEndpointNegativeCache(
            repository=_StaticEventNegativeCacheRepository({}),
            sql_executor=object(),
            now_factory=lambda: now,
            settings=EventEndpointNegativeCacheSettings(mode="enforce"),
        )

        probe_decision = self.run_async(
            gate.decide_event_probe(
                event_id=16077299,
                status_phase="inprogress",
                endpoint_pattern=pattern,
                job_type="hydrate_special_route",
            )
        )
        replay_gate = gate.build_replay_gate()
        same_job_decision = self.run_async(
            replay_gate.decide_event_probe(
                event_id=16077299,
                status_phase="inprogress",
                endpoint_pattern=pattern,
                job_type="hydrate_special_route",
            )
        )
        different_job_decision = self.run_async(
            replay_gate.decide_event_probe(
                event_id=16077299,
                status_phase="inprogress",
                endpoint_pattern=pattern,
                job_type="finalize_event",
            )
        )

        self.assertTrue(probe_decision.should_fetch)
        self.assertTrue(same_job_decision.should_fetch)
        self.assertFalse(different_job_decision.should_fetch)

    def run_async(self, awaitable):
        import asyncio

        return asyncio.run(awaitable)


if __name__ == "__main__":
    unittest.main()
