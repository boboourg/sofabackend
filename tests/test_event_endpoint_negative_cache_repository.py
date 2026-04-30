from __future__ import annotations

from datetime import UTC, datetime, timedelta
import unittest


class _CapturingExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args):
        self.calls.append((sql, args))
        return []

    async def fetchrow(self, sql: str, *args):
        self.calls.append((sql, args))
        return None

    async def execute(self, sql: str, *args):
        self.calls.append((sql, args))
        return "OK"


class EventEndpointNegativeCacheRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_states_filters_by_event_and_phase(self) -> None:
        from schema_inspector.storage.event_endpoint_negative_cache_repository import EventEndpointNegativeCacheRepository

        repository = EventEndpointNegativeCacheRepository()
        executor = _CapturingExecutor()

        await repository.list_states(
            executor,
            event_id=15235535,
            status_phase="inprogress",
            endpoint_patterns=("/api/v1/event/{event_id}/statistics",),
        )

        sql, args = executor.calls[0]
        self.assertIn("FROM event_endpoint_negative_cache_state", sql)
        self.assertIn("event_id = $1", sql)
        self.assertIn("status_phase = $2", sql)
        self.assertEqual(args[0], 15235535)
        self.assertEqual(args[1], "inprogress")

    async def test_upsert_state_persists_probe_metadata(self) -> None:
        from schema_inspector.event_endpoint_negative_cache import EventEndpointNegativeCacheState
        from schema_inspector.storage.event_endpoint_negative_cache_repository import EventEndpointNegativeCacheRepository

        now = datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
        state = EventEndpointNegativeCacheState(
            event_id=15235535,
            status_phase="finished",
            endpoint_pattern="/api/v1/event/{event_id}/highlights",
            classification="c_probation",
            first_negative_at=now - timedelta(hours=2),
            last_negative_at=now - timedelta(hours=1),
            first_success_at=None,
            last_success_at=None,
            suppressed_hits_total=3,
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
            updated_at=now,
        )
        repository = EventEndpointNegativeCacheRepository()
        executor = _CapturingExecutor()

        await repository._upsert_state(executor, state)

        sql, args = executor.calls[0]
        self.assertIn("INSERT INTO event_endpoint_negative_cache_state", sql)
        self.assertEqual(args[1], 15235535)
        self.assertEqual(args[2], "finished")
        self.assertEqual(args[3], "/api/v1/event/{event_id}/highlights")
        self.assertEqual(args[4], "c_probation")


if __name__ == "__main__":
    unittest.main()
