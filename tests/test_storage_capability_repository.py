from __future__ import annotations

from datetime import datetime
import unittest

from schema_inspector.storage.capability_repository import (
    CapabilityObservationRecord,
    CapabilityRepository,
    CapabilityRollupRecord,
)


class _FakeExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"


class CapabilityRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_repository_writes_observation_and_rollup(self) -> None:
        repository = CapabilityRepository()
        executor = _FakeExecutor()

        await repository.insert_observation(
            executor,
            CapabilityObservationRecord(
                sport_slug="handball",
                endpoint_pattern="/api/v1/event/{event_id}/statistics",
                entity_scope="event",
                context_type="live",
                http_status=200,
                payload_validity="valid_json",
                payload_root_keys=("statistics", "periods"),
                is_empty_payload=False,
                is_soft_error_payload=False,
                observed_at="2026-04-16T09:00:00+00:00",
                sample_snapshot_id=77,
            ),
        )
        await repository.upsert_rollup(
            executor,
            CapabilityRollupRecord(
                sport_slug="handball",
                endpoint_pattern="/api/v1/event/{event_id}/statistics",
                support_level="supported",
                confidence=0.95,
                last_success_at="2026-04-16T09:00:00+00:00",
                last_404_at=None,
                last_soft_error_at=None,
                success_count=14,
                not_found_count=0,
                soft_error_count=0,
                empty_count=1,
                notes="stable event statistics route",
            ),
        )

        statements = [sql for sql, _ in executor.execute_calls]
        self.assertTrue(any("INSERT INTO endpoint_capability_observation" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO endpoint_capability_rollup" in sql for sql in statements))
        observation_args = executor.execute_calls[0][1]
        rollup_args = executor.execute_calls[1][1]
        self.assertIsInstance(observation_args[9], datetime)
        self.assertIsInstance(rollup_args[4], datetime)


if __name__ == "__main__":
    unittest.main()
