from __future__ import annotations

from datetime import datetime
import unittest

from schema_inspector.storage.coverage_repository import (
    CoverageLedgerRecord,
    CoverageRepository,
)


class _FakeExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"


class CoverageLedgerTests(unittest.IsolatedAsyncioTestCase):
    def test_coverage_record_tracks_surface_and_scope(self) -> None:
        record = CoverageLedgerRecord(
            source_slug="sofascore",
            sport_slug="football",
            surface_name="season_structure",
            scope_type="unique_tournament",
            scope_id=17,
            freshness_status="fresh",
            completeness_ratio=0.95,
        )

        self.assertEqual(record.surface_name, "season_structure")
        self.assertEqual(record.scope_id, 17)

    async def test_repository_upserts_coverage_ledger_rows(self) -> None:
        repository = CoverageRepository()
        executor = _FakeExecutor()

        await repository.upsert_coverage(
            executor,
            CoverageLedgerRecord(
                source_slug="secondary-source",
                sport_slug="football",
                surface_name="season_structure",
                scope_type="unique_tournament",
                scope_id=17,
                freshness_status="fresh",
                completeness_ratio=0.95,
                last_success_at="2026-04-21T09:00:00+00:00",
                last_checked_at="2026-04-21T09:05:00+00:00",
            ),
        )

        self.assertEqual(len(executor.execute_calls), 1)
        query, args = executor.execute_calls[0]
        self.assertIn("INSERT INTO coverage_ledger", query)
        self.assertIn("ON CONFLICT", query)
        self.assertEqual(args[0], "secondary-source")
        self.assertEqual(args[2], "season_structure")
        self.assertEqual(args[4], 17)
        self.assertIsInstance(args[6], float)
        self.assertIsInstance(args[7], datetime)
        self.assertIsInstance(args[8], datetime)


if __name__ == "__main__":
    unittest.main()
