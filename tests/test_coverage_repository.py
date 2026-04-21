from __future__ import annotations

from datetime import datetime
import unittest

from schema_inspector.storage.coverage_repository import (
    CoverageLedgerRecord,
    CoverageRepository,
)


class _FakeExecutor:
    def __init__(self, *, fetch_rows: list[dict[str, object]] | None = None) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_rows = list(fetch_rows or [])

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"

    async def fetch(self, query: str, *args: object):
        self.fetch_calls.append((query, args))
        return list(self.fetch_rows)


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
        self.assertIn("COALESCE(EXCLUDED.last_success_at, coverage_ledger.last_success_at)", query)
        self.assertEqual(args[0], "secondary-source")
        self.assertEqual(args[2], "season_structure")
        self.assertEqual(args[4], 17)
        self.assertIsInstance(args[6], float)
        self.assertIsInstance(args[7], datetime)
        self.assertIsInstance(args[8], datetime)

    async def test_repository_selects_event_scope_ids_for_missing_or_partial_coverage(self) -> None:
        repository = CoverageRepository()
        executor = _FakeExecutor(
            fetch_rows=[
                {"scope_id": 901},
                {"scope_id": 777},
            ]
        )

        result = await repository.select_event_scope_ids(
            executor,
            source_slug="sofascore",
            surface_names=("statistics", "lineups"),
            freshness_statuses=("missing", "partial"),
            sport_slug="football",
            limit=25,
            offset=4,
        )

        self.assertEqual(result, (901, 777))
        self.assertEqual(len(executor.fetch_calls), 1)
        query, args = executor.fetch_calls[0]
        self.assertIn("FROM coverage_ledger", query)
        self.assertIn("scope_type = 'event'", query)
        self.assertIn("surface_name = ANY", query)
        self.assertIn("freshness_status = ANY", query)
        self.assertEqual(args[0], "sofascore")
        self.assertEqual(args[1], ("statistics", "lineups"))
        self.assertEqual(args[2], ("missing", "partial"))
        self.assertEqual(args[3], "football")
        self.assertEqual(args[4], 4)
        self.assertEqual(args[5], 25)


if __name__ == "__main__":
    unittest.main()
