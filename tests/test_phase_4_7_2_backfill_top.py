"""Phase 4.7.2 backfill (2026-05-23): top-N UT probe orchestration tests.

The backfill script `scripts/backfill_league_capabilities_top.py` walks the
DB to pick (a) the top-N unique tournaments by user_count and (b) for each
the most-recent season that has actual events (bootstrap_state IN
('events_loaded', 'fully_processed')), then probes the finished cohort for
each via ProbeExecutor.

These tests cover the pure SQL helper that does (a)+(b). The probe loop
itself is exercised separately in test_phase_4_4_probe_executor.py with a
fake HTTP client.
"""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, MagicMock


class _FakeRecord(dict):
    """asyncpg.Record-shaped wrapper (supports both index and attr access).
    Probe helper only reads with subscript ``row["name"]``, so dict is enough."""


class _FakeConnContext:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


class _FakeDatabase:
    def __init__(self, rows):
        self._rows = rows
        self._captured_args = []
        self._captured_query = None

    def connection(self):
        async def _fetch(query, *args):
            self._captured_query = query
            self._captured_args = list(args)
            return self._rows

        conn = MagicMock()
        conn.fetch = AsyncMock(side_effect=_fetch)
        return _FakeConnContext(conn)


class FetchTopUtsWithSeasonsTests(unittest.IsolatedAsyncioTestCase):
    """The helper combines a top-N UT pick (ORDER BY user_count DESC) with a
    per-UT latest-season pick (newest events_loaded_at). UTs whose catalog
    has no row with bootstrap_state IN ('events_loaded', 'fully_processed')
    come back with season_id=NULL — the caller is expected to skip them."""

    async def test_returns_rows_in_expected_shape(self) -> None:
        from scripts.backfill_league_capabilities_top import (
            fetch_top_uts_with_seasons,
        )
        db = _FakeDatabase(
            rows=[
                _FakeRecord(
                    unique_tournament_id=7,
                    name="UEFA Champions League",
                    user_count=1305212,
                    season_id=61643,
                    season_year="24/25",
                ),
                _FakeRecord(
                    unique_tournament_id=17,
                    name="Premier League",
                    user_count=1221962,
                    season_id=61627,
                    season_year="24/25",
                ),
            ]
        )

        rows = await fetch_top_uts_with_seasons(db, limit=50)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["unique_tournament_id"], 7)
        self.assertEqual(rows[0]["name"], "UEFA Champions League")
        self.assertEqual(rows[0]["season_id"], 61643)
        self.assertEqual(rows[1]["unique_tournament_id"], 17)

    async def test_passes_limit_as_sql_parameter(self) -> None:
        """LIMIT must be a parameter ($1) — never f-string interpolated —
        so a hostile ``--limit`` value can't inject SQL."""
        from scripts.backfill_league_capabilities_top import (
            fetch_top_uts_with_seasons,
            _TOP_UT_QUERY,
        )
        db = _FakeDatabase(rows=[])

        await fetch_top_uts_with_seasons(db, limit=42)

        self.assertEqual(db._captured_args, [42])
        self.assertIn("$1", _TOP_UT_QUERY)
        self.assertIn("LIMIT $1", _TOP_UT_QUERY)

    async def test_query_filters_by_catalog_bootstrap_state(self) -> None:
        """We only want seasons whose upstream catalog row has actual events
        — bootstrap_state='pending' rows mean no events table data yet, so
        ProbeExecutor._select_sample_events would return nothing and we'd
        burn HTTP budget for free."""
        from scripts.backfill_league_capabilities_top import _TOP_UT_QUERY

        self.assertIn("bootstrap_state", _TOP_UT_QUERY)
        self.assertIn("events_loaded", _TOP_UT_QUERY)
        self.assertIn("fully_processed", _TOP_UT_QUERY)

    async def test_query_orders_by_user_count_desc(self) -> None:
        from scripts.backfill_league_capabilities_top import _TOP_UT_QUERY

        # The top-N pick must rank by user_count (Sofascore-side popularity
        # proxy). NULLS LAST so silent NULLs don't poison the top of the list.
        self.assertIn("user_count DESC", _TOP_UT_QUERY)
        self.assertIn("NULLS LAST", _TOP_UT_QUERY)


if __name__ == "__main__":
    unittest.main()
