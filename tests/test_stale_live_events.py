"""Tests for the read-only stale live-event detector (M2)."""
from __future__ import annotations

import unittest

from schema_inspector.ops.stale_live_events import (
    StaleLiveEventDetail,
    StaleLiveEventsSummary,
    collect_stale_live_events_summary,
    collect_top_stale_live_events,
    format_stale_live_events_report,
)


class _FakeRow(dict):
    """Behaves like an asyncpg row: supports both index and key access."""

    def __getitem__(self, key):
        return super().__getitem__(key)


class _FakeConnection:
    """Minimal asyncpg-like fake. Returns canned rows and records the
    SQL + arg tuples passed in so we can assert the queries are
    parametrised correctly (no string interpolation, no SQL injection)."""

    def __init__(self, *, summary_row=None, detail_rows=None):
        self._summary_row = summary_row
        self._detail_rows = detail_rows or []
        self.calls: list[tuple[str, tuple]] = []

    async def fetchrow(self, query, *args):
        self.calls.append((query, args))
        return self._summary_row

    async def fetch(self, query, *args):
        self.calls.append((query, args))
        return list(self._detail_rows)


class CollectStaleLiveEventsSummaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_summary_unpacks_row_into_dataclass(self) -> None:
        row = _FakeRow(
            active_total=581,
            inprogress_total=43,
            stale_60s=581,
            stale_180s=506,
            stale_300s=297,
            stale_600s=5,
            stale_1200s=1,
            inprogress_stale_60s=43,
            inprogress_stale_180s=43,
            inprogress_stale_300s=31,
            inprogress_stale_600s=5,
            inprogress_stale_1200s=1,
        )
        connection = _FakeConnection(summary_row=row)

        summary = await collect_stale_live_events_summary(connection)

        self.assertEqual(summary.active_total, 581)
        self.assertEqual(summary.inprogress_total, 43)
        self.assertEqual(summary.inprogress_stale_300s, 31)
        self.assertEqual(summary.inprogress_stale_600s, 5)
        # Inprogress codes are passed via parameter, never inlined into the SQL.
        # That is the key correctness invariant — terminal-status events
        # (100/110/120/60/70) must be excluded by the WHERE clause, not by
        # string substitution.
        self.assertEqual(len(connection.calls), 1)
        _query, args = connection.calls[0]
        inprogress_codes = args[0]
        self.assertEqual(set(inprogress_codes), {6, 7, 8, 31, 91, 92, 93})

    async def test_summary_handles_empty_universe(self) -> None:
        connection = _FakeConnection(summary_row=None)
        summary = await collect_stale_live_events_summary(connection)
        self.assertEqual(summary.active_total, 0)
        self.assertEqual(summary.inprogress_stale_300s, 0)


class CollectTopStaleLiveEventsTests(unittest.IsolatedAsyncioTestCase):
    async def test_top_returns_detail_rows_with_counts(self) -> None:
        rows = [
            _FakeRow(
                event_id=15345874,
                age_sec=1320,
                status_code=6,
                sport_slug="football",
                unique_tournament_name="J1 League",
                success_count=18,
                retry_count=5,
                fail_count=0,
            ),
            _FakeRow(
                event_id=16114066,
                age_sec=300,
                status_code=8,
                sport_slug="football",
                unique_tournament_name="A-League Men",
                success_count=4,
                retry_count=3,
                fail_count=0,
            ),
        ]
        connection = _FakeConnection(detail_rows=rows)

        top = await collect_top_stale_live_events(
            connection, threshold_seconds=180, limit=10
        )

        self.assertEqual(len(top), 2)
        self.assertEqual(top[0].event_id, 15345874)
        self.assertEqual(top[0].age_sec, 1320)
        self.assertEqual(top[0].retry_count, 5)
        self.assertEqual(top[0].sport_slug, "football")
        self.assertEqual(top[0].unique_tournament_name, "J1 League")
        # Threshold + inprogress flag + status codes + limit are all
        # parameterised — no f-string interpolation.
        _query, args = connection.calls[0]
        self.assertEqual(args[0], 180)
        self.assertTrue(args[1])  # inprogress_only default
        self.assertEqual(set(args[2]), {6, 7, 8, 31, 91, 92, 93})
        self.assertEqual(args[3], 10)

    async def test_top_all_statuses_passes_inprogress_only_false(self) -> None:
        connection = _FakeConnection(detail_rows=[])
        await collect_top_stale_live_events(
            connection, threshold_seconds=600, limit=5, inprogress_only=False
        )
        _query, args = connection.calls[0]
        self.assertFalse(args[1])

    async def test_top_handles_null_columns(self) -> None:
        # Some events may have no sport_slug / tournament name yet
        # (e.g. just promoted from scheduled, no /event snapshot ingested
        # — though the WHERE in the SQL would filter those out, the
        # data-class layer must still accept None gracefully).
        rows = [
            _FakeRow(
                event_id=99999,
                age_sec=400,
                status_code=None,
                sport_slug=None,
                unique_tournament_name=None,
                success_count=0,
                retry_count=0,
                fail_count=0,
            )
        ]
        connection = _FakeConnection(detail_rows=rows)
        top = await collect_top_stale_live_events(
            connection, threshold_seconds=300, limit=1
        )
        self.assertEqual(top[0].status_code, None)
        self.assertEqual(top[0].sport_slug, None)
        self.assertEqual(top[0].unique_tournament_name, None)


class FormatStaleLiveEventsReportTests(unittest.TestCase):
    def test_format_includes_summary_counters_and_top_table(self) -> None:
        summary = StaleLiveEventsSummary(
            active_total=581,
            inprogress_total=43,
            stale_60s=581,
            stale_180s=506,
            stale_300s=297,
            stale_600s=5,
            stale_1200s=1,
            inprogress_stale_60s=43,
            inprogress_stale_180s=43,
            inprogress_stale_300s=31,
            inprogress_stale_600s=5,
            inprogress_stale_1200s=1,
        )
        top = (
            StaleLiveEventDetail(
                event_id=15345874,
                age_sec=1320,
                status_code=6,
                sport_slug="football",
                unique_tournament_name="J1 League",
                success_count=18,
                retry_count=5,
                fail_count=0,
            ),
        )

        out = format_stale_live_events_report(
            summary, top, threshold_seconds=300, inprogress_only=True
        )

        self.assertIn("active=581", out)
        self.assertIn("inprogress=43", out)
        self.assertIn("inprogress_stale_300s=31", out)
        self.assertIn("inprogress_stale_600s=5", out)
        self.assertIn("15345874", out)
        self.assertIn("J1 League", out)
        self.assertIn("retry", out.lower())
        self.assertIn("inprogress only", out)

    def test_format_omits_top_section_when_no_stale_events(self) -> None:
        summary = StaleLiveEventsSummary(
            active_total=10,
            inprogress_total=2,
            stale_60s=0,
            stale_180s=0,
            stale_300s=0,
            stale_600s=0,
            stale_1200s=0,
            inprogress_stale_60s=0,
            inprogress_stale_180s=0,
            inprogress_stale_300s=0,
            inprogress_stale_600s=0,
            inprogress_stale_1200s=0,
        )
        out = format_stale_live_events_report(
            summary, top=(), threshold_seconds=300, inprogress_only=True
        )
        self.assertIn("active=10", out)
        self.assertNotIn("top ", out)
        self.assertNotIn("event_id", out)


if __name__ == "__main__":
    unittest.main()
