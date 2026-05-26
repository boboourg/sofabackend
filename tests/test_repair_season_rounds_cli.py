"""Tests for ``schema_inspector.repair_season_rounds_cli``.

Cover the pure SQL/Python helpers — ``find_missing_rounds`` and
``collect_summary`` — against a stub asyncpg connection. The
subprocess-style ``_run`` orchestration is exercised indirectly via the
helpers; we don't spin up an HTTP fetcher in unit tests.
"""
from __future__ import annotations

import asyncio
import unittest
from typing import Any

from schema_inspector.repair_season_rounds_cli import (
    _CATALOG_SUMMARY_QUERY,
    _EVENTS_SUMMARY_QUERY,
    _FIND_MISSING_ROUNDS_QUERY,
    collect_summary,
    find_missing_rounds,
)


class FakeConnection:
    """Routes asyncpg-style fetch/fetchrow calls to canned responses.

    ``fetch_rows`` is keyed by the SQL constant — we don't try to parse
    SQL, just match on object identity. ``fetchrow_rows`` is the same
    keyed by query for the single-row helpers.
    """

    def __init__(
        self,
        *,
        fetch_rows: dict[str, list[dict[str, Any]]] | None = None,
        fetchrow_rows: dict[str, dict[str, Any] | None] | None = None,
    ) -> None:
        self.fetch_rows = fetch_rows or {}
        self.fetchrow_rows = fetchrow_rows or {}
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.calls.append((query, args))
        return self.fetch_rows.get(query, [])

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.calls.append((query, args))
        return self.fetchrow_rows.get(query)


def _run(coro: Any) -> Any:
    return asyncio.get_event_loop().run_until_complete(coro)


class FindMissingRoundsTests(unittest.TestCase):
    def setUp(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop = loop

    def tearDown(self) -> None:
        self.loop.close()

    def test_returns_empty_when_all_rounds_present(self) -> None:
        connection = FakeConnection(fetch_rows={_FIND_MISSING_ROUNDS_QUERY: []})
        result = _run(
            find_missing_rounds(
                connection,
                unique_tournament_id=17,
                season_id=76986,
            )
        )
        self.assertEqual(result, [])
        # Args propagation
        self.assertEqual(connection.calls[0][1], (17, 76986))

    def test_returns_sorted_missing_round_numbers(self) -> None:
        connection = FakeConnection(
            fetch_rows={
                _FIND_MISSING_ROUNDS_QUERY: [
                    {"round_number": 27},
                    {"round_number": 28},
                    {"round_number": 29},
                    {"round_number": 30},
                    {"round_number": 32},
                    {"round_number": 33},
                    {"round_number": 34},
                    {"round_number": 35},
                    {"round_number": 36},
                ]
            }
        )
        result = _run(
            find_missing_rounds(
                connection,
                unique_tournament_id=17,
                season_id=76986,
            )
        )
        self.assertEqual(result, [27, 28, 29, 30, 32, 33, 34, 35, 36])

    def test_casts_to_int_even_when_db_returns_string(self) -> None:
        # asyncpg returns int but defensive: helper must not pass through
        # whatever the DB layer hands back.
        connection = FakeConnection(
            fetch_rows={
                _FIND_MISSING_ROUNDS_QUERY: [
                    {"round_number": "5"},
                    {"round_number": "6"},
                ]
            }
        )
        result = _run(
            find_missing_rounds(
                connection,
                unique_tournament_id=1,
                season_id=11743,
            )
        )
        self.assertEqual(result, [5, 6])
        self.assertTrue(all(isinstance(r, int) for r in result))


class CollectSummaryTests(unittest.TestCase):
    def setUp(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop = loop

    def tearDown(self) -> None:
        self.loop.close()

    def test_returns_full_summary_when_both_queries_have_rows(self) -> None:
        connection = FakeConnection(
            fetchrow_rows={
                _CATALOG_SUMMARY_QUERY: {
                    "catalog_rounds": 38,
                    "min_round": 1,
                    "max_round": 38,
                },
                _EVENTS_SUMMARY_QUERY: {
                    "event_count": 292,
                    "distinct_rounds": 29,
                },
            }
        )
        result = _run(
            collect_summary(connection, unique_tournament_id=17, season_id=76986)
        )
        self.assertEqual(result["catalog_rounds"], 38)
        self.assertEqual(result["min_round"], 1)
        self.assertEqual(result["max_round"], 38)
        self.assertEqual(result["event_count"], 292)
        self.assertEqual(result["distinct_rounds"], 29)

    def test_zero_when_catalog_missing(self) -> None:
        connection = FakeConnection(
            fetchrow_rows={
                _CATALOG_SUMMARY_QUERY: None,
                _EVENTS_SUMMARY_QUERY: {
                    "event_count": 0,
                    "distinct_rounds": 0,
                },
            }
        )
        result = _run(
            collect_summary(connection, unique_tournament_id=9999, season_id=9999)
        )
        # Defensive defaults — no crash on absent rows.
        self.assertEqual(result["catalog_rounds"], 0)
        self.assertIsNone(result["min_round"])
        self.assertIsNone(result["max_round"])
        self.assertEqual(result["event_count"], 0)


class SqlConstantsTests(unittest.TestCase):
    """Pin the SQL constants — these are the contract with Postgres.

    If any of these are quietly edited, retention / repair queries can
    silently break (the same class of issue that caused 2026-05-26
    incident). Tests give a tripwire.
    """

    def test_missing_rounds_query_uses_anti_join_pattern(self) -> None:
        sql = _FIND_MISSING_ROUNDS_QUERY
        self.assertIn("season_round sr", sql)
        self.assertIn("NOT EXISTS", sql)
        self.assertIn("event_round_info eri", sql)
        self.assertIn("ORDER BY sr.round_number", sql)

    def test_catalog_summary_query_aggregates_season_round(self) -> None:
        sql = _CATALOG_SUMMARY_QUERY
        self.assertIn("FROM season_round", sql)
        self.assertIn("MIN(round_number)", sql)
        self.assertIn("MAX(round_number)", sql)

    def test_events_summary_query_left_joins_event_round_info(self) -> None:
        sql = _EVENTS_SUMMARY_QUERY
        self.assertIn("FROM event e", sql)
        self.assertIn("LEFT JOIN event_round_info eri", sql)
        self.assertIn("COUNT(DISTINCT eri.round_number)", sql)


if __name__ == "__main__":
    unittest.main()
