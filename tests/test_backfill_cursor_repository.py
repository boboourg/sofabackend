"""Tests for Phase 1 backfill cursor methods on TournamentRegistryRepository."""

from __future__ import annotations

import unittest
from typing import Any

from schema_inspector.storage.tournament_registry_repository import (
    TournamentRegistryRepository,
    _rows_affected,
)


class _FakeExecutor:
    """Minimal in-memory simulation of the asyncpg executor for cursor
    tests. Captures the last query/args and returns scripted rows from
    the ``fetch_returns`` queue.
    """

    def __init__(self, fetch_returns: list[list[dict[str, Any]]] | None = None) -> None:
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.fetched: list[tuple[str, tuple[Any, ...]]] = []
        self._fetch_returns = list(fetch_returns or [])

    async def execute(self, query: str, *args: object) -> str:
        self.executed.append((query, tuple(args)))
        return "UPDATE 7"

    async def fetch(self, query: str, *args: object) -> list[dict[str, Any]]:
        self.fetched.append((query, tuple(args)))
        if not self._fetch_returns:
            return []
        return self._fetch_returns.pop(0)


class RowsAffectedHelperTests(unittest.TestCase):
    def test_parses_command_tag_integers(self) -> None:
        self.assertEqual(_rows_affected("UPDATE 12"), 12)
        self.assertEqual(_rows_affected("INSERT 0 3"), 3)
        self.assertEqual(_rows_affected("DELETE 0"), 0)

    def test_ignores_non_string_inputs(self) -> None:
        self.assertEqual(_rows_affected(None), 0)
        self.assertEqual(_rows_affected(7), 0)
        self.assertEqual(_rows_affected(""), 0)
        self.assertEqual(_rows_affected("SELECT"), 0)


class SeedBackfillCursorsTests(unittest.IsolatedAsyncioTestCase):
    async def test_seed_sets_cursor_only_for_uninitialised_rows_by_default(self) -> None:
        executor = _FakeExecutor()
        repo = TournamentRegistryRepository()

        affected = await repo.seed_backfill_cursors(executor, sport_slug="football")

        self.assertEqual(affected, 7)
        self.assertEqual(len(executor.executed), 1)
        query, args = executor.executed[0]
        self.assertEqual(args, ("football",))
        self.assertIn("tr.next_season_backfill_id IS NULL", query)

    async def test_reseed_flag_skips_uninitialised_guard(self) -> None:
        executor = _FakeExecutor()
        repo = TournamentRegistryRepository()

        await repo.seed_backfill_cursors(
            executor,
            sport_slug=None,
            only_uninitialised=False,
        )

        query, args = executor.executed[0]
        self.assertEqual(args, (None,))
        # No NULL guard when reseed is requested.
        self.assertNotIn("tr.next_season_backfill_id IS NULL", query)


class FetchBackfillCursorTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_int_when_row_exists(self) -> None:
        executor = _FakeExecutor(fetch_returns=[[{"next_season_backfill_id": 76986}]])
        repo = TournamentRegistryRepository()

        result = await repo.fetch_backfill_cursor(
            executor, sport_slug="football", unique_tournament_id=17
        )
        self.assertEqual(result, 76986)

    async def test_returns_none_when_row_is_missing(self) -> None:
        executor = _FakeExecutor(fetch_returns=[[]])
        repo = TournamentRegistryRepository()

        result = await repo.fetch_backfill_cursor(
            executor, sport_slug="football", unique_tournament_id=99999
        )
        self.assertIsNone(result)

    async def test_returns_none_when_cursor_is_null(self) -> None:
        executor = _FakeExecutor(fetch_returns=[[{"next_season_backfill_id": None}]])
        repo = TournamentRegistryRepository()

        result = await repo.fetch_backfill_cursor(
            executor, sport_slug="football", unique_tournament_id=17
        )
        self.assertIsNone(result)


class AdvanceBackfillCursorTests(unittest.IsolatedAsyncioTestCase):
    async def test_advance_returns_next_season_id_from_returning_clause(self) -> None:
        executor = _FakeExecutor(
            fetch_returns=[[{"next_season_backfill_id": 61627}]]
        )
        repo = TournamentRegistryRepository()

        new_cursor = await repo.advance_backfill_cursor(
            executor,
            sport_slug="football",
            unique_tournament_id=17,
            completed_season_id=76986,
        )

        self.assertEqual(new_cursor, 61627)
        # The fetch path was used because the fake executor exposes fetch().
        self.assertEqual(len(executor.fetched), 1)
        query, args = executor.fetched[0]
        self.assertEqual(args, ("football", 17, 76986))
        self.assertIn("RETURNING", query)

    async def test_advance_returns_zero_when_no_older_season_exists(self) -> None:
        executor = _FakeExecutor(
            fetch_returns=[[{"next_season_backfill_id": 0}]]
        )
        repo = TournamentRegistryRepository()

        new_cursor = await repo.advance_backfill_cursor(
            executor,
            sport_slug="football",
            unique_tournament_id=17,
            completed_season_id=49,  # oldest known season
        )

        self.assertEqual(new_cursor, 0)


class ListPendingBackfillCursorsTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_pending_returns_dicts_sorted_by_priority(self) -> None:
        executor = _FakeExecutor(
            fetch_returns=[
                [
                    {
                        "source_slug": "sofascore",
                        "sport_slug": "football",
                        "unique_tournament_id": 17,
                        "priority_rank": 1,
                        "next_season_backfill_id": 76986,
                        "backfill_started_at": None,
                        "backfill_last_advance_at": None,
                        "backfill_completed_at": None,
                    },
                    {
                        "source_slug": "sofascore",
                        "sport_slug": "football",
                        "unique_tournament_id": 8,
                        "priority_rank": 1,
                        "next_season_backfill_id": 77559,
                        "backfill_started_at": None,
                        "backfill_last_advance_at": None,
                        "backfill_completed_at": None,
                    },
                ]
            ]
        )
        repo = TournamentRegistryRepository()

        rows = await repo.list_pending_backfill_cursors(
            executor, sport_slug="football", limit=10
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["unique_tournament_id"], 17)
        query, args = executor.fetched[0]
        self.assertEqual(args, ("football", 10))


if __name__ == "__main__":
    unittest.main()
