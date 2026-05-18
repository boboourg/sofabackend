"""Tests for the backfill cursor re-seed pipeline.

Context: ``seed_backfill_cursors`` originally pointed every UT at its
``MAX(start_timestamp)`` season — works for active leagues, but for
international cup-style competitions (FIFA WC, EURO, UEFA Nations,
Olympic Games, ...) that season is often a not_started future
edition (e.g. World Cup 2026). The tournament-archive worker has
nothing to hydrate, the cursor never advances, and the entire cat=20
slice of backfill silently stalls.

Fix has two parts:
  E.1 — ``re_seed_stuck_cursors_to_newest_finished_season``: one-shot
        operator method that retargets every cursor whose current
        season has zero finished events to the newest season with at
        least one finished event for the same UT.
  E.3 — ``seed_backfill_cursors_to_oldest_unfinished``: replacement
        semantics for the original seed so future UT registrations
        don't keep falling into the same trap.

This test file pins both APIs.
"""
from __future__ import annotations
import unittest
from typing import Any


class FakeExecutor:
    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self._affected = 0

    async def execute(self, query: str, *args: Any) -> str:
        self.queries.append((query, args))
        return f"UPDATE {self._affected}"


class ReSeedStuckCursorsTests(unittest.IsolatedAsyncioTestCase):
    async def test_method_exists_and_returns_affected_count(self) -> None:
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        repo = TournamentRegistryRepository()
        executor = FakeExecutor()
        executor._affected = 12
        result = await repo.re_seed_stuck_cursors_to_newest_finished_season(
            executor, cat_priority_min=0,
        )
        self.assertEqual(result, 12)

    async def test_sql_filters_to_stuck_cursors_only(self) -> None:
        """The SQL must update ONLY rows whose current next_season_backfill_id
        points at a season with zero finished events (status_code = 100).
        """
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        repo = TournamentRegistryRepository()
        executor = FakeExecutor()
        await repo.re_seed_stuck_cursors_to_newest_finished_season(
            executor, cat_priority_min=0,
        )
        query = executor.queries[0][0]
        # Must reference status_code = 100 in the "stuck" predicate.
        self.assertIn("status_code = 100", query)
        # Must filter by NOT EXISTS / absence of finished events at current cursor.
        self.assertIn("NOT EXISTS", query)

    async def test_sql_targets_only_active_historical_enabled(self) -> None:
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        repo = TournamentRegistryRepository()
        executor = FakeExecutor()
        await repo.re_seed_stuck_cursors_to_newest_finished_season(
            executor, cat_priority_min=0,
        )
        query = executor.queries[0][0]
        self.assertIn("is_active = TRUE", query)
        self.assertIn("historical_enabled = TRUE", query)

    async def test_cat_priority_min_filter(self) -> None:
        """``cat_priority_min`` lets the operator scope the re-seed
        to top categories only (e.g. cat>=6 skips amateur)."""
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        repo = TournamentRegistryRepository()
        executor = FakeExecutor()
        await repo.re_seed_stuck_cursors_to_newest_finished_season(
            executor, cat_priority_min=6,
        )
        query, args = executor.queries[0]
        # The min priority must be parameterised, not interpolated.
        self.assertIn(6, args)
        self.assertIn("c.priority", query)

    async def test_picks_newest_finished_season(self) -> None:
        """The new cursor value must be selected via
        ``ORDER BY MAX(start_timestamp) DESC LIMIT 1`` of finished events
        — same shape as ``seed_backfill_cursors`` but constrained to
        status_code = 100."""
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        repo = TournamentRegistryRepository()
        executor = FakeExecutor()
        await repo.re_seed_stuck_cursors_to_newest_finished_season(
            executor, cat_priority_min=0,
        )
        query = executor.queries[0][0]
        self.assertIn("MAX(e.start_timestamp)", query)
        self.assertIn("DESC", query)


class SeedBackfillCursorsToOldestUnfinishedTests(unittest.IsolatedAsyncioTestCase):
    """E.3 — the **replacement** seed semantics. Uses ``status_code = 100``
    so future registrations point at the newest finished season, not the
    newest future fixture."""

    async def test_method_exists(self) -> None:
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        repo = TournamentRegistryRepository()
        executor = FakeExecutor()
        await repo.seed_backfill_cursors_to_newest_finished_season(
            executor, sport_slug="football",
        )
        self.assertTrue(executor.queries)

    async def test_sql_filters_finished_events_only(self) -> None:
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        repo = TournamentRegistryRepository()
        executor = FakeExecutor()
        await repo.seed_backfill_cursors_to_newest_finished_season(
            executor, sport_slug="football",
        )
        query = executor.queries[0][0]
        self.assertIn("status_code = 100", query)

    async def test_respects_only_uninitialised_flag(self) -> None:
        """When ``only_uninitialised=True``, the UPDATE must only touch
        rows where next_season_backfill_id IS NULL — never clobber an
        in-progress backfill."""
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        repo = TournamentRegistryRepository()
        executor = FakeExecutor()
        await repo.seed_backfill_cursors_to_newest_finished_season(
            executor, sport_slug="football", only_uninitialised=True,
        )
        query = executor.queries[0][0]
        self.assertIn("next_season_backfill_id IS NULL", query)


if __name__ == "__main__":
    unittest.main()
