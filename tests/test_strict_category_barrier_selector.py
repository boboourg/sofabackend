"""Strict-barrier backfill selector: returns jobs ONLY from the
highest pending ``category.priority`` bucket.

This is the production fix for the regression where
``ORDER BY tr.priority_rank, tr.unique_tournament_id`` allowed cat=0
amateur leagues (National League South, Spain Tercera, etc.) into the
same planner tick as cat=20 international tournaments (FIFA WC,
Champions League). With ~5,280 cat=0 UTs vs 418 cat>=6 UTs, the
amateur swamp was effectively delaying every meaningful tournament.

The new method drains by ``category.priority`` strictly: until every
UT in cat=20 has its cursor cleared, no jobs from cat=19 are picked.
Within the active bucket we still sort by ``tr.priority_rank`` then
``ut.user_count DESC`` so the most-watched UTs come first.
"""
from __future__ import annotations
import unittest
from typing import Any


class FakeExecutor:
    """asyncpg-shaped fake that returns staged rows for ``fetch``."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.queries.append((query, args))
        return list(self._rows)


class StrictCategoryBarrierSelectorTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_only_highest_cat_priority_bucket(self) -> None:
        """When cat=20 has pending UTs, the result must contain ONLY
        cat=20 rows — even though cat=19, cat=10, cat=0 also have work."""
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        # Simulate the SQL having already filtered to MAX(c.priority).
        # The DB returns three cat=20 rows for an active tick.
        executor = FakeExecutor(rows=[
            {"unique_tournament_id": 16, "next_season_backfill_id": 58210,
             "priority_rank": 1, "category_priority": 20, "category_name": "World",
             "source_slug": "sofascore", "sport_slug": "football"},
            {"unique_tournament_id": 1, "next_season_backfill_id": 56953,
             "priority_rank": 1, "category_priority": 20, "category_name": "World",
             "source_slug": "sofascore", "sport_slug": "football"},
        ])
        repo = TournamentRegistryRepository()
        rows = await repo.select_pending_cursors_by_top_category(
            executor, sport_slug="football", limit=10,
        )

        self.assertEqual(len(rows), 2)
        cats = {r["category_priority"] for r in rows}
        self.assertEqual(cats, {20})

    async def test_respects_limit(self) -> None:
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        executor = FakeExecutor(rows=[
            {"unique_tournament_id": i, "next_season_backfill_id": 100 + i,
             "priority_rank": i, "category_priority": 20,
             "category_name": "World", "source_slug": "sofascore",
             "sport_slug": "football"}
            for i in range(1, 6)
        ])
        repo = TournamentRegistryRepository()
        await repo.select_pending_cursors_by_top_category(
            executor, sport_slug="football", limit=3,
        )
        # The LIMIT clause must be parameterised — verify the arg list.
        _, args = executor.queries[0]
        self.assertIn(3, args)

    async def test_filters_by_sport_slug(self) -> None:
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        executor = FakeExecutor(rows=[])
        repo = TournamentRegistryRepository()
        await repo.select_pending_cursors_by_top_category(
            executor, sport_slug="basketball", limit=10,
        )
        query, args = executor.queries[0]
        self.assertIn("basketball", args)
        self.assertIn("tr.sport_slug =", query)

    async def test_query_orders_by_priority_rank_then_user_count(self) -> None:
        """Within the active cat-priority bucket, top UT first (most
        watched), then less-followed competitions."""
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        executor = FakeExecutor(rows=[])
        repo = TournamentRegistryRepository()
        await repo.select_pending_cursors_by_top_category(
            executor, sport_slug="football", limit=10,
        )
        query = executor.queries[0][0]
        # tier-1 ordering: priority_rank ASC, user_count DESC
        self.assertIn("priority_rank", query)
        self.assertIn("user_count DESC", query)

    async def test_query_resolves_highest_cat_priority_via_cte(self) -> None:
        """The SQL must compute MAX(category.priority) over pending
        cursors in a CTE / subquery — not hardcoded."""
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        executor = FakeExecutor(rows=[])
        repo = TournamentRegistryRepository()
        await repo.select_pending_cursors_by_top_category(
            executor, sport_slug="football", limit=10,
        )
        query = executor.queries[0][0]
        # The SQL must reference MAX(c.priority) over a join on
        # tournament_registry + category constrained to pending cursors.
        self.assertIn("MAX(c.priority)", query)
        self.assertIn("next_season_backfill_id", query)

    async def test_returns_empty_when_no_pending_work(self) -> None:
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        executor = FakeExecutor(rows=[])
        repo = TournamentRegistryRepository()
        rows = await repo.select_pending_cursors_by_top_category(
            executor, sport_slug="football", limit=10,
        )
        self.assertEqual(rows, [])

    async def test_legacy_method_still_works_for_ops_view(self) -> None:
        """``list_pending_backfill_cursors`` (used by /ops/backfill-cursor)
        must NOT change behaviour — the strict barrier is a separate
        planner-side method."""
        from schema_inspector.storage.tournament_registry_repository import (
            TournamentRegistryRepository,
        )

        executor = FakeExecutor(rows=[])
        repo = TournamentRegistryRepository()
        await repo.list_pending_backfill_cursors(
            executor, sport_slug="football", limit=10,
        )
        query = executor.queries[0][0]
        # Legacy: ORDER BY tr.priority_rank, tr.unique_tournament_id —
        # NOT MAX(c.priority).
        self.assertIn("ORDER BY", query)
        self.assertNotIn("MAX(c.priority)", query)


if __name__ == "__main__":
    unittest.main()
