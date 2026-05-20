"""TDD tests for Stage 2.2 (2026-05-20): player team timeline read-path.

The hard column ``player.team_id`` overwrites on every upsert, so it
returns the player's CURRENT club regardless of the historical request.
For point-in-time correctness we reconstruct the player→team binding
from ``event_lineup_player`` (per-match snapshot) joined to ``event``
for the timestamp axis.

Existing index ``idx_event_lineup_player_player_id_event_id``
(migrations/2026-05-18) makes the lookup an index scan on a star
player's ~500 lineup rows + nested-loop event PK → 1-5 ms.
"""

from __future__ import annotations

import unittest


class _FakeExecutor:
    """Capture issued SQL + canned row results."""

    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_results: list[dict[str, object] | None] = []

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        self.fetchrow_calls.append((query, args))
        if self.fetchrow_results:
            return self.fetchrow_results.pop(0)
        return None


class PlayerHistoryRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_lookup_returns_team_from_most_recent_lineup(self) -> None:
        from schema_inspector.storage.player_history_repository import lookup_player_team_at

        executor = _FakeExecutor()
        # Latest lineup row for the player carries team_id=42 directly.
        executor.fetchrow_results = [
            {"team_id": 42, "side": "home", "home_team_id": 42, "away_team_id": 99},
        ]

        result = await lookup_player_team_at(executor, player_id=1001)

        self.assertEqual(result, 42)
        # SQL must use the (player_id, event_id) composite index.
        self.assertEqual(len(executor.fetchrow_calls), 1)
        sql, args = executor.fetchrow_calls[0]
        self.assertIn("event_lineup_player", sql)
        self.assertIn("player_id", sql)
        self.assertIn("ORDER BY", sql.upper())
        self.assertEqual(args[0], 1001)

    async def test_lookup_filters_by_at_timestamp_when_provided(self) -> None:
        from schema_inspector.storage.player_history_repository import lookup_player_team_at

        executor = _FakeExecutor()
        # Result: at this timestamp the player was on team=200 (Real Madrid).
        executor.fetchrow_results = [
            {"team_id": 200, "side": "away", "home_team_id": 50, "away_team_id": 200},
        ]

        result = await lookup_player_team_at(
            executor, player_id=1001, at_timestamp=1_700_000_000
        )

        self.assertEqual(result, 200)
        sql, args = executor.fetchrow_calls[0]
        # Timestamp must be one of the bind args.
        self.assertIn(1_700_000_000, args)
        # And the SQL must contain a start_timestamp filter.
        self.assertIn("start_timestamp", sql)

    async def test_lookup_falls_back_to_event_side_team_when_lineup_team_id_null(self) -> None:
        """Older ingests sometimes have ``event_lineup_player.team_id IS
        NULL``. In that case the team is inferable from the event's
        home/away team via the ``side`` column."""
        from schema_inspector.storage.player_history_repository import lookup_player_team_at

        executor = _FakeExecutor()
        executor.fetchrow_results = [
            {"team_id": None, "side": "home", "home_team_id": 7, "away_team_id": 8},
        ]

        result = await lookup_player_team_at(executor, player_id=1001)

        self.assertEqual(
            result, 7,
            msg=(
                "When elp.team_id is NULL the lookup must fall back to "
                "event.home_team_id (side=home) or event.away_team_id "
                "(side=away). This guarantees coverage on pre-2026 ingests "
                "where team_id was not yet populated in lineup rows."
            ),
        )

    async def test_lookup_returns_none_when_player_has_no_lineup_rows(self) -> None:
        from schema_inspector.storage.player_history_repository import lookup_player_team_at

        executor = _FakeExecutor()
        # No row → fetchrow returns None.
        executor.fetchrow_results = [None]

        result = await lookup_player_team_at(executor, player_id=999999)

        self.assertIsNone(
            result,
            msg=(
                "Player with no lineup history (very fresh account, "
                "scraped from a transfer rumor) must return None — the "
                "caller decides whether to fall back to player.team_id "
                "or omit the team key entirely."
            ),
        )


if __name__ == "__main__":
    unittest.main()
