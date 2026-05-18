"""Tests for the odds-writer half of ws_delta_writer.

Mapping (proven via /event/{id}/odds/1/all snapshots and a 6-day
WS archive — see commit log):

  WS subject = odds.{sport}.1         (always marketId=1 = "Full time")
  WS payload "id"                     = event_market.id
  WS payload "choiceN.fractionalValue"
    when market_group = '1X2':  choices[0]=name'1', choices[1]='X',  choices[2]='2'
    when market_group = 'Home/Away': choices[0]='1', choices[1]='2'

So the writer:
  1. SELECT market_group FROM event_market WHERE id=$1
  2. for each choiceN in the WS payload, resolve target choice name
     from the (market_group, N) → name table
  3. UPDATE event_market_choice SET fractional_value=$,
     initial_fractional_value=COALESCE($, prev) WHERE event_market_id=$
     AND name=$
  4. invalidate cache rows tied to the affected event
"""
from __future__ import annotations
import unittest
from typing import Any


class FakeOddsConn:
    """asyncpg-shaped fake. Captures every query."""

    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self._market_group: str | None = None
        self._event_id: int | None = None

    def stage_market(self, market_group: str | None, event_id: int | None = None) -> None:
        self._market_group = market_group
        self._event_id = event_id

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self.queries.append((query, args))
        if "FROM event_market" in query and "market_group" in query:
            if self._market_group is None:
                return None
            return {"market_group": self._market_group, "event_id": self._event_id}
        return None

    async def execute(self, query: str, *args: Any) -> str:
        self.queries.append((query, args))
        return "UPDATE 1"


class ResolveChoiceMappingTests(unittest.TestCase):
    def test_three_way_1x2(self) -> None:
        from schema_inspector.ws_odds_writer import resolve_choice_name

        self.assertEqual(resolve_choice_name("1X2", 1), "1")
        self.assertEqual(resolve_choice_name("1X2", 2), "X")
        self.assertEqual(resolve_choice_name("1X2", 3), "2")

    def test_two_way_home_away(self) -> None:
        from schema_inspector.ws_odds_writer import resolve_choice_name

        self.assertEqual(resolve_choice_name("Home/Away", 1), "1")
        self.assertEqual(resolve_choice_name("Home/Away", 2), "2")
        self.assertIsNone(resolve_choice_name("Home/Away", 3))

    def test_unknown_market_group_returns_none(self) -> None:
        from schema_inspector.ws_odds_writer import resolve_choice_name

        self.assertIsNone(resolve_choice_name("Asian handicap 1.5", 1))
        self.assertIsNone(resolve_choice_name(None, 1))

    def test_out_of_range_index_returns_none(self) -> None:
        from schema_inspector.ws_odds_writer import resolve_choice_name

        self.assertIsNone(resolve_choice_name("1X2", 0))
        self.assertIsNone(resolve_choice_name("1X2", 4))


class ApplyOddsDeltaTests(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_offer_id_silently_dropped(self) -> None:
        """Per the agreed strategy: drop silently when the market is
        not in our DB. The next polling fetch will populate the row."""
        from schema_inspector.ws_delta_normalizer import NormalizedOddsDelta
        from schema_inspector.ws_odds_writer import apply_odds_delta

        conn = FakeOddsConn()
        conn.stage_market(None)
        bundle = NormalizedOddsDelta(
            offer_id=999999,
            choices={1: {"fractionalValue": "1/2"}},
        )
        await apply_odds_delta(conn, bundle)

        # One SELECT for the market_group, then nothing else.
        update_queries = [q for q, _ in conn.queries if "UPDATE event_market_choice" in q]
        self.assertEqual(update_queries, [])

    async def test_1x2_three_choices_emit_three_updates(self) -> None:
        from schema_inspector.ws_delta_normalizer import NormalizedOddsDelta
        from schema_inspector.ws_odds_writer import apply_odds_delta

        conn = FakeOddsConn()
        conn.stage_market("1X2", event_id=15171570)
        bundle = NormalizedOddsDelta(
            offer_id=310428361,
            choices={
                1: {"fractionalValue": "13/8"},
                2: {"fractionalValue": "11/5"},
                3: {"fractionalValue": "4/9"},
            },
        )
        await apply_odds_delta(conn, bundle)

        update_qs = [
            (q, a) for q, a in conn.queries if "UPDATE event_market_choice" in q
        ]
        self.assertEqual(len(update_qs), 3, "must emit 3 UPDATEs (1, X, 2)")
        # Verify each UPDATE carries its respective choice name.
        # SQL is: UPDATE ... WHERE event_market_id = $1 AND name = $2
        # so args[1] is the name.
        names_seen = sorted([args[1] for _, args in update_qs])
        self.assertEqual(names_seen, ["1", "2", "X"])

    async def test_home_away_two_choices_emit_two_updates(self) -> None:
        from schema_inspector.ws_delta_normalizer import NormalizedOddsDelta
        from schema_inspector.ws_odds_writer import apply_odds_delta

        conn = FakeOddsConn()
        conn.stage_market("Home/Away", event_id=16195558)
        bundle = NormalizedOddsDelta(
            offer_id=310428361,
            choices={
                1: {"fractionalValue": "4/11"},
                2: {"fractionalValue": "2/1"},
            },
        )
        await apply_odds_delta(conn, bundle)

        update_qs = [q for q, _ in conn.queries if "UPDATE event_market_choice" in q]
        self.assertEqual(len(update_qs), 2)

    async def test_home_away_choice3_silently_dropped(self) -> None:
        """If WS ever sends choice3 for a Home/Away market (shouldn't,
        but defensive), we drop it instead of erroring."""
        from schema_inspector.ws_delta_normalizer import NormalizedOddsDelta
        from schema_inspector.ws_odds_writer import apply_odds_delta

        conn = FakeOddsConn()
        conn.stage_market("Home/Away", event_id=1)
        bundle = NormalizedOddsDelta(
            offer_id=1,
            choices={
                1: {"fractionalValue": "1/2"},
                3: {"fractionalValue": "10/1"},  # bogus
            },
        )
        await apply_odds_delta(conn, bundle)

        update_qs = [q for q, _ in conn.queries if "UPDATE event_market_choice" in q]
        # Only the choice1 update was applied
        self.assertEqual(len(update_qs), 1)

    async def test_initial_fractional_value_is_optional(self) -> None:
        from schema_inspector.ws_delta_normalizer import NormalizedOddsDelta
        from schema_inspector.ws_odds_writer import apply_odds_delta

        conn = FakeOddsConn()
        conn.stage_market("1X2", event_id=1)
        bundle = NormalizedOddsDelta(
            offer_id=1,
            choices={
                1: {"fractionalValue": "5/6", "initialFractionalValue": "1/2"},
                2: {"fractionalValue": "7/2"},  # no initial
            },
        )
        await apply_odds_delta(conn, bundle)

        # Two UPDATEs total; the one without initial uses COALESCE so the
        # previous initial value is preserved.
        update_qs = [q for q, _ in conn.queries if "UPDATE event_market_choice" in q]
        self.assertEqual(len(update_qs), 2)
        self.assertTrue(any("COALESCE" in q for q in update_qs))

    async def test_cache_invalidated_for_affected_event(self) -> None:
        """After odds writes succeed for an offer tied to event_id=X,
        any cached payload tied to event_id=X must be invalidated."""
        from schema_inspector.ws_delta_normalizer import NormalizedOddsDelta
        from schema_inspector.ws_odds_writer import apply_odds_delta

        conn = FakeOddsConn()
        conn.stage_market("1X2", event_id=15171570)
        bundle = NormalizedOddsDelta(
            offer_id=310428361,
            choices={1: {"fractionalValue": "13/8"}},
        )
        await apply_odds_delta(conn, bundle)

        invalidates = [
            (q, a) for q, a in conn.queries
            if "DELETE FROM event_payload_cache" in q and 15171570 in a
        ]
        self.assertTrue(invalidates)


if __name__ == "__main__":
    unittest.main()
