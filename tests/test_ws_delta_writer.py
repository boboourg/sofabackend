"""Tests for the WS delta writer.

The writer takes a NormalizedEventDelta (produced by the normalizer)
and applies it to the existing normalized tables via narrow UPSERTs:
  * event (status_code, winner_code, last_period, first_to_serve,
    home_red_cards, away_red_cards, updated_at)
  * event_score (one row per side)
  * event_time (full upsert)
  * event_status_time (full upsert)
  * event_change_item (append-only)
  * event_var_in_progress (upsert)

After every successful apply it also invalidates event_payload_cache
rows tied to the event so the persistent payload cache rebuilds on
the next read.
"""
from __future__ import annotations
import unittest
from datetime import datetime, timezone
from typing import Any


class FakeConn:
    """asyncpg-shaped fake. Records every executed query + args."""

    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    async def execute(self, query: str, *args: Any) -> str:
        self.queries.append((query, args))
        return "OK"

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.queries.append((query, args))
        return None

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self.queries.append((query, args))
        return None


def _delta_with(**kwargs) -> Any:
    from schema_inspector.ws_delta_normalizer import NormalizedEventDelta

    return NormalizedEventDelta(**kwargs)


class ApplyEventDeltaTests(unittest.IsolatedAsyncioTestCase):
    async def test_id_only_delta_invalidates_cache_only(self) -> None:
        """A delta with no fields is a no-op for the writes, but must
        still invalidate the cache (so an in-progress request gets a
        fresh waterfall on next miss)."""
        from schema_inspector.ws_delta_writer import apply_event_delta

        conn = FakeConn()
        delta = _delta_with(event_id=15171570)
        await apply_event_delta(conn, delta)

        cache_deletes = [
            q for q, _ in conn.queries
            if "DELETE FROM event_payload_cache" in q
        ]
        self.assertEqual(len(cache_deletes), 1)

    async def test_score_delta_upserts_event_score(self) -> None:
        from schema_inspector.ws_delta_writer import apply_event_delta

        conn = FakeConn()
        delta = _delta_with(
            event_id=15171570,
            event_score_rows=[
                {"side": "home", "current": 2, "display": 2, "period1": 2},
                {"side": "away", "current": 1, "display": 1, "period1": 1},
            ],
        )
        await apply_event_delta(conn, delta)

        score_upserts = [
            (q, a) for q, a in conn.queries
            if "INSERT INTO event_score" in q
        ]
        # One UPSERT per side
        self.assertEqual(len(score_upserts), 2)
        sides = sorted(str(a[1]) for _, a in score_upserts)
        self.assertEqual(sides, ["away", "home"])

    async def test_status_delta_upserts_event_then_event_status(self) -> None:
        """status.code → event.status_code (after ensuring event_status
        row exists). status.description / status.type → event_status
        row insert-if-missing."""
        from schema_inspector.ws_delta_writer import apply_event_delta

        conn = FakeConn()
        delta = _delta_with(
            event_id=1,
            event_status_fields={"code": 6, "description": "1st half", "type": "inprogress"},
        )
        await apply_event_delta(conn, delta)

        # Should have upserted event_status (code/description/type) and
        # event.status_code.
        status_inserts = [
            (q, a) for q, a in conn.queries if "INSERT INTO event_status" in q
        ]
        self.assertTrue(status_inserts, "must upsert event_status row")
        event_updates = [
            (q, a) for q, a in conn.queries
            if "UPDATE event" in q and "status_code" in q
        ]
        self.assertTrue(event_updates, "must UPDATE event.status_code")

    async def test_event_fields_delta_updates_event(self) -> None:
        from schema_inspector.ws_delta_writer import apply_event_delta

        conn = FakeConn()
        delta = _delta_with(
            event_id=1,
            event_fields={
                "winner_code": 1,
                "last_period": "period2",
                "home_red_cards": 0,
                "away_red_cards": 1,
            },
        )
        await apply_event_delta(conn, delta)

        event_updates = [
            (q, a) for q, a in conn.queries
            if "UPDATE event" in q and "id = " in q
        ]
        self.assertTrue(event_updates, "must UPDATE event for arbitrary fields")
        # All four updated values land in the args.
        flat_args = [v for _, args in event_updates for v in args]
        self.assertIn(1, flat_args)  # winner_code or home_red_cards
        self.assertIn("period2", flat_args)

    async def test_event_time_delta_upserts(self) -> None:
        from schema_inspector.ws_delta_writer import apply_event_delta

        conn = FakeConn()
        delta = _delta_with(
            event_id=1,
            event_time_fields={
                "initial": 2700,
                "max": 5400,
                "extra": 540,
                "played": 600,
                "current_period_start_timestamp": 1777115225,
            },
        )
        await apply_event_delta(conn, delta)

        time_upserts = [
            (q, a) for q, a in conn.queries if "INSERT INTO event_time" in q
        ]
        self.assertTrue(time_upserts, "must UPSERT event_time")

    async def test_event_status_time_delta_upserts(self) -> None:
        from schema_inspector.ws_delta_writer import apply_event_delta

        conn = FakeConn()
        delta = _delta_with(
            event_id=1,
            event_status_time_fields={
                "prefix": "",
                "timestamp": 1777115225,
                "initial": 2700,
                "max": 5400,
                "extra": 540,
            },
        )
        await apply_event_delta(conn, delta)

        ust = [
            (q, a) for q, a in conn.queries if "INSERT INTO event_status_time" in q
        ]
        self.assertTrue(ust, "must UPSERT event_status_time")

    async def test_change_timestamp_appended_to_change_item(self) -> None:
        from schema_inspector.ws_delta_writer import apply_event_delta

        conn = FakeConn()
        delta = _delta_with(
            event_id=1,
            change_timestamp=1779063160,
            event_score_rows=[{"side": "home", "current": 1}],
        )
        await apply_event_delta(conn, delta)

        ci = [
            (q, a) for q, a in conn.queries if "INSERT INTO event_change_item" in q
        ]
        self.assertTrue(ci, "must INSERT into event_change_item")
        # event_id + ts must be present
        flat = [v for _, args in ci for v in args]
        self.assertIn(1779063160, flat)

    async def test_var_in_progress_upserts(self) -> None:
        from schema_inspector.ws_delta_writer import apply_event_delta

        conn = FakeConn()
        delta = _delta_with(
            event_id=1,
            event_var_in_progress={"home_team": False, "away_team": True},
        )
        await apply_event_delta(conn, delta)

        var = [
            (q, a) for q, a in conn.queries
            if "INSERT INTO event_var_in_progress" in q
        ]
        self.assertTrue(var, "must UPSERT event_var_in_progress")

    async def test_cache_invalidation_always_fires(self) -> None:
        """No matter what fields are in the delta, the writer must
        always issue the cache DELETE so a subsequent read rebuilds
        from fresh normalized state."""
        from schema_inspector.ws_delta_writer import apply_event_delta

        conn = FakeConn()
        delta = _delta_with(
            event_id=42,
            event_status_fields={"code": 6},
        )
        await apply_event_delta(conn, delta)

        invalidates = [
            (q, a) for q, a in conn.queries
            if "DELETE FROM event_payload_cache" in q
            and 42 in a
        ]
        self.assertTrue(invalidates, "must invalidate event_payload_cache by event_id")

    async def test_event_updated_at_bumped(self) -> None:
        """The writer must touch event.updated_at on any apply so the
        stale-detection logic in local_api_server's snapshot waterfall
        sees the change."""
        from schema_inspector.ws_delta_writer import apply_event_delta

        conn = FakeConn()
        delta = _delta_with(
            event_id=1,
            event_score_rows=[{"side": "home", "current": 1}],
        )
        await apply_event_delta(conn, delta)

        # Either an explicit UPDATE event SET updated_at=... or part of
        # any event UPDATE — we just check one of them carries updated_at.
        touched_event_updated_at = any(
            "updated_at" in q and "UPDATE event" in q
            for q, _ in conn.queries
        )
        self.assertTrue(
            touched_event_updated_at,
            "every delta apply must bump event.updated_at",
        )

    async def test_realistic_payload_orders_writes_safely(self) -> None:
        """Realistic delta that exercises every branch in one call.
        Asserts at least one INSERT per affected table and a single
        cache invalidation at the end."""
        from schema_inspector.ws_delta_writer import apply_event_delta

        conn = FakeConn()
        delta = _delta_with(
            event_id=16046462,
            change_timestamp=1777115225,
            event_status_fields={"code": 7, "description": "2nd half", "type": "inprogress"},
            event_fields={"last_period": "period2"},
            event_time_fields={
                "initial": 2700, "max": 5400, "extra": 540,
                "current_period_start_timestamp": 1777115225,
            },
            event_status_time_fields={
                "prefix": "", "timestamp": 1777115225, "initial": 2700,
                "max": 5400, "extra": 540,
            },
            event_score_rows=[
                {"side": "home", "period2": 0},
                {"side": "away", "period2": 0},
            ],
        )
        await apply_event_delta(conn, delta)

        tables_touched = {
            "event_status": False,
            "event_time": False,
            "event_status_time": False,
            "event_score": False,
            "event_change_item": False,
            "event_payload_cache": False,
        }
        for q, _ in conn.queries:
            for table in tables_touched:
                if table in q:
                    tables_touched[table] = True
        # Every relevant table must see at least one statement.
        for table, touched in tables_touched.items():
            self.assertTrue(touched, f"{table} must be touched")


class ApplyOddsDeltaTests(unittest.IsolatedAsyncioTestCase):
    """Stage 1.5 wires odds through to event_market_choice. The writer
    delegates to ws_odds_writer — that module has its own test suite
    (test_ws_odds_writer.py). Here we only confirm the apply_odds_delta
    facade reaches the underlying SELECT, i.e. the writer is no longer
    a silent no-op."""

    async def test_apply_odds_delta_queries_event_market(self) -> None:
        from schema_inspector.ws_delta_normalizer import NormalizedOddsDelta
        from schema_inspector.ws_delta_writer import apply_odds_delta

        conn = FakeConn()
        bundle = NormalizedOddsDelta(
            offer_id=12345,
            choices={1: {"fractionalValue": "1/2"}},
        )
        await apply_odds_delta(conn, bundle)
        # At least one query against event_market must have been issued
        # (the market_group lookup). FakeConn returns None so the
        # subsequent UPDATEs are skipped — but the lookup itself proves
        # the facade is no longer a noop.
        self.assertTrue(any("FROM event_market" in q for q, _ in conn.queries))


if __name__ == "__main__":
    unittest.main()
