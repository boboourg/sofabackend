"""Tests for event_payload_cache_repository.

Variant B Stage 1: lazy materialized cache layer for hot read endpoints.
"""
from __future__ import annotations
import json
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any


class FakeConnection:
    """Minimal asyncpg-compatible fake supporting fetchrow / execute /
    fetchval with simple per-query handlers."""

    def __init__(self) -> None:
        self.queries: list[tuple[str, tuple[Any, ...]]] = []
        self._rows: dict[str, list[dict[str, Any]]] = {}

    def stage_row(self, key: str, row: dict[str, Any] | None) -> None:
        self._rows[key] = [row] if row is not None else []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.queries.append((query, args))
        # Routing by substring match for test ergonomics.
        if "SELECT cache_key" in query or "FROM event_payload_cache" in query:
            rows = self._rows.get("read", [])
            return rows[0] if rows else None
        return None

    async def execute(self, query: str, *args: Any) -> str:
        self.queries.append((query, args))
        return "INSERT 0 1"

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.queries.append((query, args))
        return None


class MakeCacheKeyTests(unittest.TestCase):
    def test_event_scoped_key_uses_event_id(self) -> None:
        from schema_inspector.event_payload_cache_repository import make_cache_key

        self.assertEqual(
            make_cache_key("/api/v1/event/{event_id}", "event", 15171570),
            "/api/v1/event/{event_id}|event|15171570",
        )

    def test_sport_scoped_key_uses_sport_slug(self) -> None:
        from schema_inspector.event_payload_cache_repository import make_cache_key

        self.assertEqual(
            make_cache_key("/api/v1/sport/football/events/live", "sport", "football"),
            "/api/v1/sport/football/events/live|sport|football",
        )

    def test_subresource_event_keys_distinct(self) -> None:
        from schema_inspector.event_payload_cache_repository import make_cache_key

        root = make_cache_key("/api/v1/event/{event_id}", "event", 15171570)
        inc = make_cache_key("/api/v1/event/{event_id}/incidents", "event", 15171570)
        self.assertNotEqual(root, inc)


class ReadCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_payload_when_fresh(self) -> None:
        from schema_inspector.event_payload_cache_repository import read_cache

        conn = FakeConnection()
        future = datetime.now(tz=timezone.utc) + timedelta(seconds=5)
        conn.stage_row("read", {
            "cache_key": "/api/v1/event/{event_id}|event|15171570",
            "payload": {"event": {"id": 15171570, "status": {"type": "inprogress"}}},
            "expires_at": future,
            "source_version": {"event_updated_at": "2026-05-18T05:30:00+00:00"},
        })
        result = await read_cache(conn, "/api/v1/event/{event_id}|event|15171570")
        self.assertIsNotNone(result)
        self.assertEqual(result["event"]["id"], 15171570)

    async def test_returns_none_on_miss(self) -> None:
        from schema_inspector.event_payload_cache_repository import read_cache

        conn = FakeConnection()
        conn.stage_row("read", None)
        result = await read_cache(conn, "/api/v1/event/{event_id}|event|9999")
        self.assertIsNone(result)

    async def test_returns_none_when_expired(self) -> None:
        from schema_inspector.event_payload_cache_repository import read_cache

        conn = FakeConnection()
        past = datetime.now(tz=timezone.utc) - timedelta(seconds=10)
        conn.stage_row("read", {
            "cache_key": "k",
            "payload": {"event": {"id": 1}},
            "expires_at": past,
            "source_version": None,
        })
        result = await read_cache(conn, "k")
        self.assertIsNone(result)

    async def test_decodes_payload_string(self) -> None:
        """asyncpg may return JSONB as a string when codec isn't registered."""
        from schema_inspector.event_payload_cache_repository import read_cache

        conn = FakeConnection()
        future = datetime.now(tz=timezone.utc) + timedelta(seconds=5)
        conn.stage_row("read", {
            "cache_key": "k",
            "payload": json.dumps({"event": {"id": 42}}),  # encoded as text
            "expires_at": future,
            "source_version": None,
        })
        result = await read_cache(conn, "k")
        self.assertIsNotNone(result)
        self.assertEqual(result["event"]["id"], 42)


class WriteCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_writes_with_ttl(self) -> None:
        from schema_inspector.event_payload_cache_repository import write_cache

        conn = FakeConnection()
        payload = {"event": {"id": 15171570, "status": {"type": "inprogress"}}}
        await write_cache(
            conn,
            cache_key="/api/v1/event/{event_id}|event|15171570",
            endpoint_pattern="/api/v1/event/{event_id}",
            context_entity_type="event",
            context_entity_id=15171570,
            sport_slug=None,
            payload=payload,
            ttl_seconds=5,
            source_version={"event_updated_at": "2026-05-18T05:30:00+00:00"},
        )
        # At least one INSERT/UPSERT issued.
        write_queries = [q for q, _ in conn.queries if "INSERT INTO event_payload_cache" in q or "event_payload_cache" in q]
        self.assertTrue(write_queries, msg="expected an UPSERT against event_payload_cache")

    async def test_writes_payload_hash(self) -> None:
        """Writer must include payload_hash for write-avoidance."""
        from schema_inspector.event_payload_cache_repository import write_cache

        conn = FakeConnection()
        payload = {"event": {"id": 1}}
        await write_cache(
            conn,
            cache_key="k",
            endpoint_pattern="/api/v1/event/{event_id}",
            context_entity_type="event",
            context_entity_id=1,
            sport_slug=None,
            payload=payload,
            ttl_seconds=5,
            source_version=None,
        )
        write_queries = [q for q, _ in conn.queries if "payload_hash" in q]
        self.assertTrue(write_queries, msg="writer must store payload_hash")


class InvalidateEventTests(unittest.IsolatedAsyncioTestCase):
    async def test_deletes_all_event_scoped_rows(self) -> None:
        from schema_inspector.event_payload_cache_repository import invalidate_event

        conn = FakeConnection()
        await invalidate_event(conn, event_id=15171570)
        deletes = [
            (q, a)
            for q, a in conn.queries
            if "DELETE FROM event_payload_cache" in q
            and "context_entity_type" in q
            and "context_entity_id" in q
        ]
        self.assertTrue(deletes, msg="must issue a DELETE filtered by event")
        # Argument 15171570 must be passed
        any_args = [a for _, a in deletes if 15171570 in a]
        self.assertTrue(any_args, msg="event_id must be passed as a parameter")


class ResolveTtlTests(unittest.TestCase):
    def test_live_status_uses_short_ttl(self) -> None:
        from schema_inspector.event_payload_cache_repository import resolve_ttl_seconds

        self.assertEqual(resolve_ttl_seconds(status_type="inprogress"), 5)

    def test_finished_uses_long_ttl(self) -> None:
        from schema_inspector.event_payload_cache_repository import resolve_ttl_seconds

        self.assertEqual(resolve_ttl_seconds(status_type="finished"), 3600)

    def test_notstarted_uses_medium_ttl(self) -> None:
        from schema_inspector.event_payload_cache_repository import resolve_ttl_seconds

        self.assertEqual(resolve_ttl_seconds(status_type="notstarted"), 30)

    def test_unknown_status_uses_default(self) -> None:
        from schema_inspector.event_payload_cache_repository import resolve_ttl_seconds

        self.assertEqual(resolve_ttl_seconds(status_type=None), 10)
        self.assertEqual(resolve_ttl_seconds(status_type="weird"), 10)


if __name__ == "__main__":
    unittest.main()
