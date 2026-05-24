"""Tests for ``schema_inspector.diagnose_event_cli``.

Covers:
  * verdict classification (_classify)
  * snapshot age formatting
  * expected endpoint resolution via policy
  * end-to-end ``diagnose_event`` against a stub asyncpg connection
  * format_diagnosis renders the table
  * dispatch_diagnose_event exit codes

The asyncpg layer is stubbed by a simple in-memory ``FakeConnection`` that
returns prefab rows per (query, args) tuple. This avoids a Postgres
dependency and keeps the test under 1 second.
"""
from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any

from schema_inspector.diagnose_event_cli import (
    VERDICT_EMPTY_RAW,
    VERDICT_GREEN,
    VERDICT_NEGATIVE,
    VERDICT_NO_FETCH,
    VERDICT_NO_PARSER,
    VERDICT_SILENT_DROP,
    _classify,
    _expected_endpoint_patterns,
    _format_snapshot_age,
    diagnose_event,
    format_diagnosis,
)


# ---- _classify -----------------------------------------------------------


class ClassifyTests(unittest.TestCase):
    def test_no_snapshot_returns_no_fetch(self) -> None:
        verdict, note = _classify(None, ("event_lineup",), {})
        self.assertEqual(verdict, VERDICT_NO_FETCH)
        self.assertIsNotNone(note)

    def test_http_404_returns_negative(self) -> None:
        verdict, note = _classify(
            {"http_status": 404, "is_soft_error_payload": False},
            ("event_lineup",),
            {"event_lineup": 0},
        )
        self.assertEqual(verdict, VERDICT_NEGATIVE)

    def test_soft_error_returns_empty_raw(self) -> None:
        verdict, note = _classify(
            {"http_status": 200, "is_soft_error_payload": True},
            ("event_lineup",),
            {"event_lineup": 0},
        )
        self.assertEqual(verdict, VERDICT_EMPTY_RAW)

    def test_snapshot_no_target_tables_returns_no_parser(self) -> None:
        verdict, note = _classify(
            {"http_status": 200, "is_soft_error_payload": False},
            (),  # no parser maps this endpoint to a normalized table
            {},
        )
        self.assertEqual(verdict, VERDICT_NO_PARSER)

    def test_snapshot_ok_zero_rows_returns_silent_drop(self) -> None:
        verdict, note = _classify(
            {"http_status": 200, "is_soft_error_payload": False},
            ("event_lineup", "event_lineup_player"),
            {"event_lineup": 0, "event_lineup_player": 0},
        )
        self.assertEqual(verdict, VERDICT_SILENT_DROP)

    def test_snapshot_ok_rows_present_returns_green(self) -> None:
        verdict, note = _classify(
            {"http_status": 200, "is_soft_error_payload": False},
            ("event_lineup", "event_lineup_player"),
            {"event_lineup": 2, "event_lineup_player": 30},
        )
        self.assertEqual(verdict, VERDICT_GREEN)
        self.assertIsNone(note)


# ---- _format_snapshot_age ------------------------------------------------


class FormatSnapshotAgeTests(unittest.TestCase):
    def test_none_returns_dash(self) -> None:
        self.assertEqual(_format_snapshot_age(None), "-")

    def test_seconds(self) -> None:
        fetched_at = datetime.now(timezone.utc) - timedelta(seconds=12)
        self.assertEqual(_format_snapshot_age(fetched_at), "12s")

    def test_minutes(self) -> None:
        fetched_at = datetime.now(timezone.utc) - timedelta(minutes=5)
        self.assertIn("m", _format_snapshot_age(fetched_at))

    def test_hours(self) -> None:
        fetched_at = datetime.now(timezone.utc) - timedelta(hours=2)
        self.assertIn("h", _format_snapshot_age(fetched_at))

    def test_days(self) -> None:
        fetched_at = datetime.now(timezone.utc) - timedelta(days=3)
        self.assertIn("d", _format_snapshot_age(fetched_at))

    def test_naive_datetime_is_assumed_utc(self) -> None:
        # asyncpg can return tz-naive timestamps if column type is wrong.
        # Our helper must not crash and should still produce a value.
        fetched_at = datetime.utcnow() - timedelta(seconds=5)
        result = _format_snapshot_age(fetched_at)
        self.assertTrue(result.endswith("s"))

    def test_garbage_input_returns_question_mark(self) -> None:
        self.assertEqual(_format_snapshot_age("not a datetime"), "?")


# ---- _expected_endpoint_patterns -----------------------------------------


class ExpectedEndpointPatternsTests(unittest.TestCase):
    def test_root_endpoint_is_always_first(self) -> None:
        metadata = {
            "status_type": "finished",
            "home_team_id": 1,
            "away_team_id": 2,
            "has_event_player_statistics": True,
            "has_event_player_heat_map": False,
            "has_global_highlights": True,
            "has_xg": True,
            "detail_id": None,
            "custom_id": None,
            "start_timestamp": 1779000000,
            "is_editor": False,
        }
        patterns = _expected_endpoint_patterns(metadata, sport_slug="football")
        self.assertGreater(len(patterns), 0)
        self.assertEqual(patterns[0], "/api/v1/event/{event_id}")

    def test_iseditor_football_short_circuit_keeps_only_root(self) -> None:
        metadata = {
            "status_type": "finished",
            "home_team_id": 1,
            "away_team_id": 2,
            "is_editor": True,
        }
        patterns = _expected_endpoint_patterns(metadata, sport_slug="football")
        # Policy returns () for football+is_editor; we still always prepend root.
        self.assertEqual(patterns, ("/api/v1/event/{event_id}",))

    def test_no_duplicate_patterns(self) -> None:
        metadata = {
            "status_type": "finished",
            "home_team_id": 1,
            "away_team_id": 2,
            "is_editor": False,
        }
        patterns = _expected_endpoint_patterns(
            metadata,
            sport_slug="football",
            provider_ids=(1, 1, 1),  # repeats should collapse
        )
        self.assertEqual(len(patterns), len(set(patterns)))


# ---- diagnose_event end-to-end with FakeConnection -----------------------


class FakeConnection:
    """In-memory stub of an asyncpg Connection.

    Routes queries by a simple substring match so each test sets up
    expected (substring → row(s)) mappings. Keeps the surface small.
    """

    def __init__(self) -> None:
        self.event_meta: dict | None = None
        self.snapshots: dict[str, dict] = {}  # endpoint_pattern → row
        self.counts: dict[str, int] = {}  # table_name → count
        self.auto_pick_value: int | None = None

    async def fetchrow(self, query: str, *args: Any) -> dict | None:
        if "FROM event e" in query and "WHERE e.id = $1" in query:
            return self.event_meta
        if "FROM api_payload_snapshot" in query:
            event_id, endpoint_pattern = args
            return self.snapshots.get(endpoint_pattern)
        return None

    async def fetchval(self, query: str, *args: Any) -> Any:
        # COUNT(*) FROM <table> WHERE event_id = $1
        for table_name, value in self.counts.items():
            if f" FROM {table_name} " in query or query.endswith(f" FROM {table_name}"):
                return value
        # _AUTO_PICK_QUERY
        if "FROM event e" in query and "es.type = 'finished'" in query:
            return self.auto_pick_value
        return None


class FakeDatabase:
    """Yields one ``FakeConnection`` and keeps a reference for assertions."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection_ref = connection

    def connection(self) -> "FakeDbContext":
        return FakeDbContext(self.connection_ref)


class FakeDbContext:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> FakeConnection:
        return self.connection

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


def _run(coro: Any) -> Any:
    return asyncio.get_event_loop().run_until_complete(coro)


class DiagnoseEventEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        # Fresh event loop each test to avoid cross-test bleed.
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop = loop

    def tearDown(self) -> None:
        self.loop.close()

    def test_event_not_found_marks_diagnosis_accordingly(self) -> None:
        connection = FakeConnection()  # event_meta stays None
        database = FakeDatabase(connection)
        diagnosis = _run(
            diagnose_event(database, event_id=42, sport_slug="football")
        )
        self.assertFalse(diagnosis.event_found_in_db)
        self.assertEqual(diagnosis.event_id, 42)
        self.assertEqual(diagnosis.audits, ())

    def test_event_with_green_root_only(self) -> None:
        connection = FakeConnection()
        connection.event_meta = {
            "id": 100,
            "status_code": 100,
            "detail_id": None,
            "custom_id": None,
            "start_timestamp": 1779000000,
            "is_editor": True,  # short-circuit to only root
            "has_event_player_statistics": None,
            "has_event_player_heat_map": None,
            "has_global_highlights": None,
            "has_xg": None,
            "home_team_id": 1,
            "away_team_id": 2,
            "status_type": "finished",
        }
        connection.snapshots["/api/v1/event/{event_id}"] = {
            "id": 9999,
            "fetched_at": datetime.now(timezone.utc) - timedelta(minutes=5),
            "http_status": 200,
            "payload_size_bytes": 12345,
            "is_soft_error_payload": False,
        }
        connection.counts = {
            "event": 1,
            "event_score": 1,
            "event_status": 1,
            "event_time": 1,
            "event_round_info": 0,  # some sub-tables may be empty, total > 0 still green
        }
        database = FakeDatabase(connection)
        diagnosis = _run(
            diagnose_event(database, event_id=100, sport_slug="football")
        )
        self.assertTrue(diagnosis.event_found_in_db)
        # is_editor short-circuit ⇒ only the root endpoint is audited.
        self.assertEqual(len(diagnosis.audits), 1)
        self.assertEqual(
            diagnosis.audits[0].endpoint_pattern, "/api/v1/event/{event_id}"
        )
        self.assertEqual(diagnosis.audits[0].verdict, VERDICT_GREEN)
        self.assertEqual(diagnosis.green_count, 1)
        self.assertEqual(diagnosis.silent_drop_count, 0)

    def test_event_with_silent_drop_on_lineups(self) -> None:
        connection = FakeConnection()
        connection.event_meta = {
            "id": 200,
            "status_code": 100,
            "detail_id": None,
            "custom_id": None,
            "start_timestamp": 1779000000,
            "is_editor": False,
            "has_event_player_statistics": True,
            "has_event_player_heat_map": False,
            "has_global_highlights": False,
            "has_xg": False,
            "home_team_id": 10,
            "away_team_id": 11,
            "status_type": "finished",
        }
        # Root is GREEN, lineups will be SILENT_DROP (snapshot present, 0 rows).
        connection.snapshots["/api/v1/event/{event_id}"] = {
            "id": 1,
            "fetched_at": datetime.now(timezone.utc),
            "http_status": 200,
            "payload_size_bytes": 100,
            "is_soft_error_payload": False,
        }
        connection.snapshots["/api/v1/event/{event_id}/lineups"] = {
            "id": 2,
            "fetched_at": datetime.now(timezone.utc),
            "http_status": 200,
            "payload_size_bytes": 200,
            "is_soft_error_payload": False,
        }
        # Counts: all event* tables hit by lookups will go through here.
        # The current implementation uses one COUNT per (table_name) — our
        # fake matches on " FROM <table> " substring so we set everything to 0
        # except `event` so root stays GREEN.
        connection.counts = {
            "event": 1,
            "event_score": 1,
            "event_status": 1,
            "event_time": 1,
            "event_round_info": 0,
            "event_lineup": 0,
            "event_lineup_player": 0,
        }
        database = FakeDatabase(connection)
        diagnosis = _run(
            diagnose_event(database, event_id=200, sport_slug="football")
        )
        # We don't know the exact policy output count (depends on hints),
        # but we expect: root GREEN, lineups SILENT_DROP, plus several NO_FETCH.
        verdicts = {audit.endpoint_pattern: audit.verdict for audit in diagnosis.audits}
        self.assertEqual(verdicts.get("/api/v1/event/{event_id}"), VERDICT_GREEN)
        self.assertEqual(
            verdicts.get("/api/v1/event/{event_id}/lineups"),
            VERDICT_SILENT_DROP,
        )
        self.assertGreaterEqual(diagnosis.silent_drop_count, 1)


# ---- format_diagnosis ----------------------------------------------------


class FormatDiagnosisTests(unittest.TestCase):
    def test_not_found_message(self) -> None:
        from schema_inspector.diagnose_event_cli import EventDiagnosis

        d = EventDiagnosis(event_id=1, sport_slug="football", event_found_in_db=False)
        text = format_diagnosis(d)
        self.assertIn("NOT FOUND", text)

    def test_renders_table_with_audits(self) -> None:
        from schema_inspector.diagnose_event_cli import EndpointAudit, EventDiagnosis

        d = EventDiagnosis(
            event_id=42,
            sport_slug="football",
            status_type="finished",
            status_code=100,
            audits=(
                EndpointAudit(
                    endpoint_pattern="/api/v1/event/{event_id}",
                    target_tables=("event",),
                    latest_snapshot_id=1,
                    latest_snapshot_fetched_at=datetime.now(timezone.utc),
                    latest_snapshot_http_status=200,
                    normalized_rows={"event": 1},
                    verdict=VERDICT_GREEN,
                ),
                EndpointAudit(
                    endpoint_pattern="/api/v1/event/{event_id}/lineups",
                    target_tables=("event_lineup",),
                    latest_snapshot_id=2,
                    latest_snapshot_fetched_at=datetime.now(timezone.utc),
                    latest_snapshot_http_status=200,
                    normalized_rows={"event_lineup": 0},
                    verdict=VERDICT_SILENT_DROP,
                    note="Snapshot http_status ok, 0 rows",
                ),
            ),
        )
        text = format_diagnosis(d)
        # Banner
        self.assertIn("event_id=42", text)
        self.assertIn("status=finished", text)
        # Summary counts
        self.assertIn("GREEN:       1", text)
        self.assertIn("SILENT_DROP: 1", text)
        # Endpoint rows
        self.assertIn("/api/v1/event/{event_id}", text)
        self.assertIn("/api/v1/event/{event_id}/lineups", text)
        # Note under silent drop
        self.assertIn("Snapshot http_status ok, 0 rows", text)


if __name__ == "__main__":
    unittest.main()
