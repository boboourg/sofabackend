"""Verify that GET /api/v1/unique-tournament/{ut}/season/{s}/info
returns the Sofascore payload **plus** two extra Unix timestamps:

  ``info.start_ut`` — earliest startTimestamp among events with
                      round_number = 1
  ``info.end_ut``   — latest startTimestamp among events with the
                      MAX round_number in this season

The enricher runs on top of whatever Sofascore returned (raw snapshot
waterfall) — it never changes existing fields, only adds the two new
ones. Failures on the DB side are tolerated (the payload is returned
unmodified).
"""
from __future__ import annotations
import unittest
from typing import Any

from schema_inspector.local_api_server import LocalApiApplication


class FakeConn:
    """Stub asyncpg connection. ``staged_row`` is what fetchrow returns
    when the enricher's query is issued."""

    def __init__(self, staged_row: dict[str, Any] | None) -> None:
        self._row = staged_row
        self.queries: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.queries.append((query, args))
        return self._row

    async def close(self) -> None:
        pass


class EnrichSeasonInfoPayloadTests(unittest.IsolatedAsyncioTestCase):
    def _app_with(self, conn: FakeConn) -> LocalApiApplication:
        app = LocalApiApplication.__new__(LocalApiApplication)

        async def _connect() -> FakeConn:
            return conn

        app._connect = _connect  # type: ignore[attr-defined]
        return app

    async def test_enricher_adds_start_ut_and_end_ut(self) -> None:
        conn = FakeConn(staged_row={"start_ut": 1755284400, "end_ut": 1779634800})
        app = self._app_with(conn)
        payload = {
            "info": {
                "id": 49510,
                "goals": 1015,
                "season": {"id": 76986, "name": "Premier League 25/26"},
            }
        }
        result = await app._enrich_season_info_payload(
            payload, ut_id=17, season_id=76986,
        )
        self.assertEqual(result["info"]["start_ut"], 1755284400)
        self.assertEqual(result["info"]["end_ut"], 1779634800)
        # Original fields untouched
        self.assertEqual(result["info"]["id"], 49510)
        self.assertEqual(result["info"]["goals"], 1015)

    async def test_enricher_preserves_payload_when_no_events(self) -> None:
        """A season we haven't ingested yet returns NULL for both — the
        enricher must NOT inject zero/None placeholders."""
        conn = FakeConn(staged_row={"start_ut": None, "end_ut": None})
        app = self._app_with(conn)
        payload = {"info": {"id": 1, "goals": 0}}
        result = await app._enrich_season_info_payload(
            payload, ut_id=999, season_id=999,
        )
        self.assertNotIn("start_ut", result["info"])
        self.assertNotIn("end_ut", result["info"])

    async def test_enricher_skips_when_payload_has_no_info_block(self) -> None:
        """Defensive — if Sofascore changes the shape and ``info`` is
        missing, we return the original payload untouched."""
        conn = FakeConn(staged_row={"start_ut": 100, "end_ut": 200})
        app = self._app_with(conn)
        payload = {"something_else": True}
        result = await app._enrich_season_info_payload(
            payload, ut_id=17, season_id=76986,
        )
        self.assertEqual(result, {"something_else": True})

    async def test_enricher_tolerates_db_failure(self) -> None:
        """Pool error must not break the API — return the original."""

        class FailingConn:
            async def fetchrow(self, *_a: Any, **_kw: Any) -> Any:
                raise RuntimeError("db is down")

            async def close(self) -> None:
                pass

        app = LocalApiApplication.__new__(LocalApiApplication)

        async def _connect() -> Any:
            return FailingConn()

        app._connect = _connect  # type: ignore[attr-defined]
        payload = {"info": {"id": 1}}
        result = await app._enrich_season_info_payload(
            payload, ut_id=17, season_id=76986,
        )
        self.assertEqual(result, payload)

    async def test_enricher_handles_partial_round_data(self) -> None:
        """Only round 1 ingested (no last round yet) → only start_ut set."""
        conn = FakeConn(staged_row={"start_ut": 1755284400, "end_ut": None})
        app = self._app_with(conn)
        payload = {"info": {"id": 1, "goals": 0}}
        result = await app._enrich_season_info_payload(
            payload, ut_id=17, season_id=76986,
        )
        self.assertEqual(result["info"]["start_ut"], 1755284400)
        self.assertNotIn("end_ut", result["info"])


if __name__ == "__main__":
    unittest.main()
