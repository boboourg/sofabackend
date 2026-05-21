"""Stage 4.4 (2026-05-21) — RED test for the replay-path payload bug.

Symptom on prod
---------------
``python -m schema_inspector.cli replay --snapshot-id <X>`` for an
incidents / statistics snapshot prints ``replay snapshots=X
families=event_incidents`` but ``event_incident`` and ``event_statistic``
stay empty afterwards. Same parser ID, same payload — manual
reproduction with ``json.loads(payload)`` on the bytes returned by
asyncpg works and writes 20 rows; the live replay path writes 0.

Root cause
----------
asyncpg's default ``jsonb`` codec returns the column as a JSON
**string**, not as a decoded dict. ``RawRepository.fetch_payload_snapshot``
sets ``payload=row["payload"]`` directly (`storage/raw_repository.py:507`),
so the constructed ``RawSnapshot`` carries a *str*. Every family parser
runs ``_as_mapping(payload) or {}`` (e.g. `parsers/families/event_incidents.py:16`)
which returns ``None`` for ``isinstance(value, Mapping)==False`` →
parser falls into the ``parsed_empty`` branch → ``metric_rows={}`` →
``_persist_event_incidents`` / ``_persist_event_statistics`` early-exit
on ``if not rows: return``.

Net effect: every snapshot that has to be loaded back from PostgreSQL
(replay, post-commit re-hydrate) goes through ``fetch_payload_snapshot``
and silently loses its payload. The CLI ``event`` path mostly hides this
because PilotOrchestrator reuses the in-memory cache populated at
``insert_payload_snapshot_returning_id``, where payload was a dict.

Fix shape (will live in raw_repository): if asyncpg gives us a ``str``
(or ``bytes``), decode with ``orjson.loads`` before handing it to the
RawSnapshot. Dicts pass through unchanged so we stay forward-compatible
with codec changes.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from schema_inspector.storage.raw_repository import RawRepository


class _FakeFetchExecutor:
    """asyncpg-shaped row container with a configurable payload column."""

    def __init__(self, *, payload: object) -> None:
        self._row = {
            "id": 42,
            "endpoint_pattern": "/api/v1/event/{event_id}/incidents",
            "sport_slug": "football",
            "source_url": "https://www.sofascore.com/api/v1/event/100/incidents",
            "resolved_url": "https://www.sofascore.com/api/v1/event/100/incidents",
            "envelope_key": "incidents",
            "http_status": 200,
            "payload": payload,
            "fetched_at": datetime(2026, 5, 21, tzinfo=timezone.utc),
            "context_entity_type": "event",
            "context_entity_id": 100,
            "context_unique_tournament_id": None,
            "context_season_id": None,
            "context_event_id": 100,
        }

    async def fetchrow(self, query, *args):
        return self._row

    async def execute(self, query, *args):
        return "OK"


class FetchPayloadSnapshotJsonbDecodingTests(unittest.IsolatedAsyncioTestCase):
    """RawRepository.fetch_payload_snapshot must return a snapshot whose
    .payload is dict-shaped, regardless of whether asyncpg gave us a
    pre-decoded dict (custom codec) or a raw JSON string (default
    codec). Without this guarantee every downstream parser sees a
    string and silently emits 0 rows."""

    async def test_payload_string_is_decoded_into_dict(self) -> None:
        repository = RawRepository()
        executor = _FakeFetchExecutor(
            payload='{"incidents": [{"id": 1, "incidentType": "goal", "time": 12}], "home": {"id": 38}, "away": {"id": 2032}}',
        )

        snapshot = await repository.fetch_payload_snapshot(executor, 42)

        self.assertIsInstance(
            snapshot.payload, dict,
            msg=(
                "fetch_payload_snapshot returned RawSnapshot.payload as "
                f"{type(snapshot.payload).__name__}. Downstream parsers gate "
                "on isinstance(payload, Mapping); a str is silently treated "
                "as parsed_empty. Decode the asyncpg jsonb-as-string before "
                "constructing the RawSnapshot."
            ),
        )
        self.assertEqual(snapshot.payload.get("home"), {"id": 38})
        incidents = snapshot.payload.get("incidents")
        self.assertEqual(len(incidents), 1)
        self.assertEqual(incidents[0]["incidentType"], "goal")

    async def test_payload_bytes_are_decoded_into_dict(self) -> None:
        """If asyncpg ever flips to returning the column as bytes
        (orjson-style), the decoder must still produce a dict."""

        repository = RawRepository()
        executor = _FakeFetchExecutor(payload=b'{"incidents": []}')

        snapshot = await repository.fetch_payload_snapshot(executor, 42)

        self.assertIsInstance(snapshot.payload, dict)
        self.assertEqual(snapshot.payload, {"incidents": []})

    async def test_payload_already_dict_passes_through_unchanged(self) -> None:
        """When asyncpg returns the jsonb already decoded (custom codec
        in production or test fixture), the repository must not double-
        decode and must not raise."""

        repository = RawRepository()
        original = {"incidents": [{"id": 7}], "home": {"id": 1}}
        executor = _FakeFetchExecutor(payload=original)

        snapshot = await repository.fetch_payload_snapshot(executor, 42)

        self.assertIs(snapshot.payload, original)

    async def test_payload_null_falls_back_to_empty_dict(self) -> None:
        """A NULL payload column (corrupt row, in-flight backfill)
        must not crash the loader. Returning an empty dict keeps
        downstream parsers in the ``parsed_empty`` branch instead of
        raising AttributeError."""

        repository = RawRepository()
        executor = _FakeFetchExecutor(payload=None)

        snapshot = await repository.fetch_payload_snapshot(executor, 42)

        self.assertEqual(snapshot.payload, {})


if __name__ == "__main__":
    unittest.main()
