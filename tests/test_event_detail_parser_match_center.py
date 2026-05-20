"""Stage 4.2 (2026-05-20 match-center fix): EventDetailParser.fetch_bundle
must fetch /incidents and /statistics for each event in the bundle.

Until this commit those two endpoints lived ONLY in the live-delta
edge path (pilot_orchestrator._endpoint_for_edge_kind), so the
historical-backfill chain (which goes through EventDetailParser ->
EventDetailBackfillJob) never pulled goals / shots-on-target /
possession for archive matches. The audit-B finding pinned this
specifically: EVENT_INCIDENTS_ENDPOINT and EVENT_STATISTICS_ENDPOINT
were not even imported in event_detail_parser.py.

This fix makes the parser fetch both for every event in
fetch_bundle, save the raw payloads to ``state.payload_snapshots``
(so ``EventDetailRepository`` persists them to
``api_payload_snapshot``), and leaves normalisation to a separate
optional hook in ``EventDetailBackfillJob`` (Stage 4.2 follow-up).
"""

from __future__ import annotations

import unittest


class EventDetailParserImportsIncidentsAndStatisticsEndpointsTests(unittest.TestCase):
    """Source-level pinning: the module-level imports must include the
    two endpoints so fetch_bundle can build URLs for them."""

    def test_event_incidents_endpoint_imported(self) -> None:
        from pathlib import Path
        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "event_detail_parser.py"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "EVENT_INCIDENTS_ENDPOINT",
            text,
            msg=(
                "event_detail_parser.py must import EVENT_INCIDENTS_ENDPOINT "
                "from .endpoints — without it fetch_bundle has no way to "
                "build the /incidents URL for archive matches."
            ),
        )

    def test_event_statistics_endpoint_imported(self) -> None:
        from pathlib import Path
        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "event_detail_parser.py"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "EVENT_STATISTICS_ENDPOINT",
            text,
            msg=(
                "event_detail_parser.py must import EVENT_STATISTICS_ENDPOINT — "
                "match-center stats (possession, shots, corners) for archive "
                "matches lived only in the live-delta path before this fix."
            ),
        )

    def test_fetch_bundle_invokes_fetch_incidents_helper(self) -> None:
        """Behavioural: fetch_bundle must build a coroutine list that
        includes _fetch_incidents (the per-match helper)."""
        from pathlib import Path
        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "event_detail_parser.py"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "_fetch_incidents",
            text,
            msg=(
                "event_detail_parser.py must define _fetch_incidents and "
                "call it from fetch_bundle so archive matches get goals "
                "ingested into api_payload_snapshot."
            ),
        )

    def test_fetch_bundle_invokes_fetch_statistics_helper(self) -> None:
        from pathlib import Path
        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "event_detail_parser.py"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "_fetch_statistics",
            text,
            msg=(
                "event_detail_parser.py must define _fetch_statistics and "
                "call it from fetch_bundle."
            ),
        )


class EventDetailParserFetchesIncidentsBehaviouralTests(unittest.IsolatedAsyncioTestCase):
    """End-to-end: build a fake SofascoreClient + run fetch_bundle and
    confirm /incidents and /statistics URLs are actually requested."""

    async def test_fetch_bundle_requests_incidents_and_statistics_urls(self) -> None:
        from schema_inspector.event_detail_parser import EventDetailParser

        # Minimal payload that lets the accumulator's root-ingest succeed.
        event_root_payload = {
            "event": {
                "id": 999001,
                "tournament": {"id": 1, "uniqueTournament": {"id": 7}},
                "season": {"id": 52162},
                "homeTeam": {"id": 100, "name": "Home"},
                "awayTeam": {"id": 200, "name": "Away"},
                "status": {"code": 100, "type": "finished"},
                "startTimestamp": 1717200000,
                "venue": {},
                "tournament_meta": {},
            }
        }

        fetched_urls: list[str] = []

        class _FakeResponse:
            def __init__(self, url: str, payload: dict) -> None:
                self.url = url
                self.source_url = url
                self.resolved_url = url
                self.envelope_key = "x"
                self.payload = payload
                self.fetched_at = "2026-05-20T22:00:00+00:00"
                self.http_status = 200

        class _FakeClient:
            async def get_json(self, url: str, *, timeout: float):
                del timeout
                fetched_urls.append(url)
                if "/incidents" in url:
                    return _FakeResponse(url, {"incidents": [
                        {"time": 74, "incidentType": "goal", "isHome": False, "homeScore": 0, "awayScore": 1},
                        {"time": 83, "incidentType": "goal", "isHome": False, "homeScore": 0, "awayScore": 2},
                    ]})
                if "/statistics" in url and "/player/" not in url:
                    return _FakeResponse(url, {"statistics": []})
                if url.endswith("/event/999001"):
                    return _FakeResponse(url, event_root_payload)
                # Default: empty success payload — every other helper just
                # records the snapshot without ingest.
                return _FakeResponse(url, {})

        parser = EventDetailParser(_FakeClient())
        bundle = await parser.fetch_bundle(999001, provider_ids=(1,), timeout=5.0)

        # 1) /incidents url was fetched
        self.assertTrue(
            any(url.endswith("/event/999001/incidents") for url in fetched_urls),
            msg=(
                "/api/v1/event/999001/incidents must be in the fetched URL "
                f"set. Got: {[u for u in fetched_urls if '/event/' in u]}"
            ),
        )
        # 2) /statistics url was fetched
        self.assertTrue(
            any(url.endswith("/event/999001/statistics") for url in fetched_urls),
            msg=(
                "/api/v1/event/999001/statistics must be in the fetched URL set"
            ),
        )
        # 3) Both raw payloads are in the bundle's payload_snapshots — that
        # is what EventDetailRepository writes to api_payload_snapshot.
        snapshot_patterns = [s.endpoint_pattern for s in bundle.payload_snapshots]
        self.assertIn(
            "/api/v1/event/{event_id}/incidents",
            snapshot_patterns,
            msg="incidents snapshot must be in the bundle for downstream persist",
        )
        self.assertIn(
            "/api/v1/event/{event_id}/statistics",
            snapshot_patterns,
            msg="statistics snapshot must be in the bundle for downstream persist",
        )


if __name__ == "__main__":
    unittest.main()
