from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.families.event_root import EventRootParser


class EventRootParserTests(unittest.TestCase):
    def test_event_root_parser_extracts_core_entities_and_relations(self) -> None:
        parser = EventRootParser()
        snapshot = RawSnapshot(
            snapshot_id=201,
            endpoint_pattern="/api/v1/event/{event_id}",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14083191",
            resolved_url="https://www.sofascore.com/api/v1/event/14083191",
            envelope_key="event",
            http_status=200,
            payload={
                "event": {
                    "id": 14083191,
                    "slug": "arsenal-chelsea",
                    "tournament": {
                        "id": 100,
                        "slug": "premier-league",
                        "name": "Premier League",
                        "category": {
                            "id": 1,
                            "slug": "england",
                            "name": "England",
                            "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                        },
                        "uniqueTournament": {"id": 17, "slug": "premier-league", "name": "Premier League"},
                    },
                    "season": {"id": 76986, "name": "Premier League 25/26", "year": "25/26"},
                    "venue": {
                        "id": 55,
                        "slug": "emirates-stadium",
                        "name": "Emirates Stadium",
                        "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                    },
                    "homeTeam": {
                        "id": 42,
                        "slug": "arsenal",
                        "name": "Arsenal",
                        "manager": {"id": 500, "slug": "arteta", "name": "Mikel Arteta"},
                    },
                    "awayTeam": {
                        "id": 43,
                        "slug": "chelsea",
                        "name": "Chelsea",
                        "manager": {"id": 501, "slug": "maresca", "name": "Enzo Maresca"},
                    },
                    "status": {"code": 100, "description": "1st half", "type": "inprogress"},
                    "startTimestamp": 1775779200,
                }
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14083191,
            context_event_id=14083191,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual(result.entity_upserts["sport"][0]["slug"], "football")
        self.assertEqual(result.entity_upserts["country"][0]["alpha2"], "EN")
        self.assertEqual(result.entity_upserts["category"][0]["id"], 1)
        self.assertEqual(result.entity_upserts["unique_tournament"][0]["id"], 17)
        self.assertEqual(result.entity_upserts["unique_tournament"][0]["category_id"], 1)
        self.assertEqual(result.entity_upserts["event"][0]["unique_tournament_id"], 17)
        self.assertEqual(result.entity_upserts["event"][0]["status_code"], 100)
        self.assertEqual(result.entity_upserts["team"][0]["sport_id"], 1)
        self.assertEqual(result.entity_upserts["team"][0]["tournament_id"], 100)
        self.assertEqual(result.entity_upserts["season"][0]["id"], 76986)
        self.assertEqual(result.entity_upserts["venue"][0]["id"], 55)
        self.assertEqual(result.entity_upserts["venue"][0]["country_alpha2"], "EN")
        self.assertEqual({item["id"] for item in result.entity_upserts["manager"]}, {500, 501})
        self.assertEqual(result.metric_rows["event_status"][0]["code"], 100)
        self.assertEqual(result.metric_rows["event_status"][0]["type"], "inprogress")
        self.assertEqual(len(result.relation_upserts["event_team"]), 2)

    def test_event_root_parser_extracts_is_editor_and_coverage_flags(self) -> None:
        """X3 patch (2026-05-12): the registry path must surface SofaEditor /
        coverage / feed signals into the event row so downstream
        ``match_center_policy`` HARD-BAN logic sees them. Previously these
        were only set by the legacy bundle path in
        ``event_detail_parser.py``; for registry-only callers the fields
        were missing → HARD BAN did not fire."""
        parser = EventRootParser()
        snapshot = RawSnapshot(
            snapshot_id=202,
            endpoint_pattern="/api/v1/event/{event_id}",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/99",
            resolved_url="https://www.sofascore.com/api/v1/event/99",
            envelope_key="event",
            http_status=200,
            payload={
                "event": {
                    "id": 99,
                    "slug": "amateur-vs-amateur",
                    "tournament": {
                        "id": 999,
                        "slug": "amateur-league",
                        "name": "Amateur Regional Cup",
                        "category": {
                            "id": 9,
                            "slug": "amateur",
                            "name": "Amateur",
                            "country": {"alpha2": "DE", "alpha3": "DEU", "slug": "germany", "name": "Germany"},
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                        },
                        "uniqueTournament": {"id": 8888, "slug": "amateur-cup", "name": "Amateur Cup"},
                    },
                    "season": {"id": 12345, "name": "2025/26", "year": "25/26"},
                    "homeTeam": {"id": 1, "slug": "a", "name": "A"},
                    "awayTeam": {"id": 2, "slug": "b", "name": "B"},
                    "status": {"code": 0, "description": "Not started", "type": "notstarted"},
                    "startTimestamp": 1_800_000_000,
                    # X3: new fields the registry path must propagate.
                    "isEditor": True,
                    "coverage": -1,
                    "crowdsourcingEnabled": True,
                    "crowdsourcingDataDisplayEnabled": True,
                    "feedLocked": False,
                    "finalResultOnly": False,
                    "detailId": None,
                    "hasXg": False,
                    "hasEventPlayerStatistics": False,
                    "hasEventPlayerHeatMap": False,
                    "hasGlobalHighlights": False,
                }
            },
            fetched_at="2026-05-12T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=99,
            context_event_id=99,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        event_row = result.entity_upserts["event"][0]
        # Critical: is_editor must be True so HARD BAN fires downstream.
        self.assertEqual(event_row.get("is_editor"), True)
        self.assertEqual(event_row.get("coverage"), -1)
        self.assertEqual(event_row.get("crowdsourcing_enabled"), True)
        self.assertEqual(event_row.get("crowdsourcing_data_display_enabled"), True)
        self.assertEqual(event_row.get("feed_locked"), False)
        self.assertEqual(event_row.get("final_result_only"), False)

    def test_event_root_parser_missing_new_fields_returns_none(self) -> None:
        """X3 patch: when the upstream payload omits the new fields (e.g.
        pre-match Premier League with detailId missing), the registry
        path returns ``None`` for each, not a magic default. Downstream
        ``is_editor=None`` is treated as "unknown / not banned"."""
        parser = EventRootParser()
        snapshot = RawSnapshot(
            snapshot_id=203,
            endpoint_pattern="/api/v1/event/{event_id}",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/100",
            resolved_url="https://www.sofascore.com/api/v1/event/100",
            envelope_key="event",
            http_status=200,
            payload={
                "event": {
                    "id": 100,
                    "slug": "pre-match",
                    "tournament": {
                        "id": 1,
                        "slug": "premier-league",
                        "name": "Premier League",
                        "category": {
                            "id": 1,
                            "slug": "england",
                            "name": "England",
                            "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                        },
                        "uniqueTournament": {"id": 17, "slug": "premier-league", "name": "Premier League"},
                    },
                    "season": {"id": 76986, "name": "Premier League 25/26", "year": "25/26"},
                    "homeTeam": {"id": 17, "slug": "man-city", "name": "Manchester City"},
                    "awayTeam": {"id": 7, "slug": "crystal-palace", "name": "Crystal Palace"},
                    "status": {"code": 0, "description": "Not started", "type": "notstarted"},
                    "startTimestamp": 1_800_000_000,
                    # No isEditor, no coverage, no crowdsourcing*, no feedLocked,
                    # no finalResultOnly, no detailId, no hasXg, etc. — pre-match
                    # Premier League payloads omit these entirely.
                }
            },
            fetched_at="2026-05-12T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=100,
            context_event_id=100,
        )

        result = parser.parse(snapshot)
        self.assertEqual(result.status, "parsed")
        event_row = result.entity_upserts["event"][0]
        # All new fields default to None when upstream omits them.
        self.assertIsNone(event_row.get("is_editor"))
        self.assertIsNone(event_row.get("coverage"))
        self.assertIsNone(event_row.get("crowdsourcing_enabled"))
        self.assertIsNone(event_row.get("crowdsourcing_data_display_enabled"))
        self.assertIsNone(event_row.get("feed_locked"))
        self.assertIsNone(event_row.get("final_result_only"))
        self.assertIsNone(event_row.get("detail_id"))


if __name__ == "__main__":
    unittest.main()
