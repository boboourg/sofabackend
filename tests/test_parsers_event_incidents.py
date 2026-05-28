from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.families.event_incidents import EventIncidentsParser


def _snapshot(payload: dict) -> RawSnapshot:
    return RawSnapshot(
        snapshot_id=601,
        endpoint_pattern="/api/v1/event/{event_id}/incidents",
        sport_slug="football",
        source_url="https://www.sofascore.com/api/v1/event/14083191/incidents",
        resolved_url="https://www.sofascore.com/api/v1/event/14083191/incidents",
        envelope_key="incidents",
        http_status=200,
        payload=payload,
        fetched_at="2026-04-16T12:00:00+00:00",
        context_entity_type="event",
        context_entity_id=14083191,
        context_event_id=14083191,
    )


class EventIncidentsParserTests(unittest.TestCase):
    def test_event_incidents_parser_extracts_incident_rows(self) -> None:
        parser = EventIncidentsParser()
        snapshot = _snapshot(
            {
                "incidents": [
                    {
                        "id": 1,
                        "incidentType": "goal",
                        "time": 17,
                        "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                        "team": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                        "homeScore": 1,
                        "awayScore": 0,
                    }
                ]
            }
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual(len(result.metric_rows["event_incident"]), 1)
        self.assertEqual(result.metric_rows["event_incident"][0]["incident_type"], "goal")
        self.assertEqual({item["id"] for item in result.entity_upserts["player"]}, {700})

    def test_goal_pulls_scorer_into_player_id_and_assister_into_assist1(self) -> None:
        """Migration 2026-05-28 added ``player_id`` + ``assist1_player_id``
        columns.  For goal incidents:
          - ``player.id``   -> player_id (the scorer)
          - ``assist1.id``  -> assist1_player_id (the assister)
        """
        parser = EventIncidentsParser()
        snapshot = _snapshot(
            {
                "incidents": [
                    {
                        "id": 100,
                        "incidentType": "goal",
                        "incidentClass": "regular",
                        "time": 17,
                        "isHome": True,
                        "player": {"id": 700, "name": "Saka"},
                        "assist1": {"id": 701, "name": "Odegaard"},
                        "homeScore": 1,
                        "awayScore": 0,
                    }
                ]
            }
        )

        row = parser.parse(snapshot).metric_rows["event_incident"][0]

        self.assertEqual(row["player_id"], 700)
        self.assertEqual(row["assist1_player_id"], 701)
        self.assertEqual(row["incident_class"], "regular")
        self.assertEqual(row["is_home"], True)

    def test_card_pulls_recipient_only(self) -> None:
        """Cards have ``player`` but no ``assist1`` — the assist column
        must stay None so the incidentsMap aggregator doesn't over-count
        assists for card recipients."""
        parser = EventIncidentsParser()
        snapshot = _snapshot(
            {
                "incidents": [
                    {
                        "id": 200,
                        "incidentType": "card",
                        "incidentClass": "yellow",
                        "time": 33,
                        "isHome": False,
                        "player": {"id": 800, "name": "Casemiro"},
                    }
                ]
            }
        )

        row = parser.parse(snapshot).metric_rows["event_incident"][0]

        self.assertEqual(row["player_id"], 800)
        self.assertIsNone(row["assist1_player_id"])
        self.assertEqual(row["incident_class"], "yellow")
        self.assertEqual(row["is_home"], False)

    def test_substitution_uses_player_in_as_primary_and_player_out_in_assist1(self) -> None:
        """Substitutions carry ``playerIn`` + ``playerOut`` (not
        ``player`` / ``assist1``).  We surface them as
        (player_id=playerIn, assist1_player_id=playerOut) so a single
        row covers both roles."""
        parser = EventIncidentsParser()
        snapshot = _snapshot(
            {
                "incidents": [
                    {
                        "id": 300,
                        "incidentType": "substitution",
                        "incidentClass": "regular",
                        "time": 76,
                        "isHome": True,
                        "playerIn": {"id": 900, "name": "Subbed In"},
                        "playerOut": {"id": 901, "name": "Subbed Out"},
                    }
                ]
            }
        )

        row = parser.parse(snapshot).metric_rows["event_incident"][0]

        self.assertEqual(row["player_id"], 900)
        self.assertEqual(row["assist1_player_id"], 901)
        self.assertEqual(row["incident_type"], "substitution")

    def test_period_and_injury_time_have_no_player_references(self) -> None:
        """``period`` / ``injuryTime`` rows carry no player at all and
        must end up with both player columns NULL.  ``length`` is
        captured for injury-time rows so the downstream surface can
        render '+5'."""
        parser = EventIncidentsParser()
        snapshot = _snapshot(
            {
                "incidents": [
                    {"id": 400, "incidentType": "period", "text": "Second half", "time": 45},
                    {"incidentType": "injuryTime", "time": 90, "length": 5},
                ]
            }
        )

        rows = parser.parse(snapshot).metric_rows["event_incident"]

        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertIsNone(row["player_id"])
            self.assertIsNone(row["assist1_player_id"])
        self.assertEqual(rows[1]["length"], 5)

    def test_own_goal_class_is_preserved(self) -> None:
        """The incidentsMap aggregator distinguishes 'goal' from 'goal +
        ownGoal' — pin that ``incident_class='ownGoal'`` survives the
        parse so downstream COUNT FILTER (...) can exclude own goals
        from the player's tally."""
        parser = EventIncidentsParser()
        snapshot = _snapshot(
            {
                "incidents": [
                    {
                        "id": 500,
                        "incidentType": "goal",
                        "incidentClass": "ownGoal",
                        "time": 60,
                        "isHome": True,
                        "player": {"id": 999, "name": "Defender"},
                    }
                ]
            }
        )

        row = parser.parse(snapshot).metric_rows["event_incident"][0]

        self.assertEqual(row["incident_class"], "ownGoal")
        self.assertEqual(row["player_id"], 999)


if __name__ == "__main__":
    unittest.main()
