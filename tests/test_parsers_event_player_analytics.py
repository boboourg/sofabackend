from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.families.event_best_players import EventBestPlayersParser
from schema_inspector.parsers.families.event_player_statistics import EventPlayerStatisticsParser
from schema_inspector.parsers.special.event_player_rating_breakdown import EventPlayerRatingBreakdownParser


class EventPlayerAnalyticsParserTests(unittest.TestCase):
    def test_best_players_parser_extracts_fact_rows(self) -> None:
        parser = EventBestPlayersParser()
        snapshot = RawSnapshot(
            snapshot_id=801,
            endpoint_pattern="/api/v1/event/{event_id}/best-players/summary",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14023987/best-players/summary",
            resolved_url="https://www.sofascore.com/api/v1/event/14023987/best-players/summary",
            envelope_key="bestHomeTeamPlayers,bestAwayTeamPlayers,playerOfTheMatch",
            http_status=200,
            payload={
                "bestHomeTeamPlayers": [
                    {
                        "label": "rating",
                        "value": "7.9",
                        "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                    }
                ],
                "bestAwayTeamPlayers": [
                    {
                        "label": "rating",
                        "value": "7.1",
                        "player": {"id": 701, "slug": "palmer", "name": "Cole Palmer"},
                    }
                ],
                "playerOfTheMatch": {
                    "label": "rating",
                    "value": "7.9",
                    "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                },
            },
            fetched_at="2026-04-17T10:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14023987,
            context_event_id=14023987,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.parser_family, "event_best_players")
        self.assertEqual(len(result.metric_rows["event_best_player_entry"]), 3)
        self.assertEqual(
            {row["bucket"] for row in result.metric_rows["event_best_player_entry"]},
            {"best_home", "best_away", "player_of_the_match"},
        )

    def test_event_player_statistics_parser_extracts_summary_and_fact_rows(self) -> None:
        parser = EventPlayerStatisticsParser()
        snapshot = RawSnapshot(
            snapshot_id=802,
            endpoint_pattern="/api/v1/event/{event_id}/player/{player_id}/statistics",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14023987/player/804508/statistics",
            resolved_url="https://www.sofascore.com/api/v1/event/14023987/player/804508/statistics",
            envelope_key="player,team,position,statistics,extra",
            http_status=200,
            payload={
                "player": {"id": 804508, "slug": "player-a", "name": "Player A"},
                "team": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                "position": "F",
                "statistics": {
                    "minutesPlayed": 90,
                    "goals": 1,
                    "rating": 7.9,
                    "ratingVersions": {"original": 7.8, "alternative": 7.6},
                    "statisticsType": {"sportSlug": "football", "statisticsType": "player"},
                },
                "extra": {"captain": True},
            },
            fetched_at="2026-04-17T10:00:00+00:00",
            context_entity_type="player",
            context_entity_id=804508,
            context_event_id=14023987,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.parser_family, "event_player_statistics")
        self.assertEqual(len(result.metric_rows["event_player_statistics"]), 1)
        self.assertEqual(len(result.metric_rows["event_player_stat_value"]), 2)
        summary_row = result.metric_rows["event_player_statistics"][0]
        self.assertEqual(summary_row["rating"], 7.9)
        self.assertEqual(summary_row["rating_original"], 7.8)
        self.assertEqual(summary_row["rating_alternative"], 7.6)

    def test_rating_breakdown_parser_extracts_action_rows(self) -> None:
        parser = EventPlayerRatingBreakdownParser()
        snapshot = RawSnapshot(
            snapshot_id=803,
            endpoint_pattern="/api/v1/event/{event_id}/player/{player_id}/rating-breakdown",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14023987/player/804508/rating-breakdown",
            resolved_url="https://www.sofascore.com/api/v1/event/14023987/player/804508/rating-breakdown",
            envelope_key="passes,dribbles,defensive,ball-carries",
            http_status=200,
            payload={
                "passes": [
                    {
                        "eventActionType": "pass",
                        "isHome": True,
                        "keypass": False,
                        "outcome": True,
                        "playerCoordinates": {"x": 80.5, "y": 58.9},
                        "passEndCoordinates": {"x": 96.9, "y": 60.3},
                    }
                ],
                "dribbles": [],
                "defensive": [],
                "ball-carries": [],
            },
            fetched_at="2026-04-17T10:00:00+00:00",
            context_entity_type="player",
            context_entity_id=804508,
            context_event_id=14023987,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.parser_family, "event_player_rating_breakdown")
        self.assertEqual(len(result.metric_rows["event_player_rating_breakdown_action"]), 1)
        row = result.metric_rows["event_player_rating_breakdown_action"][0]
        self.assertEqual(row["action_group"], "passes")
        self.assertEqual(row["event_action_type"], "pass")
        self.assertEqual(row["end_x"], 96.9)


if __name__ == "__main__":
    unittest.main()
