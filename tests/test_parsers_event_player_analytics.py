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
        # URL is authoritative for player_id — even when context_entity_id is right
        self.assertEqual(row["player_id"], 804508)

    def test_rating_breakdown_prefers_url_player_id_over_bad_context(self) -> None:
        """Regression: force_d10 backfill stored ``context_entity_id`` as
        event_id by mistake.  Parser must derive player_id from the URL,
        not blindly trust context, otherwise FK violations on player(id).
        """
        parser = EventPlayerRatingBreakdownParser()
        snapshot = RawSnapshot(
            snapshot_id=804,
            endpoint_pattern="/api/v1/event/{event_id}/player/{player_id}/rating-breakdown",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14025197/player/1394089/rating-breakdown",
            resolved_url="https://www.sofascore.com/api/v1/event/14025197/player/1394089/rating-breakdown",
            envelope_key="passes",
            http_status=200,
            payload={
                "passes": [
                    {
                        "eventActionType": "pass",
                        "isHome": True,
                        "outcome": True,
                        "playerCoordinates": {"x": 1.0, "y": 2.0},
                        "passEndCoordinates": {"x": 3.0, "y": 4.0},
                    }
                ],
            },
            fetched_at="2026-04-17T10:00:00+00:00",
            context_entity_type="event",          # ← wrong but historical
            context_entity_id=14025197,           # ← event_id, not player_id
            context_event_id=14025197,
        )

        result = parser.parse(snapshot)

        rows = result.metric_rows["event_player_rating_breakdown_action"]
        self.assertEqual(len(rows), 1)
        # Must come from URL (/player/1394089/), NOT context_entity_id (14025197)
        self.assertEqual(rows[0]["player_id"], 1394089)
        self.assertEqual(rows[0]["event_id"], 14025197)

    def test_rating_breakdown_falls_back_to_context_without_url(self) -> None:
        """Defensive: when both URLs are absent and pattern has no /player/N/,
        fall back to ``context_entity_id`` so old paths keep working."""
        parser = EventPlayerRatingBreakdownParser()
        snapshot = RawSnapshot(
            snapshot_id=805,
            endpoint_pattern="/api/v1/some/synthetic",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/some/synthetic",  # no /player/
            resolved_url=None,
            envelope_key="passes",
            http_status=200,
            payload={
                "passes": [{"eventActionType": "pass"}],
            },
            fetched_at="2026-04-17T10:00:00+00:00",
            context_entity_type="player",
            context_entity_id=99999,
            context_event_id=14023987,
        )

        result = parser.parse(snapshot)

        rows = result.metric_rows["event_player_rating_breakdown_action"]
        self.assertEqual(rows[0]["player_id"], 99999)

    def test_player_statistics_prefers_url_when_payload_player_id_missing(self) -> None:
        """Defense in depth: if payload's ``player.id`` is missing/non-int
        AND ``context_entity_id`` is wrong (= event_id), the parser should
        still pull the correct player_id from the URL."""
        parser = EventPlayerStatisticsParser()
        snapshot = RawSnapshot(
            snapshot_id=806,
            endpoint_pattern="/api/v1/event/{event_id}/player/{player_id}/statistics",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14025197/player/848276/statistics",
            resolved_url="https://www.sofascore.com/api/v1/event/14025197/player/848276/statistics",
            envelope_key="player,statistics",
            http_status=200,
            payload={
                # player.id is missing — pathological but possible if upstream
                # ever returns a partial shape.  Without URL fallback we would
                # write event_id (14025197) as player_id and hit FK fail.
                "player": {"slug": "jp-mateta", "name": "Jean Philippe Mateta"},
                "statistics": {"rating": 7.0, "minutesPlayed": 90},
            },
            fetched_at="2026-04-17T10:00:00+00:00",
            context_entity_type="event",          # ← wrong but historical
            context_entity_id=14025197,           # ← event_id, not player_id
            context_event_id=14025197,
        )

        result = parser.parse(snapshot)

        summary = result.metric_rows["event_player_statistics"][0]
        self.assertEqual(summary["player_id"], 848276)
        self.assertEqual(summary["event_id"], 14025197)
        # All fact rows must carry the same player_id
        for row in result.metric_rows["event_player_stat_value"]:
            self.assertEqual(row["player_id"], 848276)


if __name__ == "__main__":
    unittest.main()
