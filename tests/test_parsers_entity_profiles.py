from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.families.entity_profiles import EntityProfilesParser


class EntityProfilesParserTests(unittest.TestCase):
    def test_team_profile_parser_extracts_manager_and_venue(self) -> None:
        parser = EntityProfilesParser()
        snapshot = RawSnapshot(
            snapshot_id=701,
            endpoint_pattern="/api/v1/team/{team_id}",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/team/42",
            resolved_url="https://www.sofascore.com/api/v1/team/42",
            envelope_key="team",
            http_status=200,
            payload={
                "team": {
                    "id": 42,
                    "slug": "arsenal",
                    "name": "Arsenal",
                    "manager": {"id": 500, "slug": "arteta", "name": "Mikel Arteta"},
                    "venue": {"id": 55, "slug": "emirates", "name": "Emirates Stadium"},
                }
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="team",
            context_entity_id=42,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual({item["id"] for item in result.entity_upserts["manager"]}, {500})
        self.assertEqual({item["id"] for item in result.entity_upserts["venue"]}, {55})
        self.assertEqual(len(result.relation_upserts["team_manager"]), 1)

    def test_player_profile_parser_extracts_player_team_relation(self) -> None:
        parser = EntityProfilesParser()
        snapshot = RawSnapshot(
            snapshot_id=702,
            endpoint_pattern="/api/v1/player/{player_id}",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/player/700",
            resolved_url="https://www.sofascore.com/api/v1/player/700",
            envelope_key="player",
            http_status=200,
            payload={
                "player": {
                    "id": 700,
                    "slug": "saka",
                    "name": "Bukayo Saka",
                    "team": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                }
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="player",
            context_entity_id=700,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual({item["id"] for item in result.entity_upserts["player"]}, {700})
        self.assertEqual(len(result.relation_upserts["player_team"]), 1)


if __name__ == "__main__":
    unittest.main()
