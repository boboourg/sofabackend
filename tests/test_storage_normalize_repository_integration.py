from __future__ import annotations

import os
import unittest
from pathlib import Path

from schema_inspector.parsers.base import ParseResult
from schema_inspector.storage.normalize_repository import NormalizeRepository, RetriableRepositoryError


class NormalizeRepositoryIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        database_url = os.getenv("TEST_DATABASE_URL")
        if not database_url:
            raise unittest.SkipTest("TEST_DATABASE_URL not set; skipping normalize repository integration tests")
        try:
            import asyncpg
        except ModuleNotFoundError as exc:
            raise unittest.SkipTest("asyncpg is not installed; skipping normalize repository integration tests") from exc
        self.database_url = database_url
        self.connection = await asyncpg.connect(database_url)
        await self.connection.execute("DROP SCHEMA public CASCADE")
        await self.connection.execute("CREATE SCHEMA public")
        schema_path = Path(__file__).resolve().parent.parent / "postgres_schema.sql"
        await self.connection.execute(schema_path.read_text(encoding="utf-8"))

    async def asyncTearDown(self) -> None:
        if hasattr(self, "connection"):
            await self.connection.close()

    async def test_stale_cache_missing_venue_nullifies_event_venue(self) -> None:
        repository = NormalizeRepository()
        repository._known_minimal_entities["venue"].add(11505)
        result = ParseResult(
            snapshot_id=1,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "sport": ({"id": 1, "slug": "football", "name": "Football"},),
                "country": ({"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},),
                "category": ({"id": 10, "slug": "england", "name": "England", "sport_id": 1, "country_alpha2": "EN"},),
                "unique_tournament": (
                    {"id": 17, "slug": "premier-league", "name": "Premier League", "category_id": 10, "country_alpha2": "EN"},
                ),
                "event": (
                    {
                        "id": 14083191,
                        "slug": "arsenal-chelsea",
                        "tournament_id": None,
                        "unique_tournament_id": 17,
                        "season_id": None,
                        "home_team_id": None,
                        "away_team_id": None,
                        "venue_id": 11505,
                        "start_timestamp": 1_800_000_000,
                    },
                ),
            },
        )

        await repository.persist_parse_result(self.connection, result)

        row = await self.connection.fetchrow("SELECT venue_id FROM event WHERE id = $1", 14083191)
        self.assertIsNotNone(row)
        self.assertIsNone(row["venue_id"])

    async def test_stale_cache_missing_team_nullifies_player_team(self) -> None:
        repository = NormalizeRepository()
        repository._known_minimal_entities["team"].add(42)
        result = ParseResult(
            snapshot_id=2,
            parser_family="entity_profiles",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "player": (
                    {
                        "id": 700,
                        "slug": "saka",
                        "name": "Bukayo Saka",
                        "short_name": "B. Saka",
                        "team_id": 42,
                    },
                ),
            },
        )

        await repository.persist_parse_result(self.connection, result)

        row = await self.connection.fetchrow("SELECT team_id FROM player WHERE id = $1", 700)
        self.assertIsNotNone(row)
        self.assertIsNone(row["team_id"])

    async def test_db_parent_presence_preserves_player_team_fk(self) -> None:
        repository = NormalizeRepository()
        await self.connection.execute(
            """
            INSERT INTO team (id, slug, name)
            VALUES (42, 'arsenal', 'Arsenal')
            """
        )
        result = ParseResult(
            snapshot_id=3,
            parser_family="entity_profiles",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "player": (
                    {
                        "id": 701,
                        "slug": "odegaard",
                        "name": "Martin Odegaard",
                        "short_name": "M. Odegaard",
                        "team_id": 42,
                    },
                ),
            },
        )

        await repository.persist_parse_result(self.connection, result)

        row = await self.connection.fetchrow("SELECT team_id FROM player WHERE id = $1", 701)
        self.assertEqual(row["team_id"], 42)

    async def test_missing_event_unique_tournament_raises_retryable_error(self) -> None:
        repository = NormalizeRepository()
        result = ParseResult(
            snapshot_id=4,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "event": (
                    {
                        "id": 14083193,
                        "slug": "retry-me",
                        "tournament_id": None,
                        "unique_tournament_id": 17,
                        "season_id": None,
                        "home_team_id": None,
                        "away_team_id": None,
                        "venue_id": None,
                        "start_timestamp": 1_800_000_200,
                    },
                ),
            },
        )

        with self.assertRaises(RetriableRepositoryError):
            await repository.persist_parse_result(self.connection, result)

    async def test_missing_event_home_team_raises_retryable_error(self) -> None:
        repository = NormalizeRepository()
        await self.connection.execute(
            """
            INSERT INTO sport (id, slug, name) VALUES (1, 'football', 'Football');
            INSERT INTO category (id, slug, name, sport_id) VALUES (10, 'england', 'England', 1);
            INSERT INTO unique_tournament (id, slug, name, category_id) VALUES (17, 'premier-league', 'Premier League', 10);
            """
        )
        result = ParseResult(
            snapshot_id=5,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "event": (
                    {
                        "id": 14083194,
                        "slug": "missing-home",
                        "tournament_id": None,
                        "unique_tournament_id": 17,
                        "season_id": None,
                        "home_team_id": 42,
                        "away_team_id": None,
                        "venue_id": None,
                        "start_timestamp": 1_800_000_300,
                    },
                ),
            },
        )

        with self.assertRaises(RetriableRepositoryError):
            await repository.persist_parse_result(self.connection, result)

    async def test_missing_event_away_team_raises_retryable_error(self) -> None:
        repository = NormalizeRepository()
        await self.connection.execute(
            """
            INSERT INTO sport (id, slug, name) VALUES (1, 'football', 'Football');
            INSERT INTO category (id, slug, name, sport_id) VALUES (10, 'england', 'England', 1);
            INSERT INTO unique_tournament (id, slug, name, category_id) VALUES (17, 'premier-league', 'Premier League', 10);
            INSERT INTO team (id, slug, name) VALUES (42, 'arsenal', 'Arsenal');
            """
        )
        result = ParseResult(
            snapshot_id=51,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "event": (
                    {
                        "id": 140831941,
                        "slug": "missing-away",
                        "tournament_id": None,
                        "unique_tournament_id": 17,
                        "season_id": None,
                        "home_team_id": 42,
                        "away_team_id": 43,
                        "venue_id": None,
                        "start_timestamp": 1_800_000_301,
                    },
                ),
            },
        )

        with self.assertRaises(RetriableRepositoryError):
            await repository.persist_parse_result(self.connection, result)

    async def test_missing_event_competition_after_validation_raises_retryable_error(self) -> None:
        repository = NormalizeRepository()
        result = ParseResult(
            snapshot_id=52,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "event": (
                    {
                        "id": 140831942,
                        "slug": "missing-competition",
                        "tournament_id": 100,
                        "unique_tournament_id": None,
                        "season_id": None,
                        "home_team_id": None,
                        "away_team_id": None,
                        "venue_id": None,
                        "start_timestamp": 1_800_000_302,
                    },
                ),
            },
        )

        with self.assertRaises(RetriableRepositoryError):
            await repository.persist_parse_result(self.connection, result)

    async def test_bundle_with_parents_and_children_persists_successfully(self) -> None:
        repository = NormalizeRepository()
        result = ParseResult(
            snapshot_id=6,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "sport": ({"id": 1, "slug": "football", "name": "Football"},),
                "country": ({"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},),
                "category": ({"id": 10, "slug": "england", "name": "England", "sport_id": 1, "country_alpha2": "EN"},),
                "unique_tournament": (
                    {"id": 17, "slug": "premier-league", "name": "Premier League", "category_id": 10, "country_alpha2": "EN"},
                ),
                "season": ({"id": 76986, "name": "Premier League 25/26", "year": "25/26", "editor": False},),
                "tournament": (
                    {"id": 100, "slug": "premier-league", "name": "Premier League", "category_id": 10, "unique_tournament_id": 17},
                ),
                "venue": ({"id": 55, "slug": "emirates", "name": "Emirates Stadium", "country_alpha2": "EN"},),
                "team": (
                    {"id": 42, "slug": "arsenal", "name": "Arsenal", "short_name": "Arsenal", "sport_id": 1, "venue_id": 55},
                    {"id": 43, "slug": "chelsea", "name": "Chelsea", "short_name": "Chelsea", "sport_id": 1},
                ),
                "event": (
                    {
                        "id": 14083195,
                        "slug": "arsenal-chelsea",
                        "tournament_id": 100,
                        "unique_tournament_id": 17,
                        "season_id": 76986,
                        "home_team_id": 42,
                        "away_team_id": 43,
                        "venue_id": 55,
                        "start_timestamp": 1_800_000_400,
                    },
                ),
            },
        )

        await repository.persist_parse_result(self.connection, result)

        row = await self.connection.fetchrow(
            "SELECT tournament_id, unique_tournament_id, home_team_id, away_team_id, venue_id FROM event WHERE id = $1",
            14083195,
        )
        self.assertEqual(dict(row), {
            "tournament_id": 100,
            "unique_tournament_id": 17,
            "home_team_id": 42,
            "away_team_id": 43,
            "venue_id": 55,
        })

    async def test_team_parent_self_reference_ordering_is_preserved(self) -> None:
        repository = NormalizeRepository()
        result = ParseResult(
            snapshot_id=7,
            parser_family="entity_profiles",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "sport": ({"id": 1, "slug": "football", "name": "Football"},),
                "team": (
                    {
                        "id": 45,
                        "slug": "manchester-city",
                        "name": "Manchester City",
                        "short_name": "Man City",
                        "sport_id": 1,
                        "parent_team_id": 440,
                    },
                    {
                        "id": 440,
                        "slug": "city-root",
                        "name": "City Root",
                        "short_name": "City Root",
                        "sport_id": 1,
                    },
                ),
            },
        )

        await repository.persist_parse_result(self.connection, result)

        child = await self.connection.fetchrow("SELECT parent_team_id FROM team WHERE id = $1", 45)
        parent = await self.connection.fetchrow("SELECT parent_team_id FROM team WHERE id = $1", 440)
        self.assertEqual(child["parent_team_id"], 440)
        self.assertIsNone(parent["parent_team_id"])


if __name__ == "__main__":
    unittest.main()
