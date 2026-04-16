from __future__ import annotations

import unittest

from schema_inspector.normalizers.sink import DurableNormalizeSink
from schema_inspector.parsers.base import ParseResult, RawSnapshot
from schema_inspector.parsers.families.event_root import EventRootParser
from schema_inspector.storage.normalize_repository import NormalizeRepository


class _FakeExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"

    async def executemany(self, query: str, rows: list[tuple[object, ...]]) -> str:
        self.executemany_calls.append((query, rows))
        return "OK"


class NormalizeRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_repository_persists_core_metric_rows(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        result = ParseResult(
            snapshot_id=301,
            parser_family="event_statistics",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "season": ({"id": 76986, "name": "Premier League 25/26", "year": "25/26", "editor": False},),
                "team": (
                    {"id": 42, "slug": "arsenal", "name": "Arsenal", "short_name": "Arsenal", "manager_id": None},
                    {"id": 43, "slug": "chelsea", "name": "Chelsea", "short_name": "Chelsea", "manager_id": None},
                ),
                "event": (
                    {
                        "id": 14083191,
                        "slug": "arsenal-chelsea",
                        "season_id": 76986,
                        "home_team_id": 42,
                        "away_team_id": 43,
                        "venue_id": None,
                        "tournament_id": 100,
                        "unique_tournament_id": 17,
                        "status_type": "inprogress",
                        "start_timestamp": 1_800_000_000,
                    },
                ),
            },
            metric_rows={
                "event_statistic": (
                    {
                        "event_id": 14083191,
                        "period": "ALL",
                        "group_name": "Overview",
                        "name": "Possession",
                        "home_value": "55%",
                        "away_value": "45%",
                        "compare_code": None,
                        "statistics_type": None,
                    },
                ),
                "event_incident": (
                    {
                        "event_id": 14083191,
                        "ordinal": 0,
                        "incident_id": 1,
                        "incident_type": "goal",
                        "time": 17,
                        "home_score": 1,
                        "away_score": 0,
                        "text": None,
                    },
                ),
                "event_graph": (
                    {
                        "event_id": 14083191,
                        "period_time": 74,
                        "period_count": 2,
                        "overtime_length": 0,
                    },
                ),
                "event_graph_point": (
                    {
                        "event_id": 14083191,
                        "ordinal": 0,
                        "minute": 17.0,
                        "value": 1,
                    },
                ),
            },
        )

        await repository.persist_parse_result(executor, result)

        statements = [sql for sql, _ in executor.execute_calls] + [sql for sql, _ in executor.executemany_calls]
        self.assertTrue(any("INSERT INTO season" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO team" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event" in sql for sql in statements))
        self.assertTrue(any("DELETE FROM event_statistic" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_statistic" in sql for sql in statements))
        self.assertTrue(any("DELETE FROM event_incident" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_incident" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_graph" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_graph_point" in sql for sql in statements))

    async def test_durable_sink_persists_special_metric_rows(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        sink = DurableNormalizeSink(repository, executor)
        result = ParseResult(
            snapshot_id=302,
            parser_family="special_metrics",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "event": (
                    {
                        "id": 15921219,
                        "slug": "sinner-alcaraz",
                        "season_id": None,
                        "home_team_id": None,
                        "away_team_id": None,
                        "venue_id": None,
                        "tournament_id": None,
                        "unique_tournament_id": None,
                        "status_type": "inprogress",
                        "start_timestamp": 1_800_000_000,
                    },
                ),
            },
            metric_rows={
                "tennis_point_by_point": (
                    {
                        "event_id": 15921219,
                        "ordinal": 0,
                        "point_id": 11,
                        "set_number": 1,
                        "game_number": 2,
                        "server": "home",
                        "home_score": "30",
                        "away_score": "15",
                    },
                ),
                "tennis_power": (
                    {
                        "event_id": 15921219,
                        "side": "home",
                        "current": 0.61,
                        "delta": 0.04,
                    },
                ),
                "baseball_inning": (
                    {
                        "event_id": 15308201,
                        "ordinal": 0,
                        "inning": 1,
                        "home_score": 0,
                        "away_score": 1,
                    },
                ),
                "shotmap_point": (
                    {
                        "event_id": 15929810,
                        "ordinal": 0,
                        "x": 22.0,
                        "y": 18.0,
                        "shot_type": "slap",
                    },
                ),
                "esports_game": (
                    {
                        "event_id": 16017074,
                        "ordinal": 0,
                        "game_id": 1,
                        "status": "finished",
                        "map_name": "Dust2",
                    },
                ),
            },
        )

        await sink(result)

        statements = [sql for sql, _ in executor.execute_calls] + [sql for sql, _ in executor.executemany_calls]
        self.assertTrue(any("INSERT INTO tennis_point_by_point" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO tennis_power" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO baseball_inning" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO shotmap_point" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO esports_game" in sql for sql in statements))

    async def test_persists_event_root_entities_in_dependency_order(self) -> None:
        parser = EventRootParser()
        snapshot = RawSnapshot(
            snapshot_id=900,
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
                    "homeTeam": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                    "awayTeam": {"id": 43, "slug": "chelsea", "name": "Chelsea"},
                    "status": {"type": "inprogress"},
                    "startTimestamp": 1775779200,
                }
            },
            fetched_at="2026-04-17T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14083191,
            context_event_id=14083191,
        )
        result = parser.parse(snapshot)
        executor = _FakeExecutor()

        await NormalizeRepository().persist_parse_result(executor, result)

        statements = [query for query, _ in executor.executemany_calls]
        sport_index = next(i for i, sql in enumerate(statements) if "INSERT INTO sport" in sql)
        country_index = next(i for i, sql in enumerate(statements) if "INSERT INTO country" in sql)
        category_index = next(i for i, sql in enumerate(statements) if "INSERT INTO category" in sql)
        season_index = next(i for i, sql in enumerate(statements) if "INSERT INTO season" in sql)
        unique_tournament_index = next(i for i, sql in enumerate(statements) if "INSERT INTO unique_tournament" in sql)
        tournament_index = next(i for i, sql in enumerate(statements) if "INSERT INTO tournament" in sql)
        venue_index = next(i for i, sql in enumerate(statements) if "INSERT INTO venue" in sql)
        team_index = next(i for i, sql in enumerate(statements) if "INSERT INTO team" in sql)
        event_index = next(i for i, sql in enumerate(statements) if "INSERT INTO event" in sql)

        self.assertLess(sport_index, country_index)
        self.assertLess(country_index, category_index)
        self.assertLess(category_index, unique_tournament_index)
        self.assertLess(unique_tournament_index, season_index)
        self.assertLess(unique_tournament_index, tournament_index)
        self.assertLess(tournament_index, venue_index)
        self.assertLess(venue_index, team_index)
        self.assertLess(team_index, event_index)

        event_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO event" in sql)
        self.assertEqual(event_rows[0][2], 100)
        self.assertEqual(event_rows[0][3], 17)
        self.assertEqual(event_rows[0][4], 76986)

    async def test_persists_parent_team_before_child_team(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        result = ParseResult(
            snapshot_id=901,
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

        await repository.persist_parse_result(executor, result)

        team_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO team" in sql)
        self.assertEqual(team_rows[0][0], 440)
        self.assertIsNone(team_rows[0][11])
        self.assertEqual(team_rows[1][0], 45)
        self.assertEqual(team_rows[1][11], 440)


if __name__ == "__main__":
    unittest.main()
