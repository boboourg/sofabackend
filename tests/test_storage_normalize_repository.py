from __future__ import annotations

import unittest
from pathlib import Path

from schema_inspector.normalizers.sink import DurableNormalizeSink
from schema_inspector.parsers.base import ParseResult, RawSnapshot
from schema_inspector.parsers.families.event_root import EventRootParser
from schema_inspector.storage.normalize_repository import NormalizeRepository, RetriableRepositoryError


class _FakeExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_results: list[list[dict[str, object]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"

    async def executemany(self, query: str, rows: list[tuple[object, ...]]) -> str:
        self.executemany_calls.append((query, rows))
        return "OK"

    async def fetch(self, query: str, *args: object):
        self.fetch_calls.append((query, args))
        if self.fetch_results:
            return self.fetch_results.pop(0)
        return []


class NormalizeRepositoryTests(unittest.IsolatedAsyncioTestCase):
    """Statement-shape and repository-branch coverage only.

    FK correctness and ordering guarantees live in the real-Postgres
    integration tests in test_storage_normalize_repository_integration.py.
    """

    def test_base_schema_does_not_require_unique_manager_slug(self) -> None:
        schema_path = Path(__file__).resolve().parent.parent / "postgres_schema.sql"
        sql = schema_path.read_text(encoding="utf-8")

        manager_start = sql.index("CREATE TABLE manager (")
        manager_end = sql.index(");", manager_start)
        manager_block = sql[manager_start:manager_end]

        team_start = sql.index("CREATE TABLE team (")
        team_end = sql.index(");", team_start)
        team_block = sql[team_start:team_end]

        tournament_start = sql.index("CREATE TABLE tournament (")
        tournament_end = sql.index(");", tournament_start)
        tournament_block = sql[tournament_start:tournament_end]

        self.assertNotIn("slug TEXT UNIQUE", manager_block)
        self.assertNotIn("slug TEXT UNIQUE", team_block)
        self.assertNotIn("slug TEXT UNIQUE", tournament_block)

    async def test_repository_persists_core_metric_rows(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        executor.fetch_results = [[{"id": 100}], [{"id": 17}]]
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

    async def test_repository_persists_root_event_status_before_event_row(self) -> None:
        parser = EventRootParser()
        snapshot = RawSnapshot(
            snapshot_id=950,
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
                    "homeTeam": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                    "awayTeam": {"id": 43, "slug": "chelsea", "name": "Chelsea"},
                    "status": {"code": 100, "description": "2nd half", "type": "inprogress"},
                    "startTimestamp": 1775779200,
                }
            },
            fetched_at="2026-04-17T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14083191,
            context_event_id=14083191,
        )
        executor = _FakeExecutor()

        await NormalizeRepository().persist_parse_result(executor, parser.parse(snapshot))

        status_call_index = next(
            index for index, (sql, _) in enumerate(executor.executemany_calls) if "INSERT INTO event_status" in sql
        )
        event_call_index = next(
            index for index, (sql, _) in enumerate(executor.executemany_calls) if "INSERT INTO event (" in sql
        )
        self.assertLess(status_call_index, event_call_index)

        status_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO event_status" in sql)
        self.assertEqual(status_rows[0], (100, "2nd half", "inprogress"))

        event_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO event (" in sql)
        self.assertEqual(event_rows[0][8], 100)

    async def test_repository_sorts_detail_batch_rows_lexicographically(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        result = ParseResult(
            snapshot_id=953,
            parser_family="event_comments",
            parser_version="v1",
            status="parsed",
            entity_upserts={},
            metric_rows={
                "event_comment_feed": (
                    {"event_id": 200, "home_player_color": {"primary": "#fff"}},
                    {"event_id": 100, "home_player_color": {"primary": "#000"}},
                ),
                "event_comment": (
                    {"event_id": 100, "comment_id": 9, "sequence": 2, "text": "late"},
                    {"event_id": 100, "comment_id": 2, "sequence": 1, "text": "early"},
                ),
                "event_vote_option": (
                    {"event_id": 100, "vote_type": "vote", "option_name": "away", "vote_count": 8},
                    {"event_id": 100, "vote_type": "vote", "option_name": "home", "vote_count": 12},
                ),
                "event_team_heatmap": (
                    {"event_id": 100, "team_id": 3002},
                    {"event_id": 100, "team_id": 3001},
                ),
                "event_team_heatmap_point": (
                    {"event_id": 100, "team_id": 3001, "point_type": "player", "ordinal": 2, "x": 0.2, "y": 0.4},
                    {"event_id": 100, "team_id": 3001, "point_type": "player", "ordinal": 1, "x": 0.1, "y": 0.3},
                ),
            },
        )

        await repository.persist_parse_result(executor, result)

        feed_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO event_comment_feed" in sql)
        self.assertEqual([row[0] for row in feed_rows], [100, 200])

        comment_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO event_comment (" in sql)
        self.assertEqual([(row[0], row[1]) for row in comment_rows], [(100, 2), (100, 9)])

        vote_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO event_vote_option" in sql)
        self.assertEqual([(row[0], row[1], row[2]) for row in vote_rows], [(100, "vote", "away"), (100, "vote", "home")])

        heatmap_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO event_team_heatmap (" in sql)
        self.assertEqual([(row[0], row[1]) for row in heatmap_rows], [(100, 3001), (100, 3002)])

        heatmap_point_rows = next(
            rows for sql, rows in executor.executemany_calls if "INSERT INTO event_team_heatmap_point" in sql
        )
        self.assertEqual(
            [(row[0], row[1], row[2], row[3]) for row in heatmap_point_rows],
            [(100, 3001, "player", 1), (100, 3001, "player", 2)],
        )

    async def test_repository_persists_extended_event_detail_metric_rows(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        result = ParseResult(
            snapshot_id=951,
            parser_family="event_managers",
            parser_version="v1",
            status="parsed",
            entity_upserts={},
            metric_rows={
                "event_manager_assignment": ({"event_id": 15868599, "side": "home", "manager_id": 500},),
                "event_duel": ({"event_id": 15868599, "duel_type": "team", "home_wins": 1, "away_wins": 2, "draws": 3},),
                "event_pregame_form": ({"event_id": 15868599, "label": "Pts"},),
                "event_pregame_form_side": (
                    {"event_id": 15868599, "side": "home", "avg_rating": "6.7", "position": 2, "value": "70"},
                ),
                "event_pregame_form_item": (
                    {"event_id": 15868599, "side": "home", "ordinal": 0, "form_value": "W"},
                ),
                "event_vote_option": (
                    {"event_id": 15868599, "vote_type": "vote", "option_name": "home", "vote_count": 12},
                ),
                "event_team_heatmap": ({"event_id": 15868599, "team_id": 3002},),
                "event_team_heatmap_point": (
                    {"event_id": 15868599, "team_id": 3002, "point_type": "player", "ordinal": 0, "x": 0.1, "y": 0.2},
                ),
                "provider": ({"id": 1, "slug": None, "name": "Provider One", "country": None},),
                "provider_configuration": (
                    {
                        "id": 77,
                        "provider_id": 1,
                        "campaign_id": None,
                        "fallback_provider_id": None,
                        "type": "main",
                        "weight": None,
                        "branded": None,
                        "featured_odds_type": None,
                        "bet_slip_link": None,
                        "default_bet_slip_link": None,
                        "impression_cost_encrypted": None,
                    },
                ),
                "event_market": (
                    {
                        "id": 900,
                        "event_id": 15868599,
                        "provider_id": 1,
                        "fid": 44,
                        "market_id": 2,
                        "source_id": 555,
                        "market_group": "Match",
                        "market_name": "1X2",
                        "market_period": "ALL",
                        "structure_type": 1,
                        "choice_group": None,
                        "is_live": True,
                        "suspended": False,
                    },
                ),
                "event_market_choice": (
                    {
                        "source_id": 6001,
                        "event_market_id": 900,
                        "name": "Home",
                        "change_value": 0,
                        "fractional_value": "2/1",
                        "initial_fractional_value": "2/1",
                    },
                ),
                "event_winning_odds": (
                    {
                        "event_id": 15868599,
                        "provider_id": 1,
                        "side": "home",
                        "odds_id": 10,
                        "actual": 52,
                        "expected": 48,
                        "fractional_value": "1/2",
                    },
                ),
            },
        )

        await repository.persist_parse_result(executor, result)

        statements = [sql for sql, _ in executor.execute_calls] + [sql for sql, _ in executor.executemany_calls]
        self.assertTrue(any("event_manager_assignment" in sql for sql in statements))
        self.assertTrue(any("event_duel" in sql for sql in statements))
        self.assertTrue(any("event_pregame_form" in sql for sql in statements))
        self.assertTrue(any("event_vote_option" in sql for sql in statements))
        self.assertTrue(any("event_team_heatmap" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO provider " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO provider_configuration " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_market " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_market_choice " in sql for sql in statements))
        self.assertTrue(any("event_winning_odds" in sql for sql in statements))

    async def test_repository_persists_season_rounds_and_cuptrees(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        result = ParseResult(
            snapshot_id=952,
            parser_family="season_cuptrees",
            parser_version="v1",
            status="parsed",
            entity_upserts={},
            metric_rows={
                "season_round": (
                    {
                        "unique_tournament_id": 336,
                        "season_id": 80287,
                        "round_number": 27,
                        "round_name": "Quarterfinals",
                        "round_slug": "quarterfinals",
                        "is_current": False,
                    },
                    {
                        "unique_tournament_id": 336,
                        "season_id": 80287,
                        "round_number": 28,
                        "round_name": "Semifinals",
                        "round_slug": "semifinals",
                        "is_current": True,
                    },
                ),
                "season_cup_tree": (
                    {
                        "cup_tree_id": 10845780,
                        "unique_tournament_id": 336,
                        "season_id": 80287,
                        "tournament_id": 207,
                        "name": "Taca de Portugal 25/26",
                        "current_round": 7,
                    },
                ),
                "season_cup_tree_round": (
                    {
                        "cup_tree_id": 10845780,
                        "round_order": 1,
                        "round_type": 101,
                        "description": "Round 1",
                    },
                ),
                "season_cup_tree_block": (
                    {
                        "entry_id": 2873386,
                        "cup_tree_id": 10845780,
                        "round_order": 1,
                        "block_id": 2421533,
                        "block_order": 1,
                        "finished": True,
                        "matches_in_round": 1,
                        "result": "7:1",
                        "home_team_score": "7",
                        "away_team_score": "1",
                        "has_next_round_link": True,
                        "series_start_date_timestamp": 1756656000,
                        "automatic_progression": False,
                        "event_ids_json": [14410747],
                    },
                ),
                "season_cup_tree_participant": (
                    {
                        "participant_id": 5276248,
                        "entry_id": 2873386,
                        "team_id": 190324,
                        "order_value": 1,
                        "winner": True,
                    },
                ),
            },
        )

        await repository.persist_parse_result(executor, result)

        statements = [sql for sql, _ in executor.execute_calls] + [sql for sql, _ in executor.executemany_calls]
        self.assertTrue(any("DELETE FROM season_round" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_round" in sql for sql in statements))
        self.assertTrue(any("DELETE FROM season_cup_tree" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_cup_tree " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_cup_tree_round " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_cup_tree_block " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO season_cup_tree_participant " in sql for sql in statements))

    async def test_repository_coerces_event_statistic_text_fields_to_strings(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        result = ParseResult(
            snapshot_id=777,
            parser_family="event_statistics",
            parser_version="v1",
            status="parsed",
            entity_upserts={},
            metric_rows={
                "event_statistic": (
                    {
                        "event_id": 14083191,
                        "period": "ALL",
                        "group_name": "Overview",
                        "name": "Big chances",
                        "home_value": 2,
                        "away_value": 1,
                        "compare_code": 2,
                        "statistics_type": 99,
                    },
                ),
            },
        )

        await repository.persist_parse_result(executor, result)

        statistic_insert_rows = next(
            rows for sql, rows in executor.executemany_calls if "INSERT INTO event_statistic" in sql
        )
        persisted_row = statistic_insert_rows[0]
        self.assertEqual(persisted_row[5], "2")
        self.assertEqual(persisted_row[8], "1")
        self.assertEqual(persisted_row[10], "2")
        self.assertEqual(persisted_row[11], "99")

    async def test_repository_chunks_large_player_upserts_into_smaller_executemany_batches(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        result = ParseResult(
            snapshot_id=778,
            parser_family="entity_profiles",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "player": tuple(
                    {
                        "id": player_id,
                        "slug": f"player-{player_id}",
                        "name": f"Player {player_id}",
                        "short_name": f"P{player_id}",
                    }
                    for player_id in range(1, 206)
                ),
            },
        )

        await repository.persist_parse_result(executor, result)

        player_batches = [
            rows for sql, rows in executor.executemany_calls if "INSERT INTO player" in sql
        ]
        self.assertEqual([len(rows) for rows in player_batches], [100, 100, 5])

    async def test_repository_reuses_known_dimensions_across_calls(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()

        await repository._upsert_minimal_entities(
            executor,
            {
                "sport": ({"id": 1, "slug": "football", "name": "Football"},),
                "country": ({"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},),
                "category": (
                    {
                        "id": 10,
                        "slug": "england",
                        "name": "England",
                        "sport_id": 1,
                        "country_alpha2": "EN",
                    },
                ),
            },
        )

        executor.executemany_calls.clear()
        executor.fetch_results = [
            [{"id": 1}],
            [{"alpha2": "EN"}],
            [{"id": 10}],
        ]

        inserted = await repository._upsert_minimal_entities(
            executor,
            {
                "sport": ({"id": 1, "slug": "football", "name": "Football"},),
                "country": ({"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},),
                "category": (
                    {
                        "id": 10,
                        "slug": "england",
                        "name": "England",
                        "sport_id": 1,
                        "country_alpha2": "EN",
                    },
                ),
                "tournament": (
                    {
                        "id": 100,
                        "slug": "premier-league",
                        "name": "Premier League",
                        "category_id": 10,
                        "unique_tournament_id": None,
                    },
                ),
            },
        )

        statements = [sql for sql, _ in executor.executemany_calls]
        self.assertFalse(any("INSERT INTO sport" in sql for sql in statements))
        self.assertFalse(any("INSERT INTO country" in sql for sql in statements))
        self.assertFalse(any("INSERT INTO category" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO tournament" in sql for sql in statements))
        self.assertIn(1, inserted["sport"])
        self.assertIn("EN", inserted["country"])
        self.assertIn(10, inserted["category"])

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
                "baseball_pitch": (
                    {
                        "event_id": 15308201,
                        "at_bat_id": 981436,
                        "ordinal": 0,
                        "pitch_id": 1,
                        "pitch_speed": 96.4,
                        "pitch_type": "FF",
                        "pitch_zone": "up-in",
                        "pitch_x": 0.13,
                        "pitch_y": 2.71,
                        "mlb_x": 125.4,
                        "mlb_y": 234.5,
                        "outcome": "strike",
                        "pitcher_id": 9001,
                        "hitter_id": 9002,
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
        self.assertTrue(any("INSERT INTO baseball_pitch" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO shotmap_point" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO esports_game" in sql for sql in statements))

    async def test_durable_sink_can_skip_event_root_entity_upserts(self) -> None:
        parser = EventRootParser()
        snapshot = RawSnapshot(
            snapshot_id=904,
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
                    "homeTeam": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                    "awayTeam": {"id": 43, "slug": "chelsea", "name": "Chelsea"},
                }
            },
            fetched_at="2026-04-17T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14083191,
            context_event_id=14083191,
        )
        result = parser.parse(snapshot)
        executor = _FakeExecutor()
        sink = DurableNormalizeSink(
            NormalizeRepository(),
            executor,
            skip_entity_parser_families={"event_root"},
        )

        await sink(result)

        self.assertEqual(executor.execute_calls, [])
        self.assertEqual(executor.executemany_calls, [])

    async def test_durable_sink_prunes_event_statistics_entity_upserts_to_event_only(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        executor.fetch_results = [
            [{"id": 42}, {"id": 43}],
            [{"id": 76986}],
        ]
        sink = DurableNormalizeSink(repository, executor)
        result = ParseResult(
            snapshot_id=905,
            parser_family="event_statistics",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "sport": ({"id": 1, "slug": "football", "name": "Football"},),
                "country": ({"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},),
                "category": ({"id": 10, "slug": "england", "name": "England", "sport_id": 1, "country_alpha2": "EN"},),
                "team": (
                    {"id": 42, "slug": "arsenal", "name": "Arsenal", "short_name": "Arsenal"},
                    {"id": 43, "slug": "chelsea", "name": "Chelsea", "short_name": "Chelsea"},
                ),
                "season": ({"id": 76986, "name": "Premier League 25/26", "year": "25/26", "editor": False},),
                "event": (
                    {
                        "id": 14083191,
                        "slug": "arsenal-chelsea",
                        "season_id": 76986,
                        "home_team_id": 42,
                        "away_team_id": 43,
                        "venue_id": None,
                        "tournament_id": None,
                        "unique_tournament_id": None,
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
            },
        )

        await sink(result)

        statements = [sql for sql, _ in executor.executemany_calls]
        self.assertFalse(any("INSERT INTO sport" in sql for sql in statements))
        self.assertFalse(any("INSERT INTO country" in sql for sql in statements))
        self.assertFalse(any("INSERT INTO category" in sql for sql in statements))
        self.assertFalse(any("INSERT INTO season" in sql for sql in statements))
        self.assertFalse(any("INSERT INTO team" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_statistic" in sql for sql in statements))

    async def test_durable_sink_prunes_event_player_statistics_to_event_team_player_only(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        sink = DurableNormalizeSink(repository, executor)
        result = ParseResult(
            snapshot_id=906,
            parser_family="event_player_statistics",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "sport": ({"id": 1, "slug": "football", "name": "Football"},),
                "category": ({"id": 10, "slug": "england", "name": "England", "sport_id": 1, "country_alpha2": None},),
                "team": (
                    {"id": 42, "slug": "arsenal", "name": "Arsenal", "short_name": "Arsenal"},
                ),
                "player": (
                    {"id": 700, "slug": "saka", "name": "Bukayo Saka", "short_name": "B. Saka", "team_id": 42},
                ),
                "event": (
                    {
                        "id": 14083191,
                        "slug": "arsenal-chelsea",
                        "season_id": None,
                        "home_team_id": 42,
                        "away_team_id": None,
                        "venue_id": None,
                        "tournament_id": None,
                        "unique_tournament_id": None,
                        "start_timestamp": 1_800_000_000,
                    },
                ),
            },
            metric_rows={
                "event_player_statistics": (
                    {
                        "event_id": 14083191,
                        "player_id": 700,
                        "team_id": 42,
                        "position": "F",
                        "rating": 7.9,
                        "rating_original": 7.9,
                        "rating_alternative": None,
                        "statistics_type": "player",
                        "sport_slug": "football",
                        "extra_json": None,
                    },
                ),
                "event_player_stat_value": (
                    {
                        "event_id": 14083191,
                        "player_id": 700,
                        "stat_name": "goals",
                        "stat_value_numeric": 1,
                        "stat_value_text": "1",
                        "stat_value_json": None,
                    },
                ),
            },
        )

        await sink(result)

        statements = [sql for sql, _ in executor.executemany_calls]
        self.assertFalse(any("INSERT INTO sport" in sql for sql in statements))
        self.assertFalse(any("INSERT INTO category" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO team" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO player" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_player_statistics" in sql for sql in statements))

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

    async def test_manager_upsert_uses_primary_key_conflict_target_only(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        result = ParseResult(
            snapshot_id=902,
            parser_family="entity_profiles",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "manager": (
                    {"id": 101, "slug": "luis-garcia", "name": "Luis Garcia", "short_name": "L. Garcia"},
                    {"id": 202, "slug": "luis-garcia", "name": "Luis Garcia II", "short_name": "L. Garcia"},
                ),
            },
        )

        await repository.persist_parse_result(executor, result)

        manager_insert_sql = next(sql for sql, _ in executor.executemany_calls if "INSERT INTO manager" in sql)
        self.assertIn("ON CONFLICT (id)", manager_insert_sql)
        self.assertNotIn("ON CONFLICT (slug)", manager_insert_sql)

    async def test_shared_dimension_upserts_use_insert_if_missing_conflict_policy(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        result = ParseResult(
            snapshot_id=903,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "sport": ({"id": 1, "slug": "football", "name": "Football"},),
                "country": ({"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},),
                "category": (
                    {"id": 10, "slug": "england", "name": "England", "sport_id": 1, "country_alpha2": "EN"},
                ),
                "unique_tournament": (
                    {"id": 17, "slug": "premier-league", "name": "Premier League", "category_id": 10, "country_alpha2": "EN"},
                ),
                "season": (
                    {"id": 76986, "name": "Premier League 25/26", "year": "25/26", "editor": False},
                ),
                "tournament": (
                    {"id": 100, "slug": "premier-league", "name": "Premier League", "category_id": 10, "unique_tournament_id": 17},
                ),
                "venue": (
                    {"id": 55, "slug": "emirates-stadium", "name": "Emirates Stadium", "country_alpha2": "EN"},
                ),
            },
        )

        await repository.persist_parse_result(executor, result)

        statements = {sql.split("(")[0].strip(): sql for sql, _ in executor.executemany_calls}
        self.assertIn("ON CONFLICT (id) DO NOTHING", statements["INSERT INTO sport"])
        self.assertIn("ON CONFLICT (alpha2) DO NOTHING", statements["INSERT INTO country"])
        self.assertIn("ON CONFLICT (id) DO NOTHING", statements["INSERT INTO category"])
        self.assertIn("ON CONFLICT (id) DO NOTHING", statements["INSERT INTO unique_tournament"])
        self.assertIn("ON CONFLICT (id) DO NOTHING", statements["INSERT INTO season"])
        self.assertIn("ON CONFLICT (id) DO NOTHING", statements["INSERT INTO tournament"])
        self.assertIn("ON CONFLICT (id) DO NOTHING", statements["INSERT INTO venue"])

    async def test_repository_nullifies_event_venue_when_cache_is_stale(self) -> None:
        repository = NormalizeRepository()
        repository._known_minimal_entities["sport"].add(1)
        repository._known_minimal_entities["category"].add(10)
        repository._known_minimal_entities["unique_tournament"].add(17)
        repository._known_minimal_entities["venue"].add(11505)
        executor = _FakeExecutor()
        executor.fetch_results = [[{"id": 17}], []]
        result = ParseResult(
            snapshot_id=999,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
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

        await repository.persist_parse_result(executor, result)

        event_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO event" in sql)
        self.assertIsNone(event_rows[0][7])

    async def test_repository_preserves_player_team_id_when_db_confirms_parent_exists(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        executor.fetch_results = [[{"id": 42}]]
        result = ParseResult(
            snapshot_id=1000,
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

        await repository.persist_parse_result(executor, result)

        player_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO player" in sql)
        self.assertEqual(player_rows[0][4], 42)

    async def test_repository_raises_retryable_error_for_missing_event_unique_tournament(self) -> None:
        repository = NormalizeRepository()
        executor = _FakeExecutor()
        executor.fetch_results = [[]]
        result = ParseResult(
            snapshot_id=1001,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "event": (
                    {
                        "id": 14083192,
                        "slug": "arsenal-chelsea-2",
                        "tournament_id": None,
                        "unique_tournament_id": 17,
                        "season_id": None,
                        "home_team_id": None,
                        "away_team_id": None,
                        "venue_id": None,
                        "start_timestamp": 1_800_000_100,
                    },
                ),
            },
        )

        with self.assertRaises(RetriableRepositoryError):
            await repository.persist_parse_result(executor, result)


if __name__ == "__main__":
    unittest.main()
