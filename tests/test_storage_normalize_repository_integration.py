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

    async def test_status_code_does_not_regress_after_terminal_state_finalized(self) -> None:
        """F-8 P0 (regression for 14083568-class bug): once an event has a
        row in ``event_terminal_state`` with a terminal status, a delayed-
        insert snapshot whose payload says "1st half"/"inprogress" must
        NOT overwrite the persisted finished status_code on the next
        normalize. The 2026-05-09 audit observed this exact regression
        on LaLiga event 14083568, where a snapshot with id newer than the
        finalize snapshot but fetched_at hours older flipped status_code
        100 (Ended) back to 6 (1st half), which then re-livened the event
        in Redis tier_1 polling."""
        repository = NormalizeRepository()
        await self.connection.execute(
            """
            INSERT INTO sport (id, slug, name) VALUES (1, 'football', 'Football');
            INSERT INTO endpoint_registry (pattern) VALUES ('/api/v1/event/{event_id}');
            INSERT INTO event_status (code, description, type) VALUES
                (6, '1st half', 'inprogress'),
                (100, 'Ended', 'finished');
            INSERT INTO event (id, slug, status_code, start_timestamp)
                VALUES (14083568, 'la-liga-finished', 100, 1778266800);
            INSERT INTO api_payload_snapshot (id, scope_key, endpoint_pattern, http_status, payload_hash, fetched_at)
                VALUES (45674710, 'sofascore:event:14083568:/api/v1/event/{event_id}',
                        '/api/v1/event/{event_id}', 200, 'finished-hash',
                        '2026-05-09 00:40:24+03');
            INSERT INTO event_terminal_state (event_id, terminal_status, finalized_at, final_snapshot_id)
                VALUES (14083568, 'finished', '2026-05-09 00:48:16+03', 45674710);
            """
        )

        # Stale snapshot tries to flip the event back to "inprogress (1st half)".
        result = ParseResult(
            snapshot_id=45686328,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "event": (
                    {
                        "id": 14083568,
                        "slug": "stale-1st-half",
                        "tournament_id": None,
                        "unique_tournament_id": None,
                        "season_id": None,
                        "home_team_id": None,
                        "away_team_id": None,
                        "venue_id": None,
                        "status_code": 6,  # ← regression attempt
                        "start_timestamp": 1778266800,
                    },
                ),
            },
        )

        await repository.persist_parse_result(self.connection, result)

        row = await self.connection.fetchrow("SELECT status_code FROM event WHERE id = $1", 14083568)
        # Guard must hold the finished status — NEVER downgrade past terminal_state.
        self.assertEqual(row["status_code"], 100)

    async def test_status_code_normal_update_works_without_terminal_state(self) -> None:
        """Negative test for F-8 P0 guard: when no terminal_state row exists
        for the event, the COALESCE-style update must still succeed (the
        guard must NOT block ordinary in-progress status transitions like
        notstarted → 1st half → halftime → 2nd half)."""
        repository = NormalizeRepository()
        await self.connection.execute(
            """
            INSERT INTO sport (id, slug, name) VALUES (1, 'football', 'Football');
            INSERT INTO event_status (code, description, type) VALUES
                (6, '1st half', 'inprogress'),
                (7, '2nd half', 'inprogress');
            INSERT INTO event (id, slug, status_code, start_timestamp)
                VALUES (14083569, 'live-event-no-terminal', 6, 1778266800);
            """
        )
        # No event_terminal_state row → guard is dormant, normal update path applies.

        result = ParseResult(
            snapshot_id=45712000,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "event": (
                    {
                        "id": 14083569,
                        "slug": "advance-to-2nd-half",
                        "tournament_id": None,
                        "unique_tournament_id": None,
                        "season_id": None,
                        "home_team_id": None,
                        "away_team_id": None,
                        "venue_id": None,
                        "status_code": 7,
                        "start_timestamp": 1778266800,
                    },
                ),
            },
        )

        await repository.persist_parse_result(self.connection, result)

        row = await self.connection.fetchrow("SELECT status_code FROM event WHERE id = $1", 14083569)
        # Without terminal_state row, the new value still wins.
        self.assertEqual(row["status_code"], 7)

    async def test_event_winner_and_last_period_blocked_by_terminal_state(self) -> None:
        """F-8 Phase 1.5: event_list_repository / event_detail_repository
        UPSERTs use the same monotonic guard as Phase 1 on winner_code,
        aggregated_winner_code, and last_period. A delayed-insert
        event-list payload from mid-game must NOT erase the finalised
        winner or final period name."""
        from schema_inspector.storage._terminal_guard import terminal_guard_case

        await self.connection.execute(
            """
            INSERT INTO event_status (code, description, type) VALUES
                (6, '1st half', 'inprogress'),
                (100, 'Ended', 'finished');
            INSERT INTO event (id, slug, status_code, winner_code,
                               aggregated_winner_code, last_period,
                               start_timestamp)
                VALUES (14083568, 'la-liga', 100, 1, 1, '2nd half', 1778266800);
            INSERT INTO endpoint_registry (pattern) VALUES ('/api/v1/event/{event_id}');
            INSERT INTO api_payload_snapshot (id, scope_key, endpoint_pattern, http_status, payload_hash, fetched_at)
                VALUES (45674710, 'sofascore:event:14083568:/api/v1/event/{event_id}',
                        '/api/v1/event/{event_id}', 200, 'finished-hash',
                        '2026-05-09 00:40:24+03');
            INSERT INTO event_terminal_state (event_id, terminal_status, finalized_at, final_snapshot_id)
                VALUES (14083568, 'finished', '2026-05-09 00:48:16+03', 45674710);
            """
        )

        # Simulated event-list UPSERT shape (same SQL the repository emits).
        status_guard = terminal_guard_case(table="event", event_fk="id", column="status_code")
        winner_guard = terminal_guard_case(table="event", event_fk="id", column="winner_code")
        agg_guard = terminal_guard_case(
            table="event", event_fk="id", column="aggregated_winner_code"
        )
        period_guard = terminal_guard_case(
            table="event", event_fk="id", column="last_period"
        )
        await self.connection.execute(
            f"""
            INSERT INTO event (id, slug, status_code, winner_code,
                               aggregated_winner_code, last_period,
                               start_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE SET
                status_code = {status_guard},
                winner_code = {winner_guard},
                aggregated_winner_code = {agg_guard},
                last_period = {period_guard}
            """,
            14083568,
            "stale-1st-half",
            6,    # would regress status_code
            None, # would erase winner_code
            None, # would erase aggregated_winner_code
            "period1",  # would regress last_period
            1778266800,
        )

        row = await self.connection.fetchrow(
            "SELECT status_code, winner_code, aggregated_winner_code, last_period FROM event WHERE id=$1",
            14083568,
        )
        self.assertEqual(row["status_code"], 100)
        self.assertEqual(row["winner_code"], 1)
        self.assertEqual(row["aggregated_winner_code"], 1)
        self.assertEqual(row["last_period"], "2nd half")

    async def test_event_score_blocked_by_terminal_state(self) -> None:
        """F-8 Phase 1.5: every event_score column gets the guard so a
        delayed-insert "1-2 1st half" payload cannot overwrite the
        final "3-2" score row of an already-finalised match."""
        from schema_inspector.storage._terminal_guard import terminal_guard_case

        await self.connection.execute(
            """
            INSERT INTO event_status (code, description, type) VALUES (100, 'Ended', 'finished');
            INSERT INTO event (id, slug, status_code, start_timestamp)
                VALUES (14083568, 'la-liga', 100, 1778266800);
            INSERT INTO event_score (event_id, side, current, display, aggregated,
                                     normaltime, period1, period2)
                VALUES
                    (14083568, 'home', 3, 3, 3, 3, 2, 1),
                    (14083568, 'away', 2, 2, 2, 2, 2, 0);
            INSERT INTO endpoint_registry (pattern) VALUES ('/api/v1/event/{event_id}');
            INSERT INTO api_payload_snapshot (id, scope_key, endpoint_pattern, http_status, payload_hash, fetched_at)
                VALUES (45674710, 'k', '/api/v1/event/{event_id}', 200, 'h',
                        '2026-05-09 00:40:24+03');
            INSERT INTO event_terminal_state (event_id, terminal_status, finalized_at, final_snapshot_id)
                VALUES (14083568, 'finished', '2026-05-09 00:48:16+03', 45674710);
            """
        )

        score_columns = (
            "current",
            "display",
            "aggregated",
            "normaltime",
            "overtime",
            "penalties",
            "period1",
            "period2",
            "period3",
            "period4",
            "extra1",
            "extra2",
            "series",
        )
        guard_clauses = ",\n".join(
            f"{col} = {terminal_guard_case(table='event_score', event_fk='event_id', column=col)}"
            for col in score_columns
        )
        # Stale payload tries to overwrite with mid-game scores.
        await self.connection.execute(
            f"""
            INSERT INTO event_score (event_id, side, current, display, aggregated,
                                     normaltime, period1, period2)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (event_id, side) DO UPDATE SET
                {guard_clauses}
            """,
            14083568, "home", 1, 1, 1, 1, 1, 0,  # stale 1-? at 1st half
        )

        row = await self.connection.fetchrow(
            "SELECT current, display, aggregated, normaltime, period1, period2 "
            "FROM event_score WHERE event_id=$1 AND side='home'",
            14083568,
        )
        # All score fields preserved at final values, not regressed to mid-game.
        self.assertEqual(row["current"], 3)
        self.assertEqual(row["display"], 3)
        self.assertEqual(row["aggregated"], 3)
        self.assertEqual(row["normaltime"], 3)
        self.assertEqual(row["period1"], 2)
        self.assertEqual(row["period2"], 1)

    async def test_status_code_initial_fill_when_existing_null_with_terminal_state(self) -> None:
        """F-8 hotfix: COALESCE inside the guarded branch lets a NULL→value
        initial fill complete even when ``event_terminal_state`` is
        already recorded. Before the hotfix the guard preserved the
        existing NULL, leaving 138 events with NULL winner_code in 30
        minutes on 2026-05-09."""
        repository = NormalizeRepository()
        await self.connection.execute(
            """
            INSERT INTO event_status (code, description, type) VALUES
                (100, 'Ended', 'finished');
            INSERT INTO event (id, slug, status_code, start_timestamp)
                VALUES (16141590, 'race-condition-event', NULL, 1778266800);
            INSERT INTO endpoint_registry (pattern) VALUES ('/api/v1/event/{event_id}');
            INSERT INTO api_payload_snapshot (id, scope_key, endpoint_pattern, http_status, payload_hash, fetched_at)
                VALUES (45661225, 'k', '/api/v1/event/{event_id}', 200, 'h',
                        '2026-05-09 00:45:45+03');
            INSERT INTO event_terminal_state (event_id, terminal_status, finalized_at, final_snapshot_id)
                VALUES (16141590, 'finished', '2026-05-09 02:00:00+03', 45661225);
            """
        )

        # event.status_code = NULL while terminal_state already exists.
        # A subsequent "finished" parse must be allowed to complete the
        # initial fill (NULL → 100). Without the COALESCE in the THEN
        # branch the guard would keep status_code=NULL forever.
        result = ParseResult(
            snapshot_id=45661225,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "event": (
                    {
                        "id": 16141590,
                        "slug": "fill-status-100",
                        "tournament_id": None,
                        "unique_tournament_id": None,
                        "season_id": None,
                        "home_team_id": None,
                        "away_team_id": None,
                        "venue_id": None,
                        "status_code": 100,
                        "start_timestamp": 1778266800,
                    },
                ),
            },
        )

        await repository.persist_parse_result(self.connection, result)

        row = await self.connection.fetchrow("SELECT status_code FROM event WHERE id=$1", 16141590)
        self.assertEqual(row["status_code"], 100)

    async def test_event_winner_initial_fill_when_existing_null_with_terminal_state(self) -> None:
        """F-8 hotfix: legacy event_list/event_detail UPSERTs must also
        permit NULL→value initial fill for winner_code / last_period
        once terminal_state exists. Mirrors the Phase 1 hotfix test
        above but for the Phase 1.5-guarded columns."""
        from schema_inspector.storage._terminal_guard import terminal_guard_case

        await self.connection.execute(
            """
            INSERT INTO event_status (code, description, type) VALUES (100, 'Ended', 'finished');
            INSERT INTO event (id, slug, status_code, winner_code, last_period, start_timestamp)
                VALUES (16141591, 'race-condition-event-2', 100, NULL, NULL, 1778266800);
            INSERT INTO endpoint_registry (pattern) VALUES ('/api/v1/event/{event_id}');
            INSERT INTO api_payload_snapshot (id, scope_key, endpoint_pattern, http_status, payload_hash, fetched_at)
                VALUES (45661226, 'k', '/api/v1/event/{event_id}', 200, 'h',
                        '2026-05-09 00:45:45+03');
            INSERT INTO event_terminal_state (event_id, terminal_status, finalized_at, final_snapshot_id)
                VALUES (16141591, 'finished', '2026-05-09 02:00:00+03', 45661226);
            """
        )

        winner_guard = terminal_guard_case(table="event", event_fk="id", column="winner_code")
        period_guard = terminal_guard_case(
            table="event", event_fk="id", column="last_period"
        )
        # Simulate event_list/event_detail repository upsert with both
        # winner_code and last_period from a "finished" payload landing
        # after terminal_state was inserted.
        await self.connection.execute(
            f"""
            INSERT INTO event (id, slug, status_code, winner_code, last_period, start_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id) DO UPDATE SET
                winner_code = {winner_guard},
                last_period = {period_guard}
            """,
            16141591,
            "fill-winner-and-period",
            100,
            3,
            "2nd half",
            1778266800,
        )

        row = await self.connection.fetchrow(
            "SELECT winner_code, last_period FROM event WHERE id=$1",
            16141591,
        )
        self.assertEqual(row["winner_code"], 3)
        self.assertEqual(row["last_period"], "2nd half")

    async def test_event_score_initial_fill_when_existing_null_with_terminal_state(self) -> None:
        """F-8 hotfix: every event_score column also permits NULL→value
        initial fill. A finalize parse arriving after terminal_state
        was inserted must be able to write the final scores even though
        the row already exists with NULL values."""
        from schema_inspector.storage._terminal_guard import terminal_guard_case

        await self.connection.execute(
            """
            INSERT INTO event_status (code, description, type) VALUES (100, 'Ended', 'finished');
            INSERT INTO event (id, slug, status_code, start_timestamp)
                VALUES (16141592, 'race-condition-event-3', 100, 1778266800);
            -- Pre-existing event_score row with all NULL values (simulating
            -- a row created during early bootstrap before scores were known).
            INSERT INTO event_score (event_id, side) VALUES (16141592, 'home'), (16141592, 'away');
            INSERT INTO endpoint_registry (pattern) VALUES ('/api/v1/event/{event_id}');
            INSERT INTO api_payload_snapshot (id, scope_key, endpoint_pattern, http_status, payload_hash, fetched_at)
                VALUES (45661227, 'k', '/api/v1/event/{event_id}', 200, 'h',
                        '2026-05-09 00:45:45+03');
            INSERT INTO event_terminal_state (event_id, terminal_status, finalized_at, final_snapshot_id)
                VALUES (16141592, 'finished', '2026-05-09 02:00:00+03', 45661227);
            """
        )

        score_columns = (
            "current",
            "display",
            "aggregated",
            "normaltime",
            "overtime",
            "penalties",
            "period1",
            "period2",
            "period3",
            "period4",
            "extra1",
            "extra2",
            "series",
        )
        guards = ",\n".join(
            f"{col} = {terminal_guard_case(table='event_score', event_fk='event_id', column=col)}"
            for col in score_columns
        )
        await self.connection.execute(
            f"""
            INSERT INTO event_score (event_id, side, current, display, aggregated, normaltime, period1, period2)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (event_id, side) DO UPDATE SET
                {guards}
            """,
            16141592, "home", 3, 3, 3, 3, 2, 1,
        )

        row = await self.connection.fetchrow(
            "SELECT current, display, aggregated, normaltime, period1, period2 "
            "FROM event_score WHERE event_id=$1 AND side='home'",
            16141592,
        )
        self.assertEqual(row["current"], 3)
        self.assertEqual(row["display"], 3)
        self.assertEqual(row["aggregated"], 3)
        self.assertEqual(row["normaltime"], 3)
        self.assertEqual(row["period1"], 2)
        self.assertEqual(row["period2"], 1)

    async def test_event_winner_null_erasure_blocked_with_terminal_state(self) -> None:
        """F-8 hotfix idempotence check: when the column already has a
        non-NULL value and ``event_terminal_state`` exists, a payload
        with NULL must NOT erase it. The COALESCE inside the guarded
        branch returns the existing value when EXCLUDED is NULL."""
        from schema_inspector.storage._terminal_guard import terminal_guard_case

        await self.connection.execute(
            """
            INSERT INTO event_status (code, description, type) VALUES (100, 'Ended', 'finished');
            INSERT INTO event (id, slug, status_code, winner_code, start_timestamp)
                VALUES (16141593, 'erasure-test', 100, 1, 1778266800);
            INSERT INTO endpoint_registry (pattern) VALUES ('/api/v1/event/{event_id}');
            INSERT INTO api_payload_snapshot (id, scope_key, endpoint_pattern, http_status, payload_hash, fetched_at)
                VALUES (45661228, 'k', '/api/v1/event/{event_id}', 200, 'h',
                        '2026-05-09 00:45:45+03');
            INSERT INTO event_terminal_state (event_id, terminal_status, finalized_at, final_snapshot_id)
                VALUES (16141593, 'finished', '2026-05-09 02:00:00+03', 45661228);
            """
        )

        winner_guard = terminal_guard_case(table="event", event_fk="id", column="winner_code")
        await self.connection.execute(
            f"""
            INSERT INTO event (id, slug, status_code, winner_code, start_timestamp)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE SET
                winner_code = {winner_guard}
            """,
            16141593,
            "stale-payload",
            100,
            None,  # ← stale payload tries to erase winner
            1778266800,
        )

        row = await self.connection.fetchrow("SELECT winner_code FROM event WHERE id=$1", 16141593)
        # Erasure attempt blocked — existing winner_code=1 preserved.
        self.assertEqual(row["winner_code"], 1)

    async def test_event_lifecycle_columns_normal_update_without_terminal_state(self) -> None:
        """F-8 Phase 1.5 negative: when no terminal_state row exists, the
        guard branch falls through to EXCLUDED and ordinary updates
        proceed — the guard must NEVER block legitimate live transitions
        (e.g., notstarted → 1st half → 2nd half) for unfinished events."""
        from schema_inspector.storage._terminal_guard import terminal_guard_case

        await self.connection.execute(
            """
            INSERT INTO event_status (code, description, type) VALUES
                (6, '1st half', 'inprogress'),
                (7, '2nd half', 'inprogress');
            INSERT INTO event (id, slug, status_code, last_period, start_timestamp)
                VALUES (14083570, 'live-event', 6, 'period1', 1778266800);
            """
        )
        # No event_terminal_state row.

        status_guard = terminal_guard_case(table="event", event_fk="id", column="status_code")
        period_guard = terminal_guard_case(
            table="event", event_fk="id", column="last_period"
        )
        await self.connection.execute(
            f"""
            INSERT INTO event (id, slug, status_code, last_period, start_timestamp)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (id) DO UPDATE SET
                status_code = {status_guard},
                last_period = {period_guard}
            """,
            14083570,
            "live-event-2nd-half",
            7,
            "period2",
            1778266800,
        )

        row = await self.connection.fetchrow(
            "SELECT status_code, last_period FROM event WHERE id=$1",
            14083570,
        )
        # Without terminal_state the new payload values must win.
        self.assertEqual(row["status_code"], 7)
        self.assertEqual(row["last_period"], "period2")

    async def test_status_code_null_payload_keeps_existing_when_no_terminal_state(self) -> None:
        """F-8 P0 guard must preserve the original COALESCE NULL-fallback
        behaviour for events without terminal state: a payload without
        ``status_code`` (e.g., a partial entity row from a sibling parser)
        must not blank out the existing status_code."""
        repository = NormalizeRepository()
        await self.connection.execute(
            """
            INSERT INTO sport (id, slug, name) VALUES (1, 'football', 'Football');
            INSERT INTO event_status (code, description, type) VALUES (6, '1st half', 'inprogress');
            INSERT INTO event (id, slug, status_code, start_timestamp)
                VALUES (14083570, 'partial-update', 6, 1778266800);
            """
        )

        result = ParseResult(
            snapshot_id=45712001,
            parser_family="event_root",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "event": (
                    {
                        "id": 14083570,
                        "slug": "partial-update-touch",
                        "tournament_id": None,
                        "unique_tournament_id": None,
                        "season_id": None,
                        "home_team_id": None,
                        "away_team_id": None,
                        "venue_id": None,
                        "status_code": None,  # ← NULL payload status
                        "start_timestamp": 1778266800,
                    },
                ),
            },
        )

        await repository.persist_parse_result(self.connection, result)

        row = await self.connection.fetchrow("SELECT status_code FROM event WHERE id = $1", 14083570)
        # NULL payload preserves existing status_code via COALESCE fallback.
        self.assertEqual(row["status_code"], 6)


if __name__ == "__main__":
    unittest.main()
