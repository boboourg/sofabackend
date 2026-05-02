from __future__ import annotations

import unittest
from unittest.mock import patch

from schema_inspector.endpoints import event_list_registry_entries
from schema_inspector.event_list_job import EventListIngestJob
from schema_inspector.event_list_parser import (
    EventChangeItemRecord,
    EventFilterValueRecord,
    EventListBundle,
    EventRecord,
    EventRoundInfoRecord,
    EventScoreRecord,
    EventSeasonRecord,
    EventStatusRecord,
    EventStatusTimeRecord,
    EventTeamRecord,
    EventTimeRecord,
    EventVarInProgressRecord,
    TournamentRecord,
)
from schema_inspector.event_list_repository import EventListRepository, EventListWriteResult
from schema_inspector.storage.raw_repository import reset_all_registry_sync_caches
from schema_inspector.competition_parser import (
    ApiPayloadSnapshotRecord,
    CategoryRecord,
    CountryRecord,
    SportRecord,
    UniqueTournamentRecord,
)


class _FakeExecutor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    async def executemany(self, command: str, args):
        self.executemany_calls.append((command, list(args)))
        return None


class _FakeParser:
    def __init__(self, bundle: EventListBundle) -> None:
        self.bundle = bundle
        self.calls: list[tuple[str, tuple[object, ...], str, float]] = []

    async def fetch_scheduled_events(
        self,
        date: str,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> EventListBundle:
        self.calls.append(("scheduled", (date,), sport_slug, timeout))
        return self.bundle

    async def fetch_live_events(self, *, sport_slug: str = "football", timeout: float = 20.0) -> EventListBundle:
        self.calls.append(("live", (), sport_slug, timeout))
        return self.bundle

    async def fetch_featured_events(
        self,
        unique_tournament_id: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> EventListBundle:
        self.calls.append(("featured", (unique_tournament_id,), sport_slug, timeout))
        return self.bundle

    async def fetch_round_events(
        self,
        unique_tournament_id: int,
        season_id: int,
        round_number: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> EventListBundle:
        self.calls.append(("round", (unique_tournament_id, season_id, round_number), sport_slug, timeout))
        return self.bundle

    async def fetch_season_last_events(
        self,
        unique_tournament_id: int,
        season_id: int,
        page: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> EventListBundle:
        self.calls.append(("season_last", (unique_tournament_id, season_id, page), sport_slug, timeout))
        return self.bundle

    async def fetch_season_next_events(
        self,
        unique_tournament_id: int,
        season_id: int,
        page: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> EventListBundle:
        self.calls.append(("season_next", (unique_tournament_id, season_id, page), sport_slug, timeout))
        return self.bundle


class _FakeRepository:
    def __init__(self, result: EventListWriteResult) -> None:
        self.result = result
        self.calls: list[tuple[object, EventListBundle]] = []

    async def upsert_bundle(self, executor, bundle: EventListBundle) -> EventListWriteResult:
        self.calls.append((executor, bundle))
        return self.result


class _FakeTransaction:
    def __init__(self, connection: object) -> None:
        self.connection = connection

    async def __aenter__(self) -> object:
        return self.connection

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        return None


class _FakeDatabase:
    def __init__(self, connection: object) -> None:
        self.connection = connection
        self.transaction_calls = 0

    def transaction(self) -> _FakeTransaction:
        self.transaction_calls += 1
        return _FakeTransaction(self.connection)


class _FakeConnectionContext:
    def __init__(self, connection: object) -> None:
        self._connection = connection

    async def __aenter__(self) -> object:
        return self._connection

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        return None


class _FakeDatabaseWithReads:
    def __init__(self, *, read_connection: object, write_connection: object) -> None:
        self._read_connection = read_connection
        self._write_connection = write_connection
        self.transaction_calls = 0
        self.connection_calls = 0

    def transaction(self) -> _FakeTransaction:
        self.transaction_calls += 1
        return _FakeTransaction(self._write_connection)

    def connection(self) -> _FakeConnectionContext:
        self.connection_calls += 1
        return _FakeConnectionContext(self._read_connection)


class _FakeBundleAwareRepository(_FakeRepository):
    def __init__(self, result: EventListWriteResult, previous_states) -> None:
        super().__init__(result)
        self.previous_states = previous_states
        self.bundle_loader_calls: list[tuple[object, EventListBundle]] = []

    async def load_surface_states_for_bundle(self, executor, bundle: EventListBundle):
        self.bundle_loader_calls.append((executor, bundle))
        return self.previous_states

    async def load_surface_states(self, executor, event_ids):
        del executor, event_ids
        raise AssertionError("bundle-aware surface loader should be preferred")


class _FakeCorrectionDetector:
    def __init__(self) -> None:
        self.calls: list[tuple[EventListBundle, object]] = []

    def detect(self, *, bundle: EventListBundle, previous_states, existing_source_slug=None, incoming_source_slug=None):
        del existing_source_slug, incoming_source_slug
        self.calls.append((bundle, previous_states))
        return ()


class _FakeSurfaceStateExecutor:
    async def fetch(self, command: str, *args):
        normalized = " ".join(command.split())
        if normalized.startswith("SELECT id, status_code, winner_code, aggregated_winner_code FROM event WHERE id = ANY"):
            requested_ids = tuple(int(item) for item in args[0])
            if requested_ids == (15362622,):
                return [
                    {
                        "id": 15362622,
                        "status_code": 100,
                        "winner_code": 1,
                        "aggregated_winner_code": 1,
                    }
                ]
            return []
        if "JOIN unique_tournament AS ut" in normalized and "WHERE e.custom_id = ANY($1::text[])" in normalized:
            return [
                {
                    "id": 15362622,
                    "custom_id": "xyz123",
                    "sport_id": 1,
                    "start_timestamp": 1775779200,
                    "home_team_id": 44,
                    "away_team_id": 45,
                }
            ]
        if normalized.startswith("SELECT event_id, side, current FROM event_score"):
            requested_ids = tuple(int(item) for item in args[0])
            if requested_ids == (15362622,):
                return [
                    {"event_id": 15362622, "side": "home", "current": 2},
                    {"event_id": 15362622, "side": "away", "current": 1},
                ]
            return []
        if normalized.startswith("SELECT event_id, home_team, away_team FROM event_var_in_progress"):
            return [{"event_id": 15362622, "home_team": True, "away_team": False}]
        if normalized.startswith("SELECT event_id, change_timestamp, ordinal, change_value FROM event_change_item"):
            return [{"event_id": 15362622, "change_timestamp": 1712745600, "ordinal": 0, "change_value": "score"}]
        raise AssertionError(f"Unexpected query: {normalized}")


def _build_bundle() -> EventListBundle:
    return EventListBundle(
        registry_entries=event_list_registry_entries(),
        payload_snapshots=(
            ApiPayloadSnapshotRecord(
                endpoint_pattern="/api/v1/sport/football/events/live",
                source_url="https://www.sofascore.com/api/v1/sport/football/events/live",
                envelope_key="events",
                context_entity_type=None,
                context_entity_id=None,
                payload={"events": [{"id": 14083191}]},
                fetched_at="2026-04-10T10:00:00+00:00",
            ),
        ),
        sports=(SportRecord(id=1, slug="football", name="Football"),),
        countries=(CountryRecord(alpha2="EN", alpha3="ENG", slug="england", name="England"),),
        categories=(
            CategoryRecord(
                id=1,
                slug="england",
                name="England",
                sport_id=1,
                alpha2="EN",
                flag="england",
                priority=10,
                country_alpha2="EN",
                field_translations={"nameTranslation": {"ru": "Англия"}},
            ),
        ),
        teams=(
            EventTeamRecord(
                id=44,
                slug="liverpool",
                name="Liverpool",
                short_name="Liverpool",
                name_code="LIV",
                sport_id=1,
                country_alpha2="EN",
                gender="M",
                type=0,
                national=False,
                disabled=False,
                user_count=100,
                field_translations={"nameTranslation": {"ru": "Ливерпуль"}},
                team_colors={"primary": "#dd0000", "secondary": "#ffffff"},
            ),
            EventTeamRecord(
                id=45,
                slug="manchester-city",
                name="Manchester City",
                short_name="Man City",
                name_code="MCI",
                sport_id=1,
                country_alpha2="EN",
                parent_team_id=440,
            ),
            EventTeamRecord(id=440, slug="city-root", name="City Root"),
        ),
        unique_tournaments=(
            UniqueTournamentRecord(
                id=17,
                slug="premier-league",
                name="Premier League",
                category_id=1,
                country_alpha2="EN",
                gender="M",
                primary_color_hex="#3c1c5a",
                secondary_color_hex="#f80158",
                user_count=1179222,
                has_event_player_statistics=True,
                has_performance_graph_feature=True,
                display_inverse_home_away_teams=False,
                field_translations={"nameTranslation": {"ru": "Премьер-лига"}},
            ),
        ),
        seasons=(
            EventSeasonRecord(
                id=76986,
                name="Premier League 25/26",
                year="25/26",
                editor=False,
                season_coverage_info={"editorCoverageLevel": "full"},
            ),
        ),
        tournaments=(
            TournamentRecord(
                id=1,
                slug="premier-league",
                name="Premier League",
                category_id=1,
                unique_tournament_id=17,
                priority=10,
                field_translations={"nameTranslation": {"ru": "Премьер-лига"}},
            ),
        ),
        event_statuses=(EventStatusRecord(code=100, description="1st half", type="inprogress"),),
        events=(
            EventRecord(
                id=14083191,
                slug="liverpool-manchester-city",
                custom_id="xyz123",
                detail_id=10,
                tournament_id=1,
                unique_tournament_id=17,
                season_id=76986,
                home_team_id=44,
                away_team_id=45,
                status_code=100,
                season_statistics_type="overall",
                start_timestamp=1775779200,
                coverage=1,
                winner_code=1,
                aggregated_winner_code=1,
                home_red_cards=0,
                away_red_cards=0,
                previous_leg_event_id=None,
                cup_matches_in_round=1,
                default_period_count=2,
                default_period_length=45,
                default_overtime_length=15,
                last_period="2nd",
                correct_ai_insight=False,
                correct_halftime_ai_insight=False,
                feed_locked=False,
                is_editor=False,
                show_toto_promo=False,
                crowdsourcing_enabled=True,
                crowdsourcing_data_display_enabled=True,
                final_result_only=False,
                has_event_player_statistics=True,
                has_event_player_heat_map=True,
                has_global_highlights=False,
                has_xg=True,
            ),
        ),
        event_round_infos=(EventRoundInfoRecord(event_id=14083191, round_number=32, name="Round 32"),),
        event_status_times=(
            EventStatusTimeRecord(
                event_id=14083191,
                prefix="'",
                timestamp=1712745600,
                initial=45,
                max=45,
                extra=2,
            ),
        ),
        event_times=(
            EventTimeRecord(
                event_id=14083191,
                current_period_start_timestamp=1712742000,
                initial=45,
                max=45,
                extra=2,
                overtime_length=15,
                period_length=45,
                total_period_count=2,
            ),
        ),
        event_var_in_progress_items=(EventVarInProgressRecord(event_id=14083191, home_team=True, away_team=False),),
        event_scores=(
            EventScoreRecord(event_id=14083191, side="home", current=1, display=1, period1=1),
            EventScoreRecord(event_id=14083191, side="away", current=0, display=0, period1=0),
        ),
        event_filter_values=(
            EventFilterValueRecord(event_id=14083191, filter_name="category", ordinal=0, filter_value="live"),
        ),
        event_change_items=(
            EventChangeItemRecord(
                event_id=14083191,
                change_timestamp=1712745600,
                ordinal=0,
                change_value="score",
            ),
        ),
    )


class EventListStorageTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        reset_all_registry_sync_caches()

    async def test_event_list_repository_skips_redundant_sport_writes_after_first_upsert(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EventListRepository()

        await repository._upsert_sports(executor, bundle)
        await repository._upsert_sports(executor, bundle)

        sport_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO sport" in sql]
        self.assertEqual(len(sport_statements), 1)

    async def test_event_list_repository_skips_redundant_country_writes_after_first_upsert(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EventListRepository()

        await repository._upsert_countries(executor, bundle)
        await repository._upsert_countries(executor, bundle)

        country_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO country" in sql]
        self.assertEqual(len(country_statements), 1)
        self.assertIn("IS DISTINCT FROM", country_statements[0])

    async def test_event_list_repository_only_caches_country_rows_after_post_commit(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EventListRepository()
        registered_hooks: list[object] = []

        def _capture_post_commit_hook(callback):
            registered_hooks.append(callback)
            return True

        with patch(
            "schema_inspector.event_list_repository.register_post_commit_hook",
            side_effect=_capture_post_commit_hook,
        ):
            await repository._upsert_countries(executor, bundle)
            await repository._upsert_countries(executor, bundle)

        country_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO country" in sql]
        self.assertEqual(len(country_statements), 2)
        self.assertEqual(len(registered_hooks), 2)

        registered_hooks[-1]()
        await repository._upsert_countries(executor, bundle)

        country_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO country" in sql]
        self.assertEqual(len(country_statements), 2)

    async def test_event_list_repository_skips_redundant_category_writes_after_first_upsert(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EventListRepository()

        await repository._upsert_categories(executor, bundle)
        await repository._upsert_categories(executor, bundle)

        category_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO category (" in sql]
        self.assertEqual(len(category_statements), 1)
        self.assertIn("IS DISTINCT FROM", category_statements[0])

    async def test_event_list_repository_only_caches_category_rows_after_post_commit(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EventListRepository()
        registered_hooks: list[object] = []

        def _capture_post_commit_hook(callback):
            registered_hooks.append(callback)
            return True

        with patch(
            "schema_inspector.event_list_repository.register_post_commit_hook",
            side_effect=_capture_post_commit_hook,
        ):
            await repository._upsert_categories(executor, bundle)
            await repository._upsert_categories(executor, bundle)

        category_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO category (" in sql]
        self.assertEqual(len(category_statements), 2)
        self.assertEqual(len(registered_hooks), 2)

        registered_hooks[-1]()
        await repository._upsert_categories(executor, bundle)

        category_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO category (" in sql]
        self.assertEqual(len(category_statements), 2)

    async def test_event_list_repository_category_upsert_uses_distinct_guard(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()

        await EventListRepository()._upsert_categories(executor, bundle)

        category_sql = next(sql for sql, _ in executor.executemany_calls if "INSERT INTO category" in sql)
        self.assertIn("IS DISTINCT FROM", category_sql)

    async def test_event_list_repository_unique_tournament_upsert_uses_distinct_guard(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()

        await EventListRepository()._upsert_unique_tournaments(executor, bundle)

        unique_tournament_sql = next(
            sql for sql, _ in executor.executemany_calls if "INSERT INTO unique_tournament" in sql
        )
        self.assertIn("IS DISTINCT FROM", unique_tournament_sql)

    async def test_event_list_repository_only_caches_unique_tournament_rows_after_post_commit(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EventListRepository()
        registered_hooks: list[object] = []

        def _capture_post_commit_hook(callback):
            registered_hooks.append(callback)
            return True

        with patch(
            "schema_inspector.event_list_repository.register_post_commit_hook",
            side_effect=_capture_post_commit_hook,
        ):
            await repository._upsert_unique_tournaments(executor, bundle)
            await repository._upsert_unique_tournaments(executor, bundle)

        unique_tournament_statements = [
            sql for sql, _ in executor.executemany_calls if "INSERT INTO unique_tournament" in sql
        ]
        self.assertEqual(len(unique_tournament_statements), 2)
        self.assertEqual(len(registered_hooks), 2)

        registered_hooks[-1]()
        await repository._upsert_unique_tournaments(executor, bundle)

        unique_tournament_statements = [
            sql for sql, _ in executor.executemany_calls if "INSERT INTO unique_tournament" in sql
        ]
        self.assertEqual(len(unique_tournament_statements), 2)

    async def test_event_list_repository_season_upsert_uses_distinct_guard(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()

        await EventListRepository()._upsert_seasons(executor, bundle)

        season_sql = next(sql for sql, _ in executor.executemany_calls if "INSERT INTO season" in sql)
        self.assertIn("IS DISTINCT FROM", season_sql)

    async def test_event_list_repository_only_caches_season_rows_after_post_commit(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EventListRepository()
        registered_hooks: list[object] = []

        def _capture_post_commit_hook(callback):
            registered_hooks.append(callback)
            return True

        with patch(
            "schema_inspector.event_list_repository.register_post_commit_hook",
            side_effect=_capture_post_commit_hook,
        ):
            await repository._upsert_seasons(executor, bundle)
            await repository._upsert_seasons(executor, bundle)

        season_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO season" in sql]
        self.assertEqual(len(season_statements), 2)
        self.assertEqual(len(registered_hooks), 2)

        registered_hooks[-1]()
        await repository._upsert_seasons(executor, bundle)

        season_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO season" in sql]
        self.assertEqual(len(season_statements), 2)

    async def test_event_list_repository_only_caches_event_status_rows_after_post_commit(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EventListRepository()
        registered_hooks: list[object] = []

        def _capture_post_commit_hook(callback):
            registered_hooks.append(callback)
            return True

        with patch(
            "schema_inspector.event_list_repository.register_post_commit_hook",
            side_effect=_capture_post_commit_hook,
        ):
            await repository._upsert_event_statuses(executor, bundle)
            await repository._upsert_event_statuses(executor, bundle)

        event_status_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO event_status" in sql]
        self.assertEqual(len(event_status_statements), 2)
        self.assertEqual(len(registered_hooks), 2)

        registered_hooks[-1]()
        await repository._upsert_event_statuses(executor, bundle)

        event_status_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO event_status" in sql]
        self.assertEqual(len(event_status_statements), 2)

    async def test_event_list_repository_sorts_event_status_rows_before_upsert(self) -> None:
        bundle = EventListBundle(
            **{
                **_build_bundle().__dict__,
                "event_statuses": (
                    EventStatusRecord(code=300, description="AET", type="inprogress"),
                    EventStatusRecord(code=100, description="1st half", type="inprogress"),
                    EventStatusRecord(code=200, description="Halftime", type="inprogress"),
                ),
            }
        )
        executor = _FakeExecutor()

        await EventListRepository()._upsert_event_statuses(executor, bundle)

        event_status_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO event_status" in sql)
        self.assertEqual([row[0] for row in event_status_rows], [100, 200, 300])

    async def test_event_list_repository_matches_previous_surface_state_by_custom_id(self) -> None:
        bundle = _build_bundle()
        incoming_event = bundle.events[0]
        bundle = EventListBundle(
            **{
                **bundle.__dict__,
                "events": (
                    EventRecord(
                        id=16006762,
                        slug=incoming_event.slug,
                        custom_id=incoming_event.custom_id,
                        detail_id=incoming_event.detail_id,
                        tournament_id=incoming_event.tournament_id,
                        unique_tournament_id=incoming_event.unique_tournament_id,
                        season_id=incoming_event.season_id,
                        home_team_id=incoming_event.home_team_id,
                        away_team_id=incoming_event.away_team_id,
                        status_code=incoming_event.status_code,
                        season_statistics_type=incoming_event.season_statistics_type,
                        start_timestamp=incoming_event.start_timestamp,
                        coverage=incoming_event.coverage,
                        winner_code=incoming_event.winner_code,
                        aggregated_winner_code=incoming_event.aggregated_winner_code,
                        home_red_cards=incoming_event.home_red_cards,
                        away_red_cards=incoming_event.away_red_cards,
                        previous_leg_event_id=incoming_event.previous_leg_event_id,
                        cup_matches_in_round=incoming_event.cup_matches_in_round,
                        default_period_count=incoming_event.default_period_count,
                        default_period_length=incoming_event.default_period_length,
                        default_overtime_length=incoming_event.default_overtime_length,
                        last_period=incoming_event.last_period,
                        correct_ai_insight=incoming_event.correct_ai_insight,
                        correct_halftime_ai_insight=incoming_event.correct_halftime_ai_insight,
                        feed_locked=incoming_event.feed_locked,
                        is_editor=incoming_event.is_editor,
                        show_toto_promo=incoming_event.show_toto_promo,
                        crowdsourcing_enabled=incoming_event.crowdsourcing_enabled,
                        crowdsourcing_data_display_enabled=incoming_event.crowdsourcing_data_display_enabled,
                        final_result_only=incoming_event.final_result_only,
                        has_event_player_statistics=incoming_event.has_event_player_statistics,
                        has_event_player_heat_map=incoming_event.has_event_player_heat_map,
                        has_global_highlights=incoming_event.has_global_highlights,
                        has_xg=incoming_event.has_xg,
                    ),
                ),
                "event_scores": (
                    EventScoreRecord(event_id=16006762, side="home", current=1, display=1, period1=1),
                    EventScoreRecord(event_id=16006762, side="away", current=0, display=0, period1=0),
                ),
                "event_var_in_progress_items": (
                    EventVarInProgressRecord(event_id=16006762, home_team=True, away_team=False),
                ),
                "event_change_items": (
                    EventChangeItemRecord(
                        event_id=16006762,
                        change_timestamp=1712745600,
                        ordinal=0,
                        change_value="score",
                    ),
                ),
            }
        )
        repository = EventListRepository()

        previous_states = await repository.load_surface_states_for_bundle(_FakeSurfaceStateExecutor(), bundle)

        self.assertEqual(tuple(previous_states), (16006762,))
        self.assertEqual(previous_states[16006762].event_id, 15362622)
        self.assertEqual(previous_states[16006762].home_score, 2)
        self.assertEqual(previous_states[16006762].away_score, 1)
        self.assertEqual(previous_states[16006762].changes, ("score",))

    async def test_event_list_repository_writes_expected_tables(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EventListRepository()

        result = await repository.upsert_bundle(executor, bundle)

        self.assertEqual(result.event_rows, 1)
        self.assertEqual(result.payload_snapshot_rows, 1)
        statements = [sql for sql, _ in executor.executemany_calls]
        self.assertTrue(any("INSERT INTO endpoint_registry" in sql for sql in statements))
        endpoint_registry_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO endpoint_registry" in sql)
        self.assertEqual(endpoint_registry_rows[0][6], "sofascore")
        self.assertEqual(endpoint_registry_rows[0][7], "v1")
        self.assertTrue(any("INSERT INTO tournament " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_status " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event (" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_score" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO api_payload_snapshot" in sql for sql in statements))

    async def test_event_list_ingest_job_uses_transaction_and_repository(self) -> None:
        bundle = _build_bundle()
        parser = _FakeParser(bundle)
        repository_result = EventListWriteResult(
            endpoint_registry_rows=4,
            payload_snapshot_rows=1,
            sport_rows=1,
            country_rows=1,
            category_rows=1,
            team_rows=3,
            unique_tournament_rows=1,
            season_rows=1,
            tournament_rows=1,
            event_status_rows=1,
            event_rows=1,
            event_round_info_rows=1,
            event_status_time_rows=1,
            event_time_rows=1,
            event_var_in_progress_rows=1,
            event_score_rows=2,
            event_filter_value_rows=1,
            event_change_item_rows=1,
        )
        repository = _FakeRepository(repository_result)
        connection = object()
        database = _FakeDatabase(connection)
        job = EventListIngestJob(parser, repository, database)

        result = await job.run_round(17, 76986, 32, timeout=12.5)

        self.assertEqual(parser.calls, [("round", (17, 76986, 32), "football", 12.5)])
        self.assertEqual(repository.calls, [(connection, bundle)])
        self.assertEqual(database.transaction_calls, 1)
        self.assertEqual(result.written.event_rows, 1)
        self.assertEqual(result.job_name, "round:17:76986:32")

    async def test_event_list_ingest_job_runs_season_last_and_next_pages(self) -> None:
        bundle = _build_bundle()
        parser = _FakeParser(bundle)
        repository_result = EventListWriteResult(
            endpoint_registry_rows=4,
            payload_snapshot_rows=1,
            sport_rows=1,
            country_rows=1,
            category_rows=1,
            team_rows=3,
            unique_tournament_rows=1,
            season_rows=1,
            tournament_rows=1,
            event_status_rows=1,
            event_rows=1,
            event_round_info_rows=1,
            event_status_time_rows=1,
            event_time_rows=1,
            event_var_in_progress_rows=1,
            event_score_rows=2,
            event_filter_value_rows=1,
            event_change_item_rows=1,
        )
        repository = _FakeRepository(repository_result)
        connection = object()
        database = _FakeDatabase(connection)
        job = EventListIngestJob(parser, repository, database)

        last_result = await job.run_season_last(17, 76986, 0, sport_slug="football", timeout=12.5)
        next_result = await job.run_season_next(17, 76986, 1, sport_slug="football", timeout=12.5)

        self.assertEqual(
            parser.calls,
            [
                ("season_last", (17, 76986, 0), "football", 12.5),
                ("season_next", (17, 76986, 1), "football", 12.5),
            ],
        )
        self.assertEqual(repository.calls, [(connection, bundle), (connection, bundle)])
        self.assertEqual(database.transaction_calls, 2)
        self.assertEqual(last_result.job_name, "season_last:17:76986:0")
        self.assertEqual(next_result.job_name, "season_next:17:76986:1")

    async def test_event_list_ingest_job_prefers_bundle_aware_surface_loader(self) -> None:
        bundle = _build_bundle()
        parser = _FakeParser(bundle)
        repository_result = EventListWriteResult(
            endpoint_registry_rows=4,
            payload_snapshot_rows=1,
            sport_rows=1,
            country_rows=1,
            category_rows=1,
            team_rows=3,
            unique_tournament_rows=1,
            season_rows=1,
            tournament_rows=1,
            event_status_rows=1,
            event_rows=1,
            event_round_info_rows=1,
            event_status_time_rows=1,
            event_time_rows=1,
            event_var_in_progress_rows=1,
            event_score_rows=2,
            event_filter_value_rows=1,
            event_change_item_rows=1,
        )
        previous_states = {14083191: object()}
        repository = _FakeBundleAwareRepository(repository_result, previous_states)
        detector = _FakeCorrectionDetector()
        read_connection = object()
        write_connection = object()
        database = _FakeDatabaseWithReads(read_connection=read_connection, write_connection=write_connection)
        job = EventListIngestJob(parser, repository, database, correction_detector=detector)

        result = await job.run_live(timeout=11.0)

        self.assertEqual(parser.calls, [("live", (), "football", 11.0)])
        self.assertEqual(repository.bundle_loader_calls, [(read_connection, bundle)])
        self.assertEqual(repository.calls, [(write_connection, bundle)])
        self.assertEqual(detector.calls, [(bundle, previous_states)])
        self.assertEqual(database.connection_calls, 1)
        self.assertEqual(database.transaction_calls, 1)
        self.assertEqual(result.job_name, "live:football")


if __name__ == "__main__":
    unittest.main()
