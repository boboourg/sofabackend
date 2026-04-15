from __future__ import annotations

import unittest

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
    async def test_event_list_repository_writes_expected_tables(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = EventListRepository()

        result = await repository.upsert_bundle(executor, bundle)

        self.assertEqual(result.event_rows, 1)
        self.assertEqual(result.payload_snapshot_rows, 1)
        statements = [sql for sql, _ in executor.executemany_calls]
        self.assertTrue(any("INSERT INTO endpoint_registry" in sql for sql in statements))
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


if __name__ == "__main__":
    unittest.main()
