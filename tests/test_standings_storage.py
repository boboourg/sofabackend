from __future__ import annotations

import unittest

from schema_inspector.endpoints import standings_registry_entries
from schema_inspector.event_list_parser import EventTeamRecord, TournamentRecord
from schema_inspector.standings_job import StandingsIngestJob
from schema_inspector.standings_parser import (
    ApiPayloadSnapshotRecord,
    StandingPromotionRecord,
    StandingRecord,
    StandingRowRecord,
    StandingTieBreakingRuleRecord,
    StandingsBundle,
)
from schema_inspector.standings_repository import StandingsRepository, StandingsWriteResult
from schema_inspector.competition_parser import CategoryRecord, CountryRecord, SportRecord, UniqueTournamentRecord


class _FakeExecutor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    async def executemany(self, command: str, args):
        self.executemany_calls.append((command, list(args)))
        return None


class _FakeParser:
    def __init__(self, bundle: StandingsBundle) -> None:
        self.bundle = bundle
        self.unique_tournament_calls: list[tuple[int, int, tuple[str, ...], float]] = []

    async def fetch_unique_tournament_standings(
        self,
        unique_tournament_id: int,
        season_id: int,
        *,
        scopes=("total",),
        timeout: float = 20.0,
    ) -> StandingsBundle:
        self.unique_tournament_calls.append((unique_tournament_id, season_id, tuple(scopes), timeout))
        return self.bundle


class _FakeRepository:
    def __init__(self, result: StandingsWriteResult) -> None:
        self.result = result
        self.calls: list[tuple[object, StandingsBundle]] = []

    async def upsert_bundle(self, executor, bundle: StandingsBundle) -> StandingsWriteResult:
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


def _build_bundle() -> StandingsBundle:
    return StandingsBundle(
        registry_entries=standings_registry_entries(),
        payload_snapshots=(
            ApiPayloadSnapshotRecord(
                endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/{scope}",
                source_url="https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/standings/total",
                envelope_key="standings",
                context_entity_type="season",
                context_entity_id=76986,
                payload={"standings": []},
                fetched_at="2026-04-10T10:00:00+00:00",
            ),
        ),
        sports=(SportRecord(id=1, slug="football", name="Football"),),
        countries=(CountryRecord(alpha2="EN", alpha3="ENG", slug="england", name="England"),),
        categories=(CategoryRecord(id=1, slug="england", name="England", sport_id=1, country_alpha2="EN"),),
        unique_tournaments=(
            UniqueTournamentRecord(
                id=17,
                slug="premier-league",
                name="Premier League",
                category_id=1,
                country_alpha2="EN",
                primary_color_hex="#3c1c5a",
                secondary_color_hex="#f80158",
                user_count=1178753,
                has_performance_graph_feature=True,
                display_inverse_home_away_teams=False,
            ),
        ),
        tournaments=(
            TournamentRecord(
                id=1,
                slug="premier-league",
                name="Premier League",
                category_id=1,
                unique_tournament_id=17,
                is_group=False,
                is_live=False,
                priority=701,
            ),
        ),
        teams=(
            EventTeamRecord(
                id=42,
                slug="arsenal",
                name="Arsenal",
                short_name="Arsenal",
                name_code="ARS",
                sport_id=1,
                country_alpha2="EN",
                gender="M",
                type=0,
                national=False,
                disabled=False,
                user_count=3322281,
            ),
        ),
        tie_breaking_rules=(StandingTieBreakingRuleRecord(id=2393, text="Points, goal difference, goals scored."),),
        promotions=(StandingPromotionRecord(id=804, text="Champions League"),),
        standings=(
            StandingRecord(
                id=178737,
                season_id=76986,
                tournament_id=1,
                name="Premier League 25/26",
                type="total",
                updated_at_timestamp=1775634161,
                tie_breaking_rule_id=2393,
                descriptions=(),
            ),
        ),
        standing_rows=(
            StandingRowRecord(
                id=1637254,
                standing_id=178737,
                team_id=42,
                position=1,
                matches=31,
                wins=21,
                draws=7,
                losses=3,
                points=70,
                scores_for=61,
                scores_against=22,
                score_diff_formatted="+39",
                promotion_id=804,
                descriptions=(),
            ),
        ),
    )


class StandingsStorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_standings_repository_writes_expected_tables(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = StandingsRepository()

        result = await repository.upsert_bundle(executor, bundle)

        self.assertEqual(result.standing_rows, 1)
        self.assertEqual(result.standing_row_rows, 1)
        statements = [sql for sql, _ in executor.executemany_calls]
        self.assertTrue(any("INSERT INTO standing_tie_breaking_rule " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO standing_promotion " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO standing (" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO standing_row (" in sql for sql in statements))

    async def test_standings_ingest_job_uses_transaction_and_repository(self) -> None:
        bundle = _build_bundle()
        parser = _FakeParser(bundle)
        repository_result = StandingsWriteResult(
            endpoint_registry_rows=2,
            payload_snapshot_rows=1,
            sport_rows=1,
            country_rows=1,
            category_rows=1,
            unique_tournament_rows=1,
            tournament_rows=1,
            team_rows=1,
            tie_breaking_rule_rows=1,
            promotion_rows=1,
            standing_rows=1,
            standing_row_rows=1,
        )
        repository = _FakeRepository(repository_result)
        connection = object()
        database = _FakeDatabase(connection)
        job = StandingsIngestJob(parser, repository, database)

        result = await job.run_for_unique_tournament(17, 76986, scopes=("total", "home"), timeout=12.5)

        self.assertEqual(parser.unique_tournament_calls, [(17, 76986, ("total", "home"), 12.5)])
        self.assertEqual(repository.calls, [(connection, bundle)])
        self.assertEqual(database.transaction_calls, 1)
        self.assertEqual(result.source_kind, "unique_tournament")
        self.assertEqual(result.source_id, 17)
        self.assertEqual(result.season_id, 76986)
        self.assertEqual(result.scopes, ("total", "home"))


if __name__ == "__main__":
    unittest.main()
