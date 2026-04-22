from __future__ import annotations

import unittest

from schema_inspector.competition_job import CompetitionIngestJob
from schema_inspector.competition_parser import (
    ApiPayloadSnapshotRecord,
    CategoryRecord,
    CompetitionBundle,
    CountryRecord,
    ImageAssetRecord,
    SeasonRecord,
    SportRecord,
    TeamRecord,
    UniqueTournamentMostTitleTeamRecord,
    UniqueTournamentRecord,
    UniqueTournamentRelationRecord,
    UniqueTournamentSeasonRecord,
)
from schema_inspector.competition_repository import CompetitionRepository, CompetitionWriteResult
from schema_inspector.db import load_database_config
from schema_inspector.endpoints import EndpointRegistryEntry


class _FakeExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"

    async def executemany(self, command: str, args):
        self.executemany_calls.append((command, list(args)))
        return None


class _FakeParser:
    def __init__(self, bundle: CompetitionBundle) -> None:
        self.bundle = bundle
        self.calls: list[tuple[int, int | None, bool, bool | None, float]] = []

    async def fetch_bundle(
        self,
        unique_tournament_id: int,
        *,
        season_id=None,
        include_seasons: bool = True,
        include_season_info=None,
        timeout=20.0,
    ):
        self.calls.append((unique_tournament_id, season_id, include_seasons, include_season_info, timeout))
        return self.bundle


class _FakeRepository:
    def __init__(self, result: CompetitionWriteResult) -> None:
        self.result = result
        self.calls: list[tuple[object, CompetitionBundle]] = []

    async def upsert_bundle(self, executor, bundle: CompetitionBundle) -> CompetitionWriteResult:
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


def _build_bundle() -> CompetitionBundle:
    return CompetitionBundle(
        registry_entries=(
            EndpointRegistryEntry(
                pattern="/api/v1/unique-tournament/{unique_tournament_id}",
                path_template="/api/v1/unique-tournament/{unique_tournament_id}",
                query_template=None,
                envelope_key="uniqueTournament",
                target_table="unique_tournament",
                notes=None,
            ),
        ),
        payload_snapshots=(
            ApiPayloadSnapshotRecord(
                endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}",
                source_url="https://www.sofascore.com/api/v1/unique-tournament/17",
                envelope_key="uniqueTournament",
                context_entity_type="unique_tournament",
                context_entity_id=17,
                payload={"id": 17, "slug": "premier-league"},
                fetched_at="2026-04-10T10:00:00+00:00",
            ),
        ),
        image_assets=(ImageAssetRecord(id=1418035, md5="logo-md5"),),
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
            TeamRecord(
                id=42,
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
                user_count=500000,
                field_translations={"nameTranslation": {"ru": "Ливерпуль"}},
                team_colors={"primary": "#dd0000", "secondary": "#ffffff", "text": "#ffffff"},
            ),
        ),
        unique_tournaments=(
            UniqueTournamentRecord(
                id=17,
                slug="premier-league",
                name="Premier League",
                category_id=1,
                country_alpha2="EN",
                logo_asset_id=1418035,
                dark_logo_asset_id=None,
                title_holder_team_id=42,
                title_holder_titles=20,
                most_titles=20,
                gender="M",
                primary_color_hex="#3c1c5a",
                secondary_color_hex="#f80158",
                start_date_timestamp=1755216000,
                end_date_timestamp=1779580800,
                tier=1,
                user_count=1179222,
                has_rounds=True,
                has_groups=False,
                has_performance_graph_feature=True,
                has_playoff_series=False,
                disabled_home_away_standings=False,
                display_inverse_home_away_teams=False,
                field_translations={"nameTranslation": {"ru": "Премьер-лига"}},
                period_length={"regular": 90},
            ),
        ),
        unique_tournament_relations=(
            UniqueTournamentRelationRecord(
                unique_tournament_id=17,
                related_unique_tournament_id=35,
                relation_type="lower_division",
            ),
        ),
        unique_tournament_most_title_teams=(
            UniqueTournamentMostTitleTeamRecord(unique_tournament_id=17, team_id=42),
        ),
        seasons=(SeasonRecord(id=76986, name="Premier League 25/26", year="25/26", editor=False),),
        unique_tournament_seasons=(UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=76986),),
    )


class CompetitionStorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_competition_repository_skips_redundant_topology_updates(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = CompetitionRepository()

        await repository.upsert_bundle(executor, bundle)
        await repository.upsert_bundle(executor, bundle)

        sport_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO sport" in sql]
        self.assertEqual(len(sport_statements), 1)

        category_sql = next(sql for sql, _ in executor.executemany_calls if "INSERT INTO category" in sql)
        unique_tournament_sql = next(
            sql for sql, _ in executor.executemany_calls if "INSERT INTO unique_tournament " in sql
        )
        season_sql = next(sql for sql, _ in executor.executemany_calls if "INSERT INTO season" in sql)
        self.assertIn("IS DISTINCT FROM", category_sql)
        self.assertIn("IS DISTINCT FROM", unique_tournament_sql)
        self.assertIn("IS DISTINCT FROM", season_sql)

    def test_load_database_config_uses_higher_pool_defaults(self) -> None:
        config = load_database_config(
            env={
                "SOFASCORE_DATABASE_URL": "postgresql://user:pass@localhost:5432/sofascore",
            }
        )

        self.assertEqual(config.min_size, 20)
        self.assertEqual(config.max_size, 50)

    def test_load_database_config_reads_env(self) -> None:
        config = load_database_config(
            env={
                "SOFASCORE_DATABASE_URL": "postgresql://user:pass@localhost:5432/sofascore",
                "SOFASCORE_PG_MIN_SIZE": "2",
                "SOFASCORE_PG_MAX_SIZE": "12",
                "SOFASCORE_PG_COMMAND_TIMEOUT": "30.5",
            }
        )

        self.assertEqual(config.dsn, "postgresql://user:pass@localhost:5432/sofascore")
        self.assertEqual(config.min_size, 2)
        self.assertEqual(config.max_size, 12)
        self.assertEqual(config.command_timeout, 30.5)

    async def test_competition_repository_writes_expected_tables(self) -> None:
        bundle = _build_bundle()
        executor = _FakeExecutor()
        repository = CompetitionRepository()

        result = await repository.upsert_bundle(executor, bundle)

        self.assertEqual(result.unique_tournament_rows, 1)
        self.assertEqual(result.payload_snapshot_rows, 1)
        statements = [sql for sql, _ in executor.executemany_calls]
        self.assertTrue(any("INSERT INTO endpoint_registry" in sql for sql in statements))
        endpoint_registry_rows = next(rows for sql, rows in executor.executemany_calls if "INSERT INTO endpoint_registry" in sql)
        self.assertEqual(endpoint_registry_rows[0][6], "sofascore")
        self.assertEqual(endpoint_registry_rows[0][7], "v1")
        self.assertTrue(any("INSERT INTO unique_tournament " in sql for sql in statements))
        self.assertTrue(any("INSERT INTO unique_tournament_most_title_team" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO api_payload_snapshot" in sql for sql in statements))

    async def test_competition_ingest_job_uses_transaction_and_repository(self) -> None:
        bundle = _build_bundle()
        parser = _FakeParser(bundle)
        repository_result = CompetitionWriteResult(
            endpoint_registry_rows=1,
            payload_snapshot_rows=1,
            image_asset_rows=1,
            sport_rows=1,
            country_rows=1,
            category_rows=1,
            team_rows=1,
            unique_tournament_rows=1,
            unique_tournament_relation_rows=1,
            unique_tournament_most_title_team_rows=1,
            season_rows=1,
            unique_tournament_season_rows=1,
        )
        repository = _FakeRepository(repository_result)
        connection = object()
        database = _FakeDatabase(connection)
        job = CompetitionIngestJob(parser, repository, database)

        result = await job.run(17, season_id=76986, timeout=12.5)

        self.assertEqual(parser.calls, [(17, 76986, True, True, 12.5)])
        self.assertEqual(repository.calls, [(connection, bundle)])
        self.assertEqual(database.transaction_calls, 1)
        self.assertEqual(result.written.unique_tournament_rows, 1)


if __name__ == "__main__":
    unittest.main()
