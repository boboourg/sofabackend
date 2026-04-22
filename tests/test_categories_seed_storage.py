from __future__ import annotations

import unittest
from datetime import date as Date

from schema_inspector.categories_seed_job import CategoriesSeedIngestJob
from schema_inspector.categories_seed_parser import (
    CategoriesSeedBundle,
    CategoryDailySummaryRecord,
    CategoryDailyTeamRecord,
    CategoryDailyUniqueTournamentRecord,
)
from schema_inspector.categories_seed_repository import CategoriesSeedRepository, CategoriesSeedWriteResult
from schema_inspector.competition_parser import ApiPayloadSnapshotRecord, CategoryRecord, CountryRecord, SportRecord
from schema_inspector.endpoints import categories_seed_registry_entries


class _FakeExecutor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    async def executemany(self, command: str, args):
        self.executemany_calls.append((command, list(args)))
        return None


class _FakeParser:
    def __init__(self, bundle: CategoriesSeedBundle) -> None:
        self.bundle = bundle
        self.calls: list[tuple[str, int, str, float]] = []

    async def fetch_daily_categories(
        self,
        observed_date: str,
        timezone_offset_seconds: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> CategoriesSeedBundle:
        self.calls.append((observed_date, timezone_offset_seconds, sport_slug, timeout))
        return self.bundle


class _FakeRepository:
    def __init__(self, result: CategoriesSeedWriteResult) -> None:
        self.result = result
        self.calls: list[tuple[object, CategoriesSeedBundle]] = []

    async def upsert_bundle(self, executor, bundle: CategoriesSeedBundle) -> CategoriesSeedWriteResult:
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


def _build_bundle() -> CategoriesSeedBundle:
    return CategoriesSeedBundle(
        registry_entries=categories_seed_registry_entries(),
        payload_snapshots=(
            ApiPayloadSnapshotRecord(
                endpoint_pattern="/api/v1/sport/football/{date}/{timezone_offset_seconds}/categories",
                source_url="https://www.sofascore.com/api/v1/sport/football/2026-04-11/10800/categories",
                envelope_key="categories",
                context_entity_type=None,
                context_entity_id=None,
                payload={"categories": [{"category": {"id": 1}}]},
                fetched_at="2026-04-11T10:55:07+00:00",
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
        daily_summaries=(
            CategoryDailySummaryRecord(
                observed_date=Date(2026, 4, 11),
                timezone_offset_seconds=10800,
                category_id=1,
                total_events=48,
                total_event_player_statistics=9,
                total_videos=0,
            ),
        ),
        daily_unique_tournaments=(
            CategoryDailyUniqueTournamentRecord(
                observed_date=Date(2026, 4, 11),
                timezone_offset_seconds=10800,
                category_id=1,
                unique_tournament_id=17,
                ordinal=0,
            ),
            CategoryDailyUniqueTournamentRecord(
                observed_date=Date(2026, 4, 11),
                timezone_offset_seconds=10800,
                category_id=1,
                unique_tournament_id=173,
                ordinal=1,
            ),
        ),
        daily_teams=(
            CategoryDailyTeamRecord(
                observed_date=Date(2026, 4, 11),
                timezone_offset_seconds=10800,
                category_id=1,
                team_id=43,
                ordinal=0,
            ),
            CategoryDailyTeamRecord(
                observed_date=Date(2026, 4, 11),
                timezone_offset_seconds=10800,
                category_id=1,
                team_id=44,
                ordinal=1,
            ),
        ),
    )


class CategoriesSeedStorageTests(unittest.IsolatedAsyncioTestCase):
    async def test_categories_seed_repository_skips_redundant_country_writes(self) -> None:
        repository = CategoriesSeedRepository()
        executor = _FakeExecutor()
        bundle = _build_bundle()

        await repository.upsert_bundle(executor, bundle)
        await repository.upsert_bundle(executor, bundle)

        country_statements = [sql for sql, _ in executor.executemany_calls if "INSERT INTO country" in sql]
        self.assertEqual(len(country_statements), 1)
        self.assertIn("IS DISTINCT FROM", country_statements[0])

    async def test_categories_seed_repository_upserts_expected_tables(self) -> None:
        repository = CategoriesSeedRepository()
        executor = _FakeExecutor()
        bundle = _build_bundle()

        result = await repository.upsert_bundle(executor, bundle)

        self.assertEqual(result.category_rows, 1)
        self.assertEqual(result.daily_summary_rows, 1)
        self.assertEqual(result.daily_unique_tournament_rows, 2)
        self.assertEqual(result.daily_team_rows, 2)
        self.assertEqual(len(executor.executemany_calls), 9)
        contract_sql, contract_rows = executor.executemany_calls[0]
        serving_sql, serving_rows = executor.executemany_calls[1]
        self.assertIn("INSERT INTO endpoint_contract_registry", contract_sql)
        self.assertIn("source_slug", contract_sql)
        self.assertIn("contract_version", contract_sql)
        self.assertEqual(contract_rows[0][6], "sofascore")
        self.assertEqual(contract_rows[0][7], "v1")
        self.assertIn("INSERT INTO endpoint_registry", serving_sql)
        self.assertEqual(serving_rows[0][6], "sofascore")
        self.assertEqual(serving_rows[0][7], "v1")
        self.assertIn("INSERT INTO category_daily_summary", executor.executemany_calls[5][0])
        self.assertIn("INSERT INTO category_daily_unique_tournament", executor.executemany_calls[6][0])
        self.assertIn("INSERT INTO category_daily_team", executor.executemany_calls[7][0])

    async def test_categories_seed_ingest_job_runs_transactionally(self) -> None:
        bundle = _build_bundle()
        repository_result = CategoriesSeedWriteResult(
            endpoint_registry_rows=1,
            payload_snapshot_rows=1,
            sport_rows=1,
            country_rows=1,
            category_rows=1,
            daily_summary_rows=1,
            daily_unique_tournament_rows=2,
            daily_team_rows=2,
        )
        parser = _FakeParser(bundle)
        repository = _FakeRepository(repository_result)
        connection = object()
        database = _FakeDatabase(connection)

        job = CategoriesSeedIngestJob(parser, repository, database)
        result = await job.run("2026-04-11", 10800, timeout=12.5)

        self.assertEqual(parser.calls, [("2026-04-11", 10800, "football", 12.5)])
        self.assertEqual(repository.calls, [(connection, bundle)])
        self.assertEqual(database.transaction_calls, 1)
        self.assertEqual(result.observed_date, "2026-04-11")
        self.assertEqual(result.timezone_offset_seconds, 10800)
        self.assertEqual(result.written.daily_unique_tournament_rows, 2)
