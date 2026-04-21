from __future__ import annotations

from dataclasses import dataclass
import unittest

from schema_inspector.category_tournaments_job import CategoryTournamentsIngestJob
from schema_inspector.category_tournaments_parser import CategoryTournamentsBundle
from schema_inspector.competition_parser import CompetitionBundle, UniqueTournamentRecord
from schema_inspector.competition_repository import CompetitionWriteResult


class CategoryTournamentsIngestJobTests(unittest.IsolatedAsyncioTestCase):
    async def test_category_unique_tournaments_upserts_registry_inside_transaction(self) -> None:
        parsed_bundle = CategoryTournamentsBundle(
            competition_bundle=CompetitionBundle(
                registry_entries=(),
                payload_snapshots=(),
                image_assets=(),
                sports=(),
                countries=(),
                categories=(),
                teams=(),
                unique_tournaments=(
                    UniqueTournamentRecord(id=2407, slug="barcelona", name="Barcelona", category_id=3),
                ),
                unique_tournament_relations=(),
                unique_tournament_most_title_teams=(),
                seasons=(),
                unique_tournament_seasons=(),
            ),
            category_ids=(3,),
            unique_tournament_ids=(2407,),
            active_unique_tournament_ids=(2407,),
            group_names=("ATP",),
        )
        parser = _FakeParser(parsed_bundle)
        repository = _FakeCompetitionRepository()
        database = _FakeDatabase()
        registry_repository = _FakeTournamentRegistryRepository()
        job = CategoryTournamentsIngestJob(
            parser,
            repository,
            database,
            tournament_registry_repository=registry_repository,
        )

        result = await job.run_category_unique_tournaments(3, sport_slug="tennis")

        self.assertEqual(result.parsed, parsed_bundle)
        self.assertEqual(repository.connections, [database.connection])
        self.assertEqual(
            [(record.source_slug, record.sport_slug, record.category_id, record.unique_tournament_id) for record in registry_repository.records],
            [("sofascore", "tennis", 3, 2407)],
        )
        self.assertEqual(registry_repository.connections, [database.connection])
        self.assertEqual(database.events, ["enter", "exit"])


class _FakeParser:
    def __init__(self, bundle: CategoryTournamentsBundle) -> None:
        self.bundle = bundle

    async def fetch_category_unique_tournaments(self, category_id: int, *, sport_slug: str, timeout: float = 20.0):
        del category_id, sport_slug, timeout
        return self.bundle


@dataclass(frozen=True)
class _Connection:
    name: str = "transaction-connection"


class _FakeTransaction:
    def __init__(self, database: "_FakeDatabase") -> None:
        self.database = database

    async def __aenter__(self):
        self.database.events.append("enter")
        return self.database.connection

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        self.database.events.append("exit")


class _FakeDatabase:
    def __init__(self) -> None:
        self.connection = _Connection()
        self.events: list[str] = []

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)


class _FakeCompetitionRepository:
    def __init__(self) -> None:
        self.connections: list[object] = []

    async def upsert_bundle(self, connection, bundle):
        del bundle
        self.connections.append(connection)
        return CompetitionWriteResult(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)


class _FakeTournamentRegistryRepository:
    def __init__(self) -> None:
        self.connections: list[object] = []
        self.records = []

    async def upsert_records(self, connection, records) -> None:
        self.connections.append(connection)
        self.records.extend(records)


if __name__ == "__main__":
    unittest.main()
