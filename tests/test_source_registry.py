import unittest

from schema_inspector.storage.source_registry_repository import (
    SourceRegistryRecord,
    SourceRegistryRepository,
)


class _FakeExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"


class SourceRegistryTests(unittest.IsolatedAsyncioTestCase):
    def test_source_registry_record_captures_provider_contract(self) -> None:
        record = SourceRegistryRecord(
            source_slug="sofascore",
            display_name="Sofascore",
            transport_kind="http_json",
            trust_rank=100,
            is_active=True,
        )
        self.assertEqual(record.source_slug, "sofascore")
        self.assertEqual(record.transport_kind, "http_json")
        self.assertTrue(record.is_active)

    async def test_upsert_source_persists_registry_row(self) -> None:
        repository = SourceRegistryRepository()
        executor = _FakeExecutor()
        record = SourceRegistryRecord(
            source_slug="sofascore",
            display_name="Sofascore",
            transport_kind="http_json",
        )

        await repository.upsert_source(executor, record)

        self.assertEqual(len(executor.execute_calls), 1)
        query, args = executor.execute_calls[0]
        self.assertIn("INSERT INTO provider_source", query)
        self.assertIn("ON CONFLICT (source_slug) DO UPDATE SET", query)
        self.assertEqual(args, ("sofascore", "Sofascore", "http_json", 100, True))

    async def test_upsert_source_preserves_non_default_priority_and_inactive_flag(self) -> None:
        repository = SourceRegistryRepository()
        executor = _FakeExecutor()
        record = SourceRegistryRecord(
            source_slug="secondary-source",
            display_name="Secondary Source",
            transport_kind="http_json",
            trust_rank=80,
            is_active=False,
        )

        await repository.upsert_source(executor, record)

        _, args = executor.execute_calls[0]
        self.assertEqual(args, ("secondary-source", "Secondary Source", "http_json", 80, False))


if __name__ == "__main__":
    unittest.main()
