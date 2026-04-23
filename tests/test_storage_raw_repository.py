from __future__ import annotations

from datetime import datetime
import unittest
from unittest.mock import patch

from schema_inspector.storage.raw_repository import (
    ApiRequestLogRecord,
    ApiSnapshotHeadRecord,
    PayloadSnapshotRecord,
    RawRepository,
    reset_all_registry_sync_caches,
)
from schema_inspector.endpoints import EndpointRegistryEntry


class _FakeExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"

    async def executemany(self, query: str, args: list[tuple[object, ...]]) -> str:
        self.executemany_calls.append((query, args))
        return "OK"


class _FakeReturningExecutor(_FakeExecutor):
    def __init__(self, return_value: int | None) -> None:
        super().__init__()
        self.return_value = return_value
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, query: str, *args: object) -> int | None:
        self.fetchval_calls.append((query, args))
        return self.return_value


class RawRepositoryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        reset_all_registry_sync_caches()

    async def test_upsert_endpoint_registry_entries_persists_source_contract_rows_for_all_sources(self) -> None:
        repository = RawRepository()
        executor = _FakeExecutor()

        await repository.upsert_endpoint_registry_entries(
            executor,
            [
                EndpointRegistryEntry(
                    pattern="/api/v1/event/{event_id}",
                    path_template="/api/v1/event/{event_id}",
                    query_template=None,
                    envelope_key="event",
                    target_table="event",
                    notes="primary route",
                    source_slug="sofascore",
                    contract_version="v1",
                ),
                EndpointRegistryEntry(
                    pattern="/api/v1/event/{event_id}",
                    path_template="/api/v1/event/{event_id}",
                    query_template=None,
                    envelope_key="event",
                    target_table="event",
                    notes="detail route",
                    source_slug="secondary-source",
                    contract_version="v2",
                )
            ],
        )

        self.assertEqual(len(executor.executemany_calls), 2)
        query, rows = executor.executemany_calls[0]
        self.assertIn("INSERT INTO endpoint_contract_registry", query)
        self.assertIn("ON CONFLICT (pattern, source_slug)", query)
        self.assertIn("IS DISTINCT FROM", query)
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {(row[6], row[7]) for row in rows},
            {("sofascore", "v1"), ("secondary-source", "v2")},
        )

    async def test_upsert_endpoint_registry_entries_only_updates_serving_registry_for_primary_source(self) -> None:
        repository = RawRepository()
        executor = _FakeExecutor()

        await repository.upsert_endpoint_registry_entries(
            executor,
            [
                EndpointRegistryEntry(
                    pattern="/api/v1/event/{event_id}",
                    path_template="/api/v1/event/{event_id}",
                    query_template=None,
                    envelope_key="event",
                    target_table="event",
                    notes="detail route",
                    source_slug="secondary-source",
                    contract_version="v2",
                )
            ],
        )

        self.assertEqual(len(executor.executemany_calls), 1)
        query, rows = executor.executemany_calls[0]
        self.assertIn("INSERT INTO endpoint_contract_registry", query)
        self.assertIn("source_slug", query)
        self.assertIn("contract_version", query)
        self.assertIn("IS DISTINCT FROM", query)
        self.assertEqual(len(rows[0]), 8)
        self.assertEqual(rows[0][6], "secondary-source")
        self.assertEqual(rows[0][7], "v2")

    async def test_upsert_endpoint_registry_entries_skips_repeated_sync_for_same_repository_instance(self) -> None:
        repository = RawRepository()
        executor = _FakeExecutor()
        executor._enable_registry_sync_cache = True
        entries = [
            EndpointRegistryEntry(
                pattern="/api/v1/event/{event_id}",
                path_template="/api/v1/event/{event_id}",
                query_template=None,
                envelope_key="event",
                target_table="event",
                notes="primary route",
                source_slug="sofascore",
                contract_version="v1",
            )
        ]

        await repository.upsert_endpoint_registry_entries(executor, entries)
        await repository.upsert_endpoint_registry_entries(executor, entries)

        self.assertEqual(len(executor.executemany_calls), 2)

    async def test_upsert_endpoint_registry_entries_skips_repeated_sync_across_repository_instances(self) -> None:
        first_repository = RawRepository()
        second_repository = RawRepository()
        executor = _FakeExecutor()
        executor._enable_registry_sync_cache = True
        entries = [
            EndpointRegistryEntry(
                pattern="/api/v1/event/{event_id}",
                path_template="/api/v1/event/{event_id}",
                query_template=None,
                envelope_key="event",
                target_table="event",
                notes="primary route",
                source_slug="sofascore",
                contract_version="v1",
            )
        ]

        await first_repository.upsert_endpoint_registry_entries(executor, entries)
        await second_repository.upsert_endpoint_registry_entries(executor, entries)

        self.assertEqual(len(executor.executemany_calls), 2)

    async def test_upsert_endpoint_registry_entries_skips_overlapping_subset_after_rows_are_cached(self) -> None:
        first_repository = RawRepository()
        second_repository = RawRepository()
        executor = _FakeExecutor()
        executor._enable_registry_sync_cache = True
        primary_entry = EndpointRegistryEntry(
            pattern="/api/v1/event/{event_id}",
            path_template="/api/v1/event/{event_id}",
            query_template=None,
            envelope_key="event",
            target_table="event",
            notes="primary route",
            source_slug="sofascore",
            contract_version="v1",
        )
        secondary_entry = EndpointRegistryEntry(
            pattern="/api/v1/event/{event_id}/statistics",
            path_template="/api/v1/event/{event_id}/statistics",
            query_template=None,
            envelope_key="statistics",
            target_table="event_statistic",
            notes="statistics route",
            source_slug="sofascore",
            contract_version="v1",
        )

        await first_repository.upsert_endpoint_registry_entries(executor, [primary_entry, secondary_entry])
        await second_repository.upsert_endpoint_registry_entries(executor, [primary_entry])

        self.assertEqual(len(executor.executemany_calls), 2)

    async def test_upsert_endpoint_registry_entries_keeps_secondary_source_out_of_serving_registry(self) -> None:
        repository = RawRepository()
        executor = _FakeExecutor()

        await repository.upsert_endpoint_registry_entries(
            executor,
            [
                EndpointRegistryEntry(
                    pattern="/api/v1/event/{event_id}",
                    path_template="/api/v1/event/{event_id}",
                    query_template=None,
                    envelope_key="event",
                    target_table="event",
                    notes="primary route",
                    source_slug="sofascore",
                    contract_version="v1",
                ),
                EndpointRegistryEntry(
                    pattern="/api/v1/event/{event_id}",
                    path_template="/api/v1/event/{event_id}",
                    query_template=None,
                    envelope_key="event",
                    target_table="event",
                    notes="secondary route",
                    source_slug="secondary-source",
                    contract_version="v2",
                ),
            ],
        )

        serving_query, serving_rows = executor.executemany_calls[1]
        self.assertIn("INSERT INTO endpoint_registry", serving_query)
        self.assertIn("IS DISTINCT FROM", serving_query)
        self.assertEqual(len(serving_rows), 1)
        self.assertEqual(serving_rows[0][6], "sofascore")
        self.assertEqual(serving_rows[0][7], "v1")

    async def test_upsert_endpoint_registry_entries_only_caches_after_post_commit_hook(self) -> None:
        repository = RawRepository()
        executor = _FakeExecutor()
        executor._enable_registry_sync_cache = True
        entries = [
            EndpointRegistryEntry(
                pattern="/api/v1/event/{event_id}",
                path_template="/api/v1/event/{event_id}",
                query_template=None,
                envelope_key="event",
                target_table="event",
                notes="primary route",
                source_slug="sofascore",
                contract_version="v1",
            )
        ]
        registered_hooks: list[object] = []

        def _capture_post_commit_hook(callback):
            registered_hooks.append(callback)
            return True

        with patch(
            "schema_inspector.storage.raw_repository.register_post_commit_hook",
            side_effect=_capture_post_commit_hook,
        ):
            await repository.upsert_endpoint_registry_entries(executor, entries)
            await repository.upsert_endpoint_registry_entries(executor, entries)

        self.assertEqual(len(executor.executemany_calls), 4)
        self.assertEqual(len(registered_hooks), 2)

        registered_hooks[-1]()
        await repository.upsert_endpoint_registry_entries(executor, entries)

        self.assertEqual(len(executor.executemany_calls), 4)

    async def test_upsert_endpoint_registry_entries_deduplicates_duplicate_rows_before_write(self) -> None:
        repository = RawRepository()
        executor = _FakeExecutor()

        duplicate_entry = EndpointRegistryEntry(
            pattern="/api/v1/event/{event_id}",
            path_template="/api/v1/event/{event_id}",
            query_template=None,
            envelope_key="event",
            target_table="event",
            notes="primary route",
            source_slug="sofascore",
            contract_version="v1",
        )

        await repository.upsert_endpoint_registry_entries(executor, [duplicate_entry, duplicate_entry])

        contract_query, contract_rows = executor.executemany_calls[0]
        serving_query, serving_rows = executor.executemany_calls[1]
        self.assertEqual(len(contract_rows), 1)
        self.assertEqual(len(serving_rows), 1)

    async def test_repository_writes_request_snapshot_and_head(self) -> None:
        repository = RawRepository()
        executor = _FakeExecutor()

        await repository.insert_request_log(
            executor,
            ApiRequestLogRecord(
                trace_id="trace-1",
                job_id="job-1",
                job_type="hydrate_event_root",
                sport_slug="football",
                method="GET",
                source_url="https://www.sofascore.com/api/v1/event/1",
                endpoint_pattern="/api/v1/event/{event_id}",
                request_headers_redacted={"accept": "application/json"},
                query_params={},
                proxy_id="proxy_1",
                transport_attempt=1,
                http_status=200,
                challenge_reason=None,
                started_at="2026-04-16T09:00:00+00:00",
                finished_at="2026-04-16T09:00:01+00:00",
                latency_ms=123,
                attempts_json=[{"attempt_number": 1, "proxy_name": "proxy_1", "status_code": 200}],
                payload_bytes=18,
                error_message=None,
            ),
        )
        await repository.insert_payload_snapshot(
            executor,
            PayloadSnapshotRecord(
                trace_id="trace-1",
                job_id="job-1",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/1",
                resolved_url="https://www.sofascore.com/api/v1/event/1",
                envelope_key="event",
                context_entity_type="event",
                context_entity_id=1,
                context_unique_tournament_id=17,
                context_season_id=76986,
                context_event_id=1,
                http_status=200,
                payload={"event": {"id": 1}},
                payload_hash="hash-1",
                payload_size_bytes=18,
                content_type="application/json",
                is_valid_json=True,
                is_soft_error_payload=False,
                fetched_at="2026-04-16T09:00:01+00:00",
            ),
        )
        await repository.upsert_snapshot_head(
            executor,
            ApiSnapshotHeadRecord(
                endpoint_pattern="/api/v1/event/{event_id}",
                context_entity_type="event",
                context_entity_id=1,
                scope_key="event:1",
                latest_snapshot_id=99,
                latest_payload_hash="hash-1",
                latest_fetched_at="2026-04-16T09:00:01+00:00",
            ),
        )

        statements = [sql for sql, _ in executor.execute_calls]
        self.assertTrue(any("INSERT INTO api_request_log" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO api_payload_snapshot" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO api_snapshot_head" in sql for sql in statements))
        request_args = executor.execute_calls[0][1]
        snapshot_args = executor.execute_calls[1][1]
        head_args = executor.execute_calls[2][1]
        self.assertIsInstance(request_args[13], datetime)
        self.assertIsInstance(request_args[14], datetime)
        self.assertEqual(request_args[16], '[{"attempt_number":1,"proxy_name":"proxy_1","status_code":200}]')
        self.assertEqual(request_args[17], 18)
        self.assertIsInstance(snapshot_args[10], datetime)
        self.assertIsInstance(head_args[6], datetime)

    async def test_insert_payload_snapshot_binds_all_source_metadata_columns(self) -> None:
        repository = RawRepository()
        executor = _FakeExecutor()

        await repository.insert_payload_snapshot(
            executor,
            PayloadSnapshotRecord(
                trace_id="trace-2",
                job_id="job-2",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://provider.test/api/v1/event/1",
                resolved_url="https://provider.test/api/v1/event/1",
                envelope_key="event",
                context_entity_type="event",
                context_entity_id=1,
                context_unique_tournament_id=17,
                context_season_id=76986,
                context_event_id=1,
                http_status=200,
                payload={"event": {"id": 1}},
                payload_hash="hash-2",
                payload_size_bytes=18,
                content_type="application/json",
                is_valid_json=True,
                is_soft_error_payload=False,
                fetched_at="2026-04-16T09:00:01+00:00",
                source_slug="secondary-source",
                schema_fingerprint="schema-1",
                scope_hash="scope-1",
            ),
        )

        self.assertEqual(len(executor.execute_calls), 1)
        query, args = executor.execute_calls[0]
        self.assertIn("source_slug", query)
        self.assertIn("schema_fingerprint", query)
        self.assertIn("scope_hash", query)
        self.assertIn("$23", query)
        self.assertEqual(len(args), 23)
        self.assertEqual(args[20], "secondary-source")
        self.assertEqual(args[21], "schema-1")
        self.assertEqual(args[22], "scope-1")

    async def test_repository_inserts_payload_snapshot_idempotently_by_scope_and_hash(self) -> None:
        repository = RawRepository()
        executor = _FakeReturningExecutor(return_value=42)

        snapshot_id = await repository.insert_payload_snapshot_if_missing_returning_id(
            executor,
            PayloadSnapshotRecord(
                trace_id="trace-1",
                job_id="job-1",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/1",
                resolved_url="https://www.sofascore.com/api/v1/event/1",
                envelope_key="event",
                context_entity_type="event",
                context_entity_id=1,
                context_unique_tournament_id=17,
                context_season_id=76986,
                context_event_id=1,
                http_status=200,
                payload={"event": {"id": 1}},
                payload_hash="hash-1",
                payload_size_bytes=18,
                content_type="application/json",
                is_valid_json=True,
                is_soft_error_payload=False,
                fetched_at="2026-04-16T09:00:01+00:00",
            ),
        )

        self.assertEqual(snapshot_id, 42)
        self.assertEqual(len(executor.fetchval_calls), 1)
        query, args = executor.fetchval_calls[0]
        self.assertIn("INSERT INTO api_payload_snapshot", query)
        self.assertIn("ON CONFLICT (scope_key, payload_hash)", query)
        self.assertIn("WHERE scope_key IS NOT NULL AND payload_hash IS NOT NULL", query)
        self.assertIn("RETURNING id", query)
        self.assertNotIn("IS NOT DISTINCT FROM", query)
        # scope_key is the positional parameter before source metadata; payload_hash is $16.
        self.assertEqual(args[15], "hash-1")
        self.assertEqual(args[20], "sofascore:event:1:/api/v1/event/{event_id}")

    async def test_payload_snapshot_dedup_scope_includes_source_slug(self) -> None:
        repository = RawRepository()
        executor = _FakeReturningExecutor(return_value=42)
        base_record = PayloadSnapshotRecord(
            trace_id="trace-1",
            job_id="job-1",
            sport_slug="football",
            endpoint_pattern="/api/v1/event/{event_id}",
            source_url="https://www.sofascore.com/api/v1/event/1",
            resolved_url="https://www.sofascore.com/api/v1/event/1",
            envelope_key="event",
            context_entity_type="event",
            context_entity_id=1,
            context_unique_tournament_id=17,
            context_season_id=76986,
            context_event_id=1,
            http_status=200,
            payload={"event": {"id": 1}},
            payload_hash="hash-1",
            payload_size_bytes=18,
            content_type="application/json",
            is_valid_json=True,
            is_soft_error_payload=False,
            fetched_at="2026-04-16T09:00:01+00:00",
        )

        await repository.insert_payload_snapshot_if_missing_returning_id(executor, base_record)
        await repository.insert_payload_snapshot_if_missing_returning_id(
            executor,
            PayloadSnapshotRecord(**{**base_record.__dict__, "source_slug": "secondary-source"}),
        )

        first_args = executor.fetchval_calls[0][1]
        second_args = executor.fetchval_calls[1][1]
        self.assertNotEqual(first_args[20], second_args[20])
        self.assertTrue(str(first_args[20]).startswith("sofascore:"))
        self.assertTrue(str(second_args[20]).startswith("secondary-source:"))


if __name__ == "__main__":
    unittest.main()
