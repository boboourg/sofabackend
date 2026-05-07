"""D6 worker-side tests: empty-payload predicate marks EmptyDataStore."""

from __future__ import annotations

import asyncio
import unittest
from dataclasses import dataclass
from typing import Any

from schema_inspector.endpoints import (
    PLAYER_LAST_YEAR_SUMMARY_ENDPOINT,
    PLAYER_NATIONAL_TEAM_STATISTICS_ENDPOINT,
    SofascoreEndpoint,
)
from schema_inspector.workers.resource_refresh_worker import (
    DEFAULT_EMPTY_PREDICATES,
    ResourceRefreshWorker,
    _is_empty_last_year_summary,
    _is_empty_national_team_statistics,
)


class PredicateUnitTests(unittest.TestCase):
    def test_last_year_summary_empty(self) -> None:
        self.assertTrue(
            _is_empty_last_year_summary({"summary": [], "uniqueTournamentsMap": {}})
        )

    def test_last_year_summary_with_data(self) -> None:
        self.assertFalse(
            _is_empty_last_year_summary(
                {"summary": [{"x": 1}], "uniqueTournamentsMap": {"a": 1}}
            )
        )
        # Either side filled = not empty.
        self.assertFalse(
            _is_empty_last_year_summary({"summary": [{"x": 1}], "uniqueTournamentsMap": {}})
        )
        self.assertFalse(
            _is_empty_last_year_summary({"summary": [], "uniqueTournamentsMap": {"a": 1}})
        )

    def test_last_year_summary_non_dict_returns_false(self) -> None:
        self.assertFalse(_is_empty_last_year_summary(None))
        self.assertFalse(_is_empty_last_year_summary([]))
        self.assertFalse(_is_empty_last_year_summary("oops"))

    def test_national_team_statistics_empty(self) -> None:
        self.assertTrue(_is_empty_national_team_statistics({"statistics": []}))

    def test_national_team_statistics_with_data(self) -> None:
        self.assertFalse(_is_empty_national_team_statistics({"statistics": [{"x": 1}]}))

    def test_default_predicate_registry_has_both(self) -> None:
        self.assertIn("last_year_summary", DEFAULT_EMPTY_PREDICATES)
        self.assertIn("national_team_statistics", DEFAULT_EMPTY_PREDICATES)
        # The endpoint's empty_predicate must point at a known key.
        self.assertEqual(
            PLAYER_LAST_YEAR_SUMMARY_ENDPOINT.empty_predicate, "last_year_summary"
        )
        self.assertEqual(
            PLAYER_NATIONAL_TEAM_STATISTICS_ENDPOINT.empty_predicate,
            "national_team_statistics",
        )


@dataclass
class _Outcome:
    http_status: int = 200
    snapshot_id: int | None = 7
    classification: str = ""
    is_soft_error_payload: bool = False


class _RecordingEmptyStore:
    def __init__(self):
        self.marked: list[tuple[str, int, int | None]] = []

    def mark_empty(self, *, endpoint_pattern: str, entity_id: int, when_ms: int | None = None) -> None:
        self.marked.append((endpoint_pattern, int(entity_id), when_ms))

    def is_empty_recently(self, **kwargs) -> bool:  # pragma: no cover - unused
        return False

    def clear(self, **kwargs) -> None:  # pragma: no cover - unused
        return None


class _StubFetchExecutor:
    def __init__(self, outcome: _Outcome):
        self._outcome = outcome
        self.calls: list[Any] = []

    async def execute(self, task):
        self.calls.append(task)
        return self._outcome


class _StubQueue:
    def publish(self, *a, **k):  # pragma: no cover - unused in these tests
        return None


def _make_worker(
    *,
    empty_store: _RecordingEmptyStore,
    payload: Any,
    endpoints: tuple[SofascoreEndpoint, ...],
    outcome: _Outcome,
) -> ResourceRefreshWorker:
    async def _reader(snapshot_id: int):
        return payload

    return ResourceRefreshWorker(
        fetch_executor=_StubFetchExecutor(outcome),
        delayed_scheduler=None,
        queue=_StubQueue(),
        consumer="test",
        endpoints=endpoints,
        empty_data_store=empty_store,
        snapshot_reader=_reader,
    )


def _make_entry(*, pattern: str, entity_id: int):
    """Mimic StreamEntry the worker's ``handle`` decodes.

    Tests for the existing pagination flow use the same shape; here we
    short-circuit ``decode_stream_job`` by injecting a JobEnvelope-like
    object via a tiny adapter.
    """

    from schema_inspector.jobs.envelope import JobEnvelope
    from schema_inspector.jobs.types import JOB_REFRESH_RESOURCE
    from schema_inspector.queue.streams import StreamEntry
    from schema_inspector.workers._stream_jobs import encode_stream_job

    job = JobEnvelope.create(
        job_type=JOB_REFRESH_RESOURCE,
        sport_slug="football",
        entity_type="player",
        entity_id=int(entity_id),
        scope="resource_refresh",
        params={
            "endpoint_pattern": pattern,
            "path_params": {"player_id": int(entity_id)},
        },
        priority=50,
        trace_id=None,
        capability_hint="resource_refresh",
    )
    fields = encode_stream_job(job)
    return StreamEntry(stream="stream:etl:resource_refresh", message_id="0-0", values=fields)


class WorkerEmptyDataIntegrationTests(unittest.TestCase):
    def test_empty_payload_marks_store_for_last_year_summary(self) -> None:
        empty_store = _RecordingEmptyStore()
        worker = _make_worker(
            empty_store=empty_store,
            payload={"summary": [], "uniqueTournamentsMap": {}},
            endpoints=(PLAYER_LAST_YEAR_SUMMARY_ENDPOINT,),
            outcome=_Outcome(http_status=200, snapshot_id=42),
        )
        entry = _make_entry(
            pattern=PLAYER_LAST_YEAR_SUMMARY_ENDPOINT.pattern, entity_id=4747
        )
        result = asyncio.run(worker.handle(entry))
        self.assertEqual(result, "completed")
        self.assertEqual(len(empty_store.marked), 1)
        marked_pattern, marked_id, _ = empty_store.marked[0]
        self.assertEqual(marked_pattern, PLAYER_LAST_YEAR_SUMMARY_ENDPOINT.pattern)
        self.assertEqual(marked_id, 4747)

    def test_non_empty_payload_does_not_mark(self) -> None:
        empty_store = _RecordingEmptyStore()
        worker = _make_worker(
            empty_store=empty_store,
            payload={"summary": [{"x": 1}], "uniqueTournamentsMap": {"a": 1}},
            endpoints=(PLAYER_LAST_YEAR_SUMMARY_ENDPOINT,),
            outcome=_Outcome(http_status=200, snapshot_id=42),
        )
        entry = _make_entry(
            pattern=PLAYER_LAST_YEAR_SUMMARY_ENDPOINT.pattern, entity_id=750
        )
        asyncio.run(worker.handle(entry))
        self.assertEqual(empty_store.marked, [])

    def test_endpoint_without_predicate_does_not_mark(self) -> None:
        empty_store = _RecordingEmptyStore()
        # Construct a pattern WITHOUT empty_predicate.
        endpoint_no_pred = SofascoreEndpoint(
            path_template="/api/v1/player/{player_id}/statistics",
            envelope_key="seasons,typesMap",
            target_table="api_payload_snapshot",
            refresh_interval_seconds=24 * 3600,
            scope_kind="player-of-active-squad",
        )
        worker = _make_worker(
            empty_store=empty_store,
            payload={"summary": [], "uniqueTournamentsMap": {}},
            endpoints=(endpoint_no_pred,),
            outcome=_Outcome(http_status=200, snapshot_id=42),
        )
        entry = _make_entry(pattern=endpoint_no_pred.pattern, entity_id=12)
        asyncio.run(worker.handle(entry))
        self.assertEqual(empty_store.marked, [])


if __name__ == "__main__":
    unittest.main()
