"""P3 audit-trail gap fix tests.

Verifies the failure-path commit behavior in ``HybridApp.run_event``:
when ``_prefetch_event_run`` raises (typically from a transport
timeout cascade), the wrap exception carries the prefetch
``FetchExecutor`` so the caller can persist its accumulated
``api_request_log`` records before re-raising the original
exception. Without this commit, forensics (proxy_address,
attempts_json, latency_ms, error_message per attempt) is lost.

Background: ``HybridApp.run_event`` uses a 3-stage pattern
(prefetch in deferred mode → commit → persist). In deferred mode,
``FetchExecutor`` buffers ``ApiRequestLogRecord``s in
``_prefetched_records`` but does NOT write them to DB until the
``commit_prefetched_record`` step. When the prefetch stage itself
raises (e.g. ``RetryableJobError`` from libcurl timeout chain), the
commit stage is skipped and the records die with garbage
collection. Concrete observed impact: event 14036755 had 5-6
transport timeouts per retry cycle for hours, etl_job_run recorded
the outer ``RetryableJobError`` message, but api_request_log had
ZERO rows for the endpoint pattern.
"""

from __future__ import annotations

import unittest
from typing import Any
from unittest import mock

from schema_inspector.cli import HybridApp, _PrefetchFailedError
from schema_inspector.fetch_executor import (
    ApiRequestLogRecord,
    PrefetchedFetchRecord,
    FetchOutcomeEnvelope,
)
from schema_inspector.runtime import RuntimeConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeTransaction:
    def __init__(self, connection: Any) -> None:
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        del exc_type, exc, tb


class _FakeDatabase:
    def __init__(self) -> None:
        self.transaction_calls = 0
        self.connection_obj = object()

    def transaction(self):
        self.transaction_calls += 1
        return _FakeTransaction(self.connection_obj)


class _RecordingRawRepository:
    """Records insert_request_log calls; raises on demand for failure tests."""

    def __init__(self, *, per_record_raises: list[bool] | None = None) -> None:
        self.calls: list[ApiRequestLogRecord] = []
        self._per_record_raises = list(per_record_raises or [])
        self._call_idx = 0

    async def insert_request_log(self, executor: Any, record: ApiRequestLogRecord) -> None:
        del executor
        # Check per-record failure script BEFORE recording, so failures
        # don't pollute the call log.
        if self._call_idx < len(self._per_record_raises) and self._per_record_raises[self._call_idx]:
            self._call_idx += 1
            raise RuntimeError("simulated per-record DB failure")
        self._call_idx += 1
        self.calls.append(record)


class _RaisingRawRepository:
    """Raises immediately on insert_request_log call (transaction-level
    failure simulation)."""

    async def insert_request_log(self, executor: Any, record: ApiRequestLogRecord) -> None:
        del executor, record
        raise RuntimeError("simulated transaction-level DB failure")


class _FakeFetchExecutor:
    """Minimal stub of FetchExecutor exposing only the
    ``prefetched_records`` property the partial-commit code needs."""

    def __init__(self, records: tuple[PrefetchedFetchRecord, ...]) -> None:
        self._records = records

    @property
    def prefetched_records(self) -> tuple[PrefetchedFetchRecord, ...]:
        return self._records


def _make_request_log_record(*, source_url: str = "https://www.sofascore.com/api/v1/event/14036755") -> ApiRequestLogRecord:
    return ApiRequestLogRecord(
        trace_id="pilot:football:event:14036755",
        job_id="hydrate_event_root:/api/v1/event/{event_id}:14036755",
        job_type="hydrate_event_root",
        sport_slug="football",
        method="GET",
        source_url=source_url,
        endpoint_pattern="/api/v1/event/{event_id}",
        request_headers_redacted={},
        query_params={},
        proxy_id="smartproxy-3",
        proxy_address="proxy.smartproxy.net:3120",
        transport_attempt=5,
        http_status=None,
        challenge_reason=None,
        started_at="2026-05-11T15:00:00+00:00",
        finished_at="2026-05-11T15:01:26+00:00",
        latency_ms=86000,
        attempts_json=None,
        payload_bytes=None,
        error_message="Failed to perform, ErrCode: 28, Reason: 'Operation timed out after 15500 milliseconds'",
    )


def _make_prefetched_record() -> PrefetchedFetchRecord:
    # Minimal record carrying only the request_log we want to persist.
    # FetchOutcomeEnvelope fields are used only for shape; not asserted.
    return PrefetchedFetchRecord(
        task=mock.Mock(),
        outcome=FetchOutcomeEnvelope(
            trace_id="pilot:football:event:14036755",
            job_id="hydrate_event_root:/api/v1/event/{event_id}:14036755",
            endpoint_pattern="/api/v1/event/{event_id}",
            source_url="https://www.sofascore.com/api/v1/event/14036755",
            resolved_url=None,
            http_status=None,
            classification="network_error",
            proxy_id="smartproxy-3",
            challenge_reason=None,
            snapshot_id=None,
            payload_hash=None,
            retry_recommended=True,
            capability_signal="transient",
            attempts=(),
            fetched_at=None,
            error_message="timeout",
        ),
        request_log=_make_request_log_record(),
        payload_snapshot=None,
        snapshot_head=None,
    )


def _make_hybrid_app() -> HybridApp:
    return HybridApp(
        database=_FakeDatabase(),
        runtime_config=RuntimeConfig(require_proxy=False),
        redis_backend=None,
    )


# ---------------------------------------------------------------------------
# 1. _commit_request_logs_only behavior
# ---------------------------------------------------------------------------
class CommitRequestLogsOnlyTests(unittest.IsolatedAsyncioTestCase):
    async def test_writes_all_records_when_repository_healthy(self) -> None:
        app = _make_hybrid_app()
        repo = _RecordingRawRepository()
        app.raw_repository = repo
        executor = _FakeFetchExecutor(
            records=(_make_prefetched_record(), _make_prefetched_record())
        )

        await app._commit_request_logs_only(executor)

        self.assertEqual(len(repo.calls), 2)
        # Both records have the same request_log shape — assert spot-fields
        self.assertEqual(repo.calls[0].endpoint_pattern, "/api/v1/event/{event_id}")
        self.assertEqual(repo.calls[0].http_status, None)
        self.assertEqual(repo.calls[0].proxy_address, "proxy.smartproxy.net:3120")
        self.assertEqual(app.database.transaction_calls, 1)

    async def test_empty_records_short_circuits_no_transaction(self) -> None:
        """If the executor accumulated zero records (e.g. all attempts
        died before transport.fetch was reached), don't open a
        useless transaction."""
        app = _make_hybrid_app()
        repo = _RecordingRawRepository()
        app.raw_repository = repo
        executor = _FakeFetchExecutor(records=())

        await app._commit_request_logs_only(executor)

        self.assertEqual(repo.calls, [])
        self.assertEqual(app.database.transaction_calls, 0)

    async def test_per_record_exception_does_not_abort_remaining_records(self) -> None:
        """Each insert is wrapped in its own try/except so a single
        bad row (e.g. duplicate-id from runtime retry race) does not
        prevent other records from landing. This is best-effort
        partial-commit semantics."""
        app = _make_hybrid_app()
        # Record sequence: first call raises, subsequent calls succeed.
        repo = _RecordingRawRepository(per_record_raises=[True, False, False])
        app.raw_repository = repo
        executor = _FakeFetchExecutor(
            records=(
                _make_prefetched_record(),
                _make_prefetched_record(),
                _make_prefetched_record(),
            )
        )

        # Must NOT raise — per-record failure is swallowed at DEBUG level.
        await app._commit_request_logs_only(executor)

        # 2 of 3 records persisted (first raised, the other two succeeded).
        self.assertEqual(len(repo.calls), 2)

    async def test_transaction_level_exception_does_not_propagate(self) -> None:
        """If the whole DB transaction fails (network blip, pool
        exhausted), log a WARNING and return cleanly — original
        prefetch exception MUST still be the one surfacing to the
        worker layer, not a secondary DB error."""
        app = _make_hybrid_app()
        app.raw_repository = _RaisingRawRepository()
        executor = _FakeFetchExecutor(records=(_make_prefetched_record(),))

        # Must NOT raise — defensive try/except around the whole
        # transaction. The audit trail is best-effort; the user-facing
        # contract is "original exception surfaces unchanged".
        await app._commit_request_logs_only(executor)


# ---------------------------------------------------------------------------
# 2. run_event integration: prefetch failure path
# ---------------------------------------------------------------------------
class RunEventPrefetchFailurePathTests(unittest.IsolatedAsyncioTestCase):
    async def test_prefetch_failed_persists_logs_and_reraises_original(self) -> None:
        """When _prefetch_event_run raises _PrefetchFailedError, the
        partial-commit method MUST be called with the executor's
        records, and the ORIGINAL exception MUST be re-raised (not
        the wrap class itself, not anything secondary)."""
        app = _make_hybrid_app()
        repo = _RecordingRawRepository()
        app.raw_repository = repo

        original_exc = RuntimeError("simulated transport timeout cascade")
        fake_executor = _FakeFetchExecutor(records=(_make_prefetched_record(),))

        async def _raise_prefetch_failed(**kwargs):
            del kwargs
            raise _PrefetchFailedError(executor=fake_executor, original=original_exc)

        async def _resolve_sport(event_id):
            del event_id
            return "football"

        async def _ensure_registry(sport_slug):
            del sport_slug

        with mock.patch.object(
            app, "_prefetch_event_run", side_effect=_raise_prefetch_failed
        ), mock.patch.object(
            app, "resolve_event_sport_slug", side_effect=_resolve_sport
        ), mock.patch.object(
            app, "ensure_endpoint_registry", side_effect=_ensure_registry
        ):
            with self.assertRaises(RuntimeError) as caught:
                await app.run_event(event_id=14036755, sport_slug=None, hydration_mode="full")
            # Must be the ORIGINAL exception, not the wrap.
            self.assertIs(caught.exception, original_exc)

        # Partial-commit happened: 1 request_log record was persisted.
        self.assertEqual(len(repo.calls), 1)
        self.assertEqual(repo.calls[0].trace_id, "pilot:football:event:14036755")
        self.assertEqual(repo.calls[0].error_message[:30], "Failed to perform, ErrCode: 28")

    async def test_prefetch_failed_with_empty_executor_records_reraises_cleanly(self) -> None:
        """Edge case: prefetch raises before any transport.fetch attempt
        was completed → executor has no records. Re-raise must still
        surface the original exception cleanly with no DB activity."""
        app = _make_hybrid_app()
        repo = _RecordingRawRepository()
        app.raw_repository = repo

        original_exc = ValueError("simulated config error before transport")
        empty_executor = _FakeFetchExecutor(records=())

        async def _raise_prefetch_failed(**kwargs):
            del kwargs
            raise _PrefetchFailedError(executor=empty_executor, original=original_exc)

        async def _resolve_sport(event_id):
            del event_id
            return "football"

        async def _ensure_registry(sport_slug):
            del sport_slug

        with mock.patch.object(
            app, "_prefetch_event_run", side_effect=_raise_prefetch_failed
        ), mock.patch.object(
            app, "resolve_event_sport_slug", side_effect=_resolve_sport
        ), mock.patch.object(
            app, "ensure_endpoint_registry", side_effect=_ensure_registry
        ):
            with self.assertRaises(ValueError) as caught:
                await app.run_event(event_id=14036755, sport_slug=None)
            self.assertIs(caught.exception, original_exc)

        self.assertEqual(repo.calls, [])
        # No transaction opened (early-return inside _commit_request_logs_only).
        self.assertEqual(app.database.transaction_calls, 0)

    async def test_prefetch_failed_partial_commit_db_failure_does_not_mask_original(self) -> None:
        """Worst-case: DB itself dies right as we try to commit the
        audit trail. The original transport exception MUST still
        surface — the user-facing contract is "you still see the
        real error, not a secondary DB hiccup"."""
        app = _make_hybrid_app()
        app.raw_repository = _RaisingRawRepository()

        original_exc = RuntimeError("original transport timeout")
        executor = _FakeFetchExecutor(records=(_make_prefetched_record(),))

        async def _raise_prefetch_failed(**kwargs):
            del kwargs
            raise _PrefetchFailedError(executor=executor, original=original_exc)

        async def _resolve_sport(event_id):
            del event_id
            return "football"

        async def _ensure_registry(sport_slug):
            del sport_slug

        with mock.patch.object(
            app, "_prefetch_event_run", side_effect=_raise_prefetch_failed
        ), mock.patch.object(
            app, "resolve_event_sport_slug", side_effect=_resolve_sport
        ), mock.patch.object(
            app, "ensure_endpoint_registry", side_effect=_ensure_registry
        ):
            with self.assertRaises(RuntimeError) as caught:
                await app.run_event(event_id=14036755, sport_slug=None)
            # MUST be the original transport error, NOT the DB simulation.
            self.assertIs(caught.exception, original_exc)


# ---------------------------------------------------------------------------
# 3. Regression: success path unchanged
# ---------------------------------------------------------------------------
class RunEventHappyPathRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_successful_prefetch_does_not_call_commit_request_logs_only(self) -> None:
        """Happy path: when _prefetch_event_run returns normally,
        the partial-commit path MUST NOT be touched — that branch
        is exception-only. This guards against accidentally routing
        successful prefetches through the audit-fix path."""
        app = _make_hybrid_app()

        # We need _prefetch_event_run to return successfully, then
        # _commit_prefetched_run to be called (real flow). Stub all
        # three downstream stages to verify the sequencing.
        prefetched_run_obj = mock.Mock(name="prefetched_run")
        committed_run_obj = mock.Mock(name="committed_run")
        final_result = mock.Mock(name="result")

        async def _ok_prefetch(**kwargs):
            del kwargs
            return prefetched_run_obj

        async def _ok_commit(prefetched_run):
            self.assertIs(prefetched_run, prefetched_run_obj)
            return committed_run_obj

        async def _ok_persist(committed_run, *, hydration_mode, scope=None):
            del hydration_mode, scope
            self.assertIs(committed_run, committed_run_obj)
            return final_result

        async def _resolve_sport(event_id):
            del event_id
            return "football"

        async def _ensure_registry(sport_slug):
            del sport_slug

        partial_commit_calls: list[Any] = []

        async def _record_partial_commit(executor):
            partial_commit_calls.append(executor)

        with mock.patch.object(
            app, "_prefetch_event_run", side_effect=_ok_prefetch
        ), mock.patch.object(
            app, "_commit_prefetched_run", side_effect=_ok_commit
        ), mock.patch.object(
            app, "_persist_prefetched_run", side_effect=_ok_persist
        ), mock.patch.object(
            app, "_warn_if_prefetched_run_large"
        ), mock.patch.object(
            app, "_commit_request_logs_only", side_effect=_record_partial_commit
        ), mock.patch.object(
            app, "resolve_event_sport_slug", side_effect=_resolve_sport
        ), mock.patch.object(
            app, "ensure_endpoint_registry", side_effect=_ensure_registry
        ):
            result = await app.run_event(event_id=14036755, sport_slug=None)

        self.assertIs(result, final_result)
        # Partial-commit must NOT be called on success.
        self.assertEqual(partial_commit_calls, [])


if __name__ == "__main__":
    unittest.main()
