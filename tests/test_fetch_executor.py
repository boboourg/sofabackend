from __future__ import annotations

import unittest

from schema_inspector.fetch_executor import FetchExecutor
from schema_inspector.fetch_models import FetchOutcomeEnvelope, FetchTask
from schema_inspector.runtime import TransportAttempt, TransportResult


class _FakeTransport:
    def __init__(self, result: TransportResult) -> None:
        self.result = result
        self.calls: list[tuple[str, dict[str, str] | None, float]] = []

    async def fetch(self, url: str, *, headers=None, timeout: float = 20.0):
        self.calls.append((url, headers, timeout))
        return self.result


class _FakeRawRepository:
    def __init__(self) -> None:
        self.request_logs = []
        self.snapshots = []
        self.snapshot_heads = []
        self.idempotent_snapshot_ids: list[int] = []

    async def insert_request_log(self, executor, record) -> None:
        del executor
        self.request_logs.append(record)

    async def insert_payload_snapshot_returning_id(self, executor, record) -> int:
        del executor
        self.snapshots.append(record)
        return 101

    async def insert_payload_snapshot_if_missing_returning_id(self, executor, record) -> int:
        del executor
        self.snapshots.append(record)
        snapshot_id = self.idempotent_snapshot_ids.pop(0) if self.idempotent_snapshot_ids else 101
        return snapshot_id

    async def upsert_snapshot_head(self, executor, record) -> None:
        del executor
        self.snapshot_heads.append(record)


class _FakeSnapshotStore:
    def __init__(self) -> None:
        self.records = []
        self.next_snapshot_id = -1

    def stage_snapshot(self, record) -> int:
        self.records.append(record)
        snapshot_id = self.next_snapshot_id
        self.next_snapshot_id -= 1
        return snapshot_id


class _FakeFreshnessStore:
    def __init__(self) -> None:
        self.marked: list[tuple[str, int]] = []

    def mark_fetched(self, key: str, ttl_seconds: int) -> None:
        self.marked.append((key, ttl_seconds))


class FetchExecutorTests(unittest.IsolatedAsyncioTestCase):
    async def test_executor_fetches_classifies_and_writes_snapshot(self) -> None:
        transport = _FakeTransport(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/1",
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"event":{"id":1,"slug":"match"}}',
                attempts=(TransportAttempt(1, "proxy_1", 200, None, None, "10.10.10.10:12345"),),
                final_proxy_name="proxy_1",
                final_proxy_address="10.10.10.10:12345",
                challenge_reason=None,
            )
        )
        raw_repository = _FakeRawRepository()
        executor = FetchExecutor(transport=transport, raw_repository=raw_repository, sql_executor=object())

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-1",
                job_id="job-1",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/1",
                timeout_profile="standard_json",
                timeout_seconds=12.5,
                context_entity_type="event",
                context_entity_id=1,
                context_unique_tournament_id=17,
                context_season_id=76986,
                context_event_id=1,
                expected_content_type="application/json",
                fetch_reason="hydrate_event_root",
            )
        )

        self.assertEqual(outcome.classification, "success_json")
        self.assertEqual(outcome.snapshot_id, 101)
        self.assertEqual(outcome.payload_root_keys, ("event",))
        self.assertEqual(len(raw_repository.request_logs), 1)
        self.assertEqual(len(raw_repository.snapshots), 1)
        self.assertEqual(len(raw_repository.snapshot_heads), 1)
        self.assertEqual(transport.calls[0][2], 12.5)
        self.assertEqual(raw_repository.request_logs[0].payload_bytes, len(b'{"event":{"id":1,"slug":"match"}}'))
        self.assertEqual(raw_repository.request_logs[0].proxy_address, "10.10.10.10:12345")
        self.assertEqual(raw_repository.request_logs[0].attempts_json[0]["proxy_name"], "proxy_1")
        self.assertEqual(raw_repository.request_logs[0].attempts_json[0]["proxy_address"], "10.10.10.10:12345")
        self.assertIsNone(raw_repository.request_logs[0].error_message)

    async def test_executor_can_defer_raw_writes_and_collect_prefetched_record(self) -> None:
        transport = _FakeTransport(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/5",
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"event":{"id":5,"slug":"prefetched"}}',
                attempts=(TransportAttempt(1, "proxy_5", 200, None, None),),
                final_proxy_name="proxy_5",
                challenge_reason=None,
            )
        )
        raw_repository = _FakeRawRepository()
        snapshot_store = _FakeSnapshotStore()
        executor = FetchExecutor(
            transport=transport,
            raw_repository=raw_repository,
            sql_executor=None,
            snapshot_store=snapshot_store,
            write_mode="deferred",
        )

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-prefetch",
                job_id="job-prefetch",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/5",
                timeout_profile="pilot",
                context_entity_type="event",
                context_entity_id=5,
                context_event_id=5,
                fetch_reason="hydrate_event_root",
            )
        )

        self.assertEqual(outcome.snapshot_id, -1)
        self.assertEqual(len(raw_repository.request_logs), 0)
        self.assertEqual(len(raw_repository.snapshots), 0)
        self.assertEqual(len(snapshot_store.records), 1)
        self.assertEqual(len(executor.prefetched_records), 1)

    async def test_commit_prefetched_record_persists_append_only_log_and_idempotent_snapshot(self) -> None:
        transport = _FakeTransport(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/9/statistics",
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"statistics":[]}',
                attempts=(TransportAttempt(1, "proxy_9", 200, None, None),),
                final_proxy_name="proxy_9",
                challenge_reason=None,
            )
        )
        raw_repository = _FakeRawRepository()
        raw_repository.idempotent_snapshot_ids.append(404)
        deferred_executor = FetchExecutor(
            transport=transport,
            raw_repository=raw_repository,
            sql_executor=None,
            snapshot_store=_FakeSnapshotStore(),
            write_mode="deferred",
        )
        await deferred_executor.execute(
            FetchTask(
                trace_id="trace-commit",
                job_id="job-commit",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}/statistics",
                source_url="https://www.sofascore.com/api/v1/event/9/statistics",
                timeout_profile="pilot",
                context_entity_type="event",
                context_entity_id=9,
                context_event_id=9,
                fetch_reason="hydrate_event_edge",
            )
        )

        committing_executor = FetchExecutor(
            transport=transport,
            raw_repository=raw_repository,
            sql_executor=object(),
        )

        committed = await committing_executor.commit_prefetched_record(deferred_executor.prefetched_records[0])

        self.assertIsInstance(committed, FetchOutcomeEnvelope)
        self.assertEqual(committed.snapshot_id, 404)
        self.assertEqual(len(raw_repository.request_logs), 1)
        self.assertEqual(len(raw_repository.snapshots), 1)
        self.assertEqual(len(raw_repository.snapshot_heads), 1)

    async def test_executor_writes_request_log_for_guarded_non_json_response(self) -> None:
        transport = _FakeTransport(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/1/comments",
                status_code=403,
                headers={"Content-Type": "text/html"},
                body_bytes=b"<html>captcha</html>",
                attempts=(TransportAttempt(1, "proxy_2", 403, None, "bot_challenge"),),
                final_proxy_name="proxy_2",
                challenge_reason="bot_challenge",
            )
        )
        raw_repository = _FakeRawRepository()
        executor = FetchExecutor(transport=transport, raw_repository=raw_repository, sql_executor=object())

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-2",
                job_id="job-2",
                sport_slug="tennis",
                endpoint_pattern="/api/v1/event/{event_id}/comments",
                source_url="https://www.sofascore.com/api/v1/event/1/comments",
                timeout_profile="fast_live_edge",
                timeout_seconds=5.0,
                context_entity_type="event",
                context_entity_id=1,
                context_unique_tournament_id=None,
                context_season_id=None,
                context_event_id=1,
                expected_content_type="application/json",
                fetch_reason="refresh_live_event",
            )
        )

        self.assertEqual(outcome.classification, "challenge_detected")
        self.assertEqual(outcome.snapshot_id, 101)
        self.assertEqual(len(raw_repository.request_logs), 1)
        self.assertEqual(len(raw_repository.snapshots), 1)
        self.assertEqual(len(raw_repository.snapshot_heads), 0)
        self.assertEqual(raw_repository.request_logs[0].attempts_json[0]["challenge_reason"], "bot_challenge")
        self.assertEqual(raw_repository.request_logs[0].payload_bytes, len(b"<html>captcha</html>"))

    async def test_executor_skips_snapshot_insert_on_soft_error_200(self) -> None:
        """200-with-soft-error-body: keep request log, drop the snapshot insert.

        Sofascore returns ``{"error": {...}}`` with status 200 for some
        endpoints (e.g. /event/{e}/statistics for events without stats,
        /team/{t}/featured-players for teams without featured rosters).
        These rows occupy disk space without contributing to either
        parsing or read-side passthrough; the negative cache absorbs
        them via the outcome's ``is_soft_error_payload`` flag.
        """

        transport = _FakeTransport(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/9/statistics",
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"error":{"code":404,"message":"Not Found"}}',
                attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
                final_proxy_name="proxy_1",
                challenge_reason=None,
            )
        )
        raw_repository = _FakeRawRepository()
        executor = FetchExecutor(transport=transport, raw_repository=raw_repository, sql_executor=object())

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-soft",
                job_id="job-soft",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}/statistics",
                source_url="https://www.sofascore.com/api/v1/event/9/statistics",
                timeout_profile="standard_json",
                context_entity_type="event",
                context_entity_id=9,
                context_event_id=9,
                fetch_reason="hydrate_event_edge",
            )
        )

        self.assertEqual(outcome.classification, "soft_error_json")
        self.assertTrue(outcome.is_soft_error_payload)
        self.assertIsNone(outcome.snapshot_id)
        self.assertEqual(len(raw_repository.request_logs), 1)
        self.assertEqual(len(raw_repository.snapshots), 0)
        self.assertEqual(len(raw_repository.snapshot_heads), 0)

    async def test_executor_skips_snapshot_insert_on_soft_error_404(self) -> None:
        """404 with body ``{"error":...}`` (e.g. /event/{e}/innings for non-MLB)."""

        transport = _FakeTransport(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/9/innings",
                status_code=404,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"error":{"code":404,"message":"Not Found"}}',
                attempts=(TransportAttempt(1, "proxy_1", 404, None, None),),
                final_proxy_name="proxy_1",
                challenge_reason=None,
            )
        )
        raw_repository = _FakeRawRepository()
        executor = FetchExecutor(transport=transport, raw_repository=raw_repository, sql_executor=object())

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-404",
                job_id="job-404",
                sport_slug="baseball",
                endpoint_pattern="/api/v1/event/{event_id}/innings",
                source_url="https://www.sofascore.com/api/v1/event/9/innings",
                timeout_profile="standard_json",
                context_entity_type="event",
                context_entity_id=9,
                context_event_id=9,
                fetch_reason="resource_refresh",
            )
        )

        self.assertEqual(outcome.classification, "not_found")
        self.assertTrue(outcome.is_soft_error_payload)
        self.assertIsNone(outcome.snapshot_id)
        self.assertEqual(len(raw_repository.request_logs), 1)
        self.assertEqual(len(raw_repository.snapshots), 0)

    async def test_executor_skips_stage_snapshot_on_deferred_soft_error(self) -> None:
        """Deferred path also drops soft-error snapshots."""

        transport = _FakeTransport(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/9/statistics",
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"error":{"code":404,"message":"Not Found"}}',
                attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
                final_proxy_name="proxy_1",
                challenge_reason=None,
            )
        )
        raw_repository = _FakeRawRepository()
        snapshot_store = _FakeSnapshotStore()
        executor = FetchExecutor(
            transport=transport,
            raw_repository=raw_repository,
            sql_executor=None,
            snapshot_store=snapshot_store,
            write_mode="deferred",
        )

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-soft-deferred",
                job_id="job-soft-deferred",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}/statistics",
                source_url="https://www.sofascore.com/api/v1/event/9/statistics",
                timeout_profile="pilot",
                context_entity_type="event",
                context_entity_id=9,
                context_event_id=9,
                fetch_reason="hydrate_event_edge",
            )
        )

        self.assertTrue(outcome.is_soft_error_payload)
        self.assertIsNone(outcome.snapshot_id)
        self.assertEqual(len(snapshot_store.records), 0)
        self.assertEqual(len(executor.prefetched_records), 1)
        self.assertIsNone(executor.prefetched_records[0].payload_snapshot)

    async def test_executor_writes_network_error_message_into_request_log(self) -> None:
        class _FailingTransport:
            async def fetch(self, url: str, *, headers=None, timeout: float = 20.0):
                del url, headers, timeout
                raise TimeoutError("transport timed out")

        raw_repository = _FakeRawRepository()
        executor = FetchExecutor(
            transport=_FailingTransport(),
            raw_repository=raw_repository,
            sql_executor=object(),
        )

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-timeout",
                job_id="job-timeout",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/77",
                timeout_profile="standard_json",
                fetch_reason="hydrate_event_root",
            )
        )

        self.assertEqual(outcome.classification, "network_error")
        self.assertEqual(len(raw_repository.request_logs), 1)
        self.assertEqual(raw_repository.request_logs[0].error_message, "transport timed out")
        self.assertIsNone(raw_repository.request_logs[0].attempts_json)

    async def test_executor_marks_freshness_after_successful_200(self) -> None:
        freshness_store = _FakeFreshnessStore()
        executor = FetchExecutor(
            transport=_FakeTransport(_transport_result(status_code=200)),
            raw_repository=_FakeRawRepository(),
            sql_executor=object(),
            freshness_store=freshness_store,
        )

        await executor.execute(
            _fetch_task(
                freshness_key="freshness:player:10",
                freshness_ttl_seconds=86_400,
            )
        )

        self.assertEqual(freshness_store.marked, [("freshness:player:10", 86_400)])

    async def test_executor_does_not_mark_freshness_for_non_200_statuses(self) -> None:
        for status_code in (403, 404, 500):
            with self.subTest(status_code=status_code):
                freshness_store = _FakeFreshnessStore()
                executor = FetchExecutor(
                    transport=_FakeTransport(_transport_result(status_code=status_code)),
                    raw_repository=_FakeRawRepository(),
                    sql_executor=object(),
                    freshness_store=freshness_store,
                )

                await executor.execute(
                    _fetch_task(
                        freshness_key="freshness:player:10",
                        freshness_ttl_seconds=86_400,
                    )
                )

                self.assertEqual(freshness_store.marked, [])

    async def test_executor_does_not_mark_freshness_without_freshness_fields(self) -> None:
        freshness_store = _FakeFreshnessStore()
        executor = FetchExecutor(
            transport=_FakeTransport(_transport_result(status_code=200)),
            raw_repository=_FakeRawRepository(),
            sql_executor=object(),
            freshness_store=freshness_store,
        )

        await executor.execute(_fetch_task())

        self.assertEqual(freshness_store.marked, [])

    def test_build_fetch_task_key_ignores_freshness_fields(self) -> None:
        from schema_inspector.fetch_executor import build_fetch_task_key

        baseline = _fetch_task()
        with_freshness = _fetch_task(
            freshness_key="freshness:player:10",
            freshness_ttl_seconds=86_400,
        )

        self.assertEqual(build_fetch_task_key(baseline), build_fetch_task_key(with_freshness))


def _transport_result(*, status_code: int) -> TransportResult:
    return TransportResult(
        resolved_url="https://www.sofascore.com/api/v1/player/10",
        status_code=status_code,
        headers={"Content-Type": "application/json"},
        body_bytes=b'{"player":{"id":10}}',
        attempts=(TransportAttempt(1, "proxy_1", status_code, None, None, "10.10.10.10:12345"),),
        final_proxy_name="proxy_1",
        final_proxy_address="10.10.10.10:12345",
        challenge_reason=None,
    )


def _fetch_task(**overrides) -> FetchTask:
    values = {
        "trace_id": "trace-player",
        "job_id": "job-player",
        "sport_slug": "football",
        "endpoint_pattern": "/api/v1/player/{player_id}",
        "source_url": "https://www.sofascore.com/api/v1/player/10",
        "timeout_profile": "pilot",
        "context_entity_type": "player",
        "context_entity_id": 10,
        "fetch_reason": "hydrate_entity_profile",
    }
    values.update(overrides)
    return FetchTask(**values)


# -- M1: per-attempt forensics on transport exhaustion --
#
# Without these we only saw ``proxy=NULL, attempts_json=NULL`` in
# api_request_log when the transport exhausted its retry budget on a
# network-level failure (timeout / SSL / DNS / etc.). The transport
# *did* try multiple proxies and recorded them in its local ``attempts``
# list, but the bare ``raise`` lost that information by the time
# FetchExecutor's ``except Exception`` ran. After M1 the transport
# raises ``TransportExhaustedError`` carrying the per-attempt log, and
# FetchExecutor unpacks it into the request_log so on-call can see
# which proxies failed and how slowly each attempt went.


class _RaisingTransport:
    """Simulates a transport that exhausted its retry budget — raises a
    ``TransportExhaustedError`` carrying per-attempt forensics. Stand-in
    for the real ``InspectorTransport.fetch`` exhaustion path."""

    def __init__(self, exception: Exception) -> None:
        self.exception = exception
        self.calls: list[tuple[str, dict[str, str] | None, float]] = []

    async def fetch(self, url: str, *, headers=None, timeout: float = 20.0):
        self.calls.append((url, headers, timeout))
        raise self.exception


class FetchExecutorTransportExhaustedTests(unittest.IsolatedAsyncioTestCase):
    async def test_transport_exhausted_error_carries_attempts_into_request_log(self) -> None:
        from schema_inspector.transport import TransportExhaustedError

        attempts = (
            TransportAttempt(
                attempt_number=1,
                proxy_name="proxy_a",
                status_code=None,
                error="request timed out after 10s",
                challenge_reason=None,
                proxy_address="10.10.10.10:3120",
                latency_ms=10043,
            ),
            TransportAttempt(
                attempt_number=2,
                proxy_name="proxy_b",
                status_code=None,
                error="SSL: cert mismatch",
                challenge_reason=None,
                proxy_address="10.10.10.11:3120",
                latency_ms=472,
            ),
            TransportAttempt(
                attempt_number=3,
                proxy_name="proxy_c",
                status_code=None,
                error="request timed out after 10s",
                challenge_reason=None,
                proxy_address="10.10.10.12:3120",
                latency_ms=10009,
            ),
        )
        exhausted = TransportExhaustedError(
            "request timed out after 10s",
            attempts=attempts,
            final_proxy_name="proxy_c",
            final_proxy_address="10.10.10.12:3120",
            original=TimeoutError("upstream timeout"),
        )
        transport = _RaisingTransport(exhausted)
        raw_repository = _FakeRawRepository()
        executor = FetchExecutor(
            transport=transport,
            raw_repository=raw_repository,
            sql_executor=object(),
        )

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-x",
                job_id="job-x",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/15345941",
                timeout_profile="pilot",
                context_entity_type="event",
                context_entity_id=15345941,
                context_event_id=15345941,
                fetch_reason="hydrate_event_root",
            )
        )

        # Outcome propagates the network_error classification + retry signal.
        self.assertEqual(outcome.classification, "network_error")
        self.assertTrue(outcome.retry_recommended)
        self.assertIsNone(outcome.snapshot_id)
        # Critical: the new request_log row carries the attempts log,
        # not proxy=NULL like before.
        self.assertEqual(len(raw_repository.request_logs), 1)
        log = raw_repository.request_logs[0]
        self.assertEqual(log.proxy_id, "proxy_c")
        self.assertEqual(log.proxy_address, "10.10.10.12:3120")
        self.assertEqual(log.transport_attempt, 3)
        self.assertEqual(len(log.attempts_json), 3)
        # Each attempt entry carries proxy + latency_ms (the new field).
        self.assertEqual(log.attempts_json[0]["proxy_name"], "proxy_a")
        self.assertEqual(log.attempts_json[0]["proxy_address"], "10.10.10.10:3120")
        self.assertEqual(log.attempts_json[0]["latency_ms"], 10043)
        self.assertEqual(log.attempts_json[1]["proxy_name"], "proxy_b")
        self.assertEqual(log.attempts_json[1]["latency_ms"], 472)
        self.assertEqual(log.attempts_json[2]["proxy_name"], "proxy_c")
        self.assertEqual(log.attempts_json[2]["latency_ms"], 10009)
        # error_message preserved from the wrapper exception text.
        self.assertIn("timed out", log.error_message or "")

    async def test_legacy_exception_without_attempts_falls_back_to_null(self) -> None:
        # Defensive: any exception that ISN'T a TransportExhaustedError
        # (e.g. a programming bug raising RuntimeError directly) should
        # still produce a network_error outcome with proxy=NULL — same
        # behaviour as before M1. We must not crash if attempts info is
        # absent.
        transport = _RaisingTransport(RuntimeError("unrelated bug"))
        raw_repository = _FakeRawRepository()
        executor = FetchExecutor(
            transport=transport,
            raw_repository=raw_repository,
            sql_executor=object(),
        )

        outcome = await executor.execute(
            FetchTask(
                trace_id="trace-y",
                job_id="job-y",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/2",
                timeout_profile="pilot",
                context_entity_type="event",
                context_entity_id=2,
                context_event_id=2,
                fetch_reason="hydrate_event_root",
            )
        )

        self.assertEqual(outcome.classification, "network_error")
        self.assertEqual(len(raw_repository.request_logs), 1)
        log = raw_repository.request_logs[0]
        self.assertIsNone(log.proxy_id)
        self.assertIsNone(log.proxy_address)
        self.assertIsNone(log.transport_attempt)
        self.assertIsNone(log.attempts_json)


class TransportAttemptLatencyFieldTests(unittest.TestCase):
    def test_default_latency_ms_is_none_for_backwards_compat(self) -> None:
        attempt = TransportAttempt(
            attempt_number=1,
            proxy_name="proxy_x",
            status_code=200,
            error=None,
            challenge_reason=None,
        )
        # Existing call sites that don't pass latency_ms still construct
        # TransportAttempt without errors.
        self.assertIsNone(attempt.latency_ms)

    def test_latency_ms_roundtrip(self) -> None:
        attempt = TransportAttempt(
            attempt_number=2,
            proxy_name="proxy_y",
            status_code=None,
            error="timed out",
            challenge_reason=None,
            proxy_address="1.2.3.4:8080",
            latency_ms=10042,
        )
        self.assertEqual(attempt.latency_ms, 10042)


class FetchExecutorTimeoutOverrideTests(unittest.IsolatedAsyncioTestCase):
    """Phase1-A2 (2026-05-29): a per-executor ``fetch_timeout_seconds``
    overrides the task's own timeout so the live lane can apply a per-tier
    timeout (tier_1 longer to clear proxy bursts, tier_3 shorter) without
    threading a value into every FetchTask."""

    @staticmethod
    def _ok_transport() -> "_FakeTransport":
        return _FakeTransport(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/1",
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"event":{"id":1}}',
                attempts=(TransportAttempt(1, "proxy_1", 200, None, None, "10.0.0.1:1"),),
                final_proxy_name="proxy_1",
                final_proxy_address="10.0.0.1:1",
                challenge_reason=None,
            )
        )

    @staticmethod
    def _task(timeout_seconds: float) -> FetchTask:
        return FetchTask(
            trace_id="t",
            job_id="j",
            sport_slug="football",
            endpoint_pattern="/api/v1/event/{event_id}",
            source_url="https://www.sofascore.com/api/v1/event/1",
            timeout_profile="pilot",
            timeout_seconds=timeout_seconds,
            context_entity_type="event",
            context_entity_id=1,
            fetch_reason="hydrate_event_root",
        )

    async def test_override_replaces_task_timeout(self) -> None:
        transport = self._ok_transport()
        executor = FetchExecutor(
            transport=transport,
            raw_repository=_FakeRawRepository(),
            sql_executor=object(),
            fetch_timeout_seconds=25.0,
        )
        await executor.execute(self._task(timeout_seconds=10.0))
        # transport.fetch received the override (25.0), not the task's 10.0.
        self.assertEqual(transport.calls[0][2], 25.0)

    async def test_no_override_keeps_task_timeout(self) -> None:
        transport = self._ok_transport()
        executor = FetchExecutor(
            transport=transport,
            raw_repository=_FakeRawRepository(),
            sql_executor=object(),
        )
        await executor.execute(self._task(timeout_seconds=10.0))
        self.assertEqual(transport.calls[0][2], 10.0)

    async def test_zero_or_negative_override_ignored(self) -> None:
        transport = self._ok_transport()
        executor = FetchExecutor(
            transport=transport,
            raw_repository=_FakeRawRepository(),
            sql_executor=object(),
            fetch_timeout_seconds=0,
        )
        await executor.execute(self._task(timeout_seconds=10.0))
        self.assertEqual(transport.calls[0][2], 10.0)


if __name__ == "__main__":
    unittest.main()
