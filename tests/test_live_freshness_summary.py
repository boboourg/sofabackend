"""Tests for the live freshness SLO summary (P0.C 2026-05-14).

Pin the observability contract exposed by ``/ops/health``:
- oldest_hot_score_age_seconds (Redis zset:live:hot)
- refresh_live_event_success_rate_5min (Postgres etl_job_run)
- tier_1_blocked_rate_cumulative (Redis live:dispatch_metrics, via QueueSummary)
- status="healthy"/"degraded"/"unknown" based on SLO breaches
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from schema_inspector.ops.health import (
    LiveFreshnessSlo,
    LiveFreshnessSummary,
    _build_live_freshness_summary,
)
from schema_inspector.ops.queue_summary import QueueSummary


class _StubExecutor:
    """asyncpg-shaped executor stub with controllable refresh counts."""

    def __init__(self, *, succeeded: int = 0, retry_scheduled: int = 0, failed: int = 0, raise_on_fetchrow: bool = False) -> None:
        self.succeeded = succeeded
        self.retry_scheduled = retry_scheduled
        self.failed = failed
        self.raise_on_fetchrow = raise_on_fetchrow
        self.fetchrow_queries: list[str] = []

    async def fetchrow(self, query: str):
        self.fetchrow_queries.append(query)
        if self.raise_on_fetchrow:
            raise RuntimeError("forced")
        return {
            "succeeded": self.succeeded,
            "retry_scheduled": self.retry_scheduled,
            "failed": self.failed,
        }


class _StubRedis:
    """Redis backend with a configurable hot zset (oldest first)."""

    def __init__(self, *, hot_zset: list[tuple[str, int]] | None = None, raise_on_zrange: bool = False) -> None:
        self.hot_zset = list(hot_zset or [])
        self.raise_on_zrange = raise_on_zrange

    def zrange(self, key: str, start: int, end: int, withscores: bool = False):
        if self.raise_on_zrange:
            raise RuntimeError("forced redis error")
        if key != "zset:live:hot":
            return []
        # sorted by score asc (smallest first)
        sorted_zset = sorted(self.hot_zset, key=lambda item: item[1])
        sliced = sorted_zset[start : end + 1 if end >= 0 else None]
        if not withscores:
            return [member for member, _ in sliced]
        return [(member, float(score)) for member, score in sliced]


def _make_queue_summary(*, tier_1_attempts: int = 0, tier_1_blocked: int = 0, tier_1_active: int = 0) -> QueueSummary:
    metrics = {}
    if tier_1_attempts:
        metrics["claim_attempts:tier_1"] = tier_1_attempts
    if tier_1_blocked:
        metrics["claim_failed_blocked:tier_1"] = tier_1_blocked
    return QueueSummary(
        redis_backend_kind="memory",
        live_dispatch_metrics=metrics,
        live_tier_counts={"tier_1": tier_1_active},
    )


def _slo(summary: LiveFreshnessSummary, name: str) -> LiveFreshnessSlo:
    for slo in summary.slos:
        if slo.name == name:
            return slo
    raise AssertionError(f"slo {name!r} missing from summary")


class LiveFreshnessSummaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_healthy_when_redis_signals_under_threshold(self) -> None:
        # oldest = 100s, tier_1 blocked = 50% with active events
        # success_rate is None (helper does not query DB after 2026-05-14
        # to avoid the 30s etl_job_run scan); SLO not breached when None.
        now_ms = 1_700_000_000_000
        now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
        summary = await _build_live_freshness_summary(
            sql_executor=_StubExecutor(succeeded=200, retry_scheduled=0, failed=0),
            redis_backend=_StubRedis(hot_zset=[("evt-1", now_ms - 100_000)]),
            queue_summary=_make_queue_summary(
                tier_1_attempts=100, tier_1_blocked=50, tier_1_active=3
            ),
            now=now,
        )
        self.assertEqual(summary.status, "healthy")
        self.assertEqual(summary.oldest_hot_score_age_seconds, 100)
        self.assertIsNone(summary.refresh_live_event_success_rate_5min)
        self.assertEqual(summary.tier_1_blocked_rate_cumulative, 0.5)
        self.assertEqual(summary.refresh_live_event_succeeded_5min, 0)
        # No SLO breached
        self.assertFalse(any(slo.breached for slo in summary.slos))

    async def test_degraded_when_oldest_hot_age_above_threshold(self) -> None:
        now_ms = 1_700_000_000_000
        now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
        # default threshold is 900s; score 30 min in past => 1800s old
        summary = await _build_live_freshness_summary(
            sql_executor=_StubExecutor(succeeded=200),
            redis_backend=_StubRedis(hot_zset=[("evt-stale", now_ms - 1_800_000)]),
            queue_summary=_make_queue_summary(),
            now=now,
        )
        self.assertEqual(summary.status, "degraded")
        slo = _slo(summary, "oldest_hot_score_age_seconds")
        self.assertTrue(slo.breached)
        self.assertEqual(slo.actual, 1800)
        self.assertEqual(slo.threshold, 900)
        self.assertIsNotNone(slo.note)

    async def test_success_rate_slo_inactive_post_2026_05_14(self) -> None:
        """The success_rate SLO is reported as None until the etl_job_run
        index lands (see health.py docstring). Confirm it never breaches."""

        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
        summary = await _build_live_freshness_summary(
            sql_executor=_StubExecutor(succeeded=50, retry_scheduled=40, failed=10),
            redis_backend=_StubRedis(),
            queue_summary=_make_queue_summary(),
            now=now,
        )
        slo = _slo(summary, "refresh_live_event_success_rate_5min")
        self.assertIsNone(slo.actual)
        self.assertFalse(slo.breached)

    async def test_tier_1_blocked_breach_requires_active_events(self) -> None:
        """Without active tier_1 events the metric is cumulative noise and
        must NOT cause a breach even if ratio is above threshold."""

        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
        # 95% blocked but tier_1_active = 0
        summary = await _build_live_freshness_summary(
            sql_executor=_StubExecutor(succeeded=200),
            redis_backend=_StubRedis(),
            queue_summary=_make_queue_summary(
                tier_1_attempts=1000, tier_1_blocked=950, tier_1_active=0
            ),
            now=now,
        )
        self.assertEqual(summary.status, "healthy")
        slo = _slo(summary, "tier_1_blocked_rate_cumulative")
        self.assertFalse(slo.breached)
        self.assertEqual(slo.actual, 0.95)

    async def test_tier_1_blocked_breach_when_active(self) -> None:
        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
        summary = await _build_live_freshness_summary(
            sql_executor=_StubExecutor(succeeded=200),
            redis_backend=_StubRedis(),
            queue_summary=_make_queue_summary(
                tier_1_attempts=1000, tier_1_blocked=950, tier_1_active=5
            ),
            now=now,
        )
        self.assertEqual(summary.status, "degraded")
        slo = _slo(summary, "tier_1_blocked_rate_cumulative")
        self.assertTrue(slo.breached)
        self.assertEqual(slo.actual, 0.95)

    async def test_unknown_when_all_probes_return_none(self) -> None:
        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
        # empty hot zset + no refresh jobs + no dispatch metrics
        summary = await _build_live_freshness_summary(
            sql_executor=_StubExecutor(succeeded=0, retry_scheduled=0, failed=0),
            redis_backend=_StubRedis(hot_zset=[]),
            queue_summary=_make_queue_summary(),
            now=now,
        )
        self.assertEqual(summary.status, "unknown")
        self.assertIsNone(summary.oldest_hot_score_age_seconds)
        self.assertIsNone(summary.refresh_live_event_success_rate_5min)
        self.assertIsNone(summary.tier_1_blocked_rate_cumulative)

    async def test_resilient_to_redis_failure(self) -> None:
        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
        # Redis zrange raises -> still produces summary, oldest_age=None
        summary = await _build_live_freshness_summary(
            sql_executor=_StubExecutor(succeeded=10),
            redis_backend=_StubRedis(raise_on_zrange=True),
            queue_summary=_make_queue_summary(),
            now=now,
        )
        self.assertIn(summary.status, {"healthy", "unknown"})
        self.assertIsNone(summary.oldest_hot_score_age_seconds)

    async def test_does_not_call_sql_executor(self) -> None:
        """Post-2026-05-14: the helper is Redis-only. Confirm the executor
        is not invoked even if it would otherwise raise/hang."""

        import asyncio

        class _ForbiddenExecutor:
            async def fetchrow(self, query: str):
                raise AssertionError("sql executor must not be called")

        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
        summary = await _build_live_freshness_summary(
            sql_executor=_ForbiddenExecutor(),
            redis_backend=_StubRedis(),
            queue_summary=_make_queue_summary(),
            now=now,
        )
        # Counts always zero because the helper does not query DB
        self.assertEqual(summary.refresh_live_event_succeeded_5min, 0)
        # Success rate becomes None -> SLO not breached
        slo = _slo(summary, "refresh_live_event_success_rate_5min")
        self.assertFalse(slo.breached)

    async def test_resilient_to_db_failure(self) -> None:
        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
        summary = await _build_live_freshness_summary(
            sql_executor=_StubExecutor(raise_on_fetchrow=True),
            redis_backend=_StubRedis(hot_zset=[("evt", int(now.timestamp() * 1000))]),
            queue_summary=_make_queue_summary(),
            now=now,
        )
        # Failed counts default to zeros -> success_rate None -> not breached
        self.assertIn(summary.status, {"healthy", "unknown"})
        self.assertEqual(summary.refresh_live_event_succeeded_5min, 0)
        self.assertEqual(summary.refresh_live_event_retry_5min, 0)
        self.assertEqual(summary.refresh_live_event_failed_5min, 0)

    async def test_oldest_score_in_future_returns_zero_age(self) -> None:
        """When the next_poll_at score is in the future the event is
        scheduled, not stale — age should be 0, not negative."""

        now_ms = 1_700_000_000_000
        now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
        summary = await _build_live_freshness_summary(
            sql_executor=_StubExecutor(succeeded=100),
            redis_backend=_StubRedis(hot_zset=[("evt-future", now_ms + 60_000)]),
            queue_summary=_make_queue_summary(),
            now=now,
        )
        self.assertEqual(summary.oldest_hot_score_age_seconds, 0)
        slo = _slo(summary, "oldest_hot_score_age_seconds")
        self.assertFalse(slo.breached)

    async def test_summary_serialises_with_dataclasses_asdict(self) -> None:
        """The summary must round-trip via dataclasses.asdict because the
        local API serialises HealthReport that way."""

        import dataclasses

        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)
        summary = await _build_live_freshness_summary(
            sql_executor=_StubExecutor(succeeded=200, retry_scheduled=0),
            redis_backend=_StubRedis(hot_zset=[("evt-1", int(now.timestamp() * 1000))]),
            queue_summary=_make_queue_summary(
                tier_1_attempts=100, tier_1_blocked=10, tier_1_active=2
            ),
            now=now,
        )
        as_dict = dataclasses.asdict(summary)
        self.assertEqual(as_dict["status"], "healthy")
        # dataclasses.asdict preserves tuple for tuple fields; JSON
        # serialiser (orjson) handles both list and tuple identically.
        self.assertIsInstance(as_dict["slos"], (list, tuple))
        self.assertEqual(len(as_dict["slos"]), 3)
        for slo in as_dict["slos"]:
            self.assertIn("name", slo)
            self.assertIn("actual", slo)
            self.assertIn("threshold", slo)
            self.assertIn("breached", slo)


if __name__ == "__main__":
    unittest.main()
