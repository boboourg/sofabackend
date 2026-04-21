from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone


class OpsHealthTests(unittest.IsolatedAsyncioTestCase):
    async def test_collect_queue_summary_reports_stream_lag_and_delayed_counts(self) -> None:
        from schema_inspector.ops.queue_summary import collect_queue_summary

        summary = await collect_queue_summary(
            stream_queue=_FakeStreamQueue(
                stream_lengths={
                    "stream:etl:historical_enrichment": 17,
                    "stream:etl:hydrate": 9,
                },
                group_infos={
                    ("stream:etl:historical_enrichment", "cg:historical_enrichment"): _FakeGroupInfo(
                        consumers=2,
                        entries_read=11,
                        lag=6,
                        last_delivered_id="1-0",
                    ),
                    ("stream:etl:hydrate", "cg:hydrate"): _FakeGroupInfo(
                        consumers=3,
                        entries_read=8,
                        lag=1,
                        last_delivered_id="2-0",
                    ),
                },
                pending_summaries={
                    ("stream:etl:historical_enrichment", "cg:historical_enrichment"): _FakePendingSummary(
                        total=4,
                        smallest_id="1-1",
                        largest_id="1-4",
                        consumers={"historical-enrichment-1": 4},
                    )
                },
            ),
            live_state_store=_FakeLiveStateStore(
                _FakeRedisBackend(
                    {
                        "live:hot": ("1", "2"),
                        "live:warm": ("3",),
                        "live:cold": (),
                        "zset:etl:delayed": ("a", "b", "c"),
                    }
                )
            ),
            redis_backend=_FakeRedisBackend({"zset:etl:delayed": ("a", "b", "c")}),
            now_ms=5000,
        )

        self.assertEqual(summary.live_lanes["hot"], 2)
        self.assertEqual(summary.live_lanes["warm"], 1)
        self.assertEqual(summary.live_lanes["cold"], 0)
        self.assertEqual(summary.delayed_total, 3)
        self.assertEqual(summary.delayed_due, 3)
        by_stream = {item.stream: item for item in summary.streams}
        self.assertEqual(by_stream["stream:etl:historical_enrichment"].lag, 6)
        self.assertEqual(by_stream["stream:etl:historical_enrichment"].pending_total, 4)
        self.assertEqual(by_stream["stream:etl:hydrate"].lag, 1)

    async def test_collect_health_report_marks_go_live_ready_when_gates_pass(self) -> None:
        from schema_inspector.ops.health import collect_health_report

        now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
        report = await collect_health_report(
            sql_executor=_FakeSqlExecutor(
                {
                    "SELECT COUNT(*) FROM api_payload_snapshot": 7,
                    "SELECT COUNT(*) FROM endpoint_capability_rollup": 3,
                },
                rows_by_query={
                    "health_coverage": [
                        {
                            "tracked_scope_count": 11,
                            "fresh_scope_count": 11,
                            "stale_scope_count": 0,
                            "other_scope_count": 0,
                            "source_count": 1,
                            "sport_count": 13,
                            "surface_count": 4,
                            "avg_completeness_ratio": 1.0,
                        }
                    ],
                    "health_snapshot_freshness": [
                        {"latest_fetched_at": now - timedelta(minutes=2)},
                    ],
                    "health_retry_pressure": [
                        {"recent_total_runs": 250, "retry_scheduled_runs": 1},
                    ],
                },
            ),
            live_state_store=_FakeLiveStateStore(_FakeRedisBackend({})),
            redis_backend=_FakeRedisBackend({"zset:etl:delayed": ()}),
            stream_queue=_FakeStreamQueue(
                group_infos={
                    ("stream:etl:historical_enrichment", "cg:historical_enrichment"): _FakeGroupInfo(
                        consumers=2,
                        entries_read=80,
                        lag=25,
                        last_delivered_id="1-0",
                    )
                }
            ),
            housekeeping_dry_run=False,
            now=now,
        )

        self.assertTrue(report.go_live.ready)
        self.assertEqual(report.go_live.flag_count, 0)
        self.assertEqual(report.go_live.historical_enrichment_lag, 25)
        self.assertAlmostEqual(report.go_live.historical_retry_share, 0.004, places=6)
        self.assertEqual(report.go_live.snapshot_age_seconds, 120)
        self.assertFalse(report.go_live.housekeeping_dry_run)

    async def test_collect_health_report_flags_go_live_failures(self) -> None:
        from schema_inspector.ops.health import collect_health_report

        now = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)
        report = await collect_health_report(
            sql_executor=_FakeSqlExecutor(
                {
                    "SELECT COUNT(*) FROM api_payload_snapshot": 7,
                    "SELECT COUNT(*) FROM endpoint_capability_rollup": 3,
                },
                rows_by_query={
                    "health_coverage": [
                        {
                            "tracked_scope_count": 11,
                            "fresh_scope_count": 9,
                            "stale_scope_count": 2,
                            "other_scope_count": 0,
                            "source_count": 1,
                            "sport_count": 13,
                            "surface_count": 4,
                            "avg_completeness_ratio": 0.81,
                        }
                    ],
                    "health_snapshot_freshness": [
                        {"latest_fetched_at": now - timedelta(minutes=17)},
                    ],
                    "health_retry_pressure": [
                        {"recent_total_runs": 100, "retry_scheduled_runs": 4},
                    ],
                },
            ),
            live_state_store=_FakeLiveStateStore(_FailingRedisBackend({})),
            redis_backend=_FailingRedisBackend({"zset:etl:delayed": ()}),
            stream_queue=_FakeStreamQueue(
                group_infos={
                    ("stream:etl:historical_enrichment", "cg:historical_enrichment"): _FakeGroupInfo(
                        consumers=2,
                        entries_read=80,
                        lag=2400,
                        last_delivered_id="1-0",
                    ),
                    ("stream:etl:hydrate", "cg:hydrate"): _FakeGroupInfo(
                        consumers=2,
                        entries_read=90,
                        lag=1200,
                        last_delivered_id="2-0",
                    ),
                }
            ),
            housekeeping_dry_run=True,
            now=now,
        )

        self.assertFalse(report.redis_ok)
        self.assertFalse(report.go_live.ready)
        reasons = {flag.reason for flag in report.go_live.flags}
        self.assertIn("redis_unhealthy", reasons)
        self.assertIn("historical_enrichment_backlog_high", reasons)
        self.assertIn("hydrate_backlog_high", reasons)
        self.assertIn("snapshot_stale", reasons)
        self.assertIn("historical_retry_share_high", reasons)
        self.assertIn("coverage_stale_scopes_present", reasons)
        self.assertIn("housekeeping_dry_run_enabled", reasons)

    async def test_live_drift_query_covers_legacy_and_concrete_live_snapshot_patterns(self) -> None:
        from schema_inspector.ops.health import _fetch_drift_summary

        executor = _CapturingSqlExecutor()
        await _fetch_drift_summary(executor)

        self.assertIsNotNone(executor.last_query)
        normalized = " ".join(str(executor.last_query).split())
        self.assertIn("aps.endpoint_pattern = '/api/v1/sport/{sport_slug}/events/live'", normalized)
        self.assertIn("aps.endpoint_pattern LIKE '/api/v1/sport/%/events/live'", normalized)
        self.assertIn("aps.source_url LIKE '%/api/v1/sport/%/events/live%'", normalized)


class _CapturingSqlExecutor:
    def __init__(self) -> None:
        self.last_query: str | None = None

    async def fetch(self, query: str):
        self.last_query = query
        return []


class _FakeSqlExecutor:
    def __init__(self, values_by_query: dict[str, int], rows_by_query: dict[str, list[dict[str, object]]] | None = None) -> None:
        self.values_by_query = values_by_query
        self.rows_by_query = dict(rows_by_query or {})

    async def fetchval(self, query: str):
        return self.values_by_query[query]

    async def fetch(self, query: str):
        normalized = " ".join(query.split())
        if "FROM event_terminal_state AS ets" in normalized:
            return list(self.rows_by_query.get("health_drift", ()))
        if "FROM coverage_ledger" in normalized:
            return list(self.rows_by_query.get("health_coverage", ()))
        if "MAX(fetched_at) AS latest_fetched_at" in normalized:
            return list(self.rows_by_query.get("health_snapshot_freshness", ()))
        if "retry_scheduled_runs" in normalized:
            return list(self.rows_by_query.get("health_retry_pressure", ()))
        return []


class _FakeLiveStateStore:
    def __init__(self, backend) -> None:
        self.backend = backend

    def _lane_key(self, lane: str) -> str:
        return f"live:{lane}"


class _FakeRedisBackend:
    def __init__(self, values_by_key: dict[str, tuple[str, ...]]) -> None:
        self.values_by_key = dict(values_by_key)

    def ping(self) -> bool:
        return True

    def zrangebyscore(self, key: str, minimum: float, maximum: float):
        del minimum, maximum
        return tuple(self.values_by_key.get(key, ()))


class _FailingRedisBackend(_FakeRedisBackend):
    def ping(self) -> bool:
        raise RuntimeError("redis unavailable")


class _FakePendingSummary:
    def __init__(self, *, total: int, smallest_id: str | None, largest_id: str | None, consumers: dict[str, int]) -> None:
        self.total = total
        self.smallest_id = smallest_id
        self.largest_id = largest_id
        self.consumers = dict(consumers)


class _FakeGroupInfo:
    def __init__(self, *, consumers: int, entries_read: int | None, lag: int | None, last_delivered_id: str | None) -> None:
        self.consumers = consumers
        self.entries_read = entries_read
        self.lag = lag
        self.last_delivered_id = last_delivered_id


class _FakeStreamQueue:
    def __init__(
        self,
        *,
        stream_lengths: dict[str, int] | None = None,
        group_infos: dict[tuple[str, str], _FakeGroupInfo] | None = None,
        pending_summaries: dict[tuple[str, str], _FakePendingSummary] | None = None,
    ) -> None:
        self.stream_lengths = dict(stream_lengths or {})
        self.group_infos = dict(group_infos or {})
        self.pending_summaries = dict(pending_summaries or {})

    def pending_summary(self, stream_name: str, group_name: str):
        value = self.pending_summaries.get((stream_name, group_name))
        if value is None:
            raise RuntimeError("missing pending summary")
        return value

    def stream_length(self, stream_name: str) -> int:
        return int(self.stream_lengths.get(stream_name, 0))

    def group_info(self, stream_name: str, group_name: str):
        value = self.group_infos.get((stream_name, group_name))
        if value is None:
            raise RuntimeError("missing group info")
        return value


if __name__ == "__main__":
    unittest.main()
