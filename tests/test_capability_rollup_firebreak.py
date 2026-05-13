"""Tests for the 2026-05-13 firebreak that takes endpoint_capability_rollup
writes out of the live/hydrate hot path.

Covers:
- PilotOrchestrator._flush_capabilities respects
  SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED (default OFF).
- Inline legacy path, when ON, sorts keys by (sport_slug, endpoint_pattern).
- Out-of-band rebuild_rollups_from_observations aggregates counts from
  endpoint_capability_observation and replaces rollup state (not merge).
"""

from __future__ import annotations

import os
import unittest
from unittest import mock

from schema_inspector.pipeline.pilot_orchestrator import (
    DeferredCapabilityRecord,
    PilotOrchestrator,
    _inline_capability_rollup_enabled,
)
from schema_inspector.storage.capability_repository import (
    CapabilityObservationRecord,
    CapabilityRollupRecord,
    CapabilityRepository,
)


def _make_observation(sport_slug: str, endpoint_pattern: str) -> CapabilityObservationRecord:
    return CapabilityObservationRecord(
        sport_slug=sport_slug,
        endpoint_pattern=endpoint_pattern,
        entity_scope="event",
        context_type="event",
        http_status=200,
        payload_validity="valid_json",
        payload_root_keys=("event",),
        is_empty_payload=False,
        is_soft_error_payload=False,
        observed_at="2026-05-13T18:00:00+00:00",
        sample_snapshot_id=None,
    )


def _make_rollup(sport_slug: str, endpoint_pattern: str) -> CapabilityRollupRecord:
    return CapabilityRollupRecord(
        sport_slug=sport_slug,
        endpoint_pattern=endpoint_pattern,
        support_level="supported",
        confidence=0.5,
        last_success_at="2026-05-13T18:00:00+00:00",
        last_404_at=None,
        last_soft_error_at=None,
        success_count=1,
        not_found_count=0,
        soft_error_count=0,
        empty_count=0,
        notes=None,
    )


def _make_pending_records(*pairs: tuple[str, str]) -> list[DeferredCapabilityRecord]:
    return [
        DeferredCapabilityRecord(
            observation=_make_observation(sport, endpoint),
            rollup=_make_rollup(sport, endpoint),
        )
        for sport, endpoint in pairs
    ]


class _CountingCapabilityRepository:
    """Records every call so tests can assert ordering and absence/presence."""

    def __init__(self) -> None:
        self.insert_observation_calls: list[tuple[str, str]] = []
        self.upsert_rollup_calls: list[tuple[str, str]] = []

    async def insert_observation(self, executor, record):
        del executor
        self.insert_observation_calls.append((record.sport_slug, record.endpoint_pattern))

    async def upsert_rollup(self, executor, record):
        del executor
        self.upsert_rollup_calls.append((record.sport_slug, record.endpoint_pattern))


class _StubSqlExecutor:
    """Bare executor stub. _flush_capabilities does not introspect it."""


def _make_orchestrator(
    *,
    capability_repository,
    pending_records,
):
    """Construct a minimally-wired PilotOrchestrator without going through
    the heavy __init__ — we only exercise _flush_capabilities here."""

    orchestrator = PilotOrchestrator.__new__(PilotOrchestrator)
    orchestrator.capability_repository = capability_repository
    orchestrator.sql_executor = _StubSqlExecutor()
    orchestrator._pending_capability_records = list(pending_records)
    return orchestrator


class InlineCapabilityRollupFlagTests(unittest.IsolatedAsyncioTestCase):
    """Firebreak: inline rollup writes must be OFF by default."""

    async def test_inline_flag_default_is_off_no_env_set(self) -> None:
        # No SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED in env => OFF.
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED", None)
            self.assertFalse(_inline_capability_rollup_enabled())

    async def test_inline_flag_explicit_off_value(self) -> None:
        with mock.patch.dict(
            os.environ, {"SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED": "0"}
        ):
            self.assertFalse(_inline_capability_rollup_enabled())

    async def test_inline_flag_explicit_on_values(self) -> None:
        for raw in ("1", "true", "TRUE", "Yes", " yes ", "on"):
            with mock.patch.dict(
                os.environ, {"SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED": raw}
            ):
                self.assertTrue(
                    _inline_capability_rollup_enabled(),
                    msg=f"value {raw!r} should enable inline rollup",
                )

    async def test_flush_capabilities_off_writes_observation_only(self) -> None:
        """Default firebreak path: observations always, NO upsert_rollup."""

        repo = _CountingCapabilityRepository()
        orchestrator = _make_orchestrator(
            capability_repository=repo,
            pending_records=_make_pending_records(
                ("football", "/api/v1/event/{event_id}/lineups"),
                ("football", "/api/v1/event/{event_id}/statistics"),
                ("tennis", "/api/v1/event/{event_id}/point-by-point"),
            ),
        )

        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED", None)
            await orchestrator._flush_capabilities()

        self.assertEqual(len(repo.insert_observation_calls), 3)
        self.assertEqual(
            repo.upsert_rollup_calls,
            [],
            msg="default firebreak path must not invoke upsert_rollup",
        )
        # Hot path drained — pending list reset.
        self.assertEqual(orchestrator._pending_capability_records, [])

    async def test_flush_capabilities_on_preserves_legacy_writes(self) -> None:
        """Legacy fallback path: both observation and upsert_rollup run."""

        repo = _CountingCapabilityRepository()
        orchestrator = _make_orchestrator(
            capability_repository=repo,
            pending_records=_make_pending_records(
                ("football", "/api/v1/event/{event_id}/lineups"),
                ("football", "/api/v1/event/{event_id}/statistics"),
            ),
        )

        with mock.patch.dict(
            os.environ, {"SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED": "1"}
        ):
            await orchestrator._flush_capabilities()

        self.assertEqual(len(repo.insert_observation_calls), 2)
        self.assertEqual(len(repo.upsert_rollup_calls), 2)

    async def test_flush_capabilities_on_sorts_keys_deterministically(self) -> None:
        """Inline legacy path must sort writes by (sport_slug, endpoint_pattern)
        to avoid lock inversion across concurrent workers."""

        repo = _CountingCapabilityRepository()
        # Intentionally pass records in reverse order — handler must sort.
        orchestrator = _make_orchestrator(
            capability_repository=repo,
            pending_records=_make_pending_records(
                ("tennis", "/api/v1/event/{event_id}/tennis-power"),
                ("football", "/api/v1/event/{event_id}/statistics"),
                ("basketball", "/api/v1/event/{event_id}/lineups"),
                ("football", "/api/v1/event/{event_id}/incidents"),
            ),
        )

        with mock.patch.dict(
            os.environ, {"SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED": "1"}
        ):
            await orchestrator._flush_capabilities()

        self.assertEqual(
            repo.upsert_rollup_calls,
            [
                ("basketball", "/api/v1/event/{event_id}/lineups"),
                ("football", "/api/v1/event/{event_id}/incidents"),
                ("football", "/api/v1/event/{event_id}/statistics"),
                ("tennis", "/api/v1/event/{event_id}/tennis-power"),
            ],
        )

    async def test_flush_capabilities_no_op_when_no_repository(self) -> None:
        orchestrator = _make_orchestrator(
            capability_repository=None,
            pending_records=_make_pending_records(
                ("football", "/api/v1/event/{event_id}/lineups"),
            ),
        )
        await orchestrator._flush_capabilities()  # must not raise


class _RecordingExecutor:
    """Fake asyncpg connection that records calls and returns canned rows."""

    def __init__(self, aggregate_rows):
        self._aggregate_rows = aggregate_rows
        self.fetch_calls: list[tuple[str, tuple]] = []
        self.execute_calls: list[tuple[str, tuple]] = []

    async def fetch(self, query, *args):
        self.fetch_calls.append((query, args))
        return self._aggregate_rows

    async def execute(self, query, *args):
        self.execute_calls.append((query, args))


class RebuildRollupsFromObservationsTests(unittest.IsolatedAsyncioTestCase):
    """Out-of-band batch rebuilder for endpoint_capability_rollup."""

    async def test_aggregates_counts_into_rollup_per_key(self) -> None:
        repo = CapabilityRepository()
        executor = _RecordingExecutor(
            aggregate_rows=[
                {
                    "sport_slug": "football",
                    "endpoint_pattern": "/api/v1/event/{event_id}/lineups",
                    "success_count": 10,
                    "not_found_count": 2,
                    "soft_error_count": 1,
                    "empty_count": 0,
                    "last_success_at": "2026-05-13T18:00:00+00:00",
                    "last_404_at": "2026-05-12T10:00:00+00:00",
                    "last_soft_error_at": None,
                },
            ],
        )

        rebuilt = await repo.rebuild_rollups_from_observations(executor)

        self.assertEqual(rebuilt, 1)
        # Aggregation SQL must read from observations, not rollup.
        agg_query, agg_args = executor.fetch_calls[0]
        self.assertIn("FROM endpoint_capability_observation", agg_query)
        self.assertNotIn("FROM endpoint_capability_rollup", agg_query)
        self.assertEqual(agg_args, ())
        # The write must be a single ON CONFLICT DO UPDATE against rollup
        # with the aggregated counts -- *not* an additive merge.
        upsert_query, upsert_args = executor.execute_calls[0]
        self.assertIn("INSERT INTO endpoint_capability_rollup", upsert_query)
        self.assertIn(
            "ON CONFLICT (sport_slug, endpoint_pattern) DO UPDATE SET",
            upsert_query,
        )
        # support_level + confidence are computed from aggregate counts
        # (mixed success + soft_error + 404 => conditionally_supported).
        self.assertEqual(upsert_args[0], "football")
        self.assertEqual(upsert_args[1], "/api/v1/event/{event_id}/lineups")
        self.assertEqual(upsert_args[2], "conditionally_supported")
        self.assertAlmostEqual(upsert_args[3], 1.0)  # min(1.0, 13/3) = 1.0
        # success_count, not_found_count, soft_error_count, empty_count
        self.assertEqual(upsert_args[7], 10)
        self.assertEqual(upsert_args[8], 2)
        self.assertEqual(upsert_args[9], 1)
        self.assertEqual(upsert_args[10], 0)

    async def test_support_level_supported_when_only_successes(self) -> None:
        repo = CapabilityRepository()
        executor = _RecordingExecutor(
            aggregate_rows=[
                {
                    "sport_slug": "football",
                    "endpoint_pattern": "/api/v1/event/{event_id}/incidents",
                    "success_count": 5,
                    "not_found_count": 0,
                    "soft_error_count": 0,
                    "empty_count": 0,
                    "last_success_at": "2026-05-13T18:00:00+00:00",
                    "last_404_at": None,
                    "last_soft_error_at": None,
                },
            ],
        )
        await repo.rebuild_rollups_from_observations(executor)
        _, upsert_args = executor.execute_calls[0]
        self.assertEqual(upsert_args[2], "supported")

    async def test_support_level_unsupported_when_only_404(self) -> None:
        repo = CapabilityRepository()
        executor = _RecordingExecutor(
            aggregate_rows=[
                {
                    "sport_slug": "football",
                    "endpoint_pattern": "/api/v1/event/{event_id}/highlights",
                    "success_count": 0,
                    "not_found_count": 7,
                    "soft_error_count": 0,
                    "empty_count": 0,
                    "last_success_at": None,
                    "last_404_at": "2026-05-13T10:00:00+00:00",
                    "last_soft_error_at": None,
                },
            ],
        )
        await repo.rebuild_rollups_from_observations(executor)
        _, upsert_args = executor.execute_calls[0]
        self.assertEqual(upsert_args[2], "unsupported")

    async def test_filters_by_sport_slug(self) -> None:
        repo = CapabilityRepository()
        executor = _RecordingExecutor(aggregate_rows=[])
        await repo.rebuild_rollups_from_observations(executor, sport_slug="football")
        agg_query, agg_args = executor.fetch_calls[0]
        self.assertIn("sport_slug = $1", agg_query)
        self.assertEqual(agg_args, ("football",))

    async def test_respects_lookback_days(self) -> None:
        repo = CapabilityRepository()
        executor = _RecordingExecutor(aggregate_rows=[])
        await repo.rebuild_rollups_from_observations(executor, lookback_days=7)
        agg_query, agg_args = executor.fetch_calls[0]
        self.assertIn("observed_at >= (NOW() - make_interval(days => $1::int))", agg_query)
        self.assertEqual(agg_args, (7,))

    async def test_filters_combined_keep_param_order(self) -> None:
        repo = CapabilityRepository()
        executor = _RecordingExecutor(aggregate_rows=[])
        await repo.rebuild_rollups_from_observations(
            executor, sport_slug="football", lookback_days=14
        )
        agg_query, agg_args = executor.fetch_calls[0]
        self.assertIn("sport_slug = $1", agg_query)
        self.assertIn("observed_at >= (NOW() - make_interval(days => $2::int))", agg_query)
        self.assertEqual(agg_args, ("football", 14))

    async def test_replaces_not_merges_with_existing_rollup(self) -> None:
        """Firebreak contract: rebuild *replaces* counts from aggregate, never
        adds them on top of whatever the rollup row currently holds. The new
        method must not SELECT existing rollup state before writing."""

        repo = CapabilityRepository()
        executor = _RecordingExecutor(
            aggregate_rows=[
                {
                    "sport_slug": "football",
                    "endpoint_pattern": "/api/v1/event/{event_id}/lineups",
                    "success_count": 3,
                    "not_found_count": 0,
                    "soft_error_count": 0,
                    "empty_count": 0,
                    "last_success_at": "2026-05-13T18:00:00+00:00",
                    "last_404_at": None,
                    "last_soft_error_at": None,
                },
            ],
        )
        await repo.rebuild_rollups_from_observations(executor)

        # Exactly one read (the aggregate query) and one write (the ON
        # CONFLICT upsert). No pre-read of endpoint_capability_rollup, which
        # is exactly what distinguishes the firebreak path from the legacy
        # read-modify-write upsert_rollup.
        self.assertEqual(len(executor.fetch_calls), 1)
        self.assertEqual(len(executor.execute_calls), 1)
        for query, _ in executor.fetch_calls:
            self.assertNotIn(
                "FROM endpoint_capability_rollup",
                query,
                msg="rebuild must not read endpoint_capability_rollup state",
            )

    async def test_writes_in_sorted_pk_order(self) -> None:
        """Deterministic key ordering avoids future lock inversion when the
        batch runs against concurrent maintenance work."""

        repo = CapabilityRepository()
        # Aggregate query already returns rows in ORDER BY sport_slug,
        # endpoint_pattern -- but we explicitly pass them in unsorted order
        # to assert the SQL contract via the ORDER BY clause in the query.
        executor = _RecordingExecutor(
            aggregate_rows=[
                {
                    "sport_slug": "basketball",
                    "endpoint_pattern": "/api/v1/event/{event_id}/lineups",
                    "success_count": 1,
                    "not_found_count": 0,
                    "soft_error_count": 0,
                    "empty_count": 0,
                    "last_success_at": "2026-05-13T18:00:00+00:00",
                    "last_404_at": None,
                    "last_soft_error_at": None,
                },
                {
                    "sport_slug": "football",
                    "endpoint_pattern": "/api/v1/event/{event_id}/incidents",
                    "success_count": 1,
                    "not_found_count": 0,
                    "soft_error_count": 0,
                    "empty_count": 0,
                    "last_success_at": "2026-05-13T18:00:00+00:00",
                    "last_404_at": None,
                    "last_soft_error_at": None,
                },
                {
                    "sport_slug": "tennis",
                    "endpoint_pattern": "/api/v1/event/{event_id}/tennis-power",
                    "success_count": 1,
                    "not_found_count": 0,
                    "soft_error_count": 0,
                    "empty_count": 0,
                    "last_success_at": "2026-05-13T18:00:00+00:00",
                    "last_404_at": None,
                    "last_soft_error_at": None,
                },
            ],
        )
        await repo.rebuild_rollups_from_observations(executor)
        agg_query = executor.fetch_calls[0][0]
        self.assertIn("ORDER BY sport_slug, endpoint_pattern", agg_query)
        # Executor writes follow the aggregate row order (which itself is
        # sorted by the SQL ORDER BY).
        written = [(args[0], args[1]) for _, args in executor.execute_calls]
        self.assertEqual(written, sorted(written))

    async def test_returns_zero_when_no_observations(self) -> None:
        repo = CapabilityRepository()
        executor = _RecordingExecutor(aggregate_rows=[])
        rebuilt = await repo.rebuild_rollups_from_observations(executor)
        self.assertEqual(rebuilt, 0)
        self.assertEqual(executor.execute_calls, [])


class RebuildCapabilityRollupCliTests(unittest.TestCase):
    """The new ``rebuild-capability-rollup`` subcommand must be registered
    in the unified CLI parser and accept --sport-slug / --lookback-days."""

    def test_subcommand_registered_with_defaults(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()
        args = parser.parse_args(["rebuild-capability-rollup"])
        self.assertEqual(args.command, "rebuild-capability-rollup")
        self.assertIsNone(getattr(args, "sport_slug", None))
        self.assertIsNone(getattr(args, "lookback_days", None))

    def test_subcommand_accepts_sport_slug_and_lookback_days(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()
        args = parser.parse_args(
            [
                "rebuild-capability-rollup",
                "--sport-slug",
                "football",
                "--lookback-days",
                "7",
            ]
        )
        self.assertEqual(args.sport_slug, "football")
        self.assertEqual(args.lookback_days, 7)


if __name__ == "__main__":
    unittest.main()
