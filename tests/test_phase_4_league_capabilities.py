"""Phase 4: League Capabilities Registry — probe aggregator + repository.

Two layers of tests:

  1. ``ProbeVerdictAggregator`` pure logic — no I/O, no Postgres.
     Pins the 3/5 success threshold, 404 / empty / error counts,
     and the verdict mapping (allowed / disabled / unknown).

  2. ``LeagueCapabilitiesRepository`` upsert/fetch contract via
     fake executor — pins the SQL shape and the validation
     constraints without needing a live Postgres.

The probe service itself does I/O (HTTP fetcher + DB writes) and
is exercised end-to-end via a separate sandbox script after the
unit tests gate the implementation.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone, timedelta


class ProbeVerdictAggregatorTests(unittest.TestCase):
    """Pin the verdict rules so the policy gate can rely on a single
    source of truth. Thresholds:

      success_rate = ok / total
      success_rate >= 0.6 (i.e. 3/5)   → allowed
      negative_rate (404 + empty) >= 0.6 → disabled
      otherwise                        → unknown
    """

    def test_aggregator_returns_unknown_when_total_zero(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            aggregate_verdict,
        )
        result = aggregate_verdict(ok=0, http_404=0, empty=0, error=0, total=0)
        self.assertEqual(result.state, "unknown")
        self.assertIsNone(result.confidence_score)

    def test_aggregator_allowed_at_three_of_five(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            aggregate_verdict,
        )
        result = aggregate_verdict(ok=3, http_404=2, empty=0, error=0, total=5)
        self.assertEqual(result.state, "allowed")
        self.assertAlmostEqual(result.confidence_score, 0.6, places=3)

    def test_aggregator_allowed_at_five_of_five(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            aggregate_verdict,
        )
        result = aggregate_verdict(ok=5, http_404=0, empty=0, error=0, total=5)
        self.assertEqual(result.state, "allowed")
        self.assertAlmostEqual(result.confidence_score, 1.0)

    def test_aggregator_disabled_at_zero_of_five_with_404s(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            aggregate_verdict,
        )
        # 5/5 404 = disabled.
        result = aggregate_verdict(ok=0, http_404=5, empty=0, error=0, total=5)
        self.assertEqual(result.state, "disabled")
        self.assertAlmostEqual(result.confidence_score, 0.0)

    def test_aggregator_disabled_when_empty_payload_dominant(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            aggregate_verdict,
        )
        # 1/5 ok + 4/5 empty = disabled (negative-signal dominant).
        result = aggregate_verdict(ok=1, http_404=0, empty=4, error=0, total=5)
        self.assertEqual(result.state, "disabled")

    def test_aggregator_unknown_when_two_ok_three_errors(self) -> None:
        """Errors are not strong negative signal — they could mean
        transient proxy issues. 2/5 ok + 3/5 error → unknown so the
        next probe pass re-evaluates."""
        from schema_inspector.services.league_capabilities_probe import (
            aggregate_verdict,
        )
        result = aggregate_verdict(ok=2, http_404=0, empty=0, error=3, total=5)
        self.assertEqual(result.state, "unknown")

    def test_aggregator_unknown_at_two_of_five_neither_dominant(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            aggregate_verdict,
        )
        result = aggregate_verdict(ok=2, http_404=1, empty=1, error=1, total=5)
        self.assertEqual(result.state, "unknown")


class CapabilityUpsertValidationTests(unittest.TestCase):
    def test_unknown_state_rejected(self) -> None:
        from schema_inspector.storage.league_capabilities_repository import (
            CapabilityUpsert,
        )
        row = CapabilityUpsert(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_pattern="/api/v1/event/{event_id}/incidents",
            state="weird_typo",  # invalid
            probe_samples_total=5,
            probe_samples_ok=5,
            probe_samples_http_404=0,
            probe_samples_empty=0,
            probe_samples_error=0,
            confidence_score=1.0,
            expires_at=datetime.now(timezone.utc) + timedelta(days=14),
        )
        with self.assertRaises(ValueError):
            row.validate()

    def test_ok_count_cannot_exceed_total(self) -> None:
        from schema_inspector.storage.league_capabilities_repository import (
            CapabilityUpsert,
        )
        row = CapabilityUpsert(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_pattern="/x",
            state="allowed",
            probe_samples_total=3,
            probe_samples_ok=4,  # invalid: 4 > 3
            probe_samples_http_404=0,
            probe_samples_empty=0,
            probe_samples_error=0,
            confidence_score=1.0,
            expires_at=datetime.now(timezone.utc),
        )
        with self.assertRaises(ValueError):
            row.validate()


class RepositoryReadAPITests(unittest.IsolatedAsyncioTestCase):
    """Verify the repository SELECT shapes by snooping executed SQL.
    No live Postgres needed — fake executor captures queries."""

    async def test_fetch_capability_uses_is_not_distinct_from_for_null_season(self) -> None:
        from schema_inspector.storage.league_capabilities_repository import (
            LeagueCapabilitiesRepository,
        )

        captured: list[tuple[str, tuple]] = []

        class _FakeExecutor:
            async def fetchrow(self, query: str, *args):
                captured.append((query, args))
                return None
            async def execute(self, query: str, *args):
                captured.append((query, args))
            async def fetch(self, query: str, *args):
                captured.append((query, args))
                return []

        repo = LeagueCapabilitiesRepository()
        await repo.fetch_capability(
            _FakeExecutor(),
            unique_tournament_id=16,
            season_id=None,
            status_type="finished",
            endpoint_pattern="/api/v1/event/{event_id}/highlights",
        )
        self.assertEqual(len(captured), 1)
        query = captured[0][0]
        # NULL season_id requires IS NOT DISTINCT FROM to compare
        # correctly (= would always return NULL).
        self.assertIn("season_id IS NOT DISTINCT FROM", query)

    async def test_list_expired_filters_manual_overrides(self) -> None:
        from schema_inspector.storage.league_capabilities_repository import (
            LeagueCapabilitiesRepository,
        )

        captured: list[tuple[str, tuple]] = []

        class _FakeExecutor:
            async def fetchrow(self, query, *args):
                return None
            async def execute(self, query, *args):
                return None
            async def fetch(self, query, *args):
                captured.append((query, args))
                return []

        repo = LeagueCapabilitiesRepository()
        await repo.list_expired(_FakeExecutor(), limit=10)
        self.assertEqual(len(captured), 1)
        query = captured[0][0]
        self.assertIn("expires_at < $1", query)
        # Manual overrides never expire — must be excluded.
        self.assertIn("source <> 'manual_override'", query)


class ProbeServiceContractTests(unittest.TestCase):
    """High-level service contract: probe_ut_season_status accepts
    a fetch_executor that returns per-(event, endpoint) HTTP status,
    aggregates verdicts, and returns one CapabilityUpsert per
    endpoint_pattern. Pure-Python test of the orchestration shape —
    no live HTTP."""

    def test_probe_pass_constructs_upserts_per_endpoint(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            ProbeSampleOutcome,
            build_capability_upserts,
        )
        sample_outcomes_by_endpoint = {
            "/api/v1/event/{event_id}/incidents": [
                ProbeSampleOutcome(event_id=1, status=200, payload_empty=False),
                ProbeSampleOutcome(event_id=2, status=200, payload_empty=False),
                ProbeSampleOutcome(event_id=3, status=200, payload_empty=False),
                ProbeSampleOutcome(event_id=4, status=200, payload_empty=False),
                ProbeSampleOutcome(event_id=5, status=200, payload_empty=False),
            ],
            "/api/v1/event/{event_id}/statistics": [
                ProbeSampleOutcome(event_id=1, status=404, payload_empty=False),
                ProbeSampleOutcome(event_id=2, status=404, payload_empty=False),
                ProbeSampleOutcome(event_id=3, status=404, payload_empty=False),
                ProbeSampleOutcome(event_id=4, status=404, payload_empty=False),
                ProbeSampleOutcome(event_id=5, status=404, payload_empty=False),
            ],
            "/api/v1/event/{event_id}/best-players/summary": [
                ProbeSampleOutcome(event_id=1, status=200, payload_empty=True),
                ProbeSampleOutcome(event_id=2, status=200, payload_empty=True),
                ProbeSampleOutcome(event_id=3, status=200, payload_empty=True),
                ProbeSampleOutcome(event_id=4, status=200, payload_empty=False),
                ProbeSampleOutcome(event_id=5, status=200, payload_empty=False),
            ],
        }
        upserts = build_capability_upserts(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            sample_outcomes_by_endpoint=sample_outcomes_by_endpoint,
        )
        by_endpoint = {u.endpoint_pattern: u for u in upserts}

        # incidents: 5/5 200 → allowed
        self.assertEqual(by_endpoint["/api/v1/event/{event_id}/incidents"].state, "allowed")
        # statistics: 5/5 404 → disabled
        self.assertEqual(by_endpoint["/api/v1/event/{event_id}/statistics"].state, "disabled")
        # best-players: 2/5 ok-with-content + 3/5 empty → disabled (empty counts as negative)
        self.assertEqual(by_endpoint["/api/v1/event/{event_id}/best-players/summary"].state, "disabled")

        # All three upserts share the (UT, season, status) context.
        for u in upserts:
            self.assertEqual(u.unique_tournament_id, 17)
            self.assertEqual(u.season_id, 61643)
            self.assertEqual(u.status_type, "inprogress")
            self.assertEqual(u.probe_samples_total, 5)


if __name__ == "__main__":
    unittest.main()
