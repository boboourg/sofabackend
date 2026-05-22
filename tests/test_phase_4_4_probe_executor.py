"""Phase 4.4: ProbeExecutor — end-to-end probe orchestration tests.

The executor wires together:
  * Sample selection (last N events for (UT, season, status))
  * HTTP probing through SofascoreClient (fake for tests)
  * Outcome classification (ok / 404 / empty / error)
  * Verdict aggregation via Phase 4.2 build_capability_upserts
  * Repository upsert (fake)
  * Cache invalidation via registry.invalidate_quad (fake)

All tests are pure-Python — no live HTTP, no Postgres, no Redis.
"""

from __future__ import annotations

import unittest


# ---- Fake infra --------------------------------------------------------------


class _FakeClient:
    """SofascoreClient stand-in. Returns predetermined outcomes keyed
    by (event_id, endpoint_pattern). Tracks calls."""

    def __init__(self, outcomes: dict) -> None:
        self.outcomes = outcomes
        self.calls: list[tuple[int, str]] = []

    async def get_json(self, *, event_id: int, endpoint_pattern: str):
        self.calls.append((event_id, endpoint_pattern))
        key = (event_id, endpoint_pattern)
        if key not in self.outcomes:
            raise RuntimeError(f"No fake outcome for {key}")
        outcome = self.outcomes[key]
        if outcome["raise"]:
            raise RuntimeError(outcome.get("err", "fake error"))
        return outcome  # {'status': 200, 'payload': {...}}


class _FakeRepo:
    def __init__(self) -> None:
        self.upserts: list = []

    async def upsert_capability(self, executor, *, row) -> None:
        self.upserts.append(row)


class _FakeRegistry:
    def __init__(self) -> None:
        self.invalidate_calls: list = []

    async def invalidate_quad(self, **kwargs) -> int:
        self.invalidate_calls.append(kwargs)
        return len(kwargs.get("endpoint_patterns", ()))


class _FakeExecutor:
    async def __aenter__(self): return self
    async def __aexit__(self, *args): return False


class _FakeDatabase:
    def __init__(self, events: list[dict]) -> None:
        self._events = events
        self.connect_calls = 0

    def connection(self):
        self.connect_calls += 1
        return _FakeConnContext(self._events)


class _FakeConnContext:
    def __init__(self, events): self._events = events
    async def __aenter__(self): return _FakeConnection(self._events)
    async def __aexit__(self, *args): return False


class _FakeConnection:
    def __init__(self, events): self._events = events
    async def fetch(self, query, *args):
        # Pretend the SQL we issued matches; return canned events.
        return [_FakeRow(e) for e in self._events]
    async def fetchrow(self, query, *args): return None
    async def execute(self, query, *args): return None


class _FakeRow(dict):
    def __init__(self, d): super().__init__(d)


# ---- Tests -------------------------------------------------------------------


class ProbeExecutorSampleSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_executor_queries_last_n_events_for_quad(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            ProbeExecutor,
        )

        db = _FakeDatabase(events=[{"id": 1}, {"id": 2}, {"id": 3}])
        client = _FakeClient(outcomes={
            (1, "/api/v1/event/{event_id}/incidents"): {"raise": False, "status": 200, "payload": {"incidents": [{"id": 1}]}},
            (2, "/api/v1/event/{event_id}/incidents"): {"raise": False, "status": 200, "payload": {"incidents": [{"id": 2}]}},
            (3, "/api/v1/event/{event_id}/incidents"): {"raise": False, "status": 200, "payload": {"incidents": [{"id": 3}]}},
        })
        repo = _FakeRepo()
        registry = _FakeRegistry()

        executor = ProbeExecutor(
            database=db,
            client=client,
            repository=repo,
            registry=registry,
        )
        report = await executor.probe(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_patterns=("/api/v1/event/{event_id}/incidents",),
            samples_per_endpoint=5,
        )

        # Even though only 3 events available, executor uses all of them.
        self.assertEqual(report.samples_used, 3)
        self.assertEqual(len(client.calls), 3)  # 1 endpoint × 3 events


class ProbeExecutorAggregateTests(unittest.IsolatedAsyncioTestCase):
    async def test_executor_aggregates_and_writes_upserts(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            ProbeExecutor,
        )

        events = [{"id": i} for i in range(1, 6)]
        # 5/5 success for /incidents, 5/5 404 for /best-players.
        outcomes = {}
        for eid in (1, 2, 3, 4, 5):
            outcomes[(eid, "/api/v1/event/{event_id}/incidents")] = {
                "raise": False, "status": 200, "payload": {"incidents": [{"id": eid}]}}
            outcomes[(eid, "/api/v1/event/{event_id}/best-players/summary")] = {
                "raise": False, "status": 404, "payload": None}

        db = _FakeDatabase(events=events)
        client = _FakeClient(outcomes=outcomes)
        repo = _FakeRepo()
        registry = _FakeRegistry()

        executor = ProbeExecutor(
            database=db, client=client,
            repository=repo, registry=registry,
        )
        report = await executor.probe(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_patterns=(
                "/api/v1/event/{event_id}/incidents",
                "/api/v1/event/{event_id}/best-players/summary",
            ),
            samples_per_endpoint=5,
        )

        self.assertEqual(report.samples_used, 5)
        self.assertEqual(len(repo.upserts), 2)
        by_endpoint = {u.endpoint_pattern: u for u in repo.upserts}
        self.assertEqual(
            by_endpoint["/api/v1/event/{event_id}/incidents"].state, "allowed"
        )
        self.assertEqual(
            by_endpoint["/api/v1/event/{event_id}/best-players/summary"].state, "disabled"
        )


class ProbeExecutorErrorHandlingTests(unittest.IsolatedAsyncioTestCase):
    async def test_executor_classifies_exceptions_as_error_not_disabled(self) -> None:
        """Transient errors (TLS, 5xx, proxy) must classify as ERROR
        and push verdict to UNKNOWN — NOT to disabled. Otherwise one
        bad proxy round permanently kills an endpoint."""
        from schema_inspector.services.league_capabilities_probe import (
            ProbeExecutor,
        )

        events = [{"id": i} for i in range(1, 6)]
        outcomes = {}
        # 1 ok, 4 transient errors → unknown (not disabled).
        outcomes[(1, "/api/v1/event/{event_id}/lineups")] = {
            "raise": False, "status": 200,
            # Non-empty payload — real lineups have at least one player.
            "payload": {"home": {"players": [{"id": 1, "name": "p"}]}, "away": {}}}
        for eid in (2, 3, 4, 5):
            outcomes[(eid, "/api/v1/event/{event_id}/lineups")] = {
                "raise": True, "err": "TLS handshake failed"}

        db = _FakeDatabase(events=events)
        client = _FakeClient(outcomes=outcomes)
        repo = _FakeRepo()
        registry = _FakeRegistry()

        executor = ProbeExecutor(
            database=db, client=client,
            repository=repo, registry=registry,
        )
        await executor.probe(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_patterns=("/api/v1/event/{event_id}/lineups",),
            samples_per_endpoint=5,
        )

        self.assertEqual(len(repo.upserts), 1)
        verdict = repo.upserts[0]
        self.assertEqual(verdict.state, "unknown",
                         "4 transient errors must NOT classify as disabled")
        self.assertEqual(verdict.probe_samples_error, 4)
        self.assertEqual(verdict.probe_samples_ok, 1)


class ProbeExecutorCacheInvalidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_executor_invalidates_cache_after_upserts(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            ProbeExecutor,
        )

        events = [{"id": 1}]
        outcomes = {(1, "/x"): {"raise": False, "status": 200, "payload": {"data": [1]}}}

        db = _FakeDatabase(events=events)
        client = _FakeClient(outcomes=outcomes)
        repo = _FakeRepo()
        registry = _FakeRegistry()

        executor = ProbeExecutor(
            database=db, client=client,
            repository=repo, registry=registry,
        )
        await executor.probe(
            unique_tournament_id=17,
            season_id=61643,
            status_type="inprogress",
            endpoint_patterns=("/x",),
            samples_per_endpoint=5,
        )

        self.assertEqual(len(registry.invalidate_calls), 1)
        call = registry.invalidate_calls[0]
        self.assertEqual(call["unique_tournament_id"], 17)
        self.assertEqual(call["season_id"], 61643)
        self.assertEqual(call["status_type"], "inprogress")
        self.assertEqual(call["endpoint_patterns"], ("/x",))


class ProbeExecutorNoEventsTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_events_produces_unknown_upserts(self) -> None:
        """When DB has zero events for (UT, season, status), executor
        still writes UNKNOWN rows so the next refresh tick has
        something to look at (and we don't spin on the same empty
        sample selection)."""
        from schema_inspector.services.league_capabilities_probe import (
            ProbeExecutor,
        )

        db = _FakeDatabase(events=[])  # empty
        client = _FakeClient(outcomes={})
        repo = _FakeRepo()
        registry = _FakeRegistry()

        executor = ProbeExecutor(
            database=db, client=client,
            repository=repo, registry=registry,
        )
        report = await executor.probe(
            unique_tournament_id=999,
            season_id=42,
            status_type="finished",
            endpoint_patterns=("/api/v1/event/{event_id}/incidents",),
            samples_per_endpoint=5,
        )

        self.assertEqual(report.samples_used, 0)
        self.assertEqual(len(repo.upserts), 1)
        self.assertEqual(repo.upserts[0].state, "unknown")
        self.assertEqual(repo.upserts[0].probe_samples_total, 0)


class ProbeEndpointSetTests(unittest.TestCase):
    """Per-status endpoint sets — pin the probe scope so it doesn't
    grow uncontrolled."""

    def test_inprogress_set_includes_live_only_endpoints(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            PROBE_ENDPOINTS_BY_STATUS,
        )
        live = PROBE_ENDPOINTS_BY_STATUS["inprogress"]
        # Must have the always-on match-center surface.
        self.assertIn("/api/v1/event/{event_id}/incidents", live)
        self.assertIn("/api/v1/event/{event_id}/statistics", live)
        self.assertIn("/api/v1/event/{event_id}/lineups", live)
        self.assertIn("/api/v1/event/{event_id}/best-players/summary", live)

    def test_finished_set_includes_post_match_endpoints(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            PROBE_ENDPOINTS_BY_STATUS,
        )
        finished = PROBE_ENDPOINTS_BY_STATUS["finished"]
        self.assertIn("/api/v1/event/{event_id}/highlights", finished)

    def test_notstarted_set_excludes_live_only_endpoints(self) -> None:
        from schema_inspector.services.league_capabilities_probe import (
            PROBE_ENDPOINTS_BY_STATUS,
        )
        notstarted = PROBE_ENDPOINTS_BY_STATUS["notstarted"]
        # Live-only endpoints (best-players summary) should NOT be in
        # the notstarted probe set — they only make sense for matches
        # that have started.
        self.assertNotIn("/api/v1/event/{event_id}/best-players/summary", notstarted)


if __name__ == "__main__":
    unittest.main()
