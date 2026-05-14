from __future__ import annotations

import asyncio
import types
import unittest

from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator, _EventEndpointFetchSpec


class PilotOrchestratorParallelFanoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_fanout_respects_max_inflight(self) -> None:
        orchestrator = _parallel_orchestrator(max_inflight=4)
        active = 0
        max_active = 0

        async def fake_fetch(**kwargs):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return kwargs["endpoint"], None

        orchestrator._fetch_gated_event_endpoint = fake_fetch

        results = await orchestrator._fetch_event_endpoint_specs_bounded(_specs(8), phase_name="test")

        self.assertEqual(len(results), 8)
        self.assertEqual(max_active, 4)

    async def test_bounded_fanout_max_one_is_strictly_sequential(self) -> None:
        orchestrator = _parallel_orchestrator(max_inflight=1)
        active = 0
        max_active = 0
        seen: list[str] = []

        async def fake_fetch(**kwargs):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            seen.append(kwargs["endpoint"])
            await asyncio.sleep(0.01)
            active -= 1
            return kwargs["endpoint"], None

        orchestrator._fetch_gated_event_endpoint = fake_fetch

        results = await orchestrator._fetch_event_endpoint_specs_bounded(_specs(4), phase_name="test")

        self.assertEqual([outcome for outcome, _ in results], ["endpoint-0", "endpoint-1", "endpoint-2", "endpoint-3"])
        self.assertEqual(seen, ["endpoint-0", "endpoint-1", "endpoint-2", "endpoint-3"])
        self.assertEqual(max_active, 1)

    async def test_bounded_fanout_swallows_unexpected_exception_after_other_tasks_finish(self) -> None:
        # Fix #1 (2026-05-15): a single sub-endpoint exception must NOT
        # abort the whole event run. Failed sub becomes a (None, None)
        # placeholder in the results, the rest finish normally. Previously
        # this test asserted the opposite (re-raise) — that was the
        # original cause of "tier-1 subs empty" in production, since one
        # transient parse error torpedoed all 24 sibling sub-endpoints.
        orchestrator = _parallel_orchestrator(max_inflight=4)
        seen: list[str] = []

        async def fake_fetch(**kwargs):
            endpoint = kwargs["endpoint"]
            seen.append(endpoint)
            await asyncio.sleep(0.01)
            if endpoint == "endpoint-1":
                raise RuntimeError("boom")
            return endpoint, None

        orchestrator._fetch_gated_event_endpoint = fake_fetch

        results = await orchestrator._fetch_event_endpoint_specs_bounded(_specs(4), phase_name="test")

        self.assertEqual(set(seen), {"endpoint-0", "endpoint-1", "endpoint-2", "endpoint-3"})
        # All 4 specs produced a result slot (placeholder for the failed one).
        self.assertEqual(len(results), 4)
        # The 3 healthy specs return (endpoint, None); the failed one becomes (None, None).
        none_slots = [i for i, (outcome, _) in enumerate(results) if outcome is None]
        self.assertEqual(len(none_slots), 1)

    async def test_sequential_fanout_swallows_per_spec_exception(self) -> None:
        # Fix #1: sequential mode (max_inflight=1) must also handle a
        # per-spec exception gracefully. Previously raised straight out
        # of the for-loop and aborted the whole event run.
        orchestrator = _parallel_orchestrator(max_inflight=1)
        seen: list[str] = []

        async def fake_fetch(**kwargs):
            endpoint = kwargs["endpoint"]
            seen.append(endpoint)
            if endpoint == "endpoint-2":
                raise RuntimeError("seq boom")
            return endpoint, None

        orchestrator._fetch_gated_event_endpoint = fake_fetch

        results = await orchestrator._fetch_event_endpoint_specs_bounded(_specs(4), phase_name="test")

        # All four were attempted (no break on first exception)
        self.assertEqual(seen, ["endpoint-0", "endpoint-1", "endpoint-2", "endpoint-3"])
        self.assertEqual(len(results), 4)
        self.assertEqual(results[0], ("endpoint-0", None))
        self.assertEqual(results[1], ("endpoint-1", None))
        self.assertEqual(results[2], (None, None))  # placeholder for raised exception
        self.assertEqual(results[3], ("endpoint-3", None))

    async def test_event_endpoint_gate_decisions_are_serialized_before_parallel_fetches(self) -> None:
        orchestrator = _parallel_orchestrator(max_inflight=4)
        gate = _ConcurrencyTrackingGate()
        orchestrator.event_endpoint_gate = gate
        orchestrator.freshness_store = None
        fetch_active = 0
        fetch_max_active = 0

        async def fake_fetch_and_parse(**kwargs):
            nonlocal fetch_active, fetch_max_active
            fetch_active += 1
            fetch_max_active = max(fetch_max_active, fetch_active)
            await asyncio.sleep(0.05)
            fetch_active -= 1
            return types.SimpleNamespace(classification="success_json"), None

        orchestrator._fetch_and_parse = fake_fetch_and_parse

        await orchestrator._fetch_event_endpoint_specs_bounded(_real_endpoint_specs(4), phase_name="test")

        self.assertEqual(gate.max_active_decisions, 1)
        self.assertGreater(fetch_max_active, 1)


def _parallel_orchestrator(*, max_inflight: int) -> PilotOrchestrator:
    orchestrator = PilotOrchestrator.__new__(PilotOrchestrator)
    orchestrator._fanout_max_inflight = max_inflight
    orchestrator._event_endpoint_gate_decision_lock = asyncio.Lock()
    return orchestrator


def _specs(count: int) -> list[_EventEndpointFetchSpec]:
    return [
        _EventEndpointFetchSpec(
            endpoint=f"endpoint-{index}",
            sport_slug="football",
            path_params={"event_id": 123},
            context_entity_type="event",
            context_entity_id=123,
            context_event_id=123,
            fetch_reason="hydrate_event_edge",
            status_phase="live",
        )
        for index in range(count)
    ]


def _real_endpoint_specs(count: int) -> list[_EventEndpointFetchSpec]:
    specs = _specs(count)
    return [
        _EventEndpointFetchSpec(
            endpoint=types.SimpleNamespace(pattern=f"/api/v1/event/{{event_id}}/test-{index}"),
            sport_slug=spec.sport_slug,
            path_params=spec.path_params,
            context_entity_type=spec.context_entity_type,
            context_entity_id=spec.context_entity_id,
            context_event_id=spec.context_event_id,
            fetch_reason=spec.fetch_reason,
            status_phase=spec.status_phase,
        )
        for index, spec in enumerate(specs)
    ]


class _ConcurrencyTrackingGate:
    def __init__(self) -> None:
        self.active_decisions = 0
        self.max_active_decisions = 0
        self.records = []

    async def decide_event_probe(self, **kwargs):
        self.active_decisions += 1
        self.max_active_decisions = max(self.max_active_decisions, self.active_decisions)
        await asyncio.sleep(0.001)
        self.active_decisions -= 1
        return types.SimpleNamespace(
            should_fetch=True,
            endpoint_pattern=kwargs["endpoint_pattern"],
            decision_type="probe",
            classification_before=None,
            event_id=kwargs["event_id"],
            status_phase=kwargs["status_phase"],
        )

    async def record_event_outcome(self, **kwargs) -> None:
        self.records.append(kwargs)


if __name__ == "__main__":
    unittest.main()
