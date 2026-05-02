from __future__ import annotations

import asyncio
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

    async def test_bounded_fanout_re_raises_unexpected_exception_after_other_tasks_finish(self) -> None:
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

        with self.assertRaisesRegex(RuntimeError, "boom"):
            await orchestrator._fetch_event_endpoint_specs_bounded(_specs(4), phase_name="test")

        self.assertEqual(set(seen), {"endpoint-0", "endpoint-1", "endpoint-2", "endpoint-3"})


def _parallel_orchestrator(*, max_inflight: int) -> PilotOrchestrator:
    orchestrator = PilotOrchestrator.__new__(PilotOrchestrator)
    orchestrator._fanout_max_inflight = max_inflight
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


if __name__ == "__main__":
    unittest.main()
