from __future__ import annotations

import unittest

from schema_inspector.final_sweep_gate import FinalSweepGate


class FinalSweepGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_gate_applies_jitter_and_bounds_concurrency(self) -> None:
        sleeps: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)

        gate = FinalSweepGate(max_concurrency=1, jitter_seconds_factory=lambda: 3, sleep=fake_sleep)

        async def work():
            return "ok"

        result = await gate.run(work)

        self.assertEqual(result, "ok")
        self.assertEqual(sleeps, [3])


if __name__ == "__main__":
    unittest.main()
