from __future__ import annotations

import unittest

from schema_inspector.fetch_models import FetchOutcomeEnvelope
from schema_inspector.pipeline.pilot_orchestrator import CapabilityRollupAccumulator


class CapabilityRollupTests(unittest.TestCase):
    def test_rollup_accumulates_supported_and_unsupported_outcomes(self) -> None:
        accumulator = CapabilityRollupAccumulator(sport_slug="football", endpoint_pattern="/api/v1/event/{event_id}/statistics")

        supported = accumulator.observe(
            FetchOutcomeEnvelope(
                trace_id="trace-1",
                job_id="job-1",
                endpoint_pattern="/api/v1/event/{event_id}/statistics",
                source_url="https://www.sofascore.com/api/v1/event/1/statistics",
                resolved_url="https://www.sofascore.com/api/v1/event/1/statistics",
                http_status=200,
                classification="success_json",
                proxy_id="proxy_1",
                challenge_reason=None,
                snapshot_id=1,
                payload_hash="hash-1",
                payload_root_keys=("statistics",),
                is_valid_json=True,
                is_empty_payload=False,
                is_soft_error_payload=False,
                retry_recommended=False,
                capability_signal="supported",
                fetched_at="2026-04-16T12:00:00+00:00",
            )
        )
        unsupported = accumulator.observe(
            FetchOutcomeEnvelope(
                trace_id="trace-2",
                job_id="job-2",
                endpoint_pattern="/api/v1/event/{event_id}/statistics",
                source_url="https://www.sofascore.com/api/v1/event/1/statistics",
                resolved_url="https://www.sofascore.com/api/v1/event/1/statistics",
                http_status=404,
                classification="not_found",
                proxy_id="proxy_1",
                challenge_reason=None,
                snapshot_id=2,
                payload_hash="hash-2",
                payload_root_keys=("error",),
                is_valid_json=True,
                is_empty_payload=False,
                is_soft_error_payload=False,
                retry_recommended=False,
                capability_signal="unsupported",
                fetched_at="2026-04-16T12:05:00+00:00",
            )
        )

        self.assertEqual(supported.support_level, "supported")
        self.assertEqual(supported.success_count, 1)
        self.assertEqual(unsupported.support_level, "conditionally_supported")
        self.assertEqual(unsupported.not_found_count, 1)


if __name__ == "__main__":
    unittest.main()
