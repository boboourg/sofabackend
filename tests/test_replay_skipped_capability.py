import unittest

from schema_inspector.fetch_models import FetchOutcomeEnvelope
from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator


class ReplaySkippedCapabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_replay_skipped_missing_is_not_recorded_as_capability_observation(self):
        orchestrator = PilotOrchestrator.__new__(PilotOrchestrator)
        orchestrator.capability_repository = object()
        orchestrator._pending_capability_records = []
        orchestrator._rollups = {}

        await orchestrator._record_capability(
            sport_slug="football",
            outcome=FetchOutcomeEnvelope(
                trace_id="trace",
                job_id="job",
                endpoint_pattern="/api/v1/event/{event_id}/player/{player_id}/heatmap",
                source_url="https://www.sofascore.com/api/v1/event/1/player/2/heatmap",
                resolved_url=None,
                http_status=None,
                classification="replay_skipped_missing",
                proxy_id=None,
                challenge_reason=None,
                snapshot_id=None,
                payload_hash=None,
                fetched_at=None,
            ),
            context_type="player",
        )

        self.assertEqual([], orchestrator._pending_capability_records)

