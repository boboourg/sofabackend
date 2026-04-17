from __future__ import annotations

import unittest

from schema_inspector.jobs.envelope import JobEnvelope
from schema_inspector.jobs.types import (
    JOB_DISCOVER_SPORT_SURFACE,
    JOB_HYDRATE_EVENT_EDGE,
)


class JobEnvelopeTests(unittest.TestCase):
    def test_idempotency_key_is_stable_for_same_identity(self) -> None:
        left = JobEnvelope.create(
            job_type=JOB_DISCOVER_SPORT_SURFACE,
            sport_slug="football",
            entity_type="sport",
            entity_id=None,
            scope="scheduled",
            params={"date": "2026-04-16", "page": 1},
            priority=2,
            trace_id="trace-1",
        )
        right = JobEnvelope.create(
            job_type=JOB_DISCOVER_SPORT_SURFACE,
            sport_slug="football",
            entity_type="sport",
            entity_id=None,
            scope="scheduled",
            params={"page": 1, "date": "2026-04-16"},
            priority=1,
            trace_id="trace-2",
        )

        self.assertEqual(left.idempotency_key, right.idempotency_key)

    def test_child_job_preserves_trace_and_parent_link(self) -> None:
        parent = JobEnvelope.create(
            job_type=JOB_DISCOVER_SPORT_SURFACE,
            sport_slug="basketball",
            entity_type="sport",
            entity_id=None,
            scope="scheduled",
            params={"date": "2026-04-16"},
            priority=2,
            trace_id="trace-1",
        )

        child = parent.spawn_child(
            job_type=JOB_HYDRATE_EVENT_EDGE,
            entity_type="event",
            entity_id=14439306,
            scope="live",
            params={"edge_kind": "statistics"},
            priority=0,
        )

        self.assertEqual(child.parent_job_id, parent.job_id)
        self.assertEqual(child.trace_id, parent.trace_id)
        self.assertEqual(child.entity_id, 14439306)
        self.assertEqual(child.params["edge_kind"], "statistics")


if __name__ == "__main__":
    unittest.main()
