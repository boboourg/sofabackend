import unittest
from unittest import mock

from schema_inspector.fetch_models import FetchTask


class FetchTaskTimeoutTests(unittest.TestCase):
    def test_default_timeout_can_be_configured_from_environment(self):
        with mock.patch.dict("os.environ", {"SOFASCORE_FETCH_TIMEOUT_SECONDS": "10"}):
            task = FetchTask(
                trace_id="trace",
                job_id="job",
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}",
                source_url="https://www.sofascore.com/api/v1/event/1",
                timeout_profile="pilot",
            )

        self.assertEqual(10.0, task.timeout_seconds)

