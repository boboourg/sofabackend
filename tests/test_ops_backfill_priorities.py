"""Tests for the /ops/backfill-priorities endpoint."""
from __future__ import annotations
import os
import tempfile
import textwrap
import unittest
from pathlib import Path

from schema_inspector.local_api_server import LocalApiApplication


def _write_yaml(text: str) -> str:
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8")
    f.write(textwrap.dedent(text))
    f.close()
    return f.name


class OpsBackfillPrioritiesEndpointTests(unittest.TestCase):
    def setUp(self) -> None:
        # Construct a bare LocalApiApplication — the endpoint reads a
        # file from disk and does not need a DB pool.
        self.app = LocalApiApplication.__new__(LocalApiApplication)

    def _set_path(self, path: str | None) -> None:
        if path is None:
            os.environ.pop("SOFASCORE_BACKFILL_PRIORITIES_PATH", None)
        else:
            os.environ["SOFASCORE_BACKFILL_PRIORITIES_PATH"] = path

    def tearDown(self) -> None:
        os.environ.pop("SOFASCORE_BACKFILL_PRIORITIES_PATH", None)

    def test_missing_file_reports_default_uniform(self) -> None:
        self._set_path("/tmp/definitely-not-here-12345.yaml")
        result = self.app._fetch_ops_backfill_priorities_payload()
        self.assertFalse(result["exists"])
        self.assertEqual(result["status"], "default_uniform")

    def test_valid_file_returns_config_with_share_pct(self) -> None:
        path = _write_yaml("""
            sport_weights:
              football: 4
              tennis: 1
            ut_boost:
              - ut_id: 17
                multiplier: 3.0
        """)
        self._set_path(path)
        result = self.app._fetch_ops_backfill_priorities_payload()
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["sport_weights"], {"football": 4.0, "tennis": 1.0})
        self.assertEqual(result["sport_share_pct"], {"football": 80.0, "tennis": 20.0})
        # ut_boost keys are stringified for JSON-safe transport.
        self.assertEqual(result["ut_boost"], {"17": 3.0})

    def test_malformed_file_reports_invalid_with_error(self) -> None:
        path = _write_yaml("""
            sport_weights:
              football: -1
        """)
        self._set_path(path)
        result = self.app._fetch_ops_backfill_priorities_payload()
        self.assertEqual(result["status"], "invalid")
        self.assertIn("must be >= 0", result["error"])

    def test_payload_schema_is_stable(self) -> None:
        path = _write_yaml("sport_weights: {}")
        self._set_path(path)
        result = self.app._fetch_ops_backfill_priorities_payload()
        for key in (
            "config_path",
            "exists",
            "status",
            "sport_weights",
            "sport_share_pct",
            "ut_boost",
            "sport_concurrency_caps",
        ):
            self.assertIn(key, result)


if __name__ == "__main__":
    unittest.main()
