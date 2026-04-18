from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


def _load_script_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "prod_readiness_check.py"
    spec = importlib.util.spec_from_file_location("prod_readiness_check", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load script from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ProdReadinessCheckTests(unittest.TestCase):
    def test_evaluate_readiness_reports_ready_when_live_gates_hold(self) -> None:
        module = _load_script_module()

        report = module.evaluate_readiness(
            queue_summary={
                "streams": [
                    {"stream": "stream:etl:discovery", "lag": 0, "entries_read": 100},
                    {"stream": "stream:etl:live_discovery", "lag": 0, "entries_read": 100},
                    {"stream": "stream:etl:hydrate", "lag": 25, "entries_read": 500},
                    {"stream": "stream:etl:live_hot", "lag": 15, "entries_read": 300},
                    {"stream": "stream:etl:historical_hydrate", "lag": 999, "entries_read": 200},
                ],
                "delayed_total": 5,
                "delayed_due": 0,
                "live_lanes": {"hot": 8, "warm": 22, "cold": 67},
            },
            job_runs_payload={
                "status_counts": {"failed": 11, "retry_scheduled": 50, "succeeded": 1000},
                "jobRuns": [
                    {"status": "succeeded", "job_type": "hydrate_event_root"},
                    {"status": "retry_scheduled", "job_type": "discover_sport_surface"},
                ],
            },
            timeout_detected=False,
        )

        self.assertTrue(report.ready)
        self.assertEqual(report.reasons, [])

    def test_evaluate_readiness_rejects_when_discovery_not_drained(self) -> None:
        module = _load_script_module()

        report = module.evaluate_readiness(
            queue_summary={
                "streams": [
                    {"stream": "stream:etl:discovery", "lag": 7, "entries_read": 100},
                    {"stream": "stream:etl:live_discovery", "lag": 0, "entries_read": 100},
                    {"stream": "stream:etl:hydrate", "lag": 25, "entries_read": 500},
                    {"stream": "stream:etl:live_hot", "lag": 15, "entries_read": 300},
                ],
                "delayed_total": 0,
                "delayed_due": 0,
                "live_lanes": {"hot": 4, "warm": 10, "cold": 67},
            },
            job_runs_payload={
                "status_counts": {"failed": 11, "retry_scheduled": 50, "succeeded": 1000},
                "jobRuns": [{"status": "succeeded", "job_type": "hydrate_event_root"}],
            },
            timeout_detected=False,
        )

        self.assertFalse(report.ready)
        self.assertTrue(any("discovery" in reason for reason in report.reasons))

    def test_evaluate_readiness_rejects_recent_failed_jobs(self) -> None:
        module = _load_script_module()

        report = module.evaluate_readiness(
            queue_summary={
                "streams": [
                    {"stream": "stream:etl:discovery", "lag": 0, "entries_read": 100},
                    {"stream": "stream:etl:live_discovery", "lag": 0, "entries_read": 100},
                    {"stream": "stream:etl:hydrate", "lag": 25, "entries_read": 500},
                    {"stream": "stream:etl:live_hot", "lag": 15, "entries_read": 300},
                ],
                "delayed_total": 0,
                "delayed_due": 0,
                "live_lanes": {"hot": 4, "warm": 10, "cold": 67},
            },
            job_runs_payload={
                "status_counts": {"failed": 12, "retry_scheduled": 50, "succeeded": 1000},
                "jobRuns": [
                    {"status": "failed", "job_type": "hydrate_event_root"},
                    {"status": "succeeded", "job_type": "hydrate_event_root"},
                ],
            },
            timeout_detected=False,
        )

        self.assertFalse(report.ready)
        self.assertTrue(any("failed" in reason for reason in report.reasons))

    def test_scan_logs_detects_timeout_signature(self) -> None:
        module = _load_script_module()

        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "hydrate.log"
            log_path.write_text("[2026-04-18] ERROR TimeoutError asyncpg timeout\n", encoding="utf-8")

            self.assertTrue(module.scan_logs_for_timeout((str(log_path),)))

    def test_scan_logs_ignores_missing_files(self) -> None:
        module = _load_script_module()

        self.assertFalse(module.scan_logs_for_timeout(("Z:/definitely/missing.log",)))


if __name__ == "__main__":
    unittest.main()
