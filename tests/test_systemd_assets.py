from __future__ import annotations

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
RUN_SERVICE = ROOT / "deploy" / "run_service.sh"
RUNBOOK = ROOT / "docs" / "runbooks" / "prod-cutover.md"
SYSTEMD_DIR = ROOT / "ops" / "systemd"


REQUIRED_ETL_UNITS = {
    "sofascore-planner.service": "planner-daemon",
    "sofascore-live-discovery-planner.service": "live-discovery-planner-daemon",
    "sofascore-historical-planner.service": "historical-planner-daemon",
    "sofascore-historical-tournament-planner.service": "historical-tournament-planner-daemon",
    "sofascore-structure-planner.service": "structure-planner-daemon",
    "sofascore-tournament-registry-refresh.service": "tournament-registry-refresh-daemon",
    "sofascore-discovery@.service": "worker-discovery --consumer-name discovery-%i --block-ms 5000",
    "sofascore-live-discovery@.service": "worker-live-discovery --consumer-name live-discovery-%i --block-ms 5000",
    "sofascore-hydrate@.service": "worker-hydrate --consumer-name hydrate-%i --block-ms 5000",
    "sofascore-live-hot@.service": "worker-live-hot --consumer-name live-hot-%i --block-ms 5000",
    "sofascore-live-warm@.service": "worker-live-warm --consumer-name live-warm-%i --block-ms 5000",
    "sofascore-historical-discovery@.service": "worker-historical-discovery --consumer-name historical-discovery-%i --block-ms 5000",
    "sofascore-historical-tournament@.service": "worker-historical-tournament --consumer-name historical-tournament-%i --block-ms 5000",
    "sofascore-historical-hydrate@.service": "worker-historical-hydrate --consumer-name historical-hydrate-%i --block-ms 5000",
    "sofascore-historical-enrichment@.service": "worker-historical-enrichment --consumer-name historical-enrichment-%i --block-ms 5000",
    "sofascore-maintenance@.service": "worker-maintenance --consumer-name maintenance-%i --block-ms 5000",
    "sofascore-historical-maintenance@.service": "worker-historical-maintenance --consumer-name historical-maintenance-%i --block-ms 5000",
    "sofascore-structure-sync.service": "worker-structure-sync --consumer-name structure-sync-1 --block-ms 5000",
    "sofascore-resource-planner.service": "resource-planner-daemon --loop-interval-seconds 30 --publish-per-tick-cap 200 --lag-threshold 5000",
    "sofascore-resource-refresh@.service": "worker-resource-refresh --consumer-name resource-refresh-%i --block-ms 5000",
    "sofascore-normalize@.service": "worker-normalize --consumer-name normalize-%i --block-ms 5000",
}


class SystemdAssetTests(unittest.TestCase):
    def test_run_service_script_loads_env_and_execs_cli(self) -> None:
        script = RUN_SERVICE.read_text(encoding="utf-8")

        self.assertIn("source .venv/bin/activate", script)
        self.assertIn("source .env", script)
        self.assertIn('exec python -m schema_inspector.cli "$@"', script)

    def test_required_etl_units_exist(self) -> None:
        for filename in REQUIRED_ETL_UNITS:
            self.assertTrue((SYSTEMD_DIR / filename).exists(), filename)

    def test_etl_units_use_common_wrapper_and_restart_policy(self) -> None:
        for filename, command_suffix in REQUIRED_ETL_UNITS.items():
            text = (SYSTEMD_DIR / filename).read_text(encoding="utf-8")
            self.assertIn("WorkingDirectory=/opt/sofascore", text, filename)
            self.assertIn("Restart=always", text, filename)
            self.assertIn("RestartSec=5", text, filename)
            self.assertIn("KillSignal=SIGINT", text, filename)
            expected_timeout = (
                "TimeoutStopSec=90"
                if filename in {"sofascore-hydrate@.service", "sofascore-historical-hydrate@.service"}
                else "TimeoutStopSec=30"
            )
            self.assertIn(expected_timeout, text, filename)
            self.assertIn("After=network-online.target redis-server.service postgresql.service", text, filename)
            self.assertIn("Wants=network-online.target", text, filename)
            self.assertIn(f"ExecStart=/opt/sofascore/deploy/run_service.sh {command_suffix}", text, filename)

    def test_cutover_runbook_covers_tmux_shutdown_and_systemctl_enable(self) -> None:
        text = RUNBOOK.read_text(encoding="utf-8")

        self.assertIn("tmux", text)
        self.assertIn("pkill -INT -f", text)
        self.assertIn("systemctl daemon-reload", text)
        self.assertIn("systemctl enable --now", text)
        self.assertIn("journalctl", text)


class CapabilityOpsAssetTests(unittest.TestCase):
    REFRESH = ROOT / "scripts" / "ops" / "capability_probe_refresh.sh"
    PRIME_SERVICE = SYSTEMD_DIR / "sofascore-capability-prime.service"
    PRIME_TIMER = SYSTEMD_DIR / "sofascore-capability-prime.timer"

    def test_capability_probe_refresh_uses_venv_python(self) -> None:
        script = self.REFRESH.read_text(encoding="utf-8")
        self.assertIn(
            "/opt/sofascore/.venv/bin/python -m schema_inspector.cli league-capability probe",
            script,
        )
        self.assertIn(
            "/opt/sofascore/.venv/bin/python -m schema_inspector.cli league-capability prime-redis",
            script,
        )

    def test_capability_probe_refresh_has_no_bare_python_invocation(self) -> None:
        # Regression guard: a bare `python -m schema_inspector.cli` (not
        # prefixed by a venv path) silently fails under systemd.
        script = self.REFRESH.read_text(encoding="utf-8")
        bare = re.search(r"(?<![\w./-])python -m schema_inspector\.cli", script)
        self.assertIsNone(
            bare,
            "capability_probe_refresh.sh must call the venv python, not bare `python`",
        )

    def test_capability_prime_units_exist_and_use_venv_python(self) -> None:
        self.assertTrue(self.PRIME_SERVICE.exists(), self.PRIME_SERVICE.name)
        self.assertTrue(self.PRIME_TIMER.exists(), self.PRIME_TIMER.name)
        svc = self.PRIME_SERVICE.read_text(encoding="utf-8")
        self.assertIn("Type=oneshot", svc)
        self.assertIn("EnvironmentFile=/opt/sofascore/.env", svc)
        self.assertIn(
            "ExecStart=/opt/sofascore/.venv/bin/python -m schema_inspector.cli league-capability prime-redis",
            svc,
        )
        tmr = self.PRIME_TIMER.read_text(encoding="utf-8")
        self.assertIn("OnCalendar=*-*-* *:15:00", tmr)
        self.assertIn("Unit=sofascore-capability-prime.service", tmr)


if __name__ == "__main__":
    unittest.main()
