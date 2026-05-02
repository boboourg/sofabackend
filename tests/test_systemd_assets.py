from __future__ import annotations

from pathlib import Path
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


if __name__ == "__main__":
    unittest.main()
