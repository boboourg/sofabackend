# Production Cutover from tmux to systemd

## Goal

Move long-running Sofascore ETL services from ad-hoc `tmux` and `nohup` sessions to managed `systemd` units with automatic restart and predictable shutdown.

## Prerequisites

- Repository is updated on the server in `/opt/sofascore`
- Python environment exists at `/opt/sofascore/.venv`
- `.env` is present at `/opt/sofascore/.env`
- `redis-server.service` and `postgresql.service` are healthy

## Install Assets

```bash
cd /opt/sofascore
chmod +x deploy/run_service.sh
install -D -m 0644 ops/systemd/*.service /etc/systemd/system/
systemctl daemon-reload
```

## Inspect Existing tmux and Process State

```bash
tmux ls || true
pgrep -af "python -m schema_inspector.cli"
```

## Stop Legacy Sessions

Gracefully stop long-running legacy processes before enabling `systemd`.

```bash
tmux kill-server || true
pkill -INT -f "python -m schema_inspector.cli" || true
sleep 5
pgrep -af "python -m schema_inspector.cli" || true
```

## Enable Core Services

Start planners and workers in small batches so queue behavior stays easy to observe.

```bash
systemctl enable --now sofascore-planner.service
systemctl enable --now sofascore-live-discovery-planner.service
systemctl enable --now sofascore-historical-planner.service
systemctl enable --now sofascore-historical-tournament-planner.service
systemctl enable --now sofascore-structure-planner.service
systemctl enable --now sofascore-tournament-registry-refresh.service
systemctl enable --now sofascore-structure-sync.service

systemctl enable --now sofascore-discovery@1.service
systemctl enable --now sofascore-live-discovery@1.service
systemctl enable --now sofascore-hydrate@1.service
systemctl enable --now sofascore-live-hot@1.service
systemctl enable --now sofascore-live-warm@1.service
systemctl enable --now sofascore-historical-discovery@1.service
systemctl enable --now sofascore-historical-tournament@1.service
systemctl enable --now sofascore-historical-hydrate@1.service
systemctl enable --now sofascore-historical-enrichment@1.service
systemctl enable --now sofascore-maintenance@1.service
systemctl enable --now sofascore-historical-maintenance@1.service
```

Scale worker counts only after the first instances are healthy.

## Smoke Checks

```bash
systemctl status sofascore-planner.service --no-pager
systemctl status sofascore-historical-planner.service --no-pager
systemctl status sofascore-tournament-registry-refresh.service --no-pager
systemctl status 'sofascore-*.service' --no-pager

journalctl -u sofascore-planner.service -n 50 --no-pager
journalctl -u sofascore-historical-planner.service -n 50 --no-pager
journalctl -u sofascore-tournament-registry-refresh.service -n 50 --no-pager

curl http://127.0.0.1:8000/ops/health
curl http://127.0.0.1:8000/ops/queues/summary
```

## Rollback

If a cutover batch misbehaves, stop only the affected units and return to manual launch temporarily.

```bash
systemctl stop sofascore-planner.service
systemctl stop sofascore-historical-planner.service
systemctl stop sofascore-tournament-registry-refresh.service
systemctl stop 'sofascore-*@1.service'
```

After rollback, relaunch only the minimum manual services needed and inspect `journalctl` output before retrying the cutover.
