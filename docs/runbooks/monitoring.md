# Monitoring runbook

Operational reference for the **N1 monitoring daemon**. Plan / design
lives in [`docs/N1_MONITORING_PLAN.md`](../N1_MONITORING_PLAN.md); this
file is for "what do I do *right now*?" situations.

## Quick reference

| Task | Command |
| --- | --- |
| Status | `sudo systemctl status sofascore-monitoring` |
| Restart | `sudo systemctl restart sofascore-monitoring` |
| Tail logs | `journalctl -u sofascore-monitoring -n 200 --no-pager` |
| Live logs | `journalctl -u sofascore-monitoring -f` |
| Smoke test channel | `/opt/sofascore/.venv/bin/python -m schema_inspector.cli monitoring-daemon --smoke-test` |
| Silence | `SOFASCORE_MONITORING_ENABLED=0` in `/opt/sofascore/.env` + restart |
| Re-enable | `SOFASCORE_MONITORING_ENABLED=1` + restart |
| Verify Telegram bot | `curl -s "https://api.telegram.org/bot$TOKEN/getMe"` |

## Architecture

```
sofascore-monitoring.service          ┌──── /ops/live-freshness  (SLO: hot-age, tier_1, success rate)
  │                                   │
  │  every 60s                        ├──── /ops/queues/summary  (XLEN per stream)
  ├─► fetch all signals ──────────────┤
  │                                   └──── /ops/jobs/runs       (failed, retry rate, no-jobs canary)
  │                                                              [opt-in: JOB_SIGNALS_ENABLED=1]
  │
  ├─► classify each: OK / WARN / CRIT
  │
  ├─► check Redis dedupe (HGET monitoring:dedupe)
  │
  ├─► render Telegram message  (plain text, no Markdown)
  │
  └─► POST api.telegram.org/bot<TOKEN>/sendMessage
        │
        └─► on 200: HSET monitoring:dedupe (signal:severity → epoch + count + first_sent)
```

## Severity levels

| Level | Telegram | Repeat behavior |
| --- | --- | --- |
| OK | none | Sends `RESOLVED` if previous tick was WARN/CRIT |
| WARN | one message | Suppressed for `SOFASCORE_MONITORING_DEDUPE_WARN_TTL_SECONDS` (default 600s) |
| CRIT | repeat each TTL | Suppressed for `SOFASCORE_MONITORING_DEDUPE_CRIT_TTL_SECONDS` (default 1800s) |

WARN→CRIT escalation always sends — anti-snooze.

## Configured signals

### Phase 1 — SLO (always on)

| Signal | Source | Default WARN / CRIT |
| --- | --- | --- |
| `oldest_hot_score_age_seconds` | `/ops/live-freshness` | 120s / 300s |
| `tier_1_blocked_rate_cumulative` | `/ops/live-freshness` | 0.20 / 0.50 |
| `refresh_live_event_success_rate_5min` | `/ops/live-freshness` | <0.95 / <0.85 (inverted) |

### Phase 2 — Queue lengths (always on)

| Signal | Stream | Default WARN / CRIT |
| --- | --- | --- |
| `hydrate_xlen` | `stream:etl:hydrate` | 1000 / 5000 |
| `live_hot_xlen` | `stream:etl:live_hot` | 500 / 2000 |
| `live_warm_xlen` | `stream:etl:live_warm` | 500 / 2000 |
| `live_discovery_xlen` | `stream:etl:live_discovery` | 200 / 1000 |
| `discovery_xlen` | `stream:etl:discovery` | 200 / 1000 |

### Phase 3 — Job signals (opt-in)

Requires the BRIN index from
`migrations/2026-05-14_etl_job_run_started_at_index.sql`. Enable with:

```bash
echo "SOFASCORE_MONITORING_JOB_SIGNALS_ENABLED=1" >> /opt/sofascore/.env
sudo systemctl restart sofascore-monitoring
```

| Signal | Default WARN / CRIT |
| --- | --- |
| `failed_jobs_15min` | 20 / 50 |
| `retry_rate_15min` | 0.02 / 0.05 |
| `no_recent_jobs_age_seconds` | 300 / 600 |

## Tuning thresholds

Tune by editing `/opt/sofascore/.env` then `systemctl restart sofascore-monitoring`.

```bash
# Raise hot-age WARN to 5 min if 120s baseline is too tight:
SOFASCORE_MONITORING_OLDEST_HOT_AGE_WARN_SECONDS=300
SOFASCORE_MONITORING_OLDEST_HOT_AGE_CRIT_SECONDS=600

# Loosen hydrate queue thresholds during peak hours:
SOFASCORE_MONITORING_HYDRATE_XLEN_WARN=2500
SOFASCORE_MONITORING_HYDRATE_XLEN_CRIT=10000
```

The full env table lives in `MonitoringConfig.from_env` (see
`schema_inspector/monitoring/config.py`) — every numeric field is
overridable.

## How to add a new signal

1. Add a `SignalDefinition` to `schema_inspector/monitoring/signals.py`.
2. Add the corresponding env knobs to `MonitoringConfig` (warn + crit).
3. If the source is HTTP, extend `fetch_*_signals_from_api` in
   `schema_inspector/monitoring/signal_source.py`.
4. Wire it into `_run_monitoring_daemon` in `schema_inspector/cli.py`
   (overrides dict + signal source call).
5. Add tests under `tests/test_monitoring_*.py`.
6. Document the new signal in this runbook + plan.

## Common situations

### "Got a 400 chat not found from Telegram"

The user (or group) never sent the bot a message. Open the bot in
Telegram (`@flowscore_monitoring_bot` on prod) and send `/start`. Then
re-run `--smoke-test`.

### "Daemon crash-looping in journalctl"

Check the error class:

* `ImportError` → run `pip install -r requirements.txt` (httpx pinned to
  0.28.1).
* `ConnectionRefused` to `127.0.0.1:8000` → API down: `systemctl status
  sofascore-api`.
* `redis ConnectionError` → daemon falls back to NullDedupeStore but
  still runs. Check Redis: `systemctl status redis-server`.

### "Telegram channel is flooded"

* Increase WARN TTL: `SOFASCORE_MONITORING_DEDUPE_WARN_TTL_SECONDS=1800`.
* Or temporarily disable: `SOFASCORE_MONITORING_ENABLED=0`.
* Tune the noisy signal's threshold UP if the breach is below operator
  pain threshold.

### "Bot stopped sending — am I receiving alerts at all?"

* Check daemon status: `systemctl status sofascore-monitoring`.
* Run smoke test:
  ```bash
  /opt/sofascore/.venv/bin/python -m schema_inspector.cli monitoring-daemon --smoke-test
  ```
* Check Telegram getUpdates:
  ```bash
  TOKEN=$(grep -oP "SOFASCORE_MONITORING_TELEGRAM_BOT_TOKEN=\K.*" /opt/sofascore/.env)
  curl -s "https://api.telegram.org/bot${TOKEN}/getUpdates"
  ```

### "I need to rotate the bot token"

* `@BotFather` → `/revoke` → new token.
* Edit `/opt/sofascore/.env`:
  `SOFASCORE_MONITORING_TELEGRAM_BOT_TOKEN=<new token>`
* `sudo systemctl restart sofascore-monitoring`.

## Related

- [`docs/N1_MONITORING_PLAN.md`](../N1_MONITORING_PLAN.md) — design + roadmap
- [`docs/PROJECT_VISION.md`](../PROJECT_VISION.md) — why monitoring matters
- [`docs/ARCHITECTURE_AUDIT.md`](../ARCHITECTURE_AUDIT.md) D.3 — etl_job_run index history
