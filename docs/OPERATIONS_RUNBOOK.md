# Operations runbook

Практический playbook для прод-операций. Все команды read-only по умолчанию помечены **✅ safe**, mutation команды — **⚠️ MUTATION**.

Доступы к проду (см. CLAUDE.md):
- SSH alias: `sofascore-prod`
- Repo: `/opt/sofascore`
- Python: `/opt/sofascore/.venv/bin/python`
- PostgreSQL: `localhost:5432`, user `sofascore_user`, db `sofascore_schema_inspector` (пароль в `.env`)
- Redis: `redis://127.0.0.1:6379/0` (пароль в `.env`)
- API: `http://127.0.0.1:8000`

---

## Quick reference: read-only health checks

### ✅ /ops/health
```bash
ssh sofascore-prod 'curl -s http://127.0.0.1:8000/ops/health | python3 -m json.tool'
```
Возвращает: `database_ok`, `redis_ok`, `snapshot_count`, `capability_rollup_count`, `live_hot_count`, `live_warm_count`, `live_cold_count`, `drift_summary`, `coverage_summary`, `reconcile_policy_summary`, `queue_summary`.

**⚠️ Known**: иногда возвращает 500 при high DB load (TimeoutError). Не firebreak issue, pre-existing. Если 500 — fallback на отдельные endpoints (`/ops/queues/summary`, `/ops/snapshots/summary`).

### ✅ /ops/queues/summary
```bash
ssh sofascore-prod 'curl -s http://127.0.0.1:8000/ops/queues/summary | python3 -m json.tool'
```
Streams lag, consumer group sizes, `live_dispatch_metrics`. **Не зависит от DB** — работает даже при DB timeout.

### ✅ /ops/jobs/runs
```bash
ssh sofascore-prod 'curl -s "http://127.0.0.1:8000/ops/jobs/runs?limit=50" | python3 -m json.tool'
```
Последние 50 etl_job_run.

### ✅ Compact CLI health
```bash
ssh sofascore-prod '/opt/sofascore/.venv/bin/python -m schema_inspector.cli health'
```

---

## Сценарий 1: проверить общее состояние прода

```bash
# 1. Сервисы все active?
ssh sofascore-prod 'systemctl list-units "sofascore-*" --type=service --state=running --no-pager | wc -l'

# 2. Очереди не перегружены?
ssh sofascore-prod 'curl -s http://127.0.0.1:8000/ops/queues/summary | python3 -c "
import json,sys
d=json.load(sys.stdin)
for s in d[\"streams\"]:
    if s[\"lag\"] > 1000:
        print(f\"  {s[\"stream\"]:35} lag={s[\"lag\"]}\")
"'

# 3. Recent job failures
ssh sofascore-prod 'cd /opt/sofascore && PGPASSWORD=$(grep -oP "postgresql://[^:]+:\K[^@]+" .env) psql -U sofascore_user -d sofascore_schema_inspector -h localhost -p 5432 -c "
SELECT status, COUNT(*) FROM etl_job_run
WHERE started_at >= NOW() - INTERVAL '"'"'15 min'"'"'
GROUP BY status ORDER BY COUNT(*) DESC;
"'

# 4. Active live counts
ssh sofascore-prod 'redis-cli -a "<REDIS_PW>" --no-auth-warning ZCARD zset:live:hot'
ssh sofascore-prod 'redis-cli -a "<REDIS_PW>" --no-auth-warning ZCARD zset:live:warm'
```

---

## Сценарий 2: проверить live coverage

### Сколько football live events у нас vs upstream
```bash
# local
ssh sofascore-prod 'curl -s "http://127.0.0.1:8000/api/v1/sport/football/events/live" | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(f\"local football live: {len(d.get(\\\"events\\\") or [])}\")
"'

# upstream — через diagnostic probe (см. reports/diff_matchcenter.py)
```

### Sample matchcenter endpoints для конкретного event
```bash
EID=14083629  # пример
for ep in lineups statistics incidents managers h2h pregame-form best-players/summary; do
    code=$(ssh sofascore-prod "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:8000/api/v1/event/$EID/$ep")
    echo "$ep → $code"
done
```

### Stale live events
```bash
ssh sofascore-prod '/opt/sofascore/.venv/bin/python -m schema_inspector.cli stale-live-events --threshold-seconds 300 --top 20'
```

---

## Сценарий 3: проверить deadlocks / locks

### Currently blocked locks
```bash
ssh sofascore-prod 'cd /opt/sofascore && PGPASSWORD=$(grep -oP "postgresql://[^:]+:\K[^@]+" .env) psql -U sofascore_user -d sofascore_schema_inspector -h localhost -p 5432 -c "
SELECT pg_class.relname, pg_locks.mode, pg_locks.granted, pid, wait_event_type, wait_event
FROM pg_locks
JOIN pg_stat_activity ON pg_stat_activity.pid = pg_locks.pid
JOIN pg_class ON pg_locks.relation = pg_class.oid
WHERE pg_class.relname IN ('"'"'endpoint_capability_rollup'"'"','"'"'country'"'"','"'"'event'"'"','"'"'tournament'"'"')
  AND NOT pg_locks.granted
ORDER BY pid LIMIT 20;
"'
```

### DeadlockDetectedError trend
```bash
ssh sofascore-prod 'cd /opt/sofascore && PGPASSWORD=$(grep -oP "postgresql://[^:]+:\K[^@]+" .env) psql -U sofascore_user -d sofascore_schema_inspector -h localhost -p 5432 -c "
SELECT DATE_TRUNC('"'"'hour'"'"', started_at) AS hour, job_type, COUNT(*)
FROM etl_job_run
WHERE error_class = '"'"'DeadlockDetectedError'"'"' AND started_at >= NOW() - INTERVAL '"'"'6 hours'"'"'
GROUP BY 1,2 ORDER BY 1 DESC, COUNT(*) DESC LIMIT 30;
"'
```

### PostgreSQL deadlock messages (`/var/log/postgresql/`)
```bash
ssh sofascore-prod 'sudo grep "deadlock detected" /var/log/postgresql/postgresql-16-main.log | tail -20'
```

---

## Сценарий 4: проверить missing snapshots

### Сколько snapshots для конкретного event
```bash
EID=14083629
ssh sofascore-prod "cd /opt/sofascore && PGPASSWORD=\$(grep -oP 'postgresql://[^:]+:\K[^@]+' .env) psql -U sofascore_user -d sofascore_schema_inspector -h localhost -p 5432 -c \"
SELECT endpoint_pattern, COUNT(*) AS cnt, MAX(fetched_at) AS latest
FROM api_payload_snapshot
WHERE context_entity_id = $EID AND context_entity_type = 'event'
GROUP BY endpoint_pattern ORDER BY latest DESC;
\""
```

### Audit DB для event
```bash
ssh sofascore-prod "/opt/sofascore/.venv/bin/python -m schema_inspector.cli audit-db --sport-slug football --event-id $EID"
```

### Per-endpoint coverage за last 1h
```bash
ssh sofascore-prod 'cd /opt/sofascore && PGPASSWORD=$(grep -oP "postgresql://[^:]+:\K[^@]+" .env) psql -U sofascore_user -d sofascore_schema_inspector -h localhost -p 5432 -c "
SELECT endpoint_pattern,
       COUNT(*) FILTER (WHERE http_status = 200) AS ok_200,
       COUNT(*) FILTER (WHERE http_status = 404) AS not_404,
       COUNT(*) FILTER (WHERE http_status IS NULL) AS no_status,
       ROUND(100.0 * COUNT(*) FILTER (WHERE http_status = 200) / NULLIF(COUNT(*),0), 1) AS ok_pct
FROM event_endpoint_availability_log
WHERE observed_at >= NOW() - INTERVAL '"'"'1 hour'"'"' AND decision = '"'"'probe'"'"'
GROUP BY endpoint_pattern ORDER BY COUNT(*) DESC LIMIT 25;
"'
```

---

## Сценарий 5: безопасный restart worker'ов

### Single worker template
```bash
# Все instances template:
ssh sofascore-prod 'sudo systemctl restart "sofascore-hydrate@*"'

# Один экземпляр:
ssh sofascore-prod 'sudo systemctl restart sofascore-hydrate@1.service'
```

### Restart planner
**Warning**: planner иногда зависает на SIGTERM 30s → SIGKILL. Это нормально. Новый процесс стартует, но **прогрев** live state может занять 10-15 минут — ZCARD lanes восстанавливается постепенно.

```bash
ssh sofascore-prod 'sudo systemctl restart sofascore-planner.service'

# Verify:
ssh sofascore-prod 'systemctl show sofascore-planner.service -p MainPID,ActiveEnterTimestamp,SubState'

# Verify env прочитан:
ssh sofascore-prod 'PID=$(systemctl show -p MainPID --value sofascore-planner.service); sudo tr "\0" "\n" < /proc/$PID/environ | grep LIVE_DISPATCH_LEASE'
```

### Restart all hot workers (после flag change)
```bash
ssh sofascore-prod 'sudo systemctl restart sofascore-live-discovery@1 sofascore-discovery@1 \
    "sofascore-hydrate@*" "sofascore-live-tier-1@*" "sofascore-live-tier-2@*" "sofascore-live-tier-3@*" \
    "sofascore-live-warm@*" sofascore-live-hot@1'
```

**Не рестартить вместе** с planner — workers и planner не должны рестартиться одновременно (риск пропустить tier_1 work для топ-матчей).

### Restart historical lane
```bash
# Чаще нужно ВЫКЛЮЧИТЬ во время live кризисов:
ssh sofascore-prod 'sudo systemctl stop "sofascore-historical-tournament@*" "sofascore-historical-enrichment@*"'

# Включить обратно:
ssh sofascore-prod 'sudo systemctl start "sofascore-historical-tournament@*"'
```

---

## Сценарий 6: rollback кода

### Через git revert
```bash
# Local:
cd D:/sofascore
git revert <commit_hash>  # делает новый commit с reverse изменений
git push origin main

# Prod:
ssh sofascore-prod 'cd /opt/sofascore && git pull --ff-only origin main'

# Restart affected services (см. таблицу в SERVICES_AND_WORKERS.md "Когда какой restart")
```

### Через env flag (если есть)
Многие критические features имеют feature flag (см. [ENVIRONMENT.md](ENVIRONMENT.md)):

```bash
# Rollback firebreak (вернуть inline rollup writes)
ssh sofascore-prod 'echo "SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED=1" >> /opt/sofascore/.env'
ssh sofascore-prod 'sudo systemctl restart "sofascore-hydrate@*" "sofascore-live-tier-*@*"'

# Disable split-details fanout
ssh sofascore-prod 'sed -i "s/^LIVE_SPLIT_DETAILS_FANOUT=.*/LIVE_SPLIT_DETAILS_FANOUT=0/" /opt/sofascore/.env'
ssh sofascore-prod 'sudo systemctl restart "sofascore-live-tier-*@*"'
```

### Через git reset (опасно)
```bash
# ⚠️ Только если push на main был ошибкой
# и hash сохранён где-то локально:
git reset --hard <previous_commit>
git push origin main --force-with-lease  # требует ACK от пользователя!
```

---

## Сценарий 7: canary rollout

Для рискованных изменений (lease, concurrency, policy):

1. **Baseline snapshot** перед change:
   - `/ops/queues/summary` (lag, dispatch_metrics)
   - `etl_job_run` status counts за 15 мин
   - `journalctl -u sofascore-live-discovery@1 | grep -c "scheduling retry"`
   - `zset:live:hot` ZCARD

2. **Backup .env**:
```bash
ssh sofascore-prod 'cp /opt/sofascore/.env /opt/sofascore/.env.bak.$(date +%Y%m%d_%H%M%S)'
```

3. **Apply change** (один параметр за раз):
```bash
ssh sofascore-prod 'echo "NEW_VAR=value" >> /opt/sofascore/.env'
ssh sofascore-prod 'sudo systemctl restart <affected services>'
```

4. **Wait 15 минут warmup** (planner / workers медленно прогреваются).

5. **Comparison snapshot** — проверь те же 5 метрик. Если регрессия → откатить.

6. **Wait 30 минут sustained** — повторная проверка.

Пример: см. `D:\sofascore\reports\firebreak_capability_rollup_baseline.md` для firebreak 2026-05-13 deploy и `D:\sofascore\reports\lease_waterfall_2026_05_13.md` для LIVE_DISPATCH_LEASE staged rollout.

---

## Сценарий 8: восстановление после Redis flush

Если Redis потерял данные:

```bash
# Read-only: проверить состояние
ssh sofascore-prod 'redis-cli -a "<REDIS_PW>" --no-auth-warning DBSIZE'
ssh sofascore-prod 'redis-cli -a "<REDIS_PW>" --no-auth-warning ZCARD zset:live:hot'

# Recovery (⚠️ MUTATION):
ssh sofascore-prod '/opt/sofascore/.venv/bin/python -m schema_inspector.cli recover-live-state'

# Verify:
ssh sofascore-prod 'redis-cli -a "<REDIS_PW>" --no-auth-warning ZCARD zset:live:hot'
```

`recover-live-state` читает `event_terminal_state` + `event` из PostgreSQL и переписывает `zset:live:hot/warm/cold` + `live:event:*`.

---

## Сценарий 9: rebuild capability rollup

После firebreak 2026-05-13 inline rollup writes отключены. Pересборка вручную:

```bash
# ⚠️ MUTATION: пересборка rollup из last 24h observations
ssh sofascore-prod '/opt/sofascore/.venv/bin/python -m schema_inspector.cli rebuild-capability-rollup --lookback-days 1'

# Только football:
ssh sofascore-prod '/opt/sofascore/.venv/bin/python -m schema_inspector.cli rebuild-capability-rollup --sport-slug football --lookback-days 7'

# Full history (не рекомендуется чаще раза в неделю):
ssh sofascore-prod '/opt/sofascore/.venv/bin/python -m schema_inspector.cli rebuild-capability-rollup'
```

Запускать в low-traffic time. Real time на проде: ~14 секунд для 232 rollup rows.

---

## Сценарий 10: запуск ad-hoc hydration

Для debug / backfill конкретных events:

```bash
# ⚠️ MUTATION: полная hydration одного event
ssh sofascore-prod '/opt/sofascore/.venv/bin/python -m schema_inspector.cli event \
    --sport-slug football --event-id 14083629'

# Несколько events:
ssh sofascore-prod '/opt/sofascore/.venv/bin/python -m schema_inspector.cli event \
    --sport-slug football --event-id 14083629 --event-id 14083644 --event-concurrency 2'
```

После завершения — verify через `audit-db`:
```bash
ssh sofascore-prod '/opt/sofascore/.venv/bin/python -m schema_inspector.cli audit-db --sport-slug football --event-id 14083629'
```

---

## Сценарий 11: deploy code change

См. CLAUDE.md "Deploy flow":

```bash
# Local:
cd D:/sofascore
# (changes)
pytest tests -q  # full regression
git add <files>
git commit -m "..."
git push origin main

# Prod:
ssh sofascore-prod 'cd /opt/sofascore && git pull --ff-only origin main'
ssh sofascore-prod 'sudo systemctl restart <affected services>'

# Verify smoke:
ssh sofascore-prod 'curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8000/api/v1/...'
```

**Важно** для Windows local repo:
- Edit tool сохраняет файлы как UTF-8 без BOM + LF/CRLF inconsistent.
- Git autocrlf=true делает шумный diff (мусор line-endings).
- Workaround: PowerShell нормализация BOM+CRLF перед commit — см. предыдущие session notes.

---

## Что мониторить устойчиво (24/7)

| Метрика | Где | Threshold |
|---|---|---|
| Stream lag (per stream) | `/ops/queues/summary` | < `*_MAX_LAG` константа |
| refresh_live_event throughput | etl_job_run last 15min, status=succeeded | 150-400/min normal |
| refresh_live_event retry rate | etl_job_run last 15min | < 10% (5% target) |
| DeadlockDetectedError per hour | etl_job_run, last 1h, error_class | < 5/hr (after firebreak) |
| zset:live:hot ZCARD | redis-cli ZCARD | > 50 при наличии live; >100 в пик |
| Oldest hot zset score age | (now - oldest score) | < 15 мин |
| live-discovery retry rate | `journalctl -u sofascore-live-discovery@1 \| grep -c "scheduling retry"` | < 5/hr |
| Proxy success rate | api_request_log, последние 5 мин | > 95% |
| live_dispatch_metrics blocked% | Redis HASH live:dispatch_metrics | < 75% для tier_1 (после lease tune) |

---

## Read-only / Mutation команды cheatsheet

### ✅ Safe (read-only)
- `curl http://127.0.0.1:8000/ops/*`
- `curl http://127.0.0.1:8000/api/v1/...`
- `psql ... -c "SELECT ..."`
- `redis-cli ... ZCARD/HGETALL/ZRANGE/...`
- `journalctl -u sofascore-*`
- `systemctl status / is-active / show`
- `python -m schema_inspector.cli health / audit-db / stale-live-events`

### ⚠️ MUTATION (требует ACK)
- `systemctl restart / stop / start sofascore-*`
- `echo X >> /opt/sofascore/.env` (или любой edit)
- `git pull` на проде
- `python -m schema_inspector.cli recover-live-state`
- `python -m schema_inspector.cli rebuild-capability-rollup`
- `python -m schema_inspector.cli event / live / scheduled / full-backfill / replay`
- `psql ... -c "INSERT/UPDATE/DELETE ..."`
- `redis-cli ... SET/HSET/ZADD/DEL ...`

---

## Связано

- [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) — high-level
- [SERVICES_AND_WORKERS.md](SERVICES_AND_WORKERS.md) — service-level details
- [ENVIRONMENT.md](ENVIRONMENT.md) — env переменные + restart impacts
- [REDIS_AND_QUEUES.md](REDIS_AND_QUEUES.md) — Redis inspection
- [DATABASE_AND_STORAGE.md](DATABASE_AND_STORAGE.md) — таблицы и hot rows
- [CLI_AND_SCRIPTS.md](CLI_AND_SCRIPTS.md) — все CLI
