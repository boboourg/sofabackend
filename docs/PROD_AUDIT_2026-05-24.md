# Production Audit — 2026-05-24

**Дата проверки:** 2026-05-24, начало 14:00 EEST
**Метод:** 4 параллельных Opus-агента + ручная диагностика через `ssh sofascore-prod`
**Окружение:** prod-сервер `bobur`, IP `138.201.248.126`, `/opt/sofascore`
**Контекст:** первая полная проверка прода после рефакторинга локальной копии (удаление 21 `load_*.py` shim'ов, dev-утилит с Windows-путями, написание `CLAUDE.md`).

---

## TL;DR — 7 critical findings

| # | Severity | Проблема | Impact | Status |
|---|---|---|---|---|
| 1 | 🔴 CRIT | Диск `/dev/md2` 98% (406/436 G); retention `api_payload_snapshot` сломан (TimeoutError каждый tick) | **48–72 ч до OOM** | partial cleanup в процессе |
| 2 | 🔴 CRIT | `cg:hydrate` backlog 6.6M, consume 2.67/sec vs publish 13.2/sec → отстаёт на 4 дня, lag растёт +906k/день | scheduled refresh stale на 4+ дня (live OK через tier_1/2/3) | identified, не починен |
| 3 | 🔴 CRIT | `event_terminal_state`: 597k locked / 0 finalized; finalize worker не отпускает `locked_at` | первопричина 76k TimeoutError за 6h в `historical-hydrate` | identified, не починен |
| 4 | 🔴 CRIT | `DuplicatePreparedStatementError` race при reload pool после deploy: 482 failed jobs за 16 мин (23.05 18:33–18:50) | следующий deploy = такой же 17-мин outage tier_1 | identified |
| 5 | 🔴 CRIT | N1 monitoring слеп: `/ops/jobs/runs?limit=200` стабильно ReadTimeout → 1 Telegram alert за 24h на 613 failed jobs | алертинг неэффективен | identified |
| 6 | 🟡 MAJOR | Drift flags: 3 P0(b)/P5b feature flags в systemd drop-ins (не в `.env`, не в docs) | UNKNOWN-режим, ни оператор ни код не видит правды | задокументировано в этом отчёте |
| 7 | 🟡 MAJOR | pgbouncer running, но `SOFASCORE_DATABASE_URL` указывает на `:5432` напрямую → миграция полуприменена | `SOFASCORE_PG_MAX_SIZE=5` неадекватен для direct mode | identified |

---

## 1. Disk exhaustion

### 1.1 Breakdown 406G

| Path | Size | Notes |
|---|---|---|
| `/var/lib/postgresql` | **296 G** | sofascore_schema_inspector сам 291 G |
| `/var/lib/docker` | 59 G | overlay2 41 G (другие проекты Бобура: clawdbot, varmatch, yt_*) + redis_data volume 19 G |
| `/var/log` | 14 G | syslog.1 8.2 G, journald 3.9 G |
| `/var/www/varmatch_bot_usr` | 25 G | unrelated tenant |
| `/opt/sofascore` | 994 M | code only |

### 1.2 Топ-10 таблиц Postgres

| Table | Total | Heap | Indexes | TOAST | n_live_tup |
|---|---|---|---|---|---|
| `api_payload_snapshot` | **164 GB** | 14 G | 7 G | **140 G (pg_toast_16394)** | 9.28 M |
| `event_endpoint_availability_log` | 20 GB | 16 G | 4 G | — | 63.6 M |
| `top_player_entry` | 14 GB | 11 G | 3 G | — | 40.3 M |
| `endpoint_capability_observation` | 13 GB | 6 G | 7 G | — | 15.9 M |
| `event_team_heatmap_point` | 12 GB | 8 G | 5 G | — | 49.5 M |
| `etl_job_run` | 11 GB | 7 G | 4 G | — | 28.3 M |

### 1.3 Retention status

- ✅ `api_request_log` (48 h): WORKING — ~2.5–3.7k deleted/tick
- ✅ `event_live_state_history` (30 d): WORKING — ~20/tick
- ✅ `endpoint_capability_observation` (7 d): WORKING — 6–10k/tick
- 🔴 **`api_payload_snapshot` legacy retention BROKEN**:
  - Каждый tick: `ERROR retention step failed: api_payload_snapshot_legacy` + `TimeoutError`
  - Каждый tick: `snapshot=0` удалено
  - 120-секундный `statement_timeout` в [`storage/retention_repository.py:122`](../schema_inspector/storage/retention_repository.py) слишком жёсткий для текущего 140 GB TOAST
  - Запрос `victims CTE` сканирует 140 G TOAST через два NOT-EXISTS anti-joins (`api_snapshot_head` 2.18M pinned + `event_terminal_state` 413K pinned)
  - **5.37M rows / ~94 GB reclaimable** (старше 7 дней, не запинены)
- ❌ `etl_job_run`: NO retention — 28.3M rows / 11 GB, oldest 2026-04-18 (36 дней)
- ❌ `event_endpoint_availability_log`: NO retention — 20 GB / 63.6M rows

### 1.4 Memory pressure (collateral)

`free -h`: **62 G RAM 92% used, swap 31 G 100% used**. Объясняет почему DELETE'ы по `api_payload_snapshot` затаймаучивают — Postgres не может удержать working set в RAM.

### 1.5 Что сделано (этим аудитом)

- ✅ `gzip /var/log/syslog.1` — освободил **7.4 GB** (8.2G → 0.82G)
- ✅ `journalctl --vacuum-size=500M` — освободил **3.5 GB**
- ⏳ `DELETE FROM etl_job_run WHERE started_at < NOW() - INTERVAL '14 days'` в batch'ах 30k — ожидаемо освободит **~9 GB** (8.3M rows)
- ⏸️ `api_payload_snapshot` retention — **НЕ ТРОНУТО** по решению owner ("если придётся переparse-ить из raw")

### 1.6 Что ещё надо сделать (по приоритету)

1. **`event_endpoint_availability_log` retention 14d**: ~16 GB освободит
2. **Починить `api_payload_snapshot` legacy retention**:
   - Bump timeout в [`retention_repository.py:122`](../schema_inspector/storage/retention_repository.py) до `600s`
   - И/или уменьшить batch с 20 000 до 2 000
   - Запустить ручной DROP старше 30 дней с `statement_timeout=0` чтобы догнать backlog
3. **`VACUUM api_payload_snapshot`** после drain
4. **Поднять `effective_cache_size`** с 4 GB до 32 GB (RAM ~64 G)

### 1.7 Timing — критический

С учётом publish rate `api_payload_snapshot` ≈ **500–700k rows/day = 3.5 GB TOAST/day**, оставшиеся 19 GB free кончатся за **~5–6 дней без вмешательства**. С учётом swap 100% риск OOM в любой момент.

---

## 2. cg:hydrate — silent disaster

### 2.1 Цифры

| Метрика | Value | Interpretation |
|---|---|---|
| XLEN `stream:etl:hydrate` | 7 505 974 | total messages в стриме |
| Lag `cg:hydrate` | **6 700 619** | unprocessed messages |
| Last-delivered-id | `1779302894571-0` (2026-05-20 18:48 UTC) | consumers обрабатывают **4 дня назад** |
| Consumers | 10 (hydrate-1..10) | все живы (idle 12–27 sec) |
| Pending per consumer | 8–10 | низкое, не зависают на сообщениях |
| Publish rate | **13.2/sec** | измерено T→T+15s |
| Consume rate | **2.67/sec** | то же окно |
| Net growth | **+10.5/sec → +906k/day** | catastrophic |

### 2.2 Почему скрытая

- `/ops/live-freshness` показывает **healthy** (`oldest_hot_score_age=162s`)
- Live actor работает через `live_tier_1/2/3` streams, не через `hydrate`
- `hydrate` stream = scheduled/upcoming refresh (не критично для live UI)
- N1 monitoring не алертит на этот lag (нет соответствующего signal)

### 2.3 Что было 2026-05-20 18:39 UTC

Совпадает с моментом когда `cg:hydrate` last-delivered-id зафиксировался на 4 дня позади. Скорее всего:
- Произошёл deploy, перезапустивший hydrate workers
- После рестарта они начали читать через `XREADGROUP ... STREAMS hydrate >`
- Но 6.6M сообщений между 20.05 и сейчас — никто их не process'ит на скорости publish'а

### 2.4 Что НЕ сделано (требует отдельной сессии)

1. **Профилировать почему consume rate так низок (2.67/s)** — где bottleneck:
   - asyncpg pool contention (с `SOFASCORE_PG_MAX_SIZE=5` и 73 workers)?
   - Sofascore HTTP throttle (curl_cffi)?
   - Hydrate lock contention (см. §4)?
2. **Решить судьбу 6.6M backlog**:
   - либо очистить (`XTRIM stream:etl:hydrate MAXLEN ~ N` чтобы сбросить старые)
   - либо разогнать `worker-hydrate@N` (но сначала найти root cause throttle)

---

## 3. event_terminal_state — 597k locked rows

### 3.1 Картина

```sql
SELECT COUNT(*) FILTER (WHERE locked_at IS NOT NULL) AS locked,
       COUNT(*) FILTER (WHERE finalized_at IS NOT NULL) AS finalized,
       COUNT(*) AS total
FROM event_terminal_state;
-- locked=597 269, finalized=0, total=701 377
```

### 3.2 Impact

- Это первопричина **76 661 TimeoutError за 6 часов** в `historical-hydrate@*` workers
- Finalize-loop в [`schema_inspector/pipeline/pilot_orchestrator.py`](../schema_inspector/pipeline/pilot_orchestrator.py) (final_sync stage) активно стаймпит `locked_at` для new events
- Но **никто не выпускает старые locks** через `UPDATE event_terminal_state SET locked_at = NULL WHERE finalized_at IS NOT NULL` (или равнозначно)
- historical-hydrate воркеры пытаются взять lock → ждут → TimeoutError 30s → retry → loop

### 3.3 Запланированный fix (Шаг 3)

```sql
SET statement_timeout = 0;
UPDATE event_terminal_state SET locked_at = NULL
WHERE locked_at IS NOT NULL
  AND locked_at < NOW() - INTERVAL '1 hour'
  AND finalized_at IS NULL;
```

Грейс-period 1 час чтобы не сорвать активные финализации.

---

## 4. DuplicatePreparedStatementError — следующий deploy убьёт прод снова

### 4.1 Инцидент 23.05 18:33–18:50 (16 мин)

- **482 failed jobs** в hot lane: `hydrate_event_root` 311, `refresh_live_event` 171
- Trace:
  ```
  LiveTierOverrideRegistry.load failed ...
  DuplicatePreparedStatementError('prepared statement "__asyncpg_stmt_1__" already exists')
  ```
- Race при reload connection pool после deploy: asyncpg переиспользует connection, prepared statement names коллизятся

### 4.2 Где исправлять

- File: [`schema_inspector/live_tier_override.py`](../schema_inspector/live_tier_override.py)
- Option A: при создании asyncpg pool передавать `statement_cache_size=0`
- Option B: переопределить `prepared_statement_name_format` чтобы был unique (UUID или counter)

### 4.3 Impact

Без фикса **каждый последующий deploy = ~16-минутный outage** на tier_1 / hot hydrate path. Это критично, потому что pace deploy'ев высок (за неделю было видно 5+ commits в `git log`).

---

## 5. N1 monitoring — слеп на job-failures

### 5.1 Симптом

`sofascore-monitoring.service` каждые 67 секунд логирует:
```
WARNING schema_inspector.monitoring.signal_source: fetch_job_signals: fetch failed
url=http://127.0.0.1:8000/ops/jobs/runs?limit=200 err=ReadTimeout('')
```

То же для `fetch_slo_signals` (`/ops/live-freshness`) и `fetch_queue_signals` (`/ops/queues/summary`) — intermittent ~30/24h.

### 5.2 За 24h — один Telegram alert

При том что было:
- 482-job incident 23.05
- 613 failed jobs total
- `cg:hydrate` lag = 6.6M
- `event_terminal_state` 597k locked

Alerting effectively **dead для job-failures**.

### 5.3 Fix

- Поднять timeout в [`schema_inspector/monitoring/signal_source.py`](../schema_inspector/monitoring/signal_source.py) (сейчас 5s ReadTimeout)
- Или ускорить `/ops/jobs/runs?limit=200` в [`local_api_server.py`](../schema_inspector/local_api_server.py) — вероятно full scan `etl_job_run` без `started_at` filter

---

## 6. ENV drift — feature flags in systemd drop-ins, not docs

### 6.1 Drift table

| Flag | Prod | docs/ENVIRONMENT.md | Code default | Verdict |
|---|---|---|---|---|
| `LIVE_TIER_1_ROOT_ONLY` | **`1`** (drop-in `root-only.conf`) | UNKNOWN | `0` | **Drift — enabled, undocumented** |
| `LIVE_TIER_1_QUARANTINE_ENABLED` | **`1`** (drop-in `quarantine.conf`) | UNKNOWN | `0` | **Drift — enabled, undocumented** |
| `HYDRATE_EVENT_CIRCUIT_BREAKER_ENABLED` | **`1`** (drop-in `circuit-breaker.conf`) | UNKNOWN | `0` | **Drift — enabled, undocumented** |
| `SOFASCORE_LIVE_FANOUT_MAX_INFLIGHT` (tier_1) | **`4`** (drop-in `parallel-and-stop-timeout.conf`) | `1` | `1` | **Drift — bumped, undocumented** |
| `SOFASCORE_HYDRATE_FANOUT_MAX_INFLIGHT` | **`3`** (.env, since 2026-05-17) | `1` | `1` | **Drift — bumped, undocumented** |
| `SCHEMA_INSPECTOR_HYDRATE_PROXY_MAX_IN_USE_PER_ENDPOINT` | `4` (drop-in) | UNKNOWN | UNKNOWN | Per-service tuning |
| `LIVE_DISPATCH_LEASE_TIER_1_MS` | `12000` (.env) | not in docs | — | F-7 canary |

### 6.2 ENV в prod, отсутствуют в docs

- A2 Live Rescue (2026-05-16): `SOFASCORE_LIVE_RESCUE_ENABLED=true`, `_INTERVAL_S=60`, `_STALE_MINUTES=5`, `_COOLDOWN_SECONDS=300`, `_SPORT_SLUGS=…`, `_MAX_PER_TICK=50`
- Burst tolerance knobs (2026-05-18): `SOFASCORE_BACKFILL_OLDEST_HOT_AGE_MAX=7200`, `_AGE_WARN=5400`, `_TIER_1_QUARANTINED_MAX=50`
- N4 API response cache: `SOFASCORE_API_RESPONSE_CACHE_BACKEND=redis` + 6 TTL knobs
- N1 monitoring tuned thresholds: 11 vars (`_OLDEST_HOT_AGE_*`, `_HYDRATE_XLEN_*`, etc.)
- Misc: `SCHEMA_INSPECTOR_FALLBACK_HOSTS`, `HISTORICAL_ENRICHMENT_GO_LIVE_MAX_LAG`, `SOFASCORE_MOBILE_PROXY_ROTATE_URL`, `SCHEMA_INSPECTOR_FORCE_TOP_FOOTBALL_LEAGUES`

### 6.3 21 `.env.bak.*` files (audit trail)

Pattern в именах: `pgpool`, `concurrency`, `apicache`, `zombieage`, `proxy`, `maxlag`, `A2sports`, `pre-pgbouncer`. Most-touched knobs:
- `SOFASCORE_PG_MAX_SIZE` (100 → 5 после pgbouncer)
- `SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY` (8 → 16)
- `SOFASCORE_HOUSEKEEPING_ZOMBIE_MAX_AGE_MINUTES` (120 → 30)
- `HISTORICAL_TOURNAMENT_MAX_LAG` (50000 → 200000)
- `SOFASCORE_FETCH_TIMEOUT_SECONDS` (10 → 5)

### 6.4 pgbouncer — полуподключён

- pgbouncer running на `127.0.0.1:6432`, transaction pool, `default_pool_size=25`, `max_client_conn=2000`
- `SOFASCORE_PG_MAX_SIZE` снижен 100→5 в `.env`
- НО `SOFASCORE_DATABASE_URL` всё ещё указывает на **прямой Postgres :5432**
- ⇒ приложение bypass-ит pgbouncer, `PG_MAX_SIZE=5` неадекватен для direct mode (должно быть ~20)
- Либо переключить URL на `:6432`, либо вернуть `PG_MAX_SIZE` на ~20

### 6.5 Postgres config caveats

- `max_connections=500`, current 324 (65%)
- `shared_buffers=16GB`
- ⚠️ `effective_cache_size=4GB` — **слишком низко** (должно быть 50–75% RAM = 32–48 GB)
- `work_mem=16MB`, `maintenance_work_mem=2GB`
- `statement_timeout=0` глобально, `sofascore_user.statement_timeout=30s`

---

## 7. Очереди — детальная карта

### 7.1 Streams (XLEN / lag / consumers)

| Stream | XLEN | Lag | Pending | Consumers | Comment |
|---|---|---|---|---|---|
| discovery | 33 248 | 0 | 0 | 2 | OK |
| live_discovery | 282 895 | 3 | 0 | 2 | OK |
| **hydrate** | **7.5M** | **6.7M** | 97 | 10 | 🔴 backlog catch-up §2 |
| live_tier_1 | 1 041 436 | 0 | 5 | 6 | OK |
| live_tier_2 | 435 156 | 0 | 1 | 6 | OK |
| **live_tier_3** | 5 082 044 | 56 802 | 25 | 10 | растёт, 6.1h позади |
| live_hot | 0 | 0 | 0 | 2 | idle (через zset) |
| live_warm | 1 166 681 | 0 | 0 | 4 | OK (нужен XTRIM) |
| live_details | 162 | 0 | 0 | 1 | OK |
| maintenance | 0 | 0 | 0 | 1 | OK |
| historical_discovery | 446 024 | 0 | 0 | 4 | OK |
| historical_tournament | 197 621 | 192 062 | 0 | 5 | 40.5h позади |
| historical_bootstrap | 94 800 | 93 346 | 11 | 3 | работает медленно |
| historical_enrichment | 59 111 | 58 695 | 0 | 6 | last_delivered 8d назад* |
| historical_hydrate | 2.08M | 288 766 | 156 | 31 | растёт |
| historical_maintenance | 0 | 0 | 0 | 1 | OK |
| structure_sync | 78 474 | 14 913 | **85** | **1** | 🟡 consumer-leak §7.3 |
| resource_refresh | 5 680 925 | 5 160 | 335 | 10 | OK |
| normalize | 5 270 290 | 0 | 0 | 1 | OK (нужен XTRIM) |

\* `historical_enrichment` — opus интерпретировал idle 1-4h как "consumers dead", но прямая проверка показала: процессы **активно логируют backfill jobs** (`event_detail_backfill_job: progress`). То есть workers работают через **direct DB-driven backfill batch**, не через этот stream. Stream назначен legacy. **Не критично, но нужен audit что именно стрим теперь несёт.**

### 7.2 Redis memory

- `used_memory_human=10.76G`, `peak=10.77G`
- `maxmemory=0` (без лимита), `maxmemory-policy=noeviction`
- ⚠️ `mem_fragmentation_ratio=0.03` — аномально низкий
- DBSIZE=861 218 keys

**Рекомендация**: установить `maxmemory` + `maxmemory-policy=allkeys-lru`, иначе при OOM Redis будет отказывать в записи.

### 7.3 `cg:structure_sync` consumer-leak

- 1 consumer (`structure-sync-1`) держит **85 pending entries уже 7.6+ дня**
- last-delivered-id `1778963943417-0` = 2026-05-16 19:32 UTC
- Worker сейчас alive (idle 28 сек), но эти 85 jobs клеймлены давно, не процессятся
- **Fix**: `XAUTOCLAIM` policy для всех consumer groups с одним consumer, или ручной `XCLAIM cg:structure_sync structure-sync-2 ...` после рестарта.

### 7.4 Live state

- `zset:live:hot=1`, `zset:live:warm=0`, `zset:live:cold=45`
- live:hot oldest score age = 2.0 мин (нормально)
- ⚠️ `zset:live:cold` oldest score age = **185 ч (7.7 дня)** — 45 cold-событий не обновлялись неделю; нужно подтверждение что это OK для finalized

### 7.5 Делает ли что-то 38 GB Redis backlog?

Multiple streams имеют XLEN >> lag (все entries прочитаны), но **никогда не XTRIM**'ились:
- `normalize`: 5.27M entries, lag=0
- `resource_refresh`: 5.68M, lag=5k
- `live_warm`: 1.17M, lag=0
- `live_tier_2`: 435k, lag=0

Это ~5–7 GB Redis memory, безопасно XTRIM:
```
XTRIM stream:etl:normalize MAXLEN ~ 100000
XTRIM stream:etl:resource_refresh MAXLEN ~ 100000
XTRIM stream:etl:live_warm MAXLEN ~ 100000
XTRIM stream:etl:live_tier_2 MAXLEN ~ 100000
```

---

## 8. Ошибки за 24h (распределение)

### 8.1 Exception распределение

| Lane | TimeoutError | RetryableJobError | LockNotAvailable | Deadlock |
|---|---|---|---|---|
| hydrate@* | 2 712 | 991 | 217 | **0** |
| live-tier-1@* | 99 | 1 940 | 19 | 0 |
| live-tier-2@* | 1 758 | — | 502 | 2 |
| live-tier-3@* | 609 | — | 558 | 2 |
| historical-hydrate@* (6h) | **76 661** | 19 | 2 | 0 |
| historical-hydrate@* (24h, deadlock) | — | — | — | **4** |

### 8.2 Тренд vs audit 2026-05-16

- **DeadlockDetectedError**: 39/24h → **4/24h** (-90%) ✅
- Hot `hydrate` stream — **0 deadlocks** за 24h ✅
- TimeoutError в historical-hydrate: 76k/6h ≈ 300k/24h projected — драматично хуже (источник: §3 locked event_terminal_state)

### 8.3 Failed jobs в `etl_job_run` за 24h

succeeded 956k, retry_scheduled 235k, **failed 613**:

| job_type | error_class | cnt |
|---|---|---|
| hydrate_event_root | DuplicatePreparedStatementError | **311** |
| refresh_live_event | DuplicatePreparedStatementError | **171** |
| enrich_tournament_entities_batch | QueryCanceledError | 26 |
| sync_tournament_archive | QueryCanceledError | 22 |
| hydrate_event_root | InvalidSQLStatementNameError | 19 |
| refresh_live_event | RetryableJobError | 18 |
| hydrate_event_root | ProtocolViolationError + InterfaceError | 28 |
| refresh_resource | ForeignKeyViolationError | 2 |

**482 из 613 (79%) — incident 23.05 18:33–18:50** (§4).

---

## 9. Failed unit — `sofascore-team-event-surface-refresh`

### 9.1 Симптом

Unit `Type=oneshot`, ExecStart:
```
/opt/sofascore/.venv/bin/python -m schema_inspector.event_list_cli \
  --sport-slug football --timeout 30 team-surfaces --team-id 2817
```

Trace (24.05 05:38:07):
```
schema_inspector.sofascore_client.SofascoreHttpError:
HTTP request failed: status=404, proxy=proxy_2,
url=https://www.sofascore.com/api/v1/team/2817/events/next/0
```

### 9.2 Причина

- Hardcoded `--team-id 2817` (предположительно USA national team / placeholder)
- 23 мая команда успешно прошла (`events=31 tournaments=6 teams=26`)
- 24 мая Sofascore возвращает **404**: либо team 2817 удалена/переименована, либо proxy_2 забанен на этом URL

### 9.3 Fix

- File: [`schema_inspector/event_list_job.py:200`](../schema_inspector/event_list_job.py) (`run_team_next`)
- Глотать 404 как soft-skip (warn + exit 0) вместо `raise SofascoreHttpError`
- Или заменить статичный team_id 2817 на dynamic queue из `team_event_surface_subscription`

### 9.4 Impact

Live ETL не страдает. Но unit в `failed` state навсегда — засоряет dashboards, может маскировать другие fail'ы.

---

## 10. Что ещё стоит почистить

| Файл / директория | Размер | Решение |
|---|---|---|
| `/opt/sofascore/=` (0-byte) | 0 | Stray `> =` typo. **Safe to delete** |
| `/opt/sofascore/.env.bak.*` (21 файлов) | ~150 KB | Архивировать `pre-pgbouncer`, `pgpool`, `concurrency` (исторически интересные transitions), остальные удалить. Минимум — оставить последний `.env.bak` |
| `M scripts/cascade_restart_workers.sh` | ~ | Only `old mode 100644 → new mode 100755` (chmod +x). **Commit the +x bit** — это staggered restart script для Phase 4.7.6 Track 3, polezно |
| `/var/log/mover_bot_v3_error.log` (549 M) | 549 M | НЕ наш сервис (varmatch/yt_* tenants) — не трогать без согласования |

---

## 11. Roadmap fix'ов (по приоритету)

### Сегодня (Day 0)
1. ✅ syslog gzip + journalctl vacuum (+11 GB)
2. ⏳ `etl_job_run` retention 14d (+~9 GB)
3. `event_endpoint_availability_log` retention 14d (+~16 GB)
4. `event_terminal_state` release stale locks (+скорость historical-hydrate)
5. Delete `/opt/sofascore/=`

### Эта неделя
6. Bump `retention_repository.py:122` timeout 120s→600s; уменьшить batch 20k→2k
7. Запустить ручной DELETE `api_payload_snapshot` (старше 30 дней; ack required)
8. `VACUUM api_payload_snapshot` после drain
9. Fix `DuplicatePreparedStatementError` через `statement_cache_size=0` в pool creation
10. Fix `team-event-surface-refresh` soft-skip 404
11. Документация: обновить `docs/ENVIRONMENT.md` с 3 systemd drop-in flag'ами + новые ENV (A2 rescue, burst knobs, monitoring thresholds)
12. Решить pgbouncer: либо переключить `SOFASCORE_DATABASE_URL` на `:6432`, либо вернуть `PG_MAX_SIZE=20`

### Месяц
13. `XAUTOCLAIM` policy для всех consumer groups
14. `maxmemory` + `allkeys-lru` для Redis
15. Поднять Postgres `effective_cache_size` до 32 GB
16. `XTRIM MAXLEN ~ 100000` для normalize / resource_refresh / live_warm / live_tier_2
17. Профилировать `cg:hydrate` consume rate (root cause 2.67/sec)

### Эпоха 2 (см. CLAUDE.md §6)
18. HTTPS + auth + rate-limit для публичного API
19. Read cache в Redis для повторных запросов
20. Separate asyncpg pool для historical + live-first governor

---

## 12. Ссылки на исходники

- [`schema_inspector/storage/retention_repository.py:111-159`](../schema_inspector/storage/retention_repository.py) — broken retention DELETE
- [`schema_inspector/services/housekeeping.py:328-357`](../schema_inspector/services/housekeeping.py) — caller / error path
- [`schema_inspector/live_tier_override.py`](../schema_inspector/live_tier_override.py) — DuplicatePreparedStatementError root
- [`schema_inspector/monitoring/signal_source.py`](../schema_inspector/monitoring/signal_source.py) — fetch timeout
- [`schema_inspector/event_list_job.py:200`](../schema_inspector/event_list_job.py) — `run_team_next` 404 raise
- [`schema_inspector/pipeline/pilot_orchestrator.py`](../schema_inspector/pipeline/pilot_orchestrator.py) — final_sync stamps locked_at
- [`docs/ARCHITECTURE_AUDIT.md`](ARCHITECTURE_AUDIT.md) — risk register
- [`docs/PERFORMANCE_AUDIT_2026-05-20.md`](PERFORMANCE_AUDIT_2026-05-20.md) — DB performance baseline
- [`docs/parser-reliability-audit-2026-05-16.md`](parser-reliability-audit-2026-05-16.md) — parser silent drops

---

---

## 13. Incident 2026-05-24 19:47 — OOM kill Postgres от VACUUM ANALYZE

### 13.1 Timeline (EEST)

| Время | Событие |
|---|---|
| ~15:36 | Запущен `etl_job_run` retention 14d через batch DELETE (50k→30k batch) |
| 19:47:11 | OOM-killer убил процесс Postgres-16-main child (peak 46.5G RAM + 1.3G swap) |
| 19:47:44 | systemd: `postgresql@16-main.service: Failed with result 'oom-kill'` |
| 19:47:40-44 | 28+ FATAL `the database system is shutting down` для sofascore_user сессий |
| 19:47:43 | `LOG: database system is shut down` |
| 21:05 | Запущен `VACUUM ANALYZE etl_job_run` (после kill cleanup'а, удалено 8.27M rows) |
| 21:05–21:17 | API возвращает 500 (asyncpg pool reconnect-loop без validation), workers застряли в retry |
| 21:18:09 | `systemctl start postgresql@16-main` → начат WAL recovery |
| 21:20:13 | Recovery в процессе (`startup recovering 00000001000007E40000004A`) |
| 21:20:36 | Recovery завершено, Postgres accepting connections |
| 21:22 | Рестарт `sofascore-api.service` (uvicorn держал dead asyncpg pool) |
| 21:24 | Все endpoints отвечают 200, load 10 (с пика 43) |

### 13.2 Root cause

**`VACUUM ANALYZE etl_job_run` (11 GB) под текущей нагрузкой превысил RAM** (64G total, обычно 19G used).

Конкретно:
- `maintenance_work_mem=2GB` per VACUUM op
- 97 активных sofascore unit'ов держали ~250 connections × shared/private memory
- Параллельный hydrate workers активно писал в `etl_job_run` (956k/day publish rate)
- Suм всего превысил доступную память → kernel OOM-killer выбрал postgres backend как самый жирный процесс

### 13.3 Impact

- **Простой ~1.5 часа** (19:47 → 21:24)
- Все Swagger ручки + `/api/v1/*` → 500
- 97 sofascore worker unit'ов в retry loop → load average 43
- Live freshness был degraded (никто не финализирует), но `oldest_hot_score_age` не пробило порог 900s
- Никакой потери данных (Postgres сделал clean recovery из WAL за 2 минуты)

### 13.4 Lessons (правила для будущих операций)

1. **Никогда не запускать `VACUUM ANALYZE` на таблицах > 5 GB на нагруженном проде без announce window.**
2. **Перед heavy DB операцией** проверять `free -h` — должно быть как минимум 30 GB свободной RAM + 50% swap.
3. **`maintenance_work_mem` локально downgrade-ить** перед VACUUM: `SET maintenance_work_mem='256MB'` (вместо глобальных 2GB).
4. **autovacuum в фоне справляется** — manual VACUUM на нагруженной таблице обычно не нужен. Достаточно ждать.
5. **API needs PG pool with `connection_check=True` или `pool.expire_connections()` на signal** — после crash Postgres asyncpg pool возвращал dead connections до restart uvicorn.
6. **`scripts/cascade_restart_workers.sh`** (Phase 4.7.6 Track 3) уже существует — после prod-incident'ов использовать его для постепенного рестарта workers, не сразу.

### 13.5 Что сделано в этом incident'е (резюме)

- ✅ syslog gzip + journalctl vacuum: освободил **+11 GB** (диск 12G → 19G free)
- ✅ `etl_job_run` retention 14d: удалил **8.27M rows** из 28.36M (29%), осталось 20.1M
- ❌ VACUUM ANALYZE → OOM Postgres (incident 13.1–13.4)
- ✅ Recovery + рестарт API → восстановлено
- 📊 Финальное состояние: disk **14G free 97%**, load **10**, all endpoints 200

### 13.6 НЕ сделано (отложено)

- `event_endpoint_availability_log` retention 14d (~16 GB) — слишком рискованно сейчас, повторим в low-traffic окне
- VACUUM (FULL) etl_job_run для возврата физического места — требует AccessExclusiveLock, не подходит для prod
- Альтернатива: `pg_repack` (online repack без блокировок) — рекомендация для следующей сессии

---

**Документ обновляется при каждой новой prod-проверке.** Следующая запланирована после устранения critical items §1.4 + §3 + §4.
