# Environment variables

Каталог всех `.env` / `os.getenv` переменных, прочитанных проектом. Все defaults — из исходного кода. **Production values задокументированы в §0 (drift с docs).**

Конфиг-loader: `_load_project_env()` (определена в `cli.py`, `db.py`, `runtime.py`, `local_api_server.py`) — читает `/opt/sofascore/.env`, merges с `os.environ` (приоритет environment). Helpers: `_env_int`, `_env_float`, `_env_bool` (типичные в `services/backpressure_config.py`, `live_dispatch_policy.py`).

Колонка "Restart required" — нужно ли рестартить процесс после изменения. `Yes` = читается только при старте. `No` = читается каждый tick / runtime.

---

## 0. Production reality (drift с docs) — 2026-05-24

**Источник:** прямая проверка `ssh sofascore-prod` 2026-05-24 (см. [docs/PROD_AUDIT_2026-05-24.md](PROD_AUDIT_2026-05-24.md)).

### 0.1 Feature flags в systemd drop-ins, НЕ в `.env`

На проде включены 3 P0(b)/P5b feature flag'а через `/etc/systemd/system/sofascore-*@.service.d/*.conf` — оператор, читающий только `/opt/sofascore/.env`, **не увидит** их.

| Flag | Prod | Code default | Drop-in файл |
|---|---|---|---|
| `LIVE_TIER_1_ROOT_ONLY` | **`1`** | `0` | `sofascore-live-tier-1@.service.d/root-only.conf` |
| `LIVE_TIER_1_QUARANTINE_ENABLED` | **`1`** | `0` | `sofascore-live-tier-1@.service.d/quarantine.conf` |
| `HYDRATE_EVENT_CIRCUIT_BREAKER_ENABLED` | **`1`** | `0` | `sofascore-hydrate@.service.d/circuit-breaker.conf` |
| `SOFASCORE_LIVE_FANOUT_MAX_INFLIGHT` (tier_1 only) | **`4`** | `1` | `sofascore-live-tier-1@.service.d/parallel-and-stop-timeout.conf` |
| `SCHEMA_INSPECTOR_HYDRATE_PROXY_MAX_IN_USE_PER_ENDPOINT` | `4` | UNKNOWN | `sofascore-hydrate@.service.d/proxy-max-in-use.conf` |

**Чтобы посмотреть все drop-ins:**
```bash
ssh sofascore-prod 'find /etc/systemd/system -name "*.conf" -path "*sofascore*.d*" -exec head -20 {} \;'
```

### 0.2 ENV в проде, отсутствуют в этом каталоге (drift)

Эти переменные читаются кодом и установлены на проде, но не описаны ниже.

**A2 Live Rescue daemon (2026-05-16):**
- `SOFASCORE_LIVE_RESCUE_ENABLED=true`
- `SOFASCORE_LIVE_RESCUE_INTERVAL_S=60`
- `SOFASCORE_LIVE_RESCUE_STALE_MINUTES=5`
- `SOFASCORE_LIVE_RESCUE_COOLDOWN_SECONDS=300`
- `SOFASCORE_LIVE_RESCUE_SPORT_SLUGS=football,tennis,...`
- `SOFASCORE_LIVE_RESCUE_MAX_PER_TICK=50`

**Burst tolerance knobs (2026-05-18, помечены "revert after drain"):**
- `SOFASCORE_BACKFILL_OLDEST_HOT_AGE_MAX=7200`
- `SOFASCORE_BACKFILL_OLDEST_HOT_AGE_WARN=5400`
- `SOFASCORE_BACKFILL_TIER_1_QUARANTINED_MAX=50`

**N4 API response cache (Redis):**
- `SOFASCORE_API_RESPONSE_CACHE_BACKEND=redis`
- 6 TTL knobs: `_TTL_LIVE`, `_TTL_SCHEDULED`, `_TTL_INPROGRESS`, `_TTL_FINALIZED`, `_TTL_NOTSTARTED`, `_TTL_DEFAULT` (см. [api_cache.py](../schema_inspector/api_cache.py))

**N1 monitoring tuned thresholds:**
- `SOFASCORE_MONITORING_OLDEST_HOT_AGE_WARN`, `_CRIT`
- `SOFASCORE_MONITORING_HYDRATE_XLEN_WARN`, `_CRIT`
- `SOFASCORE_MONITORING_LIVE_WARM_XLEN_WARN`, `_CRIT`
- `SOFASCORE_MONITORING_LIVE_DISCOVERY_XLEN_WARN`, `_CRIT`
- `SOFASCORE_MONITORING_DISCOVERY_XLEN_WARN`, `_CRIT`
- `SOFASCORE_MONITORING_TIER_1_BLOCKED_WARN`, `_CRIT`
- `SOFASCORE_MONITORING_JOB_SIGNALS_ENABLED=1`

**Misc:**
- `SCHEMA_INSPECTOR_FALLBACK_HOSTS`
- `HISTORICAL_ENRICHMENT_GO_LIVE_MAX_LAG`
- `SOFASCORE_MOBILE_PROXY_ROTATE_URL`
- `SCHEMA_INSPECTOR_FORCE_TOP_FOOTBALL_LEAGUES`

### 0.3 Connection pool drift (важно)

Phase 4.7.6 Track 1 (2026-05-22) сократил пул `20/50 → 3/10`. На проде `.env`:

```bash
SOFASCORE_PG_MIN_SIZE=  # не установлен → код default 20 → но реальный обычно из per-service ENV
SOFASCORE_PG_MAX_SIZE=5  # снижено для pgbouncer transaction pool
SOFASCORE_HYDRATE_FANOUT_MAX_INFLIGHT=3  # bumped с 1 (drift с docs)
SOFASCORE_LIVE_FANOUT_MAX_INFLIGHT=4 (tier_1 only, via drop-in)  # bumped с 1
LIVE_DISPATCH_LEASE_TIER_1_MS=12000  # F-7 canary
LIVE_DISPATCH_LEASE_TIER_2_MS=15000
LIVE_DISPATCH_LEASE_TIER_3_MS=60000
```

### 0.4 pgbouncer — полуподключён

На проде запущен pgbouncer на `127.0.0.1:6432` (transaction pool, default_pool_size=25, max_client_conn=2000), но:
- `SOFASCORE_DATABASE_URL` всё ещё указывает на **прямой Postgres :5432**
- ⇒ приложение bypass-ит pgbouncer, `PG_MAX_SIZE=5` неадекватен для direct mode

**Решение требует выбора:** либо переключить URL на `:6432`, либо вернуть `PG_MAX_SIZE` на ~20.

### 0.5 21 `.env.bak.*` файлов

Audit trail последних месячных изменений. Pattern в именах кодирует change:
`pgpool` (snapshot до миграции к большему пулу), `concurrency`, `apicache`, `zombieage`, `proxy`, `maxlag`, `A2sports`, `pre-pgbouncer`.

Most-touched knobs (по diff между бэкапами):
- `SOFASCORE_PG_MAX_SIZE`: 100 → 5 после pgbouncer
- `SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY`: 8 → 16
- `SOFASCORE_HOUSEKEEPING_ZOMBIE_MAX_AGE_MINUTES`: 120 → 30
- `HISTORICAL_TOURNAMENT_MAX_LAG`: 50 000 → 200 000
- `SOFASCORE_FETCH_TIMEOUT_SECONDS`: 10 → 5

### 0.6 Что обновлять при следующем prod-аудите

1. Скопировать все настройки из drop-ins в этот документ как первоисточник
2. Сверить с кодом — какие из ENV доходят до runtime, какие игнорируются
3. Удалить из docs UNKNOWN там где можно verify
4. Если drift между прод и docs > 5 переменных — нужен autoconfig validator (например `schema_inspector/config_schema.py` с pydantic BaseSettings)

---

---

## Database / Redis connection

| Variable | Default | Где читается | Эффект | Restart |
|---|---|---|---|---|
| `SOFASCORE_DATABASE_URL` | — (no default) | `db.py:161` `load_database_config()` | PostgreSQL DSN (primary) | yes |
| `DATABASE_URL` | — | `db.py:161` (fallback chain) | PostgreSQL DSN (fallback) | yes |
| `POSTGRES_DSN` | — | `db.py:161` (legacy fallback) | PostgreSQL DSN | yes |
| `SOFASCORE_PG_MIN_SIZE` | `20` | `db.py:167` | asyncpg pool min size | yes |
| `SOFASCORE_PG_MAX_SIZE` | `50` | `db.py:168` | asyncpg pool max size | yes |
| `SOFASCORE_PG_COMMAND_TIMEOUT` | `60.0` | `db.py:169` | Query timeout (sec) | yes |
| `SOFASCORE_PG_STATEMENT_CACHE_SIZE` | `0` | `db.py` (DatabaseConfig) | asyncpg prepared-statement cache size per connection. **Default 0 = disabled** — prevents `DuplicatePreparedStatementError` after deploy when systemd restart reuses backend connections still holding statement names from the old process (incident 2026-05-23 18:33–18:50, 482 failed jobs in 16 min). Set >0 only if profiling proves the cache pays off (~0.5 ms/query saved). | yes |
| `SOFASCORE_PG_IDLE_IN_TX_TIMEOUT_MS` | `60000` | `db.py` (DatabaseConfig) | **Phase1-B4 (2026-05-29).** asyncpg `server_settings` → Postgres `idle_in_transaction_session_timeout` (ms) on every app connection. Bounds a session that ran `BEGIN` + a statement then went idle (hung mid-flight) so it cannot hold row locks indefinitely and trigger the 5s `lock_timeout` cascade (2026-05-29 review). Does NOT kill long-RUNNING active txns (that is `statement_timeout`). 0 disables. Prod DSN is direct (5432), so the startup param is honoured. | yes (per connect) |
| `SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_1` | `25.0` | `live_dispatch_policy.py` | **Phase1-A2 (2026-05-29).** Per-tier root/edge HTTP fetch timeout for tier_1/hot live lanes. Higher than the global default so top live matches (detailId=1) survive proxy-latency bursts (~18.7s observed) and actually hydrate their match-center — the event-15728277 regression. | no (read per worker start) |
| `SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_2` | `20.0` | `live_dispatch_policy.py` | **Phase1-A2.** Per-tier fetch timeout for tier_2/warm. | no |
| `SOFASCORE_FETCH_TIMEOUT_SECONDS_TIER_3` | `12.0` | `live_dispatch_policy.py` | **Phase1-A2.** Per-tier fetch timeout for tier_3 (long-tail/niche) — deliberately short so a dead niche fetch fails fast instead of pinning a proxy for 20-25s. | no |
| `SOFASCORE_RETENTION_LEGACY_SNAPSHOT_TIMEOUT_SECONDS` | `600` | `storage/retention_repository.py` `_resolve_legacy_snapshot_timeout_seconds` | `SET LOCAL statement_timeout` for the per-batch DELETE on `api_payload_snapshot` (legacy/non-scoped). **Default 600** — old hard-coded `120s` proved too tight once the table grew past ~140 GB TOAST, making every tick TimeoutError and letting backlog grow to 5.37M rows (disk reached 100 % on 2026-05-25). Bounded to `[60, 3600]`; out-of-range values fall back to default. Lower `SOFASCORE_HOUSEKEEPING_BATCH_SIZE` in tandem (e.g. 20000 → 2000) to keep each statement under the new ceiling. | no (read per tick) |
| `SOFASCORE_PG_APPLICATION_NAME` | `schema-inspector` | `db.py:171` | PG `application_name` (видно в pg_stat_activity) | yes |
| `SOFASCORE_PG_SOCKET_DIR` | `/var/run/postgresql` | `db.py:173` | Unix socket path | yes |
| `REDIS_URL` | — | `transport.py:717` | Redis connection string | yes (для workers) |
| `SOFASCORE_REDIS_URL` | — | `transport.py:717` | Redis DSN (alias) | yes |

**Прод-формат**:
```bash
SOFASCORE_DATABASE_URL=postgresql://sofascore_user:<pw>@localhost:5432/sofascore_schema_inspector
REDIS_URL=redis://:<pw>@127.0.0.1:6379/0
```

---

## Worker concurrency

Все поглощают `_env_int()` или `os.getenv(...)` при инициализации воркера.

| Variable | Default | Где | Эффект | Restart |
|---|---|---|---|---|
| `SOFASCORE_WORKER_MAX_CONCURRENCY` | `8` | `worker_runtime.py:30,87` | Глобальный fallback для всех воркеров | yes |
| `SOFASCORE_WORKER_COMPLETION_TTL_MS` | `3600000` (1h) | `worker_runtime.py:31` | Retention completion Redis ключей | yes |
| `SOFASCORE_HYDRATE_WORKER_MAX_CONCURRENCY` | UNKNOWN | `workers/hydrate_worker.py:80` | Параллелизм hydrate | yes |
| `SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY` | UNKNOWN | `workers/hydrate_worker.py:82` | Параллелизм historical hydrate | yes |
| `SOFASCORE_LIVE_TIER_1_WORKER_MAX_CONCURRENCY` | UNKNOWN (прод 20) | `workers/live_worker_service.py:575` | Tier 1 концурренси per worker | yes |
| `SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY` | UNKNOWN | `workers/live_worker_service.py:575,581` | Hot/legacy tier 1 | yes |
| `SOFASCORE_LIVE_TIER_2_WORKER_MAX_CONCURRENCY` | UNKNOWN (прод 15) | `workers/live_worker_service.py:577` | Tier 2 концурренси | yes |
| `SOFASCORE_LIVE_TIER_3_WORKER_MAX_CONCURRENCY` | UNKNOWN (прод 10) | `workers/live_worker_service.py:579` | Tier 3 концурренси | yes |
| `SOFASCORE_LIVE_WARM_WORKER_MAX_CONCURRENCY` | UNKNOWN | `workers/live_worker_service.py:582` | Warm концурренси | yes |
| `SOFASCORE_NORMALIZE_WORKER_MAX_CONCURRENCY` | UNKNOWN | `workers/normalize_stream_worker.py:94` | Параллелизм normalize | yes |
| `SOFASCORE_LIVE_DETAILS_WORKER_MAX_CONCURRENCY` | UNKNOWN | `workers/live_details_worker_service.py:80` | Live-details параллелизм | yes |
| `SOFASCORE_RESOURCE_REFRESH_WORKER_MAX_CONCURRENCY` | UNKNOWN | `workers/resource_refresh_worker.py:122` | Resource refresh параллелизм | yes |
| `SOFASCORE_DISCOVERY_WORKER_MAX_CONCURRENCY` | UNKNOWN | `workers/discovery_worker.py` | Discovery параллелизм | yes |
| `SOFASCORE_MAINTENANCE_WORKER_MAX_CONCURRENCY` | UNKNOWN | `workers/maintenance_worker.py` | Параллелизм maintenance | yes |

**Прод-факт (из summary 2026-05-13)**:
```
SOFASCORE_LIVE_TIER_1_WORKER_MAX_CONCURRENCY=20
SOFASCORE_LIVE_TIER_2_WORKER_MAX_CONCURRENCY=15
SOFASCORE_LIVE_TIER_3_WORKER_MAX_CONCURRENCY=10
```

---

## Transport / Proxy / TLS

Конфигурирует `D:\sofascore\schema_inspector\runtime.py` через ProxyEndpoint pool.

| Variable | Default | Где | Эффект | Restart |
|---|---|---|---|---|
| `SOFASCORE_FETCH_TIMEOUT_SECONDS` | `20.0` | `fetch_models.py:42` | Per-attempt HTTP timeout. **Прод = `20` (2026-05-29, было `10`)** — 10s был слишком жёстким для всплесков латентности прокси: топ live-матчи (tier_1, detailId=1) ловили `curl(28)` на root `/event` → `run_event` падал до фетча матч-центра → quarantine → 0 ручек (см. CLAUDE §5.1). | no (читается per fetch task) |
| `SCHEMA_INSPECTOR_PROXY_URLS` | — | `runtime.py:511` | Список proxy endpoints (comma-sep) | yes |
| `SCHEMA_INSPECTOR_PROXY_URL` | — | `runtime.py:508` | Single proxy (legacy) | yes |
| `SCHEMA_INSPECTOR_STRUCTURE_PROXY_URLS` | — | `runtime.py:533` | Proxy для structure sync | yes |
| `SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS` | — | `runtime.py:609` | Proxy для historical lane | yes |
| `SCHEMA_INSPECTOR_REQUIRE_PROXY` | `true` | `runtime.py:464` | Fail если proxy недоступен | yes |
| `SCHEMA_INSPECTOR_MAX_ATTEMPTS` | `3` | `runtime.py:467` | Max retry на ошибки | no |
| `SCHEMA_INSPECTOR_CHALLENGE_MAX_ATTEMPTS` | `5` | `runtime.py:470` | Max retry для challenge (Cloudflare) | no |
| `SCHEMA_INSPECTOR_NETWORK_ERROR_MAX_ATTEMPTS` | `4` | `runtime.py:472` | Max retry для network errors | no |
| `SCHEMA_INSPECTOR_BACKOFF_SECONDS` | `1.0` | `runtime.py:473` | Базовый exponential backoff | no |
| `SCHEMA_INSPECTOR_TLS_MIN_VERSION` | `TLSv1.2` | `runtime.py:476` | Min TLS | yes |
| `SCHEMA_INSPECTOR_TLS_MAX_VERSION` | `TLSv1.3` | `runtime.py:477` | Max TLS | yes |
| `SCHEMA_INSPECTOR_TLS_CHECK_HOSTNAME` | `true` | `runtime.py:478` | TLS hostname check | yes |
| `SCHEMA_INSPECTOR_TLS_IMPERSONATE` | `chrome110` | `runtime.py:443` | curl_cffi fingerprint (chrome110/edge99 поддерживаются в 0.5.10; chrome116-131 НЕ поддерживаются!) | yes |
| `SCHEMA_INSPECTOR_USER_AGENT` | `schema-inspector/1.0` | `runtime.py:444` | UA header | no |
| `SCHEMA_INSPECTOR_PROXY_REQUEST_COOLDOWN_SECONDS` | `1.5` | `runtime.py:484` | Min interval между запросами одной proxy session | no |
| `SCHEMA_INSPECTOR_PROXY_REQUEST_JITTER_SECONDS` | `1.0` | `runtime.py:485` | Random jitter | no |
| `SCHEMA_INSPECTOR_SOURCE_SLUG` | `sofascore` | `runtime.py:459` | source_slug в snapshot записях | no |
| `SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER` | `1` | `runtime.py:166` | Множитель размера session pool | yes |
| `SCHEMA_INSPECTOR_PROXY_MAX_IN_USE_PER_ENDPOINT` | UNKNOWN | `runtime.py:197` | Cap для concurrent session per endpoint | yes |
| `SCHEMA_INSPECTOR_SESSION_CACHE_MAX_ENTRIES` | UNKNOWN | `runtime.py:246` | LRU размер кеша sessions | yes |
| `SOFASCORE_PROXY_HEALTH_POOL_FILTER_ENABLED` | `true` | `transport.py:714` | Фильтровать unhealthy proxy из pool | no |

Per-worker overrides (structure/historical):
- `SCHEMA_INSPECTOR_STRUCTURE_MAX_ATTEMPTS`, `SCHEMA_INSPECTOR_HISTORICAL_*` — аналогичные

**Опасные значения**:
- `SOFASCORE_FETCH_TIMEOUT_SECONDS=300` — workers зависнут, не дойдут до retry
- `SCHEMA_INSPECTOR_TLS_IMPERSONATE=chrome131` — curl_cffi 0.5.10 не поддерживает! Все запросы падают с UnknownImpersonate

---

## Live dispatch & polling

| Variable | Default | Где | Эффект | Restart (planner) |
|---|---|---|---|---|
| `LIVE_TIER_1_MIN_USER_COUNT` | `6500` | `live_dispatch_policy.py:35` | Threshold для tier_1 (по uniqueTournament userCount) | no (читается в `resolve_live_dispatch_tier`) |
| `LIVE_TIER_2_MIN_USER_COUNT` | `1000` | `live_dispatch_policy.py:36` | Threshold для tier_2 | no |
| `LIVE_TIER_1_POLL_SECONDS` | `5` | `live_dispatch_policy.py:38` | Cadence tier_1 | no |
| `LIVE_TIER_2_POLL_SECONDS` | `30` | `live_dispatch_policy.py:39` | Cadence tier_2 | no |
| `LIVE_TIER_3_POLL_SECONDS` | `90` | `live_dispatch_policy.py:40` | Cadence tier_3 | no |
| `LIVE_DISPATCH_LEASE_TIER_1_MS` | `90000` (default) | `live_dispatch_policy.py:53` | Redis claim lease TTL для tier_1 — **ключевой параметр**. **На проде: 12000** (firebreak Stage B 2026-05-13). | yes (planner re-reads only at start) |
| `LIVE_DISPATCH_LEASE_TIER_2_MS` | `90000` | `live_dispatch_policy.py:54` | Tier_2 lease. **На проде: 15000**. | yes |
| `LIVE_DISPATCH_LEASE_TIER_3_MS` | `90000` | `live_dispatch_policy.py:55` | Tier_3 lease. **На проде: 60000** (Stage A). | yes |
| `SOFASCORE_LIVE_FANOUT_MAX_INFLIGHT` | `1` | `cli.py:82` | Max parallel event detail fetches per worker | no |
| `LIVE_SPLIT_DETAILS_FANOUT` | `0` | `pipeline/pilot_orchestrator.py:1021` | Включает split-details stream (P0(a)) | no |

**Lease vs poll логика**:
- `lease` = время блокировки в Redis `live:dispatch_claim:{event_id}` (`SET NX PX <lease_ms>`).
- `poll` = частота с которой planner ищет due events в `zset:live:hot`.
- Если `lease >> poll`, то planner делает много claim_attempts, большинство blocked. См. инцидент 2026-05-13.

---

## Backpressure (queue lag thresholds)

Конфигурируется в `D:\sofascore\schema_inspector\services\backpressure_config.py`. Каждая константа имеет `_env_int(name, default)`.

| Variable | Default | Эффект |
|---|---|---|
| `SCHEDULED_DISCOVERY_MAX_LAG` | `100` | Если lag `cg:discovery` > 100 → planner skip publish |
| `HYDRATE_MAX_LAG` | `800` | `cg:hydrate` lag threshold |
| `LIVE_DISCOVERY_MAX_LAG` | `50` | `cg:live_discovery` lag threshold |
| `LIVE_DISCOVERY_HYDRATE_MAX_LAG` | `1000` | Discovery-fanout hydrate threshold |
| `LIVE_HOT_MAX_LAG` | `200` | `cg:live_hot` |
| `LIVE_TIER_1_MAX_LAG` | `200` | (по умолчанию равно LIVE_HOT_MAX_LAG) |
| `LIVE_TIER_2_MAX_LAG` | `200` | |
| `LIVE_TIER_3_MAX_LAG` | `200` | |
| `LIVE_WARM_MAX_LAG` | `800` | |
| `HISTORICAL_DISCOVERY_MAX_LAG` | `100` | |
| `HISTORICAL_HYDRATE_MAX_LAG` | `500` | |
| `HISTORICAL_TOURNAMENT_MAX_LAG` | `2500` | |
| `HISTORICAL_ENRICHMENT_MAX_LAG` | `15000` | |
| `STRUCTURE_SYNC_MAX_LAG` | `1500` | |

Изменения требуют **restart планировщика** (читается при создании Backpressure objects).

---

## Feature flags

| Variable | Default | Где | Эффект | Restart |
|---|---|---|---|---|
| `SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED` | `0` (OFF) | `pipeline/pilot_orchestrator.py:2021` | После firebreak 2026-05-13: `0` → не пишет `endpoint_capability_rollup` в hot path (observations only); `1` → legacy inline writes (race-condition-prone) | yes (hot workers) |
| `LIVE_SPLIT_DETAILS_FANOUT` | `0` | `pipeline/pilot_orchestrator.py:1021` | Включает per-player split (P0(a)) — публикует в `STREAM_LIVE_DETAILS` | no |
| `LIVE_TIER_1_ROOT_ONLY` | UNKNOWN | `workers/live_worker_service.py:169` | Tier 1 в root-only mode (P0.b) | yes |
| `LIVE_TIER_1_QUARANTINE_ENABLED` | UNKNOWN | `services/service_app.py:1138` | Включает tier_1 quarantine cooldown | yes |
| `HYDRATE_EVENT_CIRCUIT_BREAKER_ENABLED` | UNKNOWN | `workers/hydrate_worker.py:72` | Per-event circuit breaker (P5b) | yes |
| `SCHEMA_INSPECTOR_NEGATIVE_CACHE_ENABLED` | `true` | `season_widget_negative_cache.py:134` | Season-widget negative cache | no |
| `SCHEMA_INSPECTOR_NEGATIVE_CACHE_MODE` | `enforce` | `season_widget_negative_cache.py:136` | `enforce` или `shadow` (observe only) | no |
| `SCHEMA_INSPECTOR_EVENT_NEGATIVE_CACHE_ENABLED` | `false` | `event_endpoint_negative_cache.py:148` | Event-endpoint negative cache | no |
| `SCHEMA_INSPECTOR_EVENT_NEGATIVE_CACHE_MODE` | `enforce` | `event_endpoint_negative_cache.py:150` | `enforce` / `shadow` | no |
| `SOFASCORE_LIVE_HYDRATION_MODE` | `delta` | `live_hydration_mode.py:27` | `delta` / `full` / `auto` (canary) | no |
| `SOFASCORE_LIVE_HYDRATION_CANARY_SPORTS` | — | `live_hydration_mode.py:43` | Список спортов для auto mode (comma-sep) | no |
| `CANARY_SPORTS` | — | `live_hydration_mode.py:45` | Legacy alias | no |

---

## Proxy health monitoring

`D:\sofascore\schema_inspector\services\proxy_health_monitor.py`

| Variable | Default | Эффект |
|---|---|---|
| `SOFASCORE_PROXY_HEALTH_ENABLED` | `true` | Включить мониторинг |
| `SOFASCORE_PROXY_HEALTH_INTERVAL_SECONDS` | `60.0` | Интервал check |
| `SOFASCORE_PROXY_HEALTH_WINDOW_SECONDS` | `3600` | Метрик-окно (1h) |
| `SOFASCORE_PROXY_HEALTH_MIN_REQUESTS` | `20` | Min sample size для классификации |
| `SOFASCORE_PROXY_HEALTH_SUCCESS_RATE_THRESHOLD` | `0.05` (5%) | Если success rate ниже — unhealthy |
| `SOFASCORE_PROXY_HEALTH_UNHEALTHY_TTL_SECONDS` | `1800` (30 min) | Cooldown unhealthy proxy |
| `SOFASCORE_PROXY_HEALTH_EXCLUDED_ADDRESS_SUBSTRINGS` | — | Skip из health checks |

---

## Retention / housekeeping

| Variable | Default | Где | Эффект |
|---|---|---|---|
| `SCHEMA_INSPECTOR_RESOURCE_NEGATIVE_TTL_SECONDS` | `604800` (7 days) | `queue/resource_negative_cache.py:42` | TTL not-found resource cache (Redis) |
| `SCHEMA_INSPECTOR_TOTW_BLACKLIST_TTL_SECONDS` | `2592000` (30 days) | `queue/totw_404_store.py:49` | Team-of-the-week 404 blacklist TTL |
| `SOFASCORE_RETENTION_CAPABILITY_OBSERVATION_DAYS` | `7` | `ops/housekeeping.py` | Retention для endpoint_capability_observation rows |
| `SOFASCORE_RETENTION_*` | UNKNOWN | прочие housekeeping helpers | retention windows |

---

## Final sweep gate

| Variable | Default | Эффект |
|---|---|---|
| `SOFASCORE_FINAL_SWEEP_MAX_CONCURRENCY` | `8` | `final_sweep_gate.py:39` |
| `SOFASCORE_FINAL_SWEEP_JITTER_MAX_SECONDS` | `30.0` | jitter против thundering herd |

---

## Inflight / throttle / quarantine TTLs

| Variable | Default | Эффект |
|---|---|---|
| `SOFASCORE_LIVE_EVENT_INFLIGHT_TTL_MS` | UNKNOWN (default code 600000) | `queue/live_inflight.py` — `live:refresh_inflight:{event_id}` |
| `SOFASCORE_LIVE_DETAILS_INFLIGHT_TTL_MS` | UNKNOWN (300000?) | `live:details_inflight:{event_id}` |
| `SOFASCORE_LIVE_ROOT_INFLIGHT_TTL_MS` | UNKNOWN (60000?) | `live:root_inflight:{event_id}` |
| `LIVE_EDGES_MIN_INTERVAL_SECONDS` | UNKNOWN (60?) | `queue/live_edges_throttle.py` |
| `LIVE_DETAILS_MIN_INTERVAL_SECONDS` | UNKNOWN (30?) | `queue/live_details_throttle.py` |
| `LIVE_TIER_1_QUARANTINE_WINDOW_SECONDS` | `600` (10 min) | `queue/live_tier_1_quarantine.py` |
| `LIVE_DETAILS_STREAM_BACKPRESSURE_LIMIT` | UNKNOWN | `workers/live_worker_service.py` |

---

## Resource scoping (managed content)

`D:\sofascore\schema_inspector\services\resource_scope\*.py`. Используется resource-planner для решения, какие endpoints публиковать в `STREAM_RESOURCE_REFRESH`.

| Variable | Default | Эффект |
|---|---|---|
| `SCHEMA_INSPECTOR_RESOURCE_PILOT_TEAMS` | — | Pilot team ids (comma-sep) |
| `SCHEMA_INSPECTOR_FORCE_TOP_FOOTBALL_LEAGUES` | — | Force tier mapping для лиг |
| `SCHEMA_INSPECTOR_RESOURCE_PILOT_SEASONS` | — | Pilot season ids (semicolon-sep pairs) |
| `SCHEMA_INSPECTOR_RESOURCE_REGISTRY_PILOT_UTS` | — | Pilot tournament ids для full registry |
| `SCHEMA_INSPECTOR_RESOURCE_REGISTRY_WINDOW_DAYS_PAST` | `60` | Historical window (days) |
| `SCHEMA_INSPECTOR_RESOURCE_REGISTRY_WINDOW_DAYS_FUTURE` | `60` | Future window |
| `SCHEMA_INSPECTOR_RESOURCE_REGISTRY_LIMIT` | `100000` | Max entities |
| `SCHEMA_INSPECTOR_RESOURCE_REGISTRY_CACHE_TTL_SECONDS` | `1800` (30 min) | Registry cache |
| `SCHEMA_INSPECTOR_POS_COMPLETED_GAP_DAYS` | `7` | Playoff gap (для player-of-the-season detect) |
| `SCHEMA_INSPECTOR_REGULAR_SEASON_UTS` | `132,11205,234,9464,262,138` | Regular season tournament ids |

---

## Misc / observability

| Variable | Default | Эффект |
|---|---|---|
| `PYTHONUNBUFFERED` | `1` (всегда в systemd) | stdout не буферизуется |
| `LOCAL_API_PUBLIC_BASE_URLS` | — | Дополнительные base URL в OpenAPI servers list |
| `BP_PREFETCH_MEMORY_WARN_MB` | `50` | `cli.py:2142` — memory warning threshold для prefetch batches |

---

## Чек-лист безопасности env изменений

Перед изменением **на проде**:

1. **Идентифицируй scope** — planner only / workers only / API only / всё.
2. **Идентифицируй restart-need**:
   - `Restart=yes` → нужно `systemctl restart <unit>`.
   - `Restart=no` → изменится при следующем tick (можно скиппнуть restart, но безопаснее всё-таки рестартнуть для гарантии).
3. **Для critical flags** (`SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED`, `HYDRATE_EVENT_CIRCUIT_BREAKER_ENABLED`, `LIVE_DISPATCH_LEASE_*`) — staged rollout: применить, мониторить 15-30 мин, потом следующий tier.
4. **Backup .env** перед изменением: `cp /opt/sofascore/.env /opt/sofascore/.env.bak.$(date +%Y%m%d_%H%M%S)`.
5. **После change** — verify env прочитался в процессе: `sudo tr "\0" "\n" < /proc/<PID>/environ | grep <VAR>`.

См. [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md) для конкретных runbook-ов.
