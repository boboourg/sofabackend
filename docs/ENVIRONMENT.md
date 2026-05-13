# Environment variables

Каталог всех `.env` / `os.getenv` переменных, прочитанных проектом. Все defaults — из исходного кода, **не из прода**.

Конфиг-loader: проект использует `_load_project_env()` (определена в `cli.py`, `db.py`, `runtime.py`, `local_api_server.py`) — читает `D:\sofascore\.env`, merges с `os.environ` (приоритет environment). Helpers: `_env_int`, `_env_float`, `_env_bool` (типичные в `services/backpressure_config.py`, `live_dispatch_policy.py`).

Колонка "Restart required" — нужно ли рестартить процесс после изменения. `Yes` = читается только при старте. `No` = читается каждый tick / runtime.

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
| `SOFASCORE_FETCH_TIMEOUT_SECONDS` | `20.0` | `fetch_models.py:42` | Per-attempt HTTP timeout. ⚠️ На проде = `10`. | no (читается per fetch task) |
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
