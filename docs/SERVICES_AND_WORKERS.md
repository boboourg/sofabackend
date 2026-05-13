# Сервисы и воркеры

Полный inventory systemd unit-файлов, планировщиков, воркеров. Все unit-файлы лежат в `D:\sofascore\ops\systemd\`. Все они запускаются через `D:\sofascore\deploy\run_service.sh` который активирует `.venv` и вызывает `python -m schema_inspector.cli <subcommand>`.

## Глоссарий

- **Singleton** — одиночный сервис (нет `@` в имени).
- **Template** — параметризованный сервис: `sofascore-<name>@N.service` для N=1..k экземпляров с разными consumer_name.
- **Stream / consumer group** — Redis Stream и его cg:* (см. [REDIS_AND_QUEUES.md](REDIS_AND_QUEUES.md)).
- **KillSignal=SIGINT** — все unit'ы посылают SIGINT при `systemctl stop`; **TimeoutStopSec=30s**. При превышении — SIGKILL. Это особенно заметно у планировщика: иногда висит 30s на shutdown — это **известное поведение**, не регрессия.

---

## Планировщики (Daemons)

### `sofascore-planner.service` (singleton)

| Поле | Значение |
|---|---|
| Команда | `run_service.sh planner-daemon` |
| Класс | `PlannerDaemon` (`D:\sofascore\schema_inspector\services\planner_daemon.py`) |
| Читает | (ничего — producer only) |
| Публикует в | `STREAM_DISCOVERY` (scheduled discover), `STREAM_HYDRATE` (delayed retry), `STREAM_LIVE_TIER_1/2/3`, `STREAM_LIVE_WARM` (refresh_live_event) |
| Пишет в БД | (косвенно через delayed jobs) |
| Env | `LIVE_TIER_*_POLL_SECONDS`, `LIVE_DISPATCH_LEASE_TIER_*_MS`, `LIVE_TIER_*_MIN_USER_COUNT`, `*_MAX_LAG` |
| Failure modes | Backpressure skip (если stream lag > limit). Может «зависнуть» в shutdown 30s — нормально. |
| Restart нужен после | Изменения env переменных related к live-dispatch, lease, poll cadence. |
| Лог | `journalctl -u sofascore-planner.service` |

### `sofascore-live-discovery-planner.service` (singleton)

| Поле | Значение |
|---|---|
| Команда | `run_service.sh live-discovery-planner-daemon` |
| Класс | `LiveDiscoveryPlannerDaemon` (`D:\sofascore\schema_inspector\services\live_discovery_planner.py`) |
| Публикует в | `STREAM_LIVE_DISCOVERY` (по cadence per sport) |
| Failure modes | repair_cooldown_ms (default 30s) при drifted_sports |
| Restart нужен | при изменении конфигурации спортов / cadence |

### `sofascore-historical-planner.service` (singleton)

| Команда | `run_service.sh historical-planner-daemon` |
| Класс | `HistoricalPlannerDaemon` (`D:\sofascore\schema_inspector\services\historical_planner.py`) |
| Публикует в | `STREAM_HISTORICAL_DISCOVERY` |
| Cursor | `etl_planner_cursor` (rolling window) + Redis state |
| Notes | На проде часто отключается во время кризисов live-tier, чтобы освободить hydrate. |

### `sofascore-historical-tournament-planner.service` (singleton)

| Команда | `run_service.sh historical-tournament-planner-daemon` |
| Класс | `HistoricalTournamentPlannerDaemon` (`D:\sofascore\schema_inspector\services\historical_tournament_planner.py`) |
| Публикует в | `STREAM_HISTORICAL_TOURNAMENT` |
| Cursor | Redis hash + `etl_planner_cursor` |

### `sofascore-structure-planner.service` (singleton)

| Команда | `run_service.sh structure-planner-daemon` |
| Публикует в | `STREAM_STRUCTURE_SYNC` |
| Cursor | Redis hash per (sport, tournament) |
| Назначение | Поддержка скелета unique_tournament/season/event (без детализации). |

### `sofascore-tournament-registry-refresh.service` (singleton)

| Команда | `run_service.sh tournament-registry-refresh-daemon` |
| Класс | `TournamentRegistryService` |
| Пишет | `tournament_registry` table + Redis set |
| Назначение | Периодическое обновление списка управляемых турниров. |

### `sofascore-resource-planner.service` (singleton)

| Команда | `run_service.sh resource-planner-daemon --loop-interval-seconds 30 --publish-per-tick-cap 200 --lag-threshold 5000` |
| Класс | `ResourcePlannerDaemon` (`D:\sofascore\schema_inspector\services\resource_planner.py`) |
| Публикует в | `STREAM_RESOURCE_REFRESH` (generic endpoint refresh с freshness-driven cadence) |
| Cursor | `resource_cursor` (Redis) + `resource_negative_cache` (Redis set) |

### `sofascore-proxy-health.service` (singleton)

| Команда | `run_service.sh proxy-health-monitor` |
| Класс | `ProxyHealthMonitor` (`D:\sofascore\schema_inspector\services\proxy_health_monitor.py`) |
| Читает | `api_request_log` (за последний window) |
| Пишет | Redis proxy health state, помечает unhealthy в `zset:proxy:cooldown` |
| Env | `SOFASCORE_PROXY_HEALTH_*` |

### `sofascore-mobile-proxy-watchdog.service` (oneshot)

| Команда | `scripts/ops/mobile_proxy_watchdog.sh` |
| Тип | shell скрипт, periodic check (`oneshot`, запускается через timer если есть) |

---

## Worker'ы — Operational (live-first) lane

Все воркеры используют `WorkerRuntime` (`D:\sofascore\schema_inspector\services\worker_runtime.py`):
- читают stream через `XREADGROUP`
- запускают handler в asyncio
- классифицируют исключения через `retry_policy.is_retryable_worker_error`
- retryable → delay через `delayed_queue` (Redis zset:etl:delayed) + ACK; status `retry_scheduled` в `etl_job_run`
- неretryable / max delivery count exceeded → `STREAM_DLQ` + status `failed`
- ECR observation запись inline (после firebreak 2026-05-13 rollup пишется только batch'ем)

### `sofascore-discovery@N.service` (template)

| Поле | Значение |
|---|---|
| Команда | `run_service.sh worker-discovery --consumer-name discovery-%i --block-ms 5000` |
| Класс | `DiscoveryWorker` (`D:\sofascore\schema_inspector\workers\discovery_worker.py`) |
| Читает | `STREAM_DISCOVERY` (`cg:discovery`) |
| Публикует в | `STREAM_HYDRATE` + `STREAM_LIVE_TIER_1/2/3` (после X4 — фильтрует `isEditor=True`) |
| Пишет в БД | `event` (upsert), `api_payload_snapshot`, capability observation |
| Env | `SOFASCORE_DISCOVERY_WORKER_MAX_CONCURRENCY` |
| Failure modes | `DeadlockDetectedError` → retry; `RetryableJobError` → delayed; `AdmissionDeferredError` (backpressure) → defer |
| Restart нужен после | Изменения policy gates (match_center_policy.py, isEditor logic) |
| Лог | `journalctl -u "sofascore-discovery@*"` |
| Особое | **X4 layer 1**: `_SurfaceEvent.is_editor=True` → skip hydrate publish (см. `_normalize_surface_result`) |

### `sofascore-live-discovery@N.service` (template)

| Команда | `run_service.sh worker-live-discovery --consumer-name live-discovery-%i --block-ms 5000` |
| Класс | `DiscoveryWorker` (та же класса, но другой сcope — live) |
| Читает | `STREAM_LIVE_DISCOVERY` (`cg:live_discovery`) |
| Публикует в | `STREAM_LIVE_TIER_1/2/3` + `STREAM_HYDRATE` |
| Пишет в БД | `event`, capability observation, **`zset:live:hot/warm/cold`** (Redis), `live:event:*` hash |
| Failure modes | DB deadlock на upsert (event/country/team) — частая причина retry storm |
| Известный риск | Если discovery worker зависает в retry-loop, **новые live events не попадают в Redis live state** → live coverage деградирует (видели в инциденте 2026-05-13). |

### `sofascore-hydrate@N.service` (template)

| Команда | `run_service.sh worker-hydrate --consumer-name hydrate-%i --block-ms 5000` |
| Класс | `HydrateWorker` (`D:\sofascore\schema_inspector\workers\hydrate_worker.py`) |
| Читает | `STREAM_HYDRATE` (`cg:hydrate`) |
| Обрабатывает | `JOB_HYDRATE_EVENT_ROOT` через `PilotOrchestrator.run_event()` |
| Пишет в БД | `api_payload_snapshot`, `event_terminal_state`, normalized event tables (event_*), `endpoint_capability_observation`, `etl_job_run`, `etl_job_stage_run` |
| Пишет в Redis | `live:event:*` (live state), dispatch metrics |
| Env | `SOFASCORE_HYDRATE_WORKER_MAX_CONCURRENCY`, `HYDRATE_EVENT_CIRCUIT_BREAKER_ENABLED`, `SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED` |
| Failure modes | TimeoutError (libcurl ErrCode 28), DeadlockDetectedError, RetryableJobError, TransportExhaustedError |
| Restart нужен после | Изменения hydration policy, parser registry, env capability flag |
| Лог | `journalctl -u "sofascore-hydrate@*"` |

### `sofascore-live-tier-1@N.service`, `sofascore-live-tier-2@N.service`, `sofascore-live-tier-3@N.service` (templates)

| Команда | `run_service.sh worker-live-tier-{1|2|3} --consumer-name live-tier-N-%i --block-ms 5000` |
| Класс | `LiveWorkerService` (`D:\sofascore\schema_inspector\workers\live_worker_service.py`) |
| Читает | `STREAM_LIVE_TIER_{1|2|3}` (`cg:live_tier_{1|2|3}`) |
| Обрабатывает | `JOB_REFRESH_LIVE_EVENT` через `PilotOrchestrator.run_event(hydration_mode="live_delta")` |
| Публикует в | `STREAM_LIVE_DETAILS` (если включён P0(a) split, `LIVE_SPLIT_DETAILS_FANOUT=1`) |
| Пишет в БД | normalized event tables, `event_live_state_history`, `api_payload_snapshot`, capability obs |
| Env | `SOFASCORE_LIVE_TIER_{1\|2\|3}_WORKER_MAX_CONCURRENCY`, `LIVE_DETAILS_STREAM_BACKPRESSURE_LIMIT` |
| Особое | tier_1 — топ-матчи, poll=5s; tier_2 — медиум, poll=15-30s; tier_3 — long-tail, poll=30-90s. Throughput тir 1 особо чувствителен к `LIVE_DISPATCH_LEASE_TIER_1_MS`. |

### `sofascore-live-warm@N.service` (template)

| Команда | `run_service.sh worker-live-warm --consumer-name live-warm-%i --block-ms 5000` |
| Читает | `STREAM_LIVE_WARM` (`cg:live_warm`) |
| Назначение | Warm lane — события, переходящие в активную фазу, hydration scope="warm" |

### `sofascore-live-hot@N.service` (template, обычно 1 экземпляр)

| Команда | `run_service.sh worker-live-hot --consumer-name live-hot-%i --block-ms 5000` |
| Читает | `STREAM_LIVE_HOT` (`cg:live_hot`) |
| Notes | High-priority обновления (sport_live_events list refresh). Прод-параметр обычно `live-hot@1`. |

### `sofascore-live-details@N.service` (template, P0(a) split)

| Команда | `run_service.sh worker-live-details --consumer-name live-details-%i --block-ms 5000` |
| Класс | `LiveDetailsWorkerService` (`D:\sofascore\schema_inspector\workers\live_details_worker_service.py`) |
| Читает | `STREAM_LIVE_DETAILS` (`cg:live_details`) |
| Обрабатывает | `JOB_REFRESH_LIVE_EVENT_DETAILS` (per-player endpoint fanout) |
| Включается | `LIVE_SPLIT_DETAILS_FANOUT=1` (если 0 — stream пустой) |
| Env | `SOFASCORE_LIVE_DETAILS_WORKER_MAX_CONCURRENCY` |

### `sofascore-maintenance@N.service` (template)

| Команда | `run_service.sh worker-maintenance --consumer-name maintenance-%i --block-ms 5000` |
| Класс | `MaintenanceWorker` (`D:\sofascore\schema_inspector\workers\maintenance_worker.py`) |
| Читает | `STREAM_MAINTENANCE` (`cg:maintenance`) |
| Назначение | Reclaim stale entries (XAUTOCLAIM) из операциональных streams, retry или DLQ |
| Reclaim targets | `STREAM_DISCOVERY`, `STREAM_HYDRATE`, `STREAM_LIVE_TIER_*`, `STREAM_LIVE_HOT`, `STREAM_LIVE_WARM` |
| Также | Tier_1 quarantine (`live:tier1_quarantine:*` keys) |

### `sofascore-normalize@N.service` (template)

| Команда | `run_service.sh worker-normalize --consumer-name normalize-%i --block-ms 5000` |
| Класс | `NormalizeWorker` (через `D:\sofascore\schema_inspector\normalizers\worker.py`) |
| Читает | `STREAM_NORMALIZE` (`cg:normalize`) |
| Назначение | Re-process raw snapshot rows через parser registry (durable normalize sink) |
| Пишет в БД | normalized event tables |

### `sofascore-resource-refresh@N.service` (template)

| Команда | `run_service.sh worker-resource-refresh --consumer-name resource-refresh-%i --block-ms 5000` |
| Класс | `ResourceRefreshWorker` (`D:\sofascore\schema_inspector\workers\resource_refresh_worker.py`) |
| Читает | `STREAM_RESOURCE_REFRESH` (`cg:resource_refresh`) |
| Обрабатывает | `JOB_REFRESH_RESOURCE` (endpoint-agnostic refresh) |
| Env | `SOFASCORE_RESOURCE_REFRESH_WORKER_MAX_CONCURRENCY` |
| Особенности | freshness skip, empty-data skip, negative cache skip — самый «гибкий» worker, который умеет early-exit |

### `sofascore-structure-sync.service` (singleton)

| Команда | `run_service.sh worker-structure-sync --consumer-name structure-sync-1 --block-ms 5000` |
| Класс | `StructureSyncWorker` (через `D:\sofascore\schema_inspector\services\structure_sync_service.py`) |
| Читает | `STREAM_STRUCTURE_SYNC` (`cg:structure_sync`) |
| Назначение | Скелетный обход tournament → season → events (без полной hydration) |

---

## Worker'ы — Historical lane

Менее приоритетные, на проде в кризисы часто **отключаются**, чтобы освободить ресурсы для live.

### `sofascore-historical-discovery@N.service` (template)
| Read | `STREAM_HISTORICAL_DISCOVERY` |
| Write | `STREAM_HISTORICAL_HYDRATE` (fanout событий за дату) |

### `sofascore-historical-hydrate@N.service` (template, обычно 9-10 экз)
| Read | `STREAM_HISTORICAL_HYDRATE` |
| Обрабатывает | `JOB_HYDRATE_EVENT_ROOT` (historical scope) |
| Env | `SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY` |

### `sofascore-historical-tournament@N.service` (template)
| Read | `STREAM_HISTORICAL_TOURNAMENT` |
| Write | `STREAM_HISTORICAL_ENRICHMENT` |

### `sofascore-historical-enrichment@N.service` (template)
| Read | `STREAM_HISTORICAL_ENRICHMENT` |
| Назначение | Post-tournament enrichment: leaderboards, standings, top players за прошедший сезон |

### `sofascore-historical-maintenance@N.service` (template)
| Read | `STREAM_HISTORICAL_MAINTENANCE` |
| Назначение | Reclaim для historical lanes |

---

## Local API server

### `sofascore-api.service` (singleton)

| Поле | Значение |
|---|---|
| Команда | `uvicorn schema_inspector.local_api_server:create_asgi_app --factory --host 0.0.0.0 --port 8000 --workers 4` |
| Класс | `LocalApiApplication` (`D:\sofascore\schema_inspector\local_api_server.py`) |
| Зависимости | PostgreSQL (read-only), Redis (для /ops/* queue summary) |
| Port | 8000 |
| Endpoints | `/api/v1/*` (Sofascore-shape), `/ops/*`, `/openapi.json`, `/` (Swagger UI), `/docs` |
| Restart нужен после | code change в local_api_server.py, endpoints.py, parsers/normalizers (если используются для synthesizer) |
| Failure modes | Long DB queries в /ops/health → 500 Internal Server Error (transactionid wait); race против lock'ов в `endpoint_capability_rollup` (известная проблема, firebreak'нут 2026-05-13) |
| Лог | `journalctl -u sofascore-api.service` |
| Подробности | [API_ROUTES.md](API_ROUTES.md) |

---

## Совместная failure surface — что мониторить

Все воркеры имеют **общий retry policy** (`D:\sofascore\schema_inspector\services\retry_policy.py`):

| Exception | Retryable? | SQLSTATE |
|---|---|---|
| `RetryableJobError` | yes | — |
| `TimeoutError` (asyncio.wait_for) | yes | — |
| `RequestsError` (curl_cffi) | yes | — |
| `DeadlockDetectedError` (asyncpg) | yes | 40P01 |
| `LockNotAvailableError` (asyncpg) | yes | 55P03 |
| `SofascoreHttpError` (4xx/5xx) | по классификации | — |
| Прочее | **no** → DLQ | — |

Структурированный retry log (после firebreak 2026-05-13):
```
WARNING worker_runtime: Worker hydrate-worker scheduling retry:
  stream=stream:etl:hydrate
  message_id=...
  delay_ms=10000
  exc_type=DeadlockDetectedError
  exc=DeadlockDetectedError('deadlock detected')
  sqlstate=40P01
  duration_ms=49644
```

Раньше было только `exc=`, теперь все 4 поля — журнал теперь self-сlassifying.

---

## Сводная таблица — сколько экземпляров на проде

Текущая конфигурация (на момент инциндент-аудита 2026-05-13):

| Service | Экз |
|---|---:|
| sofascore-api | 1 (uvicorn с workers=4) |
| sofascore-planner | 1 |
| sofascore-live-discovery-planner | 1 |
| sofascore-historical-planner | 1 |
| sofascore-historical-tournament-planner | 1 |
| sofascore-structure-planner | UNKNOWN (может быть disabled) |
| sofascore-tournament-registry-refresh | 1 |
| sofascore-resource-planner | 1 |
| sofascore-proxy-health | 1 |
| sofascore-discovery@ | 1 |
| sofascore-live-discovery@ | 1 |
| sofascore-hydrate@ | 9 (instances 1-9) |
| sofascore-live-tier-1@ | 5 |
| sofascore-live-tier-2@ | 4 |
| sofascore-live-tier-3@ | 9 |
| sofascore-live-warm@ | 3 |
| sofascore-live-hot@ | 1 |
| sofascore-live-details@ | UNKNOWN (P0(a), включается флагом) |
| sofascore-maintenance@ | 1 |
| sofascore-normalize@ | 1 |
| sofascore-resource-refresh@ | 10 |
| sofascore-historical-discovery@ | 1 |
| sofascore-historical-hydrate@ | 10 |
| sofascore-historical-tournament@ | 1 |
| sofascore-historical-enrichment@ | 1 |
| sofascore-historical-maintenance@ | 1 |
| sofascore-structure-sync | 1 |

Конкретное состояние можно посмотреть `systemctl list-units 'sofascore-*' --type=service` на проде.

---

## Когда какой restart?

| Изменилось | Что рестартить |
|---|---|
| `.env` flag для live-dispatch / lease | `sofascore-planner.service` |
| `.env` flag для capability rollup | All hot workers (hydrate, live-tier-*, live-warm, live-hot, live-discovery, discovery, live-details) |
| Код парсеров / orchestrator | All workers + API |
| Endpoints.py (новый endpoint) | API (для swagger), все workers (если endpoint в hydration scope) |
| Local API code | `sofascore-api.service` |
| Discovery / live policy | All discovery + live workers |

Подробный runbook → [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md).
