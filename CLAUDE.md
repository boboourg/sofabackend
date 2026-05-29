# CLAUDE.md

Единый источник правды по проекту. Этот документ описывает **что реально есть в коде**, а не желаемое. Цель — позволить новой сессии Claude (или человеку) войти в проект без чтения 30+ исторических `.md` файлов.

**Дата актуализации:** 2026-05-24. Документ обновляется при каждом крупном архитектурном изменении (новый stream, новый worker, удаление дубликата, изменение точки входа).

---

## 1. Что мы строим

Sofascore-подобный коммерческий ETL/API-стек: ингест данных, нормализация в Postgres, локальный read API в формате Sofascore (`/api/v1/...`), плюс WebSocket-mirror для фронта.

**Бизнес-цели:**
- Mobile app + web frontend + публичное API.
- 5000 активных пользователей за 6–12 месяцев.
- Drop-in replacement для Sofascore API (1:1 shape payload).
- "Wikipedia-level" история: 12 спортов × все лиги × прошлые сезоны.

**Технические цели (SLO):**
- Live data lag ≤ 5 секунд (сейчас ~5s polling, цель push-based ~100ms).
- Downtime < 30 минут.
- Live coverage 100% активных матчей.

**Не в скоупе:** хранение медиа-файлов (URL строятся фронтом из `https://img.sofascore.com/...`), микросервисы, multi-region, k8s.

Подробнее: [docs/PROJECT_VISION.md](docs/PROJECT_VISION.md).

---

## 2. Реальная архитектура (как код работает сейчас)

### Pipeline (poll-based, основная ветка)

```
┌─────────────┐    ┌─────────────────┐    ┌────────────────┐
│   Planners  │ →  │  Redis Streams  │ →  │    Workers     │
│  (systemd)  │    │  (15 streams)   │    │  (16 типов)    │
└─────────────┘    └─────────────────┘    └────────┬───────┘
                                                    │
                                                    ▼
                          ┌─────────────────────────────────┐
                          │  ParserRegistry (27 семейств)   │
                          └────────────────┬────────────────┘
                                           │
                                           ▼
                          ┌─────────────────────────────────┐
                          │   normalize_repository.py       │
                          │   (durable PostgreSQL sink)     │
                          └────────────────┬────────────────┘
                                           │
                                           ▼
                          ┌─────────────────────────────────┐
                          │  Postgres 16 + materialized v.  │
                          └────────────────┬────────────────┘
                                           │
                                           ▼
                          ┌─────────────────────────────────┐
                          │  local_api_server.py (FastAPI)  │
                          │  + /ops/* endpoints             │
                          └─────────────────────────────────┘
```

### Pipeline (push-based, WS-ветка — работает параллельно)

```
wss://ws.sofascore.com:9222
        │
        ▼
sofascore-ws-consumer.service
   ├─ ws_nats_parser.parse_nats_frames()
   ├─ ws_delta_normalizer.normalize_event_delta()
   ├─ ws_delta_writer.apply_event_delta()    ← пишет в event_score, event_time,
   │                                            event_status, event_status_time,
   │                                            event_var_in_progress
   ├─ ws_odds_writer.apply_odds_delta()      ← пишет в event_market_choice
   ├─ DELETE FROM event_payload_cache        ← инвалидация кэша
   └─ ws_fanout_publisher → Redis pub/sub
                            │
                            ▼
                  sofascore-ws-server.service (127.0.0.1:8001)
                            │
                            ▼ wss://api.var11.com/ws/v1
                       (фронт-клиенты)
```

### Точка входа

Всё работает через единый CLI:

```
python -m schema_inspector.cli <subcommand> [args...]
```

`schema_inspector/cli.py` (3669 строк) — argparse + 46+ subcommands + диспетчер. 34 из них запускаются через systemd unit'ы.

### Полный список subcommands (по категориям)

**Один-shot (ручной запуск):**
- `health` — проверка работоспособности
- `event --sport-slug X --event-id N` — гидрировать одно событие
- `live --sport-slug X` — гидрировать живые матчи
- `scheduled --sport-slug X --date YYYY-MM-DD` — расписание
- `full-backfill` — полный бекфилл
- `replay --snapshot-id N` — переиграть снапшот
- `audit-db --sport-slug X --event-id N` — аудит БД
- `recover-live-state` — восстановление состояния live

**Continuous planners (systemd-daemon'ы):**
- `planner-daemon` — основной планнер scheduled/refresh
- `live-discovery-planner-daemon` — поиск live матчей
- `historical-planner-daemon` — дальняя история
- `historical-tournament-planner-daemon` — turnir-driven backfill
- `final-sync-planner-daemon` — финализация терминальных событий
- `structure-planner-daemon` — синк структуры турниров
- `resource-planner-daemon` — рефреш ресурсов (TotW, leaderboards)
- `live-rescue-daemon` — спасение зависших live

**Continuous workers (systemd-daemon'ы):**
- `worker-discovery` — расширяет sport-surface jobs
- `worker-live-discovery` — live-сurface jobs
- `worker-historical-discovery` — historical discovery
- `worker-hydrate` — гидрация event root
- `worker-historical-hydrate` — гидрация архива
- `worker-historical-tournament` — backfill турнира
- `worker-historical-enrichment` — обогащение
- `worker-historical-bootstrap` — bootstrap новых турниров (Phase 4.7.7)
- `worker-live-hot` / `worker-live-warm` — live lanes по freshness
- `worker-live-tier-1`/`-2`/`-3` — shard по приоритету матча
- `worker-live-details` — per-player детали (split fanout)
- `worker-maintenance` — housekeeping/zombie cleanup
- `worker-historical-maintenance` — то же для historical
- `worker-normalize` — durable normalize sink (опционально)
- `worker-resource-refresh` — обновление ресурсов
- `worker-structure-sync` — синк структуры

**WebSocket:**
- `ws-consumer` — клиент к Sofascore WS, пишет дельты в БД
- `ws-server` — наш mirror-сервер для фронта (127.0.0.1:8001)

**Ops / admin:**
- `proxy-health-monitor`, `monitoring-daemon`, `api-cache-warmer`
- `coverage-refresh`, `rebuild-capability-rollup`, `backfill-cursor*`
- `league-capability prime-redis` — pre-warm Redis для hot path

**Локальный API:**
```
python -m schema_inspector.local_api_server --host 127.0.0.1 --port 8000
```

Подробный каталог команд: [docs/CLI_AND_SCRIPTS.md](docs/CLI_AND_SCRIPTS.md).

### Redis streams (всего 15, обязательно в проде)

**Operational lane (живой ввод):**
- `stream:etl:discovery` → `cg:discovery`
- `stream:etl:live_discovery` → `cg:live_discovery`
- `stream:etl:hydrate` → `cg:hydrate`
- `stream:etl:live_hot` / `live_warm` / `live_tier_1` / `live_tier_2` / `live_tier_3` / `live_details`
- `stream:etl:maintenance`

**Historical lane (изолированная):**
- `stream:etl:historical_discovery` / `historical_tournament` / `historical_bootstrap` / `historical_enrichment` / `historical_hydrate` / `historical_maintenance`

**Сервисные:**
- `stream:etl:structure_sync`, `stream:etl:resource_refresh`, `stream:etl:normalize`

Источник правды: [schema_inspector/queue/streams.py](schema_inspector/queue/streams.py).
Полная таблица + consumer groups: [docs/REDIS_AND_QUEUES.md](docs/REDIS_AND_QUEUES.md).

### systemd units (36 файлов)

В `ops/systemd/` — 36 unit'ов. Шаблоны (`name@.service`) разворачиваются как `sofascore-hydrate@1`, `@2`, ... для параллелизма.

Активные сейчас (типично на проде):
```
sofascore-api.service              # local_api_server, port 8000
sofascore-planner.service          # planner-daemon
sofascore-live-discovery-planner   # live discovery
sofascore-historical-planner       # historical bulk planner
sofascore-historical-tournament-planner
sofascore-structure-planner
sofascore-resource-planner
sofascore-final-sync-planner
sofascore-live-rescue              # терминальные события

sofascore-discovery@1..3
sofascore-live-discovery@1
sofascore-hydrate@1..N
sofascore-live-hot@1, live-warm@1
sofascore-live-tier-1@1..3, tier-2@1, tier-3@1
sofascore-live-details@1

sofascore-historical-hydrate@1..2
sofascore-historical-tournament@1..3   # OFF на проде
sofascore-historical-enrichment@1..2   # OFF на проде
sofascore-historical-bootstrap@1..3
sofascore-historical-maintenance@1
sofascore-historical-discovery@1

sofascore-maintenance@1
sofascore-normalize@1
sofascore-structure-sync.service
sofascore-resource-refresh@1
sofascore-tournament-registry-refresh

sofascore-cache-warmer.service
sofascore-proxy-health.service
sofascore-mobile-proxy-watchdog.{service,timer}
sofascore-monitoring.service        # alerter с Telegram

sofascore-ws-consumer.service       # Sofascore → нам
sofascore-ws-server.service         # нам → фронт
```

Все unit'ы запускают `/opt/sofascore/deploy/run_service.sh <command>`, который exec'ает `python -m schema_inspector.cli <command>`.

Подробно: [docs/SERVICES_AND_WORKERS.md](docs/SERVICES_AND_WORKERS.md).

---

## 3. Структура репозитория

```
sofabackend/
├── CLAUDE.md                         ← этот файл (источник правды)
├── README.md                         ← краткий quickstart
├── requirements.txt                  ← Python deps
├── postgres_schema.sql               ← полный DDL для bootstrap
│
├── schema_inspector/                 ← основной модуль (~50K LOC)
│   ├── cli.py                        ← все subcommands (3669 строк)
│   ├── local_api_server.py           ← FastAPI server (6123 строк)
│   ├── runtime.py, service.py        ← конфиг транспорта + schema inspection
│   ├── endpoints.py                  ← каталог 105+ endpoint records (1698)
│   ├── transport.py                  ← HTTP transport (httpx + retry + proxy)
│   ├── sofascore_client.py           ← клиент
│   ├── sport_profiles.py             ← 7 sport profiles
│   │
│   ├── services/                     ← 35 файлов
│   │   ├── service_app.py            ← god-factory (1977 строк, см. §6)
│   │   ├── planner_daemon.py
│   │   ├── live_discovery_planner.py
│   │   ├── historical_planner.py
│   │   ├── historical_tournament_planner.py
│   │   ├── live_state_sweeper.py     ← P0.B sweeper (см. §5)
│   │   ├── worker_runtime.py
│   │   ├── housekeeping.py
│   │   ├── ws_consumer_service.py    ← WS клиент к Sofascore
│   │   ├── ws_server_service.py      ← WS сервер для фронта
│   │   └── ...
│   │
│   ├── workers/                      ← 14 файлов
│   │   ├── discovery_worker.py
│   │   ├── hydrate_worker.py
│   │   ├── live_worker_service.py    ← live stream consumer
│   │   ├── live_worker.py            ← state machine helper (плохое имя, см. §6)
│   │   ├── live_details_worker_service.py
│   │   ├── historical_archive_worker.py
│   │   ├── maintenance_worker.py
│   │   ├── normalize_worker.py       ← shim (см. §6)
│   │   ├── resource_refresh_worker.py
│   │   ├── structure_worker.py
│   │   └── ...
│   │
│   ├── parsers/
│   │   ├── registry.py               ← ParserRegistry, 27 семейств
│   │   ├── base.py, classifier.py
│   │   ├── families/                 ← event_root, lineups, incidents, ...
│   │   ├── special/                  ← baseball, tennis, shotmap
│   │   └── sports/                   ← 13 sport adapters (тонкие конфиги)
│   │
│   ├── storage/                      ← 17 repositories
│   │   ├── normalize_repository.py   ← главный sink (2565 строк)
│   │   ├── raw_repository.py
│   │   ├── event_payload_cache_repository.py
│   │   ├── coverage_repository.py    ← mv_season_coverage
│   │   ├── capability_repository.py  ← league_capabilities
│   │   ├── live_state_repository.py  ← Redis live state
│   │   └── ...
│   │
│   ├── queue/                        ← 20 Redis primitives
│   │   ├── streams.py                ← stream registry, consumer groups
│   │   ├── delayed.py, leases.py
│   │   ├── live_state.py, live_inflight.py
│   │   ├── freshness.py, dedupe.py
│   │   └── ...
│   │
│   ├── parsers/, normalizers/, sources/, ops/, monitoring/, pipeline/, planner/,
│   │   cache_warmer/, jobs/
│   │
│   ├── ws_nats_parser.py             ← парсер NATS frames из Sofascore WS
│   ├── ws_delta_normalizer.py        ← нормализация дельт
│   ├── ws_delta_writer.py            ← пишет в event_score/time/status
│   ├── ws_odds_writer.py             ← пишет в event_market_choice
│   ├── ws_fanout_publisher.py        ← Redis pub/sub
│   ├── ws_server_protocol.py         ← NATS-subset для фронта
│   │
│   └── ...
│
├── migrations/                       ← 70 SQL миграций, append-only
│
├── tests/                            ← 304 файла, 0 skip/xfail
│
├── docs/                             ← техническая документация
│   ├── ARCHITECTURE_AUDIT.md         ← live risk register
│   ├── PERFORMANCE_AUDIT_2026-05-20.md
│   ├── SIMPLIFICATION_AUDIT.md       ← где god-objects, как резать
│   ├── PROJECT_VISION.md             ← vision + roadmap по эпохам
│   ├── PROJECT_OVERVIEW.md, current-runtime-architecture.md
│   ├── SERVICES_AND_WORKERS.md, CLI_AND_SCRIPTS.md
│   ├── ENVIRONMENT.md                ← каталог ENV переменных
│   ├── REDIS_AND_QUEUES.md, DATABASE_AND_STORAGE.md
│   ├── API_ROUTES.md, PARSING_AND_POLICIES.md
│   ├── OPERATIONS_RUNBOOK.md, HANDOVER.md
│   ├── PROGRESS.md, 24x7-exit-criteria.md
│   ├── FRONTEND_WS_GUIDE.md          ← WS protocol для фронта
│   ├── POSTGRES_SCHEMA_OVERVIEW.md, DB_TREE_OVERVIEW.md
│   ├── sofascore_api.md
│   ├── pipeline-tree.md, endpoint-ttl-matrix.md, football-matrix.md
│   ├── parser-reliability-audit-2026-05-16.md
│   ├── REDUNDANT_ENDPOINTS_AUDIT.md
│   ├── COVERAGE_GAP_2015_2019.md
│   ├── live-first-rollout.md, live-ready-runbook.md
│   ├── season_widget_negative_cache_shadow_guide.md
│   ├── N1_MONITORING_PLAN.md, N4_API_PERFORMANCE_PLAN.md
│   ├── BACKFILL_PRIORITIES.md, backfill-ordering-roadmap.md
│   ├── 2026-04-17-production-deployment-guide.md
│   ├── archived/                     ← устаревшие/rejected experiments
│   ├── runbooks/                     ← prod-cutover, prod-gates, monitoring
│   └── superpowers/
│       ├── plans/                    ← активные планы (4 файла)
│       └── specs/                    ← активные спецификации (1 файл)
│
├── scripts/
│   ├── prod_readiness_check.py       ← baseline live-ready check
│   ├── audit_*.py, backfill_*.py
│   ├── cascade_restart_workers.sh
│   ├── sync_schedule.sh
│   ├── ops/                          ← proxy_health_check, mobile_proxy
│   └── dev-utils/                    ← probe_*.py, serve_local_swagger.py
│
├── ops/
│   ├── systemd/                      ← 36 .service unit'ов
│   └── nginx/                        ← конфиги
│
├── deploy/
│   └── run_service.sh                ← exec wrapper для systemd
│
├── tools/                            ← dev tooling
│   ├── smoke_*.py, analyze_socket_archive.py, replay_socket_archive.py
│   ├── sniff_ws_odds.py
│   └── archived/
│
├── config/
│   └── backfill_priorities.example.yaml
│
└── local_swagger/                    ← статические OpenAPI артефакты
```

---

## 4. WebSocket: текущее состояние и gap до цели

**Цель в видении проекта:** "сокет триггерит ручки" — WS должен заменить poll-based архитектуру для live матчей.

**Что уже работает:**

| Компонент | Что делает |
|---|---|
| `sofascore-ws-consumer.service` | Подключается к `wss://ws.sofascore.com:9222` (Sofascore's NATS-protocol WS). Парсит фреймы [ws_nats_parser.py](schema_inspector/ws_nats_parser.py), нормализует дельты [ws_delta_normalizer.py](schema_inspector/ws_delta_normalizer.py), пишет в 6 normalized таблиц через [ws_delta_writer.py](schema_inspector/ws_delta_writer.py:60) + `event_market_choice` через [ws_odds_writer.py](schema_inspector/ws_odds_writer.py). **Инвалидирует `event_payload_cache`** ([ws_delta_writer.py:71](schema_inspector/ws_delta_writer.py:71)). Latency Sofascore→БД <100ms. |
| `sofascore-ws-server.service` | Mirror Sofascore-NATS-протокола для нашего фронта на `127.0.0.1:8001`. Nginx терминирует TLS → `wss://api.var11.com/ws/v1`. Фронт-протокол описан в [docs/FRONTEND_WS_GUIDE.md](docs/FRONTEND_WS_GUIDE.md). |
| `ws_fanout_publisher.py` | Redis pub/sub между consumer и server. |

**Что НЕ работает (gap до цели):**

1. **WS-дельта не триггерит refetch других endpoints.** Пример: пришла дельта `status.type=inprogress` → таблица `event_status` обновилась → но никто не запускает fetch `/statistics`, `/incidents`, `/lineups`. Эти endpoints обновляются только через polling tier_1/tier_2/tier_3 (5–90 сек).
2. **WS не отменяет polling.** Сейчас WS работает **в дополнение** к polling, а не вместо. Двойная нагрузка.
3. **Часть полей WS не сохраняется**: `period5` (TT/волейбол), `point` (теннис) — комментарий в [ws_delta_writer.py:36–40](schema_inspector/ws_delta_writer.py:36) указывает что нужны миграции.
4. **Reconnect-стратегия** клиента простая (10s backoff, см. `sofascore-ws-consumer.service:18`). Нет sentinel-probe для тихого drift Sofascore schema.

**Архитектурное направление (Эпоха 3 в PROJECT_VISION):**
- WS-дельта классифицирует тип изменения → публикует `hydrate_event_root` или `hydrate_special_route` job в Redis stream → tier-1 worker делает targeted fetch только тех endpoints, которые реально устарели.
- Polling становится fallback для матчей без WS-coverage (старые/нишевые турниры).
- Sentinel probe сравнивает дельты от WS с результатом одиночного poll → catches schema drift.

---

## 5. Известные нестабильности (правда о коде)

Эти проблемы **зафиксированы в коде** или в свежих аудитах. Указаны file:line там где можно.

### 5.1 Critical (live path может сломаться)

| # | Проблема | Где | Impact |
|---|---|---|---|
| 1 | `event_market_choice` — **28.7 миллиардов seq_tup_read** на 136MB таблице. Нет нужного индекса для access pattern. | [storage/normalize_repository.py](schema_inspector/storage/normalize_repository.py) — find_one/find_many on choice | API queries в odds-секции медленные; planner может затормозиться. |
| 2 | ~~**HydrateWorker нет in-flight lock**~~ **✅ РЕШЕНО (`d7ab82a`, 2026-05-20).** `HydrateInFlightStore` (`queue/live_inflight.py:146`, `hydrate:inflight:{id}` SETNX+Lua, TTL 300s), wired `service_app.py:1290`, claim/release `hydrate_worker.py:229-303`. Historical-hydrate намеренно БЕЗ него. | [workers/hydrate_worker.py:229-303](schema_inspector/workers/hydrate_worker.py) | (исторический контекст: race overlapping UPSERTs — закрыт) |
| 3 | `event.custom_id` — **нет индекса**, используется в `IN-list` reconcile path. | [local_api_server.py:1170-1171](schema_inspector/local_api_server.py:1170), [event_list_repository.py:249](schema_inspector/event_list_repository.py:249) | Seq scan на event (4.5M rows) при каждом `/scheduled-events/{date}` если есть custom_id match. |
| 4 | **Hydrate lock leak на crash** в `App.run_event`. Нет try/finally вокруг acquire→release. | [cli.py — поиск `acquire_hydrate_lock`](schema_inspector/cli.py) (был на line 411-473 в audit 2026-05-20) | 60 сек blocked live polling после оператор-triggered crash. |
| 5 | `/ops/snapshots/summary?detail=true` — 5 `COUNT(DISTINCT)` на `api_payload_snapshot` (148GB). | [local_api_server.py:4106-4119](schema_inspector/local_api_server.py:4106) | Гарантированный таймаут. Нужно gate за operator role. |
| 6 | **OFFSET pagination на event-table**. `_FETCH_QUERY_SEASON_LAST/NEXT/TEAM/PLAYER`. | [scheduled_events_synthesizer.py:241-407](schema_inspector/scheduled_events_synthesizer.py:241) | Page 10 → 300 rows materialized. Player Messi page 20 → 600 rows + 15 JOINs. |
| 7 | ~~`release_hydrate_lock` не atomic~~ **✅ РЕШЕНО (`d7ab82a`/`749475e`, 2026-05-20).** Lua GET+DEL-if-owner (`live_bootstrap.py:34-119`) + try/finally release на каждом exit-пути `run_event`. | [live_bootstrap.py:34-119](schema_inspector/live_bootstrap.py:34) | (исторический контекст: чужой-lock race — закрыт) |
| 8 | `LiveEventState.upsert + move_lane` — HSET → ZREM × 3 → ZADD = 5 Redis ops, не atomic. | [queue/live_state.py:59-89](schema_inspector/queue/live_state.py:59) | Planner может увидеть event в обеих lanes или ни в одной. |
| 9 | **`/event/h2h` parser — 11.5% silent drops** (13 из 113 events за 2h). | [parsers/families/event_h2h.py](schema_inspector/parsers/families/event_h2h.py) | Реальный parser bug на edge-case payload shape. |
| 10 | **Live-discovery — single instance + один большой persist**. Под evening peak (Champions League) может зависнуть. | [services/live_discovery_planner.py](schema_inspector/services/live_discovery_planner.py) + [workers/discovery_worker.py](schema_inspector/workers/discovery_worker.py) | Если worker stuck — НОВЫЕ events не попадают в `zset:live:hot`, live coverage padает. См. ARCHITECTURE_AUDIT §B.1. |
| 11 | **Топ live-матчи без матч-центра при медленных прокси** (2026-05-29 ревью event 15728277). Два компаундящихся блокера ДО фетча edges: (#1) root `/event` fetch таймаутил на `SOFASCORE_FETCH_TIMEOUT_SECONDS=10` под proxy-contention → `run_event` бросал `RetryableJobError` (`pilot_orchestrator.py:587`) до statistics/lineups/incidents → `LIVE_TIER_1_QUARANTINE` skip; (#2) `upsert_snapshot_head` ловил `lock_timeout=5s` (sqlstate 55P03, retryable) на shared dimension-строках. **Оба усилены historical-воркерами** (конкуренция за прокси + DB-локи/общий пул). | `pilot_orchestrator.py:587`, `fetch_executor.py:85`, `raw_repository.py:444` | Матч полностью покрыт Sofascore, парсеры исправны, но 19 ручек NO_FETCH → фронт 404. **Митигация (2026-05-29):** timeout 10→`20` + остановлены historical-воркеры → fetch-timeout 12→0/окно, lock 16→4, quarantine 0. Остаток: §5.1 #2 lock-контенция (live-discovery + tier на shared rows). |

Полные детали: [docs/PERFORMANCE_AUDIT_2026-05-20.md](docs/PERFORMANCE_AUDIT_2026-05-20.md) + [docs/ARCHITECTURE_AUDIT.md](docs/ARCHITECTURE_AUDIT.md).

### 5.2 Warning (накапливается технический долг)

- **Stale entries в `zset:live:hot`**. После SIGKILL planner-а — terminal events не удаляются. Oldest age 76→22→40min тренд. Mitigation: [services/live_state_sweeper.py](schema_inspector/services/live_state_sweeper.py). Один баг в `ServiceApp._build_live_state_sweep_wiring` (closure обращался к `self.database` вместо `self.app.database`) делал sweeper **полностью dead**. Зафикшен в коммите `724d77f` (см. [PROJECT_VISION §11](docs/PROJECT_VISION.md)).
  - **(2026-05-29) Вторая, доминирующая причина `oldest_hot_score_age` breach: `next_poll_at` freeze на transient-фейле.** Retryable root `/event/{id}` fetch (таймаут прокси при `SOFASCORE_FETCH_TIMEOUT_SECONDS=10`) бросает `RetryableJobError` в `pilot_orchestrator.run_event` **до** `track_event` — единственного места, где `next_poll_at` (= ZSCORE в hot/warm/cold) сдвигается вперёд. Живое событие замораживалось на старом overdue-score (наблюдали 20 мин > 600с TTL), планнер ре-диспатчил его каждый тик (coalesce-шторм), метрика росла неограниченно. Эти события **не зомби и не terminal** — sweeper тут бессилен. Фикс: `LiveWorker.reschedule_after_transient_failure` пере-zadd'ит `next_poll_at = now + tier_cadence` (floored 15с) + сбрасывает claim на failure-ветке; данные о свежести не трогаются. Коммит `12ad2f9`. Проверено на проде: oldest_hot 1084с→87с, refresh 96%→99.3%, 0 событий age>600с. **Остаточный bounded gap:** reschedule не срабатывает на coalesce-пути (занят inflight-маркер) — ограничено 600с TTL маркера, проявляется лишь при краше/зависании воркера.
- **Historical lanes давят на operational pool**. Общий asyncpg pool. Mitigation deferred (P1.2 в ARCHITECTURE_AUDIT).
- **DeadlockDetectedError 39×/24h в hydrate-worker**, retries по ~200s = 130 минут потерянной работы в день. См. [docs/parser-reliability-audit-2026-05-16.md §5.1](docs/parser-reliability-audit-2026-05-16.md).
- **TimeoutError ~13/24h** на хроник-timeout events (ITF M15, KBO). Per-event Sofascore throttle, не наш транспорт.
- **9 endpoints без парсера** (silent drop 100%): `/highlights`, `/team-streaks`, `/average-positions`, player `/heatmap`, `/shotmap/player/{pid}`, `/goalkeeper-shotmap/player/{pid}`, `/official-tweets`. Снапшоты сохраняются в `api_payload_snapshot`, но нет normalized rows.
- **Player `team_id` nullified на conflict**. Когда player создаётся раньше team — `team_id := NULL`, back-fill не происходит. ~4 случая/день.
- **80–90% 404 rate на 4 endpoints**: `/managers` 70%, `/official-tweets` 90%, `/season/*/player-of-the-season` 89%. Mitigation: negative cache + capability gating (см. [docs/parser-reliability-audit-2026-05-16.md §6.1](docs/parser-reliability-audit-2026-05-16.md)).
- **`pa_pattenr.txt`/`tracer.py`/`sports_data.db` уже удалены** (этот коммит). Раньше засоряли корень.

### 5.3 Дубликаты и затычки в архитектуре

#### Удалено в этом коммите
- 21 `load_*.py` shim в корне — все были тонкими обёртками над `python -m schema_inspector.cli`. `load_event_detail.py` был **сломан** (указывал на несуществующий `event_detail_cli.py`).
- 9 Windows-only dev утилит: `test_run.py`, `analyze_entities.py`, `check.py`, `check_issues.py`, `inspect_api.py`, `build_local_swagger.py`, `serve_local_api.py`, `setup_postgres.py`, `tracer.py`.
- Локальные артефакты: `sports_data.db` (21MB SQLite — проект на Postgres!), `pa_pattenr.txt` (224KB output от tracer), `unique_api_map.txt` (20KB generated).
- Handoff документы: `CHAT_HANDOFF.md`, `NEXT_CHAT_CONTEXT.md` (содержали Windows-пути `D:\sofascore`, `C:\Users\bobur`).
- 4 завершённых phase-плана из `docs/superpowers/plans/` (2026-04-16/17), их design spec.
- Дубликат `PROJECT_OVERVIEW.md` в корне (есть в `docs/`).

#### Осталось как технический долг

**God-objects (6 файлов = 18 731 LOC, ~36% кодовой базы):**

| Файл | LOC | Что не так |
|---|---:|---|
| [local_api_server.py](schema_inspector/local_api_server.py) | **6123** | FastAPI app + 107 routes + ops + cache + dual-loop daemon thread + 5 inline synth. Рос на 700 LOC за неделю. |
| [cli.py](schema_inspector/cli.py) | **3669** | 46 commands argparse + dispatcher + 851-строчный `HybridApp` + 224-строчный `_MemoryRedisBackend` (dev stub в проде CLI). |
| [local_swagger_builder.py](schema_inspector/local_swagger_builder.py) | 3279 | Inline schema decls для каждого route. |
| [normalize_repository.py](schema_inspector/storage/normalize_repository.py) | **2565** | Все upserts в одном файле. |
| [pilot_orchestrator.py](schema_inspector/pipeline/pilot_orchestrator.py) | **2397** | `run_event()` = 15 stage-блоков ≈ 720 строк. Единственная точка hydration для 17 workers. ImportError → весь fleet падает. |
| [event_detail_parser.py](schema_inspector/event_detail_parser.py) | 2337 | Каждый sub-endpoint inline. |
| [service_app.py](schema_inspector/services/service_app.py) | 1977 | 55 методов, 23 attributes. Construction site для всех daemon'ов/workers. ServiceApp.`__post_init__` (58 строк) инициализирует 15 Redis-coupled stores **unconditionally** — один отсутствующий метод на fake redis = весь fleet не стартует. |

**Boilerplate дубль ~1640 LOC:**
- `_progress` + `_configure_logging` идентичны в 9 CLI = 90 LOC.
- Runtime/DB argparse args (`--proxy`, `--user-agent`, `--database-url`, etc.) дублируются в 22 CLI = ~1540 LOC.
- 5 команд `worker-live-{hot,tier-1,tier-2,tier-3,warm}` = одна с `--lane`, размазана на 5.
- Pipeline CLI (`bootstrap`, `current_year`, `targeted`, `slice`) имеют ~500 LOC near-clone.
- 4 пары `*_cli.py` ↔ `*_backfill_cli.py` — argparse 100% повторяется (~280 LOC).
- 4 entity root passthrough handlers (`_fetch_team/player/manager/unique_tournament_root_payload`) — 95% identical, 144 LOC.

**Плохие имена:**
- [workers/live_worker.py](schema_inspector/workers/live_worker.py) (200 LOC, sync state machine helper) vs [workers/live_worker_service.py](schema_inspector/workers/live_worker_service.py) (662 LOC, Redis stream consumer). Confusing. Helper надо переименовать в `live_state_machine.py`.
- [workers/normalize_worker.py](schema_inspector/workers/normalize_worker.py) — 7-line shim `from ..normalizers.worker import NormalizeWorker`. 33 импорта по проекту используют именно эту обёртку. Под удаление после grep-replace.

**Затычки (фазовые феймворки в коде):**
- `SOFASCORE_LEAGUE_CAPABILITIES_ENABLED` — Phase 4.7 feature flag, должен переключаться на legacy `match_center_policy` при отключении. Сейчас одна из 4 env-driven exit branches в `pilot_orchestrator.run_event`.
- `LIVE_TIER_1_ROOT_ONLY` — P0(b) fast-path, неизвестно включён ли на проде (ARCHITECTURE_AUDIT §verified).
- `LIVE_TIER_1_QUARANTINE_ENABLED` — quarantine для chronic-timeout events, неизвестно включён ли.
- `LIVE_SPLIT_DETAILS_FANOUT` — P0(a) split-details, выключен на проде (UNKNOWN reason).
- `SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED=0` — firebreak (2026-05-12), inline rollup отключен из hot path. Альтернатива — `rebuild-capability-rollup` CLI subcommand (вызывается по cron). Но `sofascore-rebuild-capability-rollup.timer` **не создан** на проде ⇒ rollup state стареет.

**435 ENV переменных без централизованной схемы.** Дефолты разбросаны по коду. Каталог: [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md), но автоматической валидации нет.

Полный список god-objects + boilerplate + plan refactor: [docs/SIMPLIFICATION_AUDIT.md](docs/SIMPLIFICATION_AUDIT.md).

---

## 6. Roadmap до production-уровня

Источник: [docs/PROJECT_VISION.md §6](docs/PROJECT_VISION.md), [docs/ARCHITECTURE_AUDIT.md часть 3](docs/ARCHITECTURE_AUDIT.md), [docs/24x7-exit-criteria.md](docs/24x7-exit-criteria.md).

### Эпоха 1: Stabilize (закрыта 2026-05-14)
- ✅ N1 monitoring daemon + Telegram alerts (`sofascore-monitoring.service`)
- ✅ Sweeper regression fix (commit `724d77f`)
- ✅ ARCHITECTURE_AUDIT v3
- ✅ /ops/live-freshness standalone SLO endpoint
- ✅ Structured retry log
- ⏳ N3 Sentinel probe для schema drift (pending)

### Эпоха 2: Productionize (1–2 месяца) — **сейчас тут**

Цели:
1. **Live freshness < 60s sustained 7 дней.** Сейчас `oldest_hot_score_age` ~97s.
2. ~~**HydrateWorker in-flight lock**~~ ✅ **СДЕЛАНО** (`d7ab82a`, 2026-05-20) — см. §5.1 #2.
3. **`/ops/snapshots/summary?detail=true` gate** (Finding #5). ~10 LOC.
4. **Critical индексы**: `idx_event_custom_id`, `idx_event_live_window`, `idx_etl_job_run_*` (Finding #1, #3).
5. **HTTPS + auth + rate-limit** для публичного API. Сейчас на `127.0.0.1:8000` за SSH-туннелем.
6. **Historical backfill program** — bulk-ingestion план.
7. **Read cache (Redis)** для повторных API запросов.
8. **Separate asyncpg pool для historical + live-first governor**. Сейчас общий pool.
9. **Adaptive polling** для chronic-timeout events.
10. ~~**Capability-aware gating** перед fetch~~ **✅ В ОСНОВНОМ СДЕЛАНО (проверено 2026-05-29).** Root-payload флаги (`has_event_player_heat_map`, `has_xg`, `has_event_player_statistics`, `has_global_highlights`) извлекаются (`parsers/entities.py:170-186`), персистятся в `event`-таблицу и **уже гейтят** heatmap/shotmap/highlights/player-stats через `is True` в `match_center_policy.football_detail_endpoint_allowed:357-360` / `football_highlights_allowed:390` / `football_special_allowed`. Премиз §6.2 («флаги не подключены») устарел. Высоко-404 ручки `/managers` (70%) / `/official-tweets` (90%) / `/player-of-the-season` (89%) **root-флага НЕ имеют** → закрыты negative-cache + static-denylist, не capability-gating. **НЕ делать** Sportradar-matrix вариант (отвергнут `7c4ef67`, 69% false-pos). Остаток (низкий приоритет, нужна confusion-matrix `flag=false→404`): полярность `is True`→`is False` для покрытия событий с `flag=None`-но-данные-есть.

### Эпоха 3: Scale (3–6 месяцев)
- Read replicas Postgres.
- CDN, multi-region (если потребуется).
- Realtime push (полная замена polling на WS-driven re-fetch) ← **цель пользователя по WS**.
- Per-sport matchcenter parity (сейчас football-only fixed для live_delta).
- Refactor god-objects (см. [SIMPLIFICATION_AUDIT Phase 2-4](docs/SIMPLIFICATION_AUDIT.md)).
- curl_cffi upgrade до 0.7+ для chrome131+.
- Mobile app в сторе, web frontend live.

### Acceptance criteria для 24/7 ([docs/24x7-exit-criteria.md](docs/24x7-exit-criteria.md))

| Gate | Threshold | Status |
|---|---|---|
| No manual `tmux` babysitting | systemd cutover done | ✅ |
| One-command readiness check | `scripts/prod_readiness_check.py` | ✅ |
| `refresh_live_event` retry rate (15min) | < 10% | ⚠️ partial |
| Retry rate на tier_1 | < 5% | ⚠️ |
| `chronic_timeout_events` (5+ TimeoutError подряд) | ≤ 5% active | ⚠️ |
| `endpoint_capability_rollup` deadlock/hour | ≤ 2 | ✅ verified |
| `live-discovery` retries/hour | ≤ 5 | ⚠️ (low load) |
| Live-hot oldest stale score age | < 15 min | ⚠️ ~ 40 min trend |
| Sustained 24h без manual intervention | required | ❌ not yet |

---

## 7. Запуск локально

### Prerequisites
- Python 3.11
- PostgreSQL 16 (с расширениями pg_trgm и подобными — см. [postgres_schema.sql](postgres_schema.sql))
- Redis 7

### Setup

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Postgres bootstrap
psql -U postgres -c "CREATE DATABASE sofascore_schema_inspector;"
psql -U postgres -d sofascore_schema_inspector -f postgres_schema.sql

# Apply migrations (последовательно по дате имени файла)
for m in migrations/*.sql; do
  psql -U postgres -d sofascore_schema_inspector -f "$m"
done

# Или через CLI
python -m schema_inspector.cli db-setup
```

### Запуск local API
```bash
python -m schema_inspector.local_api_server --host 127.0.0.1 --port 8000
# → http://127.0.0.1:8000/  (Swagger)
# → http://127.0.0.1:8000/api/v1/sport/football/events/live
# → http://127.0.0.1:8000/ops/health
```

### Запуск одного event (без Redis/планировщика)
```bash
python -m schema_inspector.cli event \
    --sport-slug football --event-id 14083191 --audit-db
```

### Запуск continuous runtime (с Redis)
```bash
# Терминал 1: planner
python -m schema_inspector.cli planner-daemon

# Терминал 2: discovery worker
python -m schema_inspector.cli worker-discovery --consumer-name discovery-1

# Терминал 3: hydrate worker
python -m schema_inspector.cli worker-hydrate --consumer-name hydrate-1
```

### Полный production runtime
Использовать systemd unit'ы из `ops/systemd/`. Deploy: push в `main` → pull на сервере → `systemctl restart`.

### Health checks
```bash
python -m schema_inspector.cli health
python scripts/prod_readiness_check.py --base-url http://127.0.0.1:8000
curl http://127.0.0.1:8000/ops/health
curl http://127.0.0.1:8000/ops/live-freshness
curl http://127.0.0.1:8000/ops/queues/summary
```

### Тесты
```bash
pytest -q                          # все 304 теста
pytest tests/test_local_api_server.py tests/test_planner_daemon.py -q
```

---

## 8. Принципы работы (rules of engagement)

Эти правила не меняются между сессиями. Берутся из [docs/PROJECT_VISION.md §7](docs/PROJECT_VISION.md).

1. **Live-first beats historical completeness.** Никогда не включать `historical-tournament` / `historical-enrichment` lanes без явного ask, если live SLO нарушен.
2. **Backpressure is intentional, not a bug.** `skip` / `defer` / `retry_scheduled` — нормальные сигналы. `planner pauses` под нагрузкой — by design.
3. **Raw snapshot is canonical для 1:1 shape.** Не убирать `api_payload_snapshot` из waterfall в `local_api_server.py`. Synthesizers — fallback.
4. **No image/media file storage.** URL строятся фронтом из `https://img.sofascore.com/api/v1/<entity>/<id>/image|flag`.
5. **Migrations append-only.** Новый файл `YYYY-MM-DD_description.sql`, никогда не редактировать существующие.
6. **Deploy = push to `main` → pull on server.** Никаких feature-branches, никакого PR-flow.
7. **Защитные изменения (firebreak / waterfall / sweeper) → сразу замерять** через `/ops/live-freshness`, `/ops/queues/summary`. Не верить "выглядит ок".
8. **Никаких новых top-level скриптов в корне репо** для one-off задач. Всё через `python -m schema_inspector.cli <subcommand>`. Дев-утилиты — в `scripts/dev-utils/` или `tools/`.
9. **`event_terminal_state.zombie_stale` ≠ конец матча.** Read-слой не должен на него полагаться.
10. **isEditor HARD BAN в 3 политиках** ([live_dispatch_policy.py], [detail_resource_policy.py], [match_center_policy.py]). Editor-events никогда не fetched как live.

---

## 9. Карта документации (что где смотреть)

CLAUDE.md — единственный источник правды по проекту в целом. Для деталей:

| Тема | Файл | Что внутри |
|---|---|---|
| Архитектура (live audit, риски) | [docs/ARCHITECTURE_AUDIT.md](docs/ARCHITECTURE_AUDIT.md) | Risk register с Evidence column, P0–P3 roadmap |
| Видение проекта (бизнес + SLO) | [docs/PROJECT_VISION.md](docs/PROJECT_VISION.md) | Living doc, hypothesis updates, roadmap по эпохам |
| Список god-objects + refactor план | [docs/SIMPLIFICATION_AUDIT.md](docs/SIMPLIFICATION_AUDIT.md) | Phase 0–5 рефакторинг |
| Performance аудит | [docs/PERFORMANCE_AUDIT_2026-05-20.md](docs/PERFORMANCE_AUDIT_2026-05-20.md) | Top-10 проблем БД + indexes |
| Parser reliability | [docs/parser-reliability-audit-2026-05-16.md](docs/parser-reliability-audit-2026-05-16.md) | Silent drops, 404 rates, missing parsers |
| ENV переменные | [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md) | Каталог 435 vars + defaults |
| CLI commands | [docs/CLI_AND_SCRIPTS.md](docs/CLI_AND_SCRIPTS.md) | Все subcommands с side effects |
| Services + workers | [docs/SERVICES_AND_WORKERS.md](docs/SERVICES_AND_WORKERS.md) | Каждый systemd unit |
| Redis + streams | [docs/REDIS_AND_QUEUES.md](docs/REDIS_AND_QUEUES.md) | Stream registry, consumer groups |
| Database + storage | [docs/DATABASE_AND_STORAGE.md](docs/DATABASE_AND_STORAGE.md) | Таблицы по группам, repositories |
| Postgres схема | [docs/POSTGRES_SCHEMA_OVERVIEW.md](docs/POSTGRES_SCHEMA_OVERVIEW.md), [docs/DB_TREE_OVERVIEW.md](docs/DB_TREE_OVERVIEW.md) | Структура БД, иерархия данных |
| API routes (внутренний) | [docs/API_ROUTES.md](docs/API_ROUTES.md) | Waterfall в `local_api_server` |
| Parsing & policies | [docs/PARSING_AND_POLICIES.md](docs/PARSING_AND_POLICIES.md) | match_center, detail_resource, live_dispatch |
| WebSocket для фронта | [docs/FRONTEND_WS_GUIDE.md](docs/FRONTEND_WS_GUIDE.md) | NATS subset, subjects, payload |
| Operations runbook | [docs/OPERATIONS_RUNBOOK.md](docs/OPERATIONS_RUNBOOK.md) | Health checks, restart, rollback |
| Production deploy | [docs/2026-04-17-production-deployment-guide.md](docs/2026-04-17-production-deployment-guide.md) | Step-by-step deploy |
| 24/7 exit criteria | [docs/24x7-exit-criteria.md](docs/24x7-exit-criteria.md) | Gates для prod sign-off |
| Function index | [docs/FUNCTION_INDEX.md](docs/FUNCTION_INDEX.md) | Карта функций по файлам |
| Active plans | [docs/superpowers/plans/](docs/superpowers/plans/) | live-24x7, multisource-graph, housekeeping, historical-bootstrap-split |
| Sofascore API reference | [docs/sofascore_api.md](docs/sofascore_api.md) | Иерархия sport→category→UT→season→event→player |

---

## 10. Что обязательно нужно знать новой сессии

Если читаешь это в первый раз:

1. **Точка входа всегда** `python -m schema_inspector.cli <subcommand>` или systemd unit. **Не запускай** load_*.py shim — их больше нет.
2. **Тесты должны проходить.** 304 файла, 0 skip/xfail. Перед commit: `pytest -q`.
3. **god-objects будут расти**, если не следить. Каждый PR — спросить "не надо ли это вынести?". Текущие топ-3: `local_api_server.py` (6123), `cli.py` (3669), `normalize_repository.py` (2565).
4. **Live first.** Если нагрузка на hydrate/historical_hydrate — historical lanes выключаются первыми.
5. **Pool size = 3/10** per worker (Phase 4.7.6). НЕ 20/50 как было до 2026-05-22.
6. **Redis-only hot path** (Phase 4.7.6). Workers не дёргают Postgres в hot path. `cli league-capability prime-redis` нужен раз в час по cron, иначе capabilities stale.
7. **WS-mirror реально работает** (NATS frames Sofascore → нам → фронт). Но **не используется для trigger-а refetch ручек** — это следующий шаг архитектуры.
8. **БД 148GB**: `api_payload_snapshot` — самая большая. Retention есть, но cold storage для >1 года ещё не сделан.
9. **35+ ENV переменных** активны на проде, дефолты в коде расходятся с docs/ENVIRONMENT.md. Перед изменением — verify через grep.
10. **Принципы работы из §8** — не отступать.

При любом сомнении: проверяй `git log -p <file>` и читай fresh код, а не .md документ. Документация может быть стара. CLAUDE.md обновляется руками после крупных изменений.

---

## 11. Правила для AI-агентов

**Все sub-агенты, которых вызывает Claude Code в этом репо, должны запускаться на модели `opus` (Opus 4.7 / max).**

В коде проекта слишком много неочевидных политик (3-layer isEditor ban, live tier dispatch, hot-row contention, fan-out splitting, capability gating, pool sizing) и god-objects (6 файлов на 18K LOC). Ошибка от sonnet/haiku в этой среде стоит часы дебага в проде.

**Конкретные требования:**
- Любой `Agent` (Explore, general-purpose, Plan, code-review, claude-code-guide и пр.) вызывается с явным параметром `model: "opus"`.
- Это применяется и когда parent-сессия идёт на sonnet — sub-агент всё равно opus.
- Исключение: тривиальный точечный поиск (`grep "X" в одном файле`) — sonnet допустим. По умолчанию — всегда opus.

Это правило также записано в [README.md](README.md#правила-для-ai-агентов).

