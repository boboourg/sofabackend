# Sofascore ETL — Operations Handover

Документ для оператора, который принимает систему. Здесь описано **что есть**, **как оно работает**, **что трогать в типовых ситуациях** и **где искать дальше**.

> SSH: `ssh sofascore-prod` (alias). Repo: `/opt/sofascore`. Source-of-truth: `git@github.com:boboourg/sofabackend.git` branch `main`. Local dev: `D:\sofascore`.

---

## 1. Архитектура одним взглядом

```
                            wss://ws.sofascore.com:9222 (NATS)
                                       │ push deltas
                                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                  Sofascore Source (upstream)                      │
│   HTTP polling (через smartproxy)  +  WebSocket push              │
└────────────┬─────────────────────────────────────┬───────────────┘
             │                                     │
             ▼                                     ▼
   ┌─────────────────┐                  ┌──────────────────────┐
   │   PLANNERS      │                  │   WS CONSUMER        │
   │   (publish jobs)│                  │   (push → DB)        │
   └─────────┬───────┘                  └──────────┬───────────┘
             │                                     │
             ▼                                     │
   ┌─────────────────┐                             │
   │ Redis Streams   │                             │
   │ (queue layer)   │                             │
   └─────────┬───────┘                             │
             │                                     │
             ▼                                     ▼
   ┌─────────────────┐                  ┌──────────────────────┐
   │   WORKERS       │                  │  Normalize / Cache   │
   │  (consume jobs) │ ────────────────▶│  invalidation         │
   │  HTTP fetch     │                  │                       │
   └─────────┬───────┘                  └──────────┬───────────┘
             │                                     │
             ▼                                     ▼
   ┌──────────────────────────────────────────────────────────┐
   │       PostgreSQL: api_payload_snapshot + normalized      │
   │       Redis: live_state, freshness, event_payload_cache  │
   └─────────────────────────┬────────────────────────────────┘
                             │
                             ▼
            ┌─────────────────────────────────────┐
            │  FastAPI READ API (/api/v1/*)        │
            │  WebSocket MIRROR (/ws/v1)           │
            │  OPS endpoints (/ops/*)              │
            └────────────────┬────────────────────┘
                             │
                             ▼
            ┌─────────────────────────────────────┐
            │  Nginx (api.var11.com 443 + 80)     │
            └────────────────┬────────────────────┘
                             │
                             ▼
            ┌─────────────────────────────────────┐
            │  Mobile clients (React Native)       │
            └─────────────────────────────────────┘
```

**Принцип**: live-first (live UI должен быть свежим), historical backfill — в фоне, paused при live pressure. WS consumer обновляет `event/event_score/event_status/event_time` в real-time. REST API читает с overlay для гарантии 1:1 sync.

---

## 2. Все systemd сервисы

`sudo systemctl list-units 'sofascore-*'` покажет все. Они логически разбиваются на группы:

### 2.1 API surface (всегда запущено)

| Unit | Что делает |
|---|---|
| `sofascore-api.service` | FastAPI на `127.0.0.1:8000`, обслуживает `/api/v1/*`, `/ops/*`. Nginx → этот сервис |
| `sofascore-ws-consumer.service` | Подключается к wss://ws.sofascore.com, парсит NATS deltas, пишет в normalized + Redis pubsub fanout |
| `sofascore-ws-server.service` | Mirror WS для mobile (wss://api.var11.com/ws/v1), слушает Redis pubsub, broadcast'ит клиентам |

### 2.2 Live ETL pipeline (high priority, всегда крутится)

| Unit | Что делает |
|---|---|
| `sofascore-planner.service` | Operational planner — publishes scheduled hydrate jobs |
| `sofascore-live-discovery-planner.service` | Discovers live events каждые ~5 сек |
| `sofascore-discovery@1.service` | Discovery worker (single instance — обычно достаточно) |
| `sofascore-live-discovery@1.service` | Live-specific discovery |
| `sofascore-hydrate@1..9.service` | 9 operational hydrate workers — тянут `/event/{id}` + sub-resources |
| `sofascore-live-tier-1@1..5.service` | 5 voracious live workers (most active matches) |
| `sofascore-live-tier-2@1..4.service` | 4 medium-tier live |
| `sofascore-live-tier-3@1..9.service` | 9 low-tier live (long-tail матчи) |
| `sofascore-live-hot@1.service` | Hot path live |
| `sofascore-live-warm@1..3.service` | Warm path live |
| `sofascore-live-rescue.service` | Periodic force-hydrate для stuck live events (см. env) |

### 2.3 Historical backfill (low priority, paused при live pressure)

| Unit | Что делает |
|---|---|
| `sofascore-historical-planner.service` | Planner для individual events |
| `sofascore-historical-tournament-planner.service` | Planner для UT+season pairs (с **strict cat.priority barrier** см. D.1) |
| `sofascore-historical-discovery@1..3.service` | 3 historical discovery |
| `sofascore-historical-tournament@1..4.service` | 4 tournament-archive workers |
| `sofascore-historical-hydrate@1..30.service` | **30** historical hydrate workers (расширено для burst, default было 20) |
| `sofascore-historical-enrichment@1.service` | 1 enrichment worker (team/player profiles) |
| `sofascore-historical-maintenance@1.service` | Retry / cleanup |
| `sofascore-structure-sync.service` | 1 single worker — `/unique-tournament/{id}/seasons` etc. |
| `sofascore-structure-planner.service` | Publishes structure-sync jobs |

### 2.4 Прочие

| Unit | Что делает |
|---|---|
| `sofascore-normalize@1.service` | Парсит api_payload_snapshot → normalized tables |
| `sofascore-resource-refresh@1..10.service` | 10 refresh workers (generic resource refresh loop) |
| `sofascore-resource-planner.service` | Planner для resource refresh |
| `sofascore-maintenance@1.service` | Operational maintenance |
| `sofascore-cache-warmer.service` | Pre-warms Redis cache для hot endpoints |
| `sofascore-proxy-health.service` | Tracks proxy latency/errors, marks unhealthy |
| `sofascore-tournament-registry-refresh.service` | Refreshes tournament_registry table |
| `sofascore-monitoring.service` | SLO monitoring, alerting |

### 2.5 Контроль

```bash
# Статус всего
sudo systemctl list-units 'sofascore-*.service' --all --no-pager

# Restart одного
sudo systemctl restart sofascore-api.service

# Перезагрузить config (только для unit'ов с reload support)
sudo systemctl kill -s HUP sofascore-historical-tournament-planner.service

# Логи (live)
sudo journalctl -u sofascore-api -f

# Логи последние 100 строк
sudo journalctl -u sofascore-ws-consumer -n 100 --no-pager
```

---

## 3. Конфигурация

### 3.1 Главный env-файл: `/opt/sofascore/.env`

Ключевые переменные:

```bash
# Database
SOFASCORE_DATABASE_URL=postgresql://sofascore_user:<password>@localhost:5432/sofascore_schema_inspector

# Redis
REDIS_URL=redis://:<password>@127.0.0.1:6379/0

# Live rescue (force-hydrate stuck matches)
SOFASCORE_LIVE_RESCUE_ENABLED=true
SOFASCORE_LIVE_RESCUE_INTERVAL_S=60
SOFASCORE_LIVE_RESCUE_STALE_MINUTES=5
SOFASCORE_LIVE_RESCUE_COOLDOWN_SECONDS=300
SOFASCORE_LIVE_RESCUE_MAX_PER_TICK=50
SOFASCORE_LIVE_RESCUE_SPORT_SLUGS=football,tennis,basketball,...

# Historical concurrency
SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY=16   # max parallel HTTP per worker process

# Backfill governor (когда paused live SLO breach)
# Defaults: oldest_hot_age_max=1800 sec, warn=900, tier_1_quarantined_max=10
# Сейчас агрессивные значения для burst:
SOFASCORE_BACKFILL_OLDEST_HOT_AGE_MAX=7200
SOFASCORE_BACKFILL_OLDEST_HOT_AGE_WARN=5400
SOFASCORE_BACKFILL_TIER_1_QUARANTINED_MAX=50

# Strict category barrier (новое — D.1)
SOFASCORE_BACKFILL_STRICT_CATEGORY_BARRIER=true   # default true

# Stream backpressure caps (raised for burst)
HISTORICAL_TOURNAMENT_MAX_LAG=200000   # raised from default 2500
```

После изменения `.env` — restart соответствующий unit (env загружается при старте).

### 3.2 Backfill priorities YAML: `/opt/sofascore/config/backfill_priorities.yaml`

Operator-tunable per-sport / per-UT веса для historical tournament planner.

Текущий (burst mode, football-only):

```yaml
sport_weights:
  football: 50
ut_boost:
  - { ut_id: 1, multiplier: 20.0 }   # EURO
  - { ut_id: 16, multiplier: 20.0 }  # FIFA World Cup
  - { ut_id: 13, multiplier: 10.0 }  # WC Qual. CAF
  - { ut_id: 17, multiplier: 5.0 }   # Premier League
  - { ut_id: 8, multiplier: 3.0 }    # LaLiga
sport_concurrency_caps:
  football: 200
```

Baseline (всем спортам по немного):

```yaml
# Скопировать default:
sudo cp /opt/sofascore/config/backfill_priorities.example.yaml \
        /opt/sofascore/config/backfill_priorities.yaml

# Применить без рестарта:
sudo systemctl kill -s HUP sofascore-historical-tournament-planner

# Проверить применённый конфиг:
curl -s http://127.0.0.1:8000/ops/backfill-priorities | jq
```

Подробно: [docs/BACKFILL_PRIORITIES.md](BACKFILL_PRIORITIES.md).

### 3.3 Frontend WebSocket: `/opt/sofascore/docs/FRONTEND_WS_GUIDE.md`

Полный гайд для фронтенд-разработчика — protocol, subjects, поля дельт, React Native + TanStack Query пример, troubleshooting.

---

## 4. Operational endpoints (`/ops/*`)

Все на `http://127.0.0.1:8000` (внутренний адрес).

| Endpoint | Что показывает |
|---|---|
| `GET /ops/health` | Общая freshness + coverage_summary + drift_summary + queue summary |
| `GET /ops/queues/summary` | Per-stream длина, lag, consumers, live tier counts |
| `GET /ops/backfill-progress?sport=football&limit=15` | Per-UT прогресс: events/finished/pct |
| `GET /ops/backfill-cursor?sport=football&limit=50` | Список pending UTs, next_season_id, last_advance_at |
| `GET /ops/backfill-priorities` | Текущий YAML config (с computed share_pct) |
| `GET /ws/health` | WS server status (connections, subscriptions, redis ping) |
| `GET /ops/snapshots/summary` | Snapshot table stats |
| `GET /ops/jobs/runs` | Recent etl_job_run entries |

Использовать через jq:

```bash
ssh sofascore-prod 'curl -s http://127.0.0.1:8000/ops/queues/summary | jq ".streams[] | select(.lag > 1000)"'
```

---

## 5. CLI команды (`python -m schema_inspector.cli ...`)

Полный список через `python -m schema_inspector.cli --help`. Главные:

```bash
# Один из основных entry-points — используется в systemd units и для one-shot операций
./deploy/run_service.sh <subcommand>

# Health-проба
./deploy/run_service.sh health

# Discovery одного дня
./deploy/run_service.sh scheduled --sport-slug football --date 2026-05-20

# Discovery live для спорта
./deploy/run_service.sh live --sport-slug football

# Hydrate конкретного event_id
./deploy/run_service.sh event 15171570

# Backfill priorities
./deploy/run_service.sh backfill-priorities show
./deploy/run_service.sh backfill-priorities reload     # SIGHUP
./deploy/run_service.sh backfill-priorities dry-run

# Backfill cursors
./deploy/run_service.sh backfill-cursor show
./deploy/run_service.sh backfill-cursor reseed-stuck --min-cat-priority 0

# DB audit
./deploy/run_service.sh audit-db 15171570 15171571

# Replay snapshot через durable sinks
./deploy/run_service.sh replay --event-id 15171570

# Recover live state (rebuild Redis indexes из PG)
./deploy/run_service.sh recover-live-state
```

---

## 6. Database schema (главные таблицы)

### 6.1 Event-side

| Таблица | PK | Что |
|---|---|---|
| `event` | id | Main event metadata + status_code, winner_code, updated_at |
| `event_score` | (event_id, side) | home/away scores: current, display, period1-4, normaltime, etc. |
| `event_status` | code | Lookup: status_code → (description, type). 100=Ended, 6=1st half, ... |
| `event_status_time` | event_id | prefix, timestamp, initial, max, extra |
| `event_time` | event_id | currentPeriodStartTimestamp, initial, max, extra, played, clock_running, ... |
| `event_round_info` | (event_id) | round_number, slug, name, cup_round_type |
| `event_change_item` | (event_id, ordinal) | Append-only audit log; ws_delta'ы тут оставляют trace |
| `event_incident` | (event_id, ordinal) | Минимальная структура incidents (id, type, minute, scores, text) |
| `event_lineup` | (event_id, side) | formation, colors, support_staff |
| `event_lineup_player` | (event_id, side, player_id) | players в lineup |
| `event_statistic` | (event_id, ...) | Per-period statistics |
| `event_var_in_progress` | event_id | VAR review state (home_team, away_team) |
| `event_market` | id | Odds markets (1X2, BTTS, etc.) |
| `event_market_choice` | source_id | Choices внутри market'а: name, fractional_value, initial_fractional_value |

### 6.2 Entity-side

| Таблица | PK | Что |
|---|---|---|
| `team` | id | Команды |
| `player` | id | Игроки |
| `manager` | id | Тренеры |
| `tournament` | id | Tournament (одна edition) |
| `unique_tournament` | id | Лига (стабильна across seasons), `tier`, `category_id`, `user_count` |
| `season` | id | year, name |
| `category` | id | Страна/регион, `priority` (10=major, 0=amateur), `country_alpha2` |
| `sport` | id | football, tennis, ... |

### 6.3 Pipeline state

| Таблица | PK | Что |
|---|---|---|
| `api_payload_snapshot` | id | Raw upstream payloads (jsonb), TOAST-compressed, ~113 GB |
| `api_snapshot_head` | scope_key | Pointer на latest snapshot per (endpoint_pattern, context) |
| `tournament_registry` | (source_slug, sport_slug, ut_id) | Backfill cursors + `priority_rank` |
| `backfill_cursor` (view) | — | Read-only view of `tournament_registry.next_season_backfill_id` |
| `etl_job_run` | id | Audit log всех job'ов (для дашбордов / debugging) |
| `coverage_ledger` | (source, sport, surface, scope_type, scope_id) | Per-scope coverage status |
| `event_payload_cache` | cache_key | Persistent payload cache (Variant B Stage 1) |

### 6.4 Quick queries

```sql
-- DB connection (на проде):
PGPASSWORD=$(grep -oP "postgresql://[^:]+:\K[^@]+" /opt/sofascore/.env) \
  psql -U sofascore_user -d sofascore_schema_inspector -h localhost -p 5432

-- Latest events
SELECT id, status_code, updated_at FROM event ORDER BY updated_at DESC LIMIT 10;

-- Backfill progress по football
SELECT * FROM v_backfill_progress
WHERE sport_slug = 'football' AND priority_rank <= 5
ORDER BY priority_rank, ut_name;

-- Live counts
SELECT status_code, count(*) FROM event
WHERE status_code IN (6,7,8,9,10,11,12,13,14,15,16,20,30,31)
GROUP BY status_code ORDER BY status_code;
```

---

## 7. Redis streams (queue layer)

### 7.1 Operational (high priority)

| Stream | Consumer Group | Что |
|---|---|---|
| `stream:etl:discovery` | `cg:discovery` | Discovery jobs (find events to hydrate) |
| `stream:etl:live_discovery` | `cg:live_discovery` | Live-specific discovery |
| `stream:etl:hydrate` | `cg:hydrate` | Operational hydrate (`/event/{id}` + sub-resources) |
| `stream:etl:live_hot` | `cg:live_hot` | Hot live (most-active matches) |
| `stream:etl:live_tier_1` | `cg:live_tier_1` | Live tier 1 |
| `stream:etl:live_tier_2` | `cg:live_tier_2` | Live tier 2 |
| `stream:etl:live_tier_3` | `cg:live_tier_3` | Live tier 3 (long tail) |
| `stream:etl:live_warm` | `cg:live_warm` | Warm live |
| `stream:etl:live_details` | `cg:live_details` | Split-details fanout |
| `stream:etl:normalize` | `cg:normalize` | Parse api_payload_snapshot → normalized |
| `stream:etl:maintenance` | `cg:maintenance` | Cleanup, retry |
| `stream:etl:resource_refresh` | `cg:resource_refresh` | Generic resource refresh |

### 7.2 Historical (low priority)

| Stream | Consumer Group | Что |
|---|---|---|
| `stream:etl:historical_discovery` | `cg:historical_discovery` | Historical event discovery |
| `stream:etl:historical_tournament` | `cg:historical_tournament` | Per-(UT, season) jobs |
| `stream:etl:historical_enrichment` | `cg:historical_enrichment` | Team/player enrichment |
| `stream:etl:historical_hydrate` | `cg:historical_hydrate` | Historical event hydrate |
| `stream:etl:historical_maintenance` | `cg:historical_maintenance` | Cleanup |
| `stream:etl:structure_sync` | `cg:structure_sync` | `/unique-tournament/{id}/seasons` etc. |

### 7.3 WS fanout (новое)

| Channel (Redis Pub/Sub) | Что |
|---|---|
| `ws:fanout:sport:<slug>` | Все event-дельты для спорта |
| `ws:fanout:odds:<slug>:<market_id>` | Odds deltas |
| `ws:fanout:event:<event_id>` | Дельты конкретного матча |

### 7.4 Quick inspect

```bash
# REDIS_PASS из .env
PASS=$(grep -oP "REDIS_URL=redis://:\K[^@]+" /opt/sofascore/.env)

# Длина stream
redis-cli -a "$PASS" --no-auth-warning XLEN stream:etl:historical_tournament

# Pending в consumer group
redis-cli -a "$PASS" --no-auth-warning XPENDING stream:etl:hydrate cg:hydrate

# Последние 10 messages
redis-cli -a "$PASS" --no-auth-warning XREVRANGE stream:etl:historical_tournament + - COUNT 10

# Очистить stream (DANGER — теряем backlog)
redis-cli -a "$PASS" --no-auth-warning XTRIM stream:etl:historical_tournament MAXLEN 0
```

---

## 8. Главные кодовые модули (где искать что)

### 8.1 ETL pipeline

| Файл | Что |
|---|---|
| `schema_inspector/cli.py` | Unified CLI entry-point (все subcommands) |
| `schema_inspector/services/service_app.py` | ServiceApp — фабрика workers/planners |
| `schema_inspector/services/planner_daemon.py` | Operational planner |
| `schema_inspector/services/historical_planner.py` | Historical individual-event planner |
| `schema_inspector/services/historical_tournament_planner.py` | Per-UT historical planner (с strict barrier) |
| `schema_inspector/services/live_discovery_planner.py` | Live event discovery |
| `schema_inspector/services/historical_archive_service.py` | Tournament-archive runner (HTTP + parse + UPSERT) |
| `schema_inspector/workers/discovery_worker.py` | Discovery handler |
| `schema_inspector/workers/hydrate_worker.py` | Hydrate handler (operational + historical) |
| `schema_inspector/workers/historical_archive_worker.py` | Tournament + enrichment handlers |
| `schema_inspector/workers/live_worker_service.py` | Live tier workers |

### 8.2 Storage

| Файл | Что |
|---|---|
| `schema_inspector/transport.py` | HTTP transport через curl_cffi (с CA bundle via certifi) |
| `schema_inspector/event_list_repository.py` | UPSERT для scheduled/live events bundle |
| `schema_inspector/event_detail_repository.py` | UPSERT для individual event hydrate |
| `schema_inspector/storage/tournament_registry_repository.py` | Backfill cursors + seed/advance logic |
| `schema_inspector/storage/normalize_repository.py` | Snapshot → normalized tables |
| `schema_inspector/storage/raw_repository.py` | api_payload_snapshot UPSERT |
| `schema_inspector/db.py` | AsyncpgDatabase pool wrapper |

### 8.3 Parsers (sport- and surface-specific)

```
schema_inspector/parsers/
├── families/        # event_root, event_lineups, event_incidents, etc.
├── sports/          # football, basketball, tennis, etc. (sport-specific quirks)
├── special/         # baseball_pitches, tennis_point_by_point, shotmap, etc.
└── registry.py      # Dispatch: endpoint_pattern → parser class
```

### 8.4 Read API + WS

| Файл | Что |
|---|---|
| `schema_inspector/local_api_server.py` | FastAPI app — main read endpoint логика |
| `schema_inspector/scheduled_events_synthesizer.py` | Synthesizes events list from normalized tables (when snapshot missing) |
| `schema_inspector/event_payload_cache_repository.py` | Persistent cache (Variant B) — read/write/invalidate |
| `schema_inspector/api_cache.py` | Redis-backed response cache |

### 8.5 WebSocket (новое в этой сессии)

| Файл | Что |
|---|---|
| `schema_inspector/ws_nats_parser.py` | NATS frame parser (CLIENT side, для consumer) |
| `schema_inspector/ws_delta_normalizer.py` | Sofascore delta → normalized tables mapping |
| `schema_inspector/ws_delta_writer.py` | Apply normalized delta to DB + invalidate cache |
| `schema_inspector/ws_odds_writer.py` | Odds delta → event_market_choice UPSERT |
| `schema_inspector/ws_fanout_publisher.py` | Re-broadcast via Redis pubsub |
| `schema_inspector/ws_server_protocol.py` | Mirror WS server NATS subset + SubscriptionManager |
| `schema_inspector/services/ws_consumer_service.py` | Long-running consumer (Sofascore → DB + fanout) |
| `schema_inspector/services/ws_server_service.py` | FastAPI WebSocket server for mobile clients |

### 8.6 Backfill priorities (новое)

| Файл | Что |
|---|---|
| `schema_inspector/services/backfill_priority_config.py` | YAML parser + weighted_select |
| `config/backfill_priorities.example.yaml` | Default config template |
| `docs/BACKFILL_PRIORITIES.md` | Operator guide |

---

## 9. Frontend интеграция

### 9.1 REST API

Base URL: `https://api.var11.com/api/v1/`

100+ endpoints, повторяют Sofascore upstream. Полный список: `https://api.var11.com/` (Swagger UI).

### 9.2 WebSocket (новое)

Base URL: `wss://api.var11.com/ws/v1`  
Health: `https://api.var11.com/ws/health`

NATS-subset protocol (1:1 совместим с Sofascore upstream wss://ws.sofascore.com:9222).

**Полный гайд для фронтенд-разработчика**: [docs/FRONTEND_WS_GUIDE.md](FRONTEND_WS_GUIDE.md). Включает:
- TL;DR connect snippet
- Все frame форматы (CONNECT/SUB/UNSUB/PING/PONG/MSG/INFO/-ERR)
- Subjects table (sport.* / event.{id} / odds.*)
- Полный каталог delta fields (40 event + 7 odds)
- React Native + TanStack Query пример
- Reconnect strategy (fire-and-forget + REST refetch)

### 9.3 Nginx config (где смотреть/править)

Активный config:
- `/etc/nginx/fastpanel2-sites/var11_com_usr/api.var11.com.conf` — main server block
- `/etc/nginx/fastpanel2-sites/var11_com_usr/api.var11.com.includes` — общие proxy settings + **наши `/ws/v1` и `/ws/health` location'ы**

Snippet в репо: `ops/nginx/api.var11.com.snippets/ws-mirror.conf` (для следующих deploys).

```bash
sudo nginx -t              # test config
sudo systemctl reload nginx
```

---

## 10. Что делать в типовых ситуациях

### 10.1 Live UI отстаёт от реальности

Симптомы: счёт обновляется с задержкой 5+ сек, mobile показывает stale.

Проверки:
```bash
# Lag на live streams
curl -s http://127.0.0.1:8000/ops/queues/summary | jq '.streams[] | select(.stream | contains("live"))'

# Hydrate workers активны?
sudo systemctl list-units 'sofascore-hydrate@*' 'sofascore-live-tier-*' --state=active --no-pager

# WS consumer работает?
sudo systemctl is-active sofascore-ws-consumer

# Upstream latency
PGPASSWORD=... psql ... -c "SELECT date_trunc('minute', started_at) AS minute, COUNT(*) AS reqs, ROUND(AVG(latency_ms))::int AS ms FROM api_request_log WHERE started_at > now() - interval '5 minutes' GROUP BY 1 ORDER BY 1 DESC"
```

Решения:
- Если `live_tier_*` lag высокий → restart workers: `sudo systemctl restart 'sofascore-live-tier-*'`
- Если upstream latency >10 сек → проверить proxy: `sudo journalctl -u sofascore-proxy-health -n 50`
- Если WS consumer paused → `sudo systemctl restart sofascore-ws-consumer`

### 10.2 Backfill завис (cursors не двигаются)

```bash
# Видно advance?
curl -s "http://127.0.0.1:8000/ops/backfill-cursor?sport=football&limit=20" | jq '.uniques[] | {ut_name, next_season_year, backfill_last_advance_at}'

# Если backfill_last_advance_at = null для многих UTs:
# Re-seed stuck cursors:
./deploy/run_service.sh backfill-cursor reseed-stuck --min-cat-priority 0

# Проверить planner running:
sudo systemctl is-active sofascore-historical-tournament-planner
sudo journalctl -u sofascore-historical-tournament-planner -n 50 --no-pager | grep -i "backpressure\|paused"
```

Если planner paused backpressure'ом:
```bash
# Поднять stream lag threshold
echo "HISTORICAL_TOURNAMENT_MAX_LAG=200000" | sudo tee -a /opt/sofascore/.env
sudo systemctl restart sofascore-historical-tournament-planner
```

### 10.3 Mobile WS клиенты не подключаются

```bash
# WS server жив?
curl -s https://api.var11.com/ws/health | jq

# Nginx config OK?
sudo nginx -t

# WS consumer публикует в Redis?
PASS=$(grep -oP "REDIS_URL=redis://:\K[^@]+" /opt/sofascore/.env)
redis-cli -a "$PASS" --no-auth-warning PUBSUB CHANNELS 'ws:fanout:*'

# Тест локального WS endpoint:
ssh sofascore-prod 'cd /opt/sofascore && python -c "
import asyncio, websockets
async def t():
    async with websockets.connect(\"ws://127.0.0.1:8001/ws/v1\") as ws:
        print(await ws.recv())  # должна быть INFO frame
asyncio.run(t())
"'
```

### 10.4 HTTP 500 на REST API

```bash
# Логи
sudo journalctl -u sofascore-api -n 100 --no-pager | grep -i "error\|exception" | tail -20

# Pool exhaustion?
curl -s http://127.0.0.1:8000/ops/health | jq '.database_ok, .redis_ok'

# DB живая?
PGPASSWORD=... psql ... -c "SELECT 1"

# Restart API
sudo systemctl restart sofascore-api
```

### 10.5 Proxy issues (HTTP errors на upstream)

```bash
# Per-proxy latency + errors
PGPASSWORD=... psql ... -c "
  SELECT proxy_address, COUNT(*) AS reqs, ROUND(AVG(latency_ms))::int AS avg_ms,
         COUNT(*) FILTER (WHERE http_status IS NULL) AS transport_err,
         COUNT(*) FILTER (WHERE http_status = 429) AS http_429
  FROM api_request_log
  WHERE started_at > now() - interval '30 minutes'
  GROUP BY proxy_address ORDER BY avg_ms DESC
"

# Если certificate errors:
# Проверить cacert.pem reachable
ls -la $(./deploy/run_service.sh -c "python -c 'import certifi; print(certifi.where())'")

# Restart всех workers что используют transport
sudo systemctl restart 'sofascore-hydrate@*' 'sofascore-historical-hydrate@*'
```

### 10.6 Запустить burst backfill (как в этой сессии)

```bash
# 1. Aggressive priority config (football only)
sudo tee /opt/sofascore/config/backfill_priorities.yaml > /dev/null <<EOF
sport_weights:
  football: 50
ut_boost:
  - { ut_id: 1, multiplier: 20.0 }
  - { ut_id: 16, multiplier: 20.0 }
  - { ut_id: 17, multiplier: 5.0 }
sport_concurrency_caps:
  football: 200
EOF
sudo systemctl kill -s HUP sofascore-historical-tournament-planner

# 2. +10 hydrate workers
for i in $(seq 21 30); do
  sudo systemctl enable --now sofascore-historical-hydrate@$i.service
done

# 3. +2 tournament workers
sudo systemctl enable --now sofascore-historical-tournament@3.service
sudo systemctl enable --now sofascore-historical-tournament@4.service

# 4. Aggressive governor (live tolerance 2h)
cat >> /opt/sofascore/.env <<EOF
SOFASCORE_BACKFILL_OLDEST_HOT_AGE_MAX=7200
SOFASCORE_BACKFILL_OLDEST_HOT_AGE_WARN=5400
SOFASCORE_BACKFILL_TIER_1_QUARANTINED_MAX=50
HISTORICAL_TOURNAMENT_MAX_LAG=200000
EOF
sudo systemctl restart sofascore-historical-planner sofascore-historical-tournament-planner

# 5. 30-day forward scheduled sweep (creates hydrate jobs for upcoming matches)
cat > /tmp/sweep.sh <<'EOF'
#!/bin/bash
cd /opt/sofascore
for offset in $(seq -7 30); do
  D=$(date -d "+${offset} day" +%Y-%m-%d)
  ./deploy/run_service.sh scheduled --sport-slug football --date "$D" >> /tmp/sweep.log 2>&1 &
  while (( $(jobs -p | wc -l) >= 5 )); do wait -n; done
done
wait
EOF
chmod +x /tmp/sweep.sh
nohup /tmp/sweep.sh > /dev/null 2>&1 &

# 6. Re-seed stuck cursors
./deploy/run_service.sh backfill-cursor reseed-stuck --min-cat-priority 0
```

### 10.7 Rollback burst → live-first

```bash
# 1. Default priorities (uniform sports)
sudo cp /opt/sofascore/config/backfill_priorities.example.yaml \
        /opt/sofascore/config/backfill_priorities.yaml
sudo systemctl kill -s HUP sofascore-historical-tournament-planner

# 2. Remove burst env knobs
sudo sed -i '/SOFASCORE_BACKFILL_OLDEST_HOT_AGE/d' /opt/sofascore/.env
sudo sed -i '/SOFASCORE_BACKFILL_TIER_1_QUARANTINED_MAX/d' /opt/sofascore/.env
sudo sed -i '/HISTORICAL_TOURNAMENT_MAX_LAG/d' /opt/sofascore/.env
sudo systemctl restart sofascore-historical-planner sofascore-historical-tournament-planner

# 3. Down-scale workers
for i in $(seq 21 30); do
  sudo systemctl disable --now sofascore-historical-hydrate@$i.service
done
sudo systemctl disable --now sofascore-historical-tournament@{3,4}
```

### 10.8 Deploy (push на main + sync)

```bash
# Локально:
git push origin main

# На проде:
ssh sofascore-prod 'cd /opt/sofascore && git pull --ff-only origin main && sudo systemctl restart <affected service>'

# Если затронуты несколько units (e.g. transport.py):
ssh sofascore-prod 'sudo systemctl restart "sofascore-hydrate@*" "sofascore-historical-hydrate@*" sofascore-api sofascore-ws-consumer'
```

### 10.9 Запустить миграцию

```bash
# 1. Создать .sql в /opt/sofascore/migrations/2026-MM-DD_description.sql
# 2. Apply:
ssh sofascore-prod 'cd /opt/sofascore && PGPASSWORD=... psql ... -f migrations/2026-MM-DD_description.sql'

# Миграции in-repo append-only — не редактировать существующие.
```

---

## 11. Тесты

```bash
# Полный suite
D:/sofascore/.venv311/Scripts/python.exe -m pytest -q

# Одна группа
D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_ws_*.py -q

# С таймаутом
D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/ --timeout=30 -q

# Ignore 2 pre-existing failures:
D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/ \
  --ignore=tests/test_live_freshness_summary.py \
  --ignore=tests/test_monitoring_daemon.py -q
```

**~1950+ tests, 2 pre-existing failures unrelated**.

### 11.1 Главные test файлы

| Файл | Что покрывает |
|---|---|
| `tests/test_local_api_server.py` | 100+ tests для REST API endpoints |
| `tests/test_scheduled_events_synthesizer.py` | Event-list synthesizer + overlay |
| `tests/test_event_payload_cache_repository.py` | Persistent cache (Variant B) |
| `tests/test_ws_delta_normalizer.py` | WS delta parsing (27 cases) |
| `tests/test_ws_delta_writer.py` | WS DB writer (12) |
| `tests/test_ws_nats_parser.py` | NATS frame parser (18) |
| `tests/test_ws_server_protocol.py` | Mirror WS server protocol (15) |
| `tests/test_ws_server_health.py` | /ws/health endpoint (5) |
| `tests/test_ws_consumer_dispatch.py` | Consumer routing (5) |
| `tests/test_ws_fanout_publisher.py` | Redis fanout (8) |
| `tests/test_backfill_priority_config.py` | YAML parser + weighted select (13) |
| `tests/test_historical_tournament_planner_priorities.py` | Planner with config (4) |
| `tests/test_historical_tournament_planner_reload.py` | SIGHUP reload (3) |
| `tests/test_strict_category_barrier_selector.py` | Strict cat.priority barrier (7) |
| `tests/test_backfill_cursor_reseed.py` | Re-seed stuck cursors (8) |
| `tests/test_season_info_enricher.py` | /season/{s}/info start_ut/end_ut (5) |
| `tests/test_ops_backfill_priorities.py` | /ops/backfill-priorities endpoint (4) |
| `tests/test_transport_ca_bundle.py` | certifi verify path (3) |

---

## 12. Известные edge cases / TODO

### 12.1 stuck UTs без finished events в БД

После последнего re-seed остались **~240 UTs** где cursor никуда не указывает (в БД ноль finished events для них). Они были **temporarily disabled** через `UPDATE tournament_registry SET historical_enabled = FALSE`.

Чтобы поднять их обратно:
```sql
-- Re-enable + bootstrap structure
UPDATE tournament_registry SET historical_enabled = TRUE
WHERE unique_tournament_id IN (...);

-- Trigger structure_sync для каждого UT:
-- (CLI subcommand TODO: structure-sync-bootstrap)
```

В коде это **Phase E.2** — пока не реализовано (отдельный todo).

### 12.2 UT=16 (FIFA WC) 2022 season

Имеет в БД **1 event** (вместо 64). После `backfill-cursor reseed-stuck` advance не сработает на 64 events — Sofascore возвращает только этот 1 event через `/unique-tournament/16/season/41087/events/last/0`. Возможные причины:

- Sofascore deprecated данные (data drop)
- Pagination issue в их API
- Нужен другой endpoint (через team profile, например)

Воспроизвести вручную:
```bash
curl -s 'https://www.sofascore.com/api/v1/unique-tournament/16/season/41087/events/last/0' \
  | jq '.events | length'   # должно быть >1
```

### 12.3 Live UI tolerance после burst

В `.env` могут оставаться **aggressive** значения после burst:
```
SOFASCORE_BACKFILL_OLDEST_HOT_AGE_MAX=7200    # default 1800
HISTORICAL_TOURNAMENT_MAX_LAG=200000          # default 2500
```

Это означает live UI может отставать до 2 часов когда backfill активный. После завершения burst — rollback (см. 10.7).

### 12.4 Pre-existing test failures

```
tests/test_live_freshness_summary.py::test_summary_serialises_with_dataclasses_asdict — SLO count drift
tests/test_monitoring_daemon.py::test_warn_to_crit_escalation_fires_even_with_dedupe — escalation logic
```

Не блокирует deploy. Связаны с monitoring layer (alerting), не с ETL core.

### 12.5 Disabled UTs могут получить data через WS

WS consumer обновляет `event` table по `event_id` даже если UT в disabled state. То есть **live матчи всё равно ингестируются** через WS push, даже когда historical backfill для их UT выключен. Это OK — это два independent path.

---

## 13. Контакты / Resources

| Ресурс | URL |
|---|---|
| Github | https://github.com/boboourg/sofabackend |
| Prod SSH | `ssh sofascore-prod` |
| Repo path | `/opt/sofascore` |
| Local dev | `D:\sofascore` |
| Branch | `main` (no PRs — direct push) |
| Mobile app guide | `docs/FRONTEND_WS_GUIDE.md` |
| Backfill priorities | `docs/BACKFILL_PRIORITIES.md` |
| Architecture overview | `docs/current-runtime-architecture.md` |
| Project overview | `PROJECT_OVERVIEW.md`, `NEXT_CHAT_CONTEXT.md` |

---

## 14. Quick reference (cheat sheet)

```bash
# Здоровье системы
curl -s http://127.0.0.1:8000/ops/health | jq '{database_ok, redis_ok, drift_summary, coverage_summary}'

# Очереди
curl -s http://127.0.0.1:8000/ops/queues/summary | jq '.streams[] | select(.lag > 100)'

# Backfill prio config
curl -s http://127.0.0.1:8000/ops/backfill-priorities | jq

# Backfill cursors
curl -s 'http://127.0.0.1:8000/ops/backfill-cursor?sport=football&limit=20' | jq

# WS health
curl -s https://api.var11.com/ws/health | jq

# Все sofascore services
sudo systemctl list-units 'sofascore-*.service' --no-pager

# Failed services
sudo systemctl --failed --no-pager | grep sofascore

# Top throughput last 5 min
PGPASSWORD=... psql ... -c "
  SELECT date_trunc('minute', started_at) AS m, COUNT(*) AS reqs, ROUND(AVG(latency_ms))::int AS ms
  FROM api_request_log WHERE started_at > now() - interval '5 minutes' GROUP BY 1 ORDER BY 1 DESC
"

# Tail все sofascore logs
sudo journalctl -u 'sofascore-*' -f
```

---

## Финальные слова

Система **в основе своей идемпотентна** — если что-то «сломалось», в 90% случаев restart соответствующего unit'а помогает. Streams накапливают backlog, но workers догонят. Cursors сохраняют state в Postgres — потеря Redis не критична (recovery через `recover-live-state`).

Главные правила:
1. **Never push --force-with-lease на main без явного ack пользователя**
2. **Никаких `git rebase -i` / `git add -i`** в systemd-driven flow
3. **Запускать тесты перед каждым deploy** — там 1950+ tests catch regressions
4. **Backfill governor** автоматически пауcит backfill при live SLO breach — не надо force'ить
5. **Read-only first** — если не уверен, начни с `/ops/*` curl'ом, потом действуй

Удачи! Все вопросы — в репо issues или к команде в чате.
