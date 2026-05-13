# Redis и очереди

Redis выступает координатором для всего runtime: streams = очереди задач, hashes/zsets = live-state, strings = leases/dedupe/freshness, metrics. Все Redis ops через `redis-py` или `fakeredis` (для тестов).

Главный модуль: `D:\sofascore\schema_inspector\queue\` — все примитивы.

---

## Streams + Consumer Groups

`D:\sofascore\schema_inspector\queue\streams.py` — реестр всех stream constants и consumer groups.

### Operational lane (live-first)

| Stream constant | Stream name | Consumer group | Producer | Consumer (worker) | Назначение |
|---|---|---|---|---|---|
| `STREAM_DISCOVERY` | `stream:etl:discovery` | `cg:discovery` | `PlannerDaemon` | `DiscoveryWorker` | Discover sport-surface events (scheduled) |
| `STREAM_LIVE_DISCOVERY` | `stream:etl:live_discovery` | `cg:live_discovery` | `LiveDiscoveryPlannerDaemon` | `DiscoveryWorker` (live scope) | Live events discovery per sport on cadence |
| `STREAM_HYDRATE` | `stream:etl:hydrate` | `cg:hydrate` | DiscoveryWorker, PlannerDaemon | `HydrateWorker` | Core event detail hydration |
| `STREAM_LIVE_TIER_1` | `stream:etl:live_tier_1` | `cg:live_tier_1` | DiscoveryWorker, PlannerDaemon | `LiveWorkerService` | Tier 1 live refresh (poll=5s) |
| `STREAM_LIVE_TIER_2` | `stream:etl:live_tier_2` | `cg:live_tier_2` | DiscoveryWorker, PlannerDaemon | `LiveWorkerService` | Tier 2 (poll=15-30s) |
| `STREAM_LIVE_TIER_3` | `stream:etl:live_tier_3` | `cg:live_tier_3` | DiscoveryWorker, PlannerDaemon | `LiveWorkerService` | Tier 3 (poll=30-90s) |
| `STREAM_LIVE_HOT` | `stream:etl:live_hot` | `cg:live_hot` | LiveWorkerService | `LiveWorkerService` | Hot lane (топ-приоритет) |
| `STREAM_LIVE_WARM` | `stream:etl:live_warm` | `cg:live_warm` | PlannerDaemon | `LiveWorkerService` | Warm lane (medium priority) |
| `STREAM_LIVE_DETAILS` | `stream:etl:live_details` | `cg:live_details` | LiveWorkerService (если `LIVE_SPLIT_DETAILS_FANOUT=1`) | `LiveDetailsWorkerService` | P0(a) split-details fanout (per-player) |
| `STREAM_MAINTENANCE` | `stream:etl:maintenance` | `cg:maintenance` | (internal pending entries) | `MaintenanceWorker` | Reclaim stale entries via XAUTOCLAIM |
| `STREAM_NORMALIZE` | `stream:etl:normalize` | `cg:normalize` | ResourceRefreshWorker (опционально) | `NormalizeWorker` | Re-parse raw snapshots durable sink |

### Historical lane (archival)

| Stream | Group | Producer | Consumer | Назначение |
|---|---|---|---|---|
| `stream:etl:historical_discovery` | `cg:historical_discovery` | `HistoricalPlannerDaemon` | DiscoveryWorker (historical scope) | Rolling history per date |
| `stream:etl:historical_tournament` | `cg:historical_tournament` | `HistoricalTournamentPlannerDaemon` | HistoricalTournamentWorker | Tournament/season archival |
| `stream:etl:historical_enrichment` | `cg:historical_enrichment` | HistoricalTournamentWorker | HistoricalEnrichmentWorker | Post-tournament enrichment (stats, standings) |
| `stream:etl:historical_hydrate` | `cg:historical_hydrate` | DiscoveryWorker (historical fanout) | HydrateWorker (historical) | Historical event detail hydration |
| `stream:etl:historical_maintenance` | `cg:historical_maintenance` | (internal) | MaintenanceWorker (historical) | Reclaim |

### Structure + Resource

| Stream | Group | Producer | Consumer | Назначение |
|---|---|---|---|---|
| `stream:etl:structure_sync` | `cg:structure_sync` | `StructurePlannerDaemon` | StructureSyncWorker | Tournament/season skeleton (rounds/calendar mode) |
| `stream:etl:resource_refresh` | `cg:resource_refresh` | `ResourcePlannerDaemon` | ResourceRefreshWorker | Generic endpoint refresh (freshness-driven) |

### DLQ

| Stream | Group | Producer | Consumer | Назначение |
|---|---|---|---|---|
| `stream:etl:dlq` | — (no group) | MaintenanceWorker (на max_delivery exceeded или non-retryable) | — (no auto-consumer) | Dead-letter sink |

**Aggregates** в `streams.py`:
- `OPERATIONAL_CONSUMER_GROUPS` (9)
- `HISTORICAL_CONSUMER_GROUPS` (5)
- `STRUCTURE_CONSUMER_GROUPS` (1)
- `RESOURCE_REFRESH_CONSUMER_GROUPS` (1)
- `NORMALIZE_CONSUMER_GROUPS` (1)
- `ALL_CONSUMER_GROUPS` = объединение (17 total)

---

## Live state keys (zsets / hashes / claims)

`D:\sofascore\schema_inspector\queue\live_state.py` → класс `LiveEventStateStore`.

| Key pattern | Тип | Назначение | TTL | Operations |
|---|---|---|---|---|
| `zset:live:hot` | ZSET | Events в hot lane. Members=event_id, score=next_poll_at (epoch ms) | — | ZADD/ZRANGEBYSCORE/ZREM |
| `zset:live:warm` | ZSET | Warm lane | — | то же |
| `zset:live:cold` | ZSET | Cold lane (notstarted future events) | — | то же |
| `live:event:{event_id}` | HASH | Event metadata: sport_slug, status_type, poll_profile, last_seen_at, last_ingested_at, last_changed_at, next_poll_at, hot_until, home_score, away_score, version_hint, is_finalized, dispatch_tier | — | HSET/HGETALL/HMGET |
| `live:dispatch_claim:{event_id}` | STRING | Mutex для publish dispatch | `lease_ms` (per tier) | SET NX PX |
| `live:dispatch_metrics` | HASH | Cumulative dispatch metrics | — | HINCRBY (см. ниже) |

`due_events(lane, now_ms)` = `ZRANGEBYSCORE(zset:live:<lane>, -inf, now_ms)` — выбирает events ready для polling.

---

## Live dispatch metrics

`live:dispatch_metrics` HASH fields (через HINCRBY):

```
started_at_ms              (HSETNX once)
claim_attempts:total
claim_attempts:tier_1
claim_attempts:tier_2
claim_attempts:tier_3
claim_succeeded:total
claim_succeeded:tier_1
claim_succeeded:tier_2
claim_succeeded:tier_3
claim_failed_blocked:total
claim_failed_blocked:tier_1
claim_failed_blocked:tier_2
claim_failed_blocked:tier_3
published:total
published:tier_1/2/3
clear_called:total
clear_called:tier_1/2/3
```

**Cumulative** с момента запуска планировщика. **Не сбрасываются** при рестарте процесса (живут в Redis).

Доступно через `/ops/queues/summary` JSON: ключ `live_dispatch_metrics`.

---

## Delayed / Leases / Dedupe / Freshness

### Delayed
**File**: `D:\sofascore\schema_inspector\queue\delayed.py`
**Key**: `zset:etl:delayed` (ZSET)
**Member**: serialized job (или job_id)
**Score**: `run_at_epoch_ms`
**Operations**: `schedule(job, delay_ms)` → ZADD; `pop_due(now_ms)` → ZRANGEBYSCORE + ZREM.
**Использование**: worker_runtime → при retry_scheduled публикует через `delayed_queue.schedule(...)`. Planner periodically запрашивает due jobs и публикует в исходный stream.

### Leases
**File**: `queue/leases.py`
**Pattern**: custom (`lease:<resource>` или подобное от caller).
**Type**: STRING, `SET NX PX <ttl_ms>`.
**Operations**: `acquire(key, owner, ttl_ms)`, `renew(...)`, `release(...)`.
**Использование**: дистрибутивная координация (например `live:dispatch_claim:*` сам через эту инфру).

### Dedupe
**File**: `queue/dedupe.py`
**Pattern**: custom (`dedupe:<scope>:<hash>`).
**Type**: STRING, `SET NX EX <ttl_seconds>`.
**Operations**: `claim_job(key, ttl_ms)` → True если еще не было; `is_fresh(key)`.
**Использование**: window-based deduplication. Например `dedupe:hydrate:<event_id>` чтобы не делать повторный hydrate в течение N секунд.

### Freshness
**File**: `queue/freshness.py`
**Pattern**: `fresh:<endpoint>:<scope>` или подобное.
**Type**: STRING marker.
**TTL**: `ttl_seconds` (per endpoint).
**Operations**: `is_fresh(key)` → True/False; `mark_fetched(key, ttl_seconds)`.
**Использование**: ResourceRefreshWorker — skip fetch если recent uplink был.

---

## Proxy / Inflight / Throttle / Quarantine

### Proxy state
**File**: `queue/proxy_state.py`

| Key | Type | Назначение |
|---|---|---|
| `proxy:{proxy_id}` | HASH | status, cooldown_until, recent_failures, recent_successes, last_status_code, last_challenge_reason, last_used_at, avg_latency_ms |
| `zset:proxy:cooldown` | ZSET | members=proxy_id, score=cooldown_until — sorted для quick "release from cooldown" |

### Inflight locks
**File**: `queue/live_inflight.py`

| Key | Type | Назначение | TTL |
|---|---|---|---|
| `live:refresh_inflight:{event_id}` | STRING | Root+edges critical path lock | `SOFASCORE_LIVE_EVENT_INFLIGHT_TTL_MS` (default 600000ms = 10min) |
| `live:details_inflight:{event_id}` | STRING | Details fanout lock (P0a split) | `SOFASCORE_LIVE_DETAILS_INFLIGHT_TTL_MS` (default 300000ms) |
| `live:root_inflight:{event_id}` | STRING | Root-only fast path lock (P0b tier_1) | `SOFASCORE_LIVE_ROOT_INFLIGHT_TTL_MS` (default 60000ms) |

`claim(event_id, owner)` → `SET NX PX <ttl>` → True если acquired.
`release(event_id, owner)` → DEL (owner verification через Lua script).

### Throttle (rate limiting)
**File**: `queue/live_edges_throttle.py`, `queue/live_details_throttle.py`

| Key | Type | TTL |
|---|---|---|
| `live:edges_last_enqueued:{event_id}` | STRING | `LIVE_EDGES_MIN_INTERVAL_SECONDS` (default 60) |
| `live:details_last_enqueued:{event_id}` | STRING | `LIVE_DETAILS_MIN_INTERVAL_SECONDS` (default 30) |

`should_enqueue(event_id)` → True если ключ не существует → SET с TTL.

### Tier 1 quarantine
**File**: `queue/live_tier_1_quarantine.py`

| Key | Type | Назначение |
|---|---|---|
| `live:tier1_retry_failed:{event_id}` | STRING | INCR counter consecutive failures |
| `live:tier1_quarantine:{event_id}` | STRING | JSON `{"until_ms": ..., "cycles": N}` — active quarantine |

Логика: N consecutive failures → enter quarantine с exponential cooldown (base 60s, max 600s). Worker'ы skip event пока quarantined.

**Env**: `LIVE_TIER_1_QUARANTINE_ENABLED`, `LIVE_TIER_1_QUARANTINE_WINDOW_SECONDS` (default 600).

---

## Job envelope

`D:\sofascore\schema_inspector\jobs\envelope.py` → `JobEnvelope` dataclass.

```python
JobEnvelope:
    job_id: str               # UUID
    job_type: str             # e.g. "hydrate_event_root"
    sport_slug: str | None
    entity_type: str | None   # "event", "tournament", "team", ...
    entity_id: int | None
    scope: str | None         # "scheduled", "live", "historical", lane name
    params: dict[str, Any]    # arbitrary job-specific
    priority: int
    scheduled_at: str         # ISO 8601 UTC
    attempt: int = 1
    parent_job_id: str | None
    trace_id: str | None
    capability_hint: str | None
    idempotency_key: str      # SHA-256 of identity tuple
```

Сериализация: JSON (`json.dumps`, `ensure_ascii=True`, sorted keys), fields stored как Redis Stream XADD field/value pairs.

### Job types

`D:\sofascore\schema_inspector\jobs\types.py`:

**Historical**:
- `JOB_DISCOVER_SPORT_SURFACE`
- `JOB_SYNC_TOURNAMENT_ARCHIVE`
- `JOB_ENRICH_TOURNAMENT_ARCHIVE`
- `JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH`
- `JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH`
- `JOB_SYNC_TOURNAMENT_STRUCTURE`
- `JOB_EXPAND_CATEGORY`
- `JOB_SYNC_UNIQUE_TOURNAMENT`
- `JOB_SYNC_SEASON_INDEX`
- `JOB_SYNC_SEASON_SURFACE`
- `JOB_SYNC_SEASON_WIDGET`

**Live**:
- `JOB_DISCOVER_TOURNAMENT_EVENTS`
- `JOB_DISCOVER_EVENT_SURFACE`
- `JOB_TRACK_LIVE_EVENT`
- `JOB_REFRESH_LIVE_EVENT`
- `JOB_REFRESH_LIVE_EVENT_DETAILS` (P0a split)

**Hydration**:
- `JOB_HYDRATE_EVENT_ROOT`
- `JOB_HYDRATE_EVENT_EDGE`
- `JOB_HYDRATE_ENTITY_PROFILE`
- `JOB_HYDRATE_ENTITY_SEASON`
- `JOB_HYDRATE_SPECIAL_ROUTE`

**Terminal / Refresh**:
- `JOB_FINALIZE_EVENT`
- `JOB_NORMALIZE_SNAPSHOT`
- `JOB_RECONCILE_CAPABILITY`
- `JOB_REPLAY_FAILED_JOB`
- `JOB_REFRESH_RESOURCE`

---

## Backpressure limits

`D:\sofascore\schema_inspector\services\backpressure_config.py`:

| Constant | Env var | Default | Stream |
|---|---|---:|---|
| `SCHEDULED_DISCOVERY_MAX_LAG` | `SCHEDULED_DISCOVERY_MAX_LAG` | `100` | cg:discovery |
| `HYDRATE_MAX_LAG` | `HYDRATE_MAX_LAG` | `800` | cg:hydrate |
| `LIVE_DISCOVERY_MAX_LAG` | `LIVE_DISCOVERY_MAX_LAG` | `50` | cg:live_discovery |
| `LIVE_DISCOVERY_HYDRATE_MAX_LAG` | `LIVE_DISCOVERY_HYDRATE_MAX_LAG` | `1000` | cg:hydrate (discovery-fanout subset) |
| `LIVE_HOT_MAX_LAG` | `LIVE_HOT_MAX_LAG` | `200` | cg:live_hot |
| `LIVE_TIER_1_MAX_LAG` | `LIVE_TIER_1_MAX_LAG` | `200` | cg:live_tier_1 |
| `LIVE_TIER_2_MAX_LAG` | `LIVE_TIER_2_MAX_LAG` | `200` | cg:live_tier_2 |
| `LIVE_TIER_3_MAX_LAG` | `LIVE_TIER_3_MAX_LAG` | `200` | cg:live_tier_3 |
| `LIVE_WARM_MAX_LAG` | `LIVE_WARM_MAX_LAG` | `800` | cg:live_warm |
| `HISTORICAL_DISCOVERY_MAX_LAG` | `HISTORICAL_DISCOVERY_MAX_LAG` | `100` | cg:historical_discovery |
| `HISTORICAL_HYDRATE_MAX_LAG` | `HISTORICAL_HYDRATE_MAX_LAG` | `500` | cg:historical_hydrate |
| `HISTORICAL_TOURNAMENT_MAX_LAG` | `HISTORICAL_TOURNAMENT_MAX_LAG` | `2500` | cg:historical_tournament |
| `HISTORICAL_ENRICHMENT_MAX_LAG` | `HISTORICAL_ENRICHMENT_MAX_LAG` | `15000` | cg:historical_enrichment |
| `STRUCTURE_SYNC_MAX_LAG` | `STRUCTURE_SYNC_MAX_LAG` | `1500` | cg:structure_sync |

`BackpressureLimit` (defined в `services/backpressure.py`) проверяет `group_info(stream, group).lag` → если > max_lag, returns blocking reason. Planner skip-ает tick. Discovery worker отказывается publish-ить.

---

## Прочие Redis keys

| Key | Type | Назначение |
|---|---|---|
| `etl:job_runs:processed:{job_run_id}` | STRING | Completion marker (TTL `SOFASCORE_WORKER_COMPLETION_TTL_MS` = 1h default) |
| `planner_cursor:<scope>` | HASH | Persistent cursor для historical planners |
| `tournament_registry` | SET | Список managed tournaments |
| `resource_cursor:<scope>` | HASH | Cursor для resource planner |
| `resource_negative_cache:<scope>` | SET | TTL'd negative cache для resource refresh |

---

## Inspect runtime через CLI / redis-cli

Прод-сценарии (read-only):

```bash
# ZCARD lanes
redis-cli ZCARD zset:live:hot
redis-cli ZCARD zset:live:warm

# oldest event in hot lane
redis-cli ZRANGE zset:live:hot 0 0 WITHSCORES

# dispatch metrics
redis-cli HGETALL live:dispatch_metrics

# count claim keys
redis-cli --scan --pattern "live:dispatch_claim:*" | wc -l

# inspect конкретный event
redis-cli HGETALL "live:event:14083629"
redis-cli ZSCORE zset:live:hot 14083629
```

Или через FastAPI `/ops/queues/summary` JSON.

---

## Связано

- Системные сервисы потребляющие streams — [SERVICES_AND_WORKERS.md](SERVICES_AND_WORKERS.md)
- Env переменные TTL / поведения — [ENVIRONMENT.md](ENVIRONMENT.md)
- Database tables, parallel pgsql side — [DATABASE_AND_STORAGE.md](DATABASE_AND_STORAGE.md)
- Operations playbook (cleanup, inspect, recover) — [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md)
