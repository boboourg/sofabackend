# N4: API Performance Plan

**План: довести все ~171 frontend API endpoints до TTFB <50ms.**

| Поле | Значение |
| --- | --- |
| Версия | v1 (2026-05-14) |
| Статус | Implementation в процессе (ACK получен на A + B + cache warming) |
| Источник | Phase 0 audit (`scripts/audit_api_latency.py`) показал 31.6% endpoints под 50ms; цель 100% |
| Зависимости | N1 monitoring (для verification), `endpoint_registry` table, asyncpg pool, Redis backend |

---

## TL;DR

3 layer-а, deployed incrementally:

| Layer | Что | Покрытие | Effort |
| --- | --- | --- | --- |
| **A** | Один composite snapshot index (`endpoint_pattern, context_entity_type, context_entity_id, id DESC`) | 70-80% endpoints (entity-scoped lookups) → 5-50ms | 0.5 дня |
| **B** | Redis-backed response cache (replace in-process `dict`) | 95-100% endpoints на cache hit → 3-10ms | 2 дня |
| **C** | Cache-warmer daemon (CLI + systemd) для sport-level aggregations | First user hit без cold-cache penalty | 1 день |

**Total:** 3.5-4 дня. Deploy incremental — каждый layer ships независимо, можно rollback.

---

## Baseline (после Phase 0 audit)

```
Distribution of 171 endpoints by TTFB:
FAST <50ms:           54 (31.6%)  ← target line
OK 50-200ms:          37 (21.6%)
SLOW 200-1000ms:      32 (18.7%)
VERY_SLOW 1-5s:       23 (13.5%)
CATASTROPHIC >5s:     18 (10.5%)
TIMEOUT (30s):         7  (4.1%)
```

**Top 3 проблемные паттерны:**

1. `sport/{slug}/scheduled-events/{date}` × 11 спортов — 7 timeout + 4 over 4s
2. `sport/{slug}/events/live` × 11 спортов — все 1.6s–23s
3. `event/{id}/<sub>` (lineups, statistics, incidents, graph) — 4–10s

Pattern (1) и (2) — **sport-level aggregations**, не имеют `context_entity_id` scope, не выигрывают от entity index. Layer B + C для них.

Pattern (3) — **entity-scoped sub-endpoints**, не используют partial index (т.к. он только для `event/{id}` root). Layer A для них.

---

## Layer A — Composite snapshot index

### Что

Новая миграция:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_payload_snapshot_lookup_v2
    ON api_payload_snapshot (endpoint_pattern, context_entity_type, context_entity_id, id DESC)
    WHERE context_entity_id IS NOT NULL;
```

### Покрытие

Любой query вида:
```sql
SELECT ... FROM api_payload_snapshot
WHERE endpoint_pattern = '/api/v1/event/{event_id}/lineups'
  AND context_entity_type = 'event'
  AND context_entity_id = 16182210
ORDER BY id DESC LIMIT 1
```

(Это форма всех `local_api_server.py:_fetch_snapshot_payload` queries.)

### Trade-offs

| Pro | Con |
| --- | --- |
| One index covers 170+ endpoints | Disk cost ~1-2 GB (4M rows × 4 columns) |
| Same latency profile as `event/{id}` root (4ms) | Slight INSERT slowdown (~5-10%) — acceptable |
| CONCURRENTLY = no downtime | Existing `idx_api_payload_snapshot_endpoint_pattern` дублирует частично, можно потом удалить |

### Files

- `migrations/2026-05-14_api_payload_snapshot_lookup_v2.sql` (new)
- `tests/test_api_payload_snapshot_lookup_v2_migration.py` (new)

### Definition of Done

- [ ] Миграция применена на проде через `psql -f`
- [ ] `\d api_payload_snapshot` показывает новый индекс
- [ ] Re-run `audit_api_latency.py` показывает event sub-endpoints < 50ms
- [ ] `pytest tests/test_api_payload_snapshot_lookup_v2_migration.py` зелёный

### Rollback

```sql
DROP INDEX CONCURRENTLY IF EXISTS idx_api_payload_snapshot_lookup_v2;
```

Risk: low. CONCURRENTLY DROP не блокирует таблицу.

---

## Layer B — Redis response cache

### Что

Заменить `_response_cache: dict` (in-process, per-worker) в `local_api_server.py` на shared Redis backend.

### Текущая реализация (что меняем)

```python
# local_api_server.py:213-214
self._response_cache: dict[...] = {}
self._response_cache_lock: threading.Lock | None = threading.Lock()

# _cache_get / _cache_put — thread-safe dict access
```

### Новая реализация

```python
# schema_inspector/api_cache.py (NEW)
class RedisResponseCache:
    def __init__(self, redis_backend, key_prefix="api:resp:"): ...
    def get(self, key) -> SerializedApiResponse | None: ...
    def put(self, key, response, ttl_seconds) -> None: ...
    def invalidate(self, key) -> None: ...
    # Fail-open: Redis errors → return None (cache miss), don't crash
```

### Cache key

Существующий `_response_cache_key`:
```
(raw_path, sorted_query_params_tuple)
```

Redis key encoding:
```
api:resp:<sha256(raw_path + sorted_query_params_json)>
```

### TTL policy (увеличены vs current)

| Endpoint type | Current TTL | New TTL | Reason |
| --- | --- | --- | --- |
| `sport/{slug}/events/live` | 2s | **5s** | в пределах SLO ≤5s |
| Live event detail (inprogress) | 2s | **5s** | то же |
| Finalized event detail | 30s | **3600s** (1h) | данные не меняются |
| Scheduled events | 30s | **60s** | medium freshness |
| Static (sport, category, tournament info) | 30s | **86400s** (1d) | редко меняются |

**Backwards compat:** TTLs env-overridable через `SOFASCORE_API_CACHE_TTL_<TYPE>_SECONDS`. Default — старые значения; новые — через `.env` override.

### Env toggle

```
SOFASCORE_API_RESPONSE_CACHE_BACKEND=redis  # or "memory" (default for backward compat)
SOFASCORE_API_CACHE_KEY_PREFIX=api:resp:
SOFASCORE_API_CACHE_TTL_LIVE_SECONDS=5
SOFASCORE_API_CACHE_TTL_FINALIZED_SECONDS=3600
SOFASCORE_API_CACHE_TTL_SCHEDULED_SECONDS=60
SOFASCORE_API_CACHE_TTL_STATIC_SECONDS=86400
```

Default `memory` означает **deploy безопасен** — поведение не меняется без явного флага.

### Cache stampede prevention

Когда TTL expires, до 4 workers могут одновременно hit БД для same key. Mitigation:
- Existing `threading.Lock` per process сохранён для **first-hit coalescing внутри worker**
- Cross-worker coalescing через Redis SETNX lock (5s TTL) — optional, можно добавить если будет нужно

### Files

- `schema_inspector/api_cache.py` (NEW)
- `schema_inspector/local_api_server.py` — заменить `_cache_get` / `_cache_put`
- `tests/test_api_cache.py` (NEW)

### Definition of Done

- [ ] `RedisResponseCache` class + 10+ unit tests (get/put/TTL/Redis failure fallback)
- [ ] Integration: `local_api_server.py` switches backend by env flag
- [ ] Deploy с `SOFASCORE_API_RESPONSE_CACHE_BACKEND=redis` на проде
- [ ] Re-run audit: 80%+ endpoints под 50ms on warm cache
- [ ] N1 monitoring: добавить cache hit/miss signal (optional)

### Rollback

```bash
# In /opt/sofascore/.env:
SOFASCORE_API_RESPONSE_CACHE_BACKEND=memory  # was redis

sudo systemctl restart sofascore-api
```

Instant rollback through env. Зеро code redeploy.

---

## Layer C — Cache warmer daemon

### Что

Standalone daemon который периодически fetch-ит hot URLs через HTTP к local API. Это **populates cache** before first user request.

### Hot URLs (≈30 URLs)

| URL pattern | Cadence | TTL match |
| --- | --- | --- |
| `/api/v1/sport/{slug}/events/live` × 12 | 5s | 5s TTL |
| `/api/v1/sport/{slug}/scheduled-events/{today}` × 12 | 60s | 60s TTL |
| `/api/v1/sport/{slug}/scheduled-events/{tomorrow}` × 12 | 300s | 60s TTL (refresh ahead) |
| `/api/v1/sport/{slug}/scheduled-tournaments/{today}/page/0` × 12 | 60s | 60s TTL |

### CLI

```bash
python -m schema_inspector.cli api-cache-warmer --consumer-name prod-warmer-1
```

### Env config

```
SOFASCORE_CACHE_WARMER_ENABLED=1
SOFASCORE_CACHE_WARMER_BASE_URL=http://127.0.0.1:8000
SOFASCORE_CACHE_WARMER_LIVE_INTERVAL_SECONDS=5
SOFASCORE_CACHE_WARMER_SCHEDULED_INTERVAL_SECONDS=60
SOFASCORE_CACHE_WARMER_TIMEOUT_SECONDS=30
SOFASCORE_CACHE_WARMER_SPORTS=football,basketball,tennis,...
```

### Files

- `schema_inspector/cache_warmer/` (NEW module, similar to `monitoring/`)
  - `daemon.py` — `CacheWarmerDaemon` + tick loop
  - `config.py` — env-backed config
  - `url_targets.py` — hot URL list
- `schema_inspector/cli.py` — add `api-cache-warmer` subcommand
- `tests/test_cache_warmer.py` (NEW)
- `ops/systemd/sofascore-cache-warmer.service` (NEW)

### Definition of Done

- [ ] `CacheWarmerDaemon` class + 10+ tests
- [ ] CLI subcommand работает локально
- [ ] systemd unit deployed на проде
- [ ] После 1 минуты warmer running, `audit_api_latency.py` показывает all sport-level endpoints < 50ms

### Rollback

```bash
sudo systemctl stop sofascore-cache-warmer
sudo systemctl disable sofascore-cache-warmer
```

### Cost considerations

12 sports × 5s interval = 12 requests/5s = ~2.4 req/sec just on live. Plus scheduled = ~3 req/sec total. **Negligible load** for API (already serving real users much higher).

---

## Phase 4 — Verify + monitoring

### Re-run audit

```bash
ssh sofascore-prod 'cd /opt/sofascore && .venv/bin/python scripts/audit_api_latency.py --base-url http://127.0.0.1:8000 --output /tmp/api_audit_v3.csv --summary /tmp/api_audit_v3.md'
```

Compare к v2 baseline. Target: 95-100% endpoints под 50ms.

### Add N1 monitoring signal: API p95 latency

В `schema_inspector/monitoring/`:
- New signal `api_p95_latency_ms`
- Source: lightweight in-memory ring buffer в `local_api_server.py` (recent N requests, p95)
- Surfaced через `/ops/api-latency` endpoint (NEW)
- Telegram alert if p95 > 100ms over 5 minute window

Это **observability** для последующих регрессий. Если мы вдруг ломаем кэш — мы это узнаем за минуты, не за неделю.

---

## Risks

| Risk | Probability | Impact | Mitigation |
| --- | --- | --- | --- |
| Composite index disk space | Low | Low | 1-2GB acceptable; monitor disk |
| Cache stampede на TTL expire | Medium | Medium | Per-worker lock + optional Redis SETNX |
| Stale data в cache window | Low (within SLO) | Low | TTL 5s for live = within ≤5s SLO |
| Redis memory growth | Medium | Medium | Cache keys with TTL auto-expire; monitor |
| Warmer DDoS если timeout не работает | Low | Medium | 30s timeout per request, sequential cadence |
| In-process cache fallback не работает | Low | High | Default backend = memory, no behavior change без env |

---

## Implementation order

1. **Day 1:** Layer A migration (this commit). Verify event sub-endpoints fast.
2. **Day 2:** Layer B `api_cache.py` module + tests + env toggle (default off).
3. **Day 3:** Layer B deploy с env flag flip; verify on prod.
4. **Day 4:** Layer C cache-warmer module + tests + systemd.
5. **Day 5:** Phase 4 — final re-audit + monitoring signal.

Каждый layer **separately committable + deployable + revertable**. Если Layer A не работает как ожидалось — fix или revert до начала Layer B.

---

## Связанные документы

- `docs/PROJECT_VISION.md` §6 Эпоха 2 M3 ("Read cache (Redis)") — этот план это и есть M3
- `scripts/audit_api_latency.py` — Phase 0 audit tool
- `docs/N1_MONITORING_PLAN.md` — где интегрируем API p95 signal
- `docs/ARCHITECTURE_AUDIT.md` D.3 — `etl_job_run.started_at` BRIN был похожим паттерном (один index, big impact)
