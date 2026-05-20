# Performance Audit — 2026-05-20

**Mode**: READ-ONLY. Ничего не изменено. Жду ACK перед каждым исправлением.
**Метод**: 3 параллельных Opus-агента (DB / ETL workers / API endpoints) + live `pg_stat_user_tables` на проде.
**Cross-references**: `docs/SIMPLIFICATION_AUDIT.md` (V2, 2026-05-20) для архитектурного контекста.

---

## TL;DR — Top-10 проблем (приоритизировано по impact)

| # | Severity | Где | Что | Fix |
|---|----------|-----|-----|-----|
| 1 | 🔴 CRITICAL | `event_market_choice` table | **28.7 миллиардов seq_tup_read** (prod stats) — нет нужного индекса | Add index on accessed column (см. ниже) |
| 2 | 🔴 CRITICAL | `workers/hydrate_worker.py:102-191` | **Нет in-flight lock** — два HydrateWorker могут параллельно обрабатывать один event_id | Redis lock по паттерну `LiveEventInFlightStore` |
| 3 | 🔴 CRITICAL | `event.custom_id` | **Нет индекса**, но колонка используется в `IN-list` в hot reconcile path | `CREATE INDEX idx_event_custom_id ON event(custom_id)` |
| 4 | 🔴 CRITICAL | `cli.py:411-473` (`App.run_event`) | Hydrate lock **leak на crash** (нет try/finally) → 60s блок live polling | try/finally + Lua release |
| 5 | 🔴 CRITICAL | `local_api_server.py:4106-4119` | `/ops/snapshots/summary?detail=true` — 5 COUNT(DISTINCT) на 148GB | Gate behind operator role |
| 6 | 🔴 CRITICAL | OFFSET pagination на `event` (148GB) | `_FETCH_QUERY_SEASON_LAST/NEXT/TEAM/PLAYER` — deep pages walk full filter set | Keyset pagination для page >= 5 |
| 7 | 🟡 WARNING | `etl_job_run` table | **3.2B seq_tup_read** (42.8% seq scan ratio) | Index on commonly-filtered columns |
| 8 | 🟡 WARNING | `live_bootstrap.py::release_hydrate_lock` | GET→CHECK→DELETE non-atomic | Lua script (clone from `live_inflight.py:18`) |
| 9 | 🟡 WARNING | `_FETCH_QUERY_LIVE` synth path | Нет partial index `(status_code, start_timestamp DESC) WHERE is_editor IS NOT TRUE` | Add partial index |
| 10 | 🟡 WARNING | `retention_repository.delete_legacy_snapshot_batch` | 60s+ runtime now killed by 30s statement_timeout | `SET LOCAL statement_timeout = '120s'` |

---

# Task 1: DB & ORM Performance

## A. Live prod stats — что реально болит (pg_stat_user_tables)

**Источник**: `SELECT FROM pg_stat_user_tables ORDER BY seq_tup_read DESC` на проде.

| Table | Size | seq_scan | seq_tup_read | idx_scan | seq% | n_live_tup |
|-------|------|---------:|-------------:|---------:|-----:|-----------:|
| **event_market_choice** | 136 MB | 23,272 | **28.7B** | 45,018 | 34.1% | 1,384 |
| event | 1314 MB | 5,456 | 8.3B | 9.1M | 0.1% | 4.5M |
| etl_job_run | 5774 MB | 415 | 3.2B | 554 | 42.8% | 226K |
| api_request_log | 5050 MB | 1,167 | 2.7B | 1.6M | 0.1% | 6.1M |
| event_terminal_state | 53 MB | 4,839 | 1.1B | 66M | 0.0% | 455 |
| event_score | 1162 MB | 34 | 34M | 6.7M | 0.0% | 8.5M |
| event_payload_cache | 80 kB | 48,285 | **3.3M** | 21,281 | **69.4%** | 82 |

### Finding 1.A — [CRITICAL] `event_market_choice` — 28.7B seq tup reads

**Прод-метрика**: 23,272 seq_scan × ~1.2M rows avg = **28.7B tuple reads**. Таблица 136 MB, **1384 rows** in n_live_tup. **34% scans bypass index entirely**.

**Гипотеза**: каждый seq scan читает ~1.2M tuples — но `n_live_tup = 1384`?! Это значит table имеет очень много **dead tuples** (vacuum behind, или high churn rate). При seq scan PostgreSQL читает всё включая dead tuples.

**Что делать**:
1. `VACUUM (ANALYZE, VERBOSE) event_market_choice` — узнать состояние
2. Поискать в коде места которые делают `SELECT FROM event_market_choice WHERE ...` без index
3. Если seq scan вынужден — `CLUSTER` table + force `autovacuum_naptime` low

**Migration (после диагностики)**:
```sql
-- Step 1 (immediately runnable): full vacuum
VACUUM FULL ANALYZE event_market_choice;
-- Step 2 (after grep finds the access pattern): add the right index
```

### Finding 1.B — [WARNING] `etl_job_run` 3.2B seq tup reads, 42.8% seq scan

**Прод**: 5774 MB table, 226K rows, **42.8% scans bypass index**.

**Likely cause**: queries by `job_id` UUID, `created_at` range, `worker_consumer`, status — без правильного composite index.

**Migration recommended**:
```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_job_run_created_status
    ON etl_job_run (created_at DESC, status)
    WHERE status IN ('failed', 'retry_scheduled');
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_job_run_job_id
    ON etl_job_run (job_id);
```

Need verify через `pg_stat_statements` какие именно queries hit table — после grep по коду.

### Finding 1.C — [WARNING] `event_payload_cache` 69.4% seq scan ratio

**Наш own cache table** имеет 69% seq scan ratio. Cache **сам** становится slow lookup path!

**Подозрение**: PK lookup делается **по композитному ключу** не всегда покрытому. Проверить:
- `event_payload_cache_repository.py:109` — что в `fetchrow` query

Migration (after verify):
```sql
-- Likely needed: PK already covers but PK may be wrong shape
-- Verify with: \d+ event_payload_cache
```

---

## B. Index gaps в hot snapshot lookup path

### Finding 2 — [CRITICAL] `event.custom_id` NO INDEX

**File:line**: `local_api_server.py:1170-1171`, `event_list_repository.py:249`

**Issue**: Hot reconcile path:
```sql
SELECT ... FROM event e WHERE e.id = ANY($1::bigint[])
                          OR (cardinality($2::text[]) > 0 AND e.custom_id = ANY($2::text[]))
```
**Нет индекса** на `event.custom_id` → seq scan по `event` (4.5M rows, 1.3GB) при каждом hit OR-branch.

**Impact**: каждый `/scheduled-events/{date}` + `/sport/{slug}/events/live` response при reconcile падает в seq scan если есть custom_id matches.

**Migration**:
```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_custom_id
    ON event (custom_id)
    WHERE custom_id IS NOT NULL;
```

### Finding 3 — [WARNING] Нет partial index для `_FETCH_QUERY_LIVE`

**File:line**: `scheduled_events_synthesizer.py:427-440`

**Issue**: Live-list query фильтрует по `e.start_timestamp >= now-12h + st.type IN (live_statuses) + NOT EXISTS event_terminal_state + e.is_editor IS NOT TRUE`. Существующие индексы (`idx_event_start_timestamp`, `idx_event_unique_tournament_id`, `idx_event_tournament_id`) — single-column. Нет partial для (status_code, start_timestamp DESC) WHERE is_editor.

**Migration**:
```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_live_window
    ON event (status_code, start_timestamp DESC, id DESC)
    WHERE is_editor IS NOT TRUE;
```

### Finding 4 — [INFO] Snapshot lookup path — already well-indexed

**Root snapshot lookup** (`_fetch_latest_entity_passthrough`, `_fetch_*_root_payload`, 5 entity types) — **уже покрыто** `idx_api_payload_snapshot_lookup_v2` (composite partial, 2026-05-14) + `idx_api_payload_snapshot_event_root_local_api` (event-only partial, 2026-04-27). Никаких изменений не требуется.

---

## C. OFFSET pagination — N²-стиль на 148GB

### Finding 5 — [CRITICAL] OFFSET на 4 pagination endpoints

**Files**:
- `scheduled_events_synthesizer.py:241-253` `_FETCH_QUERY_SEASON_LAST` (page_size=30, OFFSET $3 LIMIT $4)
- `scheduled_events_synthesizer.py:256-268` `_FETCH_QUERY_SEASON_NEXT`
- `scheduled_events_synthesizer.py:382-407` `_FETCH_QUERY_TEAM_LAST/NEXT`
- `scheduled_events_synthesizer.py:294-306` `_FETCH_QUERY_PLAYER_EVENTS_LAST` (joins `event_lineup_player` then OFFSET)

**Impact**: page 10 = OFFSET 300 (читает + discards 300 rows). page 100 = OFFSET 3000. Для UCL season с тысячами events каждая deep page = full filter scan.

**Player events worst** (page 20 для Messi → 600 rows materialized then discarded after 15 JOINs).

**Recommendation**: Keyset pagination для page >= 5. Backward-compat:
```sql
-- Pages 0-4: existing OFFSET (fast)
-- Pages 5+: cursor from previous page's last (start_timestamp, id)
WHERE (e.start_timestamp, e.id) < ($cursor_ts, $cursor_id)
ORDER BY e.start_timestamp DESC, e.id DESC
LIMIT 30
```

Не migration, code change в synthesizer.

---

## D. Sequential scan risks

### Finding 6 — [CRITICAL] `/ops/snapshots/summary?detail=true` — 5 COUNT(DISTINCT) на 148GB

**File:line**: `local_api_server.py:4106-4119`

**Issue**: При `?detail=true`:
```sql
SELECT COUNT(*), COUNT(DISTINCT trace_id), COUNT(DISTINCT endpoint_pattern),
       COUNT(DISTINCT context_event_id), COUNT(*) FILTER (...)
FROM api_payload_snapshot
```
5 distinct full-table aggregates → **гарантированный >30s timeout**.

**Recommendation**:
1. Gate `?detail=true` behind operator role / header
2. Default path уже uses `pg_class.reltuples` (fast)

**Code change**, no migration.

### Finding 7 — [WARNING] `retention_repository.delete_legacy_snapshot_batch` будет fail-ить под 30s timeout

**File:line**: `storage/retention_repository.py:111-138`

**Issue**: Batched DELETE на 5000 rows из 148GB taking ~60s — **превышает 30s statement_timeout** (наш P0 fix).

**Recommendation**:
```python
async with connection.transaction():
    await connection.execute("SET LOCAL statement_timeout = '120s'")
    await connection.execute(delete_legacy_snapshot_batch_query, ...)
```

Также alternative: lower batch_size from 5000 to 1000.

### Finding 8 — [WARNING] `_fetch_unique_tournament_media_payload` — нет SQL LIMIT

**File:line**: `local_api_server.py:1922-1972`

**Issue**: `SELECT DISTINCT ON (aps.context_entity_id)` от `api_payload_snapshot` JOIN-ed с event+team+tournament+ut+season для **каждого event** в UT. Без `LIMIT`. Python cap = 200 events применяется **после** materialization. Premier League 4000 events → 4000 JSONB rows decoded then 3800 dropped.

**Recommendation**: добавить `LIMIT 500` + jsonb predicate:
```sql
WHERE jsonb_array_length(aps.payload->'highlights') > 0
ORDER BY fetched_at DESC
LIMIT 500
```

---

# Task 2: ETL & In-flight Deduplication

## A. In-flight deduplication coverage

| Worker | In-flight lock? | Key | TTL |
|--------|:---:|-----|-----|
| LiveWorkerService | ✅ via `LiveEventInFlightStore` | `live:refresh_inflight:{event_id}` | 600s |
| LiveDetailsWorkerService | ✅ via `LiveEventDetailsInFlightStore` | `live:details_inflight:{event_id}` | 300s |
| LiveWorkerService (tier_1 root-only) | ✅ via `LiveEventRootInFlightStore` | `live:root_inflight:{event_id}` | 60s |
| LiveBootstrapCoordinator | ✅ acquire_hydrate_lock | `live:hydrate_lock:{event_id}` | 60s |
| **HydrateWorker** | ❌ **MISSING** | — | — |
| **DiscoveryWorker** | ⚠️ partial (TTL squelch, не lock) | `fresh:event:{id}:{mode}` | 60s |
| NormalizeStreamWorker | ✅ ON CONFLICT DO NOTHING (SQL-level) | — | — |

### Finding 9 — [CRITICAL] HydrateWorker нет in-flight deduplication

**File:line**: `workers/hydrate_worker.py:102-191`

**Issue**: `HydrateWorker.handle()` идёт directly в `orchestrator.run_event(...)` (line 156) **без** Redis lock. Только circuit_breaker (flag-driven failure counter, не single-flight).

**Race scenario**: `sofascore-hydrate@1` и `sofascore-hydrate@4` оба pull `hydrate_event_root` для `event_id=12345` (planner re-published while первый message был unacked, или discovery нашёл event в нескольких scopes). Оба parallel `/event/12345` GETs + overlapping UPSERTs → race condition + wasted proxy budget.

**Fix recommendation**: mirror `LiveWorkerService.handle()` pattern (`live_worker_service.py:248-261` + `317-333`). Suggested key: `hydrate:inflight:{event_id}`, TTL ~300s.

### Finding 10 — [CRITICAL] CLI `App.run_event` — hydrate_lock leak on crash

**File:line**: `cli.py:411-473`

**Issue**: `App.run_event` (CLI / one-shot entry) acquire hydrate_lock at line 427, затем `_prefetch_event_run` / `_commit_prefetched_run` / `_persist_prefetched_run` **без try/finally release**. Lock остаётся 60s.

**Impact**: operator-triggered (или scheduled) one-shot crash mid-run → 60s of live polling silently dropped (точно тот bug который Task 6 fix должен был решить, но fix не landed в CLI path).

**Fix**: Wrap lines 436-466 в `try/finally`. Release lock в `finally` когда acquired.

### Finding 11 — [WARNING] `release_hydrate_lock` non-atomic

**File:line**: `live_bootstrap.py:86-136`

**Issue**: GET → CHECK → DELETE через **3 отдельные** Redis calls. Между GET и DELETE другой worker может claim. Release удаляет **new owner's** lock.

**Fix**: Use same `_RELEASE_IF_OWNER_SCRIPT` Lua pattern as `live_inflight.py:18-23`.

### Finding 12 — [WARNING] `LiveEventState.upsert + move_lane` — multi-step, не atomic

**File:line**: `queue/live_state.py:59-89`

**Issue**: HSET → ZREM × 3 → ZADD = 5 separate Redis calls. Между HSET и ZADD planner может видеть event в neither lane (или в обеих).

**Fix**: Combine into Redis pipeline / Lua script.

---

## B. Race conditions in concurrent DB writes

### Finding 13 — [WARNING] Bundle ingestion split across multiple `_persist_*` helpers — deadlock window

**File:line**: `storage/normalize_repository.py:92-185`

**Issue**: `persist_parse_result` calls ~25 separate `await self._persist_*` helpers. PostgreSQL row locks acquired sequentially → deadlock window when 2 parallel hydrate workers persist overlapping events в обратном порядке.

**Mitigation**: Retry path handles `DeadlockDetectedError` via `is_retryable_worker_error`. Acceptable but increases tail latency.

**Fix recommendation**: Sort by PK before each `executemany` для deterministic lock ordering.

---

# План действий — Phased по risk

## 🟢 Phase 1: Migrations safe (low risk, immediate effect)

### Migration 1.1: critical indexes
```sql
-- Finding 2: event.custom_id index
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_custom_id
    ON event (custom_id)
    WHERE custom_id IS NOT NULL;

-- Finding 3: live window partial
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_live_window
    ON event (status_code, start_timestamp DESC, id DESC)
    WHERE is_editor IS NOT TRUE;

-- Finding 1.B: etl_job_run indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_job_run_created_status
    ON etl_job_run (created_at DESC, status)
    WHERE status IN ('failed', 'retry_scheduled');
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_job_run_job_id
    ON etl_job_run (job_id);
```

### Migration 1.2: cleanup superseded indexes
```sql
-- Finding 4 (DB audit): drop superseded index (after verify zero scans)
-- DROP INDEX CONCURRENTLY IF EXISTS idx_api_payload_snapshot_endpoint_pattern;
-- (deferred until pg_stat_user_indexes verifies idx_scan == 0 for 30 days)
```

### Maintenance 1.3: VACUUM heavy
```sql
-- Finding 1.A: investigate event_market_choice
VACUUM (ANALYZE, VERBOSE) event_market_choice;
-- Then ANALYZE seq scan pattern via grep code
```

**Risk**: low. `CONCURRENTLY` не блокирует.

## 🟡 Phase 2: Code changes (medium risk, requires testing)

### Code 2.1: HydrateWorker in-flight lock (Finding 9)
- Add Redis lock in `workers/hydrate_worker.py:handle()`
- TDD-first: tests for lock acquire/release/timeout
- ~50-100 LOC

### Code 2.2: CLI `App.run_event` try/finally (Finding 10)
- Wrap `_prefetch_event_run` chain in try/finally
- Release hydrate_lock in finally
- ~10 LOC

### Code 2.3: `release_hydrate_lock` Lua atomic (Finding 11)
- Clone `_RELEASE_IF_OWNER_SCRIPT` from `live_inflight.py:18-23`
- Replace 3-call sequence with EVAL
- ~15 LOC

### Code 2.4: `LiveEventState.upsert + move_lane` Lua atomic (Finding 12)
- Pipeline / Lua wrap для HSET + ZREM × 3 + ZADD
- ~30 LOC

### Code 2.5: retention batch `SET LOCAL statement_timeout` (Finding 7)
- Wrap `delete_legacy_snapshot_batch` в transaction с `SET LOCAL statement_timeout = '120s'`
- Or: lower batch_size from 5000 to 1000
- ~5 LOC

### Code 2.6: `/ops/snapshots/summary?detail=true` gate (Finding 6)
- Add operator role / header check
- ~10 LOC

### Code 2.7: `_fetch_unique_tournament_media_payload` LIMIT in SQL (Finding 8)
- Add `LIMIT 500` + `jsonb_array_length > 0` predicate
- ~5 LOC

## 🔴 Phase 3: Keyset pagination (high risk, large refactor)

### Code 3.1: OFFSET → keyset for 4 endpoints (Finding 5)
- `_FETCH_QUERY_SEASON_LAST/NEXT/TEAM_LAST/TEAM_NEXT/PLAYER_EVENTS_LAST`
- Backward-compat: keep OFFSET for page < 5
- TDD coverage for cursor edge cases
- ~200 LOC across 4 endpoints

---

## Open Questions — нужен ACK перед каждой Phase

1. **Phase 1.1 — Apply 4 indexes сразу?** Low risk, `CONCURRENTLY` не блокирует. Effect: -8.3B seq tup reads на event table, -28.7B на market_choice (после grep + index).
2. **Phase 1.3 — VACUUM event_market_choice сразу?** Может занять 10-30 мин (136MB table но много dead tuples).
3. **Phase 2.1 (HydrateWorker lock)** — приоритет CRITICAL, делать первым в Phase 2?
4. **Phase 2.2 (CLI run_event leak)** — связан с Phase 2.1, делать вместе?
5. **Phase 3 (keyset pagination)** — отложить до следующего sprint? Текущий impact = latency only (не data correctness).

---

## Что НЕ нужно трогать

Confirmed safe / well-optimized:
- ✅ Root snapshot lookup paths (lookup_v2 partial index covers)
- ✅ Event detail single-event fetcher (no N+1, batched)
- ✅ Live worker stream consumer (proper Lua release pattern)
- ✅ Live state Redis primitives (mostly atomic, only `upsert+move_lane` weakness)
- ✅ Snapshot insert + ON CONFLICT (PostgreSQL handles concurrency)

Не сильно болит:
- ✅ `MAX(fetched_at) FROM api_payload_snapshot` (mitigated via `api_snapshot_head`)
- ✅ `COUNT(*) FROM api_payload_snapshot` (already uses `pg_class.reltuples` in default path)
- ✅ `source_url LIKE '%...%'` patterns (mostly removed in 2026-05-15/16 fixes)

---

**Готов к ACK по любой phase / migration / code change**. Каждая фаза независима, можно делать в любом порядке.
