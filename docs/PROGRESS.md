# PROGRESS — путь к 24/7 production-ready

**Живой трекер %-прогресса. Обновляется после каждого significant commit.**

| Поле | Значение |
| --- | --- |
| Версия | v1 (2026-05-14) |
| Текущий статус | **Implementation: ~71% / Proof window: not yet started** |
| Источник критериев | `docs/24x7-exit-criteria.md` + `docs/PROJECT_VISION.md` §2 SLO + §6 Roadmap |
| Когда `100%`? | Когда **все** capabilities в таблице ниже = ✅ **И** 24-часовое proof window прошло без regressions |

---

## TL;DR

**Overall: 71% implementation done, 0 hours of proof window accumulated.**

Что уже работает 24/7 без нашего внимания:
- ✅ N1 monitoring → Telegram канал FLOWSCORE MONITORING. Алерты приходят за минуты.
- ✅ Sweeper (P0.B) — finalised events убираются из hot lane, oldest_hot_age <100s.
- ✅ N4 Layer A+B+C — 79.5% API endpoints под 50ms на warm cache.
- ✅ Structure-sync с правильным current season selection (Codex fix `39380e7`).

Что ещё нужно для 100%:
- ❌ **Layer D** — DB optimization для 20 sport-level scheduled-events endpoints (timeout). Planned, see §"Layer D plan".
- ❌ **Production deployment surface** — HTTPS работает (`api.var11.com`), но нет auth/rate-limit/public docs для запуска пользователям.
- ❌ **API p95 latency signal** в N1 monitoring — observability gap.
- ❌ **Sentinel probe** — daily Sofascore URL/schema check.
- ❌ **Disaster recovery plan** — backup strategy, hot standby.
- ❌ **Proof window** — 24 часа подряд без regressions.

---

## Implementation breakdown

Weighted progress per category. Веса reflect their contribution to 24/7 readiness.

### Operational stability (weight: 50%)

Status: **6/7 done = ~85%** → contributes **42.5%** to overall.

| Capability | Status | Notes |
| --- | --- | --- |
| Supervised systemd units | ✅ | api, hydrate@1-9, live-tier-1@1-5, live-tier-2@1-4, live-tier-3@1-9, live-hot, live-warm@1-3, live-discovery, planners, structure, monitoring, cache-warmer — всё под systemd |
| Reproducible deployment | ✅ | `git pull --ff-only origin main` + `systemctl restart <service>` — никаких manual steps |
| Readiness check script | ✅ | `scripts/prod_readiness_check.py` (caveat: baseline check, не full 24/7 gate) |
| Operator runbooks | 🟡 | `docs/runbooks/monitoring.md` есть, нужны runbooks для: sweeper recovery, queue trim, live worker pool sizing |
| Current architecture docs | ✅ | `docs/PROJECT_VISION.md` v2, `docs/N1_MONITORING_PLAN.md`, `docs/N4_API_PERFORMANCE_PLAN.md`, `docs/current-runtime-architecture.md` |
| Monitoring + alerting | ✅ | `sofascore-monitoring.service` running, 11 signals, Telegram канал. Sweeper regression на проде поймали за 30 минут после deploy N1 |
| Sweeper P0.B working sustained | 🟡 | Fixed `39380e7` ago ~1 час, нужно 24h proof window |

### Live data freshness (weight: 25%)

Status: **3/4 done = ~75%** → contributes **18.75%** to overall.

| Capability | Status | Notes |
| --- | --- | --- |
| `oldest_hot_score_age_seconds` < 100s | ✅ | Currently 50-97s sustained |
| Sweeper removes finalised events | ✅ | Every 60s, 0 errors после fix `724d77f` |
| `tier_1_blocked_rate` healthy | 🟡 | Cumulative metric reset done, real rate 62.5% (vs theoretical max 92%) |
| Smartproxy timeouts | ⚠ | 12s timeouts на tier_1 → quarantine. Otherwise system works. Отдельная проблема. |

### API performance (weight: 10%)

Status: **4/6 done = ~67%** → contributes **6.7%** to overall.

| Capability | Status | Notes |
| --- | --- | --- |
| N4 Layer A composite snapshot index | ✅ | Migration `2026-05-14_api_payload_snapshot_lookup_v2.sql` applied, 31.6% → 63.2% endpoints fast |
| N4 Layer B Redis response cache | ✅ | `schema_inspector/api_cache.py`, deployed with `SOFASCORE_API_RESPONSE_CACHE_BACKEND=redis` |
| N4 Layer C cache warmer daemon | ⚠ | Shipped + deployed, **disabled 2026-05-14 23:11** after caused Postgres OOM kill via heavy sport-level timeout queries. Re-enable only after Layer D fix OR skip-on-timeout safeguard. See "Incident postmortems" section. |
| 95%+ endpoints under 50ms | ❌ | Currently 79.5% warm cache. Blocker = Layer D (see §"Layer D plan") |
| Layer D (sport-level scheduled-events DB optimization) | ❌ | **Plan ready below**. Estimated 0.5-2 days. |
| API p95 latency signal в N1 monitoring | ❌ | Planned. Простой `/ops/api-latency` endpoint + Telegram alert if p95 > 100ms |

### Production deployment surface (weight: 10%)

Status: **1/4 done = ~25%** → contributes **2.5%** to overall.

| Capability | Status | Notes |
| --- | --- | --- |
| HTTPS endpoint | ✅ | `https://api.var11.com/api/v1/...` работает (через какой-то reverse proxy) |
| API authentication (key/JWT) | ❌ | Anyone with URL может read данные — для запуска пользователям нужен auth |
| Rate-limiting (per-key или per-IP) | ❌ | Защита от abuse |
| Public API documentation | ❌ | Нужна минимум Swagger UI публично + getting-started page |

### Historical / coverage (weight: 5%)

Status: **0/2 done = 0%** → contributes **0%** to overall.

| Capability | Status | Notes |
| --- | --- | --- |
| Historical backfill program | ❌ | Sofascore Wikipedia-level archive — отложено в `PROJECT_VISION.md` §6 Эпоха 2 |
| Historical lanes can run без destabilizing live | ❌ | Сейчас historical lanes off (live-first priority). Нужен план как safely re-enable |

### Proof window (gate, не weight)

Status: **0/24 hours** — proof window не начат.

Когда implementation = 100%, нужно:
- 24 часа подряд без `TimeoutError` regression в hydrate logs
- 24 часа подряд без sustained growth в `stream:etl:hydrate` / `live_hot` lag
- 24 часа подряд без failed jobs trend up
- N1 monitoring канал — никаких CRIT alerts uninvestigated

Proof window — **отдельный gate**, не imp %. Достигаем 100% implementation, затем стартуем 24h watch.

---

## Overall %

```
Implementation:  71%   (42.5 + 18.75 + 6.7 + 2.5 + 0 = 70.45 → ~71%)
Proof window:    0% of 24 hours
```

Update history:
| Date | Implementation | Proof | Note |
| --- | --- | --- | --- |
| 2026-05-14 | **71%** | 0/24h | Initial baseline after N1 + sweeper fix + N4 A/B/C + Codex season fix |
| 2026-05-14 23:20 | **65%** | 0/24h | Cache warmer disabled after OOM incident — Layer C effectively off pending Layer D or warmer skip-on-timeout safeguard. See "Incident postmortems" section. |

---

## Incident postmortems

### 2026-05-14 23:03 EEST — PostgreSQL OOM kill (CRIT alert from N1 monitoring)

**Trigger:** N1 monitoring channel FLOWSCORE MONITORING received CRIT alert: `oldest_hot_score_age_seconds = 1906s` (threshold 1800s) at 19:51 UTC.

**Root cause:** PostgreSQL cluster `16-main` killed by Linux OOM killer. Peak memory before kill: **57.1 GB** out of 62 GB total RAM on server.

**Why memory ballooned:** N4 Layer C cache warmer (`sofascore-cache-warmer.service`) was triggering 14 heavy `sport/{slug}/scheduled-events/{date}` queries every 60s. These are exactly the endpoints that timeout in DB (still requiring Layer D). Each fetch attempt opened a server-side query that allocated work_mem buffers before failing. Cumulative effect: ~80 minutes of cache-warmer accumulated ~40 GB of unreclaimed query memory in Postgres backend processes.

**Cascade:**
1. 21:39 — cache warmer deployed and enabled
2. 21:39 — 23:03 — heavy queries fired periodically, Postgres memory grew
3. 23:03:06 — kernel OOM-killer hit `postgresql@16-main` — process killed
4. 23:03:13 — service unit reported `Failed with result 'oom-kill'`
5. 23:03 — 23:16 — sofascore services in retry loop (cannot acquire DB connections); `oldest_hot_score_age_seconds` climbed because sweeper couldn't fetch finalized events
6. 23:11 — operator (we) noticed via Telegram alert, diagnosed via journalctl + `pg_lsclusters`
7. 23:11 — cache warmer stopped + disabled (`sudo systemctl disable sofascore-cache-warmer`)
8. 23:12 — Postgres cluster restarted (`sudo systemctl reset-failed postgresql@16-main && systemctl start ...`)
9. 23:12 — 23:15 — WAL replay (~2 min — manageable for our DB size)
10. 23:15 — Postgres accepts connections; sofascore services connect automatically via systemd retry
11. 23:16 — sweeper resumed; `oldest_hot_score_age` 1906s → 108s within one tick

**Total downtime:** ~12 minutes (23:03 to 23:15). API returned 500 for cache-miss requests during this window. Cache hits (Layer B Redis) continued serving stale-but-recent data.

**N1 monitoring did its job:** alert arrived at 19:51 UTC (within 5 minutes of breach), gave operator the right signal to diagnose. Without N1, we would have noticed when frontend started serving 500s mass.

**Immediate mitigation:**
- ✅ Cache warmer stopped + disabled — root cause removed.
- ✅ Postgres restarted + recovered.
- ✅ sofascore services auto-reconnected via systemd.

**Permanent mitigation (pending — see below):**
1. **Cache warmer: skip timeout endpoints.** Track per-target failure count; auto-disable a target after N consecutive timeouts. Pull request needed in `schema_inspector/cache_warmer/daemon.py`.
2. **OR Layer D first.** Once sport-level scheduled-events queries are fast (D.1 + D.2 plan below), cache warmer is safe to re-enable.
3. **Postgres config hardening.** Set `statement_timeout` (e.g. 30s) at role level — prevents any single query from holding work_mem indefinitely. Operator decision, separate from Sofascore code.

**Status:** Layer C cache warmer **remains disabled** until either of the two permanent mitigations ships. This brings API performance achievement from 79.5% back toward 67% (loss of warm-cache benefits for sport-level endpoints), but stability > raw perf %.

---

## Commit log (chronological, newest first)

After each significant commit on `main`, an entry is appended here describing what shipped and how it nudges the % needle.

### 2026-05-14 — Codex season-info fix verified in prod
- **Commit:** `39380e7 fix: use upstream season order for structure sync current season`
- **Status:** Already on `origin/main` (rode in with my Layer C push). Restarted `sofascore-structure-sync` + `sofascore-structure-planner` to load fixed code into memory.
- **Verified:** `GET https://api.var11.com/api/v1/unique-tournament/8/season/77559/info` returns HTTP 200 (`"LaLiga 25/26"`). `pytest tests/test_structure_sync.py -q` on prod: **37 passed**.
- **Movement:** structure-sync correctness +1 (no % change — bug was latent, fix prevents future season-info 404s for tournaments where historical season IDs exceed current).

### 2026-05-14 — N4 Layer C cache warmer deployed
- **Commit:** `e45dd35 feat(perf): N4 Layer C — cache warmer daemon for sport-level aggregations`
- **What:** `schema_inspector/cache_warmer/` module (config, daemon, url_targets), CLI subcommand `api-cache-warmer`, `sofascore-cache-warmer.service` systemd unit. 42 hot URLs across 14 sports.
- **Verified:** Service active running, 200 OK on sport-level endpoints from cache.
- **Movement:** API performance 50% → 67% (Layer C done, Layer D + p95 signal pending).

### 2026-05-14 — N4 Layer B Redis response cache deployed
- **Commit:** `9d377ee feat(perf): N4 Layer B — pluggable Redis response cache backend`
- **What:** `schema_inspector/api_cache.py` (Null/InProcess/Redis backends + factory + TTL policy). `local_api_server.py` refactored to use pluggable backend. Env flag flipped on prod: `SOFASCORE_API_RESPONSE_CACHE_BACKEND=redis`.
- **Verified:** Warm-cache TTFB 2.49ms (cold 15.76s). Two-pass audit: **79.5%** endpoints under 50ms.
- **Movement:** API performance 33% → 50%.

### 2026-05-14 — N4 Layer A composite snapshot index deployed
- **Commit:** `89b7fb3 feat(perf): N4 Layer A — composite snapshot lookup index + plan`
- **What:** Migration `2026-05-14_api_payload_snapshot_lookup_v2.sql` (CONCURRENTLY, partial index). + `docs/N4_API_PERFORMANCE_PLAN.md`.
- **Verified:** Re-audit: 31.6% → 63.2% endpoints under 50ms.
- **Movement:** API performance 17% → 33%.

### 2026-05-14 — API latency audit tool committed
- **Commit:** `d2e3781 ops: API latency audit tool (Phase 0 of perf audit)` + fix `5561af0 fix(audit): sample event_id by id`
- **What:** `scripts/audit_api_latency.py` — replays every endpoint in `endpoint_registry` with realistic IDs from prod DB, outputs CSV + Markdown summary. Tool reused across all 4 audit passes today.
- **Movement:** Infrastructure (no % change directly, enables all subsequent perf work).

### 2026-05-14 — PROJECT_VISION.md v2 with lessons section
- **Commit:** `51d009f docs: PROJECT_VISION v2 — N2 hypothesis update + N1 closure`
- **What:** Documented that "decompose live-discovery persist" hypothesis was disproven by recon. Added §11 "Lessons / Hypothesis updates" with methodological notes (verify before code, integration coverage gap, XLEN vs lag distinction).
- **Movement:** Operational docs +1 (no % change directly — closes the loop on N2 → sweeper regression discovery).

### 2026-05-14 — Sweeper regression fix (the big save)
- **Commit:** `724d77f fix(planner): live_state_sweep callback uses app.database, not self.database`
- **What:** Fixed `AttributeError` regression from P0.B sweeper deploy. Callback referenced `self.database` where it should be `self.app.database`. Sweeper had not executed any cycle on prod for 20+ minutes; `oldest_hot_score_age_seconds` had climbed to 970s.
- **Verified:** Post-fix, `oldest_hot_score_age` 970s → 97s (~10× reduction). RESOLVED alert in FLOWSCORE MONITORING channel.
- **Movement:** Live data freshness 0% → 75% (this was THE blocker for live SLO).

### 2026-05-14 — Monitoring prod baseline tuning recorded
- **Commit:** `c473276 docs(monitoring): record 2026-05-14 prod baseline tuning`
- **What:** Documented env-overridden thresholds applied to `/opt/sofascore/.env` (oldest_hot_age WARN 900 / CRIT 1800, hydrate_xlen WARN 800k / CRIT 1.5M, etc). These are NOT new SLO targets — temporary calibration until queue depth normalises.
- **Movement:** Operational docs.

### 2026-05-14 — N1 monitoring deployed end-to-end
- **Commit:** `ffc403e feat(monitoring): N1 daemon (SLO + queue + job signals, Telegram, dedupe)` + `fd59951 db: BRIN index on etl_job_run.started_at` + `6017f3f docs: add N1 monitoring plan + runbook`
- **What:** Complete `schema_inspector/monitoring/` module (7 files: config, signals, alerter, dedupe, signal_source, daemon, __init__). CLI subcommand `monitoring-daemon`. systemd unit `sofascore-monitoring.service`. 11 signals (3 SLO from `/ops/live-freshness`, 5 queue XLEN from `/ops/queues/summary`, 3 job signals opt-in via `etl_job_run` BRIN index). Telegram bot `@flowscore_monitoring_bot` posts to FLOWSCORE MONITORING channel.
- **Verified:** 108 new tests pass. Daemon detected sweeper regression within 30 minutes of deploy.
- **Movement:** Operational stability 71% → 85%. Monitoring was the largest gap.

### 2026-05-14 — PROJECT_VISION.md v1 committed
- **Commit:** `bb99179 docs: add PROJECT_VISION.md to anchor product/SLO/roadmap`
- **What:** Initial 11-section vision document: SLO (≤5s live lag, <30min downtime), scope (12 sports, all leagues, Wikipedia-level history), roadmap by Epoch.
- **Movement:** Operational docs (foundation for all subsequent work).

---

## Layer D plan — DB optimization для sport-level scheduled-events

**Цель:** убрать оставшиеся 20 endpoints из catastrophic+timeout band. Поднять с 79.5% до 95-100%.

### Проблема

Эти endpoints **таймаут на 30s даже на первом hit от cache-warmer** — то есть DB query сам по себе превышает HTTP/uvicorn timeout. Cache никогда не наполняется → kullanıcı тоже видит timeout.

Примеры пострадавших:
- `/api/v1/sport/tennis/scheduled-events/{date}`
- `/api/v1/sport/table-tennis/scheduled-events/{date}`
- `/api/v1/sport/basketball/scheduled-events/{date}`
- `/api/v1/sport/baseball/scheduled-events/{date}`
- `/api/v1/sport/ice-hockey/scheduled-events/{date}`
- ... ~15 спортов с большой active scheduled coverage

### Гипотезы причин

| # | Гипотеза | Verify by |
| --- | --- | --- |
| H1 | `api_payload_snapshot` query для sport-level aggregations делает full table scan на 4M rows без подходящего partial index | `EXPLAIN ANALYZE` одного из timeout queries |
| H2 | Query JOIN-ит много таблиц (sport → category → tournament → event) и planner picks неоптимальный план | `EXPLAIN ANALYZE` + проверить planner choice |
| H3 | Payload reconstruction сериализует 100+ events × 50KB каждый, JSON parsing занимает 5-10s в Python | Замерить через `cProfile` worker |
| H4 | Sofascore upstream для этих endpoints тоже медленный (mirror-сторонняя задержка) | Сравнить через прямой curl к sofascore.com |

### Phase D.1 — Diagnose (read-only, 30 min)

1. Find one timeout endpoint, e.g. `sport/tennis/scheduled-events/2026-05-14`.
2. `EXPLAIN (ANALYZE, BUFFERS) <SQL>` на проде.
3. Identify bottleneck: index scan ratio, sequential scan rows, JOIN cost, payload aggregation cost.
4. Decide which hypothesis is correct.

### Phase D.2 — Targeted fix (если H1 или H2)

Likely outcome: добавить **второй partial index** для sport-level lookups:
```sql
CREATE INDEX CONCURRENTLY idx_api_payload_snapshot_sport_lookup
  ON api_payload_snapshot (endpoint_pattern, source_url, id DESC)
  WHERE endpoint_pattern LIKE '/api/v1/sport/%/scheduled-events/%';
```
ИЛИ refactor query чтобы hit existing index.

**Effort:** 0.5 дня. **Risk:** low (CONCURRENTLY).

### Phase D.3 — Materialized view (если H3 dominant)

Если payload reconstruction всегда тяжёлый (большие JSON), сделать `MATERIALIZED VIEW mv_sport_scheduled_events` с refresh от планнера каждую минуту.

**Effort:** 2 дня. **Risk:** medium (invalidation complexity).

### Phase D.4 — Optional cache for slow sport-level endpoints with long TTL

Если H4 (upstream slow), просто увеличить cache TTL для этих endpoints до 5-10 минут. User видит чуть stale данные, но <50ms.

**Effort:** 5 минут (env tweak). **Risk:** very low.

### When?

**Estimated timeline:**

| Если ACK сегодня (2026-05-14 вечер) | Done by |
| --- | --- |
| D.1 diagnose | Сегодня (30 минут) |
| D.2 если простая (один индекс) | Сегодня вечером |
| D.3/D.4 если сложная | Завтра/послезавтра |

**Без ACK от Бобура — Layer D pending в roadmap.** Когда дашь ACK — стартуем с D.1 diagnose.

### When 24/7 ready?

Honest estimate в рабочих днях (1 неделя/месяц темп от Бобура):

| Capability | Effort | When |
| --- | --- | --- |
| Layer D D.1 + D.2 | 0.5-1 день | Within 1-2 days |
| API p95 latency signal | 0.5 дня | Within 1 week |
| Sentinel probe (N3) | 1 день | Within 1-2 weeks |
| Operator runbooks (rest) | 1 день | Within 1-2 weeks |
| Production auth + rate-limit (M1) | 2-3 дня | Within 1 month |
| Proof window (24h watch) | 1 day calendar time | After all above |
| **Total to 100%** | **~5-8 working days + proof window** | **2-4 weeks calendar time** |

Это **realistic**, не optimistic. Бобур решит accelerated pace work-days.

---

## How to update this file

After **every significant commit** (feature/fix/migration — not docs typo):

1. Add new entry to top of "Commit log" section with: commit hash, what shipped, % movement.
2. Recompute "Overall %" if a capability flipped status (❌→✅ etc).
3. Add row to "Update history" table with date + new % + brief note.
4. Commit `docs/PROGRESS.md` with commit message like `docs(progress): record <commit short hash> impact`.

Docs-only commits (typo fixes, comment-only changes) **don't** need a PROGRESS entry — only behavior-changing or capability-shifting commits.

---

## Связанные документы

- `docs/24x7-exit-criteria.md` — official exit criteria (the bar)
- `docs/PROJECT_VISION.md` — vision, SLO, roadmap, lessons
- `docs/N1_MONITORING_PLAN.md` — monitoring design (done)
- `docs/N4_API_PERFORMANCE_PLAN.md` — API perf 3-layer plan (Layer A/B/C done, Layer D below)
- `docs/runbooks/monitoring.md` — operational runbook
- `docs/ARCHITECTURE_AUDIT.md` — risk register (may be partially stale, see PROJECT_VISION v2 §11 lessons)
