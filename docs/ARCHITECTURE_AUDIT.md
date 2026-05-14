# Архитектурный аудит и план 24/7

**Дата**: 2026-05-14 (v3 — re-check +9h после firebreak/waterfall)
**Метод**: синтез существующих docs (PROJECT_OVERVIEW, SERVICES_AND_WORKERS, DATABASE_AND_STORAGE, REDIS_AND_QUEUES, OPERATIONS_RUNBOOK, current-runtime-architecture, 24x7-exit-criteria, plans/2026-04-*, plans/2026-05-*) + опыт инцидентов 2026-05-13 + **read-only verification на проде 2026-05-14 (initial 02:00 + re-check 11:30 EEST)**.

**v3 update** (2026-05-14 11:30 EEST): sustained 9h sanity check после firebreak/waterfall deploy показал что все 5 ключевых целей держатся (0 deadlocks, 0 live-discovery retries, 0 TimeoutError, lag=0 на всех tier streams, retry rate в 8.7x ниже baseline). Минорные корректировки в C.2 (stale age **deteriorating slowly**, нужен P0.B), B.1 caveat (0 retries в low-load time-of-day не доказывает fix), A.1 (rebuild timer всё ещё отсутствует).

## Evidence convention

Каждое утверждение этого документа помечено колонкой:

- **verified** — подтверждено grep/SQL/prod env inspection 2026-05-14
- **inferred** — следует из docs/code/incidents без прямой проверки
- **unknown** — нужен дополнительный probe или upstream telemetry

Это **не финальный truth**, а живой risk register. Обновляется при каждом крупном hardening commit.

---

## Часть 1. Корневые причины — почему ломается в моменте

### Категория A. Hot row contention в PostgreSQL (deadlock storm)

#### A.1 `endpoint_capability_rollup` deadlock storm

| Поле | Значение |
|---|---|
| Status | **verified fixed** (firebreak 2026-05-13) |
| Evidence | commit `6e46522`, tests `tests/test_capability_rollup_firebreak.py` зелёные, prod env: `SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED` отсутствует → default OFF. **Re-check 2026-05-14 11:30**: pg_locks показал 1 `ShareUpdateExclusiveLock` granted на ECR (housekeeping retention, не hot path), 0 waiting; 0 DeadlockDetectedError в etl_job_run last 1h. Sustained 9h. |
| Симптом (был) | 12+ workers зависают на `INSERT ... ON CONFLICT (sport_slug, endpoint_pattern) DO UPDATE`, deadlock → retry storm → live-discovery worker не успевает persist'ить → live coverage 30-50% gap |
| Корень | 290 hot rows × ~20 concurrent workers × read-modify-write pattern |
| Что сделано | `SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED=0` default. Hot path только observations append. Rollup batch'ом через `rebuild-capability-rollup` CLI. |
| Остаточный риск | **Re-check 2026-05-14**: `systemctl list-timers` подтверждает что `sofascore-rebuild-capability-rollup.timer` всё ещё отсутствует на проде → rollup state стареет с момента deploy (~16h). Не live-critical, но нужно ops hygiene timer (см. P1.1). |

#### A.2 Topology write amplification (sport / country / category / unique_tournament / season)

| Поле | Значение |
|---|---|
| Status | **verified fixed** (план 04-22 implemented) |
| Evidence | grep по `IS DISTINCT FROM` в `categories_seed_repository.py` (line 102-159), `competition_repository.py` (line 144-398). Country/category/unique_tournament/season все имеют no-op guards. |
| Симптом (был) | `pg_stat_activity` показывал `AccessExclusiveLock waiting tuple` на `country` table в моменты пика; high tuple lock contention |
| Корень | HOT_UPD=0% — изменение индексных колонок при upsert создаёт новую row version. План 04-22 решил через `WHERE current_value IS DISTINCT FROM EXCLUDED.value` guards. |
| Что сделано | Per-repository `IS DISTINCT FROM` clauses на upserts. UNKNOWN — есть ли process-local cache для sport (план 04-22 Task 2) — но это секондарный optimization. |
| Остаточный риск | Все 5 repositories из плана 04-22 (Task 4) могут не быть покрыты — нужен grep audit и/или regression test. |

#### A.3 `event_endpoint_negative_cache_state` / `endpoint_negative_cache_state`

| Поле | Значение |
|---|---|
| Status | **inferred ok** |
| Evidence | Prod env: `SCHEMA_INSPECTOR_EVENT_NEGATIVE_CACHE_ENABLED=true`, `MODE=enforce`. Lease-based concurrency в коде. |
| Симптом | Потенциал race condition при `try_acquire_probe_lease` под concurrency. **Не наблюдалось как production incident**. |
| Корень | PK `(event_id, status_phase, endpoint_pattern)` — multiple workers конкурируют. Lease смягчает. |
| Остаточный риск | Если когда-то увидим в `pg_stat_activity` waits на этих таблицах — рассмотреть atomic CAS или Redis-only вариант. |

### Категория B. Live state coordination fragility

#### B.1 `live-discovery@1` — single instance + большая persist транзакция

| Поле | Значение |
|---|---|
| Status | **verified single instance** + **inferred large transaction** |
| Evidence | `systemctl list-units` показывает `sofascore-live-discovery@1` single template. План 04-22 self-requeue fix реализован (см. ниже B.4), но **persist transaction granularity не уменьшен**. **Re-check 2026-05-14 11:30**: 0 "scheduling retry" log lines за last 1h. **Caveat**: время суток (утро EEST, tier_1=0 live matches), низкая нагрузка ⇒ 0 retries **не доказывает** что транзакция fixed. Только означает что под спокойной нагрузкой проблема не проявилась. |
| Симптом | Если worker зависает в retry loop (deadlock / transport issue / DB lock) — НОВЫЕ events не попадают в `zset:live:hot/warm/cold`, planner не публикует refresh, live coverage stagnates. Видели 2026-05-13: 137/184 football matches missing. |
| Корень | Один большой transactional persist всей discovery bundle (events + countries + tournaments + teams + capability obs + live state update). Single instance ⇒ при stuck нет fallback. **Scale up НЕ решит**: один тяжёлый event-list persist job не распилится между N consumer'ами — это один XADD entry. |
| План | **P0**: разбить persist на стадии (см. P0.A ниже): (1) atomic snapshot insert, (2) downstream upserts отдельные batches, (3) live state update — отдельный stage с фиксированным retry budget. Затем scaling@2..3 — для backlog, не для concurrency на одном job. |
| Остаточный риск | Архитектурный (eventual consistency для live state) — требует cap'a в дизайне. **True stress test**: нужен evening EEST когда tier_1 live matches активны и upstream payload большой (Champions League, etc.). |

#### B.2 Planner SIGTERM zombie (30s timeout → SIGKILL)

| Поле | Значение |
|---|---|
| Status | **verified** |
| Evidence | Журналы 2026-05-13: `sofascore-planner.service: State 'stop-sigterm' timed out. Killing.` Прямое наблюдение. |
| Симптом | `systemctl restart` зависает на 30s. После SIGKILL — 10-15 мин warmup пока `zset:live:hot` восстанавливается. |
| Корень | Planner main loop держит длительные asyncio await'ы (вероятно DB query или Redis multi-op). Сигналы не обрабатываются достаточно быстро. |
| План | P2: persist cursor каждые 5s в Redis. Уменьшить gun-cocked time в loop. Restart возобновляется без full warmup. |

#### B.3 Tier_1 dispatch claim_failed_blocked

| Поле | Значение |
|---|---|
| Status | **verified fixed** (waterfall Stage A+B 2026-05-13) |
| Evidence | `.env`: `LIVE_DISPATCH_LEASE_TIER_1_MS=12000`, `tier_2=15000`, `tier_3=60000`. Tier_1 blocked rate 93.6% → 81.0% устойчиво в 2h-snapshot. |
| Корень | До: lease=90s vs poll=5s → 1 publish per 18 polls. |
| Остаточный риск | Можно дальше снизить tier_1 до 6000ms (с запасом против 5s poll). Зависит от worker capacity. |

#### B.4 Self-requeue / terminal handling (план 04-22 plan)

| Поле | Значение |
|---|---|
| Status | **verified fixed** |
| Evidence | grep `'canceled'`, `'cancelled'` в `planner/live.py:35` — оба в terminal set. `_publish_live_refreshes` в planner_daemon.py L128 — единственный publisher (workers только `track_event` без stream emit). |
| Что было | Live workers и planner оба могли публиковать refresh_live_event → self-requeue storm для terminal events. |
| Что сделано | Two-part contract: workers только persist state в Redis, planner единственный publisher по `due_events`. Terminal aliases (`canceled`/`cancelled`) нормализованы. |

### Категория C. Live-only гепы (data gaps в реальном времени)

#### C.1 Matchcenter dying во время live

| Поле | Значение |
|---|---|
| Status | **verified fixed** для football (X4 patch); **unknown** для остальных спортов |
| Evidence | Tests `test_detail_resource_policy.py` (X4 fall-through для football live_delta). Diagnostic probe 2026-05-13: 12/15 endpoints были `LOCAL MISSING` для football live до X4. |
| Симптом | Live football match — `/lineups`, `/statistics`, `/incidents`, `/managers` все 404 на локальном API. Заполняется только после `final_sweep` когда матч finished. |
| Корень | `live_delta_detail_endpoints("football") = ()` пустой → planner не публиковал детальные ручки в live polling. |
| Что сделано | Football live_delta fall-through на full detail spec. |
| Остаточный риск | **Football-only fix**. Для **basketball/tennis/baseball/ice-hockey/handball/rugby/cricket/esports/futsal/volleyball/table-tennis/american-football** — поведение УNKNOWN. Если продукт football-only — нет приоритета. Если frontend roadmap расширяется — нужен per-sport audit (P2 ниже). |

#### C.2 Stale entries в `zset:live:hot`

| Поле | Значение |
|---|---|
| Status | **verified observed, deteriorating** |
| Evidence | Timeline oldest score age в hot zset: 2026-05-13 22:00 = 76 мин; 2026-05-14 00:50 = 22.8 мин (post-restart); 2026-05-14 02:00 = 22.8 мин; **2026-05-14 11:30 = 40 мин**. Тренд: после рестарта планировщика начинает с малого, растёт со временем. ZCARD стабильный ~85-126 events. |
| Корень | После SIGKILL planner или crash live-discovery — events с terminal_status не убираются из live state. Без active cleanup-а они аккумулируются. |
| План | P0.B: periodic sweep — для events с `event_terminal_state.terminal_status` set + grace window 5 мин — `ZREM` из live lanes. **Важно (по фидбеку)**: grace window обязателен, иначе риск удалить event с неправильным terminal state. Sanity: cross-check с upstream live list. **Эскалация (re-check 11:30)**: stale age растёт каждые несколько часов на 5-10 мин — без cleanup за неделю можно ожидать накопление к 2-3 часам. |

### Категория D. Transport-уровневая нестабильность

#### D.1 `SOFASCORE_FETCH_TIMEOUT_SECONDS=10` per-attempt

| Поле | Значение |
|---|---|
| Status | **verified accept-reality** |
| Evidence | Diagnostic probe 2026-05-13: 20/20 chrome110+Smartproxy success на normal events. Chronic-timeout events — per-event Sofascore throttle, не наш транспорт. Closed как Fix B Option A. |
| Симптом | Chronic-timeout events (ITF M15 теннис, KBO бейсбол) — 79+ timeouts/24h на отдельных матчах. |
| Корень | Per-event Sofascore upstream throttle. |
| План | P1: **adaptive polling** (Option B из Fix B closeout). Если event имеет N consecutive timeouts → demote tier (1→2→3) или extend `poll_seconds`. Per-event cooldown state в Redis. Не отменяет TimeoutError, но не burning workers на dead events. |

#### D.2 curl_cffi version lock (chrome99-131 only)

| Поле | Значение |
|---|---|
| Status | **verified** |
| Evidence | curl_cffi 0.5.10 installed. Tests подтверждают chrome116-131 not supported. |
| Симптом | Cannot use chrome131+ TLS profile. |
| План | P3: upgrade curl_cffi до >= 0.7 (поддерживает chrome131-136). Не срочно. |

#### D.3 `/ops/health` 500 TimeoutError

| Поле | Значение |
|---|---|
| Status | **verified pre-existing** |
| Evidence | Журналы 2026-05-13: `/ops/health` периодически возвращает 500 (TimeoutError в run_in_runtime). |
| Симптом | Health endpoint иногда таймаутит под нагрузкой. |
| Корень | Много DB queries в одном handler. |
| План | P1: parallel queries через `asyncio.gather`, per-section TTL cache, fail-fast если subquery > 5s. |

### Категория E. Observability + резистентность

#### E.1 Structured retry log

| Поле | Значение |
|---|---|
| Status | **verified fixed** |
| Evidence | commit `6e46522` (firebreak), tests `tests/test_worker_runtime_retry_log.py`. Production journal показывает `exc_type=...`, `sqlstate=40P01`, `duration_ms=...`. |

#### E.2 Stage observability table

| Поле | Значение |
|---|---|
| Status | **verified table exists** + **unknown coverage** |
| Evidence | `migrations/2026-04-21_job_stage_observability.sql` существует. `etl_job_stage_run` таблица создана. |
| Остаточный | Полноту stage logging нужно проверить — некоторые stages могут не быть instrumented. Нет dashboard / p95-p99 alerts. |

#### E.3 Backpressure thresholds — static defaults

| Поле | Значение |
|---|---|
| Status | **inferred risky** |
| Evidence | `services/backpressure_config.py` — все limits hardcoded constants. |
| Корень | Не scaling automatically с количеством воркеров. Под бизнес-пиком `LIVE_TIER_1_MAX_LAG=200` достигается быстро. |
| План | P1: dynamic thresholds = base × worker_count, или per-tier ratio. |

### Категория F. Historical lane изоляция

| Поле | Значение |
|---|---|
| Status | **verified ongoing** |
| Evidence | Из docs/live-first-rollout.md: historical-tournament и historical-enrichment часто отключают вручную. Общий asyncpg pool для всех workers (один `create_pool_with_fallback` в `db.py`). |
| Симптом | Historical hydrate workers под нагрузкой конкурируют с operational hydrate workers за DB connections и за hot rows. |
| Корень | Common pool + shared hot rows. |
| План | **P1**: separate asyncpg pool **+ write governor** (по фидбеку). Только разделение pool лечит connection starvation, но не lock contention. Нужен ещё **live-first governor**: historical workers имеют write-budget tx/min, автопауза при live SLO breach. |

---

## Часть 2. Production env audit (verified 2026-05-14)

### Включено в `.env` на проде

| Variable | Value | Statement |
|---|---|---|
| `SOFASCORE_HOUSEKEEPING_ENABLED` | `true` | ✅ Real housekeeping running |
| `SOFASCORE_HOUSEKEEPING_DRY_RUN` | `false` | ✅ Production mode (план 04-21 Task 4 закрыт) |
| `SOFASCORE_HOUSEKEEPING_INTERVAL_SECONDS` | `60` | every 60s sweep |
| `SOFASCORE_HOUSEKEEPING_BATCH_SIZE` | `20000` | batch size per sweep |
| `SOFASCORE_HOUSEKEEPING_ZOMBIE_MAX_AGE_MINUTES` | `120` | zombie event detection threshold |
| `SOFASCORE_HOUSEKEEPING_MAX_STALE_RETIREMENTS_PER_TICK` | `100` | rate limit retirement |
| `SCHEMA_INSPECTOR_NEGATIVE_CACHE_MODE` | `enforce` | ✅ Season widget negative cache enforced |
| `SCHEMA_INSPECTOR_EVENT_NEGATIVE_CACHE_ENABLED` | `true` | ✅ Event-endpoint negative cache enabled |
| `SCHEMA_INSPECTOR_EVENT_NEGATIVE_CACHE_MODE` | `enforce` | ✅ Event-level enforced (default code был `false`/`enforce` — прод явно установил) |
| `LIVE_DISPATCH_LEASE_TIER_1_MS` | `12000` | ✅ Stage B applied |
| `LIVE_DISPATCH_LEASE_TIER_2_MS` | `15000` | ✅ Pre-existing |
| `LIVE_DISPATCH_LEASE_TIER_3_MS` | `60000` | ✅ Stage A applied |
| `SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED` | (отсутствует) | ✅ Default `0` (firebreak active) |

### Не включено / выключено на проде

| Variable | Default | Comment |
|---|---|---|
| `LIVE_SPLIT_DETAILS_FANOUT` | `0` (default) | P0(a) split-details выключен. UNKNOWN reason. |
| `LIVE_TIER_1_QUARANTINE_ENABLED` | (не в env) | UNKNOWN активирован ли. Quarantine code path existed (`live_tier_1_quarantine.py`). |
| `HYDRATE_EVENT_CIRCUIT_BREAKER_ENABLED` | (не в env) | UNKNOWN активирован ли P5b circuit breaker. |
| `LIVE_TIER_1_ROOT_ONLY` | (не в env) | UNKNOWN активирован ли P0.b fast-path. |

### Systemd timers (verified)

| Timer | Status |
|---|---|
| `sofascore-team-event-surface-refresh.timer` | active (last run 21h ago) |
| `sofascore-rebuild-capability-rollup.timer` | **не существует** — нужно создать (см. P1.1) |

---

## Часть 3. Roadmap до 24/7 — пересмотренный после verify + feedback

### P0 — critical fixes для live freshness (1-2 недели)

**P0.A — Decompose live-discovery persist transaction**
- Сейчас live-discovery worker делает один большой persist всей bundle (events + countries + tournaments + teams + obs + live state).
- Разбить на стадии:
  - Stage 1: insert raw snapshot (atomic, lock-free)
  - Stage 2: topology upserts (sport/country/category/ut/season) — отдельная транзакция, idempotent
  - Stage 3: live state update — отдельная stage с фиксированным retry budget
- Каждая стадия повторно-вызываема без cascade rollback.
- **После** этого — масштабировать `live-discovery@1..3` (3 instances) для backlog throughput.

**P0.B — Stale live state cleanup с grace window**
- Periodic sweep в planner: каждые 60s
- ZREM из `zset:live:hot/warm/cold` для events где:
  - `event_terminal_state.terminal_status` set **И**
  - `event_terminal_state.finalized_at` > grace_window (5 минут) **И**
  - event отсутствует в latest upstream live list snapshot (sanity check via `_fetch_sport_live_events_payload`)
- Файл: `services/planner_daemon.py` — новый `_sweep_stale_live_state()`
- Защита от false-positive cleanup (по фидбеку).
- **Эскалация после re-check 2026-05-14 11:30**: тренд oldest stale age — растёт без cleanup. Snapshot history: 76 мин (peak до restart) → 22.8 мин (post-restart) → 40 мин (+9h). Через 2-3 дня sustained — можно ожидать оригинальный 76-минутный peak повторно. Не критично, но имеет real cost: workers polling terminal events впустую → жгут proxy budget и DB queries.

**P0.C — Live freshness SLO dashboard**
- Метрики добавить в `/ops/health` (без блокирующих queries):
  - Median age of oldest score in `zset:live:hot` (target < 15 минут)
  - `refresh_live_event` success rate last 5 min (target > 95%)
  - tier_1 blocked rate (target < 80%)
  - upstream-vs-local football live count delta (target < 10%)
- Если ANY breached for > 5 минут → `health_status="degraded"` в JSON.
- Не нужно alerting в PagerDuty в P0 — просто observable.

### P1 — структурная стабильность (2-4 недели)

**P1.1 — Ops hygiene: capability rollup rebuild timer**
- File: `ops/systemd/sofascore-rebuild-capability-rollup.timer` + `.service`
- `OnCalendar=hourly`, `Persistent=true`
- Command: `python -m schema_inspector.cli rebuild-capability-rollup --lookback-days 2`
- **NOT live-critical** — это control-plane observability. (Переклассифицировано с P0.2 по фидбеку.)

**P1.2 — Separate asyncpg pool + live-first governor для historical**
- `db.py`: `create_historical_pool()` (отдельный, с меньшим max_size)
- Per-lane pool injection в worker_runtime
- **Plus** governor (по фидбеку):
  - Track historical write tx/min in Redis counter
  - Если `LIVE_TIER_1_MAX_LAG` breached **OR** `live_hot` lag breached → pause historical workers (signal через Redis flag)
  - Auto-resume через 5 минут после восстановления live SLO
- Без governor: только разделение pool лечит connection starvation, но не lock contention на shared hot rows.

**P1.3 — Adaptive polling для chronic-timeout events**
- Per-event timeout counter в Redis (`event_timeout_counter:<id>`)
- При N consecutive timeouts (N=5) → demote tier (3 → cold lane) или increase poll cadence (×2)
- Auto-recover при success
- Не lечит upstream throttle, но не burning workers

**P1.4 — Dynamic backpressure thresholds**
- `services/backpressure_config.py`: `*_MAX_LAG = max(base_constant, worker_count × per_worker_throughput × 2)`
- Defaults остаются как safety floor для dev / low-traffic time

**P1.5 — `/ops/health` rebuild без blocking queries**
- Parallel queries через `asyncio.gather` (если ещё нет)
- Per-section TTL cache (snapshot_count cached 60s, lag cached 5s)
- Fail-fast если subquery > 5s → return partial с warning JSON field

**P1.6 — Live SLO alerts (extension P0.C)**
- Hook /ops/health degraded status в external alerting (PagerDuty / Slack)
- Soak metrics в `coverage_ledger`-style table для history

### P2 — observability + резистентность (1-2 месяца)

**P2.1 — Planner graceful shutdown**
- Cursor persist каждые 5s
- Restart без 10-15 мин warmup
- Reduce SIGTERM timeout от 30s до 10s

**P2.2 — Atomic CAS для `upsert_rollup`** (если вернёмся к inline)
- Single SQL CASE/aggregate во время `ON CONFLICT DO UPDATE` (см. firebreak audit Option B)
- Сейчас не критично — firebreak path работает

**P2.3 — Per-sport matchcenter audit**
- Только если frontend roadmap расширяется на non-football sports
- X3-style 7-day production probe matrix per sport
- Reclassified с P1 на P2 (по фидбеку — football-first product)

**P2.4 — Live state — eventual consistency путь**
- Live state population отдельно от discovery transaction
- Discovery transaction остается lean: только snapshot + topology
- Отдельный live-state-updater worker слушает completion events

**P2.5 — 24-hour sustained soak test**
- Без manual интервенций
- All revised SLOs в зелёной зоне
- Document baseline KPIs

### P3 — масштабирование (3-6 месяцев)

**P3.1 — Multi-instance protection**
- Read replicas PostgreSQL для historical / API queries
- Redis cluster если single-instance bottleneck

**P3.2 — curl_cffi upgrade** для chrome131+

**P3.3 — Compression / cold storage** very-old snapshots в S3

**P3.4 — Source diversification** (план 04-21 multisource roadmap)

---

## Часть 4. План историзации

Цель: полный mirror Sofascore данных за прошлые N лет.

### Текущее состояние (verified)

| Что | Где | Status |
|---|---|---|
| Live + future scheduled | `event` + ROOT snapshot + matchcenter | active через planner/discovery/hydrate |
| Завершённые матчи (recent) | `event_terminal_state.final_snapshot_id` + normalized | заполняется final_sweep |
| Архивные сезоны прошлых лет | `historical-*` lanes | **часто disabled на проде** |
| Tournament structure | `season_round`, `season_cup_tree*` | через `structure-planner-daemon` |
| Leaderboards / TotW / POS | `top_player_snapshot`, `season_player_of_the_season`, `team_of_the_week*` | через `resource-planner-daemon` |
| Rolling horizon + cursors | `etl_planner_cursor` table | ✅ Plan 04-21 Task 5 done |

### Стратегия (H1-H6)

**H1 — Restore historical lanes stability (зависит от P1.2)**
- После P1.2 (separate pool + governor) — включить historical workers без риска для live.
- Cursor-based pagination = безостановочный backfill после restart.

**H2 — Per-tournament backfill audit**
- Query `tournament_registry` — какие tournaments имеют `historical_backfill_start_date`, `historical_backfill_end_date`.
- Per (sport, ut, season): completeness = `event` rows / expected_count.
- Manual gap fill: `python -m schema_inspector.cli full-backfill --event-ids ...`.

**H3 — Bulk import strategy**
- Audit upstream bulk endpoints (per season).
- Smartproxy 5 endpoints ≈ 250 req/min sustainable.

**H4 — Coverage SLO dashboard для history**
- Расширить `coverage_ledger` на historical seasons.
- SLO: per (sport, ut, season) completeness ≥ 95%.
- `/ops/coverage/summary` shows gaps.

**H5 — Source diversification** (P3 — multisource roadmap план 04-21)
- Reconcile policy уже есть.

**H6 — Cold storage / partitioning** (P3)
- `api_payload_snapshot` > 4M rows и растёт. Retention уже работает (housekeeping).
- Cold storage в S3 для snapshots > 1 года.
- Partitioning event tables by year.

---

## Часть 5. Acceptance criteria — revised 24/7 contract

Контракт из `docs/24x7-exit-criteria.md`, но с уточнениями по фидбеку (TimeoutError gate слишком строгий — заменён на rate threshold):

### Original gates (status check)

| Gate | Status |
|---|---|
| No manual `tmux` babysitting | ✅ verified (systemd cutover done) |
| Supervised restart exists | ✅ verified |
| Current architecture docs match | ✅ verified (commit 5774741) |
| One-command readiness check | ✅ verified (`scripts/prod_readiness_check.py`) |
| Live queues stay fresh under normal workload | ⚠️ partial (под пик иногда деградирует) |
| Historical workers can run without destabilizing live | ❌ blocked on P1.2 |
| Hydrate/historical-hydrate logs no recurring timeout signatures | ⚠️ ongoing (TimeoutError ~10k/8h, ВН classified — это chronic-timeout events) |

### Revised gates (replace "no new TimeoutError" with rate thresholds)

> «No new TimeoutError» как 24/7 gate слишком жёсткий. Если часть событий реально throttled upstream, правильнее SLO: timeout rate ниже X%, no timeout storm on live tier_1, chronic events demoted. (фидбек 2026-05-14)

Новые gates:

| Revised gate | Threshold |
|---|---|
| `refresh_live_event` retry rate (per 15 min) | < 10% |
| `refresh_live_event` retry rate **на tier_1** specifically | < 5% (top-matches must be reliable) |
| `chronic_timeout_events` (5+ consecutive TimeoutError) | ≤ 5% of active live events |
| `endpoint_capability_rollup` deadlock per hour | ≤ 2/hr (firebreak в норме = 0) |
| `live-discovery` retries per hour | ≤ 5/hr |
| Live-hot oldest stale score age | < 15 минут |
| `failed` job count trend | flat (median < 5/15-min window) |
| `stream:etl:hydrate` lag | sustained < `HYDRATE_MAX_LAG` × 0.5 (default 800 / 0.5 = 400) |
| Sustained 24h без manual intervention | required for sign-off |

### Mandatory Proof Window

Соak test: 24 часа без manual интервенций.

---

## Часть 6. Самые рискованные участки кода (revised)

По убыванию risk (после verify):

1. **`workers/live_worker.py:track_event`** + **`discovery_worker._normalize_surface_result`** — single instance, big persist transaction (B.1). High risk, no quick fix.
2. **`pipeline/pilot_orchestrator.py:_flush_capabilities`** — менялось 2 раза за неделю (firebreak). Хорошо покрыто тестами.
3. **`live_dispatch_policy.lease_ms_for_dispatch_tier`** — env-driven, staged rollout sensitivity.
4. **`hydrate_worker.handle`** — entry в `PilotOrchestrator.run_event`. Many DB writes.
5. **`local_api_server._fetch_event_root_payload`** — waterfall (terminal_state → snapshot → synthesizer). Stale data risks.
6. **`event_endpoint_negative_cache_state` lease logic** — race condition surface (inferred ok, monitor).

`country` upsert path — **REMOVED** из risk list (verified fixed in plan 04-22 с `IS DISTINCT FROM` guards).

---

## Часть 7. Что обновлять при следующем PR

(см. также OPERATIONS_RUNBOOK.md и другие docs)

| PR изменяет | Обновить |
|---|---|
| Новый policy gate | `docs/PARSING_AND_POLICIES.md`, tests |
| Новый CLI subcommand | `docs/CLI_AND_SCRIPTS.md`, `docs/FUNCTION_INDEX.md` |
| Новый env var | `docs/ENVIRONMENT.md` (с restart-need column) |
| Новая таблица / migration | `docs/DATABASE_AND_STORAGE.md` |
| Изменение hot-row patterns | `docs/ARCHITECTURE_AUDIT.md` Категория A |
| Новый stream / Redis key | `docs/REDIS_AND_QUEUES.md` |
| Изменение worker behavior | `docs/SERVICES_AND_WORKERS.md` |
| Архитектурное изменение | `docs/PROJECT_OVERVIEW.md` Mermaid |
| Новый verified status в этом docs | `docs/ARCHITECTURE_AUDIT.md` Часть 2 (env audit) |

---

## TL;DR (revised, sustained 9h re-check 2026-05-14 11:30)

**Что ломалось** (главные incidents):
1. ✅ FIXED + sustained 9h: `endpoint_capability_rollup` deadlock storm (firebreak 2026-05-13). 0 ECR waiters, 0 deadlocks.
2. ✅ FIXED + sustained 9h: tier_1 dispatch claim_failed_blocked 93% → 81-91% (waterfall). Не stressed под утром — нужен evening test.
3. ✅ FIXED: matchcenter dying для football live (X4 patch)
4. ✅ FIXED: terminal/self-requeue (план 04-22)
5. ✅ FIXED: topology write amplification country/sport/category (план 04-22)
6. ⚠️ ONGOING (низкая нагрузка): live-discovery persist large transaction (B.1). 0 retries — но не проверено под evening peak.
7. ⚠️ ONGOING + DETERIORATING: stale entries в `zset:live:hot` (C.2). Oldest age 76 → 22.8 → 40 мин за 24h.
8. ⚠️ ONGOING: historical lanes давят на operational (категория F). historical_hydrate lag = 50,480.

**Verified в production env**:
- Housekeeping в production mode ✅
- Event/Season negative caches enforced ✅
- LIVE_DISPATCH_LEASE staged ✅
- Capability rollup firebreak active ✅

**Next critical P0 work** (по revised priorities):
- **P0.A** Decompose live-discovery persist transaction (главное)
- **P0.B** Stale live state cleanup с grace window
- **P0.C** Live freshness SLO observability в /ops/health

**P1**:
- P1.1 rollup rebuild cron (ops hygiene, не P0)
- P1.2 separate pool + live-first governor для historical
- P1.3 adaptive polling
- P1.4 dynamic backpressure
- P1.5 /ops/health rebuild
- P1.6 external alerting

**Historical**:
- Зависит от P1.2 (governor) — после этого можно безопасно включить historical lanes.

**24/7 gate**: replace "no new TimeoutError" → rate threshold (< 10% retry), chronic events demoted, oldest hot zset age < 15 мин, 24h sustained soak.

---

## Документ обновляется при

- каждом крупном hardening commit
- каждой проверке UNKNOWN → verified
- изменении priorities после soak test results
