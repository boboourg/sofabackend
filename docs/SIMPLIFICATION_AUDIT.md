# Simplification Audit (deep)

**Date**: 2026-05-19
**Mode**: READ-ONLY. Никаких изменений в коде, ничего не удалено, ничего не закоммичено.
**Model**: 4 параллельных Explore-агента на **Claude Opus** — анализ глубже чем поверхностный pass.
**Scope**: 296 .py в `schema_inspector/`, 268 в `tests/`, 16 в `tools/`, 7 в `scripts/`, 21 `load_*.py` shims в корне репо, 34 systemd unit в `ops/systemd/` — итого **~650 файлов проверено**.

---

## TL;DR

### 5 главных проблем структуры (с конкретными цифрами)

1. **6 god-objects на 18 690 строк** (всё ядро прод-системы):
   - `local_api_server.py` 5447 (актуально, ~80 методов класса + ~60 free helpers)
   - `local_swagger_builder.py` 3279
   - `cli.py` 3064 (46 команд + 851-строчный HybridApp + 224-строчный `_MemoryRedisBackend`)
   - `event_detail_parser.py` 2337
   - `storage/normalize_repository.py` 2242
   - `pipeline/pilot_orchestrator.py` 2187 (`run_event()` = **15 stage-блоков**, ~720 строк)
2. **1640 LOC чистого boilerplate** дублируются:
   - `_progress` + `_configure_logging` идентичны в **9 CLI** = 90 LOC
   - Runtime/DB argparse args (`--proxy`, `--user-agent`, `--database-url`, …) в **22 CLI** = ~1540 LOC
   - 5 команд `worker-live-{hot,tier-1,tier-2,tier-3,warm}` = одна с `--lane` параметром, размазана как 5
3. **service_app = god-factory из 55 методов + 23 instance attributes**, при этом `test_service_app.py` содержит **только 3 теста**. `build_resource_planner_daemon` = 157 строк ручного instantiation 20 resolvers (0 тестов покрывают). Catastrophically under-tested vs surface area.
4. **2 single points of failure для всей системы**:
   - `pilot_orchestrator.py` — единственный путь hydration для каждого события (17 workers зависят).
   - `service_app.py` — конструирует все daemons/workers (28 construction sites в cli.py).
   ImportError в одном из них → весь fleet падает одновременно.
5. **Корень репо забит мусором**: 21 `load_*.py` shims (один из которых — `load_event_detail.py` — указывает на несуществующий `event_detail_cli.py`, **broken**), 4 личных скрипта с hardcoded `C:\Users\bobur\Desktop\` путями (analyze_entities/check/check_issues/tracer), 2 устаревших `tools/audit_volatile_blocks` (v1/v2 superseded by v3), notes (`pa_pattenr.txt` — typo!, `unique_api_map.txt`, `sofascore_api.md`).

### 5 безопасных quick wins (нулевой production risk)

| # | Действие | LOC | Время | Тест |
|---|----------|-----|-------|------|
| 1 | Удалить 4 личных скрипта (`analyze_entities.py`, `check.py`, `check_issues.py`, `tracer.py` — все с hardcoded Desktop путями) | -236 | 5 мин | grep -r |
| 2 | Архивировать `tools/audit_volatile_blocks.py` + `_v2.py` (superseded by `_v3.py`) → `tools/archived/` | -209 | 5 мин | none |
| 3 | Удалить `workers/normalize_worker.py` (7-line shim, после grep-replace 33 импорта на `normalizers.worker.NormalizeWorker`) | -7 | 15 мин | test_normalize_* |
| 4 | Извлечь константы из `pilot_orchestrator.py` (lines 62-112) → `pipeline/orchestrator_constants.py`. `MISSING_ROOT_TERMINAL_STATUS`, `_ROOT_FETCH_RETRYABLE_CLASSIFICATIONS`, `MISSING_ROOT_RETIRE_THRESHOLD`, `MISSING_ROOT_RETIRE_LOOKBACK`, freshness TTL frozensets. Чистая extraction. | 0 | 15 мин | smoke pytest |
| 5 | Извлечь `DelayedEnvelopeStore` (`service_app.py` lines 240-285) → `queue/delayed_envelope_store.py`. Self-contained, уже импортируется тестом напрямую. Co-located с `queue/delayed.py`. | 0 | 15 мин | test_service_app |

**Total saving**: ~452 LOC из репозитория без поведенческих изменений за ~1 час.

### 5 файлов, которые НЕЛЬЗЯ трогать без отдельного плана

| Файл | Почему страшно | Что защитит |
|------|----------------|-------------|
| `local_api_server.py::_fetch_snapshot_payload + _reconcile_snapshot_payload` (lines 882-1309) | Hottest production path. `_reconcile_snapshot_payload` overlay `is_editor`-filter и terminal-status promotion на каждом `events`/`featuredEvents` envelope. Connection reuse fragile (line 1009: один connection lease на оба метода). | `LocalApiSnapshotReconciliationTests` (2096-2322) + добавить "connection-acquired-once" assertion |
| `local_api_server.py::_fetch_event_root_payload` (lines 3677-3797) | 4-уровневый waterfall с implicit terminal-status sequencing. Если split инвертирует branch order — событие с terminal state + fresh snapshot получит wrong status. | `LocalApiEntityRootFallbackTests` (2323-2714) |
| `cli.py::HybridApp` + dataclasses (lines 109-292, ~700 LOC) | Огромная orchestration state machine для prefetch-режима. Импортируется напрямую `test_hybrid_cli.py`, `test_hybrid_cli_integration.py`, `test_hybrid_cli_freshness_replay.py`. Нельзя ломать публичный API. | Эти 3 теста + сохранить re-export из `cli/__init__.py` |
| `pilot_orchestrator.py::run_event` (lines 308-1034) | Единственная точка hydration для всех 17 workers. 728 строк, 4 env-flag-driven exit branches (`LIVE_SPLIT_DETAILS_FANOUT`, `LIVE_TIER_1_ROOT_ONLY`, `LIVE_TIER_1_QUARANTINE_ENABLED`, `SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED`). ImportError → всё встаёт. | test_pilot_live_paths.py (658 LOC) + test_pilot_orchestrator_parallel_fanout.py |
| `service_app.py::__post_init__` (lines 292-348) | 58 строк инициализируют **15 Redis-coupled stores** unconditionally. Один отсутствующий метод на fake redis → весь fleet не стартует. Нет lazy init, нет graceful degradation. | Нужен миграционный путь к `ServiceDependencies.from_app(app)` с DI |

### Первый безопасный шаг

**Phase 0** (docs + ownership map) — нулевой риск, можно делать прямо сейчас:
- этот документ
- `docs/MODULE_OWNERSHIP.md` (новый)
- `README.md` в `tools/`, `scripts/`, `schema_inspector/services/`, `schema_inspector/parsers/sports/`

---

## Project Map

| Директория | Назначение | Файлов | Размер | Production? |
|---|---|---:|---:|:---:|
| `schema_inspector/` (корень) | 80 .py: CLI shims, parsers, repos, policies, ws-*, transport, fetch, runtime | 80 | большой | YES |
| `schema_inspector/services/` | 30 .py: planner-daemons, worker-runtime, retry-policy, structure-sync, monitoring, **service_app** | 30 | средний | YES |
| `schema_inspector/workers/` | 11 .py: discovery/hydrate/live/normalize/structure/maintenance workers + 1 shim | 11 | средний | YES |
| `schema_inspector/storage/` | 17 .py: asyncpg repositories (normalize 2242, raw, capability, tournament_registry, …) | 17 | большой | YES |
| `schema_inspector/parsers/` | 30+ .py: registry + families + special + sports/* | 30+ | средний | YES |
| `schema_inspector/normalizers/` | 2 .py: sink (write loop) + worker (parser-runner) | 2 | маленький | YES |
| `schema_inspector/queue/` | 20 .py: Redis primitives (streams, dedupe, leases, freshness, throttles, breakers, caches) | 20 | средний | YES |
| `schema_inspector/ops/` | 6 .py: health, queue_summary, db_audit, recovery, metrics, stale_live_events | 6 | средний | YES |
| `schema_inspector/pipeline/` | 2 .py: pilot_orchestrator (2187) + pilot_cli (24) | 2 | один большой | YES |
| `schema_inspector/sources/` | 3 .py: HTTP adapters (sofascore, secondary_stub, base) | 3 | маленький | YES |
| `schema_inspector/jobs/` | 2 .py: envelope.py + types.py (job message contract) | 2 | маленький | YES |
| `schema_inspector/planner/` | 3 .py: live, planner, rules | 3 | маленький | YES |
| `schema_inspector/monitoring/` | 6 .py: daemon + alerter + signal sources | 6 | маленький | YES |
| `schema_inspector/cache_warmer/` | 3 .py: config, daemon, url_targets | 3 | маленький | YES |
| `scripts/` | 5 prod-adjacent + 2 в `ops/`: `prod_readiness_check`, `audit_api_latency`, `fetch_samples`, `rebuild_cache`, `mobile_proxy_*` | 7 | средний | YES (ops) |
| `tools/` | 15 dev/diag: `smoke_*` × 7, `audit_*` × 3 (v1 superseded), `analyze_socket_archive`, `replay_socket_archive`, `sniff_ws_odds`, `smoke_time` | 16 | средний | NO (dev) |
| `tools/archived/` | 1 .py (sportradar_openapi_audit) | 1 | — | NO (archive) |
| `docs/` | 30+ md: руководства, плэны, runbooks, archived | 30+ | средний | docs |
| `migrations/` | Дата-именованные `.sql`, append-only | many | средний | YES (manual apply) |
| `tests/` | 268 .py — pytest suite mirror-ит modules | 268 | большой | docs |
| `local_swagger/` | Static OpenAPI artifacts | — | — | dev |
| `deploy/` | 1 file: `run_service.sh` (exec python -m schema_inspector.cli "$@") | 1 | маленький | YES |
| `ops/systemd/` | 34 `.service` unit (30 идут через cli.py, 4 standalone) | 34 | средний | YES (prod runtime) |
| `config/` | env templates | — | — | YES |
| **корень репо** | 21× `load_*.py` shim + 5 диагностик + 4 entry shims (`serve_*`, `build_*`, `setup_*`, `inspect_api`) + `.md`/`.txt`/`.tsv`/`.sql` audit artifacts | ~50 + artifacts | — | mixed |

---

## Biggest Files (top 30 by lines)

| # | File | Lines | Что делает | Почему стал большим | Оставить? | Куда дробить | Risk |
|--:|---|---:|---|---|:---:|---|:---:|
| 1 | `schema_inspector/local_api_server.py` | **5447** (≠ заявленных 5941; wc -l даёт 5447) | FastAPI app + 107 routes + ops + cache + dual-loop daemon thread + 5 inline synth | Маршруты + dispatcher + waterfall + кеш + DB pool + 60 free helpers в одном файле | NO | `api/routes/{event,team,player,tournament,sport,misc}.py` + `api/ops/routes.py` + `api/cache/layer.py` + `api/route_registry.py` + `api/snapshot_pipeline.py` + `api/payload_builders.py` | **high** |
| 2 | `schema_inspector/local_swagger_builder.py` | 3279 | Сборка OpenAPI 3 schema | Inline schema decls для каждого route | maybe | разнести по доменам параллельно с api/routes/ | medium |
| 3 | `schema_inspector/cli.py` | **3064** | 46 commands argparse + dispatcher + 851-LOC HybridApp + 224-LOC `_MemoryRedisBackend` | Монолитный `_build_parser` (553 LOC) + 688-LOC if/elif dispatcher + dev-only memory Redis вкорячен в прод-CLI | NO | `cli/commands/{workers,planners,diagnostic,admin,daemons,hydrate_oneshot}.py` + `cli/shared/{env,logging,redis,db,runtime,proxy_scoping,printers,common_args}.py` + `cli/hybrid_app.py` | medium |
| 4 | `schema_inspector/event_detail_parser.py` | 2337 | Парсер `/event/{id}` + sub-маршрутов | Каждый sub-endpoint имеет parser-кусок | NO | `parsers/event/{root,stats,lineups,incidents,h2h,etc}.py` | **high** |
| 5 | `schema_inspector/storage/normalize_repository.py` | 2242 | Write all normalized tables | Все upserts/INSERTs для DB в одном файле | NO | `storage/normalize/{event,team,player,tournament,statistics,lineup}.py` | **high** |
| 6 | `schema_inspector/pipeline/pilot_orchestrator.py` | 2187 | Football pipeline orchestrator | `run_event()` = 15 stage-блоков ≈ 720 LOC; baseball + football sport-specific inline | NO | `pipeline/{orchestrator,orchestrator_stages,recorders,missing_root_policy,_endpoint_lookups,orchestrator_constants}.py` + `parsers/sports/<sport>/orchestration.py` | **high** |
| 7 | `schema_inspector/services/service_app.py` | 1870 | God-factory: 55 методов, 23 attributes | ensure_* × 5 + build_*_planner × 7 + build_*_worker × 15 + run_*_worker × 17 + __post_init__ (58 LOC) | NO | `services/dependencies.py` + `services/factories/{planners,workers,resolver_registry}.py` + thin service_app | medium |
| 8 | `schema_inspector/endpoints.py` | 1698 | Каталог 105+ endpoint records | Каждый endpoint ≈ 10 LOC декларации | YES | — (single source of truth) | low |
| 9 | `schema_inspector/scheduled_events_synthesizer.py` | 1471 | Synthesizer envelope-ов из normalized | После Item 2: team builders + h2h + calendar + groups + round_slug + standings_away | YES | Optional: split team_* в отдельный модуль | low |
| 10 | `schema_inspector/event_detail_repository.py` | 1408 | Read-side queries для event detail | По sub-endpoint | maybe | параллельно с event_detail_parser split | medium |
| 11 | `schema_inspector/entities_parser.py` | 1294 | Parser team/player ingestion | мульти-sport | YES | low priority | low |
| 12 | `schema_inspector/default_tournaments_pipeline_cli.py` | **1283** | Pipeline runner + `_run_tournament_worker` (lines 346-868, **522 LOC**) — single most complex function in project + 4 SQL season-pool loaders уникальные для этого CLI | Orchestrator + CLI args + slug-routing + SQL helpers всё в одном | NO | `services/default_tournaments_orchestrator.py` (522 LOC orchestrator) + `storage/season_pools_repository.py` (~180 LOC SQL helpers) + thin CLI (~300 LOC) | medium |
| 13 | `schema_inspector/event_list_repository.py` | 1131 | Read-side queries для event lists | По surface (live/scheduled/featured) | YES | low priority | low |
| 14 | `schema_inspector/leaderboards_parser.py` | 1101 | Parser leaderboards | мульти-sport | YES | low priority | low |
| 15 | `schema_inspector/ops/health.py` | 1026 | Production health probe | Многоуровневый: streams + DB + Redis + lag | YES | можно но не критично | low |
| 16 | `schema_inspector/event_list_parser.py` | 993 | Parser /events lists | По surface | YES | low priority | low |
| 17 | `schema_inspector/season_widget_negative_cache.py` | 906 | Negative cache для season-widget | inline-tests + builder + repo | YES | move repo part в storage/ | low |
| 18 | `schema_inspector/services/housekeeping.py` | 895 | Housekeeping daemon | Многозадачный: zombie sweep + terminal state finalize | maybe | per task | medium |
| 19 | `schema_inspector/statistics_parser.py` | 865 | Parser statistics | мульти-sport | YES | low priority | low |
| 20 | `schema_inspector/competition_parser.py` | 835 | Parser competition (tournament root) | мульти-sport | YES | low priority | low |
| 21 | `schema_inspector/services/structure_sync_service.py` | 804 | Periodic structure sync | Оркестратор + state | YES | можно вынести state | low |
| 22 | `schema_inspector/runtime.py` | 799 | Runtime config loader | sport profiles + flags | YES | low priority | low |
| 23 | `schema_inspector/transport.py` | 739 | HTTP transport (httpx + retry + proxy + ratelimit + circuit-breaker) | централизованный | YES | low priority | low |
| 24 | `tools/archived/sportradar_openapi_audit.py` | 731 | Архивный аудит | — | **already archived** | — | low |
| 25 | `schema_inspector/storage/tournament_registry_repository.py` | 686 | Read/write tournament_registry | barrier query + cursor | YES | low priority | low |
| 26 | `schema_inspector/current_year_pipeline_cli.py` | 686 | Current-year pipeline | CLI + orchestrator (overlaps с bootstrap, slice, targeted) | maybe | extract `_run_competitions` stage в `pipeline/stages.py` | medium |
| 27 | `schema_inspector/workers/live_worker_service.py` | 662 | Live stream consumer (≠ live_worker.py state machine!) | service + retry + state | YES | разумно как есть | low |
| 28 | `schema_inspector/entities_backfill_job.py` | 605 | Entities batch backfill | scan + ingest | YES | low priority | low |
| 29 | `schema_inspector/event_endpoint_negative_cache.py` | 596 | Per-event endpoint NC | inline-tests + builder | YES | low priority | low |
| 30 | `schema_inspector/competition_repository.py` | 591 | Read queries for competition | DB-side | YES | low priority | low |

### Топ-кандидаты на дробление
- **high-risk, high-value**: #1, #4, #5, #6 — фазы 3-4 в плане. Отдельные PR + canary deploy.
- **medium-risk**: #2, #3, #7, #10, #12, #18, #26 — структурно понятно.
- **low / leave alone**: всё остальное.

---

## Duplicate Logic (deep)

### A. Idiomatic boilerplate в CLI — **1640 LOC чистого дубля**

| Что дублируется | Где | LOC | Извлечь в |
|---|---|---:|---|
| `_progress` (datetime+print) + `_configure_logging` (basicConfig+format) | 9 файлов: `bootstrap_pipeline_cli`, `category_tournaments_pipeline_cli`, `cli`, `current_year_pipeline_cli`, `default_tournaments_pipeline_cli`, `entities_backfill_cli`, `event_detail_backfill_cli`, `slice_pipeline_cli`, `targeted_pipeline_cli` | 90 | `schema_inspector/cli_helpers.py::progress`, `configure_logging` |
| `--proxy/--user-agent/--max-attempts/--database-url/--db-min-size/--db-max-size/--db-timeout/--timeout/--log-level` argparse блоки | 22 CLI файла | ~1540 | `schema_inspector/cli_helpers.py::add_common_runtime_args(parser)` |
| `worker = self.build_X(...); await worker.run_forever()` async wrapper | 17 `run_*_worker` методов в `service_app.py` | ~100 | dict-driven dispatch `WORKER_DISPATCH: dict[str, Callable]` |
| `_fetch_*_events_payload` connection lifecycle (try/connect → try/fetch → close → build_payload) | 7 раз в `local_api_server.py` (lines 1760-3156) | ~80 | `async with self._lease_connection() as conn: ...` context manager |

### B. CLI pairs — намеренный split, но argparse 100% повторяется

| Pair | Назначение split | LOC saved при merge |
|------|------------------|---:|
| `leaderboards_cli.py` ↔ `leaderboards_backfill_cli.py` | single (UT, season) vs batch | 80 |
| `standings_cli.py` ↔ `standings_backfill_cli.py` | same pattern | 50 |
| `statistics_cli.py` ↔ `statistics_backfill_cli.py` | same | 60 |
| `entities_cli.py` ↔ `entities_backfill_cli.py` | same | 90 |
| **Total** | — | **~280** |

Merge через `python -m schema_inspector.cli ingest leaderboards [--ut N --season M] [--batch --season-limit X]`. `*BackfillJob` обёртка остаётся как есть — мерджится только argparse layer.

### C. Pipeline CLIs — частичный dedup потенциал

`bootstrap_pipeline_cli.py` (303), `current_year_pipeline_cli.py` (686), `targeted_pipeline_cli.py` (293), `slice_pipeline_cli.py` (547) — все имеют идентичный shape:
1. WindowsSelectorEventLoopPolicy
2. argparse runtime/DB
3. `_resolve_dates(args)`
4. `_run` opens DB, builds adapter, builds jobs
5. `_run_competitions` async semaphore pattern (~50 LOC near-clone в bootstrap+current_year)
6. `_progress`/`_configure_logging`

**Realistic merge**: extract pipeline stages в `pipeline/stages.py` (stage_categories_discovery, stage_competition_hydration, stage_scheduled_events, stage_event_detail, stage_entities). LOC savings: **~500 LOC** across 4 lighter CLI. `default_tournaments` + `category_tournaments` остаются bespoke (тяжелее).

### D. Inline SQL в local_api_server.py — **40+ call sites**

Только handlers, делегирующие в repo: cache reads/writes, synthesizer rows, health/queue. Всё остальное — **handcrafted SQL inline**:
- `_fetch_event_incidents_payload` (2228): 6-column SELECT
- `_fetch_event_lineups_payload` (2264): two SELECTs, 16 columns
- `_fetch_event_comments_payload` (2344): two SELECTs + LEFT JOIN player
- `_fetch_standings_payload` (2000): 2 SELECTs + JOIN tie_breaking + JOIN promotion
- `_fetch_unique_tournament_media_payload`: 50-line DISTINCT ON SQL inline
- `_reconcile_snapshot_payload`: **два 90-line LATERAL JOIN SQL** inline

### E. Copy-paste root entity passthroughs

`_fetch_team_root_payload` (3799), `_fetch_player_root_payload` (3837), `_fetch_manager_root_payload` (3873), `_fetch_unique_tournament_root_payload` (3909) — **структурно идентичны**: `fetchrow` snapshot → `if decoded` return → `fetchrow normalized table` → `_synthesize_<entity>_root_payload`. ~36 LOC × 4 = **144 LOC ≈ 95% identical**, отличается только table name + endpoint_pattern + synthesizer call.

### F. SQL season-pool helpers — невидимы другим CLI

`_load_season_team_ids` + `_load_season_player_ids` + `_load_season_event_ids` + `_load_season_rounds_catalog` (172 LOC) живут **только в `default_tournaments_pipeline_cli.py`**. `leaderboards_backfill_job`, `statistics_backfill_job`, `slice_pipeline_cli`, `entities_backfill_job` переизобретают похожие lookups независимо.

**Extract**: `storage/season_pools_repository.py`. LOC savings: ~180 (плюс reuse в 3+ pipelines).

### G. Не дубликаты (false positives — НЕ объединять!)

| Что выглядит как дубль | Почему НЕ объединять |
|---|---|
| `hydrate_worker.py` vs `historical_archive_worker.py` | Разные streams (`stream:etl:hydrate` vs `historical_tournament/historical_enrichment`), разные jobs, разные lifecycles |
| `live_worker.py` vs `live_worker_service.py` | **Completely different**: `live_worker.py` = синхронный state machine helper (200 LOC); `live_worker_service.py` = Redis stream consumer (662 LOC) wrapping WorkerRuntime. Confusing names — можно переименовать helper в `live_state_machine.py` |
| `endpoint_negative_cache_repository.py` vs `event_endpoint_negative_cache_repository.py` | Разные scopes (global vs per-event), enforced different schema tables |
| `bootstrap_pipeline_cli` vs `current_year_pipeline_cli` | bootstrap = cold seed; current_year = catch-up. Entry conditions разные. DAG похож — dedup только через stage helpers |
| `parsers/sports/*.py` × 13 | 10-line config dicts, не logic. Каждый = sport-specific tuple `(core_event_edges, live_optional_edges, special_families)` |
| 8 stream workers (discovery, hydrate, live_worker_service, normalize_stream_worker, resource_refresh, historical_archive, maintenance, structure) | Каждый consumer-group отличается job-type семантикой |
| `leaderboards_*` × 4 (repo/job/parser/backfill) | Classic 4-layer architecture — storage/business/parser/batch |
| 21× `load_*.py` shims | systemd-invoked entry points (через `deploy/run_service.sh`). Удаление = переписать 34 systemd unit |

---

## Dead / Legacy Candidates

| Файл / папка | Evidence (за удаление) | Evidence (против) | Verdict |
|---|---|---|---|
| `analyze_entities.py` (корень, 145 LOC) | Hardcoded `C:\Users\bobur\Desktop\sofascore\reports`. Personal one-off. | — | **DELETE** |
| `check.py` (13 LOC) | Hardcoded user path. | — | **DELETE** |
| `check_issues.py` (48 LOC) | Hardcoded user path. | — | **DELETE** |
| `tracer.py` (30 LOC) | Reads `pa_pattenr.txt` (typo). No internal imports. | — | **DELETE or → tools/archived/** |
| `pa_pattenr.txt`, `unique_api_map.txt`, `api_docs.md`, `sofascore_api.md` | Notes, no .md/.py references | — | **DELETE or → docs/archived/** |
| `event_endpoint_matrix_7d.tsv`, `event_matrix_7d_*.sql`, `audit_*.{csv,md,txt}` | Untracked audit artifacts, засоряют `git status` | используются interactively | **→ `audit/` + gitignored** |
| `tools/audit_volatile_blocks.py` (v1, 115 LOC) | Superseded by v3 (env-driven, no tempfile). Only ref in `.claude/settings.local.json` | — | **→ tools/archived/** |
| `tools/audit_volatile_blocks_v2.py` (94 LOC) | Superseded by v3. | — | **→ tools/archived/** |
| `workers/normalize_worker.py` (7 LOC) | Just `from .normalizers.worker import NormalizeWorker` shim | 33 grep hits импортируют именно отсюда | **DELETE after grep-and-replace** |
| `default_tournaments_pipeline_cli.py::_load_round_numbers` (lines 1081-1101) | Docstring говорит "Replaces ``_load_round_numbers``...". Replacement = `_load_season_rounds_catalog` at line 1025. Нет in-file callers. | grep полный по проекту | **likely dead, needs manual verify** |
| `load_event_detail.py` (корень) | References `schema_inspector.event_detail_cli:main`, но файл `event_detail_cli.py` **не существует** (только `event_detail_backfill_cli.py`) | broken import | **FIX (point to backfill_cli) or DELETE** |
| `probe_multisport_reports.py` (731 LOC), `probe_next_data_feature_families.py` (290 LOC) | Research scripts, references `reports/` directories | imports from schema_inspector real | **→ tools/probes/** |
| `sports_data.db` (корень) | Local SQLite dump | unclear callers | **needs manual check** |
| `setup_postgres.py` (5 LOC shim) | → schema_inspector.db_setup_cli.main | active CLI | **keep, но рассмотреть → scripts/** |
| `serve_local_api.py`, `serve_local_swagger.py`, `build_local_swagger.py` | Active entrypoints | используются | **keep, рассмотреть → tools/dev/** |
| `inspect_api.py`, `test_run.py` | ad-hoc debug tools | используются вручную | **keep, рассмотреть → tools/** |
| `tools/sniff_ws_odds.py`, `analyze_socket_archive.py`, `replay_socket_archive.py` | WS-debug tools | used during live development | **keep** |
| `tools/smoke_*.py` × 7 | Manual smoke tests | used for проверки | **keep** |
| `tools/archived/sportradar_openapi_audit.py` (731 LOC) | Already archived | — | **keep (it's where it should be)** |

**ВАЖНО**: Ни один файл не удаляется в этом audit. Это рекомендации для Phase 1+. Каждое удаление — отдельный PR с pre-check `grep -r <file>` по всему репо + deploy/ + ops/.

---

## Production Core vs Support Layer

### A. Production critical (НЕЛЬЗЯ менять без full test + smoke на prod)

**API & Read path**:
- `schema_inspector/local_api_server.py` (5447) — единая точка входа API
- `schema_inspector/scheduled_events_synthesizer.py` (1471)
- `schema_inspector/event_payload_cache_repository.py`
- `schema_inspector/endpoints.py` (1698) — каталог-источник правды
- `schema_inspector/api_cache.py`

**Live ingestion**:
- `schema_inspector/workers/live_worker.py` (200, state machine helper)
- `schema_inspector/workers/live_worker_service.py` (662, stream consumer)
- `schema_inspector/workers/live_details_worker_service.py`
- `schema_inspector/services/live_discovery_planner.py`, `live_rescue.py`, `live_state_sweeper.py`
- `schema_inspector/planner/{live,planner,rules}.py`
- `schema_inspector/queue/live_*.py` (10 файлов)

**Hydrate / Discovery / Historical**:
- `schema_inspector/workers/{discovery,hydrate,historical_archive,resource_refresh,structure,maintenance}_worker.py`
- `schema_inspector/pipeline/pilot_orchestrator.py` (2187) — единственная ingestion path
- `schema_inspector/services/worker_runtime.py`, `service_app.py`, `planner_daemon.py`
- `schema_inspector/services/historical_planner.py`, `historical_tournament_planner.py`, `historical_archive_service.py`

**Redis primitives** (20 файлов): streams, delayed, leases, dedupe, freshness, live_state, proxy_state, resource_cursor, event_circuit_breaker, etc.

**DB repositories**:
- `schema_inspector/storage/normalize_repository.py` (2242)
- `schema_inspector/storage/{raw,job,capability,tournament_registry,coverage,…}_repository.py` (17 files)
- `schema_inspector/normalizers/{sink,worker}.py`

**Parsers**:
- `schema_inspector/parsers/{registry,base,classifier}.py` + `families/` + `special/` + `sports/`
- `schema_inspector/{event_detail,event_list,competition,entities,leaderboards,statistics,standings}_parser.py`

**Monitoring/Health**:
- `schema_inspector/ops/{health,queue_summary,db_audit,recovery,metrics,stale_live_events}.py`
- `schema_inspector/monitoring/` (6 файлов)
- `schema_inspector/services/proxy_health_monitor.py`

**Scheduler/planners** (systemd-driven):
- `schema_inspector/services/{structure_planner,resource_planner,tournament_registry_refresh}.py`
- `schema_inspector/cache_warmer/` (3 файла)

**Transport**:
- `schema_inspector/transport.py`, `sources/sofascore_adapter.py`, `fetch.py`, `fetch_executor.py`, `fetch_classifier.py`, `fetch_models.py`

**CLI entry-point**:
- `schema_inspector/cli.py` (3064) + `__init__.py`

**Deploy**:
- `deploy/run_service.sh` + `ops/systemd/*.service` (34 unit)

### B. Operational support (можно менять с тестами + smoke)

- **Maintenance/admin** через cli.py: backfill-cursor, backfill-priorities, backfill-cursor-bootstrap, rebuild-capability-rollup, coverage-refresh, recover-live-state, audit-db (15 команд без systemd)
- **Scripts (ops)**: `prod_readiness_check`, `audit_api_latency`, `audit_event_detail_matrix`, `fetch_tournament_season_samples`, `rebuild_endpoint_negative_cache_state`, `scripts/ops/{proxy_health_check.py,mobile_proxy_watchdog.sh}`
- **Pipeline CLI** (one-shot): `bootstrap`, `categories_seed`, `category_tournaments`, `current_year`, `default_tournaments`, `full_backfill`, `slice`, `targeted`, `scheduled_tournaments`, `team_detail`
- **Per-domain CLI**: `competition_cli`, `event_detail_backfill_cli`, `event_list_cli`, `entities_cli`, `entities_backfill_cli`, `leaderboards_cli` + `_backfill`, `standings_cli` + `_backfill`, `statistics_cli` + `_backfill`

### C. Development / test / docs

- `tests/` (268 .py)
- `local_swagger/` (static OpenAPI)
- `schema_inspector/local_swagger_builder.py` (3279) — generates OpenAPI
- `tools/smoke_*.py`, `audit_*.py`, `analyze_socket_archive`, `replay_socket_archive`, `sniff_ws_odds`
- `docs/` (30+ md)
- `migrations/`

### D. Legacy / questionable

- 21× `load_*.py` (один сломан — `load_event_detail.py`)
- `analyze_entities.py`, `check.py`, `check_issues.py`, `tracer.py`
- `pa_pattenr.txt`, `unique_api_map.txt`, `api_docs.md`, `sofascore_api.md`
- `tools/audit_volatile_blocks.py` + `_v2.py`
- `workers/normalize_worker.py` (shim)
- `default_tournaments_pipeline_cli.py::_load_round_numbers` (dead?)
- `sports_data.db`

---

## Proposed Target Structure

```
schema_inspector/
├── core/                          # runtime primitives (no business logic)
│   ├── config.py                  # ← runtime.py (part)
│   ├── db.py                      # already in place
│   ├── transport.py               # already in place
│   ├── sofascore_client.py        # already in place
│   └── timezone_utils.py          # already in place
│
├── queue/                         # OK as-is, +1 file
│   ├── ... (existing 20 files)
│   └── delayed_envelope_store.py  # ← extracted from service_app.py L240-285
│
├── sources/                       # OK as-is
│   └── sofascore_adapter.py
│
├── ingestion/                     # NEW: вместо рассеянных workers/services/
│   ├── discovery/                 # discovery_worker
│   ├── hydrate/                   # hydrate_worker
│   ├── live/                      # live_worker (state machine) + live_worker_service
│   ├── historical/                # historical_archive_worker + historical_*planner
│   ├── resource_refresh/          # resource_refresh_worker + resource_planner
│   └── structure_sync/            # structure_worker + structure_*
│
├── parsing/                       # OK as-is (parsers/ already structured)
│   ├── registry.py
│   ├── base.py
│   ├── classifier.py
│   ├── families/
│   ├── sports/
│   │   └── <sport>/
│   │       └── orchestration.py  # NEW: sport-specific fanout hooks (baseball pitch, football shotmap)
│   └── special/
│
├── persistence/                   # NEW: storage + normalizers + raw + season_pools
│   ├── repositories/              # ← storage/ (split normalize_repository → normalize/{event,team,…})
│   ├── normalizers/               # ← normalizers/
│   ├── snapshots/                 # api_payload_snapshot helpers (currently inline)
│   └── season_pools_repository.py # ← extracted from default_tournaments_pipeline_cli
│
├── api/                           # NEW: разрезать local_api_server.py
│   ├── server.py                  # core dispatcher + lifecycle (~300 LOC)
│   ├── route_registry.py          # ← L4796-4870 (RouteSpec, match_route, _infer_context_*)
│   ├── routes/
│   │   ├── event.py               # 37 event routes + sub-resources
│   │   ├── tournament.py          # 30 tournament routes
│   │   ├── team.py                # 12 team routes
│   │   ├── sport.py               # 11 sport routes
│   │   ├── player.py              # 11 player routes
│   │   └── misc.py                # manager/category/calendar/config
│   ├── ops/
│   │   └── routes.py              # 11 /ops/* endpoints (L533-749, L3948-4533)
│   ├── cache/
│   │   └── layer.py               # _PAYLOAD_CACHE_PATTERNS + cache get/put + pool lease
│   ├── snapshot_pipeline.py       # ← _fetch_snapshot_payload + _reconcile_snapshot_payload + overlays
│   ├── entity_root_dispatcher.py  # ← _fetch_entity_root_fast_path + 5x _fetch_<entity>_root_payload
│   ├── synthesizers/              # ← scheduled_events_synthesizer.py + inline _synthesize_*_root
│   └── payload_builders.py        # ← free helpers tail (L5087-5705): pure builders
│
├── ops/                           # OK as-is
│   └── ... (6 existing files)
│
├── orchestration/                 # NEW: вместо pipeline/ + services/factories
│   ├── pilot/
│   │   ├── orchestrator.py        # core run_event shell (~600 LOC)
│   │   ├── orchestrator_stages.py # ← Stages 6, 11, 14 как pure functions (RunContext)
│   │   ├── recorders.py           # ← CapabilityRecorder + LiveStateRecorder
│   │   ├── missing_root_policy.py # ← _should_retire_missing_root_event (36 LOC)
│   │   ├── _endpoint_lookups.py   # ← L1892-1946 free helpers
│   │   └── orchestrator_constants.py
│   ├── default_tournaments_orchestrator.py # ← _run_tournament_worker (522 LOC из CLI)
│   └── stages.py                  # ← pipeline stage helpers (extract из bootstrap/current_year/…)
│
├── services/                      # ОСТАВИТЬ, но разредить
│   ├── dependencies.py            # NEW: ServiceDependencies dataclass + from_app(app)
│   ├── service_app.py             # SLIM: только 17 run_* async wrappers (~300 LOC вместо 1870)
│   ├── factories/
│   │   ├── planner_factories.py   # ← 7 build_*_planner_daemon методов
│   │   ├── worker_factories.py    # ← 12 build_*_worker методов
│   │   └── resolver_registry.py   # ← declarative RESOLVER_SPECS вместо 157 LOC ручного instantiation
│   ├── worker_runtime.py          # OK
│   ├── retry_policy.py            # OK
│   └── ... (rest unchanged)
│
├── planning/                      # OK (renamed from planner/)
│
├── monitoring/                    # OK as-is
│
├── cache_warmer/                  # OK as-is
│
├── cli/                           # NEW: разрезать cli.py
│   ├── __main__.py                # entry point
│   ├── main.py                    # main() + _dispatch + parser composition root (~200 LOC)
│   ├── hybrid_app.py              # ← HybridApp + dataclasses (~1050 LOC)
│   ├── shared/
│   │   ├── env.py                 # ← _load_project_env + _normalized_source_slug
│   │   ├── logging.py             # ← _configure_logging
│   │   ├── redis.py               # ← _load_redis_backend + _close_redis_backend + _MemoryRedisBackend (dev-only stub)
│   │   ├── db.py                  # ← open_database(args) ctxmgr
│   │   ├── runtime.py             # ← resolve_runtime_config(args, command)
│   │   ├── proxy_scoping.py       # ← 3 _SCOPED_*_KEY_BY_COMMAND dicts
│   │   ├── printers.py            # ← _print_batch_report + _print_db_audit_report
│   │   └── common_args.py         # ← parent parsers: worker_args, planner_args, audit_args, global_args
│   └── commands/
│       ├── __init__.py            # COMMANDS dict-based registry
│       ├── hydrate_oneshot.py     # event, live, scheduled, full-backfill (~250 LOC)
│       ├── diagnostics.py         # health, audit-db, replay, recover-live-state, stale-live-events (~180)
│       ├── admin.py               # coverage-refresh, rebuild-capability-rollup, backfill-* (~270)
│       ├── workers.py             # 17 worker-* через единую табличную диспетчеризацию (~150)
│       ├── planners.py            # 6 planner-daemons + tournament-registry-refresh (~180)
│       └── daemons.py             # proxy-health-monitor, monitoring-daemon, api-cache-warmer, live-rescue, ws-* (~370)
│
├── policies/                      # NEW: собрать разрозненные policy
│   ├── coverage_policy.py
│   ├── detail_resource_policy.py
│   ├── endpoint_ttl_policy.py
│   ├── live_delta_policy.py
│   ├── live_dispatch_policy.py
│   ├── match_center_policy.py
│   └── source_priority.py
│
└── cli_helpers.py                 # NEW: extracted _progress, configure_logging, add_common_runtime_args (-1640 LOC из 22 CLI)
```

### Где текущая структура УЖЕ хорошая (НЕ резать)

- `parsers/` — registry pattern с `families/sports/special` удобный
- `queue/` — 20 files по primitive, разделение чёткое
- `services/` — domains разделены, кроме `service_app.py`
- `ops/` — минимальная и чёткая
- `workers/` — `<role>_worker.py` per systemd-role = feature
- `endpoints.py` (1698) — каталог-источник правды, не разносить по доменам

---

## File-by-file Recommendations (top 30 + dead candidates)

| File | Current role | Issue | Recommendation | Risk | Tests |
|---|---|---|---|:---:|---|
| `local_api_server.py` | FastAPI app + 107 routes + ops + cache + dual-loop bridge | God-object 5447 LOC | Split into 7+ modules (api/server.py + api/routes/* + api/ops/routes.py + api/cache/layer.py + api/snapshot_pipeline.py + api/entity_root_dispatcher.py + api/payload_builders.py). Snapshot pipeline = HIGH risk seam, остальные lower. | **high** | Full local_api suite (105 tests) + new pool-leasing assertion в LocalApiSnapshotReconciliationTests + 1 contract test per ops endpoint |
| `cli.py` | Single CLI entry | God-object 3064 LOC, 46 commands, HybridApp inline | Split into cli/commands/* + cli/shared/* + cli/hybrid_app.py. Migration risk = ZERO (sustain `python -m schema_inspector.cli X`). | medium | `test_systemd_assets.py` + snapshot test `_build_parser().format_help()` для каждой команды + `for cmd in COMMANDS: parse_args([cmd, "--help"])` smoke |
| `pipeline/pilot_orchestrator.py` | Core hydration runner | 2187 LOC, run_event() = 15 stages, 4 exit branches | Extract orchestrator_stages.py + recorders.py (CapabilityRecorder + LiveStateRecorder) + missing_root_policy.py + _endpoint_lookups.py + orchestrator_constants.py. Sport-specific hooks → parsers/sports/<sport>/orchestration.py. | **high** | test_pilot_*.py (7 файлов, ~2000 LOC). Pre-split add: stage-level tests для Stage 11 (entity profile fanout) и Stage 14 (detail fanout). |
| `services/service_app.py` | Mega-factory | 1870 LOC, 55 methods, 23 attributes, 3 tests на весь файл | Split: services/dependencies.py (ServiceDependencies.from_app) + factories/{planner,worker}_factories.py + factories/resolver_registry.py (declarative RESOLVER_SPECS). Slim service_app to ~300 LOC of run_* wrappers. | medium | Pre-split add: snapshot test build_resource_planner_daemon → assert dict of resolver kinds matches expected 20; per build_X_planner_daemon assert daemon shape |
| `event_detail_parser.py` | Parse /event/* | 2337 LOC | Split per sub-endpoint: parsers/event/{root,stats,lineups,incidents,h2h,managers,heatmap,etc}.py | **high** | test_event_detail_parser.py |
| `storage/normalize_repository.py` | Write all normalized tables | 2242 LOC | Split per domain: storage/normalize/{event,team,player,tournament,statistics,lineup}.py + thin normalize_repository.py | **high** | test_storage_normalize_repository.py + integration |
| `local_swagger_builder.py` | OpenAPI builder | 3279 LOC | Split parallel to api/routes/ | medium | test_local_swagger_builder.py |
| `endpoints.py` | Endpoint catalog | 1698 LOC, flat | Leave as is (caller простой) | low | — |
| `scheduled_events_synthesizer.py` | Synth envelopes | 1471 LOC | Optional split team_*/event_* | low | hybrid synth tests |
| `default_tournaments_pipeline_cli.py` | Default UT pipeline | 1283 LOC, includes 522-LOC `_run_tournament_worker` orchestrator + 4 SQL season-pool helpers unique to this file | Extract `services/default_tournaments_orchestrator.py` (522 LOC) + `storage/season_pools_repository.py` (~180 LOC) → thin CLI ~300 LOC | medium | test_default_tournaments_pipeline.py |
| `event_detail_repository.py` | Read event detail | 1408 LOC | Split parallel to parser | medium | test_event_detail_storage.py |
| `ops/health.py` | Production health | 1026 LOC | Keep — well-focused | low | — |
| `services/housekeeping.py` | Multi-task housekeeping | 895 LOC | Split per task | medium | test_housekeeping.py |
| `season_widget_negative_cache.py` | Negative cache | 906 LOC | Move repo part to storage/ | low | — |
| `runtime.py` | Runtime config | 799 LOC | Keep | low | — |
| `transport.py` | HTTP transport | 739 LOC | Keep | low | — |
| `workers/live_worker_service.py` | Live stream consumer | 662 LOC | Keep | low | — |
| `workers/live_worker.py` (200 LOC) | State machine helper | confusing name | Rename → `live_state_machine.py` for clarity | low | — |
| `workers/normalize_worker.py` (7 LOC) | Compat shim | Pure re-export | Delete after grep-replace 33 imports | low | test_normalize_* |
| 21× `load_*.py` shims (корень) | Pure 3-5 line shims | 100% redundant | Archive `legacy/load_*.py` для 1 sprint, потом delete. **Verify** `load_event_detail.py` (broken — refers to нонexistent `event_detail_cli.py`) | low | test_systemd_assets |
| `analyze_entities.py`, `check.py`, `check_issues.py`, `tracer.py` | Personal one-offs with hardcoded Desktop paths | broken outside Bobur's machine | **DELETE** (или → tools/archived/) | low | — |
| `pa_pattenr.txt`, `unique_api_map.txt`, `api_docs.md`, `sofascore_api.md` | Stale notes | typo, dead | **DELETE** или → `docs/archived/` | low | — |
| `tools/audit_volatile_blocks.py` + `_v2.py` | Superseded by v3 | env-driven, no tempfile в v3 | → `tools/archived/` | low | — |
| `probe_multisport_reports.py`, `probe_next_data_feature_families.py` | Research probes | references `reports/` | → `tools/probes/` | low | — |
| `pipeline/pilot_cli.py` (24 LOC) | Probably thin runner | grep no direct callers | **manual check**; merge in pilot_orchestrator or delete | low | — |
| `services/leaderboards_backfill.py` | Only backfill in services/ | misplaced | Move to top-level + rename | low | leaderboards tests |
| `sports_data.db` (корень) | Local SQLite dump | unclear callers | **manual check**; если dead — archive | low | — |
| `event_endpoint_matrix_7d.tsv`, `event_matrix_7d_*.sql`, `audit_*.{csv,md,txt}` (корень) | Untracked audit artifacts | clutter | → `audit/` (gitignored) | low | — |
| `default_tournaments_pipeline_cli.py::_load_round_numbers` (L1081-1101) | Phase 4 replacement = `_load_season_rounds_catalog` | no in-file callers | **manual verify** then delete | low | default_tournaments tests |

---

## Incremental Refactor Plan

### Phase 0 — docs + ownership map only (NO code changes)

**Что меняется**: ничего в коде.
**Файлы**:
- `docs/SIMPLIFICATION_AUDIT.md` (этот — создан)
- `docs/MODULE_OWNERSHIP.md` (new — owner per subsystem)
- `tools/README.md`, `scripts/README.md`, `schema_inspector/services/README.md`, `schema_inspector/parsers/sports/README.md` (new)

**Риск**: zero. **Откат**: `git revert`. **Тесты**: smoke `pytest tests/ -q`. **Prod**: docs-only.

### Phase 1 — удалить очевидный мусор

**Что меняется**:
- DELETE: 4 личных скрипта (`analyze_entities.py`, `check.py`, `check_issues.py`, `tracer.py`) = **-236 LOC**
- DELETE: stale notes (`pa_pattenr.txt`, `unique_api_map.txt`, `api_docs.md`, `sofascore_api.md`)
- ARCHIVE: `tools/audit_volatile_blocks.py` + `_v2.py` → `tools/archived/` = **-209 LOC**
- DELETE: `workers/normalize_worker.py` shim (после grep-replace 33 импорта) = **-7 LOC + cleaner dep graph**
- MOVE: audit artifacts → `audit/` (gitignored)
- MOVE: `probe_multisport_reports.py`, `probe_next_data_feature_families.py` → `tools/probes/`
- FIX: `load_event_detail.py` — указывает на несуществующий `event_detail_cli.py`. Либо удалить shim, либо переделать на `event_detail_backfill_cli`.
- VERIFY + DELETE: `default_tournaments_pipeline_cli.py::_load_round_numbers` (no in-file callers, Phase 4 noted replacement)

**Файлы**: 12-15 файлов в корне + 3 файла в tools/
**Риск**: low (всё что удаляется — broken paths, dead imports, или archived already)
**Откат**: revert + восстановить archived
**Тесты**: `pytest tests/ -q` + `python -m schema_inspector.cli health` + smoke
**Prod**: zero runtime impact; deploy не нужен

**Pre-check для каждого файла**:
```bash
grep -r "<filename>" --include='*.py'
grep -r "<filename>" deploy/ ops/ docs/ scripts/
ssh sofascore-prod 'find /opt/sofascore -name "<filename>"'
```

### Phase 2 — разрезать cli.py

**Что меняется**: `cli.py` (3064) → `cli/main.py` (~200) + 6 командных модулей + `cli/shared/*` + `cli/hybrid_app.py`.

**Сохранить точку входа** `python -m schema_inspector.cli <command>` через `cli/__main__.py`.

**Сохранить публичный API** в `cli/__init__.py`: re-export `main`, `HybridApp`, `_ReplayFreshnessStore`, `PrefetchedRun`, `HydrationBatchReport`, `ReplayBatchReport`, `HybridSnapshotStore`, `run_event_command`, etc. (нужны для `test_hybrid_cli.py`, `test_hybrid_cli_integration.py`, `test_hybrid_cli_freshness_replay.py`).

**Файлы**: 1 файл → 13 файлов (~200 LOC каждый)
**Риск**: medium — каждый из 34 systemd unit зависит от cli
**Откат**: revert; cli.py возвращается как был
**Тесты** (добавить **до** split):
- Snapshot test `_build_parser().format_help()` для каждой команды (46 шт)
- Parametrized smoke `for cmd in COMMANDS: parse_args([cmd, "--help"])` не падает
- Registry-полнота `set(parser._subparsers...) == set(COMMANDS.keys())`
- `test_systemd_assets.py` (existing) — проверяет ExecStart строки

**Prod rollout**:
1. Локально `python -m schema_inspector.cli health`
2. Деплой на 1 worker unit (`sofascore-discovery@1`); journalctl -u
3. Если ОК → раскатить на остальные
4. Rollback: `git revert` + `git pull` + `systemctl restart`

### Phase 3 — разрезать orchestrator + service_app + local_api_server

**3 раздельных PR**, не один большой.

**Phase 3a — service_app split** (medium risk, low coupling):
- Extract `services/dependencies.py::ServiceDependencies.from_app(app)`
- Extract `services/factories/resolver_registry.py` (declarative RESOLVER_SPECS) — это сразу сокращает `build_resource_planner_daemon` с 157 LOC до ~30
- Extract `services/factories/{planner,worker}_factories.py` (free functions vs methods)
- Slim `service_app.py` to ~300 LOC of `run_*` wrappers + `ensure_consumer_groups` + lifecycle
- **Tests to add BEFORE**: snapshot test → assert resolver registry has expected 20 kinds; per-build_X_planner_daemon shape test

**Phase 3b — pilot_orchestrator split** (high risk, hottest path):
- Extract `pipeline/orchestrator_constants.py` (constants block L62-112) — quick win
- Extract `pipeline/_endpoint_lookups.py` (L1892-1946) — already top-level, zero risk
- Extract `pipeline/recorders.py` (CapabilityRecorder + LiveStateRecorder + DeferredCapabilityRecord + CapabilityRollupAccumulator)
- Extract `pipeline/missing_root_policy.py` (`_should_retire_missing_root_event` 36 LOC)
- Extract `pipeline/orchestrator_stages.py` — Stages 6, 11, 14 as free functions accepting `RunContext` dataclass (23 local vars from Stage 3)
- Extract sport-specific hooks → `parsers/sports/<sport>/orchestration.py` через `SportAdapter.orchestration_hooks` protocol
- **Tests to add BEFORE**: stage-level tests for entity-profile fanout (Stage 11), detail fanout (Stage 14)

**Phase 3c — local_api_server split** (HIGHEST risk, hottest production):
- 1st PR: extract `api/route_registry.py` + `api/payload_builders.py` (pure functions, no risk)
- 2nd PR: extract `api/ops/routes.py` (11 ops endpoints, independent from waterfall) + `api/cache/layer.py`
- 3rd PR: extract `api/entity_root_dispatcher.py` (5 entity root handlers, single seam)
- 4th PR: extract `api/snapshot_pipeline.py` (`_fetch_snapshot_payload` + `_reconcile_snapshot_payload`) — **HIGHEST RISK, hottest production path**. Tests must verify connection reuse via "connection-acquired-once" assertion.
- 5th PR: extract `api/routes/*` per domain (event/team/player/tournament/sport/misc)

**Каждый под-PR ≤ +600/-500 LOC**. Canary deploy на 1 API instance (port 8001 параллельно), проверить через `scripts/audit_api_latency.py`, потом переключить port 8000.

### Phase 4 — разрезать parsers + normalize_repository

- `event_detail_parser.py` (2337) → `parsers/event/{root,stats,lineups,incidents,h2h,managers,heatmap}.py`
- `storage/normalize_repository.py` (2242) → `storage/normalize/{event,team,player,tournament,statistics,lineup}.py` + thin facade
- `event_detail_repository.py` (1408) — параллельный split
- `services/housekeeping.py` (895) — split per task

**Риск**: medium-high (parsers — write path)
**Тесты**: `pytest tests/test_event_detail_parser.py tests/test_storage_normalize_repository.py tests/test_storage_normalize_repository_integration.py -v` + integration на тестовом event_id

### Phase 5 — унификация + docs + cleanup

- Унифицировать сигнатуру workers (общий `run(consumer_name, *, redis, db)` хотя бы документально)
- Унифицировать CLI args через `add_common_runtime_args(parser)` mixin (-1540 LOC boilerplate из 22 CLI)
- Извлечь `cli_helpers.py::progress` + `configure_logging` (-90 LOC из 9 CLI)
- Merge CLI pairs (leaderboards/standings/statistics/entities) через `--mode single/batch` (-280 LOC)
- Обновить `docs/SERVICES_AND_WORKERS.md`, `docs/CLI_AND_SCRIPTS.md` под новую структуру
- Rename `workers/live_worker.py` → `live_state_machine.py` (комплекс confusion)

**Риск**: low (документация + thin interfaces)

---

## Open Questions (нужен ack от Бобура перед изменениями)

1. **`load_*.py` shims**: можно удалять прямо в Phase 1, или должен быть deprecation period? Ни один systemd unit не ссылается на них (CLAUDE.md упоминает "обратную совместимость" — насколько строго?).
2. **`load_event_detail.py`** — указывает на несуществующий `schema_inspector.event_detail_cli`, файла нет. Fix (на `event_detail_backfill_cli`) или delete?
3. **`pipeline/pilot_cli.py` (24 LOC)** — grep не нашёл прямых callers. Merge в `pilot_orchestrator` или delete?
4. **`workers/normalize_worker.py` shim (7 LOC)** — 33 импорта в коде. Сделать grep-replace на `normalizers.worker.NormalizeWorker` и удалить shim в Phase 1?
5. **`services/leaderboards_backfill.py`** — единственный backfill в services/ (остальные в корне). Намеренно?
6. **`sports_data.db` в корне** — local SQLite dump. Delete?
7. **`setup_postgres.py`** в корне — bootstrap helper. Переехать в `scripts/`?
8. **`serve_local_api.py`, `serve_local_swagger.py`, `build_local_swagger.py`** в корне — dev entrypoints. Переехать в `tools/dev/`?
9. **`test_run.py`, `inspect_api.py`** в корне — ad-hoc debug. Переехать в `tools/`?
10. **`local_swagger_builder.py` (3279)** — runtime или build-time? Если build-time only — переехать в `tools/`?
11. **Тесты в worktree (`sofascore/romantic-jennings-847dfd/tests/`)** — случайный дубликат или артефакт?
12. **`workers/live_worker.py`** — переименовать в `live_state_machine.py` для clarity или оставить?
13. **`default_tournaments_pipeline_cli.py::_load_round_numbers` (L1081-1101)** — Phase 4 docstring говорит "replaced". Удалить в Phase 1 или нужна manual verification?
14. **Phase 3c timing**: разрезать `local_api_server.py` сейчас (большой риск) или после стабилизации Phase 2 (cli.py) и Phase 3a (service_app)? Голосую за "после".
15. **`HybridApp` (851 LOC) внутри cli.py** — это не команда, а orchestrator state machine. Переехать в `pipeline/hybrid_app.py` или оставить в `cli/`?

---

## Метрики этого audit

- **Файлов проверено**: ~650 (296 schema_inspector + 268 tests + 16 tools + 7 scripts + 21 load_* + 5 root diag + 34 systemd unit + остальное)
- **Top-6 god-objects содержат**: 18 156 LOC из ~50k total LOC проекта (≈36% всей кодовой базы)
- **Delete candidates (явные)**: 25 files = -250 LOC (4 личных скрипта + 4 stale notes + 2 audit-tools + 1 shim + 14 + load_*.py если delete)
- **Archive candidates**: 8 (audit/, tools/archived/, legacy/, docs/archived/)
- **Needs-manual-check**: 7 (`check_issues.py`, `normalize_stream_worker.py` vs `normalize_worker.py`, `pipeline/pilot_cli.py`, `services/leaderboards_backfill.py`, `sports_data.db`, `default_tournaments_pipeline_cli.py::_load_round_numbers`, `load_event_detail.py`)
- **Boilerplate dedup потенциал**: ~1640 LOC (90 в _progress/_configure, 1540 в argparse, остальное в pipeline stages + CLI pairs)
- **Concrete refactor scope (no deletion, move only)**: ~520 LOC (`_run_tournament_worker` → `services/`)
- **Total practical reduction**: ~1400 LOC + значительное снижение когнитивной нагрузки

### Production safety красные флаги (top 5, из opus-аудита `local_api_server.py`)

1. **`_fetch_snapshot_payload` + `_reconcile_snapshot_payload` connection-reuse fragile**: если split не сохранит `connection` от первого метода ко второму, `asyncpg.Pool` может exhaust. Add "connection-acquired-once" assertion в LocalApiSnapshotReconciliationTests **до** split.
2. **`_fetch_event_root_payload` 4-layer waterfall** имеет implicit terminal-status sequencing. Если split инвертирует branch order — событие с terminal state + fresh snapshot вернёт wrong status. LocalApiEntityRootFallbackTests — safety net.
3. **Stale snapshot detection per-event SQL** (lines 967-1004) — short-circuit через `is_staleness_sensitive_endpoint`. Если staleness branch вынести в middleware, bypass list (h2h, votes, managers) должен быть consistent.
4. **`_runtime_loop` daemon thread + `LocalApiApplication.startup` NOT idempotent** через `__new__()`-built test instances (lines 4548, 2608 defensively probe attributes). Если split вынесет cache/runtime state — все `getattr` defaults должны быть replicated в test fakes.
5. **`_PAYLOAD_CACHE_PATTERNS` TTL derives from payload status** (lines 845-857). Если entity-root split меняет envelope key shape (`events[0]` vs `event`), TTL fallback на default (10s) для live events которые должны быть 5s. Need TTL contract test per cache pattern.

---

## Что НЕ сделано в этом audit (deferred)

- Глубокий анализ `parsers/families/`, `parsers/sports/`, `parsers/special/` структуры — отдельный 2-3 часовой обзор
- Профилирование performance каждого route — не входило в scope
- Анализ `tests/` структуры (268 файлов = возможно дубли fixtures)
- Анализ migrations порядка применения
- Анализ WebSocket layer (`ws_*.py` × 6 в корне `schema_inspector/`)
- Анализ `docs/` структуры (30+ файлов, некоторые stale)
- Performance benchmark `local_api_server.py` routes (для приоритезации which route to split first)

Эти можно сделать в отдельных audit PR.
