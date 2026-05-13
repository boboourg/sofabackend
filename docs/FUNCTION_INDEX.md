# Function index

Карта ключевых функций / классов по файлам. Не описывает helper'ы на 10 строк — только entrypoint / business-critical функции и публичный API.

Все пути абсолютные от `D:\sofascore\`.

---

## CLI

### `schema_inspector\cli.py`

| Function / Class | Что делает | Caller | Side effects |
|---|---|---|---|
| `_build_parser() → ArgumentParser` (L1682) | Регистрирует все subcommands | `main()` | — |
| `main()` (L1156) | Точка входа; dispatcher через `if args.command == "..."` | `__main__` | зависит от subcommand |
| `_dispatch(args)` (L1170+) | Async wrapper для команд | `main()` | mutation если subcommand mutation |
| `ServiceApp` class (L290+) | Контейнер всех зависимостей (DB pool, Redis, repositories, queue, parsers) | используется в каждом subcommand handler | создаёт pool, redis backend, lazy-init repositories |
| `ServiceApp.recover_live_state()` (L1039) | Rebuild Redis live state из PG | CLI `recover-live-state` | Redis writes |
| `ServiceApp.rebuild_capability_rollup(sport_slug, lookback_days)` (L1048+) | Out-of-band rollup rebuild from observations | CLI `rebuild-capability-rollup` | PG writes (endpoint_capability_rollup) |
| `ServiceApp.collect_health()` (L1030) | Compact health report | CLI `health`, /ops/health | read-only |
| `run_event_command(args, *, orchestrator)` (L1049) | One-shot hydration explicit event ids | CLI `event` | DB writes, Redis writes |

---

## Pipeline

### `schema_inspector\pipeline\pilot_orchestrator.py`

Сердце hydration. ~2000 строк. Главные элементы:

| Function / Class | Что делает | Caller | Side effects |
|---|---|---|---|
| `PilotOrchestrator.__init__(...)` (L238) | DI: transport, fetch_executor, parser_registry, normalizer, repositories, sql_executor | `ServiceApp._build_orchestrator()` | — |
| `PilotOrchestrator.run_event(event_id, sport_slug, hydration_mode="full") → EventHydrationReport` (~L300-1000) | Полная hydration одного event'а | HydrateWorker, LiveWorkerService, CLI `event` | DB writes (event*, snapshot, terminal_state, history, capability obs, job_run), Redis live state |
| `PilotOrchestrator.run_event_details(...)` (~L1086+) | Split-details flow (P0a) — реконструирует detail fanout из cached context | LiveDetailsWorkerService | DB + Redis |
| `PilotOrchestrator._flush_capabilities()` (L1666) | Drains `_pending_capability_records` → insert_observation. Если flag ON → upsert_rollup with sorted keys | inside `run_event*` at 5 точках | DB writes (capability_observation; rollup только если flag ON) |
| `PilotOrchestrator._record_live_state_history(...)` (~L1682) | INSERT event_live_state_history | inside run_event | DB write |
| `PilotOrchestrator.observe(outcome) → CapabilityRollupRecord` (L174) | Из fetch outcome строит rollup candidate | inside run_event per fetch | — (defer) |
| `DeferredCapabilityRecord` dataclass (L215) | Пара observation + rollup, накапливается в `_pending_capability_records` | — | — |
| `_inline_capability_rollup_enabled() → bool` (~end) | Reads `SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED` env | `_flush_capabilities` | — |

### `schema_inspector\fetch_executor.py`

| Function | Что | Caller |
|---|---|---|
| `FetchExecutor.execute(task: FetchTask) → FetchOutcomeEnvelope` | Главный entrypoint: freshness check → HEAD probe gating → curl_cffi GET via proxy → classify → snapshot insert | PilotOrchestrator (perfetch) |
| `FetchExecutor._execute_with_retries(...)` | Retry loop с backoff per `SCHEMA_INSPECTOR_*` env | execute() |
| `FetchExecutor._classify(outcome)` | success/soft_error/network_error/challenge/rate_limited/access_denied | execute() |

---

## Workers

### `schema_inspector\workers\discovery_worker.py`

| Function / Class | Что | Caller | Side effects |
|---|---|---|---|
| `DiscoveryWorker.handle(entry: StreamEntry)` | Main handler — parse surface response, fanout hydrate/live tier jobs | WorkerRuntime | publish to STREAM_HYDRATE, STREAM_LIVE_TIER_* |
| `DiscoveryWorker._normalize_surface_result(...)` | Парсит payload → list of `_SurfaceEvent`. **X4 layer 1**: skip is_editor=True | handle() | — |
| `_SurfaceEvent` dataclass | id, sport_slug, status_type, is_editor, dispatch_tier candidates | — | — |

### `schema_inspector\workers\hydrate_worker.py`

| Function / Class | Что | Caller |
|---|---|---|
| `HydrateWorker.handle(entry)` | Handle JOB_HYDRATE_EVENT_ROOT → `orchestrator.run_event(...)` | WorkerRuntime |
| Per-event circuit breaker (P5b) | Skip event если в quarantine | handle() (если `HYDRATE_EVENT_CIRCUIT_BREAKER_ENABLED=1`) |

### `schema_inspector\workers\live_worker_service.py`

| Function / Class | Что | Caller |
|---|---|---|
| `LiveWorkerService.handle(entry)` | Handle JOB_REFRESH_LIVE_EVENT → `orchestrator.run_event(hydration_mode="live_delta")` | WorkerRuntime |
| `LiveWorkerService._batch_dispatch(entries)` | Batch preprocessor: coalesce duplicate event_ids, stale entry ACK | WorkerRuntime |
| Backpressure check для STREAM_LIVE_DETAILS publish | Skip fanout если stream lag > LIVE_DETAILS_STREAM_BACKPRESSURE_LIMIT | handle() |

### `schema_inspector\workers\live_details_worker_service.py`

| Function / Class | Что | Caller |
|---|---|---|
| `LiveDetailsWorkerService.handle(entry)` | Handle JOB_REFRESH_LIVE_EVENT_DETAILS → `orchestrator.run_event_details(...)` | WorkerRuntime (P0a split) |

### `schema_inspector\workers\maintenance_worker.py`

| Function | Что | Caller |
|---|---|---|
| `MaintenanceWorker.run_forever()` | XAUTOCLAIM cycle через все operational+historical streams; publish DLQ если max_delivery_count exceeded | systemd `sofascore-maintenance@N` |

### `schema_inspector\workers\resource_refresh_worker.py`

| Function | Что | Caller |
|---|---|---|
| `ResourceRefreshWorker.handle(entry)` | Generic endpoint refresh. Early exits: freshness skip, empty-data skip, negative cache skip. Иначе `FetchExecutor.execute` + snapshot insert | WorkerRuntime |

### `schema_inspector\workers\normalize_worker.py` / `normalizers\worker.py`

| Function | Что | Caller |
|---|---|---|
| `NormalizeWorker.handle(entry)` | Re-parse snapshot id через parser registry, upsert normalized tables | WorkerRuntime |
| `DurableNormalizeSink.process_snapshot(snapshot_id)` | Загружает snapshot, classify, parse, upsert normalized | NormalizeWorker |

### `schema_inspector\workers\historical_archive_worker.py`

| Function | Что | Caller |
|---|---|---|
| `HistoricalTournamentWorker.handle(entry)` | Historical tournament/season archival | WorkerRuntime |
| `HistoricalEnrichmentWorker.handle(entry)` | Post-tournament enrichment (stats, top players, standings) | WorkerRuntime |

---

## Services (Planners / Runtime)

### `schema_inspector\services\worker_runtime.py`

| Function / Class | Что | Caller |
|---|---|---|
| `WorkerRuntime.__init__(...)` (L93) | Initialize: queue, stream, group, consumer, handler, retry policy, audit logger | All workers |
| `WorkerRuntime.run_forever(install_signal_handlers=True)` | Main loop: XREADGROUP → handler → ACK / retry / DLQ | All workers |
| `WorkerRuntime._handle_entry(entry)` (L288) | Per-entry exception classification, retry scheduling, structured retry log | run_forever |
| `is_retryable_worker_error(exc)` (retry_policy.py) | Classify: RetryableJobError, TimeoutError, RequestsError, sqlstate 40P01/55P03 → True | _handle_entry |
| `retry_delay_ms(attempt, exc)` | Backoff calculation | _handle_entry |
| `retry_audit_status(exc)` | Returns "retry_scheduled" status string | _handle_entry |

### `schema_inspector\services\planner_daemon.py`

| Function / Class | Что | Caller |
|---|---|---|
| `PlannerDaemon.__init__(...)` (L40+) | DI: queue, live_state_store, live_backpressure, sport_targets | service_app |
| `PlannerDaemon.run_forever()` (L91) | Loop: tick + sleep(loop_interval_s) | systemd `sofascore-planner.service` |
| `PlannerDaemon.tick(now_ms)` | `_drive_scheduled_plans` + `_drive_live_refresh` + `_drive_delayed_replay` | run_forever |
| `PlannerDaemon._drive_live_refresh(now_ms)` (L131) | Для каждого lane in (hot, warm): `due_events` → fetch state → backpressure check → claim_dispatch → publish | tick |
| `lease_ms_for_dispatch_tier(tier, default_ms)` (live_dispatch_policy.py:105) | Returns LIVE_DISPATCH_LEASE_TIER_<N>_MS for tier | _drive_live_refresh |
| `_blocking_reason_for_stream(backpressure, stream) → str | None` (L272) | Backpressure check | _drive_live_refresh |

### `schema_inspector\services\live_discovery_planner.py`

| Function | Что |
|---|---|
| `LiveDiscoveryPlannerDaemon.run_forever()` | Loop per-sport JOB_DISCOVER_SPORT_SURFACE publish с cadence + repair_cooldown |

### `schema_inspector\services\historical_planner.py` / `historical_tournament_planner.py`

| Function | Что |
|---|---|
| `HistoricalPlannerDaemon.run_forever()` | Rolling date-range publish через cursor |
| `HistoricalTournamentPlannerDaemon.run_forever()` | Tournament/season publish per cursor |

### `schema_inspector\services\resource_planner.py`

| Function | Что |
|---|---|
| `ResourcePlannerDaemon.run_forever()` | Freshness-driven publish JOB_REFRESH_RESOURCE с per-tick cap + lag gating |

### `schema_inspector\services\service_app.py`

| Function | Что | Caller |
|---|---|---|
| `ServiceApp.build_orchestrator(...)` | Factory: создаёт PilotOrchestrator с правильно zip'нутыми dependencies | cli commands |
| `ServiceApp.build_planner_daemon(...)` | Factory для PlannerDaemon | cli `planner-daemon` |
| ... аналогичные factory методы для каждого daemon/worker | — | cli |

### `schema_inspector\services\structure_sync_service.py`

| Function | Что |
|---|---|
| `run_structure_sync_for_tournament(...)` | Rounds-mode или calendar-mode sync скелета tournament/season/event |

### `schema_inspector\services\tournament_registry_service.py`

| Function | Что |
|---|---|
| `TournamentRegistryService.refresh_loop()` | Periodic refresh `tournament_registry` table + Redis set |

### `schema_inspector\services\backpressure.py` / `backpressure_config.py`

| Function | Что |
|---|---|
| `QueueBackpressure.blocking_reason_for_stream(stream) → str|None` | Check stream lag vs MAX_LAG limit |
| `BackpressureLimit` dataclass | stream, group, max_lag |

---

## Storage (Repositories)

`D:\sofascore\schema_inspector\storage\`. Все repositories accept `executor` (asyncpg Connection или transaction).

| Repository | Public methods (summary) |
|---|---|
| `RawRepository` | `insert_request_log`, `insert_payload_snapshot`, `insert_payload_snapshot_if_missing_returning_id`, `upsert_snapshot_head`, `fetch_payload_snapshot`, `upsert_endpoint_registry_entries` |
| `NormalizeRepository` | 30+ insert/upsert/fetch методы per entity (event, score, lineup, incident, statistic, player_statistics, standing, season_*, tournament_*) |
| `JobRepository` | `insert_job_run(record)`, `upsert_job_effect(...)`, `insert_replay_log(...)` |
| `JobStageRepository` | `insert_stage_run(...)` |
| `CoverageRepository` | `upsert_coverage(...)`, `select_event_scope_ids(...)`, `fetch_event_scope_statuses(...)` |
| `CapabilityRepository` | `insert_observation(record)`, `upsert_rollup(record)` (legacy, hot-path race-prone), `rebuild_rollups_from_observations(executor, sport_slug=None, lookback_days=None) → int` (firebreak 2026-05-13 batch path) |
| `LiveStateRepository` | `insert_live_state_history(...)`, `upsert_terminal_state(...)`, `insert_terminal_state_if_missing(...)`, `mark_event_stale_live_retired(...)`, `fetch_latest_live_state_history(...)`, `fetch_terminal_states(...)` |
| `EventEndpointNegativeCacheRepository` | `list_states`, `try_acquire_probe_lease`, `apply_events`, `_upsert_state`, `_insert_log` |
| `EndpointNegativeCacheRepository` | то же, tournament/season scope |
| `RetentionRepository` | `delete_request_log_batch`, `delete_legacy_snapshot_batch`, `delete_live_snapshot_versions_batch`, `count_expired_*` |
| `PlannerCursorRepository` | `load_cursor`, `upsert_cursor` |
| `TournamentRegistryRepository` | `upsert_records`, `select_*`, `fetch_*` |
| `SourceRegistryRepository` | `upsert_source` |
| `ProxyHealthRepository` | `fetch_proxy_traffic` (read-only) |

---

## Queue / Redis primitives

`D:\sofascore\schema_inspector\queue\`

| File | Class / Function | Что |
|---|---|---|
| `streams.py` | `STREAM_*`, `GROUP_*` constants, `RedisStreamQueue` class | XADD/XREADGROUP/XACK/XAUTOCLAIM thin wrapper |
| `live_state.py` | `LiveEventStateStore` | upsert/fetch live:event:*, move_lane (zset:live:*), claim_dispatch, dispatch_metrics_snapshot |
| `delayed.py` | `DelayedJobScheduler` | schedule(job, delay_ms), pop_due(now_ms) |
| `leases.py` | `LeaseStore` | acquire/renew/release |
| `dedupe.py` | `DedupeStore` | claim_job, is_fresh |
| `freshness.py` | `FreshnessStore` | is_fresh, mark_fetched |
| `proxy_state.py` | `ProxyStateStore` | record_failure, record_success, due_for_release |
| `live_inflight.py` | `LiveEventInFlightStore`, `LiveEventDetailsInFlightStore`, `LiveEventRootInFlightStore` | claim, release |
| `live_edges_throttle.py` / `live_details_throttle.py` | `LiveEdgesThrottle`, `LiveDetailsThrottle` | should_enqueue |
| `live_tier_1_quarantine.py` | `LiveTier1RetryQuarantineStore` | record_failure, record_success, is_quarantined |
| `resource_negative_cache.py` | resource negative cache (TTL'd set) | claim, contains |
| `totw_404_store.py` | Team-of-the-week 404 blacklist | — |

---

## Endpoints registry

### `schema_inspector\endpoints.py`

| Symbol | Что |
|---|---|
| `SofascoreEndpoint` (dataclass) | path_template, envelope_key, target_table, query_template, notes, refresh_* fields, prefer_head_probe |
| `EndpointRegistryEntry` (dataclass) | pattern, path_template, query_template, envelope_key, target_table, notes, source_slug, contract_version |
| `local_api_endpoints(sport_slugs=LOCAL_API_SUPPORTED_SPORTS) → tuple[SofascoreEndpoint, ...]` | Главный реестр — собирает все endpoints (per sport + global) |
| Group tuples | `COMPETITION_ENDPOINTS`, `ENTITIES_ENDPOINTS`, `STATISTICS_ENDPOINTS`, `STANDINGS_ENDPOINTS`, `LEADERBOARDS_ENDPOINTS`, `EVENT_DETAIL_BASE_ENDPOINTS`, `EVENT_DETAIL_DEPRECATED_ENDPOINTS`, `EVENT_LIST_ENDPOINTS`, `CATEGORIES_SEED_ENDPOINTS` |
| Per-sport factory functions | `sport_categories_endpoint(slug)`, `sport_scheduled_events_endpoint(slug)`, `sport_live_events_endpoint(slug)`, `event_detail_endpoints(sport_slug)`, и т. д. |
| Constants | `EVENT_DETAIL_ENDPOINT`, `EVENT_STATISTICS_ENDPOINT`, `EVENT_LINEUPS_ENDPOINT`, ..., `UNIQUE_TOURNAMENT_MEDIA_ENDPOINT`, etc. |

---

## Local API server

### `schema_inspector\local_api_server.py`

| Function / Method | Что | Caller |
|---|---|---|
| `LocalApiApplication.__init__(...)` (L188+) | Initialize: db_pool, OpenAPI cache, response cache, event loop thread | startup |
| `LocalApiApplication.startup()` / `shutdown()` | Manage pool + background tasks | uvicorn lifespan |
| `LocalApiApplication.handle_api_get(path, raw_query) → ApiResponse` (L379) | Main dispatch для /api/* | FastAPI handler |
| `LocalApiApplication.handle_api_get_http_response(path, raw_query) → SerializedApiResponse` (L456) | Wrap с cache control + serialization | FastAPI handler |
| `LocalApiApplication.handle_ops_get(path, raw_query) → ApiResponse` (L475) | Dispatch для /ops/* | FastAPI handler |
| `LocalApiApplication._fetch_entity_root_fast_path(path, raw_query) → tuple` (L413) | Fast path для root paths (event/{id}, team/{id}, ...) | handle_api_get |
| `LocalApiApplication._fetch_snapshot_payload(...)` (L492) | Snapshot waterfall search | handle_api_get |
| `LocalApiApplication._reconcile_snapshot_payload(executor, route, payload)` | Overlay live state из normalized tables на raw snapshot (для list payloads) | _fetch_snapshot_payload |
| `LocalApiApplication._fetch_normalized_payload(...)` (L833) | Normalized fallback | handle_api_get |
| `LocalApiApplication._fetch_specialized_normalized_payload(...)` (L907) | Specialized handler dispatcher | _fetch_normalized_payload |
| `LocalApiApplication._fetch_generic_normalized_payload(...)` | Generic table dump (to_jsonb) | _fetch_normalized_payload |
| `LocalApiApplication._fetch_event_root_payload(event_id)` | Event root waterfall (terminal_state → snapshot → synthesizer) | fast_path |
| `LocalApiApplication._fetch_event_statistics_payload(event_id)` | Specialized handler | dispatcher |
| `LocalApiApplication._fetch_event_lineups_payload(event_id)` | Specialized | dispatcher |
| `LocalApiApplication._fetch_event_incidents_payload(event_id)` | Specialized | dispatcher |
| `LocalApiApplication._fetch_event_managers_payload(event_id)` | Specialized | dispatcher |
| `LocalApiApplication._fetch_season_events_payload(unique_tournament_id, season_id, page, direction)` | Specialized (last/next pagination) | dispatcher |
| `LocalApiApplication._fetch_standings_payload(...)` | Specialized | dispatcher |
| `LocalApiApplication._fetch_unique_tournament_media_payload(unique_tournament_id) → dict` | Synthetic media feed (aggregates highlights snapshots) | dispatcher |
| `_serialize_season_event_row(row) → dict` | Event row → Sofascore-shape dict | _fetch_season_events_payload |
| `_serialize_media_event_row(row) → dict` | Event row → media event shape | _fetch_unique_tournament_media_payload |
| `_synthesize_event_root_payload(row) → dict` | Normalized event row → event payload (synthesizer) | _fetch_event_root_payload (fallback) |
| `_minimal_entity_payload(id, slug, name, short_name) → dict` | Build minimal entity object | многие helpers |
| `_serialize_scalar(value)` | datetime/Decimal/dict/list → JSON-safe | global helper |
| `_decode_snapshot_payload(payload)` | str | dict → dict | snapshot fetch path |
| `_snapshot_row_is_soft_error(row) → bool` | Check is_soft_error_payload OR http_status >= 400 | snapshot filter |
| `_wrap_stripped_entity_payload(payload, wrapper_key)` | Re-wrap legacy snapshots без envelope key | snapshot fetch |
| `build_route_specs() → tuple[RouteSpec, ...]` (L3272) | Compile path_regex + context_entity_type per endpoint | startup |
| `match_route(path, routes) → tuple[RouteSpec, dict[str,str]] | None` (L3286) | Match path against compiled regexes | handle_api_get |
| `create_asgi_app(...)` | FastAPI factory (uvicorn entrypoint) | systemd uvicorn |

### `schema_inspector\local_swagger_builder.py`

| Function | Что | Caller |
|---|---|---|
| `build_openapi_document(summary) → dict` (L416) | Главный билдер OpenAPI 3.0 docs | LocalApiApplication.openapi_json |
| `_build_core_paths(summary) → dict` | Сборка всех `/api/v1/...` paths | build_openapi_document |
| `_build_sport_specific_core_paths(op)` | Per-sport paths (live/scheduled per slug) | _build_core_paths |
| `_make_operation_builder(summary)` | Factory `op()` callable для `{"get": {...}}` | _build_core_paths |
| `_build_schemas()` | JSON Schema components | build_openapi_document |
| `load_cached_openapi_bytes()` / `write_cached_openapi_bytes()` | Файловый кеш OpenAPI | LocalApiApplication startup |

---

## Policies

### `schema_inspector\match_center_policy.py`

| Function | Что | Caller |
|---|---|---|
| `football_detail_tier(detail_id: int | None) → str` | Map detail_id → "tier_1" / "tier_2" / "tier_3" / "tier_5" | live_dispatch_policy, detail_resource_policy, pilot_orchestrator |
| `football_edge_allowed(*, detail_tier, status_type, edge_kind, is_editor) → bool` | Gate для core edges (incidents/lineups/stats/graph/comments) | pilot_orchestrator |
| `football_detail_endpoint_allowed(*, detail_tier, status_type, endpoint_pattern, is_editor, has_event_player_heat_map, has_xg) → bool` | Gate для detail endpoints | detail_resource_policy |
| `football_special_allowed(...) → bool` | Gate для special endpoints (heatmap, shotmap, ...) | pilot_orchestrator |
| `filter_football_detail_specs(specs, *, detail_tier, status_type, ...)` | Apply gate to list of specs | pilot_orchestrator |

### `schema_inspector\detail_resource_policy.py`

| Function | Что | Caller |
|---|---|---|
| `build_event_detail_request_specs(*, sport_slug, hydration_mode, detail_tier, status_type, is_editor, ...)→ tuple[FetchTaskSpec, ...]` (~L100+) | Главный билдер списка fetch specs per event | pilot_orchestrator.run_event Phase 2 |
| `supports_live_detail_resources(sport_slug) → bool` | Check если sport имеет live detail endpoints | pilot_orchestrator |

### `schema_inspector\live_dispatch_policy.py`

| Function | Что | Caller |
|---|---|---|
| `resolve_live_dispatch_tier(*, sport_slug, detail_id, tournament_tier, tournament_user_count) → str` (L58) | Route event → "tier_1"/"tier_2"/"tier_3" | DiscoveryWorker, PlannerDaemon |
| `poll_seconds_for_live_dispatch_tier(tier, default_seconds) → int` | env-driven poll cadence per tier | PlannerDaemon |
| `lease_ms_for_dispatch_tier(tier, default_ms) → int` (L105) | env-driven Redis lease TTL per tier | PlannerDaemon._drive_live_refresh |
| `normalize_live_dispatch_tier(value) → str` | "hot"/"warm" → "tier_1"/"tier_2", fallback "tier_3" | везде |
| `live_priority_for_dispatch_tier(tier) → int` | Priority 0/1/2 для job ordering | PlannerDaemon |

### `schema_inspector\live_delta_policy.py`

| Function | Что |
|---|---|
| `live_delta_edge_kinds(sport_slug) → tuple[str, ...]` | Which core edges to refresh in live_delta mode |
| `live_delta_detail_endpoints(sport_slug) → tuple[str, ...]` | Which detail endpoints (per sport). Football intentionally empty (X4 fall-through) |
| `live_delta_detail_endpoint_patterns(sport_slug) → tuple[str, ...]` | Pattern strings |

### `schema_inspector\event_endpoint_negative_cache.py`

| Class / Function | Что |
|---|---|
| `EventEndpointNegativeCache.decide_event_probe(event_id, status_phase, endpoint_pattern, now) → ProbeDecision` | Решает: allow / block / lease_only |
| `EventEndpointNegativeCache.record_event_outcome(event_id, status_phase, endpoint_pattern, classification, http_status, ...)` | Update state в БД |
| `load_event_negative_cache_settings()` | Read env config |

### `schema_inspector\season_widget_negative_cache.py`

Аналогично, но scope = (unique_tournament_id, [season_id], endpoint_pattern).

---

## Parsers

`D:\sofascore\schema_inspector\parsers\`

| File | Class / Function |
|---|---|
| `registry.py` | `classify_snapshot(snapshot) → ParseFamily | None`, `lookup_parser(family, sport) → BaseParser`, `parse(snapshot) → ParseResult` |
| `families/event_root.py` | `EventRootParser.parse(snapshot)` |
| `families/event_statistics.py` | `EventStatisticsParser.parse(snapshot)` |
| `families/event_lineups.py` | `EventLineupsParser.parse(snapshot)` |
| `families/event_incidents.py` | `EventIncidentsParser.parse(snapshot)` |
| ... | каждый parser имеет `.parse(snapshot) → ParseResult` |
| `special/tennis_point_by_point.py` | `TennisPointByPointParser.parse(snapshot)` |
| `special/shotmap.py` | `ShotmapParser.parse(snapshot)` |

`ParseResult` (`schema_inspector\parsers\base.py`): status, normalized records, errors, metadata.

---

## Database / Transport

### `schema_inspector\db.py`

| Function | Что |
|---|---|
| `load_database_config() → DatabaseConfig` | Read env (SOFASCORE_DATABASE_URL → DATABASE_URL → POSTGRES_DSN, plus min/max size, timeout) |
| `create_pool_with_fallback(config) → asyncpg.Pool` | Create pool с retry на connection failure |

### `schema_inspector\runtime.py` / `transport.py`

| Function / Class | Что |
|---|---|
| `Transport.fetch(url, method="GET", headers=None, timeout=20) → TransportResult` | Главный fetch entrypoint через curl_cffi + proxy session |
| `ProxyEndpointPool` | Round-robin pool over `SCHEMA_INSPECTOR_PROXY_URLS` |
| `_load_project_env()` | Load `.env` файл |

---

## Где смотреть тесты

`D:\sofascore\tests\test_<area>.py`:
- `test_pilot_live_paths.py`, `test_pilot_live_state_persistence.py` — orchestrator flow
- `test_hybrid_pipeline_<sport>.py` — sport-specific E2E
- `test_local_api_server.py` — API routes
- `test_local_swagger_builder.py` — OpenAPI generation
- `test_worker_runtime.py`, `test_worker_runtime_retry_log.py` — worker runtime
- `test_storage_capability_repository.py`, `test_capability_rollup.py`, `test_capability_rollup_firebreak.py` — capability
- `test_<parser>.py` — каждый parser
- `test_match_center_policy.py`, `test_detail_resource_policy.py`, `test_live_dispatch_policy.py` — policies
- `test_*_migration.py` — некоторые миграции
- `test_event_endpoint_negative_cache.py`, `test_season_widget_negative_cache.py` — negative caches

Запуск всех: `D:/sofascore/.venv311/Scripts/python.exe -m pytest -q`

---

## Связано

- [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) — overview
- [SERVICES_AND_WORKERS.md](SERVICES_AND_WORKERS.md) — services
- [DATABASE_AND_STORAGE.md](DATABASE_AND_STORAGE.md) — taблицы
- [REDIS_AND_QUEUES.md](REDIS_AND_QUEUES.md) — Redis
- [PARSING_AND_POLICIES.md](PARSING_AND_POLICIES.md) — policies
- [API_ROUTES.md](API_ROUTES.md) — local API
