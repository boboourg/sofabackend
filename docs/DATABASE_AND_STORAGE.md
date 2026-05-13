# Database & Storage

PostgreSQL 16 — durable хранилище. Schema snapshot: `D:\sofascore\postgres_schema.sql`. Datestamped migrations: `D:\sofascore\migrations\YYYY-MM-DD_*.sql` (append-only, не редактируются).

Все async DB access — через `asyncpg` pool, создаваемый `D:\sofascore\schema_inspector\db.py:create_pool_with_fallback()` (config через `load_database_config`).

---

## Source of truth контракт

Проект следует **raw-snapshot-is-canonical** философии. Для большинства Sofascore-shape ответов источник истины:

```
1. event_terminal_state.final_snapshot_id  (если событие завершилось)
2. Latest api_payload_snapshot pinned к entity_id
3. Synthesizer из normalized tables  (только fallback)
```

Объяснение: upstream payload содержит fieldTranslations, userCount, priority, coverage, changes, teamColors, country nested, eventState — поля которые мы не нормализуем. Synthesizer корректно отдаёт data модель, но **не 1:1** с Sofascore. См. CLAUDE.md.

Для **list responses** (`/events/live`, `/scheduled-events/{date}`) snapshot reconcile overlays live данные из normalized таблиц (status_code, scores, time_payload) поверх raw snapshot — даёт actual live state с full payload shape.

---

## Repositories (storage слой)

Все в `D:\sofascore\schema_inspector\storage\`.

| Файл | Класс | Ключевые методы | Пишет | Читает |
|---|---|---|---|---|
| `raw_repository.py` | `RawRepository` | `insert_request_log`, `insert_payload_snapshot`, `insert_payload_snapshot_if_missing_returning_id`, `upsert_snapshot_head`, `fetch_payload_snapshot`, `upsert_endpoint_registry_entries` | api_request_log, api_payload_snapshot, api_snapshot_head, endpoint_registry, endpoint_contract_registry | api_payload_snapshot |
| `normalize_repository.py` | `NormalizeRepository` | 30+ методов: insert_*, upsert_*, fetch_* per entity | event, event_score, event_lineup, event_lineup_player, event_incident, event_statistic, event_player_statistics, standing, standing_row, season_*, tournament_* | event, player, team, unique_tournament, season |
| `job_repository.py` | `JobRepository` | `insert_job_run`, `upsert_job_effect`, `insert_replay_log` | etl_job_run, etl_job_effect, etl_replay_log | — |
| `job_stage_repository.py` | `JobStageRepository` | `insert_stage_run` | etl_job_stage_run | — |
| `coverage_repository.py` | `CoverageRepository` | `upsert_coverage`, `select_event_scope_ids`, `fetch_event_scope_statuses` | coverage_ledger | coverage_ledger, event |
| `capability_repository.py` | `CapabilityRepository` | `insert_observation`, `upsert_rollup` (legacy), `rebuild_rollups_from_observations` (firebreak 2026-05-13) | endpoint_capability_observation, endpoint_capability_rollup | endpoint_capability_observation |
| `live_state_repository.py` | `LiveStateRepository` | `insert_live_state_history`, `upsert_terminal_state`, `insert_terminal_state_if_missing`, `mark_event_stale_live_retired`, `fetch_latest_live_state_history`, `fetch_terminal_states` | event_live_state_history, event_terminal_state, event | event, event_live_state_history, event_terminal_state |
| `event_endpoint_negative_cache_repository.py` | `EventEndpointNegativeCacheRepository` | `list_states`, `try_acquire_probe_lease`, `apply_events`, `_upsert_state`, `_insert_log` | event_endpoint_negative_cache_state, event_endpoint_availability_log | event_endpoint_negative_cache_state |
| `endpoint_negative_cache_repository.py` | `EndpointNegativeCacheRepository` | то же, для tournament/season scope | endpoint_negative_cache_state, endpoint_availability_log | endpoint_negative_cache_state, event, season |
| `retention_repository.py` | `RetentionRepository` | `delete_request_log_batch`, `delete_legacy_snapshot_batch`, `delete_live_snapshot_versions_batch`, `count_expired_*` | (DELETE only) api_request_log, api_payload_snapshot, endpoint_capability_observation, endpoint_negative_cache_state | те же |
| `planner_cursor_repository.py` | `PlannerCursorRepository` | `load_cursor`, `upsert_cursor` | etl_planner_cursor | etl_planner_cursor |
| `tournament_registry_repository.py` | `TournamentRegistryRepository` | `upsert_records`, `select_*`, `fetch_*` | tournament_registry | tournament_registry, event, unique_tournament |
| `source_registry_repository.py` | `SourceRegistryRepository` | `upsert_source` | provider_source | — |
| `proxy_health_repository.py` | `ProxyHealthRepository` | `fetch_proxy_traffic` | — (read-only) | api_request_log |

---

## Группы таблиц

### Core entities (справочники)

| Таблица | PK | Назначение |
|---|---|---|
| `sport` | id | Виды спорта (id, slug, name) |
| `country` | alpha2 | Страны (alpha2, alpha3, slug, name). ⚠️ HOT_UPD=0% — upsert не делает HOT update, row-level lock тяжелее. |
| `category` | id | Категории турниров (sport_id FK) |
| `unique_tournament` | id | Стабильная сущность лиги/кубка |
| `tournament` | id | Турниры (unique_tournament_id FK) |
| `season` | id | Сезоны (name, year) |
| `team` | id | Команды (sport_id FK) |
| `player` | id | Игроки (team_id FK) |
| `manager` | id | Тренеры (sport_id FK) |
| `venue` | id | Стадионы |
| `referee` | id | Судьи |
| `image_asset` | id | Изображения (id, md5) — фронт строит img URL сам |

### Raw / Observation таблицы (high write rate)

| Таблица | PK | Назначение | Особенности |
|---|---|---|---|
| `api_payload_snapshot` | id (BIGSERIAL) | Сырые payload каждого API call | append-only, partial unique index `(scope_key, payload_hash)`. ~~13.9M updates~~ |
| `api_request_log` | id (BIGSERIAL) | HTTP лог каждой попытки (incl. retries) | append-only, retention via `delete_request_log_batch` |
| `api_snapshot_head` | scope_key | Latest snapshot per scope (deduplicated) | upsert (`ON CONFLICT (scope_key)`) |
| `endpoint_registry` | pattern | Реестр known endpoints | seeded из endpoints.py |
| `endpoint_contract_registry` | (pattern, source_slug) | Версионирование контрактов | |
| `provider_source` | source_slug | Реестр data providers | — |
| `endpoint_capability_observation` | id (BIGSERIAL) | Append-only observations per fetch | high append rate (~99k/30min) |
| `endpoint_capability_rollup` | (sport_slug, endpoint_pattern) | Aggregate state per (sport, endpoint) | ⚠️ HOT TABLE — 290 rows, 5.5M updates, до firebreak 2026-05-13 был источник deadlock storm |
| `event_endpoint_availability_log` | id | Append-only лог решений | |
| `endpoint_availability_log` | id | Tournament-scope availability log | |

### Normalized event таблицы

| Таблица | PK | Что хранит |
|---|---|---|
| `event` | id | Матчи. ⚠️ 14.2M updates. trigger `set_event_updated_at` |
| `event_status` | code | Справочник статусов (finished, inprogress, ...) |
| `event_score` | (event_id, side) | Счета per side |
| `event_lineup` | (event_id, side) | Составы (formation, player_color) |
| `event_lineup_player` | (event_id, side, player_id) | Игроки в составе |
| `event_lineup_missing_player` | (event_id, side, player_id) | Отсутствующие |
| `event_incident` | (event_id, ordinal) | Goal/yellow/red/sub |
| `event_statistic` | (event_id, period, group_name, stat_name) | Stats per period/group/name |
| `event_player_statistics` | (event_id, player_id) | Stats player в матче |
| `event_player_stat_value` | (event_id, player_id, stat_name) | Detailed per-stat |
| `event_round_info` | event_id | Раунд |
| `event_time` | event_id | Время |
| `event_status_time` | event_id | Status time |
| `event_graph` / `event_graph_point` | id | Momentum graph |
| `event_market` / `event_market_choice` | id | Odds markets |
| `event_team_heatmap` / `event_team_heatmap_point` | id | Team heatmap |
| `event_comment` / `event_comment_feed` | id | Comments |
| `event_pregame_form` | id | Pregame form |
| `event_player_rating_breakdown_action` | id | Rating breakdown actions |
| `event_var_in_progress` | id | VAR state |
| `event_vote_option` | id | Votes |
| `event_winning_odds` | id | Winning odds |
| `event_change_item` | id | Live state changes feed |
| `tennis_point_by_point`, `tennis_power` | id | Tennis-specific |
| `baseball_inning`, `baseball_pitch` | id | Baseball-specific |
| `shotmap_point` | id | Shotmap |
| `esports_game` | id | Esports |

### State таблицы

| Таблица | PK | Назначение |
|---|---|---|
| `event_terminal_state` | event_id | Финальный статус + final_snapshot_id (FK на api_payload_snapshot). Source-of-truth для finished events. |
| `event_live_state_history` | id (BIGSERIAL) | Append-only history (event_id, observed_status_type, scores, period_label, observed_at) |
| `event_endpoint_negative_cache_state` | (event_id, status_phase, endpoint_pattern) | Per-event-phase negative cache. Lease-based concurrency (`probe_lease_until`, `probe_lease_owner`) |
| `endpoint_negative_cache_state` | (unique_tournament_id, [season_id], endpoint_pattern) | Tournament/season scope |

### Ops / Audit таблицы

| Таблица | PK | Назначение |
|---|---|---|
| `etl_job_run` | job_run_id | Audit каждого job execution (status, started/finished, error_class, error_message, parser/normalizer/schema versions) |
| `etl_job_effect` | job_run_id | Эффект job (created_job_count, created_snapshot_count, и т. д.) |
| `etl_job_stage_run` | id (BIGSERIAL) | Stage-level observability (endpoint_pattern, http_status, rows_written, db_time_ms) |
| `etl_replay_log` | replay_id | История replay операций |
| `coverage_ledger` | (source_slug, sport_slug, surface_name, scope_type, scope_id) | Сводка покрытия per surface |
| `etl_planner_cursor` | (planner_name, source_slug, sport_slug, scope_type, scope_id) | Курсоры планировщиков (для historical lanes) |
| `tournament_registry` | (source_slug, sport_slug, unique_tournament_id) | Реестр управляемых турниров |

### Snapshot tables (для leaderboards / standings)

| Таблица | PK | Назначение |
|---|---|---|
| `season_statistics_snapshot` / `season_statistics_result` | id (BIGSERIAL) | Sezon stats (40+ columns per stat) |
| `season_statistics_config` | id | Конфиг типов stat |
| `season_statistics_type` | id | Типы stat |
| `top_player_snapshot` / `top_player_entry` | id (BIGSERIAL) | Top players per (ut, season) |
| `top_team_snapshot` / `top_team_entry` | id (BIGSERIAL) | Top teams |
| `standing` / `standing_row` | id (BIGINT) | Турнирные таблицы (total/home/away) |
| `season_round` / `season_cup_tree` / `season_cup_tree_block` | id | Структура сезона |
| `season_player_of_the_season` | id | POS |
| `team_of_the_week` / `team_of_the_week_player` | id | TOTW |
| `tournament_team_event_snapshot` / `tournament_team_event_bucket` | id | Сводка events команд per tournament |

---

## Hot tables / Known contention

| Таблица | Проблема | Mitigation |
|---|---|---|
| **`endpoint_capability_rollup`** | 290 rows, 5.5M updates → average 19k updates/row. PK `(sport_slug, endpoint_pattern)` — низкая cardinality, high concurrent writes. Был источник deadlock storm 2026-05-13. | **Firebreak 2026-05-13**: `SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED=0` (default OFF) — hot path не пишет в эту таблицу. Rollup пересобирается batch-ем через `python -m schema_inspector.cli rebuild-capability-rollup --lookback-days 1`. |
| **`country`** | 384 rows, HOT_UPD=0% — каждый update создаёт новую row version + изменяет индексы. Любой UPSERT под concurrency = AccessExclusiveLock tuple wait. | UPSERT через `ON CONFLICT (alpha2) DO UPDATE WHERE country.alpha3 IS DISTINCT FROM EXCLUDED.alpha3 OR ...` — minimizes unnecessary updates. |
| `event_endpoint_negative_cache_state` | Unique idx (event_id, status_phase, endpoint_pattern) + lease | Lease-based concurrency (`try_acquire_probe_lease`) — только один worker probe одновременно |
| `api_request_log` | append-only, high rate, no index on started_at | Retention `delete_request_log_batch` с LIMIT |
| `api_payload_snapshot` | append-only, ~13.9M lifetime updates (но HOT_UPD=84%) | Partial unique idx (scope_key, payload_hash) для dedupe; retention для legacy NULL scope_key |
| `player` | 673k rows, 28.9M updates — самая hot data table. HOT_UPD=43% — half rows нуждаются в index updates. | upsert via `ON CONFLICT (id) DO UPDATE` |

### Deadlock-prone комбинации

В наблюдениях 2026-05-13:
- Transaction A: UPDATE `country` (alpha2='EN'), затем UPDATE `unique_tournament` (id=17), затем UPDATE `endpoint_capability_rollup` (key)
- Transaction B: тот же набор в другом порядке → циклическая зависимость → deadlock

PostgreSQL detect deadlock и abort одну транзакцию (`SQLSTATE=40P01`, `DeadlockDetectedError`). Worker_runtime ловит, retry_scheduled.

---

## Recent migrations (last 20)

`D:\sofascore\migrations\`:

| Файл | Что |
|---|---|
| `2026-05-07_d1_endpoint_registry_seed.sql` | Seed endpoint_registry для D1 категории |
| `2026-05-02_update_team_player_statistics_endpoint_registry.sql` | Update registry |
| `2026-05-02_team_player_statistics_seasons.sql` | New table `team_player_statistics_season` |
| `2026-05-02_grant_team_player_statistics_read.sql` | GRANT SELECT |
| `2026-05-01_add_proxy_address_to_api_request_log.sql` | ALTER api_request_log ADD proxy_address |
| `2026-05-01_add_index_event_terminal_state_final_snapshot_id.sql` | CREATE INDEX (FK speedup) |
| `2026-05-01_add_index_api_snapshot_head_latest_snapshot_id.sql` | CREATE INDEX (FK speedup) |
| `2026-05-01_add_fk_index_on_endpoint_capability_observation.sql` | CREATE INDEX (FK speedup) |
| `2026-04-28_event_endpoint_negative_cache.sql` | New table `event_endpoint_negative_cache_state` + log |
| `2026-04-27_api_payload_snapshot_event_root_local_api_idx.sql` | CREATE INDEX (event lookup speedup) |
| `2026-04-24_tournament_registry_policy.sql` | ALTER constraint |
| `2026-04-24_event_updated_at.sql` | ALTER event ADD updated_at + TRIGGER |
| `2026-04-24_event_live_bootstrap.sql` | ALTER event ADD live_bootstrap_done_at |
| `2026-04-24_drop_api_payload_snapshot_event_detail_lookup_idx.sql` | DROP redundant idx |
| `2026-04-24_endpoint_negative_cache.sql` | CREATE TABLE endpoint_negative_cache_state, endpoint_contract_registry |
| `2026-04-23_tournament_registry.sql` | CREATE tournament_registry |
| `2026-04-23_season_structure.sql` | CREATE season_round, season_cup_tree, ... |
| `2026-04-23_event_comment_bigint.sql` | ALTER event_comment.comment_id BIGINT |
| `2026-04-22_planner_cursors.sql` | CREATE etl_planner_cursor |
| `2026-04-22_coverage_ledger_control_plane.sql` | CREATE coverage_ledger, endpoint_contract_registry, provider_source |

Все append-only. Нет migration runner — миграции применяются вручную через `psql -f migrations/<file>.sql`. Тесты подтверждают эффект через `test_*_migration.py` или `test_<area>.py`.

---

## Waterfall на чтение (для API)

```
local_api_server.handle_api_get(path)
   ↓
1. entity root fast path:
      - event_terminal_state.final_snapshot_id → api_payload_snapshot
        ↳ если HIT → отдать raw
      - latest api_payload_snapshot для (endpoint_pattern, entity_type, entity_id)
        ↳ если HIT → отдать raw (с возможным reconcile overlay для events lists)
      - synthesizer from normalized tables
        ↳ event + event_status + event_score + team + tournament + ...
   ↓
2. snapshot search для не-root paths:
      - api_payload_snapshot WHERE endpoint_pattern + source_slug + entity context
      - filter out is_soft_error_payload, http_status >= 400
      - filter out missing-normalized-details (для standings)
      - match by source_url + query
   ↓
3. specialized normalized fallback:
      - _fetch_event_statistics_payload (event_statistic) etc.
      - _fetch_season_events_payload (event + joins)
      - _fetch_standings_payload (standing + standing_row + tie_breaking_rule)
      - ...
   ↓
4. generic fallback:
      - to_jsonb(t) из target_table
   ↓
5. 404
```

---

## Где править

| Хочу | Куда смотреть |
|---|---|
| Добавить новую таблицу | новая migration `migrations/YYYY-MM-DD_<name>.sql` + repository в `storage/` + tests `test_<name>.py` |
| Добавить новый ETL stage | новый job_type в `jobs/types.py`, handler в worker, repository extension |
| Изменить retention policy | `retention_repository.py` + env var `SOFASCORE_RETENTION_*` |
| Reduce hot row contention | как на firebreak 2026-05-13: defer writes, batch rebuild, lease-based concurrency, sort PK keys |
| Investigate slow query | `pg_stat_activity` + `etl_job_stage_run.db_time_ms` + EXPLAIN ANALYZE |
| Backup / restore | UNKNOWN — not covered in repo |

См. также [REDIS_AND_QUEUES.md](REDIS_AND_QUEUES.md) для координационного слоя, [FUNCTION_INDEX.md](FUNCTION_INDEX.md) для repository API details.
