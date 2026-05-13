# Local API routes

Файл: `D:\sofascore\schema_inspector\local_api_server.py` (~4500 строк).
Класс: `LocalApiApplication`.
Запуск через systemd: `sofascore-api.service` → uvicorn :8000, 4 workers.

OpenAPI спецификация генерируется `D:\sofascore\schema_inspector\local_swagger_builder.py:build_openapi_document()`.
Swagger UI доступен по `/`, JSON — `/openapi.json`.

---

## Маршрутизация (waterfall)

`handle_api_get(path, raw_query)` выполняется в порядке:

```
1. match_route(path, self.routes) → RouteSpec
   ↳ если не нашёл → 404 "Route not registered"

2. Special case: SPORT_ALL_EVENT_COUNT
   ↳ _fetch_sport_event_count_payload() (только синтетика)

3. _fetch_entity_root_fast_path(path, raw_query)
   ↳ для /api/v1/event/{id}, /api/v1/team/{id}, /api/v1/player/{id},
     /api/v1/manager/{id}, /api/v1/unique-tournament/{id}
   ↳ root payload waterfall: terminal_state → snapshot → synthesizer

4. _fetch_snapshot_payload(route, path, raw_query, path_params)
   ↳ SELECT FROM api_payload_snapshot WHERE endpoint_pattern=$1
     [AND source_slug=$2] [AND context_entity_type=$3 AND context_entity_id=$4]
   ↳ filter rows: skip soft_error (_snapshot_row_is_soft_error),
     skip partial standings (snapshot_payload_missing_normalized_details)
   ↳ match by source_url path + query equality
   ↳ если найден → _reconcile_snapshot_payload (overlay для events/featuredEvents)

5. _fetch_normalized_payload(route, path, raw_query, path_params)
   ↳ если raw_query непустой → return None (specialized отказывается)
   ↳ sport categories / live events / category seed → fast paths
   ↳ team_statistics_seasons / player_statistics_seasons → специальные
   ↳ entity root fallback (event/team/player/manager/unique-tournament)
   ↳ _fetch_specialized_normalized_payload (template dispatch — см. ниже)
   ↳ _fetch_generic_normalized_payload (generic table dump)

6. Если всё ещё None → 404 или {"events": []} для surface-routes
```

---

## Все specialized handlers (`_fetch_specialized_normalized_payload`)

| Template path | Handler | Источник ответа |
|---|---|---|
| `/api/v1/event/{event_id}/statistics` | `_fetch_event_statistics_payload(event_id)` | `event_statistic` table |
| `/api/v1/event/{event_id}/incidents` | `_fetch_event_incidents_payload(event_id)` | `event_incident` |
| `/api/v1/event/{event_id}/lineups` | `_fetch_event_lineups_payload(event_id)` | `event_lineup`, `event_lineup_player`, `event_lineup_missing_player` |
| `/api/v1/event/{event_id}/comments` | `_fetch_event_comments_payload(event_id)` | `event_comment`, `event_comment_feed` |
| `/api/v1/event/{event_id}/h2h` | `_fetch_event_h2h_payload(event_id)` | snapshot waterfall |
| `/api/v1/event/{event_id}/managers` | `_fetch_event_managers_payload(event_id)` | `event_manager_assignment`, `manager` |
| `/api/v1/event/{event_id}/pregame-form` | `_fetch_event_pregame_form_payload(event_id)` | `event_pregame_form` |
| `/api/v1/event/{event_id}/player/{player_id}/statistics` | `_fetch_event_player_statistics_payload(event_id, player_id)` | `event_player_statistics`, `event_player_stat_value` |
| `/api/v1/event/{event_id}/player/{player_id}/rating-breakdown` | `_fetch_event_player_rating_breakdown_payload(event_id, player_id)` | `event_player_rating_breakdown_action` |
| `/api/v1/event/{event_id}/heatmap/{team_id}` | `_fetch_event_team_heatmap_payload(event_id, team_id)` | `event_team_heatmap`, `event_team_heatmap_point` |
| `/api/v1/unique-tournament/{ut_id}/season/{s_id}/events/last/{page}` | `_fetch_season_events_payload(direction="last")` | `event` + joins |
| `/api/v1/unique-tournament/{ut_id}/season/{s_id}/events/next/{page}` | `_fetch_season_events_payload(direction="next")` | `event` + joins |
| `/api/v1/unique-tournament/{ut_id}/season/{s_id}/standings/{scope}` | `_fetch_standings_payload(...)` | `standing`, `standing_row`, `tie_breaking_rule` |
| `/api/v1/tournament/{t_id}/season/{s_id}/standings/{scope}` | `_fetch_standings_payload(...)` | то же |
| `/api/v1/unique-tournament/{ut_id}/season/{s_id}/rounds` | `_fetch_season_rounds_payload(...)` | `season_round` + snapshot waterfall |
| `/api/v1/unique-tournament/{ut_id}/media` | `_fetch_unique_tournament_media_payload(ut_id)` | `api_payload_snapshot` (/event/{id}/highlights aggregated), новый synthetic endpoint (2026-05-13) |
| `/api/v1/player/{player_id}/transfer-history` | `_fetch_player_transfer_history_payload(player_id)` | `player_transfer_history` |
| `/api/v1/player/{player_id}/statistics` | `_fetch_player_statistics_payload(player_id)` | snapshot waterfall |
| `/api/v1/player/{player_id}/events/last/{page}` | `_fetch_player_events_last_payload(player_id, page)` | `event` filtered |
| `/api/v1/team/{team_id}/players` | `_fetch_team_players_payload(team_id)` | snapshot (raw passthrough) |
| `/api/v1/team/{team_id}/featured-players` | `_fetch_team_featured_players_payload(team_id)` | snapshot (raw passthrough) |
| `/api/v1/team/{team_id}/transfers` | `_fetch_team_transfers_payload(team_id)` | snapshot |
| `/api/v1/player/{player_id}/attribute-overviews` | `_fetch_player_attribute_overviews_payload(player_id)` | snapshot |
| top_player / top_team endpoints (множество) | `_fetch_top_player_payload(route, path_params)` / `_fetch_top_team_payload(...)` | `top_player_snapshot`, `top_player_entry`, `top_team_snapshot`, `top_team_entry` |

---

## Entity root fast path (`_fetch_entity_root_fast_path`)

Для path вида `/api/v1/<entity>/{id}` (без суффикса) — оптимизированный путь:

| Path | Handler | Waterfall |
|---|---|---|
| `/api/v1/event/{id}` | `_fetch_event_root_payload(id)` | 1) `event_terminal_state.final_snapshot_id` (если терминал) → 2) latest snapshot для event → 3) synthesizer из `event` + joins |
| `/api/v1/team/{id}` | `_fetch_team_root_payload(id)` | snapshot → synthesizer из `team` |
| `/api/v1/player/{id}` | `_fetch_player_root_payload(id)` | snapshot → synthesizer из `player` |
| `/api/v1/manager/{id}` | `_fetch_manager_root_payload(id)` | snapshot → synthesizer из `manager` |
| `/api/v1/unique-tournament/{id}` | `_fetch_unique_tournament_root_payload(id)` | snapshot → synthesizer из `unique_tournament` |

---

## Surface lists (sport-scoped)

| Path | Handler | Source |
|---|---|---|
| `/api/v1/sport/{slug}/events/live` | `_fetch_sport_live_events_payload(slug)` | snapshot fast path + reconcile overlay + `is_editor IS NOT TRUE` filter (X4) |
| `/api/v1/sport/{slug}/scheduled-events/{date}` | `_fetch_scheduled_sport_events_payload(slug, date)` | snapshot waterfall |
| `/api/v1/sport/{slug}/categories` | `_fetch_sport_categories_payload(slug)` | `category` table + snapshot |
| `/api/v1/sport/{slug}/{date}/{tz}/categories` | category seed | `category_daily_summary`, `category_daily_unique_tournament`, `category_daily_team` |
| `/api/v1/unique-tournament/{ut_id}/scheduled-events/{date}` | snapshot + reconcile overlay | `event` + joins |
| `/api/v1/unique-tournament/{ut_id}/featured-events` | snapshot raw passthrough | `api_payload_snapshot` |
| `/api/v1/unique-tournament/{ut_id}/season/{s_id}/events/round/{n}` | snapshot + normalized round filter | `event`, `event_round_info` |
| `/api/v1/sport/0/event-count` | `_fetch_sport_event_count_payload()` | synthetic count по `event` + `event_status` |
| `/api/v1/category/{cat_id}/unique-tournaments` | snapshot waterfall | discovery feed |

---

## isEditor filter (X4 layer 3)

`local_api_server.py` line ~808-810 и ~1835:
- `_reconcile_snapshot_payload`: для events/featuredEvents списков — drop items где `is_editor_db OR is_editor_raw` (`e.is_editor IS NOT TRUE` keeps NULL).
- `_fetch_sport_live_events_payload`: SQL фильтр `AND e.is_editor IS NOT TRUE`.

Это блокирует крауд-источник Sofascore Editor от публичного API, даже если snapshot их содержит.

---

## Response cache

`_response_cache` — in-memory TTL cache (max ~256 entries), ключ = `(path, normalized_query)`.

TTL вычисляется `_response_ttl_seconds(route, payload)`:

| Условие | TTL |
|---|---|
| `/events/live` (любой сurfaces) | 2s |
| Event с не-terminal статусом | 2s |
| `/api/v1/event/...` (default) | 2s |
| Прочее (стат, scheduled, etc.) | 30s |
| Если status_type natural terminal (finished/cancelled/...) | 30s |

Cache invalidate при write — нет (read-only API).

---

## Ops routes (`/ops/*`)

Файл: `local_api_server.py:handle_ops_get`. Все read-only.

| Path | Handler | Returns |
|---|---|---|
| `/ops/health` | `_fetch_ops_health_payload()` | DB+Redis ok, snapshot count, capability rollup count, live lane sizes, drift_summary, coverage_summary, reconcile_policy_summary |
| `/ops/snapshots/summary` | `_fetch_ops_snapshots_summary_payload()` | per-endpoint snapshot counts |
| `/ops/queues/summary` | `_fetch_ops_queue_summary_payload()` | Redis stream lengths + lag per consumer group + `live_dispatch_metrics` + lane counts |
| `/ops/jobs/runs` | `_fetch_ops_job_runs_payload(limit)` | recent `etl_job_run` (default limit=20, max=200) |
| `/ops/coverage/summary` | `_fetch_ops_coverage_summary_payload()` | aggregated `coverage_ledger` rollup |

**Известное**: `/ops/health` иногда возвращает 500 (TimeoutError) при high DB load — endpoint делает много queries. Pre-existing, не firebreak.

---

## OpenAPI / Swagger

- `/openapi.json` — генерируется `build_openapi_document()` из endpoint registry. Кешируется на диске (`.cache/local_api/`) + in-memory variants per base URL.
- `/docs` или `/` — Swagger UI (HTML viewer).
- Все registered endpoints (через `local_api_endpoints()` в `endpoints.py`) автоматически попадают в OpenAPI paths.

---

## Возможные 404 / empty cases

| Сценарий | Поведение |
|---|---|
| Route не зарегистрирован | 404 `{"error": "Route is not registered..."}` |
| Route есть, но нет snapshot и нет normalized data | 404 `{"error": "Requested path exists but no ingested payload matched"}` |
| Surface list (events/live), но БД пуста | 200 `{"events": []}` |
| Entity root (event/team/player) — нет ни snapshot ни normalized row | 404 |
| `/api/v1/unique-tournament/{id}/media` для unknown UT | 200 `{"media": []}` (NEVER 404, см. `_fetch_unique_tournament_media_payload`) |

---

## Связь endpoint registry ↔ upstream Sofascore

`D:\sofascore\schema_inspector\endpoints.py`:

```python
@dataclass(frozen=True)
class SofascoreEndpoint:
    path_template: str          # /api/v1/event/{event_id}/lineups
    envelope_key: str           # "home,away" (CSV если multiple)
    target_table: str | None    # "event_lineup" or "api_payload_snapshot"
    query_template: str | None  # для paginated endpoints
    notes: str | None
    refresh_interval_seconds: int | None  # opt-in для resource refresh loop
    refresh_priority: int = 50
    prefer_head_probe: bool = False  # P0.2 HEAD probe gating
    # ... pagination, empty_predicate, etc.
```

`local_api_endpoints()` собирает endpoints из:
- `EVENT_DETAIL_BASE_ENDPOINTS` + `EVENT_DETAIL_DEPRECATED_ENDPOINTS`
- `EVENT_LIST_ENDPOINTS` (per sport)
- `ENTITIES_ENDPOINTS`
- `STATISTICS_ENDPOINTS`, `STANDINGS_ENDPOINTS`
- `LEADERBOARDS_ENDPOINTS`
- `COMPETITION_ENDPOINTS` (включая `UNIQUE_TOURNAMENT_MEDIA_ENDPOINT`)
- `CATEGORIES_SEED_ENDPOINTS`
- Sport-specific generators (sport_live_events_endpoint, sport_scheduled_events_endpoint, и т. д.)

Total ~280-320 endpoints в registry (динамически, зависит от количества supported sports).

Upstream URL = `SOFASCORE_BASE_URL + path_template.format(**path_params)`, где `SOFASCORE_BASE_URL = "https://www.sofascore.com"`.

---

## Известные нюансы

1. **Raw snapshot is canonical** — `local_api_server.py` всегда сначала пытается отдать snapshot, потом synthesizer. Причина: upstream payload содержит fieldTranslations, userCount, teamColors, priority, coverage, eventState, changes — которые synthesizer не воспроизведёт.

2. **`event_terminal_state.zombie_stale`** — sentinel statuc от housekeeping для events с stale live polling. Read layer **не** использует его для status overwrite или drop. См. top-of-file комментарий в local_api_server.py.

3. **Category snapshot scope routing** — `/api/v1/category/{cat_id}/...` резолвит snapshot по category context (не event/tournament). См. ` _parse_context_value` / route.context_entity_type.

4. **Specialized vs generic fallback** — `_fetch_specialized_normalized_payload` имеет priority. Generic fallback (`_fetch_generic_normalized_payload`) — это `to_jsonb(t)` любой строки `target_table`. Для surface-routes (`/featured-events`) generic давал кривые payload в прошлом — отсюда жёсткие specialized handlers.

5. **`/api/v1/unique-tournament/{id}/media`** — синтетический endpoint (введён 2026-05-13). Aggregates `/event/{id}/highlights` snapshots для всех events tournament-а. DB-only, никогда не fetching upstream. См. `_fetch_unique_tournament_media_payload`.

---

## Где править / расширять

- **Новый endpoint** → добавь в `endpoints.py` (SofascoreEndpoint constant + в нужный group tuple), затем в `local_swagger_builder.py:_build_core_paths` (если хочешь Swagger entry), затем (если нужен специальный response) в `_fetch_specialized_normalized_payload` + handler.
- **Изменение response shape** — отредактируй handler + `_serialize_*` helpers (например `_serialize_season_event_row`, `_minimal_entity_payload`).
- **Новый source** для waterfall — расширь `_fetch_event_root_payload` или конкретный specialized handler.
- **Тесты** — `D:\sofascore\tests\test_local_api_server.py` (фейк-asyncpg connection через `_FakeSnapshotConnection`, `_FakeSeasonEventsConnection`, etc.).

Связано: [DATABASE_AND_STORAGE.md](DATABASE_AND_STORAGE.md), [FUNCTION_INDEX.md](FUNCTION_INDEX.md), [PARSING_AND_POLICIES.md](PARSING_AND_POLICIES.md).
