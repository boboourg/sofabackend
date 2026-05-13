# Парсинг и policies / gating

Описывает policy-слой: какие ручки парсятся, в каких режимах, какие endpoints блокируются, как работают negative caches, tier-классификация для football и isEditor HARD BAN.

---

## Policy files overview

| Файл | Назначение | Главные функции | Используется в |
|---|---|---|---|
| `D:\sofascore\schema_inspector\match_center_policy.py` | Football gating: detail_id → tier, isEditor HARD BAN, edge/detail/special allow rules | `football_edge_allowed()`, `football_detail_endpoint_allowed()`, `football_special_allowed()`, `football_detail_tier()`, `filter_football_detail_specs()` | `pipeline/pilot_orchestrator.py` (L475, 481, 517), `detail_resource_policy.py` (L281) |
| `D:\sofascore\schema_inspector\detail_resource_policy.py` | Сборка детальных endpoint specs per (sport, hydration_mode). Delegates football → match_center_policy | `build_event_detail_request_specs()`, `supports_live_detail_resources()` | `pilot_orchestrator.py` (L895, 900, 1086) |
| `D:\sofascore\schema_inspector\live_dispatch_policy.py` | Routing event'а в `tier_1`/`tier_2`/`tier_3` шард | `resolve_live_dispatch_tier()`, `poll_seconds_for_live_dispatch_tier()`, `lease_ms_for_dispatch_tier()`, `normalize_live_dispatch_tier()`, `live_priority_for_dispatch_tier()` | `discovery_worker.py` (L146), `planner_daemon.py` (L149) |
| `D:\sofascore\schema_inspector\live_delta_policy.py` | Sport-aware edge/endpoint manifest для live polling refresh | `live_delta_edge_kinds()`, `live_delta_detail_endpoints()`, `live_delta_detail_endpoint_patterns()` | `detail_resource_policy.py` (L126) |
| `D:\sofascore\schema_inspector\event_endpoint_static_denylist.py` | Hard-coded deny list (sport, endpoint_pattern) известных всегда-404 | `is_static_dead_event_endpoint()` | `detail_resource_policy.py` (L99) |
| `D:\sofascore\schema_inspector\event_endpoint_negative_cache.py` | Event×phase×endpoint negative cache c lease + cooldown | `EventEndpointNegativeCache.decide_event_probe()`, `record_event_outcome()`, `load_event_negative_cache_settings()` | `pilot_orchestrator.py` (gating перед каждым event-scope fetch) |
| `D:\sofascore\schema_inspector\season_widget_negative_cache.py` | Tournament/season widget negative cache | `SeasonWidgetNegativeCache.decide_widget_probe()`, `blocked_endpoint_patterns()`, `load_negative_cache_settings()` | resource refresh planner |

---

## Hydration режимы

Передаются как `hydration_mode` параметр в `PilotOrchestrator.run_event(event_id, sport_slug, hydration_mode="full")`. Defined в `D:\sofascore\schema_inspector\pipeline\pilot_orchestrator.py` (~ L304-320).

| Mode | core_only | lightweight_only | Что забирается | Кто использует |
|---|---|---|---|---|
| `full` | False | False | ROOT + все core edges (incidents/lineups/stats/graph/comments) + все detail endpoints (managers, h2h, pregame, odds, votes, team-streaks, best-players, heatmap, shotmap, average-positions, highlights, official-tweets) + per-player followups (player-statistics, heatmap-per-team, shotmap-player, rating-breakdown) | one-shot CLI `event`, scheduled hydration, finished sweep |
| `core` | True | True | ROOT + 5 core edges only; без details, без per-player | Лёгкая первичная hydration scheduled events |
| `live_delta` | False | True | ROOT + sport-specific edges; для football — fall-through на full detail spec (X4 patch); per-player заблокирован | Live tier-1/2/3 refresh polls |
| `root_only` | True | True | Только ROOT; если статус terminal — финализирует, иначе ставит `edges_pending=True` для отложенного догрева | P0.b tier_1 fast-path |

**`lightweight_only=True`** блокирует per-player followups (player-statistics, heatmap-per-team, shotmap-player, rating-breakdown). При live_delta это always True.

---

## PilotOrchestrator.run_event flow

`D:\sofascore\schema_inspector\pipeline\pilot_orchestrator.py:run_event` (метод, ~700 lines):

```
1. Bootstrap check (L307-318)
   ↳ если hydration_mode="live_delta" + первый раз hydrate
     → promote to "full", set should_mark_live_bootstrap=True

2. ROOT fetch (L328-336)
   ↳ GET /api/v1/event/{event_id}
   ↳ Retry on NETWORK/403/429/CHALLENGE
   ↳ После 3 неудач 404 → MISSING_ROOT_RETIRE (sleep, mark event)

3. ROOT parse → EventRootParser
   ↳ extract: isEditor, detailId, status, has_xg, has_event_player_statistics
   ↳ persist event_terminal_state if status is terminal
   ↳ persist event normalized row + score/time/status_time
   ↳ persist api_payload_snapshot

4. Phase 1: Core edges (L500+)
   ↳ football_edge_allowed(detail_tier, status_type, edge_kind) — gate
   ↳ isEditor=True → HARD BAN (all returns False)
   ↳ /incidents, /lineups — почти всегда allowed для non-editor non-tier_3
   ↳ /statistics, /graph — только для tier_1/tier_2 не-notstarted
   ↳ /comments — football-only
   ↳ для non-football спортов — bypass этот gate

5. Phase 2: Detail endpoints (L850+)
   ↳ build_event_detail_request_specs(sport, hydration_mode, ...)
   ↳ live_delta + football → fall-through (X4 patch 2026-05-13) — берём full spec
   ↳ live_delta + non-football → только live_delta_detail_endpoints (1-2 ручки sport-specific)
   ↳ применяется static denylist, negative cache, isEditor HARD BAN
   ↳ filter_football_detail_specs() per (detail_tier, status_type, has_event_player_heat_map, has_xg)

6. Phase 3: Per-player followups (L980+)
   ↳ только если has_event_player_statistics=True И tier_1/tier_2 И !lightweight_only
   ↳ EventPlayerStatistics, HeatmapPerTeam, ShotmapPlayer, RatingBreakdown

7. Live lane dispatch (L1020)
   ↳ если live_delta + LIVE_SPLIT_DETAILS_FANOUT=1 + не finalized
     → publish JOB_REFRESH_LIVE_EVENT_DETAILS в STREAM_LIVE_DETAILS
   ↳ иначе детали выполняются inline

8. Capability + edges_pending flags
   ↳ если hydration_mode="root_only", set edges_pending=True для follow-up
   ↳ _flush_capabilities (firebreak 2026-05-13: только observations, rollup OFF by default)
   ↳ _record_live_state_history (event_live_state_history)
   ↳ persist etl_job_run + etl_job_stage_run

9. Return EventHydrationReport
```

### run_event_details (split flow)

Реконструирует detail fanout из cached context. Используется `LiveDetailsWorkerService` после P0(a) split.

---

## detail_id ↔ tier mapping (football)

`D:\sofascore\schema_inspector\match_center_policy.py:football_detail_tier(detail_id)`:

| Tier | detail_id | Что разрешено (после X3 + X4 patches) |
|---|---|---|
| **tier_1** | `1` | **Все** edges (`/incidents`, `/lineups`, `/statistics`, `/graph`, `/comments`) + **все** details (`/managers`, `/h2h`, `/pregame-form`, `/odds`, `/votes`, `/team-streaks`, `/best-players/summary`, `/heatmap/{team}`, `/shotmap`, `/average-positions`, `/highlights`, `/official-tweets`) **во всех статусах** |
| **tier_2** | `4`, `6` | Все core edges; `/statistics`, `/graph`, `/comments`, `/average-positions` — только inprogress/finished; `/heatmap` gated by `has_event_player_heat_map`; `/shotmap` gated by `has_xg` |
| **tier_3** | `2`, `3`, `5` | **Problem cohort** — заблокирован от detail endpoints. Только core edges (`/incidents`, `/lineups`). |
| **tier_5** | `None` (missing) | Dominant cohort (93% football events pre-match). После X3 patch — разрешены `_FOOTBALL_NOTSTARTED_DETAIL_ENDPOINTS` (managers, h2h, pregame-form, votes, team-streaks). Premium endpoints заблокированы. |

**X3 patch** (2026-05-12): unlock notstarted detail endpoints для tier_5.
**X4 patch** (2026-05-13): live_delta для football fall-through на full spec (matchcenter теперь работает во время live).

---

## Live dispatch tier resolution

`D:\sofascore\schema_inspector\live_dispatch_policy.py:resolve_live_dispatch_tier()`:

```
1. football → football_detail_tier(detail_id):
   tier_1 (1) → LIVE_TIER_1
   tier_2 (4, 6) → LIVE_TIER_2
   tier_3 / tier_5 (2, 3, 5, missing) → LIVE_TIER_3

2. Не football → по uniqueTournament.userCount:
   userCount >= LIVE_TIER_1_MIN_USER_COUNT (6500) → tier_1
   userCount >= LIVE_TIER_2_MIN_USER_COUNT (1000) → tier_2
   иначе → tier_3

3. Fallback (нет userCount): по tournament_tier:
   <= 1 → tier_1
   <= 3 → tier_2
   else → tier_3

4. Если ничего не подходит → tier_3
```

Каждый tier имеет:
- `LIVE_TIER_<N>_POLL_SECONDS` — cadence planner-tick (default 5/30/90s)
- `LIVE_DISPATCH_LEASE_TIER_<N>_MS` — TTL Redis claim ключа (`live:dispatch_claim:{event_id}`). На проде: tier_1=12000, tier_2=15000, tier_3=60000 (firebreak 2026-05-13).

---

## isEditor HARD BAN (3 layers)

Sofascore разрешает crowdsourced редактирование событий через Editor. Это **низкокачественный** источник — мы баним его полностью.

### Layer 1: Discovery worker (X4 2026-05-13)
**File**: `D:\sofascore\schema_inspector\workers\discovery_worker.py:_normalize_surface_result`, fan-out loop L134-136.
**Поведение**: `_SurfaceEvent.is_editor=True` → skip hydrate enqueue.
**Result**: 31% throughput savings (editor events ~31% от всех).

### Layer 2: Orchestrator HARD BAN
**File**: `pilot_orchestrator.py` L475, 481, 517.
**Поведение**: `football_edge_allowed(is_editor=True)`, `football_special_allowed(is_editor=True)`, `build_event_detail_request_specs(is_editor=True)` все возвращают empty/False.
**Scope**: блокирует ROOT → все edges + все details + per-player.
**Rationale**: 67-event audit показал 100% editor events — amateur crowd. Хранить только ROOT (через discover), но не делать никаких follow-ups.

### Layer 3: Local API list filter (X4)
**File**: `local_api_server.py` L808-810, ~1835.
**Поведение**: `_reconcile_snapshot_payload` для list payloads — drop items с `is_editor_db OR is_editor_raw`.
**SQL**: `_fetch_sport_live_events_payload` имеет `AND e.is_editor IS NOT TRUE`.
**Result**: editor events не попадают в публичный API ответ.

---

## Static denylist

`D:\sofascore\schema_inspector\event_endpoint_static_denylist.py:is_static_dead_event_endpoint(sport, endpoint_pattern)`:

Hard-coded список `(sport, endpoint_pattern)` где **никогда не было 200** в production (например baseball `/innings` — pre-D2 probe showed 100% 404). Применяется в `detail_resource_policy.py:99` как первая проверка перед negative cache.

---

## Negative caches

### Event endpoint negative cache

**File**: `D:\sofascore\schema_inspector\event_endpoint_negative_cache.py`, репозиторий `EventEndpointNegativeCacheRepository`.

**Структура**: `event_endpoint_negative_cache_state` table — PK `(event_id, status_phase, endpoint_pattern)`.

**Логика**:
- Если для (event, phase, endpoint) был 404 / soft_error N раз подряд → `classification='dead'`, blocked
- Lease-based probe: `try_acquire_probe_lease` — только один worker может попробовать оживить
- Cooldown — period до следующей попытки

**Env**:
- `SCHEMA_INSPECTOR_EVENT_NEGATIVE_CACHE_ENABLED` (default false)
- `SCHEMA_INSPECTOR_EVENT_NEGATIVE_CACHE_MODE` (`enforce`/`shadow`)

### Season widget negative cache

**File**: `season_widget_negative_cache.py`, репозиторий `EndpointNegativeCacheRepository`.

**Структура**: `endpoint_negative_cache_state` table — PK `(unique_tournament_id, [season_id], endpoint_pattern)`.

**Используется**: planner перед публикацией `refresh_resource` job для tournament/season-scope endpoints (top-players, standings widgets и т. п.).

**Env**:
- `SCHEMA_INSPECTOR_NEGATIVE_CACHE_ENABLED` (default true)
- `SCHEMA_INSPECTOR_NEGATIVE_CACHE_MODE` (`enforce`/`shadow`/`off`)

---

## live_delta для разных спортов

`D:\sofascore\schema_inspector\live_delta_policy.py:live_delta_detail_endpoints(sport)`:

| Sport | Detail endpoints в live_delta |
|---|---|
| `football` | **Empty** — но `pilot_orchestrator` делает X4 fall-through на full spec |
| `tennis` | `point-by-point`, `tennis-power` |
| `basketball` | empty (только core edges + ROOT) |
| `cricket` | `innings` |
| `baseball` | `innings`, `at-bats` |
| `esports` | `esports-games` |
| прочее | UNKNOWN / минимум |

**X4 patch** (2026-05-13): football intentionally has empty entry; fall-through path в `detail_resource_policy.py:build_event_detail_request_specs` пропускает live_delta gate для football и идёт по full path.

---

## Parsers structure

`D:\sofascore\schema_inspector\parsers\`:

### Structure
- `registry.py` — диспетчер. `classify_snapshot(snapshot) → ParseFamily | None`, потом lookup парсера по `(family, sport)`.
- `families/` — кросс-спортивные парсеры:
  - `event_root.py` — EventRootParser
  - `event_statistics.py` — EventStatisticsParser
  - `event_lineups.py` — EventLineupsParser
  - `event_incidents.py` — EventIncidentsParser
  - `event_managers.py` — EventManagersParser
  - `event_h2h.py`, `event_comments.py`, `event_odds.py`, `event_votes.py`
  - `event_team_heatmap.py`, `event_graphs.py`
  - `season_rounds.py`, `season_cup_trees.py`
  - `entity_profiles.py` — Player/Team/Manager/Referee
- `sports/` — sport-specific adapters (overlay генерацию полей per-sport: football, basketball, ...)
- `special/` — узкие парсеры:
  - `baseball_innings.py`, `baseball_pitches.py`
  - `shotmap.py`, `shotmap_player.py`, `goalkeeper_shotmap.py`
  - `tennis_point_by_point.py`, `tennis_power.py`
  - `esports_games.py`
  - `event_player_rating_breakdown.py`

### Selection
`classify_snapshot(snapshot)` examines:
- `endpoint_pattern` (or URL path)
- payload root keys (`event`, `statistics`, `incidents`, etc.)
- sport context

→ возвращает family + parser. Если не нашёл — `ParseResult(status=UNSUPPORTED)`.

### Тесты
`D:\sofascore\tests\test_*parser*.py` — каждый parser имеет dedicated tests.

---

## Условия блокирующие fetch (cascade)

При hydrate event'а каждый endpoint проходит через цепочку gates:

```
1. detail_resource_policy.build_event_detail_request_specs:
   ↳ is_static_dead_event_endpoint?    → skip
   ↳ EventEndpointNegativeCache.decide_event_probe?  → skip (if dead)
   ↳ football_detail_endpoint_allowed (для football)? → skip
     ↳ isEditor=True → False
     ↳ detail_tier blocked? → False
     ↳ wrong status_phase? → False
   ↳ Otherwise → include

2. FetchExecutor.execute(task):
   ↳ FreshnessStore.is_fresh? → skip (recent fetch already in DB)
   ↳ ResourceNegativeCache (resource_refresh path)? → skip
   ↳ HEAD probe gating (если prefer_head_probe=True)? → если 4xx HEAD → skip GET
   ↳ Otherwise → curl_cffi GET через proxy

3. Fetch результат:
   ↳ classify (success/soft_error/dead/...)
   ↳ EventEndpointNegativeCache.record_event_outcome (update state)
   ↳ если success → snapshot insert + normalize
   ↳ если soft_error → skip persist
```

---

## Где править / расширять

| Хочу | Куда смотреть |
|---|---|
| Добавить новый sport | `LOCAL_API_SUPPORTED_SPORTS` (endpoints.py), `live_delta_policy.py` (если нужны custom edges), parser registry |
| Изменить tier классификацию football | `match_center_policy.py:football_detail_tier()` |
| Изменить tier для других спортов | `live_dispatch_policy.py:LIVE_TIER_*_MIN_USER_COUNT` (env) |
| Добавить endpoint в denylist | `event_endpoint_static_denylist.py:_DENYLIST` |
| Разрешить новый endpoint в live_delta для football | (X4 уже делает full fall-through) — добавь в `detail_resource_policy.py:_FOOTBALL_NOTSTARTED_DETAIL_ENDPOINTS` если хочешь pre-match |
| Изменить gating logic | `match_center_policy.py:football_edge_allowed/detail_endpoint_allowed/special_allowed` |
| Добавить per-event circuit breaker | `D:\sofascore\schema_inspector\workers\hydrate_worker.py` (P5b feature) |
| Новый parser | `parsers/special/<name>.py` + registry update |

Связано: [API_ROUTES.md](API_ROUTES.md), [FUNCTION_INDEX.md](FUNCTION_INDEX.md), [DATABASE_AND_STORAGE.md](DATABASE_AND_STORAGE.md).
