# Pipeline Tree — что и как мы парсим (сверху вниз)

Last refreshed from code: **2026-05-16**.

Этот документ объясняет **всё дерево парсинга** от корня (список спортов) до листьев
(статистика игрока в матче). Для каждого узла указано:
* **что** парсится (endpoint URL)
* **кто** ставит в очередь (planner / file)
* **через какой stream** проходит (Redis Streams)
* **какой worker** обрабатывает (systemd unit)
* **куда складывается** результат (PG table)
* **как часто** ребрётся

Если узлы выглядят похожими — они и есть похожими; pipeline честно повторяет одну
и ту же модель **planner → stream → worker → parser → DB** на каждой высоте.

---

## Корневая модель (один абзац)

У нас **6 planner daemons**, каждый пишет в свой stream. **N worker pool'ов**, каждый
читает свой stream. **1 parser registry** на 25 парсеров. **~30 нормализованных таблиц**.

```
planners (6 шт., systemd)
  └── publish JOB envelope → stream:etl:* (Redis Stream)
        └── workers (consumer groups, systemd@N) — XREADGROUP
              └── orchestrator → fetcher → parser → normalize → PostgreSQL
```

Все интервалы тиков — env-driven (`SOFASCORE_*_INTERVAL_SECONDS`). Все streams
имеют backpressure-watch и retry с задержкой (`stream:etl:delayed`).

---

# RUT 1 — Sport list (корень дерева)

## Откуда берётся

**Хардкод в `schema_inspector/sport_profiles.py`:**

```python
LOCAL_API_SUPPORTED_SPORTS = (
    "football", "basketball", "tennis", "baseball", "ice-hockey",
    "handball", "volleyball", "rugby", "american-football",
    "table-tennis", "esports", "futsal", "cricket",
)
```

13 спортов. Sofascore поддерживает больше, но мы парсим только эти.

**НЕ парсится** — это конфиг, не данные.

## Что определяет

Каждый planner проходит по этому списку (либо по подмножеству, ограниченному
`--sport-slug` flag на CLI / `SCHEMA_INSPECTOR_SERVICE_SPORT_SLUGS`).

---

# RUT 2 — Категории (страны / регионы) + турниры

## Какие endpoints

| Endpoint | Что отдаёт |
|---|---|
| `/api/v1/sport/{slug}/categories` | Полный список категорий (стран, регионов) для спорта |
| `/api/v1/sport/{slug}/categories/all` | То же, расширенная версия |
| `/api/v1/category/{cid}/unique-tournaments` | Список **uniqueTournament** в категории. Premier League, La Liga, etc. |

## Кто ставит в очередь

**`StructurePlannerDaemon`** (`schema_inspector/services/structure_planner.py`):

```
Stream: stream:etl:structure_sync
Worker: sofascore-structure-sync.service (1 instance)
Tick:   30s по умолчанию (SCHEMA_INSPECTOR_STRUCTURE_LOOP_INTERVAL_SECONDS)
```

Daemon принимает на вход **список managed UT ids** (`StructurePlanningTarget`),
который собирается из (в порядке приоритета):

1. **`tournament_registry` table** — главный источник truth для prod. Здесь хранится
   {sport, unique_tournament_id, refresh_interval_seconds, active, current_enabled}.
2. `SCHEMA_INSPECTOR_STRUCTURE_MANAGED_TOURNAMENTS` env JSON (fallback).
3. `sport_profiles.py` → `SportProfile.managed_unique_tournament_ids` (hardcoded default).

Для каждой UT планировщик публикует job `JOB_SYNC_TOURNAMENT_STRUCTURE` в
`stream:etl:structure_sync` **раз в `refresh_interval_seconds`** (typically 24h для топ
турниров, 168h для второстепенных).

## Что воркер делает

`StructureSyncWorker` → `run_structure_sync_for_tournament()` в
`services/structure_sync_service.py`:

**Phase 1** — фетчит `/unique-tournament/{ut_id}` + `/unique-tournament/{ut_id}/seasons`
получает список сезонов.

**Phase 2** — для каждого сезона:
- `/unique-tournament/{ut_id}/season/{sid}/rounds` (если режим `rounds`)
- `/unique-tournament/{ut_id}/season/{sid}/cuptrees` (если режим `bracket`)
- `/unique-tournament/{ut_id}/season/{sid}/events/last/{page}` (paginated history)
- `/unique-tournament/{ut_id}/season/{sid}/events/next/{page}` (upcoming)

**Парсеры:**
- `parsers/families/season_rounds.py` → `season_round` table
- `parsers/families/season_cuptrees.py` → `season_cup_tree*` tables
- `parsers/families/event_root.py` (для events в paginated history) → `event` table

## Что с категориями

`structure_planner` **не** фетчит `/sport/{slug}/categories` — это делает
`StructureSyncService` для конкретного UT когда нужно резолвить parent category.

Категории также фетчатся **по требованию** через CLI:
```bash
python -m schema_inspector.cli sport-categories --sport-slug football
```
И через **TournamentRegistryRefreshDaemon** (`sofascore-tournament-registry-refresh.service`)
— раз в 24h обновляет `tournament_registry` table, заодно подгружает категории.

## Coverage tracking

Состояние "что мы знаем про UT" сидит в:

| Table | Что |
|---|---|
| `tournament_registry` | active / current_enabled / refresh_interval_seconds per UT |
| `unique_tournament` | один row per UT, name/slug/has_relegation/etc. |
| `unique_tournament_season` | season ids per UT |
| `season` | season metadata (year, name, has_standings) |
| `category` | страна/регион (England, Spain, etc.) с priority |
| `coverage_ledger` | high-level состояние "что покрыто" |
| `mv_season_coverage` | materialized view — % coverage per season |

---

# RUT 3 — История турниров (исторические события сезонов)

Это самая **сложная** ветка — она забирает все события для всех сезонов "managed" UT'ов,
плюс энрчмент (player statistics, standings, etc.).

## Кто ставит в очередь

Тут участвуют **3 разных planner** для разных уровней:

### 3.1 `HistoricalPlannerDaemon` — события по датам

```
File:   services/historical_planner.py
Stream: stream:etl:historical_discovery
Worker: sofascore-historical-discovery@N.service
Tick:   5s (loop_interval), N дат на тик (dates_per_tick=1 default)
```

Шагает по календарю **назад от сегодня** или в diapason `--date-from`/`--date-to`.
Для каждой даты публикует `JOB_DISCOVER_SPORT_SURFACE scope=historical date=YYYY-MM-DD`.

**Discovery worker** фетчит `/sport/{slug}/scheduled-events/{date}` (или
`/sport/{slug}/{date}/{tz_offset}/categories`) — список всех матчей в этот день. Для
каждого event публикует child `JOB_HYDRATE_EVENT_ROOT` в `stream:etl:historical_hydrate`.

**Historical hydrate worker** (`sofascore-historical-hydrate@N.service`) делает full
hydration: root + lineups + incidents + statistics + comments + odds.

### 3.2 `HistoricalTournamentPlannerDaemon` — события по UT

```
File:   services/historical_tournament_planner.py
Stream: stream:etl:historical_tournament
Worker: sofascore-historical-tournament@N.service
Tick:   60s, tournaments_per_tick=10
```

Берёт UT id из cursor (`select_unique_tournament_ids_after_cursor` в cli.py — **A1 Phase 0
priority-aware order**: World > Europe > England > ... > Chile amateur). Публикует
`JOB_SYNC_TOURNAMENT_ARCHIVE`.

Worker через `historical_archive_service` фетчит **полный сезон** UT (events/last/{page},
events/next/{page}, standings, statistics_info) и нормализует.

### 3.3 `HistoricalEnrichmentWorker` — добор недостающего

```
Stream: stream:etl:historical_enrichment
Worker: sofascore-historical-enrichment@N.service
```

Job'ы публикуются:
- из `historical_tournament_planner` (для player season-statistics enrichment)
- из CLI команды `python -m schema_inspector.cli statistics-backfill`

Фетчит `/unique-tournament/{ut_id}/season/{sid}/top-players/overall`,
`/top-teams/overall`, `/top-players-per-game/all/{seasonType}` etc.

Парсеры:
- `parsers/families/season_standings.py` (placeholder! см. `standings_parser.py` верхний уровень)
- `statistics_parser.py` верхний уровень для top-players/top-teams снапшотов

## Coverage

- `tournament_registry.current_enabled = true` — означает "UT в активном backfill".
- `coverage_ledger` row per (sport, ut, season) — статус.
- `mv_season_coverage` — `% events_hydrated / total_events`.

## Backpressure

Все три historical lane используют `defer_on_backpressure=True` — если operational
hydrate lane перегружена, historical paused. На проде сейчас historical-tournament +
historical-enrichment **отключены** (Бобур ранее ACK'нул "live-first beats historical").

---

# RUT 4 — Матчи (scheduled / live / finished)

Это **главный hot path**. Сложнее всех других, поэтому распишу подробно.

## 4.1 SCHEDULED (ещё не начались) — `planner-daemon`

```
File:   services/planner_daemon.py
Stream: stream:etl:discovery
Worker: sofascore-discovery@N.service
Tick:   3600s (раз в час) — scheduled_interval_seconds
```

Раз в час publish'ит `JOB_DISCOVER_SPORT_SURFACE scope=scheduled date=today` для каждого
**живого** sport.

**Discovery worker** (`workers/discovery_worker.py`):
1. Фетчит `/sport/{slug}/scheduled-events/{today}` — список всех событий дня.
2. Парсит payload через `EventRootParser` → upsert в `event` table.
3. Для каждого event публикует child `JOB_HYDRATE_EVENT_ROOT scope=scheduled` в
   `stream:etl:hydrate` с `params.hydration_mode = "core"`.

**Hydrate worker** (`sofascore-hydrate@N.service`):
- Идёт через `PilotOrchestrator.run_event(hydration_mode="core")`.
- Fetch **только** root: `/event/{id}`.
- Для football scheduled (notstarted): дополнительно `/lineups`, `/managers`, `/h2h`,
  `/pregame-form`, `/votes`, `/odds/*` (если whitelist для NOT_STARTED — см.
  `match_center_policy.py` и `football-matrix.md §5`).

**Что не парсится для scheduled:** `/incidents`, `/statistics`, `/graph`, `/comments`
(не существуют до начала матча).

## 4.2 LIVE (играют сейчас) — `live-discovery-planner-daemon`

```
File:   services/live_discovery_planner.py
Stream: stream:etl:live_discovery
Worker: sofascore-live-discovery@N.service
Tick:   5s (loop_interval_seconds)
```

**Каждые 5 секунд** publish'ит `JOB_DISCOVER_SPORT_SURFACE scope=live` для всех живых
sport.

**Live discovery worker** (тот же `workers/discovery_worker.py`, scope=live):
1. Фетчит `/sport/{slug}/events/live` — список текущих live events.
2. Парсит → upsert в `event` table (если event новый — впервые видим).
3. Для каждого event:
   * `LiveTierOverrideRegistry.get(unique_tournament_id)` — если есть override → используем.
   * Иначе `resolve_live_dispatch_tier()` по `detail_id` / `tournament_user_count` /
     `tournament_tier` → `tier_1` / `tier_2` / `tier_3`.
   * Publish child `JOB_HYDRATE_EVENT_ROOT scope=live` в **соответствующий** stream:
     `stream:etl:live_tier_{1,2,3}`.

```
tier_1 → 5s poll cadence  — Premier League, La Liga, UCL
tier_2 → 15s poll          — Cup competitions, второстепенные лиги
tier_3 → 30s poll          — всё остальное (98.5% events!)
```

## 4.3 Live worker — что именно парсим во время матча

```
Files:   workers/live_worker_service.py + pipeline/pilot_orchestrator.py
Streams: stream:etl:live_tier_{1,2,3}, stream:etl:live_warm
Workers: sofascore-live-tier-{1,2,3}@N.service, sofascore-live-warm@N.service
```

**Каждый poll cycle** (5s/15s/30s в зависимости от тира):

### 4.3.1 ROOT — обязательно
```
/api/v1/event/{event_id}  → parsers/families/event_root.py
                          → event, event_score, event_status, event_time,
                            event_round_info, event_status_time
```

### 4.3.2 EDGES — fanout через `LiveEdgesThrottle` (60s per event)
```
/api/v1/event/{event_id}/lineups       → event_lineup, event_lineup_player,
                                         event_lineup_missing_player
/api/v1/event/{event_id}/incidents     → event_incident
/api/v1/event/{event_id}/statistics    → event_statistic
/api/v1/event/{event_id}/graph         → event_graph, event_graph_point
```

### 4.3.3 DETAILS — fanout через `LiveDetailsThrottle` (30s per event)
```
/api/v1/event/{event_id}/comments              → event_comment, event_comment_feed
/api/v1/event/{event_id}/shotmap               → shotmap_point  (если has_xg=true)
/api/v1/event/{event_id}/heatmap/{team_id}     → event_team_heatmap, event_team_heatmap_point
/api/v1/event/{event_id}/best-players/summary  → event_best_player_entry
/api/v1/event/{event_id}/average-positions     → (нет parser — raw passthrough)
/api/v1/event/{event_id}/official-tweets       → (нет parser — raw passthrough)
/api/v1/event/{event_id}/team-streaks          → (нет parser — raw passthrough)
/api/v1/event/{event_id}/managers              → event_manager_assignment
/api/v1/event/{event_id}/h2h                   → event_duel
/api/v1/event/{event_id}/h2h/events            → (raw passthrough в api_payload_snapshot)
/api/v1/event/{event_id}/pregame-form          → event_pregame_form*
/api/v1/event/{event_id}/votes                 → event_vote_option
/api/v1/event/{event_id}/odds/{prov}/all       → event_market, event_market_choice
/api/v1/event/{event_id}/odds/{prov}/featured  → event_market, event_market_choice
/api/v1/event/{event_id}/provider/{prov}/winning-odds → event_winning_odds
```

### 4.3.4 PER-PLAYER FAN-OUT — для каждого player из lineups
```
/api/v1/event/{event_id}/player/{player_id}/statistics       → event_player_statistics,
                                                                event_player_stat_value
/api/v1/event/{event_id}/player/{player_id}/rating-breakdown → event_player_rating_breakdown_action
/api/v1/event/{event_id}/player/{player_id}/heatmap          → (raw passthrough)
/api/v1/event/{event_id}/shotmap/player/{player_id}          → (raw passthrough)
/api/v1/event/{event_id}/goalkeeper-shotmap/player/{player_id} → (raw passthrough)
```

**Гейты** (что НЕ фетчим):
1. `match_center_policy.py` whitelist — определяет какие endpoint × status × tier разрешены.
   * Tier_3 events почти не имеют sub-endpoints (только root + incidents).
   * `is_editor=true` events → **HARD BAN** на всё кроме root.
2. `event_endpoint_negative_cache_state` — если endpoint 4 раза вернул 404 → suppress на
   30 мин, после 5-го — на 2 часа (см. B1.5 / negative cache fix).
3. `endpoint_ttl_policy.py` (B1) — per-endpoint × per-status TTL (`/comments` live=60s,
   `/official-tweets` live=20×60s, etc.). Если key fresh в FreshnessStore — skip.

## 4.4 FINISHED (после матча)

Когда `event.status_type` переходит в `finished / afterextra / afterpen`:

1. Live worker emit'ит `JOB_FINALIZE_EVENT` → `event_terminal_state` запись.
2. Event удаляется из `zset:live:hot / warm / cold`.
3. Один последний refresh (через `final_sweep_gate`) — добор того что упустили (incidents
   до 90+стопп. минуты, итоговая статистика).
4. После 2.5h после `finished_at` — фетч `/highlights` (если `hasGlobalHighlights=true`).
5. В дальнейшем — обновление через `resource_planner` (см. RUT 5) если что-то нужно
   догнать.

## Текущая стратегия (Бобур ACK на 2026-05-14)

- **Live-first beats historical**. Historical-tournament и historical-enrichment лейны
  можно отключать (`sudo systemctl stop`) когда live перегружен.
- **Tier_3 = root only** (P0(b) rollout) если `LIVE_TIER_1_ROOT_ONLY=1` — экономия 80%
  fan-out на проблемных tier_3 events.

---

# RUT 5 — Entity history (players, teams, managers, venue)

Это **самая разветвлённая** ветка. Тут участвуют **2 разных pipeline**:

## 5.1 Inline extraction из event payloads

Парсеры event'ов **попутно** извлекают entity rows:

| Откуда | Парсер | Извлекает |
|---|---|---|
| `/event/{id}` root | `EventRootParser` | `event.home_team_id` / `away_team_id` → `team` rows; venue в `event_round_info` |
| `/event/{id}/lineups` | `EventLineupsParser` | `event_lineup_player.player_id` → `player` rows |
| `/event/{id}/incidents` | `EventIncidentsParser` | scorer, assist player ids → `player` rows |
| `/event/{id}/managers` | `EventManagersParser` | home/away manager → `manager` rows + `manager_team_membership` |

**Что записывается:** только id + name + slug. **Никаких профилей** (high-res photo,
height/weight, country, etc.). Это даёт **fast inline** ingest без отдельного fetch.

⚠️ **Проблема:** если player'у нужен `team_id`, а команда ещё не была загружена —
парсер ставит `team_id = NULL` и WARN'ит в лог (`Nullified missing FK during normalized
persist`). За 24h таких ~4 раза — мало, но накапливается.

## 5.2 Lazy hydration через `ResourcePlannerDaemon`

Для **полного профиля** (player с фото, контракт, history) — отдельный planner.

```
File:   services/resource_planner.py
Stream: stream:etl:resource_refresh
Worker: sofascore-resource-refresh@N.service
Tick:   30s (tick_interval_seconds)
```

**Какие endpoints он гонит:**

`ResourcePlannerDaemon` берёт ВСЕ `SofascoreEndpoint` объявленные с
`refresh_interval_seconds != None` (это opt-in). Для каждого scope-резолвер (см.
`services/resource_scope/`) выдаёт список target'ов (player_ids, team_ids etc.).

Полный список scope резолверов:
```
schema_inspector/services/resource_scope/
├── custom_id_of_managed_events.py        — custom_id для h2h/events
├── custom_id_of_registry_events.py       — custom_id для регистровых турниров
├── event_of_finished_baseball.py         — events для baseball at-bats fan-out
├── managed.py                            — base, для tournament_registry-managed
├── managed_football_pairs.py             — team-pairs для team-streaks fetches
├── period_of_managed_pairs.py
├── period_of_registry_football.py
├── player_of_active_squad.py             — players активных команд (для season stats)
├── player_of_active_squad_first_page.py
├── player_of_national_team_history.py    — transfer history для national team players
├── round_of_managed_pairs.py
├── round_of_registry_football.py
├── season_of_active_ut.py
├── season_of_registry_ut.py
├── team_of_active_ut.py                  — teams активных UT'ов
├── team_of_active_ut_first_page.py
├── team_of_active_ut_season.py
└── team_of_registry_ut.py                — teams регистровых UT'ов
```

Каждый scope resolver выдаёт `ResourceTarget` — для каждой комбинации (resource, target)
сохраняется **cursor** в Redis (`ResourceCursorStore`), и планировщик публикует job
только если cursor старше `refresh_interval_seconds` для этого endpoint.

**Resource refresh worker** (`workers/resource_refresh_worker.py`):
- Фетчит endpoint.
- Парсит через ParserRegistry (если есть parser) или сохраняет raw в `api_payload_snapshot`.
- Обновляет cursor.

## 5.3 Что в реальности на проде

Из `etl_job_run` за 24h:
- `refresh_resource: 409 808 succeeded` — **больше всего job'ов всех типов**! Это
  refresh-pipeline активно работает.
- `hydrate_event_root: 251 609` — live + scheduled.
- `refresh_live_event: 272 517` — live edges fan-out.

## 5.4 Coverage entity history

| Table | Lifetime rows | Что покрыто |
|---|---:|---|
| `player` | ~миллионы (зависит от sport scope) | inline-извлечённые + profile-fetched |
| `team` | сотни тысяч | inline из event root + profile через resource_planner |
| `manager` | десятки тысяч | inline из event/managers |
| `manager_performance` | history записей | history через `manager/{id}` enrichment |
| `manager_team_membership` | history | как у manager |
| `venue` | (если хранится) | inline из event_round_info |
| `player_season_statistics` | сезонные snapshots | через `statistics_backfill` CLI |
| `player_transfer_history` | трансфер истории | через `resource_planner` |
| `team_of_the_week`, `team_of_the_week_player` | weekly snapshots | через resource_planner / season widgets |

## 5.5 Что определяет приоритет

**ResourcePlannerDaemon** не знает приоритетов — он round-robin'ит endpoints по resolver'ам.
Реальный приоритет задаётся:

1. **`tournament_registry.active`** + **`current_enabled`** — какие UT в принципе на радаре.
2. **`category.priority`** (Sofascore-issued) — для `select_unique_tournament_ids_after_cursor`
   в `cli.py` (A1 Phase 0 — World > Europe > England > ... > Chile).
3. **`SofascoreEndpoint.refresh_interval_seconds`** — как часто refresh для каждого
   endpoint pattern (например `player/{id}/transfer-history` = 7 дней, `team/{id}` = 1 день).
4. **`SofascoreEndpoint.refresh_priority`** — численный весовой коэффициент, влияет на
   порядок в round-robin при cold start (50 = standard, 70 = high, 30 = low).

---

# Сводная таблица: planner → stream → worker

| Planner daemon | Tick | Job type | Stream | Worker | Что в результате |
|---|---|---|---|---|---|
| `planner-daemon` | 3600s | `discover_sport_surface scope=scheduled` | `stream:etl:discovery` | `sofascore-discovery@N` | scheduled events дня + hydrate jobs |
| `live-discovery-planner-daemon` | 5s | `discover_sport_surface scope=live` | `stream:etl:live_discovery` | `sofascore-live-discovery@N` | live events + tier dispatch |
| `historical-planner-daemon` | 5s | `discover_sport_surface scope=historical date=X` | `stream:etl:historical_discovery` | `sofascore-historical-discovery@N` | events для исторических дат |
| `historical-tournament-planner-daemon` | 60s | `sync_tournament_archive` | `stream:etl:historical_tournament` | `sofascore-historical-tournament@N` | весь сезон UT |
| `structure-planner-daemon` | 30s | `sync_tournament_structure` | `stream:etl:structure_sync` | `sofascore-structure-sync` (1 inst) | season rounds/cuptrees/seasons |
| `resource-planner-daemon` | 30s | `refresh_resource` | `stream:etl:resource_refresh` | `sofascore-resource-refresh@N` | player/team/manager profiles, season widgets |
| `tournament-registry-refresh-daemon` | 86400s | — (прямой DB update) | — | `sofascore-tournament-registry-refresh` | `tournament_registry` table |

## Дополнительные daemons

| Daemon | Tick | Purpose |
|---|---|---|
| `worker-maintenance@N` | continuous | reclaim stuck consumer-group messages + retention sweeps + housekeeping (включая zombie cleanup) |
| `worker-historical-maintenance@N` | continuous | то же для исторических lane'ов |
| `worker-live-rescue` (A2, disabled by default) | 60s | force-hydrate stuck live events |
| `monitoring` (sofascore-monitoring.service) | continuous | SLO alerts → Telegram |
| `proxy-health` | continuous | smartproxy health check |
| `mobile-proxy-watchdog.timer` | periodic | rotation mobile proxy |
| `api-cache-warmer` | continuous | прогревает Redis response cache для local API |

---

# Где это всё видеть прямо сейчас (real-time)

## Что в очередях прямо сейчас

```bash
ssh sofascore-prod 'curl -s http://127.0.0.1:8000/ops/queues/summary | python3 -m json.tool'
```

Покажет:
- длину каждого `stream:etl:*`
- pending consumers
- `live:dispatch_metrics` (cumulative dispatch counter per tier)

## Что live прямо сейчас

```bash
ssh sofascore-prod 'redis-cli zrange zset:live:hot 0 -1 WITHSCORES | head -40'  # tier_1+tier_2
ssh sofascore-prod 'redis-cli zrange zset:live:warm 0 -1 WITHSCORES | head -40' # tier_3
```

`zset` score = `next_poll_at` (ms epoch). Чем меньше — тем скорее планируется next poll.

Для конкретного event подробности:
```bash
ssh sofascore-prod 'redis-cli hgetall live:event:12345678'
```

## SLO здоровья

```bash
ssh sofascore-prod 'curl -s http://127.0.0.1:8000/ops/live-freshness | python3 -m json.tool'
ssh sofascore-prod 'curl -s http://127.0.0.1:8000/ops/health | python3 -m json.tool | head -50'
```

## Какие jobs за последний час

```bash
ssh sofascore-prod "cd /opt/sofascore && PGPASSWORD=\$(grep -oP 'postgresql://[^:]+:\K[^@]+' .env) \
  psql -U sofascore_user -d sofascore_schema_inspector -h localhost -p 5432 -c \"
SELECT job_type, status, COUNT(*) AS n
FROM etl_job_run WHERE started_at > NOW() - INTERVAL '1 hour'
GROUP BY job_type, status ORDER BY n DESC;
\""
```

## Какие endpoints дёргали за час

```bash
ssh sofascore-prod "cd /opt/sofascore && PGPASSWORD=\$(grep -oP 'postgresql://[^:]+:\K[^@]+' .env) \
  psql -U sofascore_user -d sofascore_schema_inspector -h localhost -p 5432 -c \"
SELECT endpoint_pattern, COUNT(*) AS fetches,
       COUNT(*) FILTER (WHERE http_status BETWEEN 200 AND 299) AS ok_2xx
FROM api_request_log WHERE started_at > NOW() - INTERVAL '1 hour'
GROUP BY endpoint_pattern ORDER BY fetches DESC LIMIT 20;
\""
```

---

# Глоссарий (типы jobs)

| Job type | Smith |
|---|---|
| `discover_sport_surface` | Запрашивает список events (live / scheduled / historical) для одного sport+scope. Публикует child hydrate jobs. |
| `hydrate_event_root` | Полный hydration одного event'а (root + edges + details). Бывает разных режимов: `full` / `core` / `live_delta` / `root_only`. |
| `hydrate_event_edge` | Один конкретный edge endpoint (legacy, в основном заменён hydrate_event_root с fanout). |
| `hydrate_special_route` | Player-level endpoint fan-out (statistics, rating-breakdown, heatmap). |
| `refresh_live_event` | Re-fetch edges для live event'а (через live_warm pool). |
| `refresh_live_event_details` | Re-fetch details (комментарии, шотмап) — отдельный pool. |
| `track_live_event` | Sub-job для обновления live state (zset, hash). |
| `finalize_event` | Финализация event'а после `status=finished`. Запись в `event_terminal_state`. |
| `sync_tournament_structure` | Шкелет турнира (seasons, rounds, cuptrees). |
| `sync_tournament_archive` | Все события сезона UT (массивный backfill). |
| `enrich_tournament_*` | Подгрузка top-players/top-teams/standings для UT. |
| `refresh_resource` | Generic per-endpoint refresh с cursor (player profiles, team profiles, season widgets). |
| `normalize_snapshot` | Парсинг существующего snapshot через ParserRegistry (replay path). |
| `replay_failed_job` | Maintenance — повторный запуск упавших jobs из delayed queue. |

---

# TL;DR в одном абзаце

**6 planners** питают **6+ streams** в Redis, **N pool'ов workers** их обрабатывают.
Сверху мы знаем 13 спортов; для каждого `structure_planner` обновляет рамки турниров,
`historical_planner` догоняет историю по датам, `historical_tournament_planner` — историю
по UT. Текущие матчи: `live-discovery-planner` каждые 5s ищет live events → `live-tier-{1,2,3}`
pool'ы их крутят (5s/15s/30s poll cadence) и фан-аут'ят на edges+details через 25 парсеров.
Завершённые матчи финализируются и больше не опрашиваются. Параллельно `resource_planner`
догоняет профили игроков/команд/тренеров для не-event endpoints. Каждый шаг log'ируется
в `etl_job_run` и `api_request_log`, можно смотреть через `/ops/*` endpoints или psql.
