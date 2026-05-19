# Аудит дублирующихся endpoints — Sofascore ETL

**Дата**: 2026-05-19
**Скоуп**: read-only анализ `schema_inspector/endpoints.py` (105 endpoints) + резолверов + синтезаторов
**Цель**: найти ручки которые тянем с upstream, но данные уже есть в нормализованной БД

---

## TL;DR (краткие выводы)

1. **Schedule discovery дублирован 3× раза** — UT-level + team-level + player-level — все возвращают те же event_id
2. **Standings зовём 3× раза** (`total`/`home`/`away`) — `total` арифметически = `home` + `away`
3. **Player-level аггрегаты**: 3-4 ручки на одного игрока возвращают то же что есть в `event_player_statistics`
4. **Team-level аггрегаты**: per-season статистики команды строятся из `event_statistic` + `event_lineup`
5. **`auto_paginate=True` на `/player/{id}/events/last/{p}` × max_pages=50** — самая дорогая ручка (3-5K HTTP / 6h на EPL, **0 новых event_id**)
6. **Локальный synthesizer уже частично есть** ([`scheduled_events_synthesizer.py`](../schema_inspector/scheduled_events_synthesizer.py)) — нужно расширить и переключить serving с raw passthrough на DB

Оценочный win: **минус 60-80% объёма исходящих HTTP** на cohort одной лиги при сохранении 100% уникальной информации.

---

## 1. Источник истины — где данные действительно собираются

Канонический путь сбора event_id:

```
/api/v1/unique-tournament/{ut}/season/{s}/events/round/{n}        → event
/api/v1/unique-tournament/{ut}/season/{s}/events/last/{page}      → event  (наш fix eea162c)
/api/v1/unique-tournament/{ut}/season/{s}/events/next/{page}      → event
/api/v1/unique-tournament/{ut}/featured-events                    → event
/api/v1/unique-tournament/{ut}/season/{s}/brackets                → event
/api/v1/sport/football/scheduled-events/{date}                    → event
```

Все остальные source'ы event_id для football — **дубликаты**.

---

## 2. Категория A — Schedule duplication (САМАЯ ДОРОГАЯ)

### A.1 Team schedule — полный дубликат

| Endpoint | target_table | scope_kind | refresh | Дубликат чего |
|---|---|---|---:|---|
| `/team/{team_id}/events/last/{page}` | `event` | `team-of-active-ut-first-page` | **6h** | UT-level `events/last/{p}` |
| `/team/{team_id}/events/next/{page}` | `event` | `team-of-active-ut-first-page` | **6h** | UT-level `events/next/{p}` |

**Файлы**: [`endpoints.py:406-430`](../schema_inspector/endpoints.py), [`services/resource_scope/team_of_active_ut_first_page.py`](../schema_inspector/services/resource_scope/team_of_active_ut_first_page.py)

**Стоимость EPL**: 20 команд × 2 endpoint × (24h / 6h) = **160 HTTP/сутки** на одну лигу
**Уникальной информации**: 0 (`target_table="event"` — те же row'ы что UT-level)

**Решение**: отключить `scope_kind` или поднять refresh_interval до 30 дней (для аудита разовых mismatch).

### A.2 Player schedule — самый дорогой дубликат

| Endpoint | target_table | auto_paginate | max_pages | refresh |
|---|---|:---:|---:|---:|
| `/player/{player_id}/events/last/{page}` | `api_payload_snapshot` | **TRUE** | **50** | **6h** |

**Файл**: [`endpoints.py:1199-1218`](../schema_inspector/endpoints.py)

**Стоимость EPL**: 20 команд × 25 активных игроков × ~5 страниц avg × (24h/6h) = **~10,000 HTTP/сутки**
**Что в payload уникального**:
- `events[]` — мы УЖЕ имеем в `event`
- `playedForTeamMap` — синтезируется из `event_player_statistics.team_id` JOIN `event`
- `statisticsMap` — это `event_player_statistics` per-player aggregations
- `hasNextPage` — синтетика (offset-based)

**Решение**: всё derivable из БД. См. [`scheduled_events_synthesizer.py:294-519`](../schema_inspector/scheduled_events_synthesizer.py) — `_FETCH_QUERY_PLAYER_EVENTS_LAST` уже умеет строить ответ через SQL. Локальный API уже использует синтезатор как fallback — нужно сделать его дефолтом и убрать `auto_paginate`.

### A.3 UT-level scheduled by date — частично дубликат

| Endpoint | Где используется | Дубликат |
|---|---|---|
| `/unique-tournament/{ut}/scheduled-events/{date}` | `_run_calendar_mode` calendar fallback | overlaps `/events/last/{p}` + `/events/next/{p}` |

**Файл**: [`services/structure_sync_service.py:421`](../schema_inspector/services/structure_sync_service.py)

**Стоимость**: до **210 daily probes per (UT, season)** (180 forward + 30 backward) с отсечкой 14 пустых дней
**Дубликат**: события на этих датах уже в `event` через canonical путь

**Решение**: для UT у которых уже есть скелет (`tournament_registry.structure_complete=true` маркер), переключить calendar mode на синтез из `event` + Sofascore-style envelope.

---

## 3. Категория B — Standings triplicate

`/unique-tournament/{ut}/season/{s}/standings/{scope}` зовём **3 раза** для football:

```python
# sport_profiles.py:82
football: standings_scopes=("total", "home", "away")
```

**Файлы**: [`sport_profiles.py:82`](../schema_inspector/sport_profiles.py), [`default_tournaments_pipeline_cli.py:493`](../schema_inspector/default_tournaments_pipeline_cli.py)

**Endpoint**: `/unique-tournament/{ut}/season/{s}/standings/{scope}` → `standing`

**Stage 5 + resource refresh**: `refresh_interval_seconds=30*60` (30 минут), `scope_kind="season-of-active-ut-standings"` — **3 raw HTTP каждые 30 минут на (UT, season)**.

**Уникальность**:
- `total` — все матчи
- `home` — только домашние
- `away` — только гостевые
- **Арифметика**: `total[team] = home[team] + away[team]` (для большинства метрик)

**Sofascore почему-то отдаёт три раздельных payload'а, но всё считается одной агрегацией.**

**Решение**: фетчить только `total` + `home`. Запрос `away` синтезировать как `total - home` для метрик где это корректно (wins/draws/losses/goals/points). Для derived метрик типа "form streak per scope" может быть сложнее — проверить ручкой какой % использует чисто аддитивные поля.

**Экономия**: **33% HTTP** на standings.

---

## 4. Категория C — Player-level дубликаты (4 ручки делают одно)

### C.1 Player season statistics + breakdown

| Endpoint | target_table | scope | refresh | Источник истины в БД |
|---|---|---|---:|---|
| `/player/{pid}/statistics` | `player_season_statistics` | `player-of-active-squad` | 24h | aggregate of `event_player_statistics` |
| `/player/{pid}/statistics/seasons` | `entity_statistics_season` | `player-of-active-squad` | 7d | derivable from above by season grouping |
| `/player/{pid}/statistics/match-type/overall` | snapshot | `player-of-active-squad` | 24h | breakdown by `event.tournament_id` / type |
| `/player/{pid}/national-team-statistics` | snapshot | `player-of-active-squad` | 7d | filter `event_player_statistics` where event.is_national=TRUE |

**Файлы**: [`endpoints.py:965-1064`](../schema_inspector/endpoints.py)

**Стоимость EPL** (500 active players):
- `/statistics`: 500/день
- `/statistics/seasons`: 500/неделя = 71/день
- `/statistics/match-type/overall`: 500/день
- `/national-team-statistics`: 500/неделя = 71/день
- **= ~1,140 HTTP/день на player aggregations**

**Что уникального**: только сам формат envelope. Все числа — суммы поверх `event_player_statistics`.

**Решение**: синтезировать все 4 endpoint'а из `event_player_statistics` через JOIN. Это потребует:
- materialized view `mv_player_aggregate_stats` обновляемая после batch hydrate
- адаптер в `local_api_server` для каждого endpoint shape
- отключить `scope_kind` в `endpoints.py`

### C.2 Player profile noise

| Endpoint | target_table | refresh | Стоит ли тянуть |
|---|---|---:|---|
| `/player/{pid}/last-year-summary` | snapshot | 24h | **Нет** — derivable from event_player_statistics last 365d |
| `/player/{pid}/attribute-overviews` | snapshot | 24h | Sofascore-derived editorial → raw passthrough OK (нельзя синтезировать) |

**Файлы**: [`endpoints.py:1050-1053`](../schema_inspector/endpoints.py), [`endpoints.py:1186-1196`](../schema_inspector/endpoints.py)

**Решение для `last-year-summary`**: синтез из `event_player_statistics` JOIN `event` `WHERE start_timestamp > NOW() - INTERVAL '1 year'`.

### C.3 Per-(player, UT, season) duplicates

| Endpoint | target_table | scope | refresh |
|---|---|---|---:|
| `/player/{pid}/unique-tournament/{ut}/season/{s}/statistics/overall` | snapshot | hydrate | per-season |
| `/player/{pid}/unique-tournament/{ut}/season/{s}/heatmap/overall` | snapshot | hydrate | per-season |

**Файл**: [`endpoints.py:1085-1114`](../schema_inspector/endpoints.py)

**Дубликаты**:
- `statistics/overall` ≈ `/event/{id}/player/{pid}/statistics` aggregated by season
- `heatmap/overall` ≈ sum of `/event/{id}/player/{pid}/heatmap` per season

**Решение**: синтез из per-event данных + materialized aggregate.

---

## 5. Категория D — Team-level дубликаты

### D.1 Team season stats

| Endpoint | target_table | scope | refresh |
|---|---|---|---:|
| `/team/{tid}/team-statistics/seasons` | `entity_statistics_season` | (hydrate) | per-season |
| `/team/{tid}/player-statistics/seasons` | `team_player_statistics_season` | (hydrate) | per-season |
| `/team/{tid}/unique-tournament/{ut}/season/{s}/statistics/overall` | snapshot | hydrate | per-season |
| `/team/{tid}/unique-tournament/{ut}/season/{s}/goal-distributions` | snapshot | `team-of-active-ut-season` | 24h |

**Файлы**: [`endpoints.py:1073-1106`](../schema_inspector/endpoints.py)

**Что синтезируется из БД**:
- `team-statistics/seasons` — list available `(ut, season)` from `event WHERE home_team_id=X OR away_team_id=X` GROUP BY season
- `statistics/overall` — aggregate of `event_statistic` JOIN events of team in season
- `goal-distributions` — `event_incident WHERE incident_type='goal' AND scoring_team_id=X` group by 15-min buckets

**Решение**: синтез + опциональный snapshot для editorial-extras (assists rankings внутри payload).

### D.2 Team profile noise

| Endpoint | target_table | refresh | Что есть в БД |
|---|---|---:|---|
| `/team/{tid}/players` | snapshot | 12h | `event_lineup` last 30 days → DISTINCT player_id WHERE team_id=X |
| `/team/{tid}/featured-players` | snapshot | 24h | Sofascore editorial — НЕ синтезируется |
| `/team/{tid}/transfers` | snapshot | 30d | `player_transfer_history WHERE from_team_id=X OR to_team_id=X` |
| `/team/{tid}/unique-tournament/{ut}/season/{s}/top-players/overall` | `top_player_snapshot` | (hydrate) | overlaps UT-level `/top-players/overall` filtered by team |

**Файлы**: [`endpoints.py:1132-1183`](../schema_inspector/endpoints.py)

**Решение**:
- `team/{id}/players` → синтез из недавних lineups (squad по факту)
- `team/{id}/transfers` → синтез из `player_transfer_history`
- `team/{id}/.../top-players/overall` → filter UT-level data

---

## 6. Категория E — Leaderboards overlap

UT-level leaderboards (4-7 endpoint на (UT, season)):

| Endpoint | target_table | Дубликат / overlap |
|---|---|---|
| `/season/{s}/top-players/overall` | `top_player_snapshot` | rankBy="rating" by default |
| `/season/{s}/top-ratings/overall` | `top_player_snapshot` | **то же** что top-players?rankBy=rating |
| `/season/{s}/top-players-per-game/all/overall` | `top_player_snapshot` | то же поле, accumulation=per_game |
| `/season/{s}/top-teams/overall` | `top_team_snapshot` | aggregate teams |
| `/season/{s}/player-of-the-season-race` | `top_player_snapshot` | editorial overlay |

**Файлы**: [`endpoints.py:1246-1312`](../schema_inspector/endpoints.py)

**Дубликат**: первые три (top-players, top-ratings, per-game) пишут в **один** target table `top_player_snapshot` с разными `rankBy`/`accumulation` ключами. Большая часть row'ов overlapping — топ-10 игроков по rating пересекается с топ-10 по per-game.

**Решение**: посчитать union/intersection в production. Если intersection >70%, оставить только `/top-players/overall` с базовым ranking и убрать остальные.

### E.1 Statistics aggregations

| Endpoint | target_table | Что внутри |
|---|---|---|
| `/season/{s}/statistics?fields=...` | `season_statistics_snapshot` | paged player stats — основной |
| `/season/{s}/statistics/info` | `season_statistics_config` | available fields metadata |
| `/season/{s}/player-statistics/types` | `season_statistics_type` | то же что выше? |
| `/season/{s}/team-statistics/types` | `season_statistics_type` | то же для team |

**Файлы**: [`endpoints.py:901-914`](../schema_inspector/endpoints.py), [`endpoints.py:1364-1373`](../schema_inspector/endpoints.py)

**Подозрение**: `/statistics/info` + `/player-statistics/types` + `/team-statistics/types` — три способа получить metadata о доступных полях. Sofascore возможно их различает (`info` глобальная, `*-statistics/types` per-entity), но **функционально overlap есть**.

**Действие**: проверить какие keys уникальны между ними — если все три mostly похожи, оставить один.

---

## 7. Категория F — Event-level дубликаты внутри одного матча

### F.1 Heatmap overlap

| Endpoint | target_table |
|---|---|
| `/event/{eid}/heatmap/{team_id}` | `event_team_heatmap` |
| `/event/{eid}/player/{pid}/heatmap` | snapshot |

**Связь**: per-team heatmap = sum of per-player heatmaps (player heatmaps for team's starters).

**Действие**: если оба регулярно фетчатся, можно отключить per-team и собирать sum on the fly.

### F.2 Shotmap overlap

| Endpoint | target_table |
|---|---|
| `/event/{eid}/shotmap` | `shotmap_point` |
| `/event/{eid}/shotmap/{team_id}` | `shotmap_point` |
| `/event/{eid}/shotmap/player/{pid}` | snapshot |
| `/event/{eid}/goalkeeper-shotmap/player/{pid}` | snapshot |

**Файлы**: [`endpoints.py:738-754`](../schema_inspector/endpoints.py)

**Дубликат**: `shotmap/{team_id}` это `shotmap` filter by team. Per-player это `shotmap` filter by player.

**Действие**: оставить базовый `shotmap`, фильтровать в local_api_server по запросу.

### F.3 H2H duplicates

| Endpoint | target_table | Что |
|---|---|---|
| `/event/{eid}/h2h` | `event_duel` | teamDuel, managerDuel — статистика |
| `/event/{custom_id}/h2h/events` | snapshot | список прошлых матчей этих команд |

**Файлы**: [`endpoints.py:601-610`](../schema_inspector/endpoints.py)

**`/h2h/events`** дубликат — событие где встречались эти две команды УЖЕ есть в `event`. Запрос строится как:
```sql
SELECT * FROM event
WHERE (home_team_id=A AND away_team_id=B) OR (home_team_id=B AND away_team_id=A)
ORDER BY start_timestamp DESC LIMIT 20
```

**Действие**: синтезировать `/h2h/events` из БД, отключить fetch (имеет `refresh_interval_seconds=7*24*3600` сейчас — еженедельно × все custom_id).

### F.4 Team streaks

| Endpoint | target_table |
|---|---|
| `/event/{eid}/team-streaks` | snapshot |
| `/event/{eid}/team-streaks/betting-odds/{prov}` | snapshot |

**Содержимое**: серии команд (форма последних 5 матчей, W/D/L). Полностью derivable из `event` table.

**Действие**: синтезировать из истории.

### F.5 Always-on edges с низкой ценностью

| Endpoint | target_table | Notes |
|---|---|---|
| `/event/{eid}/highlights` | snapshot | video URLs — frontend строит сам |
| `/event/{eid}/weather` | snapshot | редко используется на UI |
| `/event/{eid}/average-positions` | snapshot | derivable from per-player heatmaps |
| `/event/{eid}/official-tweets` | snapshot | editorial, low value |
| `/event/{eid}/graph/sequence` | snapshot | post-D11 audit — нестабильный shape |

**Файлы**: [`endpoints.py:574-810`](../schema_inspector/endpoints.py)

**Note из кода (line 829-832)**:
> D11 removed from BASE: EVENT_GRAPH_SEQUENCE_ENDPOINT and EVENT_WEATHER_ENDPOINT. Pre-D11 audit established that both return [unstable / unused data].

→ Уже частично удалены из base hydrate. Но всё ещё в `endpoints.py`.

**Действие**: ревизия value/cost для каждого. Кандидаты на отключение: weather, official-tweets, average-positions.

---

## 8. Категория G — Categories / discovery overlap

| Endpoint | target_table | Дубликат |
|---|---|---|
| `/sport/football/categories/all` | `category` | basis (bootstrap) |
| `/sport/football/categories` | `category` | subset of /all — обычно identical |
| `/sport/football/{date}/{tz}/categories` | `category_daily_summary` | per-date wrapper над теми же category |

**Файлы**: [`endpoints.py:151-182`](../schema_inspector/endpoints.py)

**Действие**: проверить что `/all` и базовый `/categories` фактически разное. Если нет — оставить `/all`.

---

## 9. Категория H — Calendar endpoint

| Endpoint | target_table | Что |
|---|---|---|
| `/calendar/unique-tournament/{ut}/season/{s}/months-with-events` | snapshot | список месяцев с матчами |

**Файл**: [`endpoints.py:433-438`](../schema_inspector/endpoints.py)

**Полностью derivable**:
```sql
SELECT DISTINCT TO_CHAR(TO_TIMESTAMP(start_timestamp), 'YYYY-MM') AS month
FROM event
WHERE unique_tournament_id=$1 AND season_id=$2
ORDER BY month
```

**Действие**: синтезировать.

---

## 10. Сводная таблица — что отключить, что синтезировать

| # | Endpoint | Категория | Action |
|---:|---|---|---|
| 1 | `/team/{id}/events/last/{p}` | A.1 | **DISABLE refresh** (scope_kind=None или refresh=30d) |
| 2 | `/team/{id}/events/next/{p}` | A.1 | **DISABLE refresh** |
| 3 | `/player/{id}/events/last/{p}` | A.2 | **DISABLE auto_paginate**, synthesize в local_api |
| 4 | `/unique-tournament/{ut}/scheduled-events/{date}` | A.3 | conditional disable когда есть скелет |
| 5 | `/season/{s}/standings/away` | B | synthesize (total - home) where additive |
| 6 | `/player/{pid}/statistics/seasons` | C.1 | synthesize from event_player_statistics |
| 7 | `/player/{pid}/statistics/match-type/overall` | C.1 | synthesize |
| 8 | `/player/{pid}/national-team-statistics` | C.1 | synthesize |
| 9 | `/player/{pid}/last-year-summary` | C.2 | synthesize |
| 10 | `/player/{pid}/.../statistics/overall` | C.3 | synthesize from event-level |
| 11 | `/player/{pid}/.../heatmap/overall` | C.3 | synthesize |
| 12 | `/team/{id}/players` | D.2 | synthesize from recent lineups |
| 13 | `/team/{id}/transfers` | D.2 | synthesize from player_transfer_history |
| 14 | `/team/{id}/.../top-players/overall` | D.2 | filter from UT-level |
| 15 | `/season/{s}/top-ratings/overall` | E | overlap with /top-players?rankBy=rating — verify and drop one |
| 16 | `/season/{s}/top-players-per-game/all/overall` | E | overlap with same — verify and drop |
| 17 | `/event/{eid}/h2h/events` | F.3 | synthesize from event history |
| 18 | `/event/{eid}/team-streaks` | F.4 | synthesize from event history |
| 19 | `/event/{eid}/team-streaks/betting-odds/{prov}` | F.4 | synthesize + odds join |
| 20 | `/event/{eid}/weather` | F.5 | DROP (already D11 removed from BASE) |
| 21 | `/event/{eid}/official-tweets` | F.5 | consider DROP |
| 22 | `/event/{eid}/average-positions` | F.5 | synthesize or DROP |
| 23 | `/event/{eid}/highlights` | F.5 | passthrough OK (frontend uses) |
| 24 | `/calendar/.../months-with-events` | H | synthesize from event.start_timestamp |

---

## 11. Оценка экономии HTTP

### Cohort: одна активная Football лига (EPL, 20 команд, 500 игроков, ~380 матчей сезон)

| Ручка | Сейчас (req/сутки) | После | Δ |
|---|---:|---:|---:|
| `team/{id}/events/last+next/0` × 20 команд × 4 раза/день | 160 | 0 | **-160** |
| `player/{id}/events/last/{p}` (auto-paginate ×500 ×4 раза/день ×avg 5 страниц) | 10,000 | 0 | **-10,000** |
| `player/{id}/statistics/*` (4 ручки ×500 ×1/день avg) | 1,140 | ~50 (только snapshot trigger) | **-1,090** |
| `standings/away` 1 раз/30мин/UT | 48 | 0 | **-48** |
| `team/{id}/players` 1/12h × 20 | 40 | 0 | **-40** |
| `team/{id}/transfers` 1/30d × 20 | 0.6 | 0 | -0.6 |
| `team-streaks` × ~10 live матчей/день × 2 ручки | 20 | 0 | -20 |
| `h2h/events` × ~10 матчей/день | 10 | 0 | -10 |
| `months-with-events` 1/UT × ~10 | 10 | 0 | -10 |

**Total Δ для одной лиги**: **~-11,400 HTTP/сутки** при сохранении 100% уникальной информации.

**Масштаб по всему production** (~5,460 active football UT × разные cohort sizes): экономия в **сотни тысяч HTTP/сутки**.

---

## 12. План внедрения (если решишь делать)

### Фаза 1 — низкорискованные отключения (1-2 дня)
- DROP refresh на `team/{id}/events/last+next`
- DROP `auto_paginate` на `player/{id}/events/last`
- DROP `weather`, `official-tweets`, `average-positions` (уже частично сделано)

### Фаза 2 — synthesize простых ручек (3-5 дней)
- `calendar/months-with-events` — SQL one-liner
- `team/{id}/transfers` — JOIN
- `team/{id}/players` — DISTINCT from lineups
- `h2h/events` — event history filter
- `standings/away` — арифметика из home + total

### Фаза 3 — synthesize player aggregates (1-2 недели)
- `player/{pid}/events/last/{page}` — расширить `scheduled_events_synthesizer`
- `player/{pid}/statistics/seasons` — aggregate
- `player/{pid}/statistics/match-type/overall` — by event.event_type filter
- `player/{pid}/national-team-statistics` — filter national
- `player/{pid}/last-year-summary` — last 365 days

### Фаза 4 — materialized views для performance (1 неделя)
- `mv_player_aggregate_stats` — обновляется по trigger after event_player_statistics insert
- `mv_team_aggregate_stats` — аналогично
- Снимает нагрузку на live SQL queries

---

## 13. Риски и mitigation

### R.1 — Sofascore меняет shape payload
- **Риск**: synthesize строит response из БД, но shape должен 1:1 матчить upstream
- **Mitigation**: контрактные тесты — раз в неделю фетчим 5 sample endpoints, diff vs synthesized output

### R.2 — Editorial overlays (featured-players, attribute-overviews)
- **Риск**: Sofascore редакторы вручную выбирают игроков, нельзя синтезировать
- **Mitigation**: эти НЕ трогать, оставить raw passthrough

### R.3 — Frontend hard-coded на raw passthrough
- **Риск**: frontend ждёт точное число полей в JSON
- **Mitigation**: тесты на endpoint-level в `tests/test_local_api_server.py` — пройти ВСЕ synthesize endpoints через эти тесты до промоушна

### R.4 — Coverage сейчас неполный
- **Риск**: если event_player_statistics не покрыт для исторических лет, synthesize вернёт пустоту
- **Mitigation**: synthesize fallback на snapshot если БД пустая. Прогресс по coverage отдельной метрикой.

---

## 14. Связанные файлы для исследования

| Что | Файл |
|---|---|
| Все endpoints (~105) | [`schema_inspector/endpoints.py`](../schema_inspector/endpoints.py) |
| Resource refresh resolvers | [`schema_inspector/services/resource_scope/`](../schema_inspector/services/resource_scope/) |
| Synthesizer (уже частично есть) | [`schema_inspector/scheduled_events_synthesizer.py`](../schema_inspector/scheduled_events_synthesizer.py) |
| Local API serving (где synthesize vs raw) | [`schema_inspector/local_api_server.py`](../schema_inspector/local_api_server.py) |
| Sport profile config (что фетчим) | [`schema_inspector/sport_profiles.py`](../schema_inspector/sport_profiles.py) |
| Backfill orchestrator | [`schema_inspector/default_tournaments_pipeline_cli.py`](../schema_inspector/default_tournaments_pipeline_cli.py) |

---

## 15. Что НЕ трогать

- **`/event/{eid}` (event detail)** — основной payload с метаинформацией Sofascore (fieldTranslations, userCount, coverage, eventState, time). Reconstruction невозможна.
- **`/team/{id}/featured-players`** — editorial, ротируется ежедневно.
- **`/player/{pid}/attribute-overviews`** — editorial radar chart.
- **`/event/{eid}/best-players/summary`** — editorial MVP pick.
- **`/event/{eid}/votes`** — public votes, нельзя предсказать.
- **`/event/{eid}/comments`** — editorial textual comments.
- **Snapshot scope routing** для category-context endpoints (`/category/{cid}/...`).
