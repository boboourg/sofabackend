# Endpoint TTL Matrix — единый source of truth

Last refreshed from code: **2026-05-23**.

Этот документ — **единая таблица всех endpoint'ов** проекта с их текущими и **желаемыми** TTL.
Цель: Бобур руками задаёт желаемый TTL для каждого endpoint × состояние, после чего код
синхронизируется с матрицей.

---

## 0. Как читать матрицу

### 0.1 Три источника TTL в текущем коде

| Источник | Файл | Что определяет |
|---|---|---|
| **`SofascoreEndpoint.refresh_interval_seconds`** | `endpoints.py` | Как часто `ResourcePlannerDaemon` будет дёргать endpoint (для long-running ресурсов). |
| **`SofascoreEndpoint.freshness_ttl_seconds`** | `endpoints.py` | TTL ключа в `FreshnessStore` (дедупликация дубликатов между ticks). |
| **`endpoint_ttl_policy.py`** (B1 Phase 0) | `endpoint_ttl_policy.py` | Per-status TTL для event sub-endpoints (Live/NotStarted/Finished). |

Эти три механизма работают **в разных pipeline'ах**:
- Event sub-endpoints (`/event/{id}/lineups`, `/comments`, etc.) — `endpoint_ttl_policy` (per-status).
- Resource endpoints (`/player/{id}`, `/team/{id}/players`, season widgets) — `refresh_interval_seconds` (always-on).
- Discovery endpoints (`/sport/{slug}/events/live`, `/scheduled-events/{date}`) — planner tick interval (5s/3600s).

### 0.2 Состояния (status phase) в матрице

| Phase | Когда | Что обычно нужно |
|---|---|---|
| **NOT_STARTED** | event ещё не начался (`status_type` ∈ {notstarted, scheduled}) | Слабый refresh пока ждём starting lineup |
| **LIVE** | event идёт (`status_type` ∈ {inprogress, live, ht, paused}) | Hot refresh, каждые секунды |
| **FINISHED** | event закончился (`status_type` ∈ {finished, afterextra, afterpen}) | Один-два refresh для добора итоговой статистики, потом 24h |
| **ALWAYS-ON** | Не зависит от event'а — long-running resource (player profile, team players, season widgets) | Refresh раз в N часов/дней по cursor |

### 0.3 Колонки в таблицах

| Колонка | Что значит |
|---|---|
| **Endpoint pattern** | Sofascore URL (с placeholder'ами {event_id} / {team_id} / etc.) |
| **Parser** | Имя `parser_family` в `ParserRegistry.default()`, или `(raw)` если только raw passthrough |
| **Target table** | Куда нормализуется (или `api_payload_snapshot` если только raw) |
| **Current TTL** | Сейчас в коде. `-` означает нет TTL (parsed по событию, не по таймеру) |
| **Bobur Live** | ⬜ Желаемый TTL во время **live**. Заполнить руками. |
| **Bobur NotStarted** | ⬜ Желаемый TTL **до начала**. Заполнить руками. |
| **Bobur Finished** | ⬜ Желаемый TTL **после конца**. Заполнить руками. |
| **Bobur Always** | ⬜ Для resource endpoints — refresh interval вне зависимости от event'а. |

### 0.4 Sport scope

Football-only TTL'ы сейчас в `endpoint_ttl_policy._FOOTBALL_TTL_MATRIX`. Остальные виды
спорта попадают в раздел C ниже.

---

## A. Event sub-endpoints (per-status TTL)

Это endpoints которые fanout'ятся когда `PilotOrchestrator` хайдрейтит конкретный event.
Полагаются на `endpoint_ttl_policy.resolve_endpoint_ttl(sport, pattern, phase)`.

### A.1 Football core (B1 Phase 0 уже задеплоено)

| Endpoint pattern | Parser | Target table | Current Live | Current NotStarted | Current Finished | Bobur Live | Bobur NotStarted | Bobur Finished |
|---|---|---|---:|---:|---:|---:|---:|---:|
| `/event/{eid}` (root) | event_root | event, event_score, event_status, event_time, event_round_info | tier-poll (5/15/30s) | tier-poll | 24h | tier-poll ✅ | 5min ✅ | 24h ✅ |
| `/event/{eid}/lineups` | event_lineups | event_lineup, event_lineup_player | **60s** | **5min** | **24h** | 60s ✅ | 5min ✅ | 24h ✅ |
| `/event/{eid}/incidents` | event_incidents | event_incident | **60s** | — | **24h** | 60s ✅ | — | 24h ✅ |
| `/event/{eid}/statistics` | event_statistics | event_statistic | **60s** | — | **24h** | 60s ✅ | — | 24h ✅ |
| `/event/{eid}/graph` | event_graph | event_graph, event_graph_point | **60s** | — | **24h** | 60s ✅ | — | 24h ✅ |
| `/event/{eid}/comments` | event_comments | event_comment | **60s** | — | **24h** | **60s** ✅ | — | **24h** ✅ |
| `/event/{eid}/official-tweets` | (raw) | api_payload_snapshot | **20min** | — | **24h** | **20min** ✅ | — | **24h** ✅ |
| `/event/{eid}/shotmap` | shotmap | shotmap_point | **60s** | — | **24h** | 60s ✅ | — | 24h ✅ |
| `/event/{eid}/heatmap/{tid}` | event_team_heatmap | event_team_heatmap, event_team_heatmap_point | **60s** | — | **24h** | 60s ✅ | — | 24h ✅ |
| `/event/{eid}/best-players/summary` | event_best_players | event_best_player_entry | **60s** | — | **24h** | 60s ✅ | — | 24h ✅ |
| `/event/{eid}/average-positions` | (raw) | api_payload_snapshot | **60s** | — | **24h** | 60s ✅ | — | 24h ✅ |
| `/event/{eid}/managers` | event_managers | event_manager_assignment | **60s** | **1h** | **24h** | 60s ✅ | 1h ✅ | 24h ✅ |
| `/event/{eid}/h2h` | event_h2h | event_duel | — | **1h** | **24h** | — | 1h ✅ | 24h ✅ |
| `/event/{custom_id}/h2h/events` | (raw, B1 entry) | api_payload_snapshot | — | **1h** | **24h** | — | 1h ✅ | 24h ✅ |
| `/event/{eid}/pregame-form` | event_pregame_form | event_pregame_form* | — | **1h** | **24h** | — | 1h ✅ | 24h ✅ |
| `/event/{eid}/votes` | event_votes | event_vote_option | **5min** | **5min** | **24h** | 5min ✅ | 5min ✅ | 24h ✅ |
| `/event/{eid}/odds/{prov}/all` | event_odds | event_market, event_market_choice | **5min** | **5min** | **24h** | 5min ✅ | 5min ✅ | 24h ✅ |
| `/event/{eid}/odds/{prov}/featured` | event_odds | event_market, event_market_choice | **5min** | **5min** | **24h** | 5min ✅ | 5min ✅ | 24h ✅ |
| `/event/{eid}/provider/{prov}/winning-odds` | event_winning_odds | event_winning_odds | **5min** | **5min** | **24h** | 5min ✅ | 5min ✅ | 24h ✅ |
| `/event/{eid}/team-streaks` | (raw) | api_payload_snapshot | **5min** | **1h** | **24h** | 5min ✅ | 1h ✅ | 24h ✅ |
| `/event/{eid}/team-streaks/betting-odds/{prov}` | (raw) | api_payload_snapshot | **5min** | **1h** | **24h** | 5min ✅ | 1h ✅ | 24h ✅ |
| `/event/{eid}/highlights` | (raw) | api_payload_snapshot | — | — | **24h** | — | — | 24h ✅ |
| `/event/{eid}/weather` | (raw) | api_payload_snapshot | not in B1 | not in B1 | not in B1 | ⬜ | ⬜ | ⬜ |

> **Примечание про `/highlights`:** Бобур уточнил — `hasGlobalHighlights=true` означает что
> видео доступно по всему миру (нет geo-блока). Это **не** влияет на наличие данных как
> такового. Парсера у нас нет — только raw snapshot, видео ссылка лежит внутри.

### A.2 Per-player event endpoints (fan-out из lineups)

| Endpoint pattern | Parser | Target table | Current Live | Current NotStarted | Current Finished | Bobur Live | Bobur NotStarted | Bobur Finished |
|---|---|---|---:|---:|---:|---:|---:|---:|
| `/event/{eid}/player/{pid}/statistics` | event_player_statistics | event_player_statistics, event_player_stat_value | **60s** | — | **24h** | 60s ✅ | — | 24h ✅ |
| `/event/{eid}/player/{pid}/rating-breakdown` | event_player_rating_breakdown | event_player_rating_breakdown_action | **60s** | — | **24h** | 60s ✅ | — | 24h ✅ |
| `/event/{eid}/player/{pid}/heatmap` | (raw) | api_payload_snapshot | **60s** | — | **24h** | 60s ✅ | — | 24h ✅ |
| `/event/{eid}/shotmap/player/{pid}` | (raw) | api_payload_snapshot | **60s** | — | **24h** | 60s ✅ | — | 24h ✅ |
| `/event/{eid}/goalkeeper-shotmap/player/{pid}` | (raw) | api_payload_snapshot | **60s** | — | **24h** | 60s ✅ | — | 24h ✅ |

### A.3 Sport-specific event endpoints (non-football)

| Endpoint pattern | Sport | Parser | Target table | Current Live | Bobur Live | Bobur Finished |
|---|---|---|---|---:|---:|---:|
| `/event/{eid}/innings` | cricket | (placeholder, raw) | api_payload_snapshot | — | ⬜ | ⬜ |
| `/event/{eid}/innings` | baseball | baseball_innings | baseball_inning | — | ⬜ | ⬜ |
| `/event/{eid}/at-bats` | baseball | (raw) | api_payload_snapshot | **10min** | ⬜ | ⬜ |
| `/event/{eid}/atbat/{at_bat_id}/pitches` | baseball | baseball_pitches | baseball_pitch | — | ⬜ | ⬜ |
| `/event/{eid}/esports-games` | esports | esports_games | esports_game | — | ⬜ | ⬜ |
| `/event/{eid}/point-by-point` | tennis | tennis_point_by_point | tennis_point_by_point | — | ⬜ | ⬜ |
| `/event/{eid}/tennis-power` | tennis | tennis_power | tennis_power | — | ⬜ | ⬜ |

---

## B. Resource endpoints (always-on, через `ResourcePlannerDaemon`)

Это endpoints которые crawler'ятся **независимо от event'ов** — для refresh профилей и
season widgets. Полагаются на `SofascoreEndpoint.refresh_interval_seconds`.

### B.1 Player profile family

| Endpoint pattern | Parser | Target table | Current refresh | Bobur Always | Notes |
|---|---|---|---:|---:|---|
| `/player/{pid}` | entity_profiles | player | **—** (только лениво) | ⬜ | inline-извлекается из event lineups; full profile only on demand |
| `/player/{pid}/statistics` | (statistics_parser.py) | player_season_statistics | **24h** | ⬜ | scope=player-of-active-squad |
| `/player/{pid}/statistics/seasons` | (raw) | entity_statistics_season | **7d** | ⬜ | scope=player-of-active-squad |
| `/player/{pid}/statistics/match-type/overall` | (raw) | api_payload_snapshot | **24h** | ⬜ | scope=player-of-active-squad |
| `/player/{pid}/national-team-statistics` | (raw) | api_payload_snapshot | **7d** | ⬜ | scope=player-of-active-squad |
| `/player/{pid}/last-year-summary` | (raw) | api_payload_snapshot | **24h** | ⬜ | scope=player-of-active-squad |
| `/player/{pid}/transfer-history` | (raw) | player_transfer_history | **7d** | ⬜ | scope=player-of-active-squad. **Bobur**: "раз в месяц для игроков и клубов" |
| `/player/{pid}/attribute-overviews` | (raw) | api_payload_snapshot | **24h** | ⬜ | scope=player-of-active-squad |
| `/player/{pid}/events/last/{page}` | (raw) | api_payload_snapshot | **6h** | ⬜ | scope=player-of-active-squad-first-page |
| `/player/{pid}/unique-tournament/{utid}/season/{sid}/statistics/overall` | (raw) | api_payload_snapshot | — | ⬜ | not scoped — ленивая |
| `/player/{pid}/unique-tournament/{utid}/season/{sid}/heatmap/overall` | (raw) | api_payload_snapshot | — | ⬜ | not scoped — ленивая |

### B.2 Team profile family

| Endpoint pattern | Parser | Target table | Current refresh | Bobur Always | Notes |
|---|---|---|---:|---:|---|
| `/team/{tid}` | entity_profiles | team | **—** (только лениво) | ⬜ | inline из event root + on-demand profile |
| `/team/{tid}/players` | (raw) | api_payload_snapshot | **12h** | ⬜ | scope=team-of-registry-ut. Squad list. |
| `/team/{tid}/featured-players` | (raw) | api_payload_snapshot | **24h** | ⬜ | scope=team-of-active-ut |
| `/team/{tid}/transfers` | (raw) | api_payload_snapshot | **30d** | ⬜ | scope=team-of-active-ut. Bobur: ok что раз в месяц |
| `/team/{tid}/team-statistics/seasons` | (raw) | entity_statistics_season | **—** | ⬜ | not scoped (lazy) |
| `/team/{tid}/player-statistics/seasons` | (raw) | team_player_statistics_season | **—** | ⬜ | not scoped (lazy) |
| `/team/{tid}/unique-tournament/{utid}/season/{sid}/statistics/overall` | (raw) | api_payload_snapshot | — | ⬜ | not scoped (lazy) |
| `/team/{tid}/unique-tournament/{utid}/season/{sid}/goal-distributions/...` | (raw) | api_payload_snapshot | **24h** | ⬜ | scope=team-of-active-ut-season |
| `/team/{tid}/events/last/{page}` | (raw) | api_payload_snapshot | **30min** | ⬜ | scope=team-of-active-ut-season |
| `/team/{tid}/events/next/{page}` | (raw) | api_payload_snapshot | **30min** | ⬜ | scope=team-of-active-ut-season |

### B.3 Manager profile family

| Endpoint pattern | Parser | Target table | Current refresh | Bobur Always | Notes |
|---|---|---|---:|---:|---|
| `/manager/{mid}` | entity_profiles | manager | **—** | ⬜ | inline из event/managers + on-demand profile |

### B.4 Season widgets / leaderboards

Эти endpoints `ResourcePlannerDaemon` крутит per-tournament per-season.

| Endpoint pattern | Parser | Target table | Current refresh | Bobur Always | Notes |
|---|---|---|---:|---:|---|
| `/unique-tournament/{utid}/season/{sid}/standings/{scope}` | standings_parser.py | standing, standing_row | **30min** | ⬜ | scope=season-of-active-ut-standings |
| `/unique-tournament/{utid}/season/{sid}/statistics/info` | statistics_parser.py | season_statistics_config | **—** | ⬜ | not scoped |
| `/unique-tournament/{utid}/season/{sid}/statistics/{scope}` | statistics_parser.py | season_statistics_snapshot | **—** | ⬜ | not scoped — handled by statistics_job CLI |
| `/unique-tournament/{utid}/season/{sid}/top-ratings/overall` | (raw) | top_player_snapshot | **—** | ⬜ | not scoped |
| `/unique-tournament/{utid}/season/{sid}/player-of-the-season-race` | (raw) | top_player_snapshot | **—** | ⬜ | not scoped |
| `/unique-tournament/{utid}/season/{sid}/venues` | (raw) | venue | **—** | ⬜ | not scoped |
| `/unique-tournament/{utid}/season/{sid}/groups` | (raw) | season_group | **—** | ⬜ | not scoped |
| `/unique-tournament/{utid}/season/{sid}/player-of-the-season` | (raw) | season_player_of_the_season | **—** | ⬜ | not scoped |
| `/unique-tournament/{utid}/season/{sid}/team-of-the-week/periods` | (raw) | period | **—** | ⬜ | not scoped |
| `/unique-tournament/{utid}/season/{sid}/team-of-the-week/{period}` | (raw) | team_of_the_week | **14d** | ⬜ | scope=period-of-registry-football |
| `/unique-tournament/{utid}/season/{sid}/player-statistics/types` | (raw) | season_statistics_type | **—** | ⬜ | not scoped |
| `/unique-tournament/{utid}/season/{sid}/team-statistics/types` | (raw) | season_statistics_type | **—** | ⬜ | not scoped |

### B.5 Season history (events backfill через ResourcePlanner)

| Endpoint pattern | Parser | Target table | Current refresh | Bobur Always | Notes |
|---|---|---|---:|---:|---|
| `/unique-tournament/{utid}/season/{sid}/events/last/{page}` | event_root (fan-in) | event | **30min** | ⬜ | scope=season-of-active-ut |
| `/unique-tournament/{utid}/season/{sid}/events/next/{page}` | event_root (fan-in) | event | **30min** | ⬜ | scope=season-of-active-ut |
| `/unique-tournament/{utid}/season/{sid}/round/{rid}/events` | event_root (fan-in) | event | **7d** | ⬜ | scope=round-of-registry-football |
| `/unique-tournament/{utid}/season/{sid}/rounds` | season_rounds | season_round | **—** | ⬜ | event-driven (structure_planner) |
| `/unique-tournament/{utid}/season/{sid}/cuptrees` | season_cuptrees | season_cup_tree* | **—** | ⬜ | event-driven |
| `/unique-tournament/{utid}/season/{sid}/brackets` | (raw) | event | **—** | ⬜ | event-driven |
| `/unique-tournament/{utid}/season/{sid}/featured-events` | event_root (fan-in) | event | **—** | ⬜ | event-driven |
| `/unique-tournament/{utid}/season/{sid}/scheduled-events` | event_root (fan-in) | event | **—** | ⬜ | event-driven |

### B.6 Tournament metadata

| Endpoint pattern | Parser | Target table | Current refresh | Bobur Always | Notes |
|---|---|---|---:|---:|---|
| `/unique-tournament/{utid}` | (parsed inline в structure_sync) | unique_tournament | **—** | ⬜ | event-driven (structure_planner) |
| `/unique-tournament/{utid}/seasons` | (parsed inline) | season | **—** | ⬜ | event-driven |
| `/unique-tournament/{utid}/season/{sid}/info` | (raw) | api_payload_snapshot | **—** | ⬜ | event-driven |
| `/unique-tournament/{utid}/featured-events` | event_root | event | **—** | ⬜ | not scoped |
| `/unique-tournament/{utid}/scheduled-events` | event_root | event | **—** | ⬜ | not scoped |
| `/unique-tournament/{utid}/media` | (raw) | api_payload_snapshot | **—** | ⬜ | not scoped |
| `/tournament/{tid}/season/{sid}/standings/{scope}` | standings_parser | standing | **—** | ⬜ | not scoped — handled by standings_job CLI |

---

## C. Discovery / planner endpoints (tick-driven)

Эти endpoints **не** имеют TTL — они дёргаются по расписанию planner'а. TTL = tick interval.

| Endpoint pattern | Tick (тик планировщика) | Planner | Stream | Notes |
|---|---|---|---|---|
| `/sport/{slug}/events/live` | **5s** | live-discovery-planner-daemon | `stream:etl:live_discovery` | Список live events |
| `/sport/{slug}/scheduled-events/{date}` | **3600s** (1h) | planner-daemon | `stream:etl:discovery` | Список scheduled events дня |
| `/sport/{slug}/scheduled-events/{date}` (historical date) | планируется при backfill | historical-planner-daemon | `stream:etl:historical_discovery` | Walk дат назад |
| `/sport/{slug}/categories` | **on-demand** | structure-planner-daemon | `stream:etl:structure_sync` | Per UT, при structure sync |
| `/sport/{slug}/categories/all` | **on-demand** | structure-planner-daemon | `stream:etl:structure_sync` | Per UT |
| `/sport/{slug}/{date}/{tz}/categories` | tick через date walker | historical-planner-daemon | `stream:etl:historical_discovery` | Day-by-day backfill |
| `/sport/{slug}/live-categories` | — | — | — | Не fetched пока (доступен для будущего) |
| `/sport/{slug}/finished-upcoming-tournaments` | — | — | — | Не fetched пока |
| `/category/{cid}/unique-tournaments` | **on-demand** | structure-planner-daemon | `stream:etl:structure_sync` | Discover UT in category |
| `/category/{cid}/live-event-count` | — | — | — | Не fetched пока |
| `/config/default-unique-tournaments/{country}/{sport}` | **on-demand** | tournament-registry-refresh-daemon | (прямой DB) | Bootstrap |

---

## D. Что **в реальности на проде сейчас** делает / не делает

Из аудита (`docs/parser-reliability-audit-2026-05-16.md`):

### D.1 Endpoints с парсером (нормализуются)

25 парсер-семейств (`ParserRegistry.default()`):
event_root, event_lineups, event_incidents, event_statistics, event_comments, event_graph,
event_managers, event_h2h, event_pregame_form, event_votes, event_team_heatmap, event_odds,
event_winning_odds, event_best_players, event_player_statistics, event_player_rating_breakdown,
season_rounds, season_cuptrees, entity_profiles, baseball_innings, baseball_pitches,
esports_games, shotmap, tennis_point_by_point, tennis_power.

### D.2 Endpoints fetched но без парсера (raw passthrough)

Из аудита §2:
1. `/event/{eid}/highlights` (Bobur: hasGlobalHighlights — geo flag, видео ссылка в raw)
2. `/event/{eid}/official-tweets`
3. `/event/{eid}/team-streaks`
4. `/event/{eid}/team-streaks/betting-odds/{prov}`
5. `/event/{eid}/average-positions`
6. `/event/{eid}/player/{pid}/heatmap`
7. `/event/{eid}/shotmap/player/{pid}`
8. `/event/{eid}/goalkeeper-shotmap/player/{pid}`

Эти **сохраняются** в `api_payload_snapshot`, но без нормализованной таблицы.

### D.3 Endpoints с проблемой парсера

| Endpoint | Issue |
|---|---|
| `/event/{eid}/h2h` | 11.5% silent drops (parser возвращает empty parse result для ~13% events) |

---

## E. Workflow обновления матрицы

### Когда меняешь TTL

1. **Открой эту матрицу** и найди endpoint.
2. **Заполни Bobur колонки** (или измени уже заполненные).
3. **Дай ACK на изменение** — я внесу в код:
   - Event sub-endpoint → `schema_inspector/endpoint_ttl_policy.py:_FOOTBALL_TTL_MATRIX`
   - Resource endpoint → `schema_inspector/endpoints.py:SofascoreEndpoint(refresh_interval_seconds=...)`
4. **Тесты обновляются** автоматически (если pin'нуты на конкретные значения).
5. **Commit + deploy** — на проде эффект на следующий рестарт worker'а.

### Когда добавляешь новый endpoint

1. Добавь определение в `endpoints.py` с конкретным `refresh_interval_seconds`.
2. Если нужен parser — создай в `parsers/families/`.
3. Зарегистрируй в `parsers/registry.py:ParserRegistry.default()`.
4. Добавь строку в эту матрицу (раздел A или B).
5. Если sport-aware гейтинг (whitelist для status × tier) — в `match_center_policy.py`.

### Когда удаляешь endpoint

1. Найди в матрице.
2. Помечи `~~strikethrough~~` с комментарием "removed 2026-MM-DD: reason".
3. Удали из `endpoints.py`, parser, registry.

---

## F. Глоссарий

- **TTL** (Time To Live) — сколько секунд значение считается свежим. После TTL — fetch заново.
- **refresh_interval_seconds** — как часто `ResourcePlannerDaemon` будет публиковать job для resource endpoint. Каждый target имеет свой cursor в `ResourceCursorStore`.
- **freshness_ttl_seconds** — TTL ключа в `FreshnessStore` (Redis). Используется для дедупликации.
- **per-status TTL** (B1 Phase 0) — для event sub-endpoints, разные TTL для Live/NotStarted/Finished.
- **status phase** — нормализованное `event.status_type` в одно из 4 состояний (см. §0.2).
- **scope_kind** — резолвер в `services/resource_scope/` который выдаёт target list для `ResourcePlannerDaemon`. Без scope_kind endpoint не crawl'ится автоматически — только лениво.
- **(raw)** — нет парсера, только запись в `api_payload_snapshot`. Доступно через `/api/v1/...` local API passthrough.
- **inline-извлечение** — entity (player/team/manager) попутно парсится из event payload (RUT 5.1 in pipeline-tree.md).

---

## G. Дальнейшие действия

После заполнения Bobur колонок:

1. **Согласовать с `docs/football-matrix.md`** — там тоже есть Bobur-desired TTL для football events. Эта таблица должна стать суперсетом.
2. **Bобур ACK на batch** изменений — мне внести в `endpoint_ttl_policy.py` и `endpoints.py`.
3. **Тесты** — `tests/test_endpoint_ttl_policy.py` пинит конкретные значения, потребует update.
4. **Документация** — `pipeline-tree.md` ссылается на эту матрицу (добавлю ссылку).
