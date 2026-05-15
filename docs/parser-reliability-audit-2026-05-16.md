# Аудит надёжности парсеров — 2026-05-16

Источник данных: localhost code inventory + prod `psql sofascore_schema_inspector` +
`journalctl -u sofascore-hydrate@*`. Окно: 24 часа (с 02:00 UTC 15-05 по 02:00 UTC 16-05),
кое-где более узкое (1–2 часа) для точечных silent-drop probe'ов.

---

## 0. TL;DR — что мы реально нашли

Сами по себе парсеры **работают**. Главные проблемы — НЕ в parser-логике:

1. **Upstream 404 для 4 endpoints катастрофические** — `/managers` 70%, `/official-tweets` 90%, и т.д. Снапшоты в БД не падают, но мы тратим proxy budget на запросы которые заведомо безрезультатные.
   * 🟡 **Уже работает negative cache:** скрывает 80-87% повторных probes (3.6k реальных fetch'ей вместо 30k теоретических для `/managers`). Но cooldown в `inprogress` фазе слишком короткий — на 2-часовом матче выходит ~12 wasted probes per event. См. §6.0 + §6.1.
2. **9 endpoints без парсера** — fetched и сохраняются только в `api_payload_snapshot` (raw passthrough), normalized rows нет. Включая `/highlights`, `/team-streaks`, `/average-positions`, player-level `/heatmap`, `/shotmap`, `/goalkeeper-shotmap`. См. §2.
3. **DeadlockDetectedError 39× / TimeoutError 13× за 24h** в hydrate-worker → primary cause "silent drops" по факту, а не парсер.
4. **`/event/h2h` парсер имеет 11.5% silent drops** — 13 из 113 events за 2h. Единственный парсер с реальной проблемой. См. §4.
5. **`ReplayFetchExecutor soft-skipping`** для player rating-breakdown/heatmap — десятки случаев / час. Это сломанный fan-out, не parser.
6. **Player parser nullify'ит `player.team_id`** когда parent team ещё не загружен. Player сохраняется без team_id, потом back-fill вручную не происходит.

При этом для top-15 endpoints где парсер реально вызывается **silent drop rate = 0%** (кроме
h2h): каждый успешный snapshot превращается в нормализованную строку. **Парсеры писать новый
код смысла нет** — смысл расширить negative cache и добавить capability gating.

---

## 1. Inventory: что у нас есть

### 1.1 Зарегистрированные парсеры (25 штук, в `ParserRegistry.default()`)

| Family | Module | Target table(s) |
|---|---|---|
| `event_root` | `families/event_root.py` | `event`, `event_score`, `event_status`, `event_time`, `event_round_info` |
| `event_lineups` | `families/event_lineups.py` | `event_lineup`, `event_lineup_player`, `event_lineup_missing_player` |
| `event_incidents` | `families/event_incidents.py` | `event_incident` |
| `event_statistics` | `families/event_statistics.py` | `event_statistic` |
| `event_comments` | `families/event_comments.py` | `event_comment`, `event_comment_feed` |
| `event_graph` | `families/event_graph.py` | `event_graph`, `event_graph_point` |
| `event_managers` | `families/event_managers.py` | `event_manager_assignment` |
| `event_h2h` | `families/event_h2h.py` | `event_duel` |
| `event_pregame_form` | `families/event_pregame_form.py` | `event_pregame_form`, `event_pregame_form_item`, `event_pregame_form_side` |
| `event_votes` | `families/event_votes.py` | `event_vote_option` |
| `event_team_heatmap` | `families/event_team_heatmap.py` | `event_team_heatmap`, `event_team_heatmap_point` |
| `event_odds` | `families/event_odds.py` | `event_market`, `event_market_choice` |
| `event_winning_odds` | `families/event_winning_odds.py` | `event_winning_odds` |
| `event_best_players` | `families/event_best_players.py` | `event_best_player_entry` |
| `event_player_statistics` | `families/event_player_statistics.py` | `event_player_statistics`, `event_player_stat_value` |
| `event_player_rating_breakdown` | `special/event_player_rating_breakdown.py` | `event_player_rating_breakdown_action` |
| `season_rounds` | `families/season_rounds.py` | `season_round` |
| `season_cuptrees` | `families/season_cuptrees.py` | `season_cup_tree*` |
| `entity_profiles` | `families/entity_profiles.py` | `player`, `team`, `manager` |
| `baseball_innings` | `special/baseball_innings.py` | `baseball_inning` |
| `baseball_pitches` | `special/baseball_pitches.py` | `baseball_pitch` |
| `esports_games` | `special/esports_games.py` | `esports_game` |
| `shotmap` | `special/shotmap.py` | `shotmap_point` |
| `tennis_point_by_point` | `special/tennis_point_by_point.py` | `tennis_point_by_point` |
| `tennis_power` | `special/tennis_power.py` | `tennis_power` |

### 1.2 Размер таблиц (lifetime)

| Table | Rows | Distinct events |
|---|---:|---:|
| `event` | 3 985 678 | 3 985 678 |
| `event_player_stat_value` | 44 981 781 | 71 667 |
| `event_statistic` | 13 163 359 | 328 642 |
| `event_player_rating_breakdown_action` | 9 534 313 | 8 444 |
| `event_incident` | 7 535 794 | 193 060 |
| `event_score` | 7 480 037 | 3 740 022 |
| `event_lineup_player` | 6 196 824 | 207 826 |
| `event_graph_point` | 5 367 605 | 79 008 |
| `event_comment` | 3 735 168 | 30 421 |
| `event_vote_option` | 2 052 255 | 218 681 |
| `event_player_statistics` | 1 305 493 | 71 667 |
| `event_market` (odds) | 519 607 | 122 427 |
| `event_lineup` | 417 808 | 208 904 |
| `event_duel` (h2h) | 301 570 | 261 591 |
| `event_best_player_entry` | 280 909 | 40 526 |
| `event_manager_assignment` | 240 027 | 133 386 |
| `event_winning_odds` | 180 986 | 105 331 |
| `event_pregame_form` | 135 498 | 135 498 |
| `event_graph` | 79 008 | 79 008 |
| `event_team_heatmap` | 64 666 | 32 340 |
| `shotmap_point` | 30 660 | 987 |

**Реда флаги в покрытии:**
- `event_manager_assignment` всего **133k** events из **3.99M** events = **3.3%** coverage. См. §3.1 — упирается в upstream 404.
- `shotmap_point` всего **987** events. Sofascore возвращает shotmap только для топ матчей.
- `event_player_rating_breakdown_action` 8 444 events, но 9.5M rows. Тяжёлая таблица с малой шириной.

---

## 2. 9 endpoint patterns без парсера (silent drop 100%)

Это endpoints которые мы **запрашиваем** (см. матрицу whitelist в `match_center_policy.py`),
**сохраняем** в `api_payload_snapshot`, но **не парсим** в нормализованные таблицы. Только raw.

| Endpoint | classify() returns | Has parser? | Snapshots / 24h (success) | Notes |
|---|---|:---:|---:|---|
| `/event/{eid}/highlights` | `unknown` | ❌ | 433 | Whitelisted для FINISHED |
| `/event/{eid}/official-tweets` | `unknown` | ❌ | 20 (но 11 379 fetches — 90% 404!) | См. §3.1 |
| `/event/{eid}/team-streaks` | `unknown` | ❌ | 788 | Live whitelist |
| `/event/{eid}/team-streaks/betting-odds/{provider_id}` | `unknown` | ❌ | (no probe) | Live whitelist |
| `/event/{eid}/average-positions` | `unknown` | ❌ | 5 377 | Live whitelist |
| `/event/{eid}/player/{pid}/heatmap` | `unknown` | ❌ | 3 718 | Per-player fan-out |
| `/event/{eid}/shotmap/player/{pid}` | `unknown` | ❌ | 4 406 | Per-player fan-out |
| `/event/{eid}/goalkeeper-shotmap/player/{pid}` | `unknown` | ❌ | 728 | Per-player fan-out |
| `/.../season/{sid}/standings/{scope}` | `season_standings` | ❌ (placeholder) | 67 340 fetches | Парсится отдельным `standings_job.py` |
| `/.../season/{sid}/statistics/info` | `season_info` | ❌ (placeholder) | — | Парсится отдельным `statistics_job.py` |

**Что это значит на проде:** ~15k бесполезных fetches/24h (за вычетом standings/statistics
у которых есть отдельный pipeline) на endpoints, чьи данные читаются только из raw snapshot —
без индексов, без нормализации, через `api_payload_snapshot` JSON. Это:
1. **Тратит proxy budget** (~10-15k запросов/24h).
2. **Не даёт читателям data** (наш `/api/...` localhost API эти endpoints возвращает из raw
   snapshot — медленно и не индексируется по полям payload).
3. **Не позволяет аналитике** делать SQL JOIN'ы на этих данных.

См. §6 рекомендации.

---

## 3. HTTP outcome за 24 часа (top issues)

Источник: `api_request_log`, фильтр `endpoint_pattern LIKE '/api/v1/event/%'`.

### 3.1 Endpoints с катастрофическим 404 rate

| Endpoint | fetches | ok_2xx | 404 | **% 404** | Comment |
|---|---:|---:|---:|---:|---|
| `/event/{eid}/managers` | 3 679 | 1 035 | 2 594 | **70%** | Upstream не возвращает managers для подавляющего большинства live events. Whitelist слишком широкий — мы запрашиваем для всех, получаем 404 для 70%. |
| `/event/{eid}/official-tweets` | 11 379 | 1 141 | 10 201 | **90%** | Sofascore возвращает только для топ-матчей. Whitelist для всех live tier_1+tier_2 — overshoot. |
| `/season/.../player-of-the-season` | 20 653 | 2 271 | 18 359 | **89%** | Сезон-уровень, не event. Большинство сезонов не имеют POTS. |
| `/season/.../top-teams/regularSeason` | 4 090 | 1 159 | 2 926 | **72%** | Только для конкретных competition formats. |
| `/event/{eid}/lineups` | 240 559 | 204 408 | 35 846 | **15%** | Понятно — lineups часто не публикуются до 1ч до матча. |
| `/event/{eid}/comments` | 95 402 | 83 085 | 12 237 | **13%** | Comments не для всех турниров. |
| `/event/{eid}/statistics` | 336 533 | 296 834 | 39 012 | **12%** | OK для football, плохо для tennis/handball/etc. |
| `/event/{eid}/graph` | 151 477 | 134 021 | 17 315 | **11%** | Football-only data в принципе. |

**Что делать:** см. §6 — нужны **upstream coverage filters** (типа `event.has_managers=true` →
fetch managers). Sofascore выдаёт эти hints в `event` root payload (`hasGlobalHighlights`,
`hasEventPlayerStatistics`, etc.) — мы их парсим, но не используем в gating.

### 3.2 Endpoints со здоровым outcome

| Endpoint | fetches | ok_2xx | % ok | Comment |
|---|---:|---:|---:|---|
| `/event/{eid}/atbat/{at_bat_id}/pitches` | 727 796 | 727 755 | 99.99% | Baseball — идеальное покрытие |
| `/event/{eid}/player/{pid}/statistics` | 624 277 | 624 242 | 99.99% | Football player stats |
| `/event/{eid}/player/{pid}/heatmap` | 622 304 | 622 163 | 99.98% | Но нет parser → silent drop! |
| `/event/{eid}/player/{pid}/rating-breakdown` | 615 241 | 615 037 | 99.97% | Football |
| `/event/{eid}/incidents` | 264 625 | 253 163 | 95.7% | Football live |
| `/event/{eid}` (root) | 372 251 | 367 093 | 98.6% | Очень здоровое |

### 3.3 Latency outliers

`avg_latency_ms` слишком высокие — основные кандидаты на оптимизацию:

| Endpoint | avg_latency_ms |
|---|---:|
| `/event/{eid}/h2h` | **12 452** (!!) |
| `/event/{eid}/managers` | **10 785** |
| `/event/{eid}` (root) | **10 402** |
| `/event/{eid}/tennis-power` | 9 280 |
| `/event/{eid}/point-by-point` | 9 180 |
| `/event/{eid}/h2h/events` (custom_id) | 9 222 |
| `/event/{eid}/esports-games` | 8 524 |

10+ секунд на root payload — главный bottleneck. Возможные причины: upstream slow для tier_1
heavy events (Premier League full root с 5MB payload), curl_cffi TLS handshake overhead.
Это отдельный аудит (see §6.5).

---

## 4. Silent drops: snapshot есть, normalized rows нет

Probe: для каждого endpoint, distinct events с успешным snapshot за последние 2 часа
vs distinct events в соответствующей normalized table.

| Endpoint | snap events | in target table | silent drop |
|---|---:|---:|---|
| `/event/{eid}/lineups` | 587 | 587 | **0%** ✅ |
| `/event/{eid}/best-players/summary` | 586 | 586 | **0%** ✅ |
| `/event/{eid}/incidents` | 439 | 439 | **0%** ✅ |
| `/event/{eid}/comments` | 157 | 157 | **0%** ✅ |
| **`/event/{eid}/h2h`** | **113** | **100** | **🚨 11.5%** (13 events) |
| `/event/{eid}/statistics` | 111 | 111 | **0%** ✅ |
| `/event/{eid}/votes` | 75 | 75 | **0%** ✅ |
| `/event/{eid}/odds/{provider_id}/all` | 65 | 65 | **0%** ✅ |
| `/event/{eid}/player/{pid}/statistics` | 58 | 58 | **0%** ✅ |
| `/event/{eid}/heatmap/{team_id}` | 40 | 40 | **0%** ✅ |
| `/event/{eid}/player/{pid}/rating-breakdown` | 37 | 37 | **0%** ✅ |
| `/event/{eid}/provider/{pid}/winning-odds` | 29 | 29 | **0%** ✅ |
| `/event/{eid}/shotmap` | 18 | 18 | **0%** ✅ |
| `/event/{eid}/graph` | 7 | 7 | **0%** ✅ |
| `/event/{eid}/managers` | 6 | 6 | **0%** ✅ |
| `/event/{eid}/pregame-form` | 4 | 4 | **0%** ✅ |

**Вывод:** для 15 из 16 проверенных endpoints silent drop = 0% — парсеры работают надёжно.

**Исключение: `/event/{eid}/h2h` — 11.5% silent drop.** Это **реальный parser issue**, требует
расследования. Возможные причины (без открытия snapshot'ов):
- payload отдаёт пустые `teamDuel` / `managerDuel` для некоторых матчей (Sofascore не
  считал H2H ещё) — parser отрабатывает корректно (ParseResult.empty), но row не пишется.
- payload содержит данные, но parser имеет bug на конкретной структуре.

Если хочется починить — нужно достать `snapshot_id` для одного из 13 silent-drop events
из последних 2h и руками прогнать через `EventH2HParser`. **Effort: 1-2 часа.**

---

## 5. Системные ошибки в hydrate workers (последние 24h)

Источник: `journalctl -u sofascore-hydrate@*`.

### 5.1 Топ типы exceptions

| Exception | Count | Что значит |
|---|---:|---|
| `DeadlockDetectedError` (sqlstate 40P01) | **39** | Postgres detected deadlock → kill victim. ETL retries через `retry_scheduled`. Duration retry: 30-230 секунд. |
| `TimeoutError` | **13** | asyncpg pool timeout (вероятно при upstream slow root payload + lock contention). |

39 deadlock retries / 24h × ~200s avg duration = **~130 минут потраченной hydrate работы** в день,
которая не записалась в БД с первого раза. **Главный источник latency для live events.**

### 5.2 Типичные предметы deadlock'а

Без `pg_locks` snapshot в момент инцидента нельзя сказать наверняка, но судя по
архитектуре и времени deadlock'ов (часто в момент live peaks 20:00-23:00 MSK),
гипотеза:
- Параллельные `INSERT...ON CONFLICT...UPDATE` на `event_market`, `event_player_stat_value`
  (самые широкие fanout таблицы) от разных hydrate workers.
- Lock order: некоторые upsert'ы заказывают rows по `event_id ASC`, другие по другому ключу
  → классический A→B vs B→A deadlock.

### 5.3 Player rating-breakdown / heatmap soft-skip

| Pattern | Frequency last 6h |
|---|---:|
| `ReplayFetchExecutor soft-skipping hydrate_special_route ... player/{pid}/rating-breakdown` | **много** (десятки за час) |
| `... player/{pid}/heatmap` | много |
| `... goalkeeper-shotmap/player/{pid}` | много |

Что это означает: live worker породил `hydrate_special_route` job для конкретного player,
job достал snapshot из store через `ReplayFetchExecutor`, но **в store нет prefetched record**
для этого URL. Значит upstream fetch ещё не выполнился (или не записался в `api_payload_snapshot`
вовремя). Job тихо пропускается, через несколько минут — повтор.

**Это сигнал двух проблем сразу:**
1. **Race** между fetch executor и replay path для per-player fan-out.
2. Per-player URLs не имеют parser → даже если fetch отработает, ничего полезного не запишется
   (см. §2 — `unknown` family).

### 5.4 `Nullified missing FK during normalized persist`

| Pattern | Count last 24h |
|---|---:|
| `player.team_id` → NULL | **4** (за 24h) |

Один и тот же FK rules: player принадлежит team-у, которой ещё нет в БД (порядок ingest:
player → team вместо team → player). Нормализатор не падает, а просто `team_id := NULL`.
Эффект — у некоторых player'ов нет team association, downstream JOIN не находит.

4 раза в день не критично, но **системная проблема** — никто эти `team_id=NULL` строки
back-fill'ом не чинит. Со временем накапливается фантомные player'ы без команды.

---

## 6. Recommendations (по убывающему приоритету)

### 6.0 (Уже работает, но не до конца) — что **уже** сделано против 404'ов

Бобур поднял правильный вопрос — у нас уже есть **два слоя защиты**:

#### Layer 1: Static deny-list (`event_endpoint_static_denylist.py`)

Hard ban на уровне `(sport, endpoint)`. 31 пара hardcoded — например
`(tennis, /event/{eid}/comments)` никогда не fetched (Sofascore не возвращает comments для
tennis). Football в этот список НЕ попадает по `/managers` etc., потому что эти endpoints
для **части** football events реально работают.

#### Layer 2: Per-event negative cache (`event_endpoint_negative_cache.py`)

Per-event состояние: после 404 / empty payload event переходит в `c_probation`, и следующие
fetches **suppressed** до конца cooldown. Cooldown зависит от status phase:

| Phase | recheck интервалы |
|---|---|
| `notstarted` | 15min → 1h → 3h |
| `inprogress` | **2min → 5min → 10min** ← короткие! |
| `finished` | 15min → 1h → 6h |
| `terminated` | 1h → 6h → 24h |

**Эффективность за 24h** (из `event_endpoint_negative_cache_state`):

| Endpoint | suppressed_total | probes_total | savings |
|---|---:|---:|---:|
| `/lineups` (c_probation) | **836 659** | 191 111 | **81% saved** |
| `/statistics` (c_probation) | 821 369 | 204 562 | 80% saved |
| `/official-tweets` (c_probation) | 174 093 | 64 450 | 73% saved |
| `/incidents` (c_probation) | 126 936 | 60 926 | 68% saved |
| `/graph` (c_probation) | 105 155 | 99 845 | 51% saved |
| `/comments` (c_probation) | 77 332 | 70 700 | 52% saved |
| `/managers` (c_probation) | 26 865 | 3 993 | **87% saved** |

Без negative cache мы бы делали **~30 тысяч** запросов на `/managers` в день вместо текущих
3.6 тыс. Это работает.

### 6.1 (Hot) Что **не** работает: cooldown слишком короткий для permanent-404 events

Проблема:
- Live football match идёт ~2 часа.
- Если event перманентно 404'ит `/managers` (Sofascore просто не имеет данных) — мы попадаем
  в cooldown 2/5/10 минут.
- После 3-го recheck остаёмся на 10 минут навсегда. За 2h матча = ~12 wasted probes per event.
- Помножь на 100-500 concurrent live football matches → 1.2k-6k бесполезных fetches/h.

**Improvement A** (1-2ч): добавить **5-й тир в `_PHASE_INTERVALS`** для `inprogress`:
```python
PHASE_INPROGRESS: (
    timedelta(minutes=2),
    timedelta(minutes=5),
    timedelta(minutes=10),
    timedelta(minutes=30),   # NEW — после 4-х подряд negative
    timedelta(hours=2),       # NEW — для permanently-dead для этого матча
),
```

После 5 неудачных probes — суппресим на 2 часа, что в случае live матча равносильно
"больше не пробуем до конца матча". **Ожидаемое снижение** /managers fetches: 60-80% от
текущих 3.6k → ~1.2k/24h.

### 6.2 (Hot) **Capability-aware gating** — поверх negative cache, для root-payload hints

`event` root payload Sofascore содержит boolean флаги:
- `hasEventPlayerStatistics: bool`
- `hasEventPlayerHeatMap: bool`
- `hasGlobalHighlights: bool`
- `hasXg: bool`
- `homeTeam.manager IS NOT NULL` / `awayTeam.manager IS NOT NULL`

EventRootParser **уже парсит** эти данные (в `event` table некоторые поля есть, проверить
точно). `_fetch_gated_event_endpoint` сейчас **игнорирует** эти hints.

**Что сделать:** если `event.has_managers = false` (или managers IS NULL в root payload)
→ **никогда не пробовать `/managers`** для этого event, даже первый раз. Эта проверка
ставится **до** негативного кеша, и убивает 100% wasted probes (а не 87%).

**Что менять:**
1. Убедиться что `event_root` parser сохраняет capability flags в `event` table или Redis.
2. Добавить в `_fetch_gated_event_endpoint` шаг: SELECT capabilities из event по
   `context_event_id` → if specific endpoint disabled → return (None, None) сразу.

**Combined с Improvement A:** /managers fetches уйдут с 3.6k → 100-500 в день (только
для events которые **реально** имеют managers).

**Effort:** 3-5ч (зависит от того, как уже устроен event_root parser + где capability flags).

### 6.2 (Hot) **Deadlock investigation** — `pg_lock` snapshot во время deadlock

Сейчас знаем "что-то deadlock'ит 39 раз/день", но не знаем какие именно rows и какой path.

**Что сделать:** включить `log_lock_waits = on` в postgresql.conf, поднять
`deadlock_timeout = 200ms` (с дефолтных 1s) чтобы дольше ловить, в момент CRIT alert
снять `pg_locks` snapshot. Идентифицировать конкретный pair таблиц, заоптимизировать lock order.

**Effort:** 1-2 часа configure + ~1 неделя observation для diagnosis.

### 6.3 (Warm) **Parsers для 7 unparsed event endpoints**

Если данные **нужны** в нормализованных таблицах (а сейчас они доступны только raw):

| Endpoint | Predicted target table | Parser size estimate |
|---|---|---|
| `/event/{eid}/official-tweets` | `event_official_tweet` (id, event_id, author, text, posted_at) | ~50 строк |
| `/event/{eid}/highlights` | `event_highlight` (id, event_id, type, title, url, posted_at) | ~50 строк |
| `/event/{eid}/team-streaks` | `event_team_streak` (event_id, side, name, value, type) | ~70 строк |
| `/event/{eid}/average-positions` | `event_player_average_position` (event_id, player_id, x, y, touches) | ~60 строк |
| `/event/{eid}/player/{pid}/heatmap` | `event_player_heatmap_point` (event_id, player_id, x, y, value) | ~60 строк |
| `/event/{eid}/shotmap/player/{pid}` | `shotmap_point` (extend существующий с player_id filter) | 0 — уже есть |
| `/event/{eid}/goalkeeper-shotmap/player/{pid}` | `event_goalkeeper_shotmap` (event_id, player_id, x, y, blocked) | ~50 строк |

**Альтернатива:** оставить как raw passthrough если их редко нужно JOIN'ить. Этот раздел —
opt-in расширение, не обязательное.

**Effort:** 2-3 часа per парсер + migration + tests. Итого 1-2 дня.

### 6.4 (Warm) **Fix player.team_id null-on-conflict**

Когда parser нормализатора видит player с team_id, который ещё не в `team` — он null'ит.
Лучше: положить в **queue для back-fill**, чтобы после того как team появится — догрузить
team_id для всех таких player'ов.

**Effort:** 3-4 часа (новый job type + worker logic).

### 6.5 (Warm) **Investigate 10s latency на `/event/{eid}` root**

Avg latency 10s — слишком долго. Гипотезы:
- Upstream slow для tier_1 (Sofascore возвращает full payload).
- TLS handshake overhead curl_cffi.
- Proxy hop через mobile proxy adds 2-3s.

**Что сделать:** snapshot latency by `proxy_id` + по `tournament_tier`. Если разница большая —
ясно где throttle.

**Effort:** ~2 часа аналитика.

### 6.6 (Cold) **`ReplayFetchExecutor soft-skipping` для player fan-out**

Симптом известен, но root cause требует анализа: то ли fetch executor не успевает записать
snapshot до replay, то ли job отправляется без context. Не критично пока player парсеры
ничего полезного из этих endpoints не делают (см. §6.3).

---

## 7. Что **сейчас работает хорошо** (don't fix)

- **Lineups parser** — 99.83% snapshot→row (1 / 590 silent drop, скорее всего payload edge case).
- **Comments, statistics, incidents, shotmap, odds** parsers — **100%** snapshot→row.
- **`event_root`** — 98.6% 2xx, парсится в 5 разных таблиц без issues.
- **`event_player_stat_value`** — 45M rows, 71k events — самая горячая таблица, нет видимых проблем.

**Парсеры не нуждаются в переписывании.** Им нужны капабилити gates перед, и стабильный
persistence layer снизу.

---

## 8. Метрика следующего раза — как мониторить надёжность

Сейчас "надёжность парсера" не отслеживается. Предложу 3 SLO-сигнала для `/ops/health`:

1. **`silent_drop_rate_5min`** — за 5 минут: `(snapshots ok / corresponding target table writes) - 1`.
   Threshold: WARN >2%, CRIT >5%.
2. **`deadlock_rate_5min`** — `count(DeadlockDetectedError in stage_run) per 5 minutes`.
   Threshold: WARN >1/min, CRIT >5/min.
3. **`unknown_family_rate_24h`** — `count(snapshots with classify() = 'unknown') / total snapshots`.
   Если > некоего baseline (например 5%) — кто-то fetch'ит endpoint без парсера.

Это будет **Phase 1** работы если ACK'нешь. Сейчас — финальный отчёт.

---

## 9. Команды для повтора аудита

Чтобы я (или кто-то другой) мог пересобрать те же числа через неделю:

```bash
# HTTP outcome per endpoint
ssh sofascore-prod "cd /opt/sofascore && PGPASSWORD=\$(grep -oP 'postgresql://[^:]+:\K[^@]+' .env) \
  psql -U sofascore_user -d sofascore_schema_inspector -h localhost -p 5432 -P pager=off -c \"
SELECT endpoint_pattern, COUNT(*) AS fetches,
       COUNT(*) FILTER (WHERE http_status BETWEEN 200 AND 299) AS ok_2xx,
       COUNT(*) FILTER (WHERE http_status = 404) AS not_found_404
FROM api_request_log
WHERE started_at > NOW() - INTERVAL '24 hours'
  AND endpoint_pattern ~ '/event/|/season/'
GROUP BY endpoint_pattern ORDER BY fetches DESC LIMIT 50;
\""

# Silent drop probe (один endpoint)
ssh sofascore-prod "cd /opt/sofascore && PGPASSWORD=\$(grep -oP 'postgresql://[^:]+:\K[^@]+' .env) \
  psql -U sofascore_user -d sofascore_schema_inspector -h localhost -p 5432 -P pager=off -A -F'|' -c \"
WITH s AS (SELECT DISTINCT context_event_id AS event_id FROM api_payload_snapshot
  WHERE fetched_at > NOW() - INTERVAL '1 hour' AND endpoint_pattern = '/api/v1/event/{event_id}/lineups'
    AND http_status BETWEEN 200 AND 299 AND context_event_id IS NOT NULL)
SELECT (SELECT COUNT(*) FROM s),
       (SELECT COUNT(DISTINCT el.event_id) FROM event_lineup el JOIN s ON s.event_id=el.event_id);
\""

# Deadlock count
ssh sofascore-prod 'journalctl -u "sofascore-hydrate@*" --since="24 hours ago" --no-pager 2>&1 \
  | grep -oE "exc_type=[A-Za-z]+" | sort | uniq -c | sort -rn'

# Player fan-out soft-skips
ssh sofascore-prod 'journalctl -u "sofascore-hydrate@*" --since="6 hours ago" --no-pager 2>&1 \
  | grep "soft-skipping hydrate_special_route" | wc -l'
```

---

## Summary

Парсеры **здоровы**, проблемы лежат **до** и **после** parser-слоя:
- **До:** избыточный fetch на endpoints где Sofascore возвращает 70-90% 404 (managers, official-tweets, etc.). Тратит proxy budget.
- **После:** DB deadlocks 39/24h в hydrate-worker, retries 200s каждый, suммарно ~2 часа потерянного hydrate в день.

**Что ставить в работу:**
1. **Capability-aware gating** (см. §6.1) — 2-3ч, убирает 70-90% мусорных fetch'ей.
2. **Deadlock investigation** (см. §6.2) — observability + fix lock order.
3. (Опционально) 5-7 new parsers для unparsed endpoints (см. §6.3) — если данные нужны в SQL JOIN'ах.

Парсеры — низкий приоритет переписывания. Главные ROI — gating и deadlocks.
