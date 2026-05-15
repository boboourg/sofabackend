# Football Endpoint Matrix

**Source of truth для:** какие football endpoints парсим, в каких статусах, как часто, и какие желаемые TTL должны быть.

This file должен читаться каждой сессией перед обсуждением refresh cadence для football. Read code → update this file → re-read.

Last refreshed from code: **2026-05-15**.

---

## 1. Где это в коде (для будущих сессий — заглянуть сюда первым)

| Что | Файл | Что определяет |
|---|---|---|
| Whitelist endpoints per status | `schema_inspector/match_center_policy.py` | какие `/event/{eid}/...` URLs allowed для tier × status |
| Tiers по detail_id | `schema_inspector/match_center_policy.py` | mapping detail_id → tier_1/tier_2/tier_3/tier_5 |
| Poll cadence per tier | `schema_inspector/live_dispatch_policy.py` | `LIVE_TIER_{1,2,3}_POLL_SECONDS` |
| Edges throttle window | `schema_inspector/queue/live_edges_throttle.py` | default 60s per event |
| Details throttle window | `schema_inspector/queue/live_details_throttle.py` | default 30s per event |
| Per-endpoint TTL (если есть) | `schema_inspector/endpoints.py` | `refresh_interval_seconds`, `freshness_ttl_seconds` |
| Highlights delay | `schema_inspector/match_center_policy.py` | `FOOTBALL_HIGHLIGHTS_DELAY_SECONDS = 150 * 60` |
| Bootstrap (первый full fetch) | `schema_inspector/live_bootstrap.py` | first-seen → full hydration; subsequent → delta |

---

## 2. Tier mapping (по detailId на event)

| Tier | detail_id значения | Что это |
|---|---|---|
| **tier_1** | `{1}` | Premier League, La Liga, Champions League — full coverage |
| **tier_2** | `{4, 6}` | Cup/secondary leagues — partial coverage |
| **tier_3** | `{2, 3, 5}` | "Problem detail" — legacy poor upstream coverage, mostly blocked |
| **tier_5** | `NULL` (detail_id отсутствует на root payload) | Большинство events на проде. Limited coverage. |

---

## 3. Live worker poll cadence (`.env` overrides на проде)

| Tier | Default in code | Prod override |
|---|---|---|
| tier_1 | 5s | **5s** (`LIVE_TIER_1_POLL_SECONDS`) |
| tier_2 | 30s | **15s** (`LIVE_TIER_2_POLL_SECONDS`) |
| tier_3 | 90s | **30s** (`LIVE_TIER_3_POLL_SECONDS`) |
| tier_5 | — falls into tier_3 lane | — |

Каждый poll публикует follow-up edges/details job, **gated by throttle window**.

---

## 4. Effective refresh cadence per event (real)

Per-event lower bound (event level):

| Category | Min interval | Source |
|---|---|---|
| `/event/{eid}` root | = tier poll (5/15/30s) | every tier poll publishes root |
| Core edges (`lineups`, `incidents`, `statistics`, `graph`) | 60s | `live_edges_throttle._DEFAULT_INTERVAL_SECONDS` |
| Details (`comments`, `tweets`, `shotmap`, `heatmap`, `best-players`, `average-positions`, ...) | 30s | `live_details_throttle._DEFAULT_INTERVAL_SECONDS` |

Это **upper bound** — реальный refresh может быть реже из-за coalesce / negative cache / status_phase gates.

**Сейчас всё одинаково — нет per-endpoint TTL.** Бобур-desired per-endpoint TTLs см. **раздел 7 ниже**.

---

## 5. Whitelist matrix per status × tier

### NOT STARTED (`status_type in {notstarted, scheduled}`)

| Endpoint | tier_1 | tier_2 | tier_3 | tier_5 |
|---|:---:|:---:|:---:|:---:|
| `/event/{eid}` root | ✅ | ✅ | ✅ | ✅ |
| Edge: `lineups` (X'' patch always-on except tier_3) | ✅ | ✅ | ❌ | ✅ |
| Edge: `incidents` (X'' patch always-on) | ✅ | ✅ | ✅ | ✅ |
| Edge: `statistics` | ❌ | ❌ | ❌ | ❌ |
| Edge: `graph` | ❌ | ❌ | ❌ | ❌ |
| `/managers`, `/h2h`, `/h2h/events`, `/pregame-form`, `/votes`, `/odds/{prov}/all`, `/odds/{prov}/featured`, `/provider/{prov}/winning-odds`, `/team-streaks`, `/team-streaks/betting-odds/{prov}` | ✅ | ✅ | ❌ | ✅ |
| `/comments` | ❌ | ❌ | ❌ | ❌ |
| `/shotmap` | ❌ | ❌ | ❌ | ❌ |
| `/official-tweets` | ❌ | ❌ | ❌ | ❌ |
| `/heatmap/{team_id}` | ❌ | ❌ | ❌ | ❌ |
| `/average-positions` | ❌ | ❌ | ❌ | ❌ |
| `/best-players/summary` | ❌ | ❌ | ❌ | ❌ |
| `/highlights` | ❌ | ❌ | ❌ | ❌ |

### LIVE (`status_type in {inprogress, live}`)

| Endpoint | tier_1 | tier_2 | tier_3 | tier_5 |
|---|:---:|:---:|:---:|:---:|
| `/event/{eid}` root | ✅ | ✅ | ✅ | ✅ |
| Edge: `lineups` | ✅ | ✅ | ❌ | ✅ |
| Edge: `incidents` (X'' patch always-on) | ✅ | ✅ | ✅ | ✅ |
| Edge: `statistics` | ✅ | ✅ | ❌ | ❌ |
| Edge: `graph` | ✅ | ✅ | ❌ | ❌ |
| `/managers`, `/h2h`, `/h2h/events`, `/pregame-form`, `/votes`, `/odds/*`, `/team-streaks*` | ✅ | ✅ | ❌ | ✅ |
| **`/comments`** | ✅ **(only tier_1!)** | ❌ | ❌ | ❌ |
| `/shotmap` (требует `has_xg=True`) | ✅ | ✅ | ❌ | ❌ |
| `/official-tweets` | ✅ | ✅ | ❌ | ❌ |
| `/heatmap/{team_id}` (требует `has_event_player_heat_map=True`) | ✅ | ✅ | ❌ | ❌ |
| `/average-positions` | ✅ | ✅ | ❌ | ❌ |
| `/best-players/summary` | ✅ | ✅ (требует `has_event_player_statistics=True`) | ❌ | ❌ |
| `/highlights` | ❌ (только finished) | ❌ | ❌ | ❌ |

### FINISHED (`status_type in {finished, afterextra, afterpen, aet, apen}`)

Все live endpoints + дополнительно `/highlights`:

| Endpoint | tier_1 | tier_2 | tier_3 | tier_5 |
|---|:---:|:---:|:---:|:---:|
| Все строки **LIVE** выше | ✅ | ✅ | ❌ | ❌ |
| **`/highlights`** (требует `has_global_highlights=True` + match age ≥ 2.5 ч) | ✅ | ✅ | ❌ | ❌ |

---

## 6. Hard bans (никогда не fetched)

| Условие | Эффект |
|---|---|
| `event.is_editor = True` (SofaEditor crowdsourced) | **HARD BAN на ВСЕ** sub-endpoints. Только root |
| Sport ≠ football | Этот module не применяется; gating live в другом модуле |

---

## 7. Bobur desired per-endpoint TTL (заполняется руками)

**ЭТО ТА КАРТА ЧТО Я ЧИТАЮ КАЖДЫЙ РАЗ.** Здесь Бобур руками задаёт **желаемый** refresh cadence. Когда per-endpoint TTL будет implemented в коде, эти значения станут источником истины.

Сейчас системно используется **60s edges / 30s details** на event — для **всех** endpoints одинаково.

### Шаблон

| Endpoint | NOT_STARTED TTL | LIVE TTL | FINISHED TTL | Notes |
|---|---|---|---|---|
| `/event/{eid}` (root) | 5min | tier-poll (5s) | 1×24h | автообновление по tier |
| `/event/{eid}/lineups` | 5min | 60s | 1×24h | confirmed=true стабилен |
| `/event/{eid}/incidents` | n/a | 60s | 1×24h | каждый incident +5min refresh |
| `/event/{eid}/statistics` | n/a | 60s | 1×24h | |
| `/event/{eid}/graph` | n/a | 60s | 1×24h | |
| **`/event/{eid}/comments`** | n/a | **60s** | **1×24h** | Бобур: live раз в минуту, finished раз в день |
| **`/event/{eid}/official-tweets`** | n/a | **20×60s** | **1×24h** | Бобур: live раз в 20 минут, finished раз в день |
| `/event/{eid}/shotmap` | n/a | ? | ? | (Бобур: указать) |
| `/event/{eid}/heatmap/{team_id}` | n/a | ? | ? | (Бобур: указать) |
| `/event/{eid}/best-players/summary` | n/a | ? | ? | (Бобур: указать) |
| `/event/{eid}/average-positions` | n/a | ? | ? | (Бобур: указать) |
| `/event/{eid}/managers` | 1h | 60s | 1×24h | |
| `/event/{eid}/h2h` | 1h | n/a | 1×24h | meta stable |
| `/event/{eid}/h2h/events` | 1h | n/a | 1×24h | meta stable |
| `/event/{eid}/pregame-form` | 1h | n/a | 1×24h | |
| `/event/{eid}/votes` | 5min | 5min | 1×24h | live changes |
| `/event/{eid}/odds/{prov}/all` | 5min | 5min | 1×24h | live betting |
| `/event/{eid}/odds/{prov}/featured` | 5min | 5min | 1×24h | |
| `/event/{eid}/provider/{prov}/winning-odds` | 5min | 5min | 1×24h | |
| `/event/{eid}/team-streaks` | 1h | 5min | 1×24h | |
| `/event/{eid}/team-streaks/betting-odds/{prov}` | 1h | 5min | 1×24h | |
| `/event/{eid}/highlights` | n/a | n/a | 1×24h | only after 2.5h post-finish |

**Соглашения:**
- `n/a` — endpoint не whitelisted для этого статуса (см. матрицу выше)
- Числа `5min`, `1h`, `1×24h` — Бобур может уточнить
- `?` — Бобур заполнит конкретные значения

### Player-level (per-player endpoints, fan-out из lineups)

| Endpoint | NOT_STARTED | LIVE | FINISHED | Notes |
|---|---|---|---|---|
| `/event/{eid}/player/{pid}/statistics` | n/a | 60s | 1×24h | tier_1+tier_2 only |
| `/event/{eid}/player/{pid}/rating-breakdown` | n/a | 60s | 1×24h | |
| `/event/{eid}/player/{pid}/heatmap` | n/a | 60s | 1×24h | требует `has_event_player_heat_map` |
| `/event/{eid}/shotmap/player/{pid}` | n/a | 60s | 1×24h | требует `has_xg` |
| `/event/{eid}/goalkeeper-shotmap/player/{pid}` | n/a | 60s | 1×24h | only goalkeepers, HEAD-gated |

---

## 8. Что **не** реализовано в коде (Бобур может ACK Task для implement)

| Feature | Effort | Priority |
|---|---|---|
| Per-endpoint TTL (sectiona 7 как config) | 3-5 ч | high — exact Бобур request |
| Status-aware TTL (live vs finished) | 1-2 ч | medium |
| Finished events refresh once per day | 1-2 ч | medium |
| Cancel auto-refresh для finished events после N часов | 1 ч | low |
| Tier-aware TTL multipliers (tier_3 = 10× slower) | 1 ч | low |

---

## 9. Quick action — изменить cadence сейчас (env-only)

Через `/opt/sofascore/.env`:

| Env var | Default | Эффект | Risk |
|---|---|---|---|
| `LIVE_TIER_1_POLL_SECONDS` | 5 | Tier_1 root poll cadence | ↑ → slow live, ↓ → больше proxy budget |
| `LIVE_TIER_2_POLL_SECONDS` | 30 | Tier_2 | same |
| `LIVE_TIER_3_POLL_SECONDS` | 90 | Tier_3 | same |
| `SOFASCORE_LIVE_EDGES_THROTTLE_SECONDS` | 60 | Edges enqueue throttle window | ↑ → реже full edges refresh |
| `SOFASCORE_LIVE_DETAILS_THROTTLE_SECONDS` | 30 | Details enqueue throttle | ↑ → реже details refresh |

Эти env переключают глобально для всех endpoints этого слоя. **Per-endpoint** не настраивается без code change.

---

## Changelog

| Date | Что | Кто |
|---|---|---|
| 2026-05-15 | Создан после Task 6 (bootstrap fix) | Claude (Bobur ACK) |
| (пиши сюда manual entries) | | |
