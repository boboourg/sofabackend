# Football Endpoint Matrix

**Source of truth для:** какие football endpoints парсим, в каких статусах, как часто, и какие желаемые TTL должны быть.

This file должен читаться каждой сессией перед обсуждением refresh cadence для football. Read code → update this file → re-read.

Last refreshed from code: **2026-05-16**.

> **B1 (2026-05-16):** section 7 TTLs теперь runtime-enforced. Любое изменение
> ниже надо синхронизировать с `schema_inspector/endpoint_ttl_policy.py`
> (constant `_FOOTBALL_TTL_MATRIX`). Тесты: `tests/test_endpoint_ttl_policy.py`.

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
| **Per-endpoint × per-status TTL** (B1) | `schema_inspector/endpoint_ttl_policy.py` | matrix sport→pattern→{not_started, live, finished} |
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

**~~Сейчас всё одинаково — нет per-endpoint TTL.~~ B1 (2026-05-16) deployed:** per-endpoint × per-status TTL gate enforced for football через `endpoint_ttl_policy.py`. Edges/details throttle (60s/30s) остаётся как нижний уровень защиты. Бобур-desired TTLs см. **раздел 7 ниже**.

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

**ЭТО ТА КАРТА ЧТО Я ЧИТАЮ КАЖДЫЙ РАЗ.** Здесь Бобур руками задаёт **желаемый** refresh cadence. **B1 (2026-05-16):** эти значения теперь runtime-enforced через `endpoint_ttl_policy.py`. Менять в коде → менять здесь. Edges/details throttle (60s/30s) остаётся как нижний уровень защиты на producer side.

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

| Feature | Effort | Priority | Status |
|---|---|---|---|
| Per-endpoint TTL (sectiona 7 как config) | 3-5 ч | high — exact Бобур request | ✅ B1 (2026-05-16) |
| Status-aware TTL (live vs finished) | 1-2 ч | medium | ✅ B1 (2026-05-16) — same matrix |
| Finished events refresh once per day | 1-2 ч | medium | ✅ B1 (24h TTL для finished по всей матрице) |
| Cancel auto-refresh для finished events после N часов | 1 ч | low | (next) — A4 / housekeeping interaction |
| Tier-aware TTL multipliers (tier_3 = 10× slower) | 1 ч | low | (next) |

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

## Phase 0 progress

### A1 (2026-05-16) — priority-aware backfill ✅

`select_unique_tournament_ids_after_cursor` (cli.py) теперь сортирует
по `category.priority DESC NULLS LAST, ut.user_count DESC NULLS LAST, ut.id`.
Backfill cycle забирает FIFA World Cup → UEFA → Premier League первыми,
amateur Chile в конце.

Verified on prod:
- Top-10: World category (priority=20) UT'ы, ordered by user_count
- Bottom: Chile amateur tournaments (priority=0)

Commit: `fe62b26`.

### B1 (2026-05-16) — per-endpoint × per-status TTL ✅

`schema_inspector/endpoint_ttl_policy.py` содержит matrix sport →
endpoint pattern → `EndpointTtlSpec(not_started, live, finished)`.
Resolver `resolve_endpoint_ttl(sport_slug, endpoint_pattern, status_phase)`
возвращает seconds или `None` (fall-through на legacy globals).

Интеграция: `pilot_orchestrator._event_detail_freshness_fields` теперь
принимает `sport_slug` + `status_phase` и сначала консультируется с
matrix. Старая логика остаётся как fallback. `_fetch_gated_event_endpoint`
передаёт оба контекста.

Эффекты (football):
- `/comments` live 60s + finished 24h
- `/official-tweets` live 1200s (20×60s) + finished 24h
- `/lineups` not_started 5min + live 60s + finished 24h
- `/managers` not_started 1h + live 60s + finished 24h
- `/highlights` only finished 24h
- player-level (`/player/{pid}/statistics` etc.) live 60s + finished 24h
  (было 300s legacy)

Tests: 19 unit-tests в `tests/test_endpoint_ttl_policy.py`. 1 pin-test
обновлён в `tests/test_pilot_orchestrator_freshness.py` (player TTL
300→60 по matrix).

### A3b (2026-05-16) — LiveTierOverrideRegistry wired ✅

`HybridApp.ensure_tier_override_registry()` lazy-loads override snapshot
из `live_tier_override` table. `ServiceApp.run_*_worker/_planner_daemon`
зовут его на boot перед стартом loop'а. Registry прокинут до `LiveWorker`
через PilotOrchestrator + до `DiscoveryWorker` через
`build_*_discovery_worker`. Empty table → no behavioural change.

Commit: `f08b1e6`.

### A2 (2026-05-16) — Live Rescue Daemon (disabled by default) ✅

`schema_inspector/services/live_rescue.py` — отдельный loop, каждые 60s:
SELECT `/events/live` snapshot → cross-check Redis live_state → если
event есть в surface но `last_ingested_at` стар (>5min) или `live_state`
вообще нет → publish force_rehydrate job. Per-event Redis cooldown
300s. Disabled by default — flag `SOFASCORE_LIVE_RESCUE_ENABLED=true`.
Systemd unit `sofascore-live-rescue.service`.

Commit: `72a9759`.

### A3 (2026-05-16) — per-UT live tier override (backend готов, wiring pending) 🟡

Table `live_tier_override` создана:
```sql
CREATE TABLE live_tier_override (
    unique_tournament_id BIGINT PRIMARY KEY REFERENCES unique_tournament(id),
    override_tier TEXT NOT NULL CHECK in {tier_1, tier_2, tier_3},
    reason TEXT, created_at, created_by
);
```

Code `LiveTierOverrideRegistry` в `schema_inspector/live_tier_override.py`.
`resolve_live_dispatch_tier` принимает optional `tier_override_registry`
и `unique_tournament_id` — если registry имеет row для UT → returns
override, skipping heuristic.

**Wiring through `cli.py` 3 orchestrator constructions — pending** (Task A3b).
Без wiring registry **не auto-loaded**. Operator workflow для активации:

1. Insert override rows (Phase 0 — через psql):
   ```sql
   INSERT INTO live_tier_override (unique_tournament_id, override_tier, reason, created_by)
   VALUES (17, 'tier_1', 'Premier League: tournament.tier upstream NULL', 'bobur');
   ```
2. Wiring через `service_app.py` (Task A3b): construct + load registry, pass
   into all PilotOrchestrator instances.
3. Restart live workers → registry loads → override takes effect.

Commit: `ec2b9b1`.

## Changelog

| Date | Что | Кто |
|---|---|---|
| 2026-05-15 | Создан после Task 6 (bootstrap fix) | Claude (Bobur ACK) |
| 2026-05-16 | Phase 0 A1 deployed (priority backfill), A3 backend ready (wiring pending) | Claude (Bobur ACK) |
| 2026-05-16 | Phase 0 A3b deployed (registry wired), A2 added (live-rescue daemon, disabled by default), B1 deployed (per-endpoint × per-status TTL) | Claude (Bobur ACK) |
| (пиши сюда manual entries) | | |
