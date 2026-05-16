# Backfill Ordering Roadmap (Phase 1, Lightweight)

Bobur ACK 2026-05-16. Цель: docs-of-record для следующей сессии чтобы можно
было начать кодить без re-discovery.

---

## Цель

Перейти от **chaotic** backfill (worker обрабатывает все сезоны UT в случайном
порядке) к **deterministic** ordering:

1. UTs orderied по priority (как в A1): `category.priority DESC, ut.user_count DESC`.
2. Внутри каждой UT: текущий сезон → 24/25 → 23/24 → ... → oldest.
3. Per-(UT, season) cursor: можно остановить/возобновить без потери прогресса.
4. Видимость через `/ops/backfill-progress` + `/ops/backfill-cursor`.

## Текущее состояние (2026-05-16)

| Что | Сейчас | Хочу |
|---|---|---|
| UT ordering | по `unique_tournament_id` (FIFO) | по priority (A1) |
| Season ordering | внутри UT — `season_id DESC` (новые first), но без persistent cursor | persistent cursor per UT |
| Job granularity | `JOB_SYNC_TOURNAMENT_ARCHIVE` обрабатывает **все** сезоны сразу | один сезон per job |
| Resume after restart | теряется state на середине UT | продолжает с next_season_backfill_id |
| Видимость прогресса | `v_backfill_progress` view (deployed 2026-05-16) | + `tournament_registry.next_season_backfill_id` column в view |

## Архитектурный план (Lightweight, 1-2 дня)

### Phase 1.1 — Schema (30 мин)

```sql
ALTER TABLE tournament_registry
  ADD COLUMN next_season_backfill_id BIGINT NULL,
  ADD COLUMN backfill_started_at TIMESTAMPTZ NULL,
  ADD COLUMN backfill_completed_at TIMESTAMPTZ NULL;

COMMENT ON COLUMN tournament_registry.next_season_backfill_id IS
  'Phase 1 backfill cursor: next season_id to process for this UT. '
  'NULL = not started (will start from the most recent season). '
  '0 / negative = no more seasons to backfill (oldest reached).';
```

### Phase 1.2 — Worker logic (3-4 ч)

В `services/historical_archive_service.py:run_historical_tournament_archive`:

1. Принять опциональный `target_season_id` параметр.
2. Если `target_season_id` указан:
   - Обработать **только** этот сезон (skip остальные).
   - В конце update `tournament_registry.next_season_backfill_id` к
     **следующему** сезону по DESC (через query `unique_tournament_season`).
3. Если `target_season_id is None` (legacy path):
   - Текущее поведение (все сезоны).

### Phase 1.3 — Planner ordering (2 ч)

В `services/historical_tournament_planner.py`:

1. При выборе UT — учитывать `tournament_registry.next_season_backfill_id`:
   - UTs с непустым cursor имеют **приоритет** (continue work).
   - UTs с NULL cursor получают cursor = первый сезон при первом старте.
2. Per-job params: `{"target_season_id": <int>}`.

### Phase 1.4 — Bootstrap cursors (1 ч, одноразовая операция)

CLI команда:
```bash
python -m schema_inspector.cli backfill-cursor-bootstrap --sport football
```

Для каждой UT в `tournament_registry` (где `historical_enabled=true`):
- Найти **самый свежий** сезон по `unique_tournament_season` (max season_id).
- Установить `next_season_backfill_id = <тот season_id>`.

После этого worker'ы начнут c этого сезона и пойдут down.

### Phase 1.5 — `/ops/backfill-cursor` endpoint (1 ч)

Показывает для каждой UT:
- ut_id, ut_name, priority
- next_season_backfill_id, next_season_year
- progress: completed_seasons / total_seasons
- ETA based on average duration per season

### Phase 1.6 — View update (30 мин)

Расширить `v_backfill_progress` колонкой `cursor_at_this_season` (bool) —
чтобы видно где сейчас планировщик.

### Phase 1.7 — Tests (2-3 ч)

- Unit: `_run_tournament_worker` honors `target_season_id`.
- Unit: planner selects UTs based on cursor state.
- Integration: bootstrap CLI sets correct initial cursor.

## Не входит в Phase 1

- Multi-sport ordering (сейчас только football). Tennis/basketball/etc.
  через bootstrap CLI флаги `--sport`.
- ETag/If-Modified-Since (отдельная задача).
- UI/dashboard поверх endpoint (отдельная задача).
- Migration к `JOB_SYNC_TOURNAMENT_SEASON` (новый job_type) — это
  full overhaul, не нужно для lightweight.

## Какие риски

1. **Worker timeout** на больших сезонах (1000+ events). Сейчас текущее
   решение полагается на `seasons_per_tournament` cap. После phase 1 каждый
   job = один сезон → simpler timeout boundary.
2. **Backfill loop** если cursor advances неправильно. Защита: cursor only
   moves on `succeeded` job (нет `failed`/`retry_scheduled`).
3. **Cold start** (NULL cursor) для **тысяч** UTs из registry. Defense:
   bootstrap CLI делает это **один раз** + новые UTs получают cursor через
   `tournament-registry-refresh-daemon`.

## Ожидаемый эффект

- Линейный прогресс backfill: можно сказать "за 24h забэкфилили X сезонов".
- Можно остановить worker'ы → resume без потери прогресса.
- Бобур видит в `/ops/backfill-cursor` где сейчас работаем.
- 23/24 гепы автоматически закроются (текущая дыра).

## Когда стартовать

После того как 5 jobs из 2026-05-16 (top-5 leagues 23/24 fill) завершатся
и Бобур увидит `pct=100` в `/ops/backfill-progress`. Это даст baseline и
confidence что worker механика OK.

---

**Ссылки:**
- `migrations/2026-05-16_v_backfill_progress_v2.sql`
- `schema_inspector/local_api_server.py:_fetch_ops_backfill_progress_payload`
- `services/historical_tournament_planner.py`
- `services/historical_archive_service.py:run_historical_tournament_archive`
- `cli.py:select_unique_tournament_ids_after_cursor` (A1 priority ordering)
