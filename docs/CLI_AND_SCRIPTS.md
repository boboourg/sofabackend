# CLI команды и скрипты

Единая точка входа — `D:\sofascore\schema_inspector\cli.py`. Все команды запускаются как `python -m schema_inspector.cli <subcommand> [args]`.

Subparser зарегистрирован в `_build_parser()` (около строки 1682). Dispatcher — в `main()` через цепочку `if args.command == "..."`.

Локально: `D:/sofascore/.venv311/Scripts/python.exe -m schema_inspector.cli <subcommand>`
На проде: `/opt/sofascore/.venv/bin/python -m schema_inspector.cli <subcommand>` (через `deploy/run_service.sh`)

---

## Daemons / Planners

### `planner-daemon`
**Назначение**: Continuous scheduled planner — отвечает за scheduled discovery, delayed retry replay, live refresh publishing.
**Аргументы**: (none required)
**Side effects**: publish в `STREAM_DISCOVERY`, `STREAM_HYDRATE`, `STREAM_LIVE_TIER_*`, `STREAM_LIVE_WARM`. Пишет в `live:dispatch_metrics` (Redis).
**Mutation**: yes (Redis publish).
**Пример**: используется systemd `sofascore-planner.service`. Локально для теста: `python -m schema_inspector.cli planner-daemon`.

### `live-discovery-planner-daemon`
**Назначение**: Continuous live sport-surface discovery планировщик.
**Side effects**: publish в `STREAM_LIVE_DISCOVERY`.
**Mutation**: yes.

### `historical-planner-daemon`
**Назначение**: Rolling historical date-range планировщик (по cursor).
**Side effects**: publish в `STREAM_HISTORICAL_DISCOVERY`, updates `etl_planner_cursor` table + Redis cursor.

### `historical-tournament-planner-daemon`
**Назначение**: Архивный планировщик tournament/season jobs.
**Side effects**: publish в `STREAM_HISTORICAL_TOURNAMENT`, updates cursor.

### `structure-planner-daemon`
**Назначение**: Скелетный sync для турниров (без полной hydration).
**Side effects**: publish в `STREAM_STRUCTURE_SYNC`.

### `tournament-registry-refresh-daemon`
**Назначение**: Периодическое обновление registry управляемых турниров.
**Side effects**: пишет в `tournament_registry` table + Redis set.

### `resource-planner-daemon`
**Назначение**: Generic resource refresh planner.
**Аргументы**:
- `--loop-interval-seconds` (default 30)
- `--publish-per-tick-cap` (default 20)
- `--lag-threshold` (default 5000)

**Side effects**: publish в `STREAM_RESOURCE_REFRESH`.

---

## Worker'ы (consumer group loops)

Все они: read stream → handler → ACK или retry / DLQ. Аргументы общие: `--consumer-name`, `--block-ms`.

### `worker-discovery`
**Назначение**: Discovery consumer group loop (scheduled scope).
**Stream**: `STREAM_DISCOVERY` (`cg:discovery`)
**Publishes**: `STREAM_HYDRATE`, `STREAM_LIVE_TIER_*`
**Mutation**: yes (DB + Redis).

### `worker-live-discovery`
**Stream**: `STREAM_LIVE_DISCOVERY` (`cg:live_discovery`)
**Publishes**: `STREAM_LIVE_TIER_*`, `STREAM_HYDRATE`. Также пишет в `zset:live:hot/warm/cold` + `live:event:*`.

### `worker-historical-discovery`
**Stream**: `STREAM_HISTORICAL_DISCOVERY` (`cg:historical_discovery`)
**Publishes**: `STREAM_HISTORICAL_HYDRATE`

### `worker-historical-tournament`
**Stream**: `STREAM_HISTORICAL_TOURNAMENT`
**Publishes**: `STREAM_HISTORICAL_ENRICHMENT`

### `worker-structure-sync`
**Stream**: `STREAM_STRUCTURE_SYNC` (`cg:structure_sync`)
**Mutation**: пишет `unique_tournament`, `season`, `event` (skeleton only).

### `worker-hydrate`
**Stream**: `STREAM_HYDRATE` (`cg:hydrate`)
**Handles**: `JOB_HYDRATE_EVENT_ROOT` → `PilotOrchestrator.run_event()`
**Mutation**: пишет full event normalized stack + `api_payload_snapshot` + `event_terminal_state`.

### `worker-historical-hydrate`
**Stream**: `STREAM_HISTORICAL_HYDRATE`
**Идентично** worker-hydrate, но архивный scope.

### `worker-live-hot`
**Stream**: `STREAM_LIVE_HOT` (`cg:live_hot`)

### `worker-live-tier-1`, `worker-live-tier-2`, `worker-live-tier-3`
**Stream**: соответственно `STREAM_LIVE_TIER_{1|2|3}` (`cg:live_tier_{1|2|3}`)
**Handles**: `JOB_REFRESH_LIVE_EVENT` → `PilotOrchestrator.run_event(hydration_mode="live_delta")`
**Optional publish**: `STREAM_LIVE_DETAILS` (если `LIVE_SPLIT_DETAILS_FANOUT=1`)

### `worker-live-warm`
**Stream**: `STREAM_LIVE_WARM` (`cg:live_warm`)

### `worker-live-details`
**Stream**: `STREAM_LIVE_DETAILS` (`cg:live_details`)
**Handles**: per-player endpoint fanout (P0(a) split). Пустой stream если split не включён.

### `worker-maintenance`
**Stream**: `STREAM_MAINTENANCE` (`cg:maintenance`)
**Назначение**: reclaim stale entries из других streams через XAUTOCLAIM, DLQ overflow.
**Mutation**: ACK/REPUBLISH чужих streams, publishes в `STREAM_DLQ`.

### `worker-historical-maintenance`
**Stream**: `STREAM_HISTORICAL_MAINTENANCE`

### `worker-normalize`
**Stream**: `STREAM_NORMALIZE` (`cg:normalize`)
**Handles**: `JOB_NORMALIZE_SNAPSHOT` — re-parse snapshot через parser registry.
**Mutation**: пишет normalized event tables, не пишет в другие streams.

### `worker-resource-refresh`
**Stream**: `STREAM_RESOURCE_REFRESH` (`cg:resource_refresh`)
**Handles**: `JOB_REFRESH_RESOURCE` (generic refresh).
**Особенность**: freshness-skip, empty-data-skip, negative-cache-skip — early exits до fetch'а.
**Env**: `SOFASCORE_RESOURCE_REFRESH_WORKER_MAX_CONCURRENCY`

---

## Ad-hoc / Backfill команды

### `event` — hydrate one or more event ids
**Назначение**: One-shot hydration конкретных event_id для debug или backfill.
**Аргументы**:
- `--event-id <int>` (повторяемый)
- `--sport-slug <str>`
- `--hydration-mode {full,core,live_delta,root_only}` (default `full`)
- `--event-concurrency <int>` (default 1)

**Mutation**: yes (полная hydration + snapshot + normalized).
**Пример**:
```bash
python -m schema_inspector.cli event --sport-slug football --event-id 14083629
```

### `live` — discover live events for a sport and hydrate
**Аргументы**: `--sport-slug` (required), `--limit`
**Mutation**: yes
**Пример**: `python -m schema_inspector.cli live --sport-slug football`

### `scheduled` — discover scheduled events for sport/date
**Аргументы**: `--sport-slug`, `--date` (default today)
**Mutation**: yes
**Пример**: `python -m schema_inspector.cli scheduled --sport-slug football --date 2026-05-14`

### `full-backfill` — hydrate указанные events через hybrid backbone
**Аргументы**: `--event-ids`, `--source-slug`, `--scope`
**Mutation**: yes

### `replay` — replay одного или нескольких snapshot ids
**Назначение**: Re-process snapshots через normalize sink, не fetching заново.
**Аргументы**: UNKNOWN — см. cli.py L1362
**Mutation**: yes (normalized tables overwrite).

### `backfill-leaderboards` — refresh leaderboards для турниров/сезонов
**Аргументы**: UNKNOWN — см. cli.py L1574
**Mutation**: yes

---

## Health & Diagnostics

### `health` — compact hybrid health summary
**Mutation**: no (read-only).
**Output**: short JSON / text summary с DB/Redis OK, snapshot counts.
**Пример**: `python -m schema_inspector.cli health`

### `audit-db` — durable/raw audit за event ids
**Аргументы**: `--sport-slug`, `--event-id` (повторяемый, required)
**Mutation**: no
**Output**: тable per-endpoint per-event — что есть в snapshot vs normalized.

### `recover-live-state` — rebuild Redis live-state из PostgreSQL
**Mutation**: yes (Redis).
**Назначение**: Восстановление `zset:live:hot/warm/cold` + `live:event:*` после сбоя Redis. Читает `event_terminal_state` и `event` для определения active live events.
**Пример**: `python -m schema_inspector.cli recover-live-state`

### `rebuild-capability-rollup` — пересборка endpoint_capability_rollup
**Аргументы**:
- `--sport-slug <str>` (опционально, restrict)
- `--lookback-days <int>` (опционально, ограничить окно observations)

**Mutation**: yes (DB INSERT ON CONFLICT DO UPDATE на `endpoint_capability_rollup`).
**Назначение**: После firebreak 2026-05-13 inline rollup writes отключены. Эта команда пересчитывает rollup state из `endpoint_capability_observation` aggregates. Запускать вручную или по расписанию.
**Пример**:
```bash
python -m schema_inspector.cli rebuild-capability-rollup --lookback-days 1
```

### `stale-live-events` — список стэйл live событий
**Назначение**: Report событий с inprogress статусом, у которых /event snapshot не обновлялся за N секунд. Surfaces transport / proxy регрессии.
**Аргументы**: `--threshold-seconds`, `--top`
**Mutation**: no (read-only).

### `proxy-health-monitor` — continuous proxy health daemon
**Mutation**: yes (Redis state).
**Запускается через**: `sofascore-proxy-health.service`. Локально только для теста.

---

## Скрипты в `D:\sofascore\scripts\` (если есть)

### `scripts/prod_readiness_check.py`
**Назначение**: Базовый readiness probe для прод-стека (streams exist, lag=0, no recent failed jobs).
**Mutation**: no
**Пример**: `python3 scripts/prod_readiness_check.py --base-url http://127.0.0.1:8000`
**Важно**: это **baseline** check. Не означает 24/7 ready — не проверяет hydrate/live_hot lag decreasing, throughput. См. CLAUDE.md note.

### `scripts/ops/mobile_proxy_watchdog.sh`
**Назначение**: Mobile proxy watchdog (shell).
**Запускается через**: `sofascore-mobile-proxy-watchdog.service` (oneshot, видимо по таймеру).

---

## Deploy script

### `deploy/run_service.sh`
**Назначение**: Тонкий launcher для systemd units. Активирует `.venv` и вызывает `python -m schema_inspector.cli "$@"`.
**Использование**: `ExecStart=/opt/sofascore/deploy/run_service.sh planner-daemon`

---

## Сводная таблица — read-only vs mutation

| Команда | Mutation | DB | Redis | Stream publish |
|---|---|---|---|---|
| `health` | no | read | read | — |
| `audit-db` | no | read | — | — |
| `stale-live-events` | no | read | — | — |
| `recover-live-state` | yes | read | **write** | — |
| `rebuild-capability-rollup` | yes | **write** | — | — |
| `event` | yes | **write** | — | — |
| `live`, `scheduled` | yes | **write** | **write** | **yes** |
| `full-backfill` | yes | **write** | — | yes |
| `replay` | yes | **write** | — | — |
| `backfill-leaderboards` | yes | **write** | — | — |
| `planner-daemon` (long-running) | yes | minimal | **write** | **yes** |
| `worker-*` (long-running) | yes | **write** | **write** | conditional |

Перед запуском любого `mutation=yes` локально на dev — убедитесь что `SOFASCORE_DATABASE_URL` указывает на dev БД, а не на прод.

---

## Безопасные ad-hoc запуски

Эти команды стабильно используются в operations и read-only:

```bash
# Compact health snapshot
python -m schema_inspector.cli health

# Audit для конкретного event
python -m schema_inspector.cli audit-db --sport-slug football --event-id 14083629

# Stale detection
python -m schema_inspector.cli stale-live-events --threshold-seconds 300 --top 20
```

Mutation команды только в plan'ной maintenance window:

```bash
# После firebreak rollout — пересборка rollup
python -m schema_inspector.cli rebuild-capability-rollup --lookback-days 1

# После Redis flush — восстановление live state из PostgreSQL
python -m schema_inspector.cli recover-live-state
```

См. также [OPERATIONS_RUNBOOK.md](OPERATIONS_RUNBOOK.md) для прод-runbook'ов.
