# N1: Monitoring Plan

**План monitoring daemon для Sofascore-like backend.**

| Поле | Значение |
| --- | --- |
| Версия | v1 (2026-05-14) |
| Статус | Draft (ждёт ACK на реализацию) |
| Эпоха | 1 (Stabilize) — см. `docs/PROJECT_VISION.md` §6 |
| Зависимости | `/ops/live-freshness`, `/ops/queues/summary` (уже задеплоены) |

---

## TL;DR

Запускаем отдельный демон, который каждые 60 секунд читает SLO/queue/job сигналы из локального API (или Redis напрямую), сравнивает с порогами, и отправляет алерт в Telegram. Никакого Prometheus/Grafana — слишком тяжёлый стек для одного VPS и одного инженера. Цель — узнавать о поломке через 2 минуты, а не утром.

---

## Зачем (motivation)

В `docs/PROJECT_VISION.md` §4 главный pain #2: **"Никакого мониторинга."** Конкретно:

- Падает `sofascore-hydrate@1` в 3 ночи → узнаём утром.
- `oldest_hot_score_age_seconds` пробивает 5 минут → live-матчи лагают → пользователи видят stale данные → мы не знаем, пока не зайдём в `/ops/live-freshness` руками.
- Sofascore меняет URL → парсер падает молча → данные перестают капать в одном спорте → замечаем через дни.

**SLO без monitoring — это просто комментарий в коде.** Без alerts SLO не существует.

**Решение этой проблемы разблокирует всё остальное:**
- N2 (decompose live-discovery persist) — мы не узнаем, помогло ли изменение, без monitoring до/после.
- M1 (production deployment) — невозможен без alerts: если упадёт прод в 3 ночи, мы теряем пользователей.
- Любой experiment / refactor — без monitoring мы не видим regression.

---

## Цели и Definition of Done

### Цели

1. **Обнаружение SLO violation < 2 минут.** Если `oldest_hot_score_age` > 300s, алерт приходит в течение 2 минут.
2. **Single point of truth.** Один Telegram чат, все алерты. Никаких email / SMS / разных каналов на старте.
3. **Низкий шум.** Дедупликация: тот же алерт не повторяется чаще раз в 10 минут.
4. **Простая эксплуатация.** Один systemd unit, один CLI subcommand, один env-block.

### Definition of Done (Phase 1)

- [ ] CLI subcommand `monitoring-daemon` стартует и держит loop
- [ ] 3 SLO сигнала (oldest_hot_age, tier_1_blocked, refresh_success) → проверка каждые 60s
- [ ] Telegram alert приходит при пересечении порога
- [ ] Dedupe: повторный алерт о том же → не спамит в течение 10 мин
- [ ] systemd unit `sofascore-monitoring.service` поднимает демон под `sudo systemctl start`
- [ ] Демон работает 7 дней без падений (`journalctl -u sofascore-monitoring`)
- [ ] Минимум 1 синтетический alert получен (для проверки канала)

### Out of scope для Phase 1

- Grafana дашборды (это можно добавить в Эпохе 2 если будут метрики в Prometheus)
- PagerDuty / Opsgenie escalation chain
- Auto-remediation (рестарт сервиса при падении) — это N4
- Multi-channel routing (Slack + Telegram + email)
- Dashboard внутри Telegram bot (commands `/status`, `/silence`)

---

## Что мониторим (signals)

Сигналы разделены по приоритетам. Phase 1 = только P0. Phase 2-4 расширяют покрытие.

### P0 — SLO signals (Phase 1)

Эти три сигнала уже посчитаны в `/ops/live-freshness`. Просто читаем endpoint каждые 60s.

| Signal | Источник | Порог CRIT | Порог WARN | Окно |
| --- | --- | --- | --- | --- |
| `oldest_hot_score_age_seconds` | `/ops/live-freshness` | > 300 | > 120 | sample каждые 60s |
| `tier_1_blocked_rate` | `/ops/live-freshness` | > 0.50 (50%) | > 0.20 (20%) | sample каждые 60s |
| `refresh_live_event_success_rate_5min` | `/ops/live-freshness` | < 0.85 | < 0.95 | sample каждые 60s |

**Educational:** `oldest_hot_score_age_seconds` — главный live SLO. Это возраст самого "забытого" события в hot lane. После P0.B (sweeper) он держится 96s. Если пробьёт 300s — что-то застряло: либо sweeper встал, либо новые events заливаются быстрее чем мы их обрабатываем, либо upstream Sofascore лагает.

### P1 — Queue signals (Phase 2)

| Signal | Источник | Порог CRIT | Порог WARN |
| --- | --- | --- | --- |
| `XLEN stream:etl:hydrate` | `/ops/queues/summary` | > 5000 | > 1000 |
| `XLEN stream:etl:live_hot` | `/ops/queues/summary` | > 2000 | > 500 |
| `XLEN stream:etl:live_warm` | `/ops/queues/summary` | > 2000 | > 500 |
| `XLEN stream:etl:live_discovery` | `/ops/queues/summary` | > 1000 | > 200 |
| `XLEN stream:etl:discovery` | `/ops/queues/summary` | > 1000 | > 200 |
| Pending message age (max) | XPENDING per consumer-group | > 600s | > 300s |

**Educational:** XLEN — длина Redis Stream. Если растёт, значит workers не успевают консьюмить. Pending message age — насколько долго сообщение взято в работу, но не ack-нуто (worker может зависнуть на запросе).

### P1 — Job signals (Phase 3)

| Signal | Источник | Порог CRIT | Порог WARN |
| --- | --- | --- | --- |
| Failed jobs за последние 15 мин | `etl_job_run WHERE status='failed'` | > 50 | > 20 |
| Retry rate (% от total) за 15 мин | `etl_job_run` | > 5% | > 2% |
| Никаких jobs за последние 10 мин | `etl_job_run.started_at MAX()` | > 600s | > 300s |

**Educational:** "Никаких jobs за 10 мин" — это canary. Если планнеры легли, jobs просто перестают приезжать. Без этого сигнала мы бы видели "0 failed jobs" и думали что всё ок.

**Внимание:** для job signals нужен index на `etl_job_run.started_at`. Иначе query тяжёлый (см. ARCHITECTURE_AUDIT.md D.3). Сначала ставим index, потом включаем эти сигналы в Phase 3.

### P2 — Service health signals (Phase 4)

| Signal | Источник | Порог CRIT |
| --- | --- | --- |
| systemd unit `inactive`/`failed` | `systemctl is-active sofascore-*` | сразу |
| `/ops/health` non-200 | HTTP probe | сразу |
| Memory usage > 80% | `/proc/meminfo` | sustained 5 min |
| Disk usage > 85% | `df` | sustained 1 min |

### P2 — Database signals (Phase 4, optional)

| Signal | Источник | Порог |
| --- | --- | --- |
| Deadlocks за 15 мин | `pg_stat_database.deadlocks` | > 0 |
| Lock waits (transactions waiting) | `pg_stat_activity.wait_event_type='Lock'` | > 5 sustained 60s |
| Long-running transactions | `pg_stat_activity.xact_start < now() - 5min` | > 0 |

---

## Архитектура решения

### Pull-based daemon

```
sofascore-monitoring.service
  │
  ├─► every 60s
  │      │
  │      ├─► GET http://127.0.0.1:8000/ops/live-freshness     ──► SLO signals
  │      ├─► GET http://127.0.0.1:8000/ops/queues/summary     ──► Queue signals
  │      ├─► query etl_job_run via shared asyncpg pool         ──► Job signals (Phase 3)
  │      └─► systemctl is-active per unit (Phase 4)            ──► Service signals
  │
  ├─► classify each signal vs thresholds (OK / WARN / CRIT)
  │
  ├─► check dedupe key in Redis (HGET monitoring:dedupe <signal_id>)
  │      │
  │      └─► if last alert was < dedupe_ttl ago, skip
  │
  └─► send Telegram message (bot token + chat_id from env)
         └─► on success, SET monitoring:dedupe <signal_id> <timestamp> EX <ttl>
```

**Почему pull, не push:**
- Push требует instrumentation в каждом сервисе (changing planner, workers, normalize sink). Слишком много кода менять.
- Pull читает уже существующие `/ops/*` endpoints. Zero invasion в существующий код.
- Для 5K users одна VPS — pull достаточен. Push нужен когда сервисов 50+.

**Почему отдельный демон, не часть API:**
- API stateless — не должен иметь long-running loops.
- Если API упадёт, monitoring должен заметить это и алертить → значит monitoring должен жить вне API.
- systemd unit independent от `sofascore-api.service`.

### Где живёт код

```
schema_inspector/
  monitoring/
    __init__.py
    daemon.py              # MonitoringDaemon class with tick loop
    signals.py             # signal definitions (thresholds, classifier)
    alerter.py             # AlertSink interface + TelegramAlertSink
    dedupe.py              # Redis-backed dedupe (HSET with TTL)
  cli.py
    monitoring-daemon subcommand → MonitoringDaemon
tests/
  test_monitoring_daemon.py
  test_monitoring_signals.py
  test_monitoring_alerter.py
ops/systemd/
  sofascore-monitoring.service
```

### Зависимости

- `httpx` (async HTTP client) — для GET `/ops/*` endpoints. Уже есть в проекте.
- `asyncpg` — для DB-based signals (Phase 3). Уже есть.
- `redis.asyncio` — для dedupe. Уже есть.
- **Никаких новых третьесторонних пакетов** для core. Telegram через простой HTTP POST на `https://api.telegram.org/bot<TOKEN>/sendMessage`, без python-telegram-bot library.

---

## Severity levels

Три уровня. Не пять. Не семь.

| Level | Реакция | Telegram |
| --- | --- | --- |
| `OK` | ничего | — |
| `WARN` | сообщение в Telegram, but raw | one message, no repeat |
| `CRIT` | сообщение в Telegram, в начале `🚨 CRIT:` | repeats every `crit_repeat_ttl` (по умолчанию 30 мин) пока сигнал в `CRIT` |

**Educational:** Различие между WARN и CRIT — WARN это "что-то не так, посмотри когда сможешь", CRIT это "посмотри сейчас, продукт ломается". CRIT повторяется (anti-snooze), WARN — один раз чтоб не спамить.

Никакого PAGE уровня (буди в 3 ночи). Пока нет on-call ротации — все алерты в один Telegram-чат, Бобур читает когда увидит.

---

## Alert dedupe / rate-limit

**Проблема:** если `oldest_hot_score_age` пробил 300s, и проблема не решилась 30 минут, мы не хотим получить 30 одинаковых алертов.

**Решение:** Redis HSET `monitoring:dedupe`:
- key: `<signal_id>:<severity>` (например, `oldest_hot_score_age:CRIT`)
- value: timestamp последнего отправленного алерта
- TTL для WARN: 600s (10 мин)
- TTL для CRIT: 1800s (30 мин)

**Логика:**
```python
last_sent = redis.hget("monitoring:dedupe", f"{signal_id}:{severity}")
if last_sent is None or (now - last_sent) > ttl:
    send_telegram(message)
    redis.hset("monitoring:dedupe", f"{signal_id}:{severity}", now)
    redis.expire("monitoring:dedupe", max_ttl)
```

**Recovery alert:** когда сигнал вернулся в `OK`, отправляем "✅ RESOLVED: oldest_hot_score_age back to <120s" и **сбрасываем dedupe** для этого сигнала. Это даёт closure — Бобур видит "проблема ушла" без необходимости проверять руками.

---

## Telegram bot setup

### Что нужно от Бобура (one-time)

1. Открыть `@BotFather` в Telegram.
2. `/newbot` → имя бота (например, `sofascore_monitoring_bot`).
3. BotFather даст **BOT_TOKEN** (формат: `1234567890:ABC...xyz`).
4. Открыть свой профиль или создать группу, добавить бота туда.
5. Открыть `@userinfobot` → получить **CHAT_ID** (числовой, для группы — отрицательный).

### Где хранить credentials

В `.env`:
```
SOFASCORE_MONITORING_TELEGRAM_BOT_TOKEN=1234567890:ABC...xyz
SOFASCORE_MONITORING_TELEGRAM_CHAT_ID=-1001234567890
SOFASCORE_MONITORING_ENABLED=1
SOFASCORE_MONITORING_INTERVAL_SECONDS=60
SOFASCORE_MONITORING_DEDUPE_WARN_TTL_SECONDS=600
SOFASCORE_MONITORING_DEDUPE_CRIT_TTL_SECONDS=1800
SOFASCORE_MONITORING_BASE_URL=http://127.0.0.1:8000
```

**Безопасность:** `.env` в `.gitignore` (проверить). BOT_TOKEN надо ротировать если случайно попал в git.

### Формат сообщений

**WARN:**
```
⚠️ WARN: oldest_hot_score_age_seconds
Value: 156s (threshold: 120s)
Time: 2026-05-14 18:23:45 UTC
Host: sofascore-prod
Run: /ops/live-freshness
```

**CRIT:**
```
🚨 CRIT: oldest_hot_score_age_seconds
Value: 412s (threshold: 300s)
Time: 2026-05-14 18:24:00 UTC
Host: sofascore-prod
Run: /ops/live-freshness

This is repeat #3 (first at 18:00:00).
```

**RESOLVED:**
```
✅ RESOLVED: oldest_hot_score_age_seconds
Was: CRIT @ 412s
Now: 98s
Duration: 24 min
```

Markdown отключить (`parse_mode=None`), чтобы случайные `*` или `_` в сообщениях не ломали отправку.

---

## Phasing (как разворачиваем)

### Phase 1: Skeleton + SLO signals (1-2 дня)

**Что:**
- `schema_inspector/monitoring/daemon.py` — class `MonitoringDaemon` with tick loop
- `signals.py` — 3 SLO signal definitions
- `alerter.py` — `TelegramAlertSink` (HTTP POST)
- `dedupe.py` — Redis-backed dedupe
- CLI: `python -m schema_inspector.cli monitoring-daemon --consumer-name local-test`
- Tests: 8-12 штук покрывают: signal classification, dedupe logic, alerter error handling, daemon loop tick

**Definition of Done:**
- Локально запускается, читает `/ops/live-freshness`, классифицирует
- Telegram message приходит при синтетическом нарушении (мокнем `oldest_hot_score_age` через temporary env var override)
- Dedupe работает (второй такой же alert не отправляется)
- Pytest суит зелёный

### Phase 2: Queue signals (1 день)

**Что:**
- Добавляем чтение `/ops/queues/summary`
- 5 новых сигналов (XLEN per stream + pending age)
- Tests для каждого

**Definition of Done:**
- Симулируем XLEN > порог через локальный Redis → alert приходит

### Phase 3: Job signals (1-2 дня + миграция)

**Что:**
- Сначала миграция: `2026-05-XX_etl_job_run_started_at_index.sql` → `CREATE INDEX CONCURRENTLY idx_etl_job_run_started_at ON etl_job_run (started_at)`
- Job query через **отдельный** asyncpg pool (не разделяет соединение с API — не блокирует `/ops/health`)
- 3 новых сигнала (failed jobs, retry rate, no-jobs canary)
- Tests

**Definition of Done:**
- Index есть в проде (`\d etl_job_run`)
- Query runs in <100ms (replace 30s scan)
- Сигналы приходят при синтетических failed jobs

### Phase 4: Deploy + tune (1 день)

**Что:**
- systemd unit `ops/systemd/sofascore-monitoring.service`
- Push to main → pull on server → `sudo systemctl enable --now sofascore-monitoring`
- Первые 48 часов — наблюдаем тюнингу пороги по реальным значениям
- README в `docs/runbooks/monitoring.md` (как silence, как пересмотреть пороги, как добавить новый сигнал)

**Definition of Done:**
- Демон 7 дней без падений
- Минимум 1 синтетический alert получен (для smoke test канала)
- Минимум 1 реальный alert получен (для подтверждения что сигналы корректны)
- Пороги откалиброваны по реальным данным

---

## systemd unit

```ini
# ops/systemd/sofascore-monitoring.service
[Unit]
Description=Sofascore Monitoring Daemon
After=network.target sofascore-api.service
PartOf=sofascore.target

[Service]
Type=simple
User=sofascore
WorkingDirectory=/opt/sofascore
EnvironmentFile=/opt/sofascore/.env
ExecStart=/opt/sofascore/.venv/bin/python -m schema_inspector.cli monitoring-daemon --consumer-name prod-monitoring
Restart=on-failure
RestartSec=10s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

**Educational:** `Restart=on-failure` означает systemd сам поднимет демон если он крашнется. `RestartSec=10s` — пауза перед рестартом, чтоб не loop-нуть мгновенно при упорной поломке.

---

## Тесты

```python
# tests/test_monitoring_signals.py
- test_oldest_hot_age_classification_below_warn
- test_oldest_hot_age_classification_warn_zone
- test_oldest_hot_age_classification_crit_zone
- test_tier_1_blocked_rate_classification
- test_refresh_success_rate_classification (inverted: lower = worse)
- test_signal_with_none_value_is_ok  # P0.C contract: None ≠ violation

# tests/test_monitoring_dedupe.py
- test_first_alert_sent
- test_repeat_alert_within_ttl_suppressed
- test_repeat_alert_after_ttl_sent
- test_resolved_clears_dedupe_for_signal
- test_warn_to_crit_escalation_sends_new_alert  # severity changed
- test_redis_failure_does_not_crash_daemon  # resilience

# tests/test_monitoring_alerter.py
- test_telegram_post_includes_bot_token
- test_telegram_post_includes_chat_id
- test_telegram_post_failure_logged_not_raised
- test_alerter_disabled_when_token_missing

# tests/test_monitoring_daemon.py
- test_daemon_tick_reads_ops_endpoint
- test_daemon_tick_classifies_and_alerts
- test_daemon_loop_continues_on_signal_fetch_error
- test_daemon_loop_continues_on_alerter_error
- test_daemon_respects_interval_seconds
```

Цель: 15-20 тестов в Phase 1. Не overkill, но coverage по dedupe и alerter resilience должен быть полным — это самые "тихие" failure modes.

---

## Risks

| Риск | Митигация |
| --- | --- |
| Telegram API упадёт / rate-limit | Alerter swallows exception, logs warning. Демон продолжает loop. На следующем tick попробует снова. |
| Redis упадёт (dedupe не доступен) | Dedupe возвращает "send" (fail-open). Лучше получить дубль алертов, чем пропустить. |
| Сам monitoring daemon крашнется | systemd `Restart=on-failure` поднимет. Если будет crash-loop — мы это увидим через `journalctl`. Но *кто проследит за монитором?* — см. ниже. |
| Watchdog watchdog (кто следит за monitor?) | На старте: ничего. В Эпохе 2: добавить `dead_man_switch` на `healthchecks.io` (бесплатный сервис) — daemon пингует URL каждый tick, healthchecks.io алертит email если 10 мин без пинга. |
| Алерты слишком шумные | Тюнингуем пороги в Phase 4. Если 80% алертов — false positive, пересматриваем threshold вверх. Если упускаем real incidents, пересматриваем вниз. |
| BOT_TOKEN утечёт | `.env` в `.gitignore`. Если утечёт — `/revoke` в BotFather, заменить TOKEN в `.env`. |
| Database query для job signals тяжёлый | Phase 3 включает миграцию для index *до* активации сигнала. Без index — сигнал не активируем. |

---

## Открытые вопросы (ждут ответа от Бобура)

1. **Канал Telegram: личный чат с ботом или группа?**
   - Если личный — только Бобур видит, проще.
   - Если группа — можно добавлять co-founders / future team.
   - **Рекомендация:** группа с одним участником (Бобур) на старте — расширяемо без миграции.

2. **Хочется ли silence-команды через Telegram?**
   - `/silence oldest_hot_score_age 1h` — заглушить конкретный сигнал на час.
   - Это +50% сложности в Phase 4. Можно отложить до Phase 5.
   - **Рекомендация:** отложить. Если нужен silence, можно временно `SOFASCORE_MONITORING_ENABLED=0` и рестартнуть unit.

3. **healthchecks.io watchdog watchdog?**
   - Бесплатно, простой URL ping.
   - Альтернатива: cron на втором VPS (но у нас нет второго).
   - **Рекомендация:** да, добавить в Phase 4 — две минуты setup, спасает от blind spot "кто следит за монитором".

4. **Тюнинг порогов: где задокументировать?**
   - Здесь в этом плане? Или отдельный `docs/runbooks/monitoring.md`?
   - **Рекомендация:** этот документ — план + threshold rationale. После запуска — отдельный `runbooks/monitoring.md` для оперативных деталей (как silence, как добавить сигнал, как читать алерт).

5. **Что делать с jobs signals если index migration требует downtime?**
   - `CREATE INDEX CONCURRENTLY` не требует downtime, но тратит CPU.
   - Запуск в quiet окно? (нет такого — live 24/7).
   - **Рекомендация:** запуск в любое время, мониторим `pg_stat_progress_create_index`, прекращаем если CPU >80%.

---

## Связанные документы

- `docs/PROJECT_VISION.md` §4 (главный pain #2) и §6 (Эпоха 1 → N1)
- `docs/ARCHITECTURE_AUDIT.md` D.3 (etl_job_run index gap)
- `docs/OPERATIONS_RUNBOOK.md` — после реализации сюда добавить раздел "monitoring"
- `schema_inspector/local_api_server.py` — `/ops/live-freshness`, `/ops/queues/summary` endpoints (источники сигналов)
- `CLAUDE.md` — git workflow, deploy flow

---

## После ACK на план — что я начну делать

1. Создать структуру `schema_inspector/monitoring/` (4 модуля).
2. Написать `signals.py` с тремя SLO signal definitions + thresholds из env.
3. Написать `alerter.py` с `TelegramAlertSink` (httpx POST).
4. Написать `dedupe.py` (Redis-backed).
5. Написать `daemon.py` с tick loop.
6. CLI subcommand `monitoring-daemon` в `cli.py`.
7. Тесты (15-20 штук, Phase 1).
8. Локальный smoke test (нужен будет BOT_TOKEN от Бобура).
9. Commit + push (без deploy).
10. Ждать ACK на Phase 2 (queue signals).

---

*План — Phase 1. Phases 2-4 после успешной Phase 1. Tuning thresholds по реальным данным — не по моим guess'ам.*
