# PROJECT_VISION

**Документ-якорь для проекта Sofascore-like ETL/API/Mobile/Web.**

| Поле | Значение |
| --- | --- |
| Версия | **v2** (2026-05-14, evening update) |
| Автор | Бобур (vision), Claude (текст) |
| Статус | Living document — обновляется когда меняется vision, scope или приоритеты |
| Зачем | Зафиксировать ответы на 11 вопросов о проекте, чтобы Claude (и сам автор) не теряли контекст между сессиями |
| Что в v2 | (1) Главная P0 боль уточнена — гипотеза "decompose persist" оказалась неточной; реальная причина была sweeper regression от P0.B, fixed коммитом 724d77f. (2) Defenses таблица в §5 пополнена. (3) Эпоха 1 N2 закрыта. (4) Новый раздел §11 "Lessons / Hypothesis updates" — зафиксированы методологические уроки. |

---

## TL;DR (30-секундное чтение)

Мы строим **коммерческий Sofascore-подобный продукт** (mobile app + web frontend + публичное API). Не хобби, не proof-of-concept — **стартап для работы**, целевая аудитория 5000 пользователей.

**Главное обещание:** live-данные с задержкой ≤5 секунд, downtime >30 минут — катастрофа.

**Главная боль на 2026-05-14 утром:** live-матчи не переходят в статус "live". **На 2026-05-14 вечером:** реальная корневая причина выявлена через N1 monitoring + Phase 0 diagnose — это была регрессия P0.B sweeper (`AttributeError` каждую минуту в planner, sweeper не запускался). Fix задеплоен (commit 724d77f), `oldest_hot_score_age_seconds` упал с 970s до 97s. **Гипотеза "decompose live-discovery persist" из v1 этого документа оказалась неверной** — см. §11.

**Mode сотрудничества с Claude:** implement + educate. Я (Claude) реализую решения и **попутно объясняю**, чтобы Бобур учился вдоль пути. Без излишней теории, без длинных нотаций — пояснения короткие, по делу.

**Текущая фаза (Эпоха 1, 1-2 недели):** N1 monitoring **готов и в проде** (commit `fd59951` + `c473276`). N2 переосмыслен — **гипотеза была устаревшей**, реальный fix занял one-line edit + 2 теста (commit `724d77f`). Эпоха 1 фактически закрыта раньше, чем планировалось. Следующий шаг — N3 sentinel probe (защита от тихих регрессий Sofascore) или переход к Эпохе 2.

---

## 1. Что мы строим

### Продукт (явное)

- **Mobile app** (Sofascore-like UX): live-результаты, расписания, статистика игроков, history.
- **Web frontend** (Sofascore-like): тот же охват, browser-first.
- **Публичное API** Sofascore-формата (`/api/v1/event/{id}`, `/api/v1/sport/...` и т. д.) — *drop-in replacement*, чтобы любой клиент Sofascore мог переключиться на нас, поменяв только base URL.

### Цели стартапа

- **5000 активных пользователей** на горизонте 6-12 месяцев.
- **Запуск в продакшене** (HTTPS, auth, rate-limit) — пока не сделан, текущий API работает на `127.0.0.1:8000` за SSH-туннелем.
- **Wikipedia-level history**: все 12 спортов, все лиги, исторические сезоны для архива/статистики. Не just live.

### Любимый источник вдохновения

[sofascore.com](https://www.sofascore.com) — Бобур использует его сам, считает референсом UX и data shape. Поэтому API возвращает payload **1:1 shape** (включая `fieldTranslations`, `userCount`, `coverage`, `changes`, и т. д. — даже то, что мы не нормализуем).

---

## 2. SLO и Constraints

### Жёсткие пороги (catastrophic, MUST hold)

| Метрика | Порог | Что произойдёт при нарушении |
| --- | --- | --- |
| Live data lag | ≤5 секунд | Пользователь видит устаревший счёт → продукт ломается, репутация падает |
| Downtime окно | <30 минут | Пользователи переключаются обратно на Sofascore, retention падает |
| Live coverage | 100% активных матчей | Матч, который не виден в live-режиме = недостоверный продукт |

### Жёлтые пороги (degraded, можно жить временно)

| Метрика | Порог | Реакция |
| --- | --- | --- |
| `oldest_hot_score_age_seconds` | <300s (5 мин) | После P0.B сейчас ~96s; цель — стабильно <60s |
| `tier_1_blocked_rate` | <20% | Сейчас 81% — это всё ещё много, надо ниже |
| `retry_rate` discovery | <2% | Сейчас 0.32% (хорошо) |
| Hydrate lag (`stream:etl:hydrate` XLEN) | <500 | Зависит от нагрузки |

### Бюджет

- **Smartproxy:** $300/мес, 5 endpoints (residential rotation). Главная статья расходов.
- **VPS:** single-server сейчас, всё на одном хосте (PostgreSQL + Redis + Python).
- **Время инженерное:** Бобур готов тратить **~1 неделю/месяц** на hardening (не full-time). Темп ускоренный (для работы).

### Технические ограничения

- Python 3.11, asyncpg, FastAPI, Redis 7.
- PostgreSQL 16, single instance, localhost.
- Без k8s, без сервис-меша, без облака — bare metal / VPS.
- Windows (dev) + Linux (prod, `sofascore-prod` SSH alias).

---

## 3. Scope

### IN scope

- **Все 12 спортов** Sofascore-формата (football, basketball, tennis, ice-hockey, volleyball, handball, baseball, american-football, rugby, cricket, mma, e-sports — список из реестра sport_registry).
- **Все лиги/туры/категории** — без cherry-pick.
- **Все типы данных** Sofascore-формата:
  - live + scheduled + finished events;
  - lineups, statistics, incidents, commentary;
  - player career, season stats, attribute overviews;
  - team info, transfer history, squad;
  - tournament structure, standings, knockout brackets;
  - venue, country/category metadata;
  - **media** (только метаданные/ссылки, не сами файлы).
- **Historical archive** — Wikipedia-уровень, года назад.
- **Snapshot-based API**: raw `api_payload_snapshot` остаётся canonical для 1:1 shape (см. `local_api_server.py`).

### OUT of scope (явно не делаем)

- **Хранение image/media файлов** — фронтенд строит URL из `https://img.sofascore.com/api/v1/<entity>/<id>/image|flag` напрямую.
- **Сложный CDN, edge-кэш на старте** — нет 5K пользователей пока.
- **Multi-region deploy** — единый VPS до тех пор, пока SLO держится.
- **Real-time push (WebSocket)** — пока pull-based polling, WS будет после стабилизации pull-пути.
- **Microservices split** — текущий monorepo достаточен; разбиение преждевременно.

---

## 4. Главные боли (по приоритету)

### P0 (катастрофа сейчас)

1. **Live-матчи не переходят в статус "live".** ✅ **Закрыто 2026-05-14 вечером.** Пайплан ручек не отрабатывал в realtime: `event.status.type = inprogress` upstream → наш `/api/v1/event/{id}` показывал stale данные.
   - **Гипотеза v1 (неверная):** `live_discovery_planner` персистит большие батчи в одной транзакции → блокировки. Reconnaissance показал что **такой транзакции в коде нет** — planner просто publishes в Redis streams.
   - **Реальная корневая причина:** Регрессия P0.B sweeper deploy (commit `97ce25c`, 2026-05-14 утром). Callback в `ServiceApp._build_live_state_sweep_wiring` обращался к `self.database`, но database живёт на `self.app.database`. Planner логировал `AttributeError` каждую минуту — sweeper **никогда не выполнял ни одной cycle**. Finalised events накапливались в `zset:live:hot`, `oldest_hot_score_age` пробил 970s.
   - **Fix:** commit `724d77f` — one-line edit `self.database` → `self.app.database` + 2 integration теста, которые would have поймали bug. После deploy: `oldest_hot_score_age` 970s → 97s (~10× reduction).
   - **Как нашли:** N1 monitoring через 30 минут после старта поднял WARN на `oldest_hot_score_age`. Phase 0 diagnose (15 мин чтения логов планнера) сразу выявил `AttributeError` в journal.
   - **Defenses теперь стоят:** firebreak `endpoint_capability_rollup`, tier_1 lease waterfall, P0.B sweeper (теперь реально работает), `/ops/live-freshness` SLO endpoint, **N1 monitoring → Telegram канал FLOWSCORE MONITORING**.

2. **Никакого мониторинга.** ✅ **Закрыто 2026-05-14.** Был "никак" — Telegram alerts отсутствовали. Сейчас работает `sofascore-monitoring.service` (commit `fd59951`): 11 сигналов (3 SLO + 5 queue + 3 job, последние opt-in), Telegram bot `@flowscore_monitoring_bot`, dedupe в Redis, severity OK/WARN/CRIT. На этом мониторинге сразу же был обнаружен bug №1 выше.

### P1 (не катастрофа, но критично к запуску)

3. **Нет production deployment.** API на `127.0.0.1:8000`, доступ только по SSH. Нет HTTPS, нет auth, нет rate-limit. Запуск для пользователей невозможен.
4. **Нет sentinel probe**: Sofascore может тихо поменять URL или формат payload → наш парсер падает молча → данные пропадают, никто не замечает.
5. **Historical backfill program не запущен**: Wikipedia-level archive требует bulk-ingestion плана. Сейчас history лента работает только на текущих событиях.

### P2 (важно, но без срочности)

6. **Read scale для 5K пользователей**: текущая БД single-instance, нет cache layer, нет read replicas. Запас прочности 50-100 RPS, не 5000 users × peak rate.
7. **Snapshot strategy review**: для historical archive raw snapshots раздуваются. Вопрос — нормализовать ли history или хранить только raw.
8. **Other sports matchcenter parity**: football протестирован, остальные 11 спортов проверены частично.

---

## 5. Что уже сделано (defenses в production)

| # | Изменение | Что фиксит | Дата |
| --- | --- | --- | --- |
| 1 | Firebreak `endpoint_capability_rollup` (`SOFASCORE_INLINE_CAPABILITY_ROLLUP_ENABLED=0`) | Deadlock storm 12+ waiters → 0; live workers больше не блокируются на hot-row contention | 2026-05-12 |
| 2 | Tier_1 lease waterfall (LIVE_DISPATCH_LEASE_TIER_1_MS убран) | tier_1 `claim_blocked` 93.6% → 81% sustained; live matchcenter unlock | 2026-05-13 |
| 3 | X3/X4 патчи: isEditor 3-layer ban + live_delta fall-through | Matchcenter unlock для live football | 2026-05-13 |
| 4 | `GET /api/v1/unique-tournament/{id}/media` synthetic endpoint | Media tab в frontend больше не 404-ит | 2026-05-13 |
| 5 | P0.B: `LiveStateSweeper` (services/live_state_sweeper.py) | `oldest_hot_score_age` 5949s → 96s (62x improvement); finalized events не висят в zset:live:hot | 2026-05-14 |
| 6 | P0.C: `/ops/live-freshness` standalone SLO endpoint | `/ops/health` больше не таймаутит на 30s; SLO видно в 140ms | 2026-05-14 |
| 7 | Structured retry log (`exc_type`, `sqlstate`, `duration_ms`) | Можно понять *почему* retry, не only *что* | 2026-05-14 |
| 8 | `rebuild-capability-rollup` CLI subcommand | Альтернатива inline rollup — пересчёт rollup state из observations асинхронно | 2026-05-12 |
| 9 | Architecture audit v3 (`docs/ARCHITECTURE_AUDIT.md`) | Risk register с Evidence column (verified/inferred/unknown) | 2026-05-14 |
| 10 | Полная документация (10 файлов, ~3155 строк) в `docs/` | PROJECT_OVERVIEW, SERVICES_AND_WORKERS, CLI_AND_SCRIPTS, ENVIRONMENT, API_ROUTES, PARSING_AND_POLICIES, DATABASE_AND_STORAGE, REDIS_AND_QUEUES, FUNCTION_INDEX, OPERATIONS_RUNBOOK | 2026-05-13 |
| 11 | **N1 monitoring daemon** (commits `fd59951` + `c473276`) | 11 сигналов в FLOWSCORE MONITORING Telegram канал; dedupe в Redis; severity OK/WARN/CRIT; systemd unit `sofascore-monitoring.service`. **Сразу же поймал bug №12 ниже.** | 2026-05-14 |
| 12 | **Sweeper regression fix** (commit `724d77f`) | `oldest_hot_score_age` 970s → 97s. P0.B sweeper не выполнял ни одной cycle 20+ минут на проде; one-line edit + 2 integration теста. | 2026-05-14 |
| 13 | **BRIN index** `idx_etl_job_run_started_at_brin` (commit `fd59951` migration) | Time-windowed queries на `etl_job_run.started_at` без полного сканирования. Открывает путь к monitoring Phase 3 (job signals) — `SOFASCORE_MONITORING_JOB_SIGNALS_ENABLED=1`. | 2026-05-14 |

**Verified post-9h:**
- 0 deadlocks
- 0 live-discovery retries
- retry rate 0.32% (baseline был 2.8%, 8.7x улучшение)
- sustained `oldest_hot_score_age` 96s

---

## 6. Roadmap (по эпохам)

### Эпоха 1: Stabilize (1-2 недели) — **частично закрыта 2026-05-14**

Цель: остановить кровотечение из главной боли и поставить мониторинг.

| Tag | Item | Описание | Статус |
| --- | --- | --- | --- |
| N1 | **Monitoring daemon + Telegram alerts** | `sofascore-monitoring.service`, 11 сигналов (3 SLO + 5 queue + 3 job opt-in), dedupe в Redis, Telegram канал FLOWSCORE MONITORING. | ✅ **Done** 2026-05-14 (commits `fd59951`, `c473276`) |
| N2 | ~~**Decompose live-discovery persist**~~ → **Sweeper regression fix** | **Гипотеза была неверной** — такой транзакции в коде нет. Реальный fix: исправлен `AttributeError` в P0.B sweeper wiring. `oldest_hot_score_age` 970s → 97s. | ✅ **Done** 2026-05-14 (commit `724d77f`); подробности §11 |
| N3 | **Daily sentinel probe** | Cron: ходит на 5-10 опорных Sofascore URL, валидирует JSON schema. Алерт в Telegram если schema drift. Защита от тихих регрессий. | _pending_ |
| P0.X | Завершить остаток P0 из ARCHITECTURE_AUDIT.md | См. документ — там детали | partially done |

**Definition of Done для Эпохи 1:**
- SLO `oldest_hot_score_age` < 60s sustained 7 дней — **в процессе наблюдения** (текущее 97s)
- `tier_1_blocked_rate` < 50% — **cumulative metric**, требует `HDEL live:dispatch_metrics` для пересчёта (см. open items)
- Telegram alert приходит в течение 2 минут после нарушения SLO ✅
- Sentinel probe запущен и зелёный 7 дней — _pending_ (N3)
- `live матчи не переходят в live` voice-test от Бобура: "выглядит ок" — _pending verification_

### Эпоха 2: Productionize (1-2 месяца)

Цель: довести до публичного запуска.

| Tag | Item | Описание |
| --- | --- | --- |
| M1 | **Production deployment** | HTTPS (Let's Encrypt), Nginx reverse proxy, API auth (key или JWT), rate-limit (per-key) |
| M2 | **Historical backfill program** | Bulk-ingestion плана: за какие сезоны бэкфилл, как избежать DDoS на Sofascore, deduplication |
| M3 | **Read cache (Redis)** | API endpoints, которые повторно запрашиваются — TTL-кэш в Redis (event detail 30s, league standings 5 min, etc.) |
| M4 | **Snapshot strategy review** | Решить — нормализовать ли history или хранить только raw. Если raw — partitioning по году, retention для дешёвых tier. |
| P1 | Завершить P1 из ARCHITECTURE_AUDIT.md | См. документ |

**Definition of Done для Эпохи 2:**
- Публичный URL с HTTPS работает
- 100 beta-пользователей могут попробовать без поломок
- Историческая база покрывает основные турниры за 5 лет
- Read latency p95 < 200ms

### Эпоха 3: Scale (3-6 месяцев)

Цель: 5000 пользователей без катастроф.

- Read replicas PostgreSQL (1-2 реплики)
- CDN для статики (если будет статика)
- Многопрокси-pool, чтобы $300/мес хватало под 5K users
- Multi-region rollout (если будет нужно по latency)
- Realtime push (WebSocket) поверх pull-pipeline
- Coverage всех 12 спортов с parity (не just football)
- Mobile app в сторе
- Web frontend live

---

## 7. Принципы разработки (правила движения)

Эти принципы Claude должен помнить во всех взаимодействиях. Они **не** меняются от сессии к сессии.

1. **Live-first beats historical completeness.** Никогда не предлагать включить `historical-tournament` или `historical-enrichment` lanes без явного ask, пока live SLO нарушен. См. CLAUDE.md.

2. **Backpressure is intentional, not a bug.** `skip` / `defer` / `retry_scheduled` — это нормальные сигналы, не failure-modes. См. CLAUDE.md.

3. **Raw snapshot is canonical for 1:1 shape.** Не убирать `api_payload_snapshot` из waterfall в `local_api_server.py`. Synthesizers — fallback. См. CLAUDE.md.

4. **No image/media file storage.** URL строятся фронтендом из `https://img.sofascore.com/...`. См. CLAUDE.md.

5. **Migrations append-only.** Новый файл `YYYY-MM-DD_description.sql`, никогда не редактировать существующие.

6. **Deploy = push to main → pull on server.** Никаких PR-flow, никаких feature-branches на server. См. CLAUDE.md.

7. **Защитные изменения (firebreak / waterfall / sweeper) → сразу замерять через `/ops/live-freshness`.** Не верить "выглядит ок" — мерить.

8. **Educational comments в коде, когда что-то не очевидно.** Mode (g) = educate. Один-два предложения комментария вместо нотации.

9. **Никаких новых top-level скриптов в корне репо** для one-off задач. Всё через `python -m schema_inspector.cli <subcommand>`.

10. **`event_terminal_state.zombie_stale` ≠ конец матча.** Read-слой не должен на него полагаться. См. CLAUDE.md.

---

## 8. Mode сотрудничества с Claude

**Выбран mode (a)+(g): implement + educate.**

- (a) Claude реализует решения сам — пишет код, миграции, тесты, документацию.
- (g) Claude **попутно** объясняет:
  - что меняет и почему
  - какой trade-off видит
  - какое название паттерна (firebreak, waterfall, backpressure, etc.) — чтобы Бобур мог гуглить дальше
  - короткое "почему это работает" — 2-3 предложения, не лекция

**Что Claude НЕ делает по умолчанию:**
- Не пишет тесты-нотации без запроса
- Не открывает PR (deploy = push to main → pull)
- Не включает historical lanes без ask
- Не делает destructive ops без явного "ack"
- Не предлагает k8s / microservices / cloud — это не наша architecture

**Что Claude делает по умолчанию:**
- Замеряет до/после через `/ops/live-freshness`, `/ops/queues/summary`, `/ops/jobs/runs`
- Сначала проверяет prod (`ssh sofascore-prod`), потом утверждает "это уже сделано"
- Объясняет на русском (Бобур → русскоязычный)
- В коммитах указывает `Co-Authored-By: Claude <noreply@anthropic.com>`

---

## 9. Открытые вопросы (UNKNOWN, ждут уточнения)

Эти вопросы пока без ответов, но они важны для будущих эпох. Не блокеры для Эпохи 1.

1. **Кто endpoint authority когда Sofascore меняет URL?**
   - Сейчас: реестр в `endpoints.py`, ручной апдейт.
   - В Эпохе 2: автоматический detection через sentinel probe?
   - В Эпохе 3: webhook от наблюдателя?

2. **Какая стратегия для multi-language?**
   - Sofascore возвращает `fieldTranslations` — мы пробрасываем 1:1.
   - Но frontend для англоязычной аудитории или мульти?

3. **Историческая глубина = сколько лет?**
   - "Wikipedia-level" → 50+ лет в теории.
   - Но Sofascore сам не имеет данных за 1970-е по всем спортам.
   - Реалистично: 5-10 лет full coverage, дальше — best-effort.

4. **Monetization model**
   - Subscription? Freemium? API-as-a-service?
   - Это влияет на rate-limit стратегию и tier of users.

5. **Cluster Sofascore endpoints в "must / nice / archival"**
   - Какие endpoints обязательны для core experience?
   - Какие можно lag-ать на минуты без боли?
   - Сейчас все endpoints на одной полке.

6. **Disaster recovery план**
   - Что если упадёт VPS? PostgreSQL corruption? Smartproxy баннит нас?
   - Backup strategy? Hot standby? Read-only failover?

---

## 11. Lessons / Hypothesis updates

Раздел добавлен в v2 (2026-05-14). Каждый раз когда гипотеза о проекте обновляется на основе новых данных — фиксируем здесь. Это **анти-паттерн против** "вечно неактуальной документации": если PROJECT_VISION твердит что-то, что reconnaissance опроверг, мы это **явно** маркируем.

### Update #1 — N2 "decompose live-discovery persist" гипотеза была неверной (2026-05-14)

**Что v1 утверждал (§4 пункт 1):**
> Корневая причина (гипотеза): `live_discovery_planner` персистит большие батчи в одной транзакции → блокировки → backpressure → matchcenter не получает свежий snapshot вовремя.
>
> Что ещё надо: N2 — decompose live-discovery persist transaction (P0.A в audit).

**Что recon показал:**

1. `schema_inspector/services/live_discovery_planner.py` — **только publish** в `stream:etl:live_discovery`. Никаких DB-вызовов в hot-path.
2. `schema_inspector/services/planner_daemon.py` — **только publish** + live refresh schedule. Никакой `BEGIN/COMMIT`, никакой большой транзакции.
3. `schema_inspector/workers/discovery_worker.py` — **только publish** в hydrate/tier streams.
4. DB writes реально происходят дальше: в `DurableNormalizeSink.persist_parse_result` (storage/normalize_repository.py) — sequence of ~30 SQL ops. Но это **не "одна большая транзакция"**, и это запускается из `hydrate-worker`, а не из planner.

**Что было реальной болью:**
- Регрессия после P0.B sweeper deploy (commit `97ce25c`): callback в `ServiceApp._build_live_state_sweep_wiring` написан как `self.database.connection()` где `self` это `ServiceApp` instance, а `.database` живёт на `self.app.database`.
- Прод-planner логировал `AttributeError("'ServiceApp' object has no attribute 'database'")` каждую минуту. Sweeper **никогда не выполнял ни одной cycle** с момента deploy.
- Finalised events накапливались в `zset:live:hot` без удаления → `oldest_hot_score_age_seconds` пробил 970s.
- P0.B sweeper unit-тесты использовали моки, integration-уровень (ServiceApp wiring) не был покрыт.

**Что Fix:**
- Commit `724d77f`: one-line edit `self.database` → `self.app.database`.
- Добавлены 2 integration теста в `tests/test_service_app.py::LiveStateSweepWiringTests`, которые конструируют реальный ServiceApp и **вызывают callback** — это уровень который would have поймал bug изначально.
- После deploy на prod: `oldest_hot_score_age_seconds` 970s → 97s (~10× reduction).

**Методологические выводы:**

1. **Гипотезы стареют.** Когда vision-документ говорит про root cause со словами "(гипотеза)" — это **сигнал** что нужно verify через recon перед code.
2. **Unit tests + моки ≠ integration coverage.** P0.B sweeper тесты были полные и зелёные. Bug был в **call site** где closure обращался к `self.X`. Тип теста, который would have поймал: "construct the real wiring, invoke the wired callable end-to-end".
3. **Мониторинг проявил bug за минуты.** Без N1 daemon мы бы продолжили жить с дохлым sweeper неделями. N1 окупил себя на первом же тике.
4. **Mode (g) educate работает.** Я сначала **сказал** "стоп, гипотеза неверна, давай diagnose" вместо реакционно писать "decompose persist" код. Это спасло 4-8 часов работы в неверном направлении.

**Pattern для запоминания:**
- Если ты внедряешь зависимость через `self` в closure внутри service-application класса — **обязательно** напиши тест который вызовет closure после `__init__`. Mock receiver класса (ServiceApp) недостаточно если closure ссылается на атрибуты другого объекта (self.app.X).

### Update #2 — XLEN ≠ backlog (2026-05-14)

Когда N1 monitoring впервые сработал, я думал что `XLEN stream:etl:hydrate = 665 000` это backlog. На самом деле:

- `XLEN` — это **total messages ever published** в Redis stream. После ACK сообщения не удаляются автоматически.
- Реальный backlog = **`lag`** consumer group (новые сообщения, ещё не доставленные).
- При `pending_total = 0` и `lag = 0` — все сообщения обработаны, despite огромного XLEN.

**Следствие:** queue thresholds в monitoring config должны таргетить `lag`, не `XLEN`. Phase 2 был построен на XLEN потому что это что отдаёт `/ops/queues/summary`. Это **частично misleading** — но XLEN всё ещё полезен для **memory leak detection** (растёт бесконечно если нет XTRIM policy).

**Action item (вне scope сейчас):** добавить `lag` сигналы либо XTRIM policy в Redis (MAXLEN ~ 100k per stream). См. open items в §6.

---

## 10. Как этим документом пользоваться

### Когда читать
- При старте новой сессии Claude (после `/compact` или нового чата) — Claude читает `docs/PROJECT_VISION.md` сразу, чтобы вспомнить контекст.
- Когда Бобур не уверен "зачем мы это делали" — открыть и пересмотреть Эпоху, в которой он сейчас.
- Перед серьёзным архитектурным решением — свериться с принципами разработки (раздел 7).

### Когда обновлять
- Закрытие Эпохи → пометить items как done, обновить Definition of Done.
- Изменение vision (если решим добавить новый продукт, регион, спорт) → новая версия документа.
- Открытие нового UNKNOWN → раздел 9.
- Изменение mode сотрудничества → раздел 8.

### Версионирование
- `v1` (2026-05-14 утром): первая версия, по итогам 11-вопросного questionnaire.
- `v2` (2026-05-14 вечером): N1 monitoring deployed; N2 переосмыслен — гипотеза о "decompose persist" опровергнута через Phase 0 diagnose, реальный fix = sweeper regression (commit `724d77f`). Добавлен §11 для отслеживания обновлений гипотез.
- Следующие версии: `v3`, `v4` ... с указанием даты в шапке и кратким "что изменилось" в начале документа.

---

## Связанные документы

- **`docs/PROJECT_OVERVIEW.md`** — что и как работает в коде (рендер архитектуры)
- **`docs/ARCHITECTURE_AUDIT.md`** — risk register + roadmap P0/P1/P2/P3 с техническими деталями
- **`docs/OPERATIONS_RUNBOOK.md`** — что делать когда что-то сломалось
- **`CLAUDE.md`** (root) — short-form инструкции для Claude (не дублирует этот документ)
- **`NEXT_CHAT_CONTEXT.md`** — handoff между сессиями (rolling, чаще обновляется)

---

*Этот документ — якорь. Когда теряемся в деталях, возвращаемся к нему и проверяем: то, что мы делаем сейчас, продвигает нас в Эпохе 1, или это отвлекающая оптимизация?*
