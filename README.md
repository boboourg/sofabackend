# Sofascore Backend

ETL/API стек для Sofascore-подобных спортивных данных. Парсит данные из Sofascore (HTTP + WebSocket), нормализует в PostgreSQL, отдаёт через локальный API в 1:1 формате Sofascore, плюс WebSocket-mirror для фронта.

> **Источник правды — [CLAUDE.md](CLAUDE.md).** Этот README — только короткий quickstart.

## Стек

- Python 3.11 (asyncpg, FastAPI, httpx, curl_cffi)
- PostgreSQL 16
- Redis 7
- 36 systemd unit'ов (см. [ops/systemd/](ops/systemd/))

## Quickstart

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Postgres
createdb sofascore_schema_inspector
psql -d sofascore_schema_inspector -f postgres_schema.sql
for m in migrations/*.sql; do psql -d sofascore_schema_inspector -f "$m"; done

# Запуск local API
python -m schema_inspector.local_api_server --host 127.0.0.1 --port 8000

# Health check
curl http://127.0.0.1:8000/ops/health
```

## Основные команды

```bash
# Один-shot
python -m schema_inspector.cli health
python -m schema_inspector.cli event --sport-slug football --event-id 14083191
python -m schema_inspector.cli live --sport-slug football
python -m schema_inspector.cli scheduled --sport-slug football --date 2026-05-24

# Continuous (требует Redis)
python -m schema_inspector.cli planner-daemon
python -m schema_inspector.cli worker-discovery --consumer-name discovery-1
python -m schema_inspector.cli worker-hydrate --consumer-name hydrate-1

# WebSocket
python -m schema_inspector.cli ws-consumer   # Sofascore → нам
python -m schema_inspector.cli ws-server     # нам → фронт (127.0.0.1:8001)
```

Полный каталог: [docs/CLI_AND_SCRIPTS.md](docs/CLI_AND_SCRIPTS.md).

## Тесты

```bash
pytest -q                              # 304 файла
pytest tests/test_local_api_server.py -q
```

## Архитектура

```
planner-daemon / live-discovery-planner / historical-planner
   → Redis Streams (15 потоков)
   → workers (discovery → hydrate → live tier_1/2/3 / historical)
   → ParserRegistry (27 семейств)
   → normalize_repository → PostgreSQL
   → local_api_server (FastAPI на 8000) + /ops/*

Параллельно:
wss://ws.sofascore.com:9222 → ws-consumer → нормализация дельт → БД
                                          → Redis pub/sub → ws-server → wss://api.var11.com/ws/v1
```

Подробнее: [CLAUDE.md](CLAUDE.md) + [docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md).

## Документация

| Файл | О чём |
|---|---|
| [CLAUDE.md](CLAUDE.md) | **Главный источник правды**: архитектура, точки входа, дубликаты, известные баги, roadmap |
| [docs/PROJECT_VISION.md](docs/PROJECT_VISION.md) | Бизнес-цели, SLO, эпохи разработки |
| [docs/ARCHITECTURE_AUDIT.md](docs/ARCHITECTURE_AUDIT.md) | Live risk register |
| [docs/SIMPLIFICATION_AUDIT.md](docs/SIMPLIFICATION_AUDIT.md) | God-objects + refactor план |
| [docs/PERFORMANCE_AUDIT_2026-05-20.md](docs/PERFORMANCE_AUDIT_2026-05-20.md) | DB performance issues |
| [docs/OPERATIONS_RUNBOOK.md](docs/OPERATIONS_RUNBOOK.md) | Production playbook |
| [docs/FRONTEND_WS_GUIDE.md](docs/FRONTEND_WS_GUIDE.md) | WebSocket protocol для фронта |

## Принципы (важно)

1. Точка входа — только `python -m schema_inspector.cli <subcommand>`. **Никаких `load_*.py` shim в корне.**
2. Live-first. Historical lanes выключаются при нагрузке.
3. Raw `api_payload_snapshot` — canonical для 1:1 shape API.
4. Migrations — append-only, `YYYY-MM-DD_description.sql`.
5. Deploy = push в `main` → pull на сервере. Без feature branches.
6. Перед merge — `pytest -q` должен проходить полностью.

Полные правила: [CLAUDE.md §8](CLAUDE.md).

## Правила для AI-агентов

**Все sub-агенты, которых вызывает Claude Code в этом репозитории, обязаны запускаться на модели `opus` (Opus 4.7 / max).** Sonnet и Haiku не годятся для архитектурных решений в этом проекте — слишком много контекста, god-objects, и тонких политик (live tier dispatch, isEditor 3-layer ban, capability gating, hot-row contention).

Конкретно:
- При вызове `Agent` (или `TaskCreate` / Task tools) — всегда передавать `model: "opus"` явно, даже если parent работает на sonnet.
- Это применяется к `Explore`, `general-purpose`, `Plan`, `code-review`, `claude-code-guide` и любым другим subagent_type'ам.
- Исключение можно делать только для тривиальных задач (например, найти один файл по точному имени) — но по умолчанию **всегда opus**.

Почему: ошибка в hydrate-worker race / live dispatch / pool sizing стоит дороже, чем разница в стоимости модели. Один неверный архитектурный совет от sonnet может породить часы дебага.

