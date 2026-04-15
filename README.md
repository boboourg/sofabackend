# API Schema Inspector

Минимальный проект для одной задачи: дать URL JSON API и получить Markdown-отчет со схемой ответа.

## Что делает

- отправляет `GET` по URL
- отправляет все запросы через единую transport-архитектуру
- парсит JSON-ответ
- рекурсивно определяет:
  - типы полей
  - обязательность полей
  - nullable
  - вложенные объекты
  - массивы и их item schema
  - candidate key fields вроде `id`, `eventId`, `team_id`, `slug`
- сохраняет отчет в `reports/<url_name>.md`

## Сетевая архитектура

Все запросы идут через один и тот же слой:

- `schema_inspector.runtime`
  Конфиг запроса, retry policy, TLS policy, proxy endpoints
- `schema_inspector.proxy`
  Proxy pool с cooldown после ошибок
- `schema_inspector.transport`
  Единый transport для HTTP(S) и `file://`
- `schema_inspector.challenge`
  Детекция `429`, `401`, `403` и challenge-like HTML
- `schema_inspector.fetch`
  JSON fetch поверх transport-слоя

Что это дает:

- все URL обрабатываются одной архитектурой
- можно подключить один или несколько proxy URL
- есть retry/backoff
- есть verified TLS через стандартный SSL stack
- в Markdown-отчете видно число попыток, финальный proxy и challenge status

Важно:

- здесь нет CAPTCHA bypass
- здесь нет spoofed TLS fingerprinting
- здесь нет anti-bot обхода защит

То есть проект использует transport-архитектуру с прокси и TLS, но не занимается обходом защит.

## Запуск

```powershell
python inspect_api.py "https://example.com/api/player/123"
```

С заголовками:

```powershell
python inspect_api.py "https://example.com/api/player/123" --header "Authorization=Bearer TOKEN"
```

С прокси:

```powershell
python inspect_api.py "https://example.com/api/player/123" --proxy "http://login:password@host:port"
```

С несколькими прокси и retry:

```powershell
python inspect_api.py "https://example.com/api/player/123" `
  --proxy "http://proxy-1.local:8080" `
  --proxy "http://proxy-2.local:8080" `
  --max-attempts 4
```

С кастомной папкой:

```powershell
python inspect_api.py "https://example.com/api/player/123" --outdir output
```

## Выходной файл

На выходе будет файл вида:

```text
reports/api_example_com_api_player_123.md
```

Внутри:

- source URL
- top-level type
- network attempts
- final proxy
- challenge detection result
- список сущностей
- suggested PostgreSQL table names
- candidate primary keys
- таблицы полей с типами и заметками по вложенности

## Переменные окружения

- `SCHEMA_INSPECTOR_PROXY_URL`
- `SCHEMA_INSPECTOR_PROXY_URLS`
- `SCHEMA_INSPECTOR_USER_AGENT`
- `SCHEMA_INSPECTOR_MAX_ATTEMPTS`
- `SCHEMA_INSPECTOR_BACKOFF_SECONDS`
- `SCHEMA_INSPECTOR_TLS_MIN_VERSION`
- `SCHEMA_INSPECTOR_TLS_MAX_VERSION`
- `SCHEMA_INSPECTOR_TLS_CHECK_HOSTNAME`

## Тесты

```powershell
python -m unittest discover -s tests -p "test_*.py"
```

## PostgreSQL Setup

Проект теперь умеет сам подготовить PostgreSQL:

- создать целевую БД, если она отсутствует
- на пустую БД накатить `postgres_schema.sql`
- затем применить все SQL-файлы из `migrations/`
- не применять уже зафиксированные миграции повторно

Базовый запуск:

```powershell
.\.venv311\Scripts\python.exe setup_postgres.py
```

Если БД уже есть и создавать ее не нужно:

```powershell
.\.venv311\Scripts\python.exe setup_postgres.py --skip-create-database
```

После подготовки БД можно снова запускать пайплайны, например targeted smoke-run:

```powershell
.\.venv311\Scripts\python.exe load_targeted_pipeline.py --unique-tournament-id 17 --season-id 76986 --team-id 35 --player-id 288205 --timeout 20
```

## Local Swagger

Локальный Swagger/OpenAPI для футбольного среза можно сгенерировать из текущего состояния БД:

```powershell
.\.venv311\Scripts\python.exe build_local_swagger.py
```

Это создаст:

- `local_swagger/football.openapi.json`
- `local_swagger/index.html`

Чтобы открыть локально через HTTP:

```powershell
.\.venv311\Scripts\python.exe serve_local_swagger.py --port 8088
```

После этого Swagger UI будет доступен по адресу:

```text
http://127.0.0.1:8088/
```

## Local API Server

Если нужен не только статический Swagger, а именно живые локальные `/api/v1/...` ручки из PostgreSQL, поднимай сервер так:

```powershell
.\.venv311\Scripts\python.exe serve_local_api.py --host 127.0.0.1 --port 8000
```

Что он дает:

- `http://127.0.0.1:8000/` — Swagger UI
- `http://127.0.0.1:8000/openapi.json` — live OpenAPI JSON
- `http://127.0.0.1:8000/api/v1/...` — сами локальные пути в Sofascore-style формате

Важно:

- сервер читает данные из `api_payload_snapshot` в PostgreSQL
- если конкретный путь или query еще ни разу не были ingested в базу, локальный API вернет `404`
- для query-driven statistics endpoint без query-параметров сервер пытается вернуть самый свежий сохраненный snapshot для этого сезона

## Ограничения

- Сейчас поддерживается только `GET`
- Генератор строит infer-схему по фактическому ответу, а не по OpenAPI/Swagger
- Локальный PostgreSQL сервер и учетная запись из `SOFASCORE_DATABASE_URL` должны уже существовать
