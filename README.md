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

## Ограничения

- Сейчас поддерживается только `GET`
- Генератор строит infer-схему по фактическому ответу, а не по OpenAPI/Swagger
- База PostgreSQL пока не создается: этот шаг будет следующим после накопления отчетов по endpoint'ам
