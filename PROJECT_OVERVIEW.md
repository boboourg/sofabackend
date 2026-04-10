# Project Overview

## Назначение проекта

Текущий проект предназначен для одного конкретного сценария:

1. Получить URL, который возвращает JSON.
2. Выполнить запрос к этому URL через единый transport-слой.
3. Проанализировать структуру JSON-ответа.
4. Построить Markdown-отчет, который помогает понять:
   - какие сущности есть в ответе;
   - какие поля у этих сущностей;
   - какие типы у полей;
   - какие поля обязательные;
   - какие поля nullable;
   - где есть вложенные объекты и массивы;
   - какие поля похожи на candidate primary key;
   - как можно назвать будущие таблицы в PostgreSQL.

Сейчас проект не пишет данные в PostgreSQL и не парсит данные в БД. Он находится на подготовительном этапе: сначала мы изучаем реальные JSON-ответы, потом по накопленным отчетам проектируется схема PostgreSQL.

## Ключевая идея архитектуры

В проекте есть два больших слоя:

1. Сетевой слой.
   Он отвечает за то, как запрос выполняется: конфиг, прокси, TLS, retry, backoff, challenge detection.

2. Слой анализа схемы.
   Он отвечает за то, как уже полученный JSON преобразуется в структурированное описание сущностей и полей.

На выходе оба слоя сходятся в Markdown-отчет.

## Актуальная структура проекта

```text
C:\Users\bobur\Desktop\sofascore
|-- .env
|-- .gitignore
|-- inspect_api.py
|-- PROJECT_OVERVIEW.md
|-- README.md
|-- requirements.txt
|-- sports_data.db
|-- schema_inspector/
|   |-- __init__.py
|   |-- challenge.py
|   |-- cli.py
|   |-- fetch.py
|   |-- proxy.py
|   |-- report.py
|   |-- runtime.py
|   |-- schema.py
|   |-- service.py
|   `-- transport.py
`-- tests/
    |-- test_schema_inspector.py
    |-- fixtures/
    |   `-- sample_response.json
    |-- _output/
    |   `-- .gitkeep
    `-- tmpimw0cz_7
```

## Что делает каждый файл

### Корневой уровень

#### `inspect_api.py`

Это самый короткий entrypoint проекта.

Его задача:

- импортировать `main` из `schema_inspector.cli`;
- передать управление CLI-слою;
- завершить процесс с нужным exit code.

То есть `inspect_api.py` не содержит бизнес-логики. Это просто удобная точка запуска:

```powershell
python inspect_api.py "https://example.com/api/..."
```

#### `README.md`

Краткая пользовательская документация.

Описывает:

- зачем нужен проект;
- как запускать CLI;
- какие есть флаги;
- как подключать прокси;
- какие env-переменные поддерживаются;
- какие ограничения есть у текущей версии.

`README.md` нужен скорее как quick start, а `PROJECT_OVERVIEW.md` как инженерная документация.

#### `requirements.txt`

Сейчас в проекте не требуется внешних Python-зависимостей.

Файл фиксирует, что проект работает на стандартной библиотеке Python и не требует установки сторонних пакетов.

#### `.gitignore`

Исключает из отслеживания:

- `__pycache__`
- `.pyc`
- папку `reports/`
- сгенерированные `.md` внутри `tests/_output`

Это важно, потому что проект генерирует отчеты, а они не должны засорять репозиторий.

#### `.env`

Локальный конфиг проекта.

Используется модулем `schema_inspector.runtime` как дополнительный источник настроек.

Через `.env` можно задавать:

- proxy URL;
- список proxy URL;
- user-agent;
- retry/backoff;
- параметры TLS policy.

#### `sports_data.db`

На текущую архитектуру не влияет.

Это legacy-артефакт из старой версии проекта, где раньше использовалась SQLite-база для хранения данных парсера.
Сейчас код `schema_inspector` эту базу не читает и не пишет в нее.

Ее можно считать историческим остатком, который сохранен в рабочей папке, но не участвует в текущем execution flow.

### Пакет `schema_inspector`

Это основное ядро проекта.

#### `schema_inspector/__init__.py`

Публичный API пакета.

Экспортирует:

- `inspect_url_to_markdown`
- `RuntimeConfig`
- `load_runtime_config`

То есть если другой код когда-то будет использовать этот проект как библиотеку, он скорее всего будет импортировать именно из `schema_inspector`.

#### `schema_inspector/cli.py`

CLI-обвязка над сервисным слоем.

Ее зона ответственности:

- распарсить аргументы командной строки;
- собрать кастомные headers;
- собрать runtime config;
- вызвать высокоуровневый service method;
- вывести путь к готовому отчету.

Поддерживаемые аргументы:

- `url`
- `--outdir`
- `--timeout`
- `--header`
- `--proxy`
- `--user-agent`
- `--max-attempts`

`cli.py` ничего не знает о внутреннем устройстве схемы JSON. Он только переводит CLI-ввод в параметры сервиса.

#### `schema_inspector/service.py`

Высокоуровневый orchestration layer.

Это центральная функция use-case уровня:

- получить URL;
- вызвать `fetch_json`;
- вызвать `infer_schema`;
- вызвать `build_markdown_report`;
- записать `.md` в нужную папку;
- вернуть путь к файлу.

Если упростить:

- `cli.py` отвечает за запуск;
- `service.py` отвечает за выполнение сценария целиком.

#### `schema_inspector/fetch.py`

Модуль безопасного получения JSON через transport-архитектуру.

Его ответственность:

- создать transport;
- выполнить запрос;
- проверить, нет ли challenge/guarded response;
- проверить HTTP status;
- попытаться декодировать JSON;
- вернуть `FetchResult`.

Ключевые сущности:

- `FetchResult`
  Содержит весь результат fetch-операции:
  - исходный URL;
  - resolved URL;
  - время получения;
  - HTTP status;
  - headers;
  - body bytes;
  - уже распарсенный `payload`;
  - список сетевых попыток;
  - финальный proxy;
  - challenge reason.

- `FetchJsonError`
  Исключение, которое поднимается, если:
  - запрос закончился challenge-like ответом;
  - HTTP status >= 400;
  - body не является валидным JSON.

То есть `fetch.py` переводит сырую сетевую операцию в строго проверенный JSON-level результат.

#### `schema_inspector/runtime.py`

Модуль конфигурации runtime-сети.

Это один из самых важных модулей проекта, потому что именно он определяет, как будет устроен transport.

Содержит dataclass-модели:

- `ProxyEndpoint`
  Описывает один прокси:
  - `name`
  - `url`
  - `cooldown_seconds`

- `RetryPolicy`
  Описывает retry policy:
  - `max_attempts`
  - `backoff_seconds`
  - `retry_status_codes`

- `TlsPolicy`
  Описывает параметры TLS:
  - `minimum_version`
  - `maximum_version`
  - `check_hostname`

- `RuntimeConfig`
  Центральная структура всех сетевых настроек:
  - `user_agent`
  - `default_headers`
  - `retry_policy`
  - `tls_policy`
  - `proxy_endpoints`
  - `challenge_markers`

- `TransportAttempt`
  Одна попытка запроса:
  - номер попытки;
  - какой proxy использовался;
  - какой status получен;
  - была ли ошибка;
  - был ли detected challenge.

- `TransportResult`
  Финальный результат transport-операции:
  - resolved URL;
  - status;
  - headers;
  - body;
  - attempts;
  - final proxy;
  - challenge reason.

Главная функция:

- `load_runtime_config(...)`

Она собирает конфиг из:

1. явных параметров функции;
2. переменных окружения;
3. локального `.env`.

Таким образом, конфиг можно задавать:

- через CLI;
- через shell env;
- через `.env`;
- программно через Python API.

#### `schema_inspector/proxy.py`

Модуль управления пулом прокси.

Не делает реальных сетевых запросов сам, но решает:

- какой proxy взять на текущую попытку;
- как переключаться между proxy;
- как временно исключать проблемный proxy.

Внутренняя модель:

- `_ProxyState`
  Хранит runtime-состояние одного прокси:
  - cooldown deadline;
  - success count;
  - failure count;
  - consecutive failures.

Публичный класс:

- `ProxyPool`

Его логика:

1. Получить список proxy endpoints из runtime config.
2. Выбирать их round-robin.
3. Если proxy дал сбой, отправить его в cooldown.
4. Если proxy сработал успешно, сбросить счетчик consecutive failures.

Именно этот модуль обеспечивает “архитектуру прокси” для всех запросов `schema_inspector`.

#### `schema_inspector/challenge.py`

Модуль детекции guarded responses.

Не занимается обходом защит. Его задача только распознать, что ответ выглядит как challenge.

Функция:

- `detect_challenge(status_code, headers, body, markers)`

Что она умеет распознавать:

- `429` -> `rate_limited`
- `401` / `403` + маркеры в HTML/body -> `bot_challenge`
- `401` / `403` без явных маркеров -> `access_denied`
- другие ответы, содержащие challenge markers в теле -> `bot_challenge`

Это позволяет сетевому слою корректно:

- пометить попытку как challenge;
- отдать правильную диагностику в отчет;
- не пытаться трактовать HTML challenge page как JSON.

#### `schema_inspector/transport.py`

Главный сетевой движок проекта.

Это core transport layer, через который идут все запросы.

Основной класс:

- `InspectorTransport`

Его ответственность:

- собрать headers;
- применить user-agent;
- выбрать proxy через `ProxyPool`;
- создать `urllib.request.Request`;
- создать HTTPS transport c нужным TLS context;
- выполнить запрос;
- обработать `HTTPError` как осмысленный response;
- обработать `URLError` как retry/failure;
- запускать retry/backoff;
- детектить challenge;
- вернуть полный `TransportResult`.

Внутренний вспомогательный dataclass:

- `_RawResponse`
  Это промежуточная форма ответа до challenge analysis и сборки списка попыток.

Важные особенности реализации:

1. Поддерживаются `http(s)` и `file://`.
2. Для `http(s)` используется стандартный `urllib`.
3. Для TLS создается verified `SSLContext`.
4. При наличии proxy строится `ProxyHandler`.
5. При retryable status кодах transport делает повторную попытку.
6. При повторной попытке пул может выбрать другой proxy.
7. Каждая попытка логически сохраняется в `TransportAttempt`.

То есть именно `transport.py` связывает вместе:

- `runtime.py`
- `proxy.py`
- `challenge.py`

#### `schema_inspector/schema.py`

Модуль анализа JSON-структуры.

Это главный аналитический слой проекта.

Основная модель:

- `NodeSummary`

Она представляет узел дерева JSON и хранит:

- `path`
- `occurrence_count`
- `kind_counts`
- `examples`
- `children`
- `item_summary`
- `object_instance_count`
- `array_instance_count`
- `null_count`
- `min_items`
- `max_items`
- `total_items`

Ключевые методы `NodeSummary`:

- `observe(value)`
  Рекурсивно обходит значение и обновляет статистику узла.

- `rendered_types()`
  Возвращает человекочитаемое описание типа.

- `is_required_for_parent(parent_object_count)`
  Определяет, обязательное ли поле для объекта-родителя.

- `is_nullable()`
  Проверяет, встречался ли `null`.

- `candidate_keys()`
  Пытается выделить поля, которые похожи на ключи:
  - `id`
  - `uuid`
  - `slug`
  - `code`
  - `...id`
  - `..._id`

- `suggested_table_name()`
  Строит suggested table name для PostgreSQL по path.

- `field_rows()`
  Формирует строки для markdown-таблицы полей.

- `collect_object_nodes()`
  Возвращает все объектные сущности дерева.

- `collect_array_nodes()`
  Возвращает все массивы дерева.

Другие функции:

- `infer_schema(payload)`
  Стартовая точка анализа JSON.

- `detect_kind(value)`
  Классификация типов:
  - string
  - integer
  - number
  - boolean
  - null
  - object
  - array

- `render_example(value)`
  Формирует короткий пример значения.

- `describe_child(child)`
  Пишет заметки для markdown-отчета:
  - object path
  - диапазон размера массива
  - suggested child table
  - candidate keys

Таким образом `schema.py` превращает JSON в дерево аналитических summary-объектов.

#### `schema_inspector/report.py`

Модуль генерации конечного Markdown-документа.

Зона ответственности:

- собрать source metadata;
- показать сетевые попытки;
- показать summary по объектам и массивам;
- перечислить suggested entities;
- для каждой сущности построить таблицу полей;
- вычислить стабильное имя выходного файла.

Ключевые функции:

- `build_markdown_report(fetch_result, root)`
  Генерирует полный Markdown-отчет.

- `report_filename(url)`
  Возвращает имя выходного файла `<url_name>.md`.

- `report_basename(url)`
  Делает стабильный basename из URL.

Содержание отчета:

1. Source
2. Summary
3. Network Attempts
4. Suggested Entities
5. Arrays
6. Entity sections with field tables

Это последний “presentation layer” перед записью на диск.

### Каталог `tests`

#### `tests/test_schema_inspector.py`

Основной тестовый набор проекта.

Покрывает:

- правильный recursive schema inference;
- генерацию report из `file://` JSON;
- стабильность имени выходного файла;
- чтение runtime config из env;
- retry + proxy switch logic в transport layer;
- исключение при challenge-like response.

То есть тестируется не только “schema part”, но и transport-архитектура.

#### `tests/fixtures/sample_response.json`

Небольшой тестовый JSON fixture.

Нужен для интеграционного сценария:

- взять реальный JSON-файл;
- прогнать через fetch/service/report pipeline;
- убедиться, что Markdown строится корректно.

#### `tests/_output/.gitkeep`

Папка под тестовые артефакты.

Нужна для того, чтобы каталог существовал в репозитории, даже если внутри нет сгенерированных `.md`.

#### `tests/tmpimw0cz_7`

Это не часть архитектуры.

Судя по состоянию рабочего каталога, это остаточный временный каталог от одного из прошлых тестовых прогонов. Код проекта его не использует.

## Как связаны модули между собой

Ниже дано логическое направление вызовов.

```text
inspect_api.py
  -> schema_inspector.cli.main()
      -> parse_headers()
      -> load_runtime_config()
      -> inspect_url_to_markdown()
          -> fetch_json()
              -> InspectorTransport.fetch()
                  -> ProxyPool.acquire()
                  -> _execute_once()
                  -> detect_challenge()
                  -> ProxyPool.record_success()/record_failure()
              -> json.loads(...)
          -> infer_schema()
          -> build_markdown_report()
          -> write .md to disk
```

## Жизненный цикл запроса

### Шаг 1. Пользователь запускает CLI

Пример:

```powershell
python inspect_api.py "https://api.example.com/v1/player/123" --proxy "http://proxy-1:8080"
```

### Шаг 2. `cli.py` собирает входные параметры

Он:

- читает URL;
- читает optional headers;
- читает optional proxy list;
- читает max attempts;
- собирает `RuntimeConfig`.

### Шаг 3. `service.py` запускает основной сценарий

Сервис вызывает `fetch_json(...)`.

### Шаг 4. `fetch.py` запускает transport

Если `runtime_config` не передан явно, он строится автоматически через `load_runtime_config(...)`.

### Шаг 5. `transport.py` выполняет реальный запрос

Transport:

1. собирает headers;
2. выставляет `User-Agent`;
3. берет proxy из `ProxyPool`;
4. создает TLS context;
5. отправляет запрос;
6. ловит `HTTPError` как обычный response;
7. при `URLError` делает retry;
8. анализирует response через `detect_challenge`.

### Шаг 6. `fetch.py` валидирует, что response действительно JSON

Он:

- отбрасывает challenge-ответы;
- отбрасывает HTTP ошибки;
- поднимает `FetchJsonError`, если body не JSON;
- возвращает `FetchResult`.

### Шаг 7. `schema.py` строит дерево схемы

`infer_schema(payload)` рекурсивно проходит по JSON и накапливает:

- типы;
- примеры;
- candidate keys;
- структуру вложенности;
- статистику массивов.

### Шаг 8. `report.py` строит Markdown

В отчет добавляются:

- исходный URL;
- resolved URL;
- HTTP status;
- список сетевых попыток;
- финальный proxy;
- challenge status;
- suggested entities;
- массивы;
- таблицы полей.

### Шаг 9. `service.py` пишет `.md` на диск

Имя файла вычисляется детерминированно из URL.

## Архитектура прокси

Прокси в проекте встроены в runtime/transport слой, а не размазаны по бизнес-логике.

### Где задаются

- через CLI `--proxy`
- через `.env`
- через env:
  - `SCHEMA_INSPECTOR_PROXY_URL`
  - `SCHEMA_INSPECTOR_PROXY_URLS`
- программно через `load_runtime_config(proxy_urls=[...])`

### Как представлены

Каждый прокси описывается как `ProxyEndpoint`.

Он включает:

- имя;
- URL;
- cooldown period.

### Как используются

1. `RuntimeConfig` передает список прокси в `InspectorTransport`.
2. `InspectorTransport` создает `ProxyPool`.
3. Перед каждой попыткой transport вызывает `ProxyPool.acquire()`.
4. Если попытка проваливается, transport вызывает `record_failure`.
5. Проблемный прокси уходит в cooldown.
6. Следующая попытка может взять другой proxy.

### Что важно понимать

Прокси здесь являются частью transport orchestration.

То есть:

- schema layer не знает ничего о proxy;
- report layer только отображает, какой proxy был использован;
- business use-case не зависит от конкретной реализации proxy selection.

Это хорошее разделение ответственности.

## Архитектура TLS

TLS в проекте централизован и задается через `TlsPolicy`.

### Где задается

- `RuntimeConfig.tls_policy`
- env:
  - `SCHEMA_INSPECTOR_TLS_MIN_VERSION`
  - `SCHEMA_INSPECTOR_TLS_MAX_VERSION`
  - `SCHEMA_INSPECTOR_TLS_CHECK_HOSTNAME`

### Как применяется

В `transport.py` создается `ssl.create_default_context()`, после чего:

- `verify_mode = CERT_REQUIRED`
- `check_hostname` берется из политики
- `minimum_version` берется из политики
- `maximum_version` берется из политики

### Что это значит

Проект использует:

- стандартную TLS-верификацию;
- обычный SSL stack Python;
- централизованную политику версий TLS.

Это транспортная архитектура с контролем TLS, но не система TLS spoofing.

## Challenge / anti-bot слой

Сейчас anti-bot слой используется только для диагностики guarded responses.

### Что умеет

- увидеть `429`
- увидеть `401` и `403`
- заметить маркеры challenge в HTML/body

### Что делает после этого

- пишет `challenge_reason` в `TransportAttempt`
- пишет `challenge_reason` в итоговый `TransportResult`
- не дает `fetch.py` трактовать ответ как валидный JSON

### Что не делает

- не решает CAPTCHA
- не выполняет обход challenge
- не подменяет TLS fingerprints
- не эмулирует браузерные handshakes на низком уровне

То есть слой нужен для корректной диагностики и остановки, а не для обхода защиты.

## Архитектура анализа схемы

После того как JSON успешно получен, проект переключается в режим чистого анализа данных.

### Центральная единица анализа

`NodeSummary` — это узел дерева схемы.

Каждый узел знает:

- какой у него `path`
- сколько раз он встретился
- какие типы наблюдались
- какие примеры значений есть
- какие у него дети
- есть ли item schema, если это массив

### Почему это удобно

Потому что одно и то же дерево можно использовать для:

- генерации Markdown;
- будущей генерации PostgreSQL DDL;
- поиска candidate relations;
- поиска parent-child таблиц.

То есть `NodeSummary` — это основной промежуточный формат проекта.

## Какие файлы реально участвуют в runtime

При обычном запуске через CLI реально участвуют:

- `inspect_api.py`
- `schema_inspector/cli.py`
- `schema_inspector/runtime.py`
- `schema_inspector/service.py`
- `schema_inspector/fetch.py`
- `schema_inspector/transport.py`
- `schema_inspector/proxy.py`
- `schema_inspector/challenge.py`
- `schema_inspector/schema.py`
- `schema_inspector/report.py`

### Какие файлы не участвуют напрямую в runtime

- `README.md`
- `PROJECT_OVERVIEW.md`
- `requirements.txt`
- `tests/*`
- `sports_data.db`
- `tests/tmpimw0cz_7`

## Что сейчас можно считать “ядром проекта”

Если бы нужно было выделить самое важное ядро, это были бы:

1. `runtime.py`
2. `transport.py`
3. `fetch.py`
4. `schema.py`
5. `report.py`
6. `service.py`

Именно эти файлы вместе формируют полный pipeline:

`config -> network -> validation -> schema inference -> markdown`

## Точки расширения

Проект уже подготовлен к следующим шагам.

### 1. Генерация PostgreSQL DDL

Можно добавить модуль, например:

`schema_inspector/postgres.py`

Который будет:

- принимать `NodeSummary`;
- строить `CREATE TABLE`;
- выделять parent-child связи;
- предлагать foreign keys.

### 2. Пакетный режим

Можно добавить режим:

- прочитать список URL;
- построить много отчетов;
- накопить каталог схем;
- затем агрегировать их в общую модель БД.

### 3. Поддержка OpenAPI

Сейчас анализ идет только по фактическому JSON.
Позже можно добавить режим чтения:

- OpenAPI
- Swagger
- JSON Schema

### 4. Сохранение metadata в PostgreSQL

Даже до начала “настоящего парсинга” можно сохранять сами schema reports и entity maps в PostgreSQL как metadata catalog.

## Ограничения текущей версии

1. Поддерживается только `GET`.
2. Анализ строится по фактическому JSON-ответу, а не по формальному контракту API.
3. Нет экспорта в PostgreSQL schema/DDL.
4. Нет слоя записи данных в PostgreSQL.
5. Нет пакетного анализа нескольких endpoint-ов в один normalized domain model.

## Краткое резюме

Текущий проект — это не парсер данных в БД, а подготовительный schema analysis tool.

Он состоит из:

- строгого transport-слоя с proxy/TLS/retry/challenge detection;
- аналитического слоя schema inference;
- presentation-слоя, который генерирует Markdown-отчет.

Главный сценарий:

1. Взять URL.
2. Безопасно получить JSON.
3. Понять его структуру.
4. Зафиксировать это в `.md`.
5. Использовать отчет как основу для будущего проектирования PostgreSQL.
