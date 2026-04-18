# Production Deployment Guide

Пошаговая инструкция по разворачиванию текущего hybrid ETL runtime на production Linux-сервере.

## 1. Что должно быть готово до деплоя

- все нужные изменения закоммичены
- локально проходит `pytest`
- известны:
  - PostgreSQL DSN
  - Redis URL
  - proxy URL
  - Linux user, от которого будут запускаться сервисы

## 2. Подготовка сервера

```bash
sudo apt update
sudo apt install -y git python3.11 python3.11-venv python3-pip build-essential tmux
```

Если Redis будет локальным:

```bash
sudo apt install -y redis-server
sudo systemctl enable redis-server
sudo systemctl start redis-server
```

## 3. Клонирование проекта

```bash
sudo mkdir -p /opt/sofascore
sudo chown "$USER":"$USER" /opt/sofascore
git clone <YOUR_GIT_REMOTE_URL> /opt/sofascore
cd /opt/sofascore
git checkout <YOUR_DEPLOY_BRANCH>
```

## 4. Python окружение

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install redis
```

## 5. Настройка `.env`

Создайте `/opt/sofascore/.env`:

```bash
cat > /opt/sofascore/.env <<'EOF'
SOFASCORE_DATABASE_URL=postgresql://user:password@host:5432/sofascore_schema_inspector
SOFASCORE_PG_MIN_SIZE=20
SOFASCORE_PG_MAX_SIZE=50
SOFASCORE_PG_COMMAND_TIMEOUT=60

REDIS_URL=redis://127.0.0.1:6379/0

SCHEMA_INSPECTOR_USER_AGENT=schema-inspector/1.0
SCHEMA_INSPECTOR_REQUIRE_PROXY=true
SCHEMA_INSPECTOR_PROXY_URLS=http://proxy1:port,http://proxy2:port
SCHEMA_INSPECTOR_MAX_ATTEMPTS=5
SCHEMA_INSPECTOR_BACKOFF_SECONDS=1.0
SCHEMA_INSPECTOR_TLS_IMPERSONATE=chrome110
EOF
chmod 600 /opt/sofascore/.env
```

Минимально критичные переменные:

- `SOFASCORE_DATABASE_URL`
- `REDIS_URL`
- `SCHEMA_INSPECTOR_REQUIRE_PROXY`
- `SCHEMA_INSPECTOR_PROXY_URLS`

## 6. Инициализация БД

```bash
cd /opt/sofascore
source .venv/bin/activate
python -m schema_inspector.db_setup_cli
```

## 7. Полный прогон тестов

```bash
python -m pytest -q
```

Если тесты не проходят, continuous runtime не поднимать.

## 8. Предпусковая проверка runtime

### Health

```bash
python -m schema_inspector.cli health
```

Ожидается:

- `db_ok=1`
- `redis_ok=1`
- `redis_backend!=memory`

### Scheduled smoke-run

```bash
python -m schema_inspector.cli scheduled --sport-slug football --date 2026-04-17 --event-concurrency 6 --audit-db
```

### Live smoke-run

```bash
python -m schema_inspector.cli live --sport-slug football --audit-db
```

## 9. Поднятие local API и ops surface

```bash
python -m schema_inspector.local_api_server --host 0.0.0.0 --port 8000
```

Проверьте:

- `/`
- `/openapi.json`
- `/ops/health`
- `/ops/snapshots/summary`
- `/ops/queues/summary`
- `/ops/jobs/runs`

## 10. Рекомендуемый способ демонизации: systemd

Рекомендуется использовать unit-файлы из `ops/systemd/`.

### 10.1 Куда копировать

```bash
sudo cp ops/systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
```

### 10.2 Базовый live-first профиль

Сначала включайте только этот профиль:

```bash
sudo systemctl enable --now sofascore-planner.service
sudo systemctl enable --now sofascore-discovery@1.service
sudo systemctl enable --now sofascore-live-discovery@1.service
sudo systemctl enable --now sofascore-hydrate@1.service
sudo systemctl enable --now sofascore-hydrate@2.service
sudo systemctl enable --now sofascore-hydrate@3.service
sudo systemctl enable --now sofascore-live-hot@1.service
sudo systemctl enable --now sofascore-live-warm@1.service
sudo systemctl enable --now sofascore-historical-discovery@1.service
sudo systemctl enable --now sofascore-historical-hydrate@1.service
sudo systemctl enable --now sofascore-historical-hydrate@2.service
sudo systemctl enable --now sofascore-maintenance@1.service
sudo systemctl enable --now sofascore-api.service
```

### 10.3 Что пока не включать

Пока `hydrate` и `historical_hydrate` ещё перегружены, не включайте:

- `historicaltournament`
- `historicalenrichment1`
- `historicalenrichment2`

### 10.4 Проверка статуса

```bash
systemctl status sofascore-planner.service
systemctl status sofascore-discovery@1.service
systemctl status sofascore-live-discovery@1.service
systemctl status sofascore-hydrate@1.service
systemctl status sofascore-live-hot@1.service
systemctl status sofascore-api.service
```

### 10.5 Логи

```bash
journalctl -u sofascore-planner.service -f
journalctl -u sofascore-hydrate@1.service -f
journalctl -u sofascore-live-hot@1.service -f
journalctl -u sofascore-api.service -f
```

## 11. Если systemd пока не используете

Временный вариант через `tmux`:

```bash
tmux new -s sofascore
source /opt/sofascore/.venv/bin/activate
cd /opt/sofascore
python -m schema_inspector.cli planner-daemon
```

В отдельных окнах:

- `python -m schema_inspector.cli worker-discovery --consumer-name discovery-1`
- `python -m schema_inspector.cli worker-live-discovery --consumer-name live-discovery-1`
- `python -m schema_inspector.cli worker-hydrate --consumer-name hydrate-1`
- `python -m schema_inspector.cli worker-hydrate --consumer-name hydrate-2`
- `python -m schema_inspector.cli worker-hydrate --consumer-name hydrate-3`
- `python -m schema_inspector.cli worker-live-hot --consumer-name live-hot-1`
- `python -m schema_inspector.cli worker-live-warm --consumer-name live-warm-1`
- `python -m schema_inspector.cli worker-historical-discovery --consumer-name historical-discovery-1`
- `python -m schema_inspector.cli worker-historical-hydrate --consumer-name historical-hydrate-1`
- `python -m schema_inspector.cli worker-historical-hydrate --consumer-name historical-hydrate-2`
- `python -m schema_inspector.cli worker-maintenance --consumer-name maintenance-1`
- `python -m schema_inspector.local_api_server --host 0.0.0.0 --port 8000`

Это допустимо только как временный режим.

## 12. Live-ready gates

Проект можно считать live-ready только если одновременно выполняется всё ниже:

- `stream:etl:discovery lag == 0`
- `stream:etl:live_discovery lag == 0`
- `stream:etl:hydrate lag` падает или хотя бы не растёт
- `stream:etl:live_hot lag` падает или хотя бы не растёт
- `failed` не растёт
- в hydrate и historical-hydrate логах нет свежих `TimeoutError`

Быстрая проверка:

```bash
python3 - <<'PY'
import json, urllib.request
data = json.load(urllib.request.urlopen("http://127.0.0.1:8000/ops/queues/summary"))
for item in data["streams"]:
    if item["stream"] in {
        "stream:etl:discovery",
        "stream:etl:live_discovery",
        "stream:etl:hydrate",
        "stream:etl:live_hot",
        "stream:etl:historical_hydrate",
    }:
        print(item["stream"], "lag=", item["lag"], "entries_read=", item["entries_read"])
print("delayed_total=", data["delayed_total"], "delayed_due=", data["delayed_due"])
PY

tail -n 20 /opt/sofascore/logs/planner.log
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-1.log | tail -n 20
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-2.log | tail -n 20
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-3.log | tail -n 20
```

## 13. Stop conditions

Сразу останавливайте promotion или scale-up, если:

- появился новый `TimeoutError`
- `hydrate lag` растёт 10-15 минут подряд
- `live_hot lag` снова начинает устойчиво расти
- `failed` начинает расти

## 13.1 One-command readiness gate

```bash
python3 scripts/prod_readiness_check.py --base-url http://127.0.0.1:8000
```

Ожидаемое поведение:

- код возврата `0`, если runtime готов
- код возврата `1`, если live gates не выполнены
- в выводе есть summary по `hydrate`, `historical_hydrate`, `live_hot` и статус `READY` или `NOT_READY`

## 14. Historical promotion order

Когда `hydrate` и `historical_hydrate` стабильно дренируются:

1. включить `historicaltournament`
2. подождать 15-20 минут
3. включить `historicalenrichment1`
4. подождать 15-20 минут
5. включить `historicalenrichment2`

Не включайте их все сразу.

## 15. Production acceptance checklist

- `python -m pytest -q` проходит
- `python -m schema_inspector.db_setup_cli` проходит
- `python -m schema_inspector.cli health` показывает:
  - `db_ok=1`
  - `redis_ok=1`
  - `redis_backend!=memory`
- `scheduled --audit-db` даёт:
  - `snapshots>0`
  - `event_rows>0`
- `live --audit-db` даёт:
  - `snapshots>0`
  - `event_rows>0`
- Swagger и local API отвечают
- `/ops/queues/summary` отвечает валидным payload
- systemd services находятся в состоянии `active (running)`
- live-ready gates выполняются

## 16. Самая безопасная последовательность первого запуска

1. клонировать проект
2. создать `.venv`
3. установить зависимости
4. заполнить `.env`
5. запустить `db_setup_cli`
6. прогнать `pytest -q`
7. проверить `cli health`
8. сделать `scheduled --audit-db`
9. сделать `live --audit-db`
10. поднять `local_api_server`
11. включить live-first systemd profile
12. дождаться live-ready gates
13. только потом начинать по одному возвращать historical bulk
