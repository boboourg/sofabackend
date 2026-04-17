# Production Deployment Guide

Пошаговая инструкция по выкладке Hybrid ETL и Continuous Workload на боевой Linux-сервер.

## 1. Что должно быть готово до деплоя

- Локально все изменения должны быть закоммичены.
- Локально должен проходить `pytest`.
- Должны быть известны:
  - PostgreSQL DSN
  - Redis URL
  - рабочие proxy URL
  - Linux-пользователь, от которого будут запускаться сервисы

## 2. Git Push с рабочей машины

Проверьте состояние ветки:

```powershell
git status
git log --oneline -n 5
```

Если есть незакоммиченные изменения:

```powershell
git add schema_inspector docs tests
git commit -m "feat: update local ops api and production deployment guide"
```

Отправьте ветку в удалённый репозиторий:

```powershell
git push origin codex/hybrid-etl-phase1-2
```

Если у вас другой production branch workflow:

- либо создайте Pull Request в `main`
- либо смержите ветку в ваш release branch
- либо деплойте сервер прямо из этой ветки, если это ваш принятый процесс

## 3. Подготовка Ubuntu/Linux сервера

Подключитесь к серверу:

```bash
ssh your-user@your-server
```

Установите базовые пакеты:

```bash
sudo apt update
sudo apt install -y git python3.11 python3.11-venv python3-pip build-essential tmux
```

Если Redis будет крутиться на этом же сервере:

```bash
sudo apt install -y redis-server
sudo systemctl enable redis-server
sudo systemctl start redis-server
```

Создайте рабочую директорию и склонируйте проект:

```bash
sudo mkdir -p /opt/sofascore
sudo chown "$USER":"$USER" /opt/sofascore
git clone <YOUR_GIT_REMOTE_URL> /opt/sofascore
cd /opt/sofascore
git checkout codex/hybrid-etl-phase1-2
```

## 4. Поднятие Python 3.11 окружения

Создайте virtualenv:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

Установите зависимости проекта:

```bash
python -m pip install -r requirements.txt
python -m pip install redis
```

Пакет `redis` ставится отдельно, потому что Continuous Workload и operational endpoints используют Redis runtime и fail-closed startup.

## 5. Настройка .env на сервере

Создайте файл:

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
```

Минимально критичные переменные:

- `SOFASCORE_DATABASE_URL`
- `REDIS_URL`
- `SCHEMA_INSPECTOR_REQUIRE_PROXY`
- `SCHEMA_INSPECTOR_PROXY_URLS`

Практически рекомендуемые:

- `SOFASCORE_PG_MIN_SIZE=20`
- `SOFASCORE_PG_MAX_SIZE=50`
- `SOFASCORE_PG_COMMAND_TIMEOUT=60`
- `SCHEMA_INSPECTOR_MAX_ATTEMPTS=5`
- `SCHEMA_INSPECTOR_BACKOFF_SECONDS=1.0`
- `SCHEMA_INSPECTOR_TLS_IMPERSONATE=chrome110`

Сделайте файл закрытым:

```bash
chmod 600 /opt/sofascore/.env
```

## 6. Инициализация БД и миграций

Активируйте окружение, если оно ещё не активировано:

```bash
cd /opt/sofascore
source .venv/bin/activate
```

Запустите инициализацию схемы и миграций:

```bash
python -m schema_inspector.db_setup_cli
```

Успешный результат должен закончиться строкой вида:

```text
postgres_setup database=... created=no schema_initialized=no applied_migrations=... skipped_migrations=...
```

## 7. Полный прогон тестов на сервере

Перед запуском сервисов прогоните весь suite:

```bash
python -m pytest -q
```

Если вы хотите сначала короткую smoke-проверку:

```bash
python -m pytest tests/test_local_api_server.py tests/test_local_swagger_builder.py tests/test_hybrid_cli.py -q
```

Если `pytest -q` не проходит, не запускайте демоны до исправления.

## 8. Предпусковая проверка runtime

Проверьте health:

```bash
python -m schema_inspector.cli health
```

Ожидаемое поведение:

- `db_ok=1`
- `redis_ok=1`
- `redis_backend!=memory`

Проверьте одиночный scheduled smoke-run с аудитом:

```bash
python -m schema_inspector.cli scheduled --sport-slug football --date 2026-04-17 --event-concurrency 6 --audit-db
```

Проверьте live smoke-run с аудитом:

```bash
python -m schema_inspector.cli live --sport-slug football --audit-db
```

Если после прогона `snapshots=0` или `event_rows=0`, команда завершится ошибкой. Это ожидаемый runtime gate, а не баг.

## 9. Проверка Swagger и operational API

Запустите local API server:

```bash
python -m schema_inspector.local_api_server --host 0.0.0.0 --port 8000 --redis-url "$REDIS_URL"
```

Проверьте:

- [http://SERVER_IP:8000/](http://SERVER_IP:8000/)
- [http://SERVER_IP:8000/openapi.json](http://SERVER_IP:8000/openapi.json)

Новые operational endpoints:

- `/ops/health`
- `/ops/snapshots/summary`
- `/ops/queues/summary`
- `/ops/jobs/runs`

## 10. Рекомендуемый способ демонизации: systemd

### 10.1 Planner daemon

Создайте файл `/etc/systemd/system/sofascore-planner.service`:

```ini
[Unit]
Description=Sofascore Hybrid ETL Planner Daemon
After=network.target

[Service]
Type=simple
User=YOUR_USER
WorkingDirectory=/opt/sofascore
EnvironmentFile=/opt/sofascore/.env
ExecStart=/opt/sofascore/.venv/bin/python -m schema_inspector.cli planner-daemon
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### 10.2 Hydrate workers

Создайте шаблон `/etc/systemd/system/sofascore-worker-hydrate@.service`:

```ini
[Unit]
Description=Sofascore Hydrate Worker %i
After=network.target

[Service]
Type=simple
User=YOUR_USER
WorkingDirectory=/opt/sofascore
EnvironmentFile=/opt/sofascore/.env
ExecStart=/opt/sofascore/.venv/bin/python -m schema_inspector.cli worker-hydrate --consumer-name hydrate-%i --block-ms 5000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### 10.3 Live hot worker

Создайте `/etc/systemd/system/sofascore-worker-live-hot.service`:

```ini
[Unit]
Description=Sofascore Live Hot Worker
After=network.target

[Service]
Type=simple
User=YOUR_USER
WorkingDirectory=/opt/sofascore
EnvironmentFile=/opt/sofascore/.env
ExecStart=/opt/sofascore/.venv/bin/python -m schema_inspector.cli worker-live-hot --consumer-name live-hot-1 --block-ms 5000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### 10.4 Live warm worker

Создайте `/etc/systemd/system/sofascore-worker-live-warm.service`:

```ini
[Unit]
Description=Sofascore Live Warm Worker
After=network.target

[Service]
Type=simple
User=YOUR_USER
WorkingDirectory=/opt/sofascore
EnvironmentFile=/opt/sofascore/.env
ExecStart=/opt/sofascore/.venv/bin/python -m schema_inspector.cli worker-live-warm --consumer-name live-warm-1 --block-ms 5000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### 10.5 Maintenance worker

Создайте `/etc/systemd/system/sofascore-worker-maintenance.service`:

```ini
[Unit]
Description=Sofascore Maintenance Worker
After=network.target

[Service]
Type=simple
User=YOUR_USER
WorkingDirectory=/opt/sofascore
EnvironmentFile=/opt/sofascore/.env
ExecStart=/opt/sofascore/.venv/bin/python -m schema_inspector.cli worker-maintenance --consumer-name maintenance-1 --block-ms 5000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### 10.6 Optional: local API service

Создайте `/etc/systemd/system/sofascore-local-api.service`:

```ini
[Unit]
Description=Sofascore Local Swagger API
After=network.target

[Service]
Type=simple
User=YOUR_USER
WorkingDirectory=/opt/sofascore
EnvironmentFile=/opt/sofascore/.env
ExecStart=/opt/sofascore/.venv/bin/python -m schema_inspector.local_api_server --host 0.0.0.0 --port 8000 --redis-url ${REDIS_URL}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### 10.7 Активация systemd сервисов

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now sofascore-planner.service
sudo systemctl enable --now sofascore-worker-hydrate@1.service
sudo systemctl enable --now sofascore-worker-hydrate@2.service
sudo systemctl enable --now sofascore-worker-hydrate@3.service
sudo systemctl enable --now sofascore-worker-hydrate@4.service
sudo systemctl enable --now sofascore-worker-live-hot.service
sudo systemctl enable --now sofascore-worker-live-warm.service
sudo systemctl enable --now sofascore-worker-maintenance.service
sudo systemctl enable --now sofascore-local-api.service
```

Проверка статуса:

```bash
systemctl status sofascore-planner.service
systemctl status sofascore-worker-hydrate@1.service
systemctl status sofascore-worker-live-hot.service
systemctl status sofascore-worker-live-warm.service
systemctl status sofascore-worker-maintenance.service
systemctl status sofascore-local-api.service
```

Просмотр логов:

```bash
journalctl -u sofascore-planner.service -f
journalctl -u sofascore-worker-hydrate@1.service -f
journalctl -u sofascore-worker-live-hot.service -f
```

## 11. Если systemd пока не хотите использовать

Временный вариант через `tmux`:

```bash
tmux new -s sofascore
source /opt/sofascore/.venv/bin/activate
cd /opt/sofascore
python -m schema_inspector.cli planner-daemon
```

В новых tmux windows запускайте отдельно:

- `python -m schema_inspector.cli worker-hydrate --consumer-name hydrate-1`
- `python -m schema_inspector.cli worker-hydrate --consumer-name hydrate-2`
- `python -m schema_inspector.cli worker-live-hot --consumer-name live-hot-1`
- `python -m schema_inspector.cli worker-live-warm --consumer-name live-warm-1`
- `python -m schema_inspector.cli worker-maintenance --consumer-name maintenance-1`
- `python -m schema_inspector.local_api_server --host 0.0.0.0 --port 8000 --redis-url "$REDIS_URL"`

Это годится для первого запуска, но для production лучше использовать `systemd`.

## 12. Production acceptance checklist

Перед тем как считать deployment успешным:

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
- Swagger открывается без ошибок
- `/ops/health` отвечает
- `/ops/queues/summary` показывает непустой monitoring payload
- systemd services находятся в состоянии `active (running)`

## 13. Самая безопасная последовательность первого production запуска

1. Склонировать проект на сервер
2. Создать `.venv`
3. Установить `requirements.txt` и пакет `redis`
4. Заполнить `.env`
5. Запустить `db_setup_cli`
6. Прогнать `pytest -q`
7. Проверить `cli health`
8. Сделать `scheduled --audit-db`
9. Сделать `live --audit-db`
10. Поднять `local_api_server`
11. Только после этого включать planner и 24/7 workers через systemd
