# Sofascore Hybrid ETL Runtime

Continuous multi-sport ETL runtime for ingesting Sofascore-style data into PostgreSQL, serving a local read API, and running scheduled, live, and historical workloads through Redis Streams.

## What This Project Is Now

This repository is no longer a small "inspect one JSON URL and print Markdown" tool. It now contains:

- a PostgreSQL-backed raw snapshot and normalized fact pipeline
- Redis Stream-based continuous workers
- scheduled discovery and live refresh planning
- historical archival discovery, tournament, enrichment, and hydrate lanes
- operational monitoring endpoints exposed by the local API server
- recovery and replay workflows for delayed and failed jobs

## Project Documentation

Подробная документация проекта (на русском) в [`docs/`](docs/):

| Документ | Что внутри |
|---|---|
| [docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md) | High-level архитектура, Mermaid data flow, external deps, hydration режимы |
| [docs/SERVICES_AND_WORKERS.md](docs/SERVICES_AND_WORKERS.md) | Все systemd units, daemons, workers — что читает/пишет, какой env, failure modes, restart triggers |
| [docs/CLI_AND_SCRIPTS.md](docs/CLI_AND_SCRIPTS.md) | Каталог всех `python -m schema_inspector.cli <subcommand>` команд — назначение, аргументы, side effects, safe/mutation |
| [docs/ENVIRONMENT.md](docs/ENVIRONMENT.md) | Полный каталог `.env` переменных — default, где читается, эффект, restart-need |
| [docs/API_ROUTES.md](docs/API_ROUTES.md) | Local FastAPI: handle_api_get waterfall, specialized handlers, ops routes, OpenAPI |
| [docs/PARSING_AND_POLICIES.md](docs/PARSING_AND_POLICIES.md) | match_center_policy, detail_resource_policy, live_dispatch_policy, isEditor HARD BAN (3 layers), tier/detail_id mapping, negative caches |
| [docs/DATABASE_AND_STORAGE.md](docs/DATABASE_AND_STORAGE.md) | Все таблицы по группам, repositories, source-of-truth waterfall, hot rows/known contention, recent migrations |
| [docs/REDIS_AND_QUEUES.md](docs/REDIS_AND_QUEUES.md) | Streams + consumer groups, live state keys, leases/freshness/dedupe/throttle, dispatch metrics, JobEnvelope, backpressure |
| [docs/FUNCTION_INDEX.md](docs/FUNCTION_INDEX.md) | Карта ключевых функций/классов по файлам: что делает, кто вызывает, side effects |
| [docs/OPERATIONS_RUNBOOK.md](docs/OPERATIONS_RUNBOOK.md) | Практический playbook: health checks, restart процедуры, rollback, canary, восстановление |

Архитектурный single source of truth — [docs/current-runtime-architecture.md](docs/current-runtime-architecture.md). Cross-cut handoff — [NEXT_CHAT_CONTEXT.md](NEXT_CHAT_CONTEXT.md) и [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md).

## Operator Quickstart

### Local Workspace

Ensure you are working in the correct root directory of the repository before running scripts or commands:

```powershell
git rev-parse --show-toplevel
git status
```

### Production Access

Production SSH alias:

```powershell
ssh sofascore-prod
```

Production project path:

```bash
cd /opt/sofascore
```

Common production checks:

```bash
git log -1 --oneline
systemctl list-units --type=service --state=running | grep sofascore
journalctl -u "sofascore-*" --since "5 minutes ago" --no-pager
```

Database access on production:

```bash
sudo -u postgres psql -p 5432 -d sofascore_schema_inspector
```

### Git Workflow

This project currently works directly on `main`. Do not create feature branches or PR branches
unless explicitly requested.

Expected local flow:

```powershell
git status
git add <files>
git commit -m "<short scope>: <summary>"
git push origin main
```

Expected production deploy flow:

```bash
cd /opt/sofascore
git pull
git log -1 --oneline
```

After code changes, restart only the affected systemd units, preferably in small waves. Do not
restart unrelated workers just because code was pulled.

## Runtime Pipeline

```text
planner/live-discovery-planner/historical-planners
    -> discovery/live_discovery/historical_discovery
    -> hydrate/live_hot/live_warm/historical_hydrate
    -> parsers
    -> durable normalize sink
    -> PostgreSQL
```

The user-facing local API reads from PostgreSQL and exposes:

- local Sofascore-style paths
- Swagger / OpenAPI
- `/ops/health`
- `/ops/snapshots/summary`
- `/ops/queues/summary`
- `/ops/jobs/runs`

## Main Components

- `schema_inspector/cli.py`
  Unified command surface for one-shot hydration, planners, and workers.
- `schema_inspector/services/service_app.py`
  Builds long-running planner and worker processes over Redis Streams.
- `schema_inspector/services/planner_daemon.py`
  Runs scheduled planning, delayed replay, and live refresh planning with backpressure.
- `schema_inspector/workers/discovery_worker.py`
  Expands sport-surface jobs into event-level hydrate jobs and applies skip/defer admission control.
- `schema_inspector/storage/normalize_repository.py`
  Writes normalized facts and dimensions into PostgreSQL.
- `schema_inspector/local_api_server.py`
  Serves the local API, Swagger, and operational endpoints.

## Core Commands

### One-shot commands

```powershell
python -m schema_inspector.cli health
python -m schema_inspector.cli live --sport-slug football --audit-db
python -m schema_inspector.cli scheduled --sport-slug football --date 2026-04-18 --audit-db
python -m schema_inspector.cli event --sport-slug football --event-id 14083191 --audit-db
python -m schema_inspector.cli replay --snapshot-id 123
python -m schema_inspector.cli audit-db --sport-slug football --event-id 14083191
python -m schema_inspector.cli recover-live-state
```

### Continuous planners

```powershell
python -m schema_inspector.cli planner-daemon
python -m schema_inspector.cli live-discovery-planner-daemon
python -m schema_inspector.cli historical-planner-daemon --date-from 2020-01-01 --date-to 2020-12-31
python -m schema_inspector.cli historical-tournament-planner-daemon
```

### Continuous workers

```powershell
python -m schema_inspector.cli worker-discovery --consumer-name discovery-1
python -m schema_inspector.cli worker-live-discovery --consumer-name live-discovery-1
python -m schema_inspector.cli worker-historical-discovery --consumer-name historical-discovery-1
python -m schema_inspector.cli worker-hydrate --consumer-name hydrate-1
python -m schema_inspector.cli worker-historical-hydrate --consumer-name historical-hydrate-1
python -m schema_inspector.cli worker-live-hot --consumer-name live-hot-1
python -m schema_inspector.cli worker-live-warm --consumer-name live-warm-1
python -m schema_inspector.cli worker-historical-tournament --consumer-name historical-tournament-1
python -m schema_inspector.cli worker-historical-enrichment --consumer-name historical-enrichment-1
python -m schema_inspector.cli worker-maintenance --consumer-name maintenance-1
python -m schema_inspector.cli worker-historical-maintenance --consumer-name historical-maintenance-1
```

### Local API server

```powershell
python -m schema_inspector.local_api_server --host 127.0.0.1 --port 8000
```

## Runtime Requirements

- Python 3.11
- PostgreSQL
- Redis
- `asyncpg`
- `redis` Python package for continuous services and ops endpoints

Redis is not optional for continuous runtime mode. The CLI only allows in-memory Redis as an explicit development fallback.

## Operational Notes

- The system uses backpressure instead of blind fanout when downstream queues are overloaded.
- Operational discovery can skip non-force hydrate fanout under pressure.
- Historical discovery can defer fanout and retry later instead of flooding historical hydrate.
- This protects the database and queue consumers, but it also means "system survived" is not the same as "live freshness is healthy".

See:

- `docs/current-runtime-architecture.md`
- `docs/live-ready-runbook.md`
- `docs/live-first-rollout.md`
- `docs/24x7-exit-criteria.md`
- `docs/2026-04-17-production-deployment-guide.md`

## Tests

Run the full suite:

```powershell
D:\sofascore\.venv311\Scripts\python.exe -m pytest -q
```

Run targeted operational suites:

```powershell
D:\sofascore\.venv311\Scripts\python.exe -m pytest tests\test_discovery_worker_service.py tests\test_planner_daemon.py tests\test_local_api_server.py -q
```

## Current Readiness Summary

- Stable enough for controlled continuous operation and backlog recovery
- Not yet safe to claim fully unattended 24x7 operation without additional hardening
- Live readiness and 24x7 readiness are tracked separately in the docs listed above
