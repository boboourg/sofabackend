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
