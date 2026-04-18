# Project Overview

## Purpose

This project is a hybrid ingestion and serving stack for Sofascore-style sports data.

At a high level, it does three things:

1. Ingest raw API payloads for scheduled, live, and historical workloads
2. Normalize those payloads into durable PostgreSQL tables
3. Serve a local read API and operational monitoring surface over the ingested dataset

## Current Architecture

The real architecture is:

```text
planner/live-discovery-planner/historical planners
    -> Redis Streams
    -> discovery/live_discovery/historical_discovery workers
    -> hydrate/live_hot/live_warm/historical_hydrate workers
    -> parser registry
    -> durable normalize sink
    -> PostgreSQL
    -> local API server + ops endpoints
```

This is a continuous multi-worker system, not a single-shot schema report generator.

## Primary Runtime Layers

### 1. Planning and admission control

These components decide what work is published and when:

- scheduled planner
- live discovery planner
- historical date-range planner
- historical tournament planner
- queue backpressure checks
- delayed replay

Relevant files:

- `schema_inspector/services/planner_daemon.py`
- `schema_inspector/services/live_discovery_planner.py`
- `schema_inspector/services/historical_planner.py`
- `schema_inspector/services/historical_tournament_planner.py`

### 2. Redis Stream transport

Continuous workloads are pushed through Redis Streams and consumer groups.

Operational streams:

- `stream:etl:discovery`
- `stream:etl:live_discovery`
- `stream:etl:hydrate`
- `stream:etl:live_hot`
- `stream:etl:live_warm`
- `stream:etl:maintenance`

Historical streams:

- `stream:etl:historical_discovery`
- `stream:etl:historical_tournament`
- `stream:etl:historical_enrichment`
- `stream:etl:historical_hydrate`
- `stream:etl:historical_maintenance`

Relevant file:

- `schema_inspector/queue/streams.py`

### 3. Worker runtime

Workers claim jobs from Redis consumer groups, execute them, and write job-run status into audit storage.

Important worker classes:

- discovery worker
- hydrate worker
- live worker service
- historical archive workers
- maintenance worker

Relevant files:

- `schema_inspector/services/service_app.py`
- `schema_inspector/services/worker_runtime.py`
- `schema_inspector/workers/discovery_worker.py`
- `schema_inspector/workers/hydrate_worker.py`
- `schema_inspector/workers/live_worker_service.py`
- `schema_inspector/workers/historical_archive_worker.py`
- `schema_inspector/workers/maintenance_worker.py`

### 4. Parsing and durable normalization

Hydrated payloads are parsed by family-specific parsers and written into PostgreSQL through a durable sink.

Important pieces:

- parser registry
- normalize worker
- durable normalize sink
- normalize repository

Relevant files:

- `schema_inspector/parsers/registry.py`
- `schema_inspector/normalizers/worker.py`
- `schema_inspector/normalizers/sink.py`
- `schema_inspector/storage/normalize_repository.py`

### 5. Serving and ops surface

The local API server exposes:

- Sofascore-style read routes
- Swagger / OpenAPI
- `/ops/health`
- `/ops/snapshots/summary`
- `/ops/queues/summary`
- `/ops/jobs/runs`

Relevant file:

- `schema_inspector/local_api_server.py`

## Important Runtime Behaviors

### Backpressure

The system intentionally slows or pauses itself when downstream queues are overloaded.

- planner pauses scheduled planning when hydrate lag is too high
- planner pauses live refresh planning when live queues are too high
- operational discovery skips non-force hydrate fanout under hydrate pressure
- historical discovery defers non-force fanout under historical hydrate pressure

This means the system prefers self-protection over blind completeness.

### `skip`, `defer`, and `retry_scheduled`

- `skip`
  The discovery worker does not publish non-force hydrate jobs when the operational hydrate lane is overloaded.
- `defer`
  Historical discovery raises a retryable admission error and lets the job be replayed later.
- `retry_scheduled`
  The worker runtime classified the failure as retryable and scheduled it into the delayed path instead of marking it failed.

### Live-first reality

Live freshness and full historical throughput are not the same goal.

In practice, production recovery often has to:

- keep live lanes healthy first
- restrict historical bulk work
- slowly re-enable historical stages only after hydrate pressure drops

## Current State Of The Repository

The repository already contains:

- continuous worker commands in `schema_inspector.cli`
- Redis and PostgreSQL runtime integration
- operational endpoints
- durable normalize writes
- recovery and delayed replay behavior
- a broad pytest regression suite

The repository still needs additional hardening before it can honestly be called fully unattended 24x7-safe:

- supervised service management as the default runtime, not tmux
- accurate operator-facing documentation everywhere
- a one-command readiness gate
- explicit live-ready and 24x7-ready exit criteria

## Recommended Reading Order

If you are new to the codebase, read in this order:

1. `README.md`
2. `docs/current-runtime-architecture.md`
3. `docs/live-ready-runbook.md`
4. `docs/live-first-rollout.md`
5. `docs/24x7-exit-criteria.md`
6. `docs/2026-04-17-production-deployment-guide.md`
