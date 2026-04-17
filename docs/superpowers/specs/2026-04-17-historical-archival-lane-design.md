# Historical Archival Lane Design

**Date:** 2026-04-17

## Goal

Add a second, archival ETL contour that can backfill historical date ranges for all 13 sports without changing the already-running operational `today + live` contour.

## Scope

This first implementation slice adds a historical **date-range lane**:

- separate Redis Streams, consumer groups, and tmux workers
- a historical planner that walks an inclusive `date_from..date_to` range per sport
- historical discovery and hydrate workers that reuse the existing hybrid orchestrator
- historical maintenance/reclaim coverage
- queue monitoring exposure in `/ops/queues/summary`

This slice does **not** yet implement season/tournament archival planning. Existing CLIs such as `current_year_pipeline_cli.py` and `default_tournaments_pipeline_cli.py` remain available for deeper tournament/season sweeps and are the next natural extension after the date lane is stable.

## Non-Goals

- No changes to existing operational stream names, consumer groups, or tmux commands
- No change to live hot/warm behavior
- No attempt to merge operational and archival planners into one loop

## Architecture

### Operational Contour

Keep the current contour unchanged:

- `planner-daemon`
- `worker-discovery`
- `worker-hydrate`
- `worker-live-hot`
- `worker-live-warm`
- `worker-maintenance`

### Historical Contour

Add a new archival contour:

- `historical-planner-daemon`
- `worker-historical-discovery`
- `worker-historical-hydrate`
- `worker-historical-maintenance`

The historical planner only publishes historical date jobs. It does not touch live queues.

## Streams and Groups

New Redis Streams:

- `stream:etl:historical_discovery`
- `stream:etl:historical_hydrate`
- `stream:etl:historical_maintenance`

New consumer groups:

- `cg:historical_discovery`
- `cg:historical_hydrate`
- `cg:historical_maintenance`

## Data Flow

1. `historical-planner-daemon` publishes `discover_sport_surface` jobs with:
   - `scope=historical`
   - `params.date=<historical-date>`
2. `worker-historical-discovery` reuses the existing event-list discovery path and expands those jobs into `hydrate_event_root`.
3. `worker-historical-hydrate` reuses the existing hybrid event hydrator.
4. `worker-historical-maintenance` reclaims stale archival jobs and requeues/DLQs them independently of operational queues.

## Cursoring and Restart Safety

The archival planner stores a per-sport cursor in Redis so restarts do not rewind to the beginning of the range on every launch. The cursor only tracks the next unpublished date for each sport.

## Monitoring

`/ops/queues/summary` should include the archival streams so operational visibility works the same way for the second contour.

## Testing

Add tests for:

- archival stream/group registration
- historical CLI command parsing and dispatch
- historical planner date cursor progression
- historical service wiring
- operational queue summary including archival streams

