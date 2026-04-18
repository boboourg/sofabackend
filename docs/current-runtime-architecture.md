# Current Runtime Architecture

## Why This Document Exists

This file is the source-of-truth description of the runtime that exists today.

It documents:

- queue names
- worker groups
- operational versus historical lanes
- backpressure boundaries
- the meaning of `skip`, `defer`, and `retry_scheduled`

## Topology

```text
scheduled planner
live discovery planner
historical date planner
historical tournament planner
    -> Redis Streams
    -> discovery/live/historical workers
    -> hydrate/live/historical workers
    -> parsers
    -> durable normalize sink
    -> PostgreSQL
    -> local API + ops endpoints
```

## Streams And Consumer Groups

### Operational

| Stream | Group | Purpose |
|---|---|---|
| `stream:etl:discovery` | `cg:discovery` | scheduled surface discovery |
| `stream:etl:live_discovery` | `cg:live_discovery` | live sport-surface discovery |
| `stream:etl:hydrate` | `cg:hydrate` | event hydration jobs |
| `stream:etl:live_hot` | `cg:live_hot` | hot live refresh |
| `stream:etl:live_warm` | `cg:live_warm` | warm live refresh |
| `stream:etl:maintenance` | `cg:maintenance` | reclaim and recovery work |

### Historical

| Stream | Group | Purpose |
|---|---|---|
| `stream:etl:historical_discovery` | `cg:historical_discovery` | historical date discovery |
| `stream:etl:historical_tournament` | `cg:historical_tournament` | unique tournament / season archival jobs |
| `stream:etl:historical_enrichment` | `cg:historical_enrichment` | archival enrichment |
| `stream:etl:historical_hydrate` | `cg:historical_hydrate` | historical event hydration |
| `stream:etl:historical_maintenance` | `cg:historical_maintenance` | historical reclaim and recovery |

## Worker Classes

### Planners

- `planner-daemon`
  publishes scheduled discovery and live refresh work
- `live-discovery-planner-daemon`
  publishes live sport-surface discovery
- `historical-planner-daemon`
  publishes historical discovery by date range
- `historical-tournament-planner-daemon`
  publishes historical tournament and enrichment work

### Discovery workers

- `worker-discovery`
  consumes `stream:etl:discovery`
- `worker-live-discovery`
  consumes `stream:etl:live_discovery`
- `worker-historical-discovery`
  consumes `stream:etl:historical_discovery`

### Execution workers

- `worker-hydrate`
- `worker-historical-hydrate`
- `worker-live-hot`
- `worker-live-warm`
- `worker-historical-tournament`
- `worker-historical-enrichment`
- `worker-maintenance`
- `worker-historical-maintenance`

## Backpressure Boundaries

### Planner-level protection

The planner can pause:

- scheduled planning if discovery or hydrate lag is too high
- live refresh planning if live queues are too high

This prevents fresh upstream fanout from endlessly inflating downstream queues.

### Discovery admission control

Operational discovery checks the operational hydrate lane.

If the hydrate lane is overloaded:

- non-force event fanout is skipped
- force rehydrate corrections still publish

Historical discovery checks the historical hydrate lane.

If the historical hydrate lane is overloaded:

- non-force event fanout is deferred
- the job is retried later through the delayed path

## Meaning Of Runtime Statuses

### `skip`

`skip` means the worker intentionally chose not to publish non-force hydrate children because operational capacity was already saturated.

This is not a crash.

### `defer`

`defer` means the worker raised a retryable admission error because the downstream historical lane was overloaded.

This is also not a crash. It is controlled backpressure.

### `retry_scheduled`

`retry_scheduled` means the worker runtime caught a retryable failure and scheduled the job back into delayed execution instead of marking it failed.

This can happen because of:

- admission deferral
- retryable DB timeout
- lock timeout
- deadlock-like signatures

## Why This Matters

The runtime can now protect itself from runaway queue growth much better than before, but there is still a major difference between:

- "the system stayed alive"
- "the user-facing live app stayed fresh"

That is why readiness is split into:

- `live-ready`
- `24x7-ready`

## Related Documents

- `README.md`
- `docs/live-ready-runbook.md`
- `docs/live-first-rollout.md`
- `docs/24x7-exit-criteria.md`
- `docs/2026-04-17-production-deployment-guide.md`
