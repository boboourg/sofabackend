# Production Go-Live Gates

Use this checklist before calling the Hybrid ETL backend production-ready.

## Hard Gates

All of these must be green in `/ops/health`:

- `database_ok = true`
- `redis_ok = true`
- `go_live.ready = true`
- no `error` entries in `go_live.flags`

## Gate Meanings

The current `go_live` block enforces:

- latest snapshot younger than 5 minutes
- historical enrichment lag below 1000
- historical retry share below 1% over the last 2 hours
- housekeeping not running in dry-run mode
- no live snapshot drift flags

The same health payload also surfaces warning flags for queue pressure in:

- `hydrate`
- `live_hot`
- `live_warm`
- `historical_hydrate`
- `structure_sync`

Warnings do not block `go_live.ready`, but they should be reviewed before declaring the contour stable.

## Recommended Soak Check

Even after `go_live.ready = true`, keep the contour under observation for at least 2 hours:

- `historical_enrichment` remains below 1000
- retry share does not climb above 1%
- `/ops/queues/summary` shows no growing hot-path backlog
- `/ops/health` keeps `go_live.ready = true`

## Commands

```bash
curl -s http://127.0.0.1:8000/ops/health
curl -s http://127.0.0.1:8000/ops/queues/summary
curl -s "http://127.0.0.1:8000/ops/jobs/runs?limit=20"
```

## Today's Practical Rule

If `go_live.ready = false`, do not call the backend fully production-green yet.

If `go_live.ready = true` but warning flags remain, the platform is up and self-running, but still in stabilization / soak mode.
