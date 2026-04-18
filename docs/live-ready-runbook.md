# Live-Ready Runbook

## Goal

This runbook defines when the system is healthy enough to serve the live application safely, even if historical backlog still exists.

## Minimum Live-Ready Gates

All of the following must be true:

- `stream:etl:discovery lag == 0`
- `stream:etl:live_discovery lag == 0`
- `stream:etl:hydrate lag` is flat or decreasing
- `stream:etl:live_hot lag` is flat or decreasing
- no fresh `TimeoutError` in hydrate worker logs for at least 30 minutes
- `failed` job count is not increasing

Historical backlog may still exist. That does not automatically block live readiness as long as it does not destabilize live.

## Stop Conditions

Immediately stop promotion or scale-up if any of these happens:

- a new `TimeoutError` appears in any hydrate or historical-hydrate log
- `stream:etl:hydrate lag` grows for 10 to 15 minutes straight
- `stream:etl:live_hot lag` resumes sustained growth
- `failed` job count increases
- planner stops draining and begins to oscillate upward

## Required Checks

### One-command readiness gate

```bash
python3 scripts/prod_readiness_check.py --base-url http://127.0.0.1:8000
```

Healthy pattern:

- script exits with code `0`
- final line is `READY`

### Queue summary

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
print("live_lanes=", data["live_lanes"])
PY
```

### Planner health

```bash
tail -n 20 /opt/sofascore/logs/planner.log
```

Healthy pattern:

- planner still logs backpressure pauses
- `hydrate` and `live_hot` lag numbers are flat or declining

### Timeout scan

```bash
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-1.log | tail -n 20
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-2.log | tail -n 20
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-3.log | tail -n 20
grep -E "TimeoutError" /opt/sofascore/logs/historical-hydrate-1.log | tail -n 20
grep -E "TimeoutError" /opt/sofascore/logs/historical-hydrate-2.log | tail -n 20
```

Healthy pattern:

- no new output

### Recent job runs

```bash
curl -s "http://127.0.0.1:8000/ops/jobs/runs?limit=20"
```

Healthy pattern:

- `succeeded` keeps growing
- `failed` stays flat
- `retry_scheduled` can grow for historical deferred jobs without meaning the system is failing

## Interpretation Rules

### Good

- discovery lanes drained
- hydrate drains
- live hot drains
- no new timeouts
- retry-scheduled is mostly admission deferral, not DB panic

### Bad

- hydrate grows again
- live hot grows again
- timeout signature returns
- failed jobs rise

## Live-First Rule

Do not wait for all historical backlog to reach zero before restoring live confidence.

The correct question is:

- "can the live path stay fresh and stable?"

Not:

- "is the historical backlog fully empty?"

## Promotion Rule

If all live-ready gates hold for 30 minutes:

- keep the current runtime profile
- do not re-enable historical bulk yet unless historical hydrate is also trending down safely

If they hold for longer and operational throughput remains safe:

- consider controlled scale-up of operational hydrate only
- re-enable historical stages later and one at a time
