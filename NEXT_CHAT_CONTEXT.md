# Next Chat Context

This file is the current handoff source of truth for the next chat.
It supersedes the older `NEXT_CHAT_CONTEXT.md` from `2026-04-16`.

## Date And Environment

- Repo: `D:\sofascore`
- Local OS: Windows
- Local shell: PowerShell
- Local Python: `D:\sofascore\.venv311\Scripts\python.exe`
- Remote server repo: `/opt/sofascore`
- Remote server shell: bash
- Remote server Python: use `python3`, not `python`
- Git remote: `origin -> https://github.com/boboourg/sofabackend.git`
- Deployment flow in reality:
  - local changes in `D:\sofascore`
  - `git push origin main`
  - server: `cd /opt/sofascore && git pull --ff-only origin main`

Important operator truth:

- this is **not** a fresh deploy
- this is **not** a new branch workflow
- do **not** suggest "first deployment" or "new branch then track on server" unless the user explicitly asks for it

The user called this out already and was right.

## Why This Chat Happened

This chat moved through three connected tracks:

1. continuous-runtime stabilization under large queue backlog
2. live-first recovery and controlled hydrate worker scaling
3. frontend/API documentation for a mobile-first Sofascore-style MVP

The key operational question became:

- can the backend survive and drain without new DB timeout meltdowns?
- can it already serve something useful to a frontend?

## Current Big Picture

The backend is no longer a small inspection tool.
It is now a continuous multi-sport ETL runtime with:

- Redis Streams
- planners
- discovery workers
- hydrate workers
- historical lanes
- PostgreSQL normalize sink
- a local multi-sport read API
- ops monitoring endpoints

The architecture source of truth is:

- `D:\sofascore\docs\current-runtime-architecture.md`

Core topology:

`planner/live-discovery/historical planners -> Redis Streams -> discovery/live/historical workers -> hydrate/live/historical workers -> parsers -> durable normalize sink -> PostgreSQL -> local API + ops endpoints`

## What Changed In This Chat

### 1. Operational readiness / runtime hardening docs

These files were added or rewritten:

- `D:\sofascore\README.md`
- `D:\sofascore\PROJECT_OVERVIEW.md`
- `D:\sofascore\docs\current-runtime-architecture.md`
- `D:\sofascore\docs\live-ready-runbook.md`
- `D:\sofascore\docs\live-first-rollout.md`
- `D:\sofascore\docs\24x7-exit-criteria.md`
- `D:\sofascore\docs\2026-04-17-production-deployment-guide.md`
- `D:\sofascore\docs\superpowers\plans\2026-04-18-live-24x7-readiness-plan.md`

### 2. systemd service files for server runtime

Added under:

- `D:\sofascore\ops\systemd\*.service`

These cover API, planners, hydrate, historical hydrate, discovery, live hot/warm, maintenance, and historical archival lanes.

### 3. Baseline readiness tooling

Added:

- `D:\sofascore\scripts\prod_readiness_check.py`
- `D:\sofascore\tests\test_prod_readiness_check.py`
- `D:\sofascore\tests\test_capacity_backpressure_policy.py`

### 4. Frontend handoff docs

Added:

- `D:\sofascore\backend_to_front_mvp.md`
- `D:\sofascore\ai_mobile_mvp_prompt.md`

These describe:

- what the backend can already serve to a frontend
- how to think about `sport -> category -> uniqueTournament -> season -> event`
- how to design a mobile-first MVP around real backend capabilities
- how to build image URLs from Sofascore image endpoints instead of expecting logos in the DB

## Full Test Status On Local Machine

At the time the runtime-hardening batch was completed locally:

- targeted suites passed
- full `pytest -q` passed with:
  - `340 passed, 17 subtests passed`

No fresh local full test rerun was performed after the final docs-only frontend handoff files.

## Important Truths The Next AI Must Not Get Wrong

### 1. `prod_readiness_check.py` is **not** a full Slice 2 gate

This is critical.

The script is only a **baseline live-ready check**.
It verifies:

- required streams exist
- `stream:etl:discovery` lag is `0`
- `stream:etl:live_discovery` lag is `0`
- no `TimeoutError` in hydrate/historical-hydrate logs
- no recent `failed` jobs in latest runs

It does **not** verify:

- that `stream:etl:hydrate` is decreasing over time
- that `stream:etl:live_hot` is decreasing over time
- that adding another hydrate worker improved net throughput

This misunderstanding already happened once in chat and irritated the user.
Do not repeat it.

### 2. `READY` from the script does **not** mean "24/7-ready"

It only means:

- the system is not in an obvious live-path failure state

It does **not** mean:

- backlog is small
- freshness is guaranteed
- unattended production is proven

### 3. The system is currently in `Slice 2`, not `Slice 3`

Current operational interpretation:

- `Slice 1` runaway growth was stopped
- `Slice 2` is controlled drain / throughput tuning
- `Slice 3` historical bulk re-enablement is still too early

### 4. Historical bulk stays off

At this stage:

- `historical-tournament`
- `historical-enrichment@1`
- `historical-enrichment@2`

should remain off unless the user explicitly chooses a new experiment.

### 5. The backend can already serve frontend data

This is no longer theoretical.

The local API already exposes:

- Swagger at `/`
- OpenAPI at `/openapi.json`
- ops endpoints
- `/api/...` Sofascore-style read routes for sport lists, leagues, standings, events, lineups, comments, odds, players, teams, etc.

But:

- data coverage depends on what was ingested
- valid contract routes can still return "known route but no ingested payload yet"

## Current Backend Capability For Frontend

The backend is already capable of powering a mobile-first MVP.

Safe product areas:

- home feed with live and upcoming matches
- sport hub
- category/country screens
- league/competition screens
- standings/table screens
- match center
- basic team pages
- basic player pages

Key conceptual hierarchy:

`sport -> category(country/region) -> uniqueTournament(league) -> season -> event(match)`

Image/media rule:

- logos and photos should be built from Sofascore image URLs, not expected from our DB

Examples:

- country flag:
  - `https://img.sofascore.com/api/v1/country/ES/flag`
- team:
  - `https://img.sofascore.com/api/v1/team/2836/image`
- unique tournament / league:
  - `https://img.sofascore.com/api/v1/unique-tournament/218/image`
- manager:
  - `https://img.sofascore.com/api/v1/manager/53376/image`
- player:
  - `https://img.sofascore.com/api/v1/player/944656/image`

Public frontend docs created in this chat:

- `D:\sofascore\backend_to_front_mvp.md`
- `D:\sofascore\ai_mobile_mvp_prompt.md`

## Current Production / Server State

### Summary

The server was in heavy backlog recovery.
The recovery plan shifted to **live-first** instead of "wait for every historical queue to clear".

Sequence:

1. `hydrate3` was started as a controlled throughput increase
2. multiple 20-minute queue deltas showed real negative lag without new timeouts
3. after repeated stable intervals, `hydrate4` was started as an attended experiment
4. latest observed state before this handoff: `hydrate4` was left running for ~12 hours to observe overnight behavior

### What was already proven before `hydrate4`

With `hydrate3`, real interval deltas included:

- `stream:etl:hydrate: delta=-3040` over 20 minutes
- `stream:etl:live_hot: delta=-247`
- `stream:etl:historical_hydrate: delta=-1246`

Later interval:

- `stream:etl:hydrate: delta=-1019`
- `stream:etl:live_hot: delta=-134`
- `stream:etl:historical_hydrate: delta=-684`

Interpretation:

- controlled drain was real
- system was stable
- no new `TimeoutError`
- but throughput was slow and still under strong backpressure

### What was already proven after `hydrate4` start

Observed samples after starting `hydrate4`:

- `Sun Apr 19 02:12:37 EEST 2026`
  - `stream:etl:hydrate: lag=770955`
  - `stream:etl:live_hot: lag=901441`
  - `stream:etl:historical_hydrate: lag=349617`
- `Sun Apr 19 02:20:13 EEST 2026`
  - `stream:etl:hydrate: lag=770247`
  - `stream:etl:live_hot: lag=901325`
  - `stream:etl:historical_hydrate: lag=349379`
- `Sun Apr 19 02:22:33 EEST 2026`
  - `stream:etl:hydrate: lag=770030`
  - `stream:etl:live_hot: lag=901318`
  - `stream:etl:historical_hydrate: lag=349310`
  - `failed=11`
  - `retry_scheduled=13583`
  - fresh `jobRuns` included `worker_id=hydrate-4` with `status=succeeded`
  - no new `TimeoutError`

Interpretation at that point:

- `hydrate4` did **not** immediately break the system
- queue movement stayed downward
- planner remained in backpressure pause mode
- historical discovery still deferred
- the system was not "stuck"; it was in a slow controlled drain / throughput ceiling state

### Most recent operator action

The user explicitly said:

- they left `hydrate4` running for 12 hours

That means the very next AI should begin by checking:

- whether `hydrate4` stayed healthy overnight
- whether lag continued to decrease
- whether `TimeoutError` returned
- whether `failed` remained flat

## Current Operational Interpretation

The system is currently best described as:

- stable enough for continued live-first drain
- not yet safe to call unattended 24/7-ready
- not yet ready for `Slice 3`
- capable of feeding a frontend, but with partial-data awareness

## Server Commands That Matter

### 1. Baseline live check

```bash
cd /opt/sofascore
python3 scripts/prod_readiness_check.py --base-url http://127.0.0.1:8000 || true
```

Remember:

- this is baseline-only
- do not treat it as proof of throughput health

### 2. Queue summary

```bash
python3 - <<'PY'
import json, urllib.request
data = json.load(urllib.request.urlopen("http://127.0.0.1:8000/ops/queues/summary"))
for item in data["streams"]:
    if item["stream"] in {
        "stream:etl:hydrate",
        "stream:etl:live_hot",
        "stream:etl:historical_discovery",
        "stream:etl:historical_hydrate",
    }:
        print(f'{item["stream"]}: lag={item["lag"]} entries_read={item["entries_read"]} length={item["length"]}')
print("delayed_total=", data["delayed_total"], "delayed_due=", data["delayed_due"])
print("live_lanes=", data["live_lanes"])
PY
```

### 3. Planner log

```bash
tail -n 20 /opt/sofascore/logs/planner.log
```

### 4. Timeout checks

```bash
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-1.log | tail -n 5
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-2.log | tail -n 5
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-3.log | tail -n 5
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-4.log | tail -n 5
grep -E "TimeoutError" /opt/sofascore/logs/historical-hydrate-1.log | tail -n 5
grep -E "TimeoutError" /opt/sofascore/logs/historical-hydrate-2.log | tail -n 5
```

### 5. Job run status

```bash
curl -s "http://127.0.0.1:8000/ops/jobs/runs?limit=20"
```

## Decision Rules For The Next AI

### If overnight `hydrate4` looks healthy

Healthy means all of these are true:

- no new `TimeoutError`
- `failed` remains flat
- `hydrate` lag is down vs previous known values
- `live_hot` lag is down or at least not meaningfully worse
- `hydrate-4` appears in fresh `jobRuns` as `succeeded`

Then:

- keep `hydrate4`
- keep historical bulk off
- keep calling this `Slice 2`
- do **not** promote to `Slice 3` yet

### If overnight `hydrate4` looks unhealthy

Unhealthy means any of:

- new `TimeoutError`
- `failed` starts increasing
- `hydrate` or `live_hot` clearly trend upward
- `hydrate-4` disappears or only fails/retries

Then:

- kill `hydrate4`
- return to the known safer `hydrate3` profile
- continue live-first drain

Rollback command:

```bash
tmux kill-session -t hydrate4
```

## What The Next AI Should Do First

When the next chat starts, the first useful sequence is:

1. read this file
2. confirm current server state with the commands above
3. compare new lag values against the latest known `hydrate4` values from this file
4. decide:
   - keep `hydrate4`
   - or kill `hydrate4`
5. do **not** jump to `Slice 3`
6. do **not** confuse baseline `READY` with full readiness

## What The Next AI Should Avoid

Do not:

- treat `prod_readiness_check.py` as a throughput oracle
- suggest a fresh deploy flow
- suggest branch-based server deployment unless the user asks
- claim the backend is already "24/7 without problems"
- enable historical tournament/enrichment lanes just because the system is alive
- forget that the user is specifically sensitive to loss of context and repeated explanations

## Files To Open First In The Next Chat

1. `D:\sofascore\NEXT_CHAT_CONTEXT.md`
2. `D:\sofascore\docs\current-runtime-architecture.md`
3. `D:\sofascore\docs\live-ready-runbook.md`
4. `D:\sofascore\docs\2026-04-17-production-deployment-guide.md`
5. `D:\sofascore\scripts\prod_readiness_check.py`
6. `D:\sofascore\tests\test_capacity_backpressure_policy.py`
7. `D:\sofascore\schema_inspector\local_api_server.py`
8. `D:\sofascore\schema_inspector\endpoints.py`
9. `D:\sofascore\backend_to_front_mvp.md`
10. `D:\sofascore\ai_mobile_mvp_prompt.md`

## Final Reminder

The current product truth is:

- backend is already capable of serving useful frontend data
- the runtime is alive and draining under backpressure
- `hydrate4` was promising but still needs overnight validation
- the system is in controlled recovery, not final production readiness

The next AI should act like an operator with context, not like a fresh consultant walking in cold.
