# Sofascore Live And 24x7 Readiness Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the project from stable-but-degraded recovery mode to a live-safe and then 24x7-safe production runtime.

**Architecture:** Keep the current Redis-stream and worker topology, but harden the system in two gates. Gate 1 is `live-ready`: protect user-facing freshness first while historical backlog stays constrained. Gate 2 is `24x7-ready`: make the runtime self-healing, observable, documented, and reproducible without tmux-only manual operations.

**Tech Stack:** Python 3.11, Redis Streams, PostgreSQL, asyncpg, pytest, local ops API, Linux systemd

---

### Task 1: Freeze The Current Control Plane As The New Baseline

**Files:**
- Modify: `D:\sofascore\README.md`
- Modify: `D:\sofascore\PROJECT_OVERVIEW.md`
- Create: `D:\sofascore\docs\current-runtime-architecture.md`
- Test: `D:\sofascore\.venv311\Scripts\python.exe -m pytest -q`

- [ ] **Step 1: Replace outdated project description in the README**

Add a top section that says the project is now a continuous ETL runtime with Redis queues, PostgreSQL durable sinks, scheduled discovery, live refresh lanes, and historical archival workers.

- [ ] **Step 2: Replace the outdated architecture section in `PROJECT_OVERVIEW.md`**

Document the real pipeline:

```text
planner -> discovery/live_discovery/historical_discovery
        -> hydrate/live_hot/live_warm/historical_hydrate
        -> parsers -> durable normalize sink -> PostgreSQL
```

- [ ] **Step 3: Create one source-of-truth runtime architecture doc**

Document:
- queue names and worker groups
- which workers are operational vs historical
- where backpressure is enforced
- what `skip` means
- what `defer` means
- what `retry_scheduled` means

- [ ] **Step 4: Re-run the full test suite**

Run:

```powershell
D:\sofascore\.venv311\Scripts\python.exe -m pytest -q
```

Expected:

```text
all tests pass
```

- [ ] **Step 5: Commit**

```bash
git add README.md PROJECT_OVERVIEW.md docs/current-runtime-architecture.md
git commit -m "docs: describe current continuous runtime architecture"
```

### Task 2: Define Live-Ready Gates And Alert Thresholds

**Files:**
- Create: `D:\sofascore\docs\live-ready-runbook.md`
- Modify: `D:\sofascore\docs\2026-04-17-production-deployment-guide.md`
- Test: `curl http://127.0.0.1:8000/ops/queues/summary`

- [ ] **Step 1: Write explicit live SLO gates**

Document the minimum live-safe gates:
- `stream:etl:discovery lag == 0`
- `stream:etl:live_discovery lag == 0`
- `stream:etl:hydrate lag` is flat or decreasing
- `stream:etl:live_hot lag` is flat or decreasing
- no fresh `TimeoutError` in any hydrate worker log for at least 30 minutes

- [ ] **Step 2: Add stop conditions**

Document rollback triggers:
- any new `TimeoutError`
- `hydrate lag` grows for 10 to 15 minutes straight
- `live_hot lag` resumes sustained growth
- `failed` count increases

- [ ] **Step 3: Add the live check commands**

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
PY

tail -n 20 /opt/sofascore/logs/planner.log
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-1.log | tail -n 20
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-2.log | tail -n 20
grep -E "TimeoutError" /opt/sofascore/logs/hydrate-3.log | tail -n 20
```

- [ ] **Step 4: Commit**

```bash
git add docs/live-ready-runbook.md docs/2026-04-17-production-deployment-guide.md
git commit -m "docs: add live-ready gates and rollback triggers"
```

### Task 3: Replace Tmux-Only Runtime Control With Supervised Services

**Files:**
- Create: `D:\sofascore\ops\systemd\sofascore-api.service`
- Create: `D:\sofascore\ops\systemd\sofascore-planner.service`
- Create: `D:\sofascore\ops\systemd\sofascore-discovery@.service`
- Create: `D:\sofascore\ops\systemd\sofascore-hydrate@.service`
- Create: `D:\sofascore\ops\systemd\sofascore-live-hot@.service`
- Create: `D:\sofascore\ops\systemd\sofascore-historical-hydrate@.service`
- Modify: `D:\sofascore\docs\2026-04-17-production-deployment-guide.md`
- Test: `systemctl daemon-reload`, `systemctl status ...`

- [ ] **Step 1: Create a templated worker service unit**

Use one worker template per class of worker with:
- `Restart=always`
- `RestartSec=5`
- `WorkingDirectory=/opt/sofascore`
- `EnvironmentFile=/opt/sofascore/.env`
- stdout/stderr routed to journald

- [ ] **Step 2: Create explicit service examples**

Document launch examples:

```bash
sudo systemctl enable --now sofascore-planner.service
sudo systemctl enable --now sofascore-discovery@1.service
sudo systemctl enable --now sofascore-hydrate@1.service
sudo systemctl enable --now sofascore-hydrate@2.service
sudo systemctl enable --now sofascore-hydrate@3.service
```

- [ ] **Step 3: Add safe scale-up and scale-down commands**

Document:

```bash
sudo systemctl start sofascore-hydrate@4.service
sudo systemctl stop sofascore-hydrate@4.service
```

- [ ] **Step 4: Add boot-recovery instructions**

Document how to verify the server returns to a working state after reboot:
- API up
- planner up
- discovery up
- hydrate workers up
- ops endpoints respond

- [ ] **Step 5: Commit**

```bash
git add ops/systemd docs/2026-04-17-production-deployment-guide.md
git commit -m "ops: add systemd units for supervised runtime control"
```

### Task 4: Add An Operational Readiness Script

**Files:**
- Create: `D:\sofascore\scripts\prod_readiness_check.py`
- Create: `D:\sofascore\tests\test_prod_readiness_check.py`
- Modify: `D:\sofascore\docs\2026-04-17-production-deployment-guide.md`

- [ ] **Step 1: Write tests for the readiness gate**

The script should fail if:
- Redis backend is unavailable
- `/ops/queues/summary` is unreachable
- any required stream is missing
- `failed` jobs increased in the inspection window
- a recent worker log contains `TimeoutError`

- [ ] **Step 2: Implement the script**

The script should print:
- queue lags for `hydrate`, `historical_hydrate`, `live_hot`
- whether `discovery` and `live_discovery` are drained
- whether any new timeouts were seen
- a final `READY` or `NOT_READY`

- [ ] **Step 3: Add the operator command**

```bash
python3 scripts/prod_readiness_check.py --base-url http://127.0.0.1:8000
```

- [ ] **Step 4: Commit**

```bash
git add scripts/prod_readiness_check.py tests/test_prod_readiness_check.py docs/2026-04-17-production-deployment-guide.md
git commit -m "ops: add automated production readiness gate"
```

### Task 5: Add Load And Capacity Regression Coverage

**Files:**
- Create: `D:\sofascore\tests\test_capacity_backpressure_policy.py`
- Modify: `D:\sofascore\tests\test_discovery_worker_service.py`
- Modify: `D:\sofascore\tests\test_planner_daemon.py`

- [ ] **Step 1: Add a queue-growth regression test**

Simulate high hydrate lag and assert:
- scheduled planning pauses
- live planning pauses
- operational discovery skips non-force fanout
- historical discovery defers non-force fanout

- [ ] **Step 2: Add a recovery regression test**

Simulate lag falling back under threshold and assert:
- planning resumes
- discovery resumes fanout
- deferred historical jobs replay into historical streams

- [ ] **Step 3: Add a worker-scale safety test**

Model higher operational throughput and assert the policy still protects historical lanes when historical hydrate lag remains above threshold.

- [ ] **Step 4: Run focused tests**

```powershell
D:\sofascore\.venv311\Scripts\python.exe -m pytest tests\test_discovery_worker_service.py tests\test_planner_daemon.py tests\test_capacity_backpressure_policy.py -q
```

- [ ] **Step 5: Commit**

```bash
git add tests/test_discovery_worker_service.py tests/test_planner_daemon.py tests/test_capacity_backpressure_policy.py
git commit -m "test: add capacity and recovery policy coverage"
```

### Task 6: Define The Live-First Production Rollout

**Files:**
- Create: `D:\sofascore\docs\live-first-rollout.md`
- Modify: `D:\sofascore\docs\2026-04-17-production-deployment-guide.md`

- [ ] **Step 1: Document the allowed runtime profile**

At first production stage:
- planner enabled
- discovery enabled
- live discovery enabled
- `hydrate1..3` enabled
- historical discovery enabled with defer
- historical tournament disabled
- historical enrichment disabled

- [ ] **Step 2: Document the promotion gates**

Promotion to the next stage requires:
- `hydrate lag` decreasing for at least 30 minutes
- `historical_hydrate lag` decreasing for at least 30 minutes
- no new `TimeoutError`
- no increase in `failed`

- [ ] **Step 3: Document the historical re-enable order**

Strict order:
1. `historicaltournament`
2. `historicalenrichment1`
3. `historicalenrichment2`

Each step gets a 15 to 20 minute observation window.

- [ ] **Step 4: Commit**

```bash
git add docs/live-first-rollout.md docs/2026-04-17-production-deployment-guide.md
git commit -m "docs: define live-first production rollout gates"
```

### Task 7: Define 24x7-Ready Exit Criteria

**Files:**
- Create: `D:\sofascore\docs\24x7-exit-criteria.md`

- [ ] **Step 1: Define the minimum 24x7 bar**

The system is only 24x7-ready when all are true:
- no manual `tmux` intervention required for normal runtime
- supervised restart exists
- current architecture docs are accurate
- one-command readiness check exists
- live queues stay fresh under daily workload
- historical workers can run without causing a live regression
- no recurring timeout signature in hydrate and historical hydrate logs

- [ ] **Step 2: Define the final sign-off window**

Require:
- one full day with normal scheduled/live traffic
- no fresh `TimeoutError`
- no rising `failed`
- no sustained growth in `hydrate` or `live_hot`

- [ ] **Step 3: Commit**

```bash
git add docs/24x7-exit-criteria.md
git commit -m "docs: define 24x7 production exit criteria"
```

## Success Criteria

- `Live-ready` means the app can serve current users without runaway backlog or repeated DB timeouts.
- `24x7-ready` means the runtime is supervised, observable, documented, and survives normal daily load without manual babysitting.
- The project is not considered done when queues merely drain slowly; it is done when the system is both stable and operationally repeatable.

