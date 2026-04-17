# Phase 9.5 Operationalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the hybrid ETL safe to run continuously by failing closed on Redis, adding an automatic database audit gate, and hardening operational observability around `scheduled` and `live`.

**Architecture:** Keep the current one-shot CLI as the production entrypoint, but add a strict runtime guard around Redis selection and a post-run audit layer that validates raw ingestion, durable sinks, and live-state persistence. This keeps rollout incremental: we do not redesign the backbone first; we make the existing backbone safe, measurable, and repeatable.

**Tech Stack:** Python 3.11, asyncpg, Redis, PostgreSQL, argparse CLI, pytest

---

### Task 1: Fail Closed on Redis in Production

**Files:**
- Modify: `D:\sofascore\schema_inspector\cli.py`
- Test: `D:\sofascore\tests\test_hybrid_cli.py`

- [ ] **Step 1: Write the failing tests for Redis runtime selection**

```python
def test_load_redis_backend_fails_without_url_in_production(monkeypatch):
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("SOFASCORE_REDIS_URL", raising=False)
    monkeypatch.setenv("SOFASCORE_ENV", "production")

    with pytest.raises(RuntimeError, match="Redis is required"):
        cli._load_redis_backend(None, allow_memory_fallback=False)


def test_load_redis_backend_fails_without_redis_package_in_production(monkeypatch):
    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    monkeypatch.setenv("SOFASCORE_ENV", "production")
    monkeypatch.setitem(sys.modules, "redis", None)

    with pytest.raises(RuntimeError, match="python package `redis`"):
        cli._load_redis_backend(None, allow_memory_fallback=False)
```

- [ ] **Step 2: Run the Redis-selection tests to verify they fail**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_hybrid_cli.py -k redis -q
```

Expected:
```text
FAIL ... RuntimeError not raised
```

- [ ] **Step 3: Implement explicit production-safe Redis policy**

```python
def _load_redis_backend(
    redis_url: str | None,
    *,
    allow_memory_fallback: bool,
):
    resolved_url = redis_url or os.environ.get("REDIS_URL") or os.environ.get("SOFASCORE_REDIS_URL")
    if not resolved_url:
        if allow_memory_fallback:
            LOGGER.warning("Redis URL missing; using in-memory backend.")
            return _MemoryRedisBackend()
        raise RuntimeError("Redis is required for production runs. Set REDIS_URL or SOFASCORE_REDIS_URL.")

    try:
        import redis  # type: ignore
    except ImportError as exc:
        if allow_memory_fallback:
            LOGGER.warning("redis package missing; using in-memory backend.")
            return _MemoryRedisBackend()
        raise RuntimeError("Redis is required for production runs. Install python package `redis`.") from exc

    backend = redis.Redis.from_url(resolved_url, decode_responses=True)
    backend.ping()
    return backend
```

- [ ] **Step 4: Wire CLI policy flags and startup logging**

```python
parser.add_argument(
    "--allow-memory-redis",
    action="store_true",
    help="Development-only escape hatch for running without Redis.",
)
```

```python
redis_backend = _load_redis_backend(
    args.redis_url,
    allow_memory_fallback=bool(args.allow_memory_redis),
)
LOGGER.info("Redis backend ready: backend=%s", type(redis_backend).__name__)
```

- [ ] **Step 5: Re-run the Redis-selection tests and the CLI regression suite**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_hybrid_cli.py -q
```

Expected:
```text
PASS
```

- [ ] **Step 6: Commit**

```bash
git add schema_inspector/cli.py tests/test_hybrid_cli.py
git commit -m "fix: require real redis for production hybrid runs"
```

### Task 2: Upgrade `health` Into a Real Runtime Gate

**Files:**
- Modify: `D:\sofascore\schema_inspector\ops\health.py`
- Modify: `D:\sofascore\schema_inspector\ops\metrics.py`
- Modify: `D:\sofascore\schema_inspector\cli.py`
- Test: `D:\sofascore\tests\test_hybrid_cli.py`

- [ ] **Step 1: Write the failing tests for Redis and DB connectivity in health**

```python
async def test_health_reports_backend_kind_and_redis_ping(fake_sql_executor):
    report = await collect_health_report(
        sql_executor=fake_sql_executor,
        live_state_store=FakeLiveStateStore(FakeRedisBackend()),
        redis_backend=FakeRedisBackend(),
    )

    assert report.redis_backend_kind == "fakeredis"
    assert report.redis_ok is True
    assert report.database_ok is True
```

- [ ] **Step 2: Run the targeted health tests to verify they fail**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_hybrid_cli.py -k health -q
```

Expected:
```text
FAIL ... attribute redis_backend_kind does not exist
```

- [ ] **Step 3: Expand `HealthReport` with runtime-critical fields**

```python
@dataclass(frozen=True)
class HealthReport:
    snapshot_count: int
    capability_rollup_count: int
    live_hot_count: int
    live_warm_count: int
    live_cold_count: int
    database_ok: bool
    redis_ok: bool
    redis_backend_kind: str
```

```python
async def collect_health_report(*, sql_executor, live_state_store=None, redis_backend=None) -> HealthReport:
    snapshot_count = int(await _fetch_count(sql_executor, "SELECT COUNT(*) FROM api_payload_snapshot"))
    capability_rollup_count = int(await _fetch_count(sql_executor, "SELECT COUNT(*) FROM endpoint_capability_rollup"))
    redis_ok = _ping_redis(redis_backend)
    return HealthReport(
        snapshot_count=snapshot_count,
        capability_rollup_count=capability_rollup_count,
        live_hot_count=_lane_count(live_state_store, "hot"),
        live_warm_count=_lane_count(live_state_store, "warm"),
        live_cold_count=_lane_count(live_state_store, "cold"),
        database_ok=True,
        redis_ok=redis_ok,
        redis_backend_kind=type(redis_backend).__name__.lower() if redis_backend is not None else "none",
    )
```

- [ ] **Step 4: Print the richer health line from the CLI**

```python
print(
    "health "
    f"db_ok={int(report.database_ok)} "
    f"redis_ok={int(report.redis_ok)} "
    f"redis_backend={report.redis_backend_kind} "
    f"snapshots={report.snapshot_count} "
    f"rollups={report.capability_rollup_count} "
    f"live_hot={report.live_hot_count} "
    f"live_warm={report.live_warm_count} "
    f"live_cold={report.live_cold_count}"
)
```

- [ ] **Step 5: Re-run health tests**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_hybrid_cli.py -k health -q
```

Expected:
```text
PASS
```

- [ ] **Step 6: Commit**

```bash
git add schema_inspector/ops/health.py schema_inspector/ops/metrics.py schema_inspector/cli.py tests/test_hybrid_cli.py
git commit -m "feat: add runtime health checks for redis and postgres"
```

### Task 3: Build the Automatic Database Audit Layer

**Files:**
- Create: `D:\sofascore\schema_inspector\ops\db_audit.py`
- Modify: `D:\sofascore\schema_inspector\cli.py`
- Test: `D:\sofascore\tests\test_ops_db_audit.py`

- [ ] **Step 1: Write the failing audit tests**

```python
async def test_collect_db_audit_counts_raw_and_durable_tables(fake_sql_executor):
    report = await collect_db_audit(
        sql_executor=fake_sql_executor,
        sport_slug="football",
        event_ids=(15632088, 15736636),
    )

    assert report.raw_snapshots > 0
    assert report.events > 0
    assert report.statistics > 0
    assert report.incidents > 0
    assert report.lineups > 0
```

```python
async def test_collect_db_audit_surfaces_special_tables(fake_sql_executor):
    report = await collect_db_audit(
        sql_executor=fake_sql_executor,
        sport_slug="tennis",
        event_ids=(15991729,),
    )

    assert report.special_counts["tennis_point_by_point"] >= 0
    assert report.special_counts["tennis_power"] >= 0
```

- [ ] **Step 2: Run the targeted audit tests to verify they fail**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_ops_db_audit.py -q
```

Expected:
```text
FAIL ... ModuleNotFoundError: No module named 'schema_inspector.ops.db_audit'
```

- [ ] **Step 3: Implement the audit report model and queries**

```python
@dataclass(frozen=True)
class DatabaseAuditReport:
    sport_slug: str
    event_count: int
    raw_requests: int
    raw_snapshots: int
    events: int
    statistics: int
    incidents: int
    lineup_sides: int
    lineup_players: int
    special_counts: dict[str, int]
```

```python
SPECIAL_TABLES_BY_SPORT = {
    "tennis": ("tennis_point_by_point", "tennis_power"),
    "baseball": ("baseball_inning", "baseball_pitch"),
    "ice-hockey": ("shotmap_point",),
    "esports": ("esports_game",),
}
```

```python
async def collect_db_audit(*, sql_executor, sport_slug: str, event_ids: tuple[int, ...]) -> DatabaseAuditReport:
    event_filter = tuple(int(item) for item in event_ids)
    return DatabaseAuditReport(
        sport_slug=sport_slug,
        event_count=len(event_filter),
        raw_requests=await _count(sql_executor, "SELECT COUNT(*) FROM api_request_log WHERE context_event_id = ANY($1::bigint[])", event_filter),
        raw_snapshots=await _count(sql_executor, "SELECT COUNT(*) FROM api_payload_snapshot WHERE context_event_id = ANY($1::bigint[])", event_filter),
        events=await _count(sql_executor, "SELECT COUNT(*) FROM event WHERE id = ANY($1::bigint[])", event_filter),
        statistics=await _count(sql_executor, "SELECT COUNT(*) FROM event_statistic WHERE event_id = ANY($1::bigint[])", event_filter),
        incidents=await _count(sql_executor, "SELECT COUNT(*) FROM event_incident WHERE event_id = ANY($1::bigint[])", event_filter),
        lineup_sides=await _count(sql_executor, "SELECT COUNT(*) FROM event_lineup WHERE event_id = ANY($1::bigint[])", event_filter),
        lineup_players=await _count(sql_executor, "SELECT COUNT(*) FROM event_lineup_player WHERE event_id = ANY($1::bigint[])", event_filter),
        special_counts=await _collect_special_counts(sql_executor, sport_slug=sport_slug, event_ids=event_filter),
    )
```

- [ ] **Step 4: Add a new CLI command for one-shot audit**

```python
audit = subparsers.add_parser("audit-db", help="Print raw and durable DB counts for hydrated events.")
audit.add_argument("--sport-slug", required=True, help="Sport slug for report labelling.")
audit.add_argument("--event-id", type=int, action="append", required=True, help="Repeatable hydrated event id.")
```

- [ ] **Step 5: Print a compact audit line**

```python
print(
    "db_audit "
    f"sport={report.sport_slug} "
    f"events={report.event_count} "
    f"requests={report.raw_requests} "
    f"snapshots={report.raw_snapshots} "
    f"event_rows={report.events} "
    f"statistics={report.statistics} "
    f"incidents={report.incidents} "
    f"lineup_sides={report.lineup_sides} "
    f"lineup_players={report.lineup_players} "
    + " ".join(f"{name}={count}" for name, count in sorted(report.special_counts.items()))
)
```

- [ ] **Step 6: Re-run the targeted audit tests**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_ops_db_audit.py -q
```

Expected:
```text
PASS
```

- [ ] **Step 7: Commit**

```bash
git add schema_inspector/ops/db_audit.py schema_inspector/cli.py tests/test_ops_db_audit.py
git commit -m "feat: add automatic database audit for hybrid runs"
```

### Task 4: Add Post-Run Audit Gating to `scheduled` and `live`

**Files:**
- Modify: `D:\sofascore\schema_inspector\cli.py`
- Test: `D:\sofascore\tests\test_hybrid_cli.py`

- [ ] **Step 1: Write the failing tests for post-run audit**

```python
async def test_scheduled_can_run_with_post_run_audit(monkeypatch):
    args = parser.parse_args(
        ["scheduled", "--sport-slug", "football", "--date", "2026-04-17", "--audit-db"]
    )
    assert args.audit_db is True
```

```python
async def test_live_can_fail_when_audit_finds_no_snapshots(fake_app):
    with pytest.raises(RuntimeError, match="DB audit failed"):
        await run_live_with_audit(fake_app, sport_slug="football")
```

- [ ] **Step 2: Run the CLI tests to verify they fail**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_hybrid_cli.py -k audit -q
```

Expected:
```text
FAIL ... unrecognized arguments: --audit-db
```

- [ ] **Step 3: Add optional audit switches to hydration commands**

```python
for subparser in (event, live, scheduled, backfill):
    subparser.add_argument(
        "--audit-db",
        action="store_true",
        help="Run the automatic DB audit after hydration completes.",
    )
```

- [ ] **Step 4: Run the audit automatically after hydration**

```python
if args.audit_db:
    audit_report = await app.collect_db_audit(
        sport_slug=args.sport_slug,
        event_ids=tuple(report.event_ids),
    )
    _print_db_audit(audit_report)
    if audit_report.raw_snapshots == 0 or audit_report.events == 0:
        raise RuntimeError("DB audit failed: raw or durable counts are empty.")
```

- [ ] **Step 5: Re-run the CLI audit tests**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_hybrid_cli.py -k audit -q
```

Expected:
```text
PASS
```

- [ ] **Step 6: Commit**

```bash
git add schema_inspector/cli.py tests/test_hybrid_cli.py
git commit -m "feat: gate hydration commands with optional db audit"
```

### Task 5: Publish the Production Runbook and Acceptance Commands

**Files:**
- Modify: `D:\sofascore\docs\superpowers\plans\2026-04-17-phase-9-5-operationalization.md`

- [ ] **Step 1: Add the exact smoke commands to the plan**

```powershell
docker run --name sofascore-redis -p 6379:6379 -d redis:7-alpine
docker exec -it sofascore-redis redis-cli ping
.\.venv311\Scripts\python.exe -m pip install redis
.\.venv311\Scripts\python.exe -m schema_inspector.db_setup_cli
.\.venv311\Scripts\python.exe -m schema_inspector.cli health
.\.venv311\Scripts\python.exe -m schema_inspector.cli scheduled --sport-slug football --date 2026-04-17 --event-concurrency 10 --audit-db
.\.venv311\Scripts\python.exe -m schema_inspector.cli live --sport-slug football --event-concurrency 6 --audit-db
.\.venv311\Scripts\python.exe -m schema_inspector.cli audit-db --sport-slug football --event-id 15632088
.\.venv311\Scripts\python.exe -m schema_inspector.local_api_server
```

- [ ] **Step 2: Add the acceptance thresholds**

```text
- Redis smoke check must return PONG
- health must report db_ok=1 redis_ok=1 and redis_backend!=memory
- scheduled must finish without traceback
- scheduled audit must show snapshots>0 and event_rows>0
- live must finish without traceback
- live audit must show snapshots>0 and event_rows>0
- manual audit-db checks must agree with the post-run audit summary for the same event ids
- tennis runs must produce non-zero rows in tennis_point_by_point and tennis_power when those upstream routes return 200
- baseball runs must produce non-zero rows in baseball_inning and baseball_pitch when innings / pitches routes return 200
- ice-hockey runs must produce non-zero rows in shotmap_point when the shotmap route returns 200
- esports runs must produce non-zero rows in esports_game when the esports-games route returns 200
```

**Production smoke sequence**

1. Start Redis and verify the container responds:

```powershell
docker run --name sofascore-redis -p 6379:6379 -d redis:7-alpine
docker exec -it sofascore-redis redis-cli ping
```

Expected:
```text
PONG
```

2. Ensure the real Redis client is installed in the project venv:

```powershell
.\.venv311\Scripts\python.exe -m pip install redis
```

3. Initialize PostgreSQL schema and apply all migrations:

```powershell
.\.venv311\Scripts\python.exe -m schema_inspector.db_setup_cli
```

4. Run the runtime gate before any ingestion:

```powershell
.\.venv311\Scripts\python.exe -m schema_inspector.cli health
```

Expected fields in output:
```text
health db_ok=1 redis_ok=1 redis_backend=redis ...
```

5. Run scheduled hydration with the post-run DB audit gate enabled:

```powershell
.\.venv311\Scripts\python.exe -m schema_inspector.cli scheduled --sport-slug football --date 2026-04-17 --event-concurrency 10 --audit-db
```

Expected behavior:
```text
scheduled_hydrate events=... event_ids=...
db_audit sport=football events=... requests=... snapshots=... event_rows=... statistics=... incidents=... lineup_sides=... lineup_players=...
```

6. Run live hydration with the same post-run DB audit gate:

```powershell
.\.venv311\Scripts\python.exe -m schema_inspector.cli live --sport-slug football --event-concurrency 6 --audit-db
```

Expected behavior:
```text
live_hydrate events=... event_ids=...
db_audit sport=football events=... requests=... snapshots=... event_rows=... statistics=... incidents=... lineup_sides=... lineup_players=...
```

7. Run a direct DB audit against a known hydrated event when debugging or validating spot checks:

```powershell
.\.venv311\Scripts\python.exe -m schema_inspector.cli audit-db --sport-slug football --event-id 15632088
```

8. Start the local read API only after the above gates are green:

```powershell
.\.venv311\Scripts\python.exe -m schema_inspector.local_api_server
```

Expected startup lines:
```text
Serving local multi-sport API on http://127.0.0.1:8000
Swagger UI: http://127.0.0.1:8000/
OpenAPI JSON: http://127.0.0.1:8000/openapi.json
```

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/plans/2026-04-17-phase-9-5-operationalization.md
git commit -m "docs: add phase 9.5 operationalization checklist"
```
