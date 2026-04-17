# Continuous Workers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the current one-shot hybrid ETL commands into long-running planner and worker services that continuously schedule, consume, recover, and persist work without duplicate durable writes.

**Architecture:** Keep the existing `cli.py` commands as operator tools, but add a service runtime on top of the existing Redis Streams, leases, delayed jobs, orchestrator, and durable sinks. The new runtime will be fail-fast on lock contention, requeue retryable jobs through Redis/delayed scheduling, and drain in-flight transactions before shutdown.

**Tech Stack:** Python 3.11, asyncio, asyncpg, redis-py, Redis Streams, PostgreSQL, pytest

---

## File Structure

**Queue Layer**
- Modify: `D:\sofascore\schema_inspector\queue\streams.py`
  Adds consumer-group lifecycle helpers: create group, inspect pending, reclaim stale messages.
- Modify: `D:\sofascore\schema_inspector\queue\leases.py`
  Reuse existing lease primitives for entity-level and job-level coordination.
- Modify: `D:\sofascore\schema_inspector\queue\__init__.py`
  Export the new queue/runtime APIs.

**Runtime Layer**
- Create: `D:\sofascore\schema_inspector\services\worker_runtime.py`
  Shared long-running loop with signal handling, drain mode, lease handling, retry classification, ack/requeue flow.
- Create: `D:\sofascore\schema_inspector\services\planner_daemon.py`
  Continuous planner loop that publishes jobs from schedules, live lanes, and delayed jobs.
- Create: `D:\sofascore\schema_inspector\services\service_app.py`
  Shared bootstrap for DB, Redis, transport, planner, orchestrator, stream queue, and shutdown wiring.
- Create: `D:\sofascore\schema_inspector\services\retry_policy.py`
  Central classification of retryable DB lock errors and backoff delays.

**Workers**
- Modify: `D:\sofascore\schema_inspector\workers\discovery_worker.py`
  Turn wrapper into real job handler.
- Modify: `D:\sofascore\schema_inspector\workers\hydrate_worker.py`
  Execute the orchestrator for queued hydrate jobs.
- Modify: `D:\sofascore\schema_inspector\workers\maintenance_worker.py`
  Add reclaim/recovery handling and DLQ/replay jobs.
- Create: `D:\sofascore\schema_inspector\workers\live_worker_service.py`
  Specialized live hot/warm continuous worker loop.

**CLI / Entry Points**
- Modify: `D:\sofascore\schema_inspector\cli.py`
  Add service commands: `planner-daemon`, `worker-discovery`, `worker-hydrate`, `worker-live-hot`, `worker-live-warm`, `worker-maintenance`.

**Tests**
- Modify: `D:\sofascore\tests\test_queue_streams.py`
- Create: `D:\sofascore\tests\test_worker_runtime.py`
- Create: `D:\sofascore\tests\test_worker_shutdown.py`
- Create: `D:\sofascore\tests\test_planner_daemon.py`
- Create: `D:\sofascore\tests\test_hydrate_worker_service.py`
- Create: `D:\sofascore\tests\test_live_worker_service.py`
- Create: `D:\sofascore\tests\test_retry_policy.py`
- Modify: `D:\sofascore\tests\test_hybrid_cli.py`

---

### Task 1: Extend Redis Streams for Consumer Groups and Reclaim

**Files:**
- Modify: `D:\sofascore\schema_inspector\queue\streams.py`
- Test: `D:\sofascore\tests\test_queue_streams.py`

- [ ] **Step 1: Write the failing tests for consumer-group lifecycle**

```python
def test_stream_queue_can_create_group_idempotently():
    backend = FakeStreamsBackend()
    queue = RedisStreamQueue(backend)

    queue.ensure_group(STREAM_HYDRATE, "hydrate")
    queue.ensure_group(STREAM_HYDRATE, "hydrate")

    assert backend.created_groups == [(STREAM_HYDRATE, "hydrate", "0-0")]


def test_stream_queue_can_claim_stale_messages():
    backend = FakeStreamsBackend()
    queue = RedisStreamQueue(backend)

    entries = queue.claim_stale(
        STREAM_HYDRATE,
        "hydrate",
        "worker-2",
        min_idle_ms=30_000,
        count=10,
    )

    assert entries[0].message_id == "171333-0"
```

- [ ] **Step 2: Run the queue-stream tests to verify they fail**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_queue_streams.py -q
```

Expected:
```text
FAIL ... AttributeError: 'RedisStreamQueue' object has no attribute 'ensure_group'
```

- [ ] **Step 3: Implement group creation, pending inspection, and reclaim**

```python
class RedisStreamQueue:
    def ensure_group(self, stream: str, group: str, *, start_id: str = "0-0") -> None:
        try:
            self.backend.xgroup_create(stream, group, id=start_id, mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def claim_stale(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        min_idle_ms: int,
        count: int = 10,
    ) -> tuple[StreamEntry, ...]:
        rows = self.backend.xautoclaim(stream, group, consumer, min_idle_ms, "0-0", count=count)
        claimed = rows[1] if len(rows) > 1 else ()
        return tuple(
            StreamEntry(stream=str(stream), message_id=str(message_id), values={str(k): _stringify(v) for k, v in values.items()})
            for message_id, values in claimed
        )
```

- [ ] **Step 4: Re-run the queue-stream tests**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_queue_streams.py -q
```

Expected:
```text
PASS
```

- [ ] **Step 5: Commit**

```bash
git add schema_inspector/queue/streams.py tests/test_queue_streams.py
git commit -m "feat: add redis stream reclaim primitives"
```

### Task 2: Build the Shared Worker Runtime With Graceful Shutdown

**Files:**
- Create: `D:\sofascore\schema_inspector\services\worker_runtime.py`
- Create: `D:\sofascore\schema_inspector\services\retry_policy.py`
- Test: `D:\sofascore\tests\test_worker_runtime.py`
- Test: `D:\sofascore\tests\test_worker_shutdown.py`

- [ ] **Step 1: Write the failing tests for shutdown and retry classification**

```python
async def test_worker_runtime_stops_polling_on_shutdown_and_drains_inflight():
    runtime = WorkerRuntime(...)
    await runtime.request_shutdown()
    assert runtime.shutdown_requested is True
    assert runtime.accepting_new_work is False


def test_retry_policy_marks_lock_errors_as_retryable():
    exc = FakePostgresError("canceling statement due to lock timeout")
    assert is_retryable_db_error(exc) is True
```

- [ ] **Step 2: Run the worker-runtime tests to verify they fail**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_worker_runtime.py tests\test_worker_shutdown.py -q
```

Expected:
```text
FAIL ... ModuleNotFoundError: No module named 'schema_inspector.services.worker_runtime'
```

- [ ] **Step 3: Implement the shared runtime skeleton**

```python
class WorkerRuntime:
    def __init__(self, *, name: str, queue, stream: str, group: str, consumer: str, handler, block_ms: int = 5_000) -> None:
        self.name = name
        self.queue = queue
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.handler = handler
        self.block_ms = block_ms
        self.shutdown_requested = False
        self.accepting_new_work = True
        self._inflight: set[asyncio.Task] = set()

    async def request_shutdown(self) -> None:
        self.shutdown_requested = True
        self.accepting_new_work = False

    async def run_forever(self) -> None:
        while not self.shutdown_requested:
            entries = self.queue.read_group(self.stream, self.group, self.consumer, count=1, block_ms=self.block_ms)
            for entry in entries:
                if self.shutdown_requested:
                    return
                task = asyncio.create_task(self._handle_entry(entry))
                self._inflight.add(task)
                task.add_done_callback(self._inflight.discard)

    async def drain(self, *, timeout_s: float) -> None:
        if self._inflight:
            await asyncio.wait(self._inflight, timeout=timeout_s)
```

- [ ] **Step 4: Implement retryable DB error classification**

```python
def is_retryable_db_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "lock timeout",
            "could not obtain lock",
            "deadlock detected",
            "locknotavailableerror",
        )
    )
```

- [ ] **Step 5: Re-run the runtime tests**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_worker_runtime.py tests\test_worker_shutdown.py -q
```

Expected:
```text
PASS
```

- [ ] **Step 6: Commit**

```bash
git add schema_inspector/services/worker_runtime.py schema_inspector/services/retry_policy.py tests/test_worker_runtime.py tests/test_worker_shutdown.py
git commit -m "feat: add shared runtime for continuous workers"
```

### Task 3: Create the Continuous Planner Daemon

**Files:**
- Create: `D:\sofascore\schema_inspector\services\planner_daemon.py`
- Test: `D:\sofascore\tests\test_planner_daemon.py`

- [ ] **Step 1: Write the failing tests for planner scheduling**

```python
async def test_planner_daemon_publishes_due_delayed_jobs():
    daemon = PlannerDaemon(...)
    await daemon.tick(now_ms=1_800_000_000_000)
    assert daemon.queue_published == ["stream:etl:hydrate"]


async def test_planner_daemon_reads_live_lanes_and_emits_refresh_jobs():
    daemon = PlannerDaemon(...)
    await daemon.tick(now_ms=1_800_000_000_000)
    assert daemon.published_job_types == ["refresh_live_event"]
```

- [ ] **Step 2: Run the planner-daemon tests to verify they fail**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_planner_daemon.py -q
```

Expected:
```text
FAIL ... ModuleNotFoundError: No module named 'schema_inspector.services.planner_daemon'
```

- [ ] **Step 3: Implement the planner daemon tick loop**

```python
class PlannerDaemon:
    def __init__(self, *, queue, delayed_scheduler, live_state_store, planner, now_ms_factory) -> None:
        self.queue = queue
        self.delayed_scheduler = delayed_scheduler
        self.live_state_store = live_state_store
        self.planner = planner
        self.now_ms_factory = now_ms_factory

    async def tick(self, *, now_ms: int | None = None) -> None:
        observed_now = int(now_ms if now_ms is not None else self.now_ms_factory())
        for delayed_job in self.delayed_scheduler.pop_due(now_epoch_ms=observed_now):
            self.queue.publish(STREAM_HYDRATE, {"job_id": delayed_job.job_id})
        for event_id in self.live_state_store.due_events(lane="hot", now_ms=observed_now):
            envelope = JobEnvelope.create(
                job_type=JOB_REFRESH_LIVE_EVENT,
                sport_slug=None,
                entity_type="event",
                entity_id=event_id,
                scope="live_hot",
                params={},
                priority=100,
                trace_id=None,
            )
            self.queue.publish(STREAM_LIVE_HOT, envelope.__dict__)
```

- [ ] **Step 4: Re-run the planner-daemon tests**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_planner_daemon.py -q
```

Expected:
```text
PASS
```

- [ ] **Step 5: Commit**

```bash
git add schema_inspector/services/planner_daemon.py tests/test_planner_daemon.py
git commit -m "feat: add continuous planner daemon"
```

### Task 4: Turn Hydrate and Maintenance Wrappers Into Real Service Workers

**Files:**
- Modify: `D:\sofascore\schema_inspector\workers\hydrate_worker.py`
- Modify: `D:\sofascore\schema_inspector\workers\maintenance_worker.py`
- Create: `D:\sofascore\schema_inspector\workers\live_worker_service.py`
- Test: `D:\sofascore\tests\test_hydrate_worker_service.py`
- Test: `D:\sofascore\tests\test_live_worker_service.py`

- [ ] **Step 1: Write the failing tests for hydrate and live service behavior**

```python
async def test_hydrate_worker_requeues_retryable_db_lock_errors():
    worker = HydrateWorker(...)
    result = await worker.handle(job)
    assert result == "requeued"


async def test_live_worker_acks_after_successful_refresh():
    worker = LiveWorkerService(...)
    result = await worker.handle(entry)
    assert result == "acked"
```

- [ ] **Step 2: Run the worker-service tests to verify they fail**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_hydrate_worker_service.py tests\test_live_worker_service.py -q
```

Expected:
```text
FAIL ... current worker wrappers do not expose async service behavior
```

- [ ] **Step 3: Implement `HydrateWorker.handle` around the orchestrator**

```python
class HydrateWorker:
    def __init__(self, *, orchestrator, delayed_scheduler, retry_delay_ms: int = 15_000) -> None:
        self.orchestrator = orchestrator
        self.delayed_scheduler = delayed_scheduler
        self.retry_delay_ms = retry_delay_ms

    async def handle(self, job: JobEnvelope) -> str:
        try:
            await self.orchestrator.run_event(
                event_id=int(job.entity_id),
                sport_slug=str(job.sport_slug or "football"),
                hydration_mode=str(job.params.get("hydration_mode", "full")),
            )
            return "acked"
        except Exception as exc:
            if is_retryable_db_error(exc):
                self.delayed_scheduler.schedule(job.job_id, run_at_epoch_ms=_now_ms() + self.retry_delay_ms)
                return "requeued"
            raise
```

- [ ] **Step 4: Implement specialized live service handler**

```python
class LiveWorkerService:
    def __init__(self, *, orchestrator, lane: str) -> None:
        self.orchestrator = orchestrator
        self.lane = lane

    async def handle(self, job: JobEnvelope) -> str:
        await self.orchestrator.run_event(
            event_id=int(job.entity_id),
            sport_slug=str(job.sport_slug or "football"),
            hydration_mode="full",
        )
        return "acked"
```

- [ ] **Step 5: Re-run the worker-service tests**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_hydrate_worker_service.py tests\test_live_worker_service.py -q
```

Expected:
```text
PASS
```

- [ ] **Step 6: Commit**

```bash
git add schema_inspector/workers/hydrate_worker.py schema_inspector/workers/maintenance_worker.py schema_inspector/workers/live_worker_service.py tests/test_hydrate_worker_service.py tests/test_live_worker_service.py
git commit -m "feat: add continuous hydrate and live worker services"
```

### Task 5: Add Service Commands to the Unified CLI

**Files:**
- Modify: `D:\sofascore\schema_inspector\cli.py`
- Create: `D:\sofascore\schema_inspector\services\service_app.py`
- Test: `D:\sofascore\tests\test_hybrid_cli.py`

- [ ] **Step 1: Write the failing CLI tests for service entrypoints**

```python
def test_parser_accepts_planner_and_worker_service_commands():
    parser = _build_parser()
    assert parser.parse_args(["planner-daemon"]).command == "planner-daemon"
    assert parser.parse_args(["worker-hydrate"]).command == "worker-hydrate"
```

- [ ] **Step 2: Run the CLI tests to verify they fail**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_hybrid_cli.py -k service -q
```

Expected:
```text
FAIL ... invalid choice: 'planner-daemon'
```

- [ ] **Step 3: Add service bootstrapping and CLI commands**

```python
planner_daemon = subparsers.add_parser("planner-daemon", help="Run the continuous planner loop.")
worker_hydrate = subparsers.add_parser("worker-hydrate", help="Run the hydrate consumer group loop.")
worker_live_hot = subparsers.add_parser("worker-live-hot", help="Run the live-hot consumer loop.")
worker_live_warm = subparsers.add_parser("worker-live-warm", help="Run the live-warm consumer loop.")
worker_maintenance = subparsers.add_parser("worker-maintenance", help="Run the maintenance/recovery consumer loop.")
```

```python
if args.command == "planner-daemon":
    await service_app.run_planner_daemon()
    return 0
```

- [ ] **Step 4: Re-run the CLI tests**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_hybrid_cli.py -k service -q
```

Expected:
```text
PASS
```

- [ ] **Step 5: Commit**

```bash
git add schema_inspector/cli.py schema_inspector/services/service_app.py tests/test_hybrid_cli.py
git commit -m "feat: add service entrypoints for continuous workers"
```

### Task 6: Add Recovery and Soak-Test Readiness

**Files:**
- Modify: `D:\sofascore\schema_inspector\workers\maintenance_worker.py`
- Modify: `D:\sofascore\schema_inspector\ops\recovery.py`
- Create: `D:\sofascore\tests\test_worker_reclaim.py`
- Create: `D:\sofascore\tests\test_service_recovery.py`

- [ ] **Step 1: Write the failing tests for reclaim and duplicate prevention**

```python
async def test_maintenance_worker_reclaims_stale_pending_messages():
    worker = MaintenanceWorker(...)
    reclaimed = await worker.reclaim_once()
    assert reclaimed == 2


async def test_restarted_worker_does_not_duplicate_completed_job():
    runtime = WorkerRuntime(...)
    outcome = await runtime.handle_reclaimed_message(entry)
    assert outcome == "skipped_completed"
```

- [ ] **Step 2: Run the recovery tests to verify they fail**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_worker_reclaim.py tests\test_service_recovery.py -q
```

Expected:
```text
FAIL ... reclaim flow not implemented
```

- [ ] **Step 3: Implement reclaim-on-idle flow**

```python
class MaintenanceWorker:
    async def reclaim_once(self) -> int:
        entries = self.queue.claim_stale(
            STREAM_HYDRATE,
            "hydrate",
            self.consumer_name,
            min_idle_ms=self.min_idle_ms,
            count=self.batch_size,
        )
        return len(entries)
```

- [ ] **Step 4: Re-run the recovery tests**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest tests\test_worker_reclaim.py tests\test_service_recovery.py -q
```

Expected:
```text
PASS
```

- [ ] **Step 5: Run the full suite**

Run:
```powershell
.\.venv311\Scripts\python.exe -m pytest -q
```

Expected:
```text
PASS
```

- [ ] **Step 6: Commit**

```bash
git add schema_inspector/workers/maintenance_worker.py schema_inspector/ops/recovery.py tests/test_worker_reclaim.py tests/test_service_recovery.py
git commit -m "feat: add reclaim and recovery flow for continuous workers"
```
