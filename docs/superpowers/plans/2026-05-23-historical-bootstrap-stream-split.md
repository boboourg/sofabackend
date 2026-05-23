# Historical Bootstrap Stream Split (Phase 4.7.7) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Isolate bootstrap publishes (catalog `pending` → `events_loaded`) onto a dedicated Redis stream + consumer group + worker pool so they stop losing the FIFO race against heavy cursor walks in the shared `stream:etl:historical_tournament`.

**Architecture:** New stream `stream:etl:historical_bootstrap` consumed by a new `cg:historical_bootstrap` group via a thin worker factory that reuses the existing `HistoricalTournamentWorker.handle` (which already inspects catalog state and dispatches in `bootstrap_mode=True` for pending rows). The planner gains a `bootstrap_stream` kwarg; `_publish_bootstrap_batch` writes to that stream instead of the shared one. Plus structured logging at planner publish ticks and worker claim/done/fail.

**Tech Stack:** Python 3.11, asyncpg, redis-py (Redis Streams), `unittest.IsolatedAsyncioTestCase`, systemd unit templates.

**Reference spec:** [docs/superpowers/specs/2026-05-23-historical-bootstrap-stream-split.md](../specs/2026-05-23-historical-bootstrap-stream-split.md)

---

## File Structure

| Action | File | Responsibility |
|---|---|---|
| Modify | `schema_inspector/queue/streams.py` | Add `STREAM_HISTORICAL_BOOTSTRAP` / `GROUP_HISTORICAL_BOOTSTRAP` constants; add pair to `HISTORICAL_CONSUMER_GROUPS` tuple |
| Modify | `schema_inspector/queue/__init__.py` | Re-export new constants alongside existing stream constants |
| Modify | `tests/test_queue_streams.py` | Assert constants exist and are included in `HISTORICAL_CONSUMER_GROUPS` |
| Modify | `schema_inspector/services/historical_tournament_planner.py` | Accept `bootstrap_stream` kwarg (default `STREAM_HISTORICAL_TOURNAMENT` — back-compat for tests); route `_publish_bootstrap_batch` to `self.bootstrap_stream`; add structured `bootstrap_tick` and `bootstrap_publish` log lines |
| Modify | `tests/test_historical_tournament_planner.py` | Add tests: default routes bootstrap to legacy stream; explicit kwarg routes to new stream; `bootstrap_tick` INFO log emitted per tick; `bootstrap_publish` DEBUG log emitted per publish |
| Create | `schema_inspector/workers/historical_bootstrap_worker.py` | Factory `make_historical_bootstrap_worker(orchestrator, queue, consumer, ...)` returning a `HistoricalTournamentWorker` wired to bootstrap stream/group; wrap `handle` with structured logging (`bootstrap_claim`, `bootstrap_job_done`, `bootstrap_job_failed`). Name `make_*` (not `build_*`) to avoid shadowing the `ServiceApp.build_historical_bootstrap_worker` method that calls it. |
| Create | `tests/test_historical_bootstrap_worker.py` | Assert factory wires correct stream/group; assert log lines emit on success/failure paths |
| Modify | `schema_inspector/cli.py` | Register `worker-historical-bootstrap` subparser (mirror `worker-historical-tournament` style: `worker_historical_bootstrap = subparsers.add_parser(...)`); add to `_HISTORICAL_COMMANDS` frozenset; add dispatch branch `if args.command == "worker-historical-bootstrap"` that calls `service_app.run_historical_bootstrap_worker(...)` |
| Modify | `schema_inspector/services/service_app.py` | Add `build_historical_bootstrap_worker(consumer_name, block_ms)` factory method + `run_historical_bootstrap_worker(consumer_name, block_ms)` runner (mirroring `build_historical_tournament_worker` + `run_historical_tournament_worker` at lines 1373 and 1680); modify `build_historical_tournament_planner_daemon` (line 653) to pass `bootstrap_stream=STREAM_HISTORICAL_BOOTSTRAP` into the `HistoricalTournamentPlannerDaemon` constructor |
| Modify | `tests/test_service_app.py` (or add new `tests/test_historical_bootstrap_wiring.py` if `test_service_app.py` is too crowded) | Assert factory present and produces consumer-named instance; assert planner factory passes `bootstrap_stream` through |
| Create | `ops/systemd/sofascore-historical-bootstrap@.service` | Unit template; copy of `sofascore-historical-tournament@.service` with new Description and `worker-historical-bootstrap` subcommand |

---

## Task 1: Stream Constants

**Files:**
- Modify: `schema_inspector/queue/streams.py`
- Modify: `schema_inspector/queue/__init__.py`
- Modify: `tests/test_queue_streams.py`

- [ ] **Step 1: Write the failing test**

Append at the bottom of `tests/test_queue_streams.py` (after the last class, before any `if __name__ == "__main__"` line if present):

```python
class HistoricalBootstrapConstantsTests(unittest.TestCase):
    def test_bootstrap_stream_constant_exists(self) -> None:
        from schema_inspector.queue import STREAM_HISTORICAL_BOOTSTRAP

        self.assertEqual(STREAM_HISTORICAL_BOOTSTRAP, "stream:etl:historical_bootstrap")

    def test_bootstrap_group_constant_exists(self) -> None:
        from schema_inspector.queue.streams import GROUP_HISTORICAL_BOOTSTRAP

        self.assertEqual(GROUP_HISTORICAL_BOOTSTRAP, "cg:historical_bootstrap")

    def test_bootstrap_pair_in_historical_consumer_groups(self) -> None:
        from schema_inspector.queue.streams import (
            GROUP_HISTORICAL_BOOTSTRAP,
            HISTORICAL_CONSUMER_GROUPS,
            STREAM_HISTORICAL_BOOTSTRAP,
        )

        self.assertIn(
            (STREAM_HISTORICAL_BOOTSTRAP, GROUP_HISTORICAL_BOOTSTRAP),
            HISTORICAL_CONSUMER_GROUPS,
        )
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_queue_streams.py::HistoricalBootstrapConstantsTests -v`
Expected: 3× FAIL with `ImportError: cannot import name 'STREAM_HISTORICAL_BOOTSTRAP'` (or similar).

- [ ] **Step 3: Add constants to `schema_inspector/queue/streams.py`**

In `schema_inspector/queue/streams.py`, locate the block around line 15 with `STREAM_HISTORICAL_MAINTENANCE = "stream:etl:historical_maintenance"`. Add immediately after:

```python
STREAM_HISTORICAL_BOOTSTRAP = "stream:etl:historical_bootstrap"
```

Then locate the block around line 40 with `GROUP_HISTORICAL_MAINTENANCE = "cg:historical_maintenance"`. Add immediately after:

```python
GROUP_HISTORICAL_BOOTSTRAP = "cg:historical_bootstrap"
```

Then locate the `HISTORICAL_CONSUMER_GROUPS = (...)` tuple (around lines 65-71). Add the bootstrap pair as a new entry (place between historical_tournament and historical_enrichment so bootstrap is consumer-ready right after the tournament lane):

```python
HISTORICAL_CONSUMER_GROUPS = (
    (STREAM_HISTORICAL_DISCOVERY, GROUP_HISTORICAL_DISCOVERY),
    (STREAM_HISTORICAL_TOURNAMENT, GROUP_HISTORICAL_TOURNAMENT),
    (STREAM_HISTORICAL_BOOTSTRAP, GROUP_HISTORICAL_BOOTSTRAP),
    (STREAM_HISTORICAL_ENRICHMENT, GROUP_HISTORICAL_ENRICHMENT),
    (STREAM_HISTORICAL_HYDRATE, GROUP_HISTORICAL_HYDRATE),
    (STREAM_HISTORICAL_MAINTENANCE, GROUP_HISTORICAL_MAINTENANCE),
)
```

- [ ] **Step 4: Re-export from `schema_inspector/queue/__init__.py`**

Open `schema_inspector/queue/__init__.py`. Find the existing import block from `.streams`. Add `STREAM_HISTORICAL_BOOTSTRAP` to the import list (alphabetical-ish, place near `STREAM_HISTORICAL_DISCOVERY`). Then add `"STREAM_HISTORICAL_BOOTSTRAP"` to the `__all__` tuple if present (also alphabetical-ish).

Example diff:

```python
from .streams import (
    ...
    STREAM_HISTORICAL_BOOTSTRAP,
    STREAM_HISTORICAL_DISCOVERY,
    ...
)

__all__ = (
    ...
    "STREAM_HISTORICAL_BOOTSTRAP",
    "STREAM_HISTORICAL_DISCOVERY",
    ...
)
```

(If `__all__` does not exist, skip that part — the bare `from .streams import` is sufficient.)

- [ ] **Step 5: Run the test to verify it passes**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_queue_streams.py::HistoricalBootstrapConstantsTests -v`
Expected: 3× PASS.

- [ ] **Step 6: Run the full queue_streams test to verify no regression**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_queue_streams.py -q`
Expected: all pass, no failures introduced.

- [ ] **Step 7: Commit**

```bash
git add schema_inspector/queue/streams.py schema_inspector/queue/__init__.py tests/test_queue_streams.py
git commit -m "Phase 4.7.7 step 1: bootstrap stream/group constants

Add STREAM_HISTORICAL_BOOTSTRAP and GROUP_HISTORICAL_BOOTSTRAP
constants; include in HISTORICAL_CONSUMER_GROUPS so recover-live-state
and ops surfaces auto-create the group. Re-export from queue package.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 2: Planner Accepts `bootstrap_stream` Kwarg + Structured Logging

**Files:**
- Modify: `schema_inspector/services/historical_tournament_planner.py`
- Modify: `tests/test_historical_tournament_planner.py`

**Context for the engineer:** `HistoricalTournamentPlannerDaemon.__init__` already accepts `stream` (the cursor-walk stream). We add a sibling `bootstrap_stream` kwarg that defaults to `STREAM_HISTORICAL_TOURNAMENT` so any existing test that constructs the daemon without the new kwarg continues to work (jobs flow into the legacy stream in that case). `_publish_bootstrap_batch` then publishes to `self.bootstrap_stream`. We also add two structured log lines that today do not exist anywhere (the planner only logs `Cursor publishes paused by backpressure: ...`) so operators can observe bootstrap throughput directly.

- [ ] **Step 1: Write the failing test for `bootstrap_stream` routing**

Append in `tests/test_historical_tournament_planner.py` (use existing `_FakeQueue` / `_FakeRedisBackend` fixtures already in that file):

```python
    async def test_planner_routes_bootstrap_publishes_to_bootstrap_stream(self) -> None:
        from schema_inspector.queue.streams import (
            STREAM_HISTORICAL_BOOTSTRAP,
            STREAM_HISTORICAL_TOURNAMENT,
        )
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()

        async def selector(*, sport_slug, after_unique_tournament_id, limit):
            return ()

        async def bootstrap_selector(*, sport_slug, limit):
            return [
                {"unique_tournament_id": 17, "season_id": 701},
                {"unique_tournament_id": 19, "season_id": 702},
            ]

        cursor_store = HistoricalTournamentCursorStore(backend)
        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            selector=selector,
            targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
            tournaments_per_tick=2,
            bootstrap_pending_selector=bootstrap_selector,
            max_bootstrap_jobs_per_tick=5,
            bootstrap_stream=STREAM_HISTORICAL_BOOTSTRAP,
        )

        published = await daemon.tick()

        self.assertEqual(published, 2)
        self.assertEqual(
            [stream for stream, _ in queue.published],
            [STREAM_HISTORICAL_BOOTSTRAP, STREAM_HISTORICAL_BOOTSTRAP],
        )
        # Legacy stream untouched — no cursor walks selected by the empty
        # legacy selector and bootstrap_stream is distinct from stream.
        for stream, _ in queue.published:
            self.assertNotEqual(stream, STREAM_HISTORICAL_TOURNAMENT)

    async def test_planner_bootstrap_stream_defaults_to_legacy_stream(self) -> None:
        """Backwards-compat: if bootstrap_stream not provided, bootstrap
        publishes still go to STREAM_HISTORICAL_TOURNAMENT so pre-Phase-4.7.7
        tests and operators see the same behaviour."""
        from schema_inspector.queue.streams import STREAM_HISTORICAL_TOURNAMENT
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()

        async def selector(*, sport_slug, after_unique_tournament_id, limit):
            return ()

        async def bootstrap_selector(*, sport_slug, limit):
            return [{"unique_tournament_id": 17, "season_id": 701}]

        cursor_store = HistoricalTournamentCursorStore(backend)
        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            selector=selector,
            targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
            tournaments_per_tick=2,
            bootstrap_pending_selector=bootstrap_selector,
            max_bootstrap_jobs_per_tick=5,
        )

        await daemon.tick()

        self.assertEqual(
            [stream for stream, _ in queue.published],
            [STREAM_HISTORICAL_TOURNAMENT],
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_historical_tournament_planner.py::HistoricalTournamentPlannerTests::test_planner_routes_bootstrap_publishes_to_bootstrap_stream tests/test_historical_tournament_planner.py::HistoricalTournamentPlannerTests::test_planner_bootstrap_stream_defaults_to_legacy_stream -v`
Expected: 2× FAIL with `TypeError: ... got an unexpected keyword argument 'bootstrap_stream'`.

- [ ] **Step 3: Add `bootstrap_stream` kwarg to `__init__`**

In `schema_inspector/services/historical_tournament_planner.py`, modify `HistoricalTournamentPlannerDaemon.__init__` signature (around lines 56-82) — add after `stream: str = STREAM_HISTORICAL_TOURNAMENT,`:

```python
        bootstrap_stream: str = STREAM_HISTORICAL_TOURNAMENT,
```

In the body (after `self.stream = stream` around line 88) add:

```python
        self.bootstrap_stream = bootstrap_stream
```

- [ ] **Step 4: Route `_publish_bootstrap_batch` to `self.bootstrap_stream`**

In the same file, locate `_publish_bootstrap_batch` (around lines 322-362). Find the line that publishes:

```python
            self.queue.publish(self.stream, encode_stream_job(job))
```

Change `self.stream` to `self.bootstrap_stream`:

```python
            self.queue.publish(self.bootstrap_stream, encode_stream_job(job))
```

- [ ] **Step 5: Run the routing tests to verify they pass**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_historical_tournament_planner.py::HistoricalTournamentPlannerTests::test_planner_routes_bootstrap_publishes_to_bootstrap_stream tests/test_historical_tournament_planner.py::HistoricalTournamentPlannerTests::test_planner_bootstrap_stream_defaults_to_legacy_stream -v`
Expected: 2× PASS.

- [ ] **Step 6: Write the failing test for structured logging**

Append in `tests/test_historical_tournament_planner.py`:

```python
    async def test_planner_logs_bootstrap_tick_per_target(self) -> None:
        """Per-tick INFO log: bootstrap_tick: sport=X published=N selector_returned=M cursor_paused=bool"""
        import logging
        from schema_inspector.queue.streams import STREAM_HISTORICAL_BOOTSTRAP
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()

        async def selector(*, sport_slug, after_unique_tournament_id, limit):
            return ()

        async def bootstrap_selector(*, sport_slug, limit):
            return [
                {"unique_tournament_id": 17, "season_id": 701},
                {"unique_tournament_id": 19, "season_id": 702},
            ]

        cursor_store = HistoricalTournamentCursorStore(backend)
        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            selector=selector,
            targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
            tournaments_per_tick=2,
            bootstrap_pending_selector=bootstrap_selector,
            max_bootstrap_jobs_per_tick=5,
            bootstrap_stream=STREAM_HISTORICAL_BOOTSTRAP,
        )

        with self.assertLogs(
            "schema_inspector.services.historical_tournament_planner",
            level="INFO",
        ) as captured:
            await daemon.tick()

        tick_lines = [r for r in captured.records if "bootstrap_tick" in r.getMessage()]
        self.assertEqual(len(tick_lines), 1)
        msg = tick_lines[0].getMessage()
        self.assertIn("sport=football", msg)
        self.assertIn("published=2", msg)
        self.assertIn("selector_returned=2", msg)
```

- [ ] **Step 7: Run logging test to verify it fails**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_historical_tournament_planner.py::HistoricalTournamentPlannerTests::test_planner_logs_bootstrap_tick_per_target -v`
Expected: FAIL — no `bootstrap_tick` log produced yet, `tick_lines` is empty.

- [ ] **Step 8: Emit `bootstrap_tick` log in `tick`**

In `schema_inspector/services/historical_tournament_planner.py`, locate the `tick` method. Inside the `for target in targets:` loop (around line 218), track per-target bootstrap counts. Then at the **end of each target iteration** (just before `return published` falls out of the loop), emit the structured log.

Concretely: refactor the bootstrap publish to capture both the selector-row-count and published count per target. Easiest path — make `_publish_bootstrap_batch` return a 2-tuple `(published, selector_returned)`:

In `_publish_bootstrap_batch`, change the return type and value:

```python
    async def _publish_bootstrap_batch(self, target) -> tuple[int, int]:
        """... existing docstring ...

        Returns (published_count, selector_returned_count) so the caller
        can emit a structured per-tick log."""

        if self.bootstrap_pending_selector is None:
            return 0, 0
        limit = int(self.max_bootstrap_jobs_per_tick)
        if limit <= 0:
            return 0, 0
        rows = await _await_maybe(
            self.bootstrap_pending_selector(
                sport_slug=target.sport_slug, limit=limit
            )
        )
        if not rows:
            return 0, 0
        published = 0
        for row in rows[:limit]:
            ut_id = int(row["unique_tournament_id"])
            season_id = int(row["season_id"])
            job = JobEnvelope.create(
                job_type=JOB_SYNC_TOURNAMENT_ARCHIVE,
                sport_slug=target.sport_slug,
                entity_type="unique_tournament",
                entity_id=ut_id,
                scope="historical",
                params={"target_season_id": season_id},
                priority=target.priority,
                trace_id=None,
            )
            self.queue.publish(self.bootstrap_stream, encode_stream_job(job))
            logger.debug(
                "bootstrap_publish: sport=%s ut=%s season=%s stream=%s",
                target.sport_slug, ut_id, season_id, self.bootstrap_stream,
            )
            published += 1
        return published, len(rows)
```

Now update **every call site** in `tick` that does `published += await self._publish_bootstrap_batch(target)`. There are three call sites in the existing code (one when cursor is paused, one after cursor_rows publish, one in the legacy path). Each one becomes:

```python
            bootstrap_published, bootstrap_selector_returned = await self._publish_bootstrap_batch(target)
            published += bootstrap_published
            logger.info(
                "bootstrap_tick: sport=%s published=%d selector_returned=%d cursor_paused=%s",
                target.sport_slug,
                bootstrap_published,
                bootstrap_selector_returned,
                cursor_paused_reason is not None,
            )
            continue  # only present in the cursor-paused branch — keep the existing control flow
```

For the two call sites where there is no `continue` after (they fall through to next target iteration via the for loop), drop the `continue` line. The `logger.info(...)` line goes immediately after `published += bootstrap_published` in all three sites.

- [ ] **Step 9: Run the logging test to verify it passes**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_historical_tournament_planner.py::HistoricalTournamentPlannerTests::test_planner_logs_bootstrap_tick_per_target -v`
Expected: PASS.

- [ ] **Step 10: Run the entire planner test file for regression**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_historical_tournament_planner.py tests/test_historical_tournament_planner_priorities.py tests/test_historical_tournament_planner_reload.py tests/test_phase_3_planner_bootstrap.py -q`
Expected: all pass. If a pre-existing test fails because it asserted on a publish stream that is now the legacy default but the test happens to also have a `bootstrap_pending_selector`, the test was already exercising bootstrap publishes in the shared stream — they should still land in `STREAM_HISTORICAL_TOURNAMENT` because the kwarg default keeps back-compat. Investigate any failure rather than patching tests.

- [ ] **Step 11: Commit**

```bash
git add schema_inspector/services/historical_tournament_planner.py tests/test_historical_tournament_planner.py
git commit -m "Phase 4.7.7 step 2: planner routes bootstrap to dedicated stream + logging

Add HistoricalTournamentPlannerDaemon.bootstrap_stream kwarg
(default STREAM_HISTORICAL_TOURNAMENT for back-compat); route
_publish_bootstrap_batch via self.bootstrap_stream. Add structured
INFO log bootstrap_tick (per target per tick) and DEBUG log
bootstrap_publish (per job).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 3: Bootstrap Worker Factory + Structured Logging

**Files:**
- Create: `schema_inspector/workers/historical_bootstrap_worker.py`
- Create: `tests/test_historical_bootstrap_worker.py`

**Context for the engineer:** We do not subclass `HistoricalTournamentWorker`. The existing handler already does the correct thing for bootstrap-mode jobs: it inspects `tournament_season_upstream_catalog.bootstrap_state` at handle time, sets `bootstrap_mode=True` when state is `pending`, and the bootstrap-mode branch in the orchestrator skips enrichment fan-out. We only need a thin **factory** that constructs a `HistoricalTournamentWorker` instance pointed at the bootstrap stream/group, plus a wrapping handler that emits structured logs before/after delegating to the real handler.

- [ ] **Step 1: Write the failing test for the factory**

Create new file `tests/test_historical_bootstrap_worker.py`:

```python
from __future__ import annotations

import unittest
from types import SimpleNamespace

from schema_inspector.queue.streams import (
    GROUP_HISTORICAL_BOOTSTRAP,
    STREAM_HISTORICAL_BOOTSTRAP,
    StreamEntry,
)


class HistoricalBootstrapWorkerFactoryTests(unittest.IsolatedAsyncioTestCase):
    def test_factory_wires_bootstrap_stream_and_group(self) -> None:
        from schema_inspector.workers.historical_bootstrap_worker import (
            make_historical_bootstrap_worker,
        )

        orchestrator = _FakeArchiveOrchestrator()
        queue = _FakeQueue()

        worker = make_historical_bootstrap_worker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="historical-bootstrap-1",
        )

        self.assertEqual(worker.stream, STREAM_HISTORICAL_BOOTSTRAP)
        self.assertEqual(worker.group, GROUP_HISTORICAL_BOOTSTRAP)
        self.assertEqual(worker.consumer, "historical-bootstrap-1")

    async def test_handle_emits_claim_and_done_logs(self) -> None:
        from schema_inspector.workers.historical_bootstrap_worker import (
            make_historical_bootstrap_worker,
        )

        orchestrator = _FakeArchiveOrchestrator()
        queue = _FakeQueue()
        worker = make_historical_bootstrap_worker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="historical-bootstrap-1",
        )

        entry = StreamEntry(
            stream=STREAM_HISTORICAL_BOOTSTRAP,
            message_id="1-1",
            values={
                "job_id": "job-bootstrap-1",
                "job_type": "sync_tournament_archive",
                "sport_slug": "football",
                "entity_type": "unique_tournament",
                "entity_id": "17",
                "scope": "historical",
                "params_json": '{"target_season_id": 701}',
                "attempt": "1",
                "idempotency_key": "key-1",
            },
        )

        with self.assertLogs(
            "schema_inspector.workers.historical_bootstrap_worker",
            level="INFO",
        ) as captured:
            result = await worker.handle(entry)

        self.assertEqual(result, "completed")
        messages = [r.getMessage() for r in captured.records]
        claim = [m for m in messages if "bootstrap_claim" in m]
        done = [m for m in messages if "bootstrap_job_done" in m]
        self.assertEqual(len(claim), 1, msg=f"claim log missing: {messages}")
        self.assertEqual(len(done), 1, msg=f"done log missing: {messages}")
        self.assertIn("ut=17", claim[0])
        self.assertIn("season=701", claim[0])
        self.assertIn("ut=17", done[0])
        self.assertIn("duration_ms=", done[0])

    async def test_handle_emits_failed_log_on_exception(self) -> None:
        from schema_inspector.workers.historical_bootstrap_worker import (
            make_historical_bootstrap_worker,
        )

        orchestrator = _FakeArchiveOrchestrator(raise_on_archive=RuntimeError("boom"))
        queue = _FakeQueue()
        worker = make_historical_bootstrap_worker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="historical-bootstrap-1",
        )

        entry = StreamEntry(
            stream=STREAM_HISTORICAL_BOOTSTRAP,
            message_id="1-2",
            values={
                "job_id": "job-bootstrap-2",
                "job_type": "sync_tournament_archive",
                "sport_slug": "football",
                "entity_type": "unique_tournament",
                "entity_id": "19",
                "scope": "historical",
                "params_json": '{"target_season_id": 702}',
                "attempt": "1",
                "idempotency_key": "key-2",
            },
        )

        with self.assertLogs(
            "schema_inspector.workers.historical_bootstrap_worker",
            level="WARNING",
        ) as captured:
            with self.assertRaises(RuntimeError):
                await worker.handle(entry)

        failed = [
            r.getMessage() for r in captured.records
            if "bootstrap_job_failed" in r.getMessage()
        ]
        self.assertEqual(len(failed), 1, msg=f"failed log missing: {[r.getMessage() for r in captured.records]}")
        self.assertIn("ut=19", failed[0])
        self.assertIn("season=702", failed[0])


class _FakeQueue:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    def publish(self, stream: str, values: dict) -> str:
        self.published.append((stream, dict(values)))
        return f"1-{len(self.published)}"


class _FakeArchiveOrchestrator:
    """Minimal orchestrator stub matching what HistoricalTournamentWorker.handle calls."""

    def __init__(self, *, raise_on_archive: Exception | None = None) -> None:
        self.archive_calls: list[tuple[int, str, int | None, bool]] = []
        self._raise = raise_on_archive
        self.database = None  # _fetch_catalog_state returns None on missing database → bootstrap_mode=False

    async def run_historical_tournament_archive(
        self,
        *,
        unique_tournament_id: int,
        sport_slug: str,
        target_season_id: int | None,
        bootstrap_mode: bool,
    ) -> dict:
        self.archive_calls.append(
            (unique_tournament_id, sport_slug, target_season_id, bootstrap_mode)
        )
        if self._raise is not None:
            raise self._raise
        return {
            "season_ids": (target_season_id,) if target_season_id else (),
            "capabilities_completed": ("events",),
            "discovered_event_ids": 5,
        }


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_historical_bootstrap_worker.py -v`
Expected: 3× FAIL with `ModuleNotFoundError: No module named 'schema_inspector.workers.historical_bootstrap_worker'`.

- [ ] **Step 3: Implement the worker factory**

Create new file `schema_inspector/workers/historical_bootstrap_worker.py`:

```python
"""Dedicated worker for the bootstrap historical-archive lane.

Reuses ``HistoricalTournamentWorker.handle`` (which already inspects
catalog state and dispatches in bootstrap_mode=True for pending rows),
but consumes from ``stream:etl:historical_bootstrap`` /
``cg:historical_bootstrap`` so bootstrap publishes do not lose the
FIFO race against heavy cursor walks in the shared lane.

Adds structured logging: ``bootstrap_claim``, ``bootstrap_job_done``,
``bootstrap_job_failed`` so operators can grep journalctl for
per-job throughput without parsing the existing event-detail noise.
"""

from __future__ import annotations

import logging
import time

from ..queue.streams import (
    GROUP_HISTORICAL_BOOTSTRAP,
    STREAM_HISTORICAL_BOOTSTRAP,
    StreamEntry,
)
from ..workers._stream_jobs import decode_stream_job
from ..workers.historical_archive_worker import HistoricalTournamentWorker

logger = logging.getLogger(__name__)


def make_historical_bootstrap_worker(
    *,
    orchestrator,
    queue,
    consumer: str,
    block_ms: int = 5_000,
    delayed_scheduler=None,
    delayed_payload_store=None,
    completion_store=None,
    now_ms_factory=None,
    job_audit_logger=None,
) -> HistoricalTournamentWorker:
    """Construct a HistoricalTournamentWorker pinned to the bootstrap
    stream + group, with a logging wrapper around its ``handle`` method.

    Named ``make_*`` (not ``build_*``) so it does not shadow the
    ``ServiceApp.build_historical_bootstrap_worker`` method that imports
    and calls this factory.
    """

    worker = HistoricalTournamentWorker(
        orchestrator=orchestrator,
        queue=queue,
        consumer=consumer,
        group=GROUP_HISTORICAL_BOOTSTRAP,
        stream=STREAM_HISTORICAL_BOOTSTRAP,
        block_ms=block_ms,
        delayed_scheduler=delayed_scheduler,
        delayed_payload_store=delayed_payload_store,
        completion_store=completion_store,
        now_ms_factory=now_ms_factory,
        job_audit_logger=job_audit_logger,
    )

    original_handle = worker.handle

    async def _handle_with_logging(entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        ut_id = job.entity_id
        season_id = None
        if job.params:
            raw = job.params.get("target_season_id")
            if raw not in (None, ""):
                try:
                    season_id = int(raw)
                except (TypeError, ValueError):
                    season_id = None

        logger.info(
            "bootstrap_claim: msg_id=%s consumer=%s ut=%s season=%s",
            entry.message_id, consumer, ut_id, season_id,
        )

        start_ms = int(time.time() * 1000)
        try:
            result = await original_handle(entry)
        except Exception as exc:
            duration_ms = int(time.time() * 1000) - start_ms
            logger.warning(
                "bootstrap_job_failed: ut=%s season=%s duration_ms=%d exc=%s",
                ut_id, season_id, duration_ms, exc,
                exc_info=True,
            )
            raise

        duration_ms = int(time.time() * 1000) - start_ms
        logger.info(
            "bootstrap_job_done: ut=%s season=%s duration_ms=%d result=%s",
            ut_id, season_id, duration_ms, result,
        )
        return result

    worker.handle = _handle_with_logging  # type: ignore[assignment]
    # WorkerRuntime captured the original ``self.handle`` reference at
    # construction time — rebind so the runtime loop calls our wrapper.
    worker.runtime.handler = _handle_with_logging
    return worker
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_historical_bootstrap_worker.py -v`
Expected: 3× PASS.

- [ ] **Step 5: Run adjacent worker tests for regression**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_historical_archive_worker_service.py tests/test_phase_3_bootstrap_dispatch.py -q`
Expected: all pass. If anything regressed it is likely a test that imports `HistoricalTournamentWorker` and the bootstrap worker module accidentally affected it — investigate import-side effects.

- [ ] **Step 6: Commit**

```bash
git add schema_inspector/workers/historical_bootstrap_worker.py tests/test_historical_bootstrap_worker.py
git commit -m "Phase 4.7.7 step 3: bootstrap worker factory + structured logging

Add build_historical_bootstrap_worker — thin factory that constructs
HistoricalTournamentWorker pinned to the dedicated bootstrap stream
and group. Wraps handle() with bootstrap_claim / bootstrap_job_done /
bootstrap_job_failed structured logs (INFO + WARNING) so operators
can grep journalctl for per-job throughput without subclassing.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 4: CLI Subcommand `worker-historical-bootstrap`

**Files:**
- Modify: `schema_inspector/cli.py`
- Create: `tests/test_historical_bootstrap_cli.py`

**Context for the engineer:** `schema_inspector/cli.py` uses a subparser-with-variable-assignment pattern and dispatches via an `if args.command == "..."` chain inside `main()`. The exact sibling to mirror is `worker-historical-tournament`:

- **Subparser registration** at line 2809:
  ```python
  worker_historical_tournament = subparsers.add_parser("worker-historical-tournament", help="Run the archival tournament/season consumer group loop.")
  worker_historical_tournament.add_argument("--consumer-name", default="worker-historical-tournament-1", help="Redis consumer name for the archival tournament worker.")
  worker_historical_tournament.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")
  ```
- **Dispatch branch** at line 2154 inside `main()`:
  ```python
  if args.command == "worker-historical-tournament":
      service_app = ServiceApp(app)
      await service_app.run_historical_tournament_worker(
          consumer_name=args.consumer_name,
          block_ms=args.block_ms,
      )
      return 0
  ```
- **`_HISTORICAL_COMMANDS` frozenset** at line 1428 — bootstrap worker also consumes the historical proxy pool, so it MUST be added to this set so the dispatcher routes through `load_historical_runtime_config` (otherwise it falls back to operational config and uses residential proxies — wrong).

There is no `_handle_*` function pattern in this file — the dispatch is inline.

- [ ] **Step 1: Write the failing test**

Create `tests/test_historical_bootstrap_cli.py`:

```python
from __future__ import annotations

import argparse
import unittest


def _find_parser_factory():
    """Find the top-level argparse factory used by ``main()``.

    ``schema_inspector.cli`` may name it ``build_parser``, ``_build_parser``,
    or construct it inline. Returns a zero-arg callable that produces the
    parser, or raises if it cannot be found.
    """
    from schema_inspector import cli

    for name in ("build_parser", "_build_parser", "make_parser", "_make_parser"):
        fn = getattr(cli, name, None)
        if callable(fn):
            return fn
    raise RuntimeError(
        "schema_inspector.cli does not expose a build_parser factory. "
        "Refactor main() to extract the argparse.ArgumentParser construction "
        "into a top-level build_parser() function before adding the bootstrap subcommand."
    )


class WorkerHistoricalBootstrapCLITests(unittest.TestCase):
    def test_subcommand_registered_in_parser(self) -> None:
        parser = _find_parser_factory()()
        subparser_actions = [
            action for action in parser._actions
            if action.__class__.__name__ == "_SubParsersAction"
        ]
        self.assertTrue(subparser_actions, "no subparsers registered on cli parser")
        names: set[str] = set()
        for action in subparser_actions:
            names.update(action.choices.keys())
        self.assertIn("worker-historical-bootstrap", names)

    def test_subparser_has_consumer_name_and_block_ms_flags(self) -> None:
        parser = _find_parser_factory()()
        subparser_actions = [
            action for action in parser._actions
            if action.__class__.__name__ == "_SubParsersAction"
        ]
        sub: argparse.ArgumentParser | None = None
        for action in subparser_actions:
            sub = action.choices.get("worker-historical-bootstrap")
            if sub is not None:
                break
        assert sub is not None
        flags = {a.option_strings[0] for a in sub._actions if a.option_strings}
        self.assertIn("--consumer-name", flags)
        self.assertIn("--block-ms", flags)

    def test_command_added_to_historical_commands_set(self) -> None:
        from schema_inspector.cli import _HISTORICAL_COMMANDS

        self.assertIn("worker-historical-bootstrap", _HISTORICAL_COMMANDS)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_historical_bootstrap_cli.py -v`
Expected: 3× FAIL — `_find_parser_factory` may raise (no factory exported), and the `_HISTORICAL_COMMANDS` assertion will fail.

- [ ] **Step 3: Expose a `build_parser` factory if `main()` builds the parser inline**

In `schema_inspector/cli.py`, locate `def main(...):` (likely near the bottom or near `if __name__ == "__main__":`). If the body starts with `parser = argparse.ArgumentParser(...)` followed by subparser construction, extract a top-level function:

```python
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(...)  # ← whatever main() had
    subparsers = parser.add_subparsers(dest="command", required=True)
    # ... move ALL subparser construction here ...
    return parser
```

Then `main()` calls it:

```python
def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    # ... dispatch chain follows ...
```

Do not change the dispatch logic — only the parser construction. If the file already exposes `build_parser` or similar, skip this step.

- [ ] **Step 4: Add the bootstrap subparser**

In `schema_inspector/cli.py`, find the existing block at line 2809:

```python
    worker_historical_tournament = subparsers.add_parser("worker-historical-tournament", help="Run the archival tournament/season consumer group loop.")
    worker_historical_tournament.add_argument("--consumer-name", default="worker-historical-tournament-1", help="Redis consumer name for the archival tournament worker.")
    worker_historical_tournament.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")
```

Immediately after, add the bootstrap variant (same shape, mirror style):

```python
    worker_historical_bootstrap = subparsers.add_parser("worker-historical-bootstrap", help="Run the archival bootstrap consumer group loop (cg:historical_bootstrap on stream:etl:historical_bootstrap).")
    worker_historical_bootstrap.add_argument("--consumer-name", default="worker-historical-bootstrap-1", help="Redis consumer name for the archival bootstrap worker.")
    worker_historical_bootstrap.add_argument("--block-ms", type=int, default=5000, help="XREADGROUP block timeout in milliseconds.")
```

- [ ] **Step 5: Add the command to `_HISTORICAL_COMMANDS`**

In the same file, find `_HISTORICAL_COMMANDS = frozenset({...})` at line 1428. Add `"worker-historical-bootstrap"` between `"worker-historical-tournament"` and `"worker-historical-enrichment"` (alphabetical-ish, mirror the surrounding workers):

```python
_HISTORICAL_COMMANDS = frozenset({
    "worker-historical-hydrate",
    "worker-historical-discovery",
    "worker-historical-tournament",
    "worker-historical-bootstrap",
    "worker-historical-enrichment",
    "worker-historical-maintenance",
    "historical-planner-daemon",
    "historical-tournament-planner-daemon",
    "historical-backfill",
})
```

- [ ] **Step 6: Add the dispatch branch**

In the same file, find the dispatch branch for `worker-historical-tournament` at line 2154:

```python
            if args.command == "worker-historical-tournament":
                service_app = ServiceApp(app)
                await service_app.run_historical_tournament_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
```

Immediately after that block, add the parallel branch:

```python
            if args.command == "worker-historical-bootstrap":
                service_app = ServiceApp(app)
                await service_app.run_historical_bootstrap_worker(
                    consumer_name=args.consumer_name,
                    block_ms=args.block_ms,
                )
                return 0
```

(Note: `service_app.run_historical_bootstrap_worker` is added in Task 5. The dispatch branch references it, but the test in this task does not exercise the dispatch — only the subparser registration. The dispatch only runs at full CLI invocation time after Task 5 lands.)

- [ ] **Step 7: Run the CLI test to verify it passes**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_historical_bootstrap_cli.py -v`
Expected: 3× PASS.

- [ ] **Step 8: Smoke-test that `--help` works**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m schema_inspector.cli worker-historical-bootstrap --help`
Expected: usage line that includes `--consumer-name CONSUMER_NAME` and `--block-ms BLOCK_MS`. No traceback.

- [ ] **Step 9: Commit**

```bash
git add schema_inspector/cli.py tests/test_historical_bootstrap_cli.py
git commit -m "Phase 4.7.7 step 4: CLI subcommand worker-historical-bootstrap

Add worker-historical-bootstrap subparser mirroring worker-historical-tournament
(line 2809 sibling) and dispatch branch (line 2154 sibling) that calls
ServiceApp.run_historical_bootstrap_worker (added in step 5). Add the
command to _HISTORICAL_COMMANDS frozenset so the dispatcher routes it
through load_historical_runtime_config (non-residential proxy pool),
matching the rest of the historical lane.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 5: ServiceApp Wiring

**Files:**
- Modify: `schema_inspector/services/service_app.py`
- Modify: `tests/test_service_app.py` (append new test class; if file is over ~500 lines, add `tests/test_historical_bootstrap_wiring.py` instead)

**Context for the engineer:** Three changes here:

1. **Add `ServiceApp.build_historical_bootstrap_worker`** — mirror `build_historical_tournament_worker` at line 1373:
   ```python
   def build_historical_tournament_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> HistoricalTournamentWorker:
       self.ensure_historical_consumer_groups()
       return HistoricalTournamentWorker(
           orchestrator=self.app,
           queue=self.stream_queue,
           consumer=consumer_name,
           delayed_scheduler=self.delayed_scheduler,
           delayed_payload_store=self.delayed_envelope_store,
           completion_store=self.completion_store,
           block_ms=block_ms,
           job_audit_logger=self.job_audit_logger,
       )
   ```
   Note: the orchestrator attribute is `self.app` (NOT `self.archive_orchestrator`), and the delayed store is `self.delayed_envelope_store` (NOT `delayed_payload_store`). Copy verbatim.

2. **Add `ServiceApp.run_historical_bootstrap_worker`** — mirror `run_historical_tournament_worker` at line 1680:
   ```python
   async def run_historical_tournament_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> None:
       worker = self.build_historical_tournament_worker(consumer_name=consumer_name, block_ms=block_ms)
       await worker.run_forever()
   ```

3. **Modify `build_historical_tournament_planner_daemon`** (line 653) to pass `bootstrap_stream=STREAM_HISTORICAL_BOOTSTRAP` into the `HistoricalTournamentPlannerDaemon(...)` constructor at line 767-803 (per spec).

- [ ] **Step 1: Write the failing test for the new builder + runner + planner kwarg**

Append in `tests/test_service_app.py` (or in a new `tests/test_historical_bootstrap_wiring.py` if `test_service_app.py` is over ~500 lines):

```python
class HistoricalBootstrapWiringTests(unittest.IsolatedAsyncioTestCase):
    def _build_app(self):
        # Mirror the most-used ServiceApp construction helper in this test
        # module — search for ``ServiceApp(`` and copy that wiring inline.
        # The bootstrap wiring tests need a real ServiceApp wired against
        # the same fakes the rest of the file uses (no new fakes invented).
        return _build_service_app_for_test()  # see note below

    def test_service_app_builds_historical_bootstrap_worker(self) -> None:
        from schema_inspector.queue.streams import (
            GROUP_HISTORICAL_BOOTSTRAP,
            STREAM_HISTORICAL_BOOTSTRAP,
        )

        app = self._build_app()
        worker = app.build_historical_bootstrap_worker(
            consumer_name="historical-bootstrap-1",
        )

        self.assertEqual(worker.stream, STREAM_HISTORICAL_BOOTSTRAP)
        self.assertEqual(worker.group, GROUP_HISTORICAL_BOOTSTRAP)
        self.assertEqual(worker.consumer, "historical-bootstrap-1")

    def test_service_app_exposes_run_historical_bootstrap_worker(self) -> None:
        app = self._build_app()
        self.assertTrue(
            callable(getattr(app, "run_historical_bootstrap_worker", None)),
            msg="ServiceApp must expose async run_historical_bootstrap_worker for CLI dispatch",
        )

    def test_planner_factory_passes_bootstrap_stream(self) -> None:
        from schema_inspector.queue.streams import STREAM_HISTORICAL_BOOTSTRAP

        app = self._build_app()
        planner = app.build_historical_tournament_planner_daemon()

        self.assertEqual(planner.bootstrap_stream, STREAM_HISTORICAL_BOOTSTRAP)
```

**Engineer note on `_build_service_app_for_test()`:** if `tests/test_service_app.py` already has a helper that constructs `ServiceApp(app)` with the existing fakes (search for `ServiceApp(` to find the pattern in use), import or reuse it. Otherwise, write a 5-10 line helper at the top of `HistoricalBootstrapWiringTests` that mirrors what the file's existing tests do — do not invent new fake stores from scratch; reuse what is already there.

- [ ] **Step 2: Run the test to verify it fails**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_service_app.py::HistoricalBootstrapWiringTests -v`
Expected: 3× FAIL — `AttributeError: 'ServiceApp' object has no attribute 'build_historical_bootstrap_worker'` for tests 1 and 2; `AssertionError` on `planner.bootstrap_stream` for test 3.

- [ ] **Step 3: Add `STREAM_HISTORICAL_BOOTSTRAP` import to `service_app.py`**

In `schema_inspector/services/service_app.py`, find the existing `from ..queue.streams import (` block near the top. Add `STREAM_HISTORICAL_BOOTSTRAP` to the imported names (alphabetical-ish placement near `STREAM_HISTORICAL_DISCOVERY`).

- [ ] **Step 4: Add `build_historical_bootstrap_worker` after `build_historical_tournament_worker` (line 1373)**

Place this method immediately after `build_historical_tournament_worker` ends (around line 1385, before `build_historical_enrichment_worker`):

```python
    def build_historical_bootstrap_worker(
        self,
        *,
        consumer_name: str,
        block_ms: int = 5_000,
    ) -> HistoricalTournamentWorker:
        """Build a worker dedicated to the bootstrap historical-archive
        lane. Mirrors build_historical_tournament_worker but pins the
        consumer to stream:etl:historical_bootstrap / cg:historical_bootstrap
        and wraps handle() with structured per-job logging (see
        schema_inspector/workers/historical_bootstrap_worker.py)."""
        from ..workers.historical_bootstrap_worker import (
            make_historical_bootstrap_worker,
        )

        self.ensure_historical_consumer_groups()
        return make_historical_bootstrap_worker(
            orchestrator=self.app,
            queue=self.stream_queue,
            consumer=consumer_name,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            completion_store=self.completion_store,
            block_ms=block_ms,
            job_audit_logger=self.job_audit_logger,
        )
```

(Note: orchestrator is `self.app`, delayed store is `self.delayed_envelope_store` — copy verbatim from the sibling at line 1373.)

- [ ] **Step 5: Add `run_historical_bootstrap_worker` after `run_historical_tournament_worker` (line 1680)**

Place this method immediately after `run_historical_tournament_worker`:

```python
    async def run_historical_bootstrap_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> None:
        worker = self.build_historical_bootstrap_worker(consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()
```

- [ ] **Step 6: Pass `bootstrap_stream` into the planner constructor**

In the same file, locate `build_historical_tournament_planner_daemon` (line 653). Find the `HistoricalTournamentPlannerDaemon(...)` constructor call inside it (per spec, lines 767-803). Add the kwarg adjacent to the existing bootstrap kwargs:

```python
        return HistoricalTournamentPlannerDaemon(
            queue=self.stream_queue,
            # ... existing kwargs (cursor_store, selector, targets, etc.) ...
            bootstrap_pending_selector=bootstrap_pending_selector,
            max_bootstrap_jobs_per_tick=max_bootstrap_jobs_per_tick,
            bootstrap_stream=STREAM_HISTORICAL_BOOTSTRAP,  # ← NEW
            # ... remaining existing kwargs (priority_config, backpressure) ...
        )
```

Do not move existing kwargs; just slot the new one next to `max_bootstrap_jobs_per_tick`.

- [ ] **Step 7: Run the wiring tests to verify they pass**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_service_app.py::HistoricalBootstrapWiringTests -v`
Expected: 3× PASS.

- [ ] **Step 8: Run the full service_app test for regression**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_service_app.py -q`
Expected: all pass.

- [ ] **Step 9: Run the broader operational test suite (per CLAUDE.md guidance)**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest tests/test_discovery_worker_service.py tests/test_planner_daemon.py tests/test_local_api_server.py tests/test_historical_tournament_planner.py tests/test_historical_archive_worker_service.py tests/test_historical_bootstrap_worker.py tests/test_historical_bootstrap_cli.py tests/test_queue_streams.py -q`
Expected: all pass.

- [ ] **Step 10: Commit**

```bash
git add schema_inspector/services/service_app.py tests/test_service_app.py
git commit -m "Phase 4.7.7 step 5: ServiceApp wires bootstrap worker + planner

Add ServiceApp.build_historical_bootstrap_worker factory and
async run_historical_bootstrap_worker runner (mirroring the
tournament-worker pair at lines 1373/1680). Pass
bootstrap_stream=STREAM_HISTORICAL_BOOTSTRAP into
HistoricalTournamentPlannerDaemon constructed by
build_historical_tournament_planner_daemon so the planner publishes
pending-state jobs to the dedicated lane instead of the shared
stream:etl:historical_tournament.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 6: Systemd Unit Template

**Files:**
- Create: `ops/systemd/sofascore-historical-bootstrap@.service`

**Context for the engineer:** Pure infra file. Copy the historical-tournament template and change two things: the `Description` line and the `ExecStart` subcommand. Servers run `sudo systemctl daemon-reload` after pull and then `enable --now` on instance numbers (`@1`, `@2`, `@3`) to start workers.

- [ ] **Step 1: Create the unit template**

Create `ops/systemd/sofascore-historical-bootstrap@.service` with this exact content:

```ini
[Unit]
Description=Sofascore Historical Bootstrap Worker %i
After=network-online.target redis-server.service postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/sofascore
Environment=PYTHONUNBUFFERED=1
ExecStart=/opt/sofascore/deploy/run_service.sh worker-historical-bootstrap --consumer-name historical-bootstrap-%i --block-ms 5000
Restart=always
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 2: Sanity-check formatting**

Run: `D:/sofascore/.venv311/Scripts/python.exe -c "import configparser; p = configparser.RawConfigParser(); p.optionxform = str; p.read('ops/systemd/sofascore-historical-bootstrap@.service'); print(list(p.sections())); print(p['Service']['ExecStart'])"`
Expected output:
```
['Unit', 'Service', 'Install']
/opt/sofascore/deploy/run_service.sh worker-historical-bootstrap --consumer-name historical-bootstrap-%i --block-ms 5000
```

- [ ] **Step 3: Commit**

```bash
git add ops/systemd/sofascore-historical-bootstrap@.service
git commit -m "Phase 4.7.7 step 6: systemd unit template for bootstrap workers

sofascore-historical-bootstrap@.service — mirror of
sofascore-historical-tournament@.service with new Description and
worker-historical-bootstrap subcommand. Deploy via sudo systemctl
enable --now sofascore-historical-bootstrap@{1,2,3} after server
git pull + daemon-reload.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

## Task 7: Full Regression + Deploy-Ready Verification

**Files:** none modified — verification only.

- [ ] **Step 1: Run the full pytest suite**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m pytest -q`
Expected: all tests pass. If a pre-existing test fails that did not fail on a clean checkout, investigate before declaring done — bootstrap stream constants showing up in `HISTORICAL_CONSUMER_GROUPS` could break a test that asserts an exact tuple length somewhere; if so, update that test to reflect the new expected length (one extra entry).

- [ ] **Step 2: Verify the new CLI subcommand resolves**

Run: `D:/sofascore/.venv311/Scripts/python.exe -m schema_inspector.cli worker-historical-bootstrap --help`
Expected: `usage:` line that includes `--consumer-name CONSUMER_NAME` and `--block-ms BLOCK_MS`. Confirms argparse wiring.

- [ ] **Step 3: Verify the planner constructs with bootstrap_stream**

Run:
```
D:/sofascore/.venv311/Scripts/python.exe -c "
from schema_inspector.queue.streams import STREAM_HISTORICAL_BOOTSTRAP
from schema_inspector.services.historical_tournament_planner import (
    HistoricalTournamentPlannerDaemon,
    HistoricalTournamentCursorStore,
    HistoricalTournamentPlanningTarget,
)

class _Q:
    def publish(self, *a, **kw): return '0-0'

class _B:
    def hgetall(self, *a, **kw): return {}
    def hset(self, *a, **kw): return 1

cs = HistoricalTournamentCursorStore(_B())
async def sel(**kw): return ()

d = HistoricalTournamentPlannerDaemon(
    queue=_Q(),
    cursor_store=cs,
    selector=sel,
    targets=(HistoricalTournamentPlanningTarget(sport_slug='football'),),
    bootstrap_stream=STREAM_HISTORICAL_BOOTSTRAP,
)
assert d.bootstrap_stream == STREAM_HISTORICAL_BOOTSTRAP
print('bootstrap_stream wired OK')
"
```
Expected output: `bootstrap_stream wired OK`.

- [ ] **Step 4: Confirm no uncommitted changes**

Run: `git status --short`
Expected: clean working tree (only untracked files unrelated to this work — none of the modified files from Tasks 1-6 should appear).

- [ ] **Step 5: Confirm the commit chain**

Run: `git log --oneline -8`
Expected: 6 commits on top of the design-doc commit (`0708b59`), one per task in order:
- `step 1: bootstrap stream/group constants`
- `step 2: planner routes bootstrap to dedicated stream + logging`
- `step 3: bootstrap worker factory + structured logging`
- `step 4: CLI subcommand worker-historical-bootstrap`
- `step 5: ServiceApp wires bootstrap worker + planner`
- `step 6: systemd unit template for bootstrap workers`

If a commit is missing or out of order, do not amend or rebase — investigate and discuss before any history rewrite (per CLAUDE.md "Git Safety Protocol").

---

## Deploy (Out of Plan Scope — Operator Action)

After all tasks land on `main` and are pushed:

```bash
# Server side
ssh sofascore-prod 'cd /opt/sofascore && git pull --ff-only origin main && sudo systemctl daemon-reload'

# Start 3 bootstrap workers initially (per spec risk register: per-job
# duration unknown, start small and measure).
ssh sofascore-prod 'sudo systemctl enable --now sofascore-historical-bootstrap@1 sofascore-historical-bootstrap@2 sofascore-historical-bootstrap@3'

# Pick up the planner change (new bootstrap_stream kwarg)
ssh sofascore-prod 'sudo systemctl restart sofascore-historical-tournament-planner'
```

Verify:

```bash
# T+60s: bootstrap group exists, consumers attached
ssh sofascore-prod 'cd /opt/sofascore && PASS=$(sed -n "s|^REDIS_URL=redis://:\([^@]*\)@.*|\1|p" .env); redis-cli -h 127.0.0.1 -p 6379 -a "$PASS" --no-auth-warning XINFO GROUPS stream:etl:historical_bootstrap'

# T+5min: throughput visible in logs
ssh sofascore-prod 'journalctl -u "sofascore-historical-bootstrap@*" --since="5 min ago" | grep bootstrap_job_done | wc -l'
# Expected ≥ 10 (rough sanity check; per-job duration unknown).

# T+60min: catalog drain rate
ssh sofascore-prod 'cd /opt/sofascore && PGPASSWORD=$(grep -oP "postgresql://[^:]+:\K[^@]+" .env) psql -U sofascore_user -d sofascore_schema_inspector -h localhost -p 5432 -c "SELECT count(*) FROM tournament_season_upstream_catalog WHERE bootstrap_state=\"events_loaded\" AND events_loaded_at > NOW() - INTERVAL \"1 hour\";"'
# Goal: ≥ 50 (vs. baseline ~1.7).
```

If goal missed at T+60min, scale workers: `sudo systemctl enable --now sofascore-historical-bootstrap@4 sofascore-historical-bootstrap@5`.

---

## Self-Review Notes

- **Spec coverage:** all 6 implementation slices from spec mapped to Tasks 1-6. Verification + deploy in Task 7 + Deploy section. Open questions left as deferred (per spec defaults).
- **Type consistency:** `HistoricalTournamentPlannerDaemon.bootstrap_stream` (Task 2) is read in `_publish_bootstrap_batch` and asserted in tests in Tasks 2 and 5 — same name throughout. `build_historical_bootstrap_worker` (Task 3) signature matches call sites in Tasks 4 and 5. `STREAM_HISTORICAL_BOOTSTRAP` / `GROUP_HISTORICAL_BOOTSTRAP` (Task 1) used identically in Tasks 2, 3, 5.
- **No placeholders:** every code block is concrete. Where the engineer must inspect existing code (e.g. CLI handler name in Task 4), explicit instructions are given for how to discover it.
- **TDD discipline:** each task starts with the failing test and only then introduces production code. Each task ends with a commit.
- **YAGNI:** no Prometheus counter, no `--bootstrap-only` flag, no XTRIM, no catalog index — all explicitly deferred per spec.
