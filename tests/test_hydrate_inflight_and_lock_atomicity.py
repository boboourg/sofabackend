"""TDD tests for Phase 2.1+2.2+2.3 — race condition + lock leak fixes
identified in PERFORMANCE_AUDIT_2026-05-20.md.

Three concerns covered:

* **Phase 2.1**: ``HydrateInFlightStore`` — Redis single-flight guard
  that prevents two HydrateWorker instances from concurrently
  processing the same ``event_id``.

* **Phase 2.2**: ``App.run_event`` in ``cli.py`` must release the
  bootstrap hydrate_lock in a ``finally`` block so an exception in
  the prefetch/commit/persist chain doesn't strand a 60-second lock
  blocking subsequent live polls.

* **Phase 2.3**: ``LiveBootstrapCoordinator.release_hydrate_lock``
  must use the Lua ``GET+DEL if owner matches`` atomic script
  (mirrored from ``live_inflight._RELEASE_IF_OWNER_SCRIPT``) so the
  release path cannot race with another worker's acquire.

Both 2.2 and 2.3 unblock the prod bug where a crashed CLI run leaves
``live:hydrate_lock:{event_id}`` populated for its full TTL, silently
dropping every live poll for that event during the window.
"""

from __future__ import annotations

import asyncio
import unittest


# ---------------------------------------------------------------------------
# Phase 2.1 — HydrateInFlightStore
# ---------------------------------------------------------------------------


class _FakeRedisBackend:
    """In-memory Redis stub supporting SET NX PX, GET, DELETE, EVAL."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.eval_calls: list[tuple[str, int, tuple[str, ...]]] = []

    def set(self, key, value, *, nx=False, px=None):
        if nx and key in self.store:
            return None
        self.store[key] = str(value)
        return True

    def get(self, key):
        return self.store.get(key)

    def delete(self, key):
        return 1 if self.store.pop(key, None) is not None else 0

    def eval(self, script, numkeys, *args):
        self.eval_calls.append((script, numkeys, tuple(args)))
        # Implement the _RELEASE_IF_OWNER_SCRIPT semantics
        key = args[0]
        owner = args[1]
        if self.store.get(key) == owner:
            del self.store[key]
            return 1
        return 0


class HydrateInFlightStoreTests(unittest.TestCase):
    def test_claim_succeeds_when_no_existing_lock(self) -> None:
        from schema_inspector.queue.live_inflight import HydrateInFlightStore

        backend = _FakeRedisBackend()
        store = HydrateInFlightStore(backend)
        self.assertTrue(store.claim(event_id=42, owner="worker-1:msg-1"))
        self.assertIn("hydrate:inflight:42", backend.store)

    def test_claim_fails_when_lock_already_held(self) -> None:
        from schema_inspector.queue.live_inflight import HydrateInFlightStore

        backend = _FakeRedisBackend()
        store = HydrateInFlightStore(backend)
        self.assertTrue(store.claim(event_id=42, owner="worker-A"))
        self.assertFalse(store.claim(event_id=42, owner="worker-B"))

    def test_release_with_matching_owner_uses_lua(self) -> None:
        from schema_inspector.queue.live_inflight import HydrateInFlightStore

        backend = _FakeRedisBackend()
        store = HydrateInFlightStore(backend)
        store.claim(event_id=42, owner="worker-A")
        store.release(event_id=42, owner="worker-A")
        # Lua script invoked exactly once
        self.assertEqual(len(backend.eval_calls), 1)
        self.assertNotIn("hydrate:inflight:42", backend.store)

    def test_release_with_wrong_owner_is_noop(self) -> None:
        from schema_inspector.queue.live_inflight import HydrateInFlightStore

        backend = _FakeRedisBackend()
        store = HydrateInFlightStore(backend)
        store.claim(event_id=42, owner="worker-A")
        store.release(event_id=42, owner="worker-B")
        # Lua refused (returns 0), key still present
        self.assertIn("hydrate:inflight:42", backend.store)


# ---------------------------------------------------------------------------
# Phase 2.3 — LiveBootstrapCoordinator atomic Lua release
# ---------------------------------------------------------------------------


class LiveBootstrapLuaReleaseTests(unittest.IsolatedAsyncioTestCase):
    async def test_release_hydrate_lock_uses_lua_eval(self) -> None:
        from schema_inspector.live_bootstrap import LiveBootstrapCoordinator

        backend = _FakeRedisBackend()
        coord = LiveBootstrapCoordinator(redis_backend=backend, worker_id="w1")
        coord.acquire_hydrate_lock(event_id=99)

        deleted = coord.release_hydrate_lock(event_id=99)
        self.assertTrue(deleted)
        # Lua release fired
        self.assertGreaterEqual(len(backend.eval_calls), 1)
        self.assertNotIn("live:hydrate_lock:99", backend.store)

    async def test_release_hydrate_lock_skips_when_owner_differs(self) -> None:
        from schema_inspector.live_bootstrap import LiveBootstrapCoordinator

        backend = _FakeRedisBackend()
        # Pre-populate with foreign owner
        backend.store["live:hydrate_lock:99"] = "other-worker"
        coord = LiveBootstrapCoordinator(redis_backend=backend, worker_id="w1")

        deleted = coord.release_hydrate_lock(event_id=99)
        self.assertFalse(deleted)
        # Lua was called, Lua returned 0, key still present (owned by other-worker)
        self.assertEqual(backend.store.get("live:hydrate_lock:99"), "other-worker")

    async def test_release_falls_back_when_backend_has_no_eval(self) -> None:
        """For non-Redis backends (in-memory stub used in some tests),
        the legacy GET+CHECK+DELETE path must still work."""
        from schema_inspector.live_bootstrap import LiveBootstrapCoordinator

        class _NoEvalBackend:
            def __init__(self) -> None:
                self.store: dict[str, str] = {}

            def set(self, key, value, *, nx=False, px=None):
                if nx and key in self.store:
                    return None
                self.store[key] = str(value)
                return True

            def get(self, key):
                return self.store.get(key)

            def delete(self, key):
                return 1 if self.store.pop(key, None) is not None else 0

        backend = _NoEvalBackend()
        coord = LiveBootstrapCoordinator(redis_backend=backend, worker_id="w1")
        coord.acquire_hydrate_lock(event_id=99)
        deleted = coord.release_hydrate_lock(event_id=99)
        self.assertTrue(deleted)
        self.assertNotIn("live:hydrate_lock:99", backend.store)


# ---------------------------------------------------------------------------
# Phase 2.2 — App.run_event try/finally
# ---------------------------------------------------------------------------


class CliRunEventReleasesLockOnExceptionTests(unittest.IsolatedAsyncioTestCase):
    """``App.run_event`` must release the hydrate_lock in a finally
    block when an exception escapes the prefetch/commit/persist chain.
    Previously the lock leaked for the full 60s TTL on every CLI crash.
    """

    async def test_release_called_in_finally_when_acquired(self) -> None:
        """Inspect the source of cli.py App.run_event — the body where
        ``acquire_hydrate_lock`` is called must contain a matching
        ``release_hydrate_lock`` call in a finally block."""
        import re
        from pathlib import Path

        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "cli.py"
        ).read_text(encoding="utf-8")

        # Extract the body of class App's run_event method. Look for
        # ``async def run_event(`` … next ``async def`` boundary.
        body_match = re.search(
            r"async def run_event\(.*?(?=\n {4}async def \w+)",
            text,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(body_match, "could not locate App.run_event body")
        body = body_match.group(0)

        # The body must contain both acquire AND release within a
        # try/finally structure. The simplest invariant: there's a
        # ``finally:`` clause that calls ``release_hydrate_lock``.
        self.assertIn("acquire_hydrate_lock", body)
        self.assertIn("release_hydrate_lock", body)
        self.assertRegex(
            body,
            re.compile(r"finally:.*?release_hydrate_lock", re.DOTALL),
            "release_hydrate_lock must be called inside a finally block "
            "of App.run_event",
        )


# ---------------------------------------------------------------------------
# Phase 2.1 — HydrateWorker uses HydrateInFlightStore
# ---------------------------------------------------------------------------


class HydrateWorkerInFlightLockTests(unittest.IsolatedAsyncioTestCase):
    """``HydrateWorker.handle()`` must take a Redis lock keyed on
    ``event_id`` before invoking ``orchestrator.run_event`` and
    release it in a finally block. When two workers race on the
    same event_id, only one must call run_event; the loser ACKs
    with a special ``inflight_skip`` outcome."""

    async def test_handle_skips_when_inflight_lock_is_held(self) -> None:
        from schema_inspector.workers.hydrate_worker import HydrateWorker
        from schema_inspector.queue.streams import StreamEntry

        class _StubOrchestrator:
            def __init__(self) -> None:
                self.calls: list[int] = []

            async def run_event(self, **kwargs):
                self.calls.append(kwargs.get("event_id"))

        class _StubLockStore:
            def __init__(self, claim_result: bool) -> None:
                self.claim_result = claim_result
                self.released: list[tuple[int, str]] = []

            def claim(self, *, event_id, owner):
                return self.claim_result

            def release(self, *, event_id, owner):
                self.released.append((event_id, owner))

        orchestrator = _StubOrchestrator()
        lock_store = _StubLockStore(claim_result=False)  # Lock unavailable
        worker = HydrateWorker.__new__(HydrateWorker)
        worker.orchestrator = orchestrator
        worker.consumer = "hydrate-test"
        worker.default_sport_slug = "football"
        worker.circuit_breaker = None
        worker.circuit_breaker_inprogress_count_provider = None
        worker.now_ms_factory = lambda: 0
        worker.hydrate_inflight_store = lock_store

        entry = StreamEntry(
            stream="stream:etl:hydrate",
            message_id="1-0",
            values={
                "job_type": "hydrate_event_root",
                "entity_id": "12345",
                "entity_type": "event",
                "sport_slug": "football",
                "job_id": "job-1",
            },
        )

        result = await worker.handle(entry)
        # Worker must NOT call run_event when lock unavailable
        self.assertEqual(orchestrator.calls, [])
        # Special outcome string for telemetry
        self.assertEqual(result, "inflight_skip")

    async def test_handle_releases_lock_on_success(self) -> None:
        from schema_inspector.workers.hydrate_worker import HydrateWorker
        from schema_inspector.queue.streams import StreamEntry

        class _StubOrchestrator:
            async def run_event(self, **kwargs):
                pass

        class _StubLockStore:
            def __init__(self) -> None:
                self.released: list[tuple[int, str]] = []

            def claim(self, *, event_id, owner):
                return True

            def release(self, *, event_id, owner):
                self.released.append((event_id, owner))

        lock_store = _StubLockStore()
        worker = HydrateWorker.__new__(HydrateWorker)
        worker.orchestrator = _StubOrchestrator()
        worker.consumer = "hydrate-test"
        worker.default_sport_slug = "football"
        worker.circuit_breaker = None
        worker.circuit_breaker_inprogress_count_provider = None
        worker.now_ms_factory = lambda: 0
        worker.hydrate_inflight_store = lock_store

        entry = StreamEntry(
            stream="stream:etl:hydrate",
            message_id="1-0",
            values={
                "job_type": "hydrate_event_root",
                "entity_id": "777",
                "entity_type": "event",
                "sport_slug": "football",
                "job_id": "job-2",
            },
        )

        result = await worker.handle(entry)
        self.assertEqual(result, "completed")
        self.assertEqual(len(lock_store.released), 1)
        self.assertEqual(lock_store.released[0][0], 777)

    async def test_handle_releases_lock_on_exception(self) -> None:
        from schema_inspector.workers.hydrate_worker import HydrateWorker
        from schema_inspector.queue.streams import StreamEntry

        class _BoomOrchestrator:
            async def run_event(self, **kwargs):
                raise RuntimeError("boom")

        class _StubLockStore:
            def __init__(self) -> None:
                self.released: list[tuple[int, str]] = []

            def claim(self, *, event_id, owner):
                return True

            def release(self, *, event_id, owner):
                self.released.append((event_id, owner))

        lock_store = _StubLockStore()
        worker = HydrateWorker.__new__(HydrateWorker)
        worker.orchestrator = _BoomOrchestrator()
        worker.consumer = "hydrate-test"
        worker.default_sport_slug = "football"
        worker.circuit_breaker = None
        worker.circuit_breaker_inprogress_count_provider = None
        worker.now_ms_factory = lambda: 0
        worker.hydrate_inflight_store = lock_store

        entry = StreamEntry(
            stream="stream:etl:hydrate",
            message_id="1-0",
            values={
                "job_type": "hydrate_event_root",
                "entity_id": "555",
                "entity_type": "event",
                "sport_slug": "football",
                "job_id": "job-3",
            },
        )

        with self.assertRaises(RuntimeError):
            await worker.handle(entry)
        # Lock must still be released
        self.assertEqual(len(lock_store.released), 1)


if __name__ == "__main__":
    unittest.main()
