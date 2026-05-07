from __future__ import annotations

import unittest

from schema_inspector.endpoints import (
    UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT,
)
from schema_inspector.fetch_models import FetchOutcomeEnvelope, FetchTask
from schema_inspector.jobs.envelope import JobEnvelope
from schema_inspector.jobs.types import JOB_REFRESH_RESOURCE
from schema_inspector.queue.resource_negative_cache import ResourceNegativeCache
from schema_inspector.queue.streams import StreamEntry
from schema_inspector.queue.totw_404_store import (
    HASH_KEY,
    STATE_NO,
    STATE_YES,
    ToTW404Store,
)
from schema_inspector.workers._stream_jobs import encode_stream_job
from schema_inspector.workers.resource_refresh_worker import ResourceRefreshWorker


class _FakeRedisHash:
    """Minimal in-memory backend with hset/hget/hdel and exists/set."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.scalars: dict[str, str] = {}

    def hset(self, key, mapping=None, **kwargs):
        if mapping is None:
            mapping = kwargs.get("mapping")
        bucket = self.hashes.setdefault(key, {})
        bucket.update({str(k): str(v) for k, v in (mapping or {}).items()})
        return len(mapping or {})

    def hget(self, key, field):
        return self.hashes.get(key, {}).get(str(field))

    def hdel(self, key, field):
        return 1 if self.hashes.get(key, {}).pop(str(field), None) is not None else 0

    def exists(self, key):
        return 1 if key in self.scalars else 0

    def set(self, key, value, ex=None):
        self.scalars[key] = str(value)
        return True

    def delete(self, key):
        return 1 if self.scalars.pop(key, None) is not None else 0


class ToTW404StoreTests(unittest.TestCase):
    def test_blank_state_is_not_blacklisted(self) -> None:
        store = ToTW404Store(_FakeRedisHash())
        self.assertFalse(store.is_blacklisted(17, 76986))

    def test_mark_no_then_is_blacklisted_returns_true(self) -> None:
        backend = _FakeRedisHash()
        store = ToTW404Store(backend, now_factory=lambda: 1_700_000_000.0)
        store.mark_no(17, 76986)
        self.assertTrue(store.is_blacklisted(17, 76986))
        self.assertEqual(backend.hashes[HASH_KEY]["17:76986"], "no:1700000000")

    def test_mark_yes_does_not_blacklist(self) -> None:
        backend = _FakeRedisHash()
        store = ToTW404Store(backend, now_factory=lambda: 1_700_000_000.0)
        store.mark_yes(17, 76986)
        self.assertFalse(store.is_blacklisted(17, 76986))
        self.assertEqual(backend.hashes[HASH_KEY]["17:76986"], "yes:1700000000")

    def test_clear_resets(self) -> None:
        backend = _FakeRedisHash()
        store = ToTW404Store(backend)
        store.mark_no(17, 76986)
        self.assertTrue(store.is_blacklisted(17, 76986))
        store.clear(17, 76986)
        self.assertFalse(store.is_blacklisted(17, 76986))

    def test_blacklist_expires_after_ttl(self) -> None:
        now = [1_700_000_000.0]
        backend = _FakeRedisHash()
        store = ToTW404Store(
            backend,
            ttl_seconds=600,
            now_factory=lambda: now[0],
        )
        store.mark_no(17, 76986)
        self.assertTrue(store.is_blacklisted(17, 76986))
        now[0] += 700  # past TTL
        self.assertFalse(store.is_blacklisted(17, 76986))

    def test_legacy_value_without_timestamp_is_blacklisted(self) -> None:
        # Older entries (state-only, no ts) should still be respected
        # so a code rollback doesn't unblacklist seasons.
        backend = _FakeRedisHash()
        backend.hset(HASH_KEY, mapping={"17:76986": "no"})
        store = ToTW404Store(backend)
        self.assertTrue(store.is_blacklisted(17, 76986))

    def test_no_redis_backend_is_no_op(self) -> None:
        store = ToTW404Store(None)
        self.assertFalse(store.is_blacklisted(17, 76986))
        store.mark_no(17, 76986)  # should not raise
        store.clear(17, 76986)  # should not raise


# ----------------------------------------------------------------------
# Worker hook
# ----------------------------------------------------------------------


class _CapturingExecutor:
    def __init__(self, *, http_status: int = 404) -> None:
        self.http_status = http_status

    async def execute(self, task: FetchTask) -> FetchOutcomeEnvelope:
        return FetchOutcomeEnvelope(
            trace_id=task.trace_id,
            job_id=task.job_id,
            endpoint_pattern=task.endpoint_pattern,
            source_url=task.source_url,
            resolved_url=task.source_url,
            http_status=self.http_status,
            classification="not_found" if self.http_status == 404 else "success_json",
            proxy_id=None,
            challenge_reason=None,
            snapshot_id=None,
            payload_hash=None,
        )


class _StubScheduler:
    def schedule(self, job_id, *, run_at_epoch_ms):
        pass


class _StubPayloadStore:
    def save_entry(self, entry):
        pass


def _make_envelope(*, pattern, ut, season, period_id) -> JobEnvelope:
    return JobEnvelope.create(
        job_type=JOB_REFRESH_RESOURCE,
        sport_slug="football",
        entity_type="period",
        entity_id=period_id,
        scope="resource_refresh",
        params={
            "endpoint_pattern": pattern,
            "path_params": {
                "unique_tournament_id": ut,
                "season_id": season,
                "period_id": period_id,
            },
        },
        priority=40,
        trace_id="t-1",
        capability_hint="resource_refresh",
    )


def _entry(envelope: JobEnvelope) -> StreamEntry:
    payload = {str(k): str(v) for k, v in encode_stream_job(envelope).items()}
    return StreamEntry(
        stream="stream:etl:resource_refresh",
        message_id="0-1",
        values=payload,
    )


class WorkerToTW404HookTests(unittest.IsolatedAsyncioTestCase):
    async def test_404_on_totw_marks_season(self) -> None:
        backend = _FakeRedisHash()
        store = ToTW404Store(backend)
        executor = _CapturingExecutor(http_status=404)

        worker = ResourceRefreshWorker(
            fetch_executor=executor,
            delayed_scheduler=_StubScheduler(),
            delayed_payload_store=_StubPayloadStore(),
            completion_store=None,
            queue=None,
            consumer="t-1",
            endpoints=(UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT,),
            negative_cache=ResourceNegativeCache(backend),
            totw_404_store=store,
        )
        envelope = _make_envelope(
            pattern=UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT.pattern,
            ut=999,
            season=88888,
            period_id=12345,
        )
        result = await worker.handle(_entry(envelope))
        self.assertEqual(result, "completed")
        self.assertTrue(store.is_blacklisted(999, 88888))

    async def test_200_on_totw_does_not_mark_season(self) -> None:
        backend = _FakeRedisHash()
        store = ToTW404Store(backend)
        executor = _CapturingExecutor(http_status=200)

        worker = ResourceRefreshWorker(
            fetch_executor=executor,
            delayed_scheduler=_StubScheduler(),
            delayed_payload_store=_StubPayloadStore(),
            completion_store=None,
            queue=None,
            consumer="t-1",
            endpoints=(UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT,),
            negative_cache=ResourceNegativeCache(backend),
            totw_404_store=store,
        )
        envelope = _make_envelope(
            pattern=UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT.pattern,
            ut=17,
            season=76986,
            period_id=100,
        )
        await worker.handle(_entry(envelope))
        self.assertFalse(store.is_blacklisted(17, 76986))

    async def test_404_on_other_endpoints_does_not_blacklist(self) -> None:
        backend = _FakeRedisHash()
        store = ToTW404Store(backend)
        executor = _CapturingExecutor(http_status=404)

        # Use a different endpoint (rounds) with the same path_params shape.
        from schema_inspector.endpoints import UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT

        worker = ResourceRefreshWorker(
            fetch_executor=executor,
            delayed_scheduler=_StubScheduler(),
            delayed_payload_store=_StubPayloadStore(),
            completion_store=None,
            queue=None,
            consumer="t-1",
            endpoints=(UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT,),
            negative_cache=ResourceNegativeCache(backend),
            totw_404_store=store,
        )
        envelope = JobEnvelope.create(
            job_type=JOB_REFRESH_RESOURCE,
            sport_slug="football",
            entity_type="season",
            entity_id=88888,
            scope="resource_refresh",
            params={
                "endpoint_pattern": UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT.pattern,
                "path_params": {
                    "unique_tournament_id": 999,
                    "season_id": 88888,
                },
            },
            priority=40,
            trace_id="t-1",
            capability_hint="resource_refresh",
        )
        await worker.handle(_entry(envelope))
        self.assertFalse(store.is_blacklisted(999, 88888))


if __name__ == "__main__":
    unittest.main()
