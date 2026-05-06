from __future__ import annotations

import json
import unittest
from typing import Iterable

from schema_inspector.endpoints import SofascoreEndpoint
from schema_inspector.jobs.types import JOB_REFRESH_RESOURCE
from schema_inspector.queue.resource_cursor import ResourceCursorStore
from schema_inspector.services.resource_planner import ResourcePlannerDaemon
from schema_inspector.services.resource_scope import ResourceTarget


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, dict[str, str]] = {}

    def hget(self, key, field):
        return self.store.get(key, {}).get(field)

    def hset(self, key, mapping=None, **kwargs):
        if mapping is None:
            mapping = kwargs.get("mapping")
        bucket = self.store.setdefault(key, {})
        bucket.update(mapping or {})
        return len(mapping or {})


class _FakeQueue:
    def __init__(self, *, length: int = 0) -> None:
        self.length = length
        self.published: list[tuple[str, dict[str, object]]] = []

    def stream_length(self, stream: str) -> int:
        return self.length

    def publish(self, stream: str, values) -> str:
        self.published.append((stream, dict(values)))
        return f"id-{len(self.published)}"


class _FakeResolver:
    kind = "test"

    def __init__(self, targets: Iterable[ResourceTarget]) -> None:
        self.targets = tuple(targets)
        self.resolve_calls = 0

    async def resolve(self) -> Iterable[ResourceTarget]:
        self.resolve_calls += 1
        return self.targets


def _stub_endpoint(
    *,
    pattern: str = "/api/v1/team/{team_id}/players",
    refresh_interval: int | None = 3600,
    scope_kind: str | None = "test",
    freshness_ttl: int | None = 1800,
) -> SofascoreEndpoint:
    return SofascoreEndpoint(
        path_template=pattern,
        envelope_key="players",
        target_table="api_payload_snapshot",
        refresh_interval_seconds=refresh_interval,
        scope_kind=scope_kind,
        freshness_ttl_seconds=freshness_ttl,
    )


def _team_target(team_id: int) -> ResourceTarget:
    return ResourceTarget(
        entity_type="team",
        entity_id=team_id,
        path_params={"team_id": team_id},
    )


class ResourcePlannerDaemonTests(unittest.IsolatedAsyncioTestCase):
    def _build(
        self,
        *,
        endpoint: SofascoreEndpoint,
        resolver: _FakeResolver,
        queue: _FakeQueue | None = None,
        redis_backend=None,
        publish_per_tick_cap: int = 20,
        lag_threshold: int = 5000,
        now_ms: int = 10_000_000,
    ) -> tuple[ResourcePlannerDaemon, _FakeQueue, ResourceCursorStore]:
        queue = queue or _FakeQueue()
        cursor_store = ResourceCursorStore(redis_backend or _FakeRedis())
        planner = ResourcePlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            endpoints=(endpoint,),
            resolvers={resolver.kind: resolver},
            stream="stream:etl:resource_refresh",
            tick_interval_seconds=30.0,
            publish_per_tick_cap=publish_per_tick_cap,
            lag_threshold=lag_threshold,
            now_ms_factory=lambda: now_ms,
        )
        return planner, queue, cursor_store

    async def test_endpoint_without_refresh_interval_is_ignored(self) -> None:
        endpoint = _stub_endpoint(refresh_interval=None)
        resolver = _FakeResolver([_team_target(42)])
        planner, queue, _ = self._build(endpoint=endpoint, resolver=resolver)
        published = await planner.tick()
        self.assertEqual(published, 0)
        self.assertEqual(queue.published, [])
        self.assertEqual(resolver.resolve_calls, 0)

    async def test_first_tick_publishes_one_job_per_target(self) -> None:
        endpoint = _stub_endpoint()
        resolver = _FakeResolver([_team_target(42), _team_target(2672)])
        planner, queue, cursor_store = self._build(endpoint=endpoint, resolver=resolver)
        published = await planner.tick()
        self.assertEqual(published, 2)
        self.assertEqual(len(queue.published), 2)
        # Envelope shape sanity:
        envelope = queue.published[0][1]
        self.assertEqual(envelope["job_type"], JOB_REFRESH_RESOURCE)
        self.assertEqual(envelope["entity_type"], "team")
        self.assertIn(int(envelope["entity_id"]), {42, 2672})
        params = json.loads(envelope["params_json"])
        self.assertEqual(params["endpoint_pattern"], endpoint.pattern)
        self.assertEqual(params["path_params"], {"team_id": int(envelope["entity_id"])})
        self.assertEqual(params["freshness_ttl_seconds"], endpoint.freshness_ttl_seconds)
        self.assertIn("freshness_key", params)
        # Cursor recorded for both:
        for tid in (42, 2672):
            self.assertGreater(
                cursor_store.load_last_refresh_ms(
                    endpoint_pattern=endpoint.pattern, path_params={"team_id": tid}
                ),
                0,
            )

    async def test_second_tick_within_interval_publishes_nothing(self) -> None:
        endpoint = _stub_endpoint(refresh_interval=3600)
        resolver = _FakeResolver([_team_target(42)])
        planner, queue, _ = self._build(endpoint=endpoint, resolver=resolver, now_ms=10_000_000)
        await planner.tick()
        self.assertEqual(len(queue.published), 1)
        # Second tick at same now -> cursor still fresh, must skip.
        published = await planner.tick()
        self.assertEqual(published, 0)
        self.assertEqual(len(queue.published), 1)

    async def test_tick_after_interval_republishes(self) -> None:
        endpoint = _stub_endpoint(refresh_interval=3600)
        resolver = _FakeResolver([_team_target(42)])
        backend = _FakeRedis()
        planner, queue, _ = self._build(
            endpoint=endpoint, resolver=resolver, redis_backend=backend, now_ms=0
        )
        await planner.tick()  # publishes at now=0
        # Mutate planner clock forward beyond interval and re-tick.
        planner.now_ms_factory = lambda: 3700 * 1000
        published = await planner.tick()
        self.assertEqual(published, 1)
        self.assertEqual(len(queue.published), 2)

    async def test_per_tick_cap_limits_publishing(self) -> None:
        endpoint = _stub_endpoint()
        resolver = _FakeResolver([_team_target(i) for i in range(50)])
        planner, queue, _ = self._build(
            endpoint=endpoint, resolver=resolver, publish_per_tick_cap=10
        )
        published = await planner.tick()
        self.assertEqual(published, 10)
        self.assertEqual(len(queue.published), 10)

    async def test_backpressure_skips_tick_when_lag_high(self) -> None:
        endpoint = _stub_endpoint()
        resolver = _FakeResolver([_team_target(42)])
        queue = _FakeQueue(length=10_000)
        planner, _, _ = self._build(
            endpoint=endpoint, resolver=resolver, queue=queue, lag_threshold=5_000
        )
        published = await planner.tick()
        self.assertEqual(published, 0)
        self.assertEqual(queue.published, [])
        self.assertEqual(resolver.resolve_calls, 0)

    async def test_missing_resolver_logs_and_skips(self) -> None:
        endpoint = _stub_endpoint(scope_kind="not-registered")
        resolver = _FakeResolver([_team_target(42)])
        planner, queue, _ = self._build(endpoint=endpoint, resolver=resolver)
        published = await planner.tick()
        self.assertEqual(published, 0)
        self.assertEqual(queue.published, [])
        self.assertEqual(resolver.resolve_calls, 0)

    async def test_pagination_targets_get_distinct_cursors(self) -> None:
        endpoint = _stub_endpoint(
            pattern="/api/v1/team/{team_id}/events/last/{page}",
        )
        resolver = _FakeResolver(
            [
                ResourceTarget(
                    entity_type="team",
                    entity_id=42,
                    path_params={"team_id": 42, "page": 0},
                ),
                ResourceTarget(
                    entity_type="team",
                    entity_id=42,
                    path_params={"team_id": 42, "page": 1},
                ),
            ]
        )
        planner, queue, cursor_store = self._build(endpoint=endpoint, resolver=resolver)
        published = await planner.tick()
        self.assertEqual(published, 2)
        # Both pages must record their own cursor row.
        c0 = cursor_store.load_last_refresh_ms(
            endpoint_pattern=endpoint.pattern, path_params={"team_id": 42, "page": 0}
        )
        c1 = cursor_store.load_last_refresh_ms(
            endpoint_pattern=endpoint.pattern, path_params={"team_id": 42, "page": 1}
        )
        self.assertGreater(c0, 0)
        self.assertGreater(c1, 0)


if __name__ == "__main__":
    unittest.main()
