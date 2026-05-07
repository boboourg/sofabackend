from __future__ import annotations

import json
import unittest
from typing import Iterable

from schema_inspector.endpoints import SofascoreEndpoint
from schema_inspector.jobs.types import JOB_REFRESH_RESOURCE
from schema_inspector.queue.empty_data import EmptyDataStore
from schema_inspector.queue.resource_cursor import ResourceCursorStore
from schema_inspector.queue.resource_negative_cache import ResourceNegativeCache
from schema_inspector.services.resource_planner import ResourcePlannerDaemon
from schema_inspector.services.resource_scope import ResourceTarget


class _FakeRedis:
    """Minimal Redis stub supporting both hash ops (cursor) and key/value
    ops with TTL (negative cache).
    """

    def __init__(self) -> None:
        self.store: dict[str, dict[str, str]] = {}
        self.kv: dict[str, str] = {}

    def hget(self, key, field):
        return self.store.get(key, {}).get(field)

    def hset(self, key, mapping=None, **kwargs):
        if mapping is None:
            mapping = kwargs.get("mapping")
        bucket = self.store.setdefault(key, {})
        bucket.update(mapping or {})
        return len(mapping or {})

    def exists(self, key):
        return 1 if key in self.kv else 0

    def set(self, key, value, ex=None):
        self.kv[key] = str(value)
        return True

    def delete(self, key):
        return 1 if self.kv.pop(key, None) is not None else 0


class _FakeGroupInfo:
    def __init__(self, lag: int | None) -> None:
        self.lag = lag


class _FakeQueue:
    def __init__(self, *, lag: int = 0) -> None:
        self.lag = lag
        self.published: list[tuple[str, dict[str, object]]] = []

    def stream_length(self, stream: str) -> int:
        # Kept for compatibility with older callers; planner now uses group_info.
        return self.lag

    def group_info(self, stream: str, group: str):
        return _FakeGroupInfo(self.lag)

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
        negative_cache: ResourceNegativeCache | None = None,
        empty_data_store: EmptyDataStore | None = None,
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
            negative_cache=negative_cache,
            empty_data_store=empty_data_store,
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

    async def test_backpressure_skips_tick_when_group_lag_high(self) -> None:
        # Backpressure MUST consult the consumer-group lag (unread tail), not
        # the total stream length. Streams retain ack'd messages forever
        # without XTRIM/MAXLEN, so using XLEN here would wedge the planner
        # the moment XLEN exceeded the threshold even with lag=0.
        endpoint = _stub_endpoint()
        resolver = _FakeResolver([_team_target(42)])
        queue = _FakeQueue(lag=10_000)
        planner, _, _ = self._build(
            endpoint=endpoint, resolver=resolver, queue=queue, lag_threshold=5_000
        )
        published = await planner.tick()
        self.assertEqual(published, 0)
        self.assertEqual(queue.published, [])
        self.assertEqual(resolver.resolve_calls, 0)

    async def test_backpressure_does_not_trip_on_high_xlen_with_zero_lag(self) -> None:
        # Regression: pre-fix, the planner read XLEN (which keeps growing
        # because Redis Streams do not auto-trim ack'd messages) and got
        # stuck in permanent backpressure.
        endpoint = _stub_endpoint()
        resolver = _FakeResolver([_team_target(42)])

        class _XlenHighLagZeroQueue(_FakeQueue):
            def stream_length(self, stream: str) -> int:
                return 50_000  # huge tail of already-acked messages

            def group_info(self, stream: str, group: str):
                return _FakeGroupInfo(0)  # but the unread lag is zero

        queue = _XlenHighLagZeroQueue()
        planner, _, _ = self._build(
            endpoint=endpoint, resolver=resolver, queue=queue, lag_threshold=5_000
        )
        published = await planner.tick()
        self.assertEqual(published, 1)

    async def test_missing_resolver_logs_and_skips(self) -> None:
        endpoint = _stub_endpoint(scope_kind="not-registered")
        resolver = _FakeResolver([_team_target(42)])
        planner, queue, _ = self._build(endpoint=endpoint, resolver=resolver)
        published = await planner.tick()
        self.assertEqual(published, 0)
        self.assertEqual(queue.published, [])
        self.assertEqual(resolver.resolve_calls, 0)

    async def test_round_robin_fairness_across_endpoints(self) -> None:
        """Sequential walking under a shared cap starves later endpoints
        during cold start. Round-robin must give every opted-in endpoint a
        proportional share of the per-tick cap."""

        endpoint_a = _stub_endpoint(pattern="/api/v1/team/{team_id}/players")
        endpoint_b = _stub_endpoint(pattern="/api/v1/player/{player_id}/transfer-history")
        endpoint_c = _stub_endpoint(pattern="/api/v1/player/{player_id}/attribute-overviews")

        # endpoint_a has 50 overdue targets (simulating the cold-start spike);
        # the others have plenty as well. With cap=9 fairness means 3+3+3.
        targets_a = [
            ResourceTarget(entity_type="team", entity_id=i, path_params={"team_id": i})
            for i in range(50)
        ]
        targets_b = [
            ResourceTarget(entity_type="player", entity_id=10_000 + i, path_params={"player_id": 10_000 + i})
            for i in range(20)
        ]
        targets_c = [
            ResourceTarget(entity_type="player", entity_id=20_000 + i, path_params={"player_id": 20_000 + i})
            for i in range(20)
        ]

        class _MultiResolver:
            kind = "test"

            def __init__(self, plans: dict[str, list]):
                self.plans = plans
                self.calls = 0

            async def resolve(self):
                self.calls += 1
                # Resolver returns the union; planner filters per endpoint via
                # cursor anyway. To test fairness we fake one resolver that
                # yields different targets per endpoint by reading the call
                # state. Easier: register three separate resolvers under
                # distinct kinds.
                raise NotImplementedError

        # Use distinct scope_kinds and register separate resolvers per endpoint.
        endpoint_a = _stub_endpoint(
            pattern="/api/v1/team/{team_id}/players", scope_kind="kind-a"
        )
        endpoint_b = _stub_endpoint(
            pattern="/api/v1/player/{player_id}/transfer-history", scope_kind="kind-b"
        )
        endpoint_c = _stub_endpoint(
            pattern="/api/v1/player/{player_id}/attribute-overviews", scope_kind="kind-c"
        )
        resolver_a = _FakeResolver(targets_a)
        resolver_a.kind = "kind-a"
        resolver_b = _FakeResolver(targets_b)
        resolver_b.kind = "kind-b"
        resolver_c = _FakeResolver(targets_c)
        resolver_c.kind = "kind-c"

        queue = _FakeQueue()
        cursor_store = ResourceCursorStore(_FakeRedis())
        planner = ResourcePlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            endpoints=(endpoint_a, endpoint_b, endpoint_c),
            resolvers={
                "kind-a": resolver_a,
                "kind-b": resolver_b,
                "kind-c": resolver_c,
            },
            stream="stream:etl:resource_refresh",
            tick_interval_seconds=30.0,
            publish_per_tick_cap=9,
            lag_threshold=5_000,
            now_ms_factory=lambda: 10_000_000,
        )

        published = await planner.tick()

        self.assertEqual(published, 9)
        # Count published per endpoint pattern: each must get exactly 3.
        counts = {"a": 0, "b": 0, "c": 0}
        for stream, env in queue.published:
            params = json.loads(env["params_json"])
            if "team_id" in params["path_params"]:
                counts["a"] += 1
            elif params["endpoint_pattern"].endswith("transfer-history"):
                counts["b"] += 1
            elif params["endpoint_pattern"].endswith("attribute-overviews"):
                counts["c"] += 1
        self.assertEqual(counts, {"a": 3, "b": 3, "c": 3})

    async def test_negative_cache_skips_publish_and_does_not_update_cursor(self) -> None:
        endpoint = _stub_endpoint()
        resolver = _FakeResolver([_team_target(1161574)])
        backend = _FakeRedis()
        negative_cache = ResourceNegativeCache(backend, env={})
        # Pre-mark the target as 404.
        negative_cache.mark_404(
            endpoint_pattern=endpoint.pattern,
            path_params={"team_id": 1161574},
        )
        planner, queue, cursor_store = self._build(
            endpoint=endpoint,
            resolver=resolver,
            redis_backend=backend,
            negative_cache=negative_cache,
        )

        published = await planner.tick()

        self.assertEqual(published, 0)
        self.assertEqual(queue.published, [])
        # Cursor must NOT advance -- otherwise after the negative TTL expires
        # the planner would still skip due to a "fresh" cursor.
        self.assertEqual(
            cursor_store.load_last_refresh_ms(
                endpoint_pattern=endpoint.pattern,
                path_params={"team_id": 1161574},
            ),
            0,
        )
        self.assertEqual(planner._stats_negative_skipped, 1)

    async def test_negative_cache_does_not_block_healthy_targets(self) -> None:
        endpoint = _stub_endpoint()
        resolver = _FakeResolver([_team_target(42), _team_target(1161574)])
        backend = _FakeRedis()
        negative_cache = ResourceNegativeCache(backend, env={})
        negative_cache.mark_404(
            endpoint_pattern=endpoint.pattern,
            path_params={"team_id": 1161574},
        )
        planner, queue, _ = self._build(
            endpoint=endpoint,
            resolver=resolver,
            redis_backend=backend,
            negative_cache=negative_cache,
        )

        published = await planner.tick()

        self.assertEqual(published, 1)
        self.assertEqual(len(queue.published), 1)
        self.assertEqual(int(queue.published[0][1]["entity_id"]), 42)

    async def test_empty_data_marker_skips_publish_and_does_not_update_cursor(self) -> None:
        # D6: a target stamped as "empty" within the endpoint's TTL must be
        # skipped at publish time, exactly like the negative cache path.
        endpoint = SofascoreEndpoint(
            path_template="/api/v1/player/{player_id}/last-year-summary",
            envelope_key="summary,uniqueTournamentsMap",
            target_table="api_payload_snapshot",
            refresh_interval_seconds=24 * 3600,
            scope_kind="test",
            freshness_ttl_seconds=22 * 3600,
            empty_predicate="last_year_summary",
            empty_data_ttl_seconds=14 * 86400,
        )
        target = ResourceTarget(
            entity_type="player",
            entity_id=4747,
            path_params={"player_id": 4747},
        )
        resolver = _FakeResolver([target])
        backend = _FakeRedis()
        empty_store = EmptyDataStore(backend)
        empty_store.mark_empty(
            endpoint_pattern=endpoint.pattern,
            entity_id=4747,
            when_ms=10_000_000 - 60_000,  # marked 60 s ago, well within TTL
        )
        planner, queue, cursor_store = self._build(
            endpoint=endpoint,
            resolver=resolver,
            redis_backend=backend,
            empty_data_store=empty_store,
        )

        published = await planner.tick()

        self.assertEqual(published, 0)
        self.assertEqual(queue.published, [])
        self.assertEqual(
            cursor_store.load_last_refresh_ms(
                endpoint_pattern=endpoint.pattern,
                path_params={"player_id": 4747},
            ),
            0,
        )
        self.assertEqual(planner._stats_empty_skipped, 1)

    async def test_empty_data_marker_does_not_block_other_targets(self) -> None:
        endpoint = SofascoreEndpoint(
            path_template="/api/v1/player/{player_id}/last-year-summary",
            envelope_key="summary,uniqueTournamentsMap",
            target_table="api_payload_snapshot",
            refresh_interval_seconds=24 * 3600,
            scope_kind="test",
            freshness_ttl_seconds=22 * 3600,
            empty_predicate="last_year_summary",
            empty_data_ttl_seconds=14 * 86400,
        )
        active = ResourceTarget(
            entity_type="player",
            entity_id=750,
            path_params={"player_id": 750},
        )
        empty = ResourceTarget(
            entity_type="player",
            entity_id=4747,
            path_params={"player_id": 4747},
        )
        resolver = _FakeResolver([active, empty])
        backend = _FakeRedis()
        empty_store = EmptyDataStore(backend)
        empty_store.mark_empty(
            endpoint_pattern=endpoint.pattern,
            entity_id=4747,
            when_ms=10_000_000 - 60_000,
        )
        planner, queue, _ = self._build(
            endpoint=endpoint,
            resolver=resolver,
            redis_backend=backend,
            empty_data_store=empty_store,
        )

        published = await planner.tick()

        self.assertEqual(published, 1)
        self.assertEqual(int(queue.published[0][1]["entity_id"]), 750)

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
