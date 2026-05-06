"""Generic Resource Refresh Planner Daemon.

For every endpoint that opts in via ``SofascoreEndpoint.refresh_interval_seconds``,
this daemon walks the matching ``ScopeResolver`` once per tick and publishes
a ``JOB_REFRESH_RESOURCE`` envelope into ``STREAM_RESOURCE_REFRESH`` for any
target whose cursor is older than ``refresh_interval_seconds``.

Design notes:

* One job type for every leaf endpoint, regardless of family. Endpoint-
  specific knowledge is carried inside ``params``: ``endpoint_pattern``,
  ``path_params``, ``context_*``, ``freshness_key``, ``freshness_ttl_seconds``.
* Cursor key includes the full ``path_params`` mapping, so paginated targets
  (``team/{id}/events/last/{page}``) get distinct cursors per page.
* ``publish_per_tick_cap`` bounds the cold-start spike: if we ever wake up
  with 10k overdue targets we still publish at most N per tick instead of
  flooding the smartproxy pool.
* Backpressure: skip the entire tick if the stream lag is too high. This
  guarantees the planner never wedges hot path streams.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Iterable, Mapping

from ..endpoints import SofascoreEndpoint
from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_REFRESH_RESOURCE
from ..queue.resource_cursor import ResourceCursorStore
from ..queue.resource_negative_cache import ResourceNegativeCache
from ..queue.streams import GROUP_RESOURCE_REFRESH, STREAM_RESOURCE_REFRESH, RedisStreamQueue
from ..workers._stream_jobs import encode_stream_job
from .resource_scope import ResourceTarget, ScopeResolver

logger = logging.getLogger(__name__)


class ResourcePlannerDaemon:
    """Periodic publisher of resource-refresh jobs."""

    def __init__(
        self,
        *,
        queue: RedisStreamQueue,
        cursor_store: ResourceCursorStore,
        endpoints: tuple[SofascoreEndpoint, ...],
        resolvers: Mapping[str, ScopeResolver],
        stream: str = STREAM_RESOURCE_REFRESH,
        group: str = GROUP_RESOURCE_REFRESH,
        tick_interval_seconds: float = 30.0,
        publish_per_tick_cap: int = 20,
        lag_threshold: int = 5000,
        now_ms_factory: Callable[[], int] | None = None,
        negative_cache: ResourceNegativeCache | None = None,
    ) -> None:
        self.queue = queue
        self.cursor_store = cursor_store
        self.endpoints = tuple(
            ep for ep in endpoints if ep.refresh_interval_seconds is not None
        )
        self.resolvers = dict(resolvers)
        self.stream = stream
        self.group = group
        self.tick_interval_seconds = float(tick_interval_seconds)
        self.publish_per_tick_cap = int(publish_per_tick_cap)
        self.lag_threshold = int(lag_threshold)
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.negative_cache = negative_cache
        self.shutdown_requested = False
        self._stats_negative_skipped = 0

    def request_shutdown(self) -> None:
        self.shutdown_requested = True

    async def run_forever(self) -> None:
        while not self.shutdown_requested:
            try:
                await self.tick()
            except Exception:
                logger.exception("Resource planner tick failed; will retry next interval")
            if self.shutdown_requested:
                break
            await asyncio.sleep(self.tick_interval_seconds)

    async def tick(self) -> int:
        if not self.endpoints:
            return 0
        if not self.resolvers:
            logger.debug("Resource planner: no resolvers wired; skipping tick")
            return 0
        if self._is_blocked_by_backpressure():
            logger.info(
                "Resource planner paused by backpressure: stream=%s lag>=%s",
                self.stream,
                self.lag_threshold,
            )
            return 0

        published = 0
        now_ms = int(self.now_ms_factory())
        cap = self.publish_per_tick_cap

        # Build per-endpoint iterables once. Sequentially walking endpoints
        # under a shared cap starves any endpoint that is not first in the
        # list during a cold-start spike (e.g. 50k overdue PLAYER_STATISTICS
        # jobs would block PLAYER_TRANSFER_HISTORY for hours). Round-robin
        # across endpoints keeps every family making forward progress on
        # every tick.
        per_endpoint_streams: list[tuple[SofascoreEndpoint, int, "Iterable[ResourceTarget]"]] = []
        for endpoint in self.endpoints:
            resolver = self.resolvers.get(endpoint.scope_kind or "")
            if resolver is None:
                logger.warning(
                    "Resource planner: no resolver for scope_kind=%r endpoint=%s",
                    endpoint.scope_kind,
                    endpoint.pattern,
                )
                continue
            try:
                targets = await _resolve_targets(resolver)
            except Exception:
                logger.exception(
                    "Resource planner: resolver %s failed; skipping endpoint %s",
                    type(resolver).__name__,
                    endpoint.pattern,
                )
                continue
            interval_ms = int(endpoint.refresh_interval_seconds) * 1000
            per_endpoint_streams.append((endpoint, interval_ms, iter(targets)))

        for endpoint, interval_ms, target in _round_robin(per_endpoint_streams):
            if published >= cap:
                break
            if self._publish_if_due(
                endpoint=endpoint,
                target=target,
                interval_ms=interval_ms,
                now_ms=now_ms,
            ):
                published += 1

        if published:
            logger.info(
                "Resource planner: published=%s stream=%s neg_cache_skipped_total=%s",
                published,
                self.stream,
                self._stats_negative_skipped,
            )
        return published

    # ------------------------------------------------------------------

    def _is_blocked_by_backpressure(self) -> bool:
        """Return True when the consumer-group LAG (unread messages, NOT the
        total stream length) is at or above the threshold.

        Using ``stream_length`` (XLEN) here would be a bug: Redis Streams
        retain ack'd messages until XTRIM/MAXLEN, so XLEN keeps growing
        forever even when consumers are caught up. Backpressure must read
        the unread tail via XINFO GROUPS lag.
        """

        if self.lag_threshold <= 0:
            return False
        try:
            info = self.queue.group_info(self.stream, self.group)
        except Exception as exc:
            logger.warning(
                "Resource planner: group_info probe failed (fail-open): %s",
                exc,
            )
            return False
        if info is None or info.lag is None:
            return False
        return int(info.lag) >= self.lag_threshold

    def _publish_if_due(
        self,
        *,
        endpoint: SofascoreEndpoint,
        target: ResourceTarget,
        interval_ms: int,
        now_ms: int,
    ) -> bool:
        last_ms = self.cursor_store.load_last_refresh_ms(
            endpoint_pattern=endpoint.pattern,
            path_params=target.path_params,
        )
        if last_ms > 0 and (now_ms - last_ms) < interval_ms:
            return False
        # Negative cache: skip targets known to 404 within the configured TTL.
        # We do NOT update the cursor when skipping for this reason -- once the
        # negative TTL expires we want the next tick to retry naturally.
        if self.negative_cache is not None and self.negative_cache.is_negatively_cached(
            endpoint_pattern=endpoint.pattern,
            path_params=target.path_params,
        ):
            self._stats_negative_skipped += 1
            return False
        envelope = _build_envelope(endpoint, target)
        try:
            self.queue.publish(self.stream, encode_stream_job(envelope))
        except Exception:
            logger.exception(
                "Resource planner: publish failed endpoint=%s target=%s",
                endpoint.pattern,
                target.entity_id,
            )
            return False
        self.cursor_store.save_last_refresh_ms(
            endpoint_pattern=endpoint.pattern,
            path_params=target.path_params,
            when_ms=now_ms,
        )
        return True


def _build_envelope(endpoint: SofascoreEndpoint, target: ResourceTarget) -> JobEnvelope:
    """Build a stable JobEnvelope for a (endpoint, target) pair.

    ``params`` carries everything the worker needs to fetch and persist the
    snapshot without consulting any planner state.
    """

    freshness_key = _build_freshness_key(endpoint, target)
    params: dict[str, object] = {
        "endpoint_pattern": endpoint.pattern,
        "path_params": dict(target.path_params),
        "freshness_key": freshness_key,
    }
    if endpoint.freshness_ttl_seconds is not None:
        params["freshness_ttl_seconds"] = int(endpoint.freshness_ttl_seconds)
    if target.context_unique_tournament_id is not None:
        params["context_unique_tournament_id"] = int(target.context_unique_tournament_id)
    if target.context_season_id is not None:
        params["context_season_id"] = int(target.context_season_id)
    if target.context_event_id is not None:
        params["context_event_id"] = int(target.context_event_id)

    return JobEnvelope.create(
        job_type=JOB_REFRESH_RESOURCE,
        sport_slug=target.sport_slug,
        entity_type=target.entity_type,
        entity_id=int(target.entity_id),
        scope="resource_refresh",
        params=params,
        priority=int(endpoint.refresh_priority),
        trace_id=None,
        capability_hint="resource_refresh",
    )


def _build_freshness_key(endpoint: SofascoreEndpoint, target: ResourceTarget) -> str:
    """Stable resource-level freshness key for FreshnessStore.

    Includes path_params so paginated endpoints do not collide.
    """

    import json

    params_repr = json.dumps(
        dict(target.path_params),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"freshness:{endpoint.pattern}|{params_repr}"


async def _resolve_targets(resolver: ScopeResolver) -> Iterable[ResourceTarget]:
    """Adapt the resolver protocol to a single awaited call.

    ``ScopeResolver.resolve`` is declared ``async``; calling it always
    returns a coroutine. Tests can substitute a sync function returning
    a tuple by passing it through a small adapter; the production protocol
    is async.
    """

    return await resolver.resolve()


def _round_robin(
    per_endpoint_streams: list[tuple[SofascoreEndpoint, int, "Iterable[ResourceTarget]"]],
):
    """Interleave one target from each endpoint per round.

    Yields ``(endpoint, interval_ms, target)`` triples. When an endpoint's
    iterator is exhausted it is dropped from the rotation. This guarantees
    every endpoint gets at least ``ceil(cap / N_endpoints)`` publish slots
    per tick instead of being starved by an earlier endpoint with a long
    overdue list.
    """

    active = [(ep, interval, iter(targets)) for ep, interval, targets in per_endpoint_streams]
    while active:
        next_round = []
        for endpoint, interval_ms, iterator in active:
            try:
                target = next(iterator)
            except StopIteration:
                continue
            yield endpoint, interval_ms, target
            next_round.append((endpoint, interval_ms, iterator))
        active = next_round


__all__ = ["ResourcePlannerDaemon"]
