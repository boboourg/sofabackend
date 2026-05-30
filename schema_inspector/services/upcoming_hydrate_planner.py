"""Upcoming pre-match hydrate planner daemon (2026-05-30).

Fills the gap surfaced by the WC2026 trace: NOTSTARTED future matches are
never per-event hydrated. The operational planner (``sofascore-planner``)
discovers only ``/scheduled-events/{today}`` and otherwise refreshes only
*live* events; the historical-enrichment lane is clamped to ``end = now`` and
is intentionally stopped on prod. So once a season's fixtures are discovered,
nobody hydrates them until kickoff day — pre-match endpoints (managers, h2h,
pregame-form, team-streaks, lineups, odds) never populate ahead of time.

This daemon periodically scans for events that are notstarted
(``status_code = 0``) and kick off within a horizon window
(``now <= start_timestamp <= now + horizon_days``), then publishes one
``JOB_HYDRATE_EVENT_ROOT`` per event with ``scope="prematch"`` and
``hydration_mode="full"``. That mode + a NON-live scope makes
``pilot_orchestrator.run_event`` fetch the full pre-match matrix:

  * root ``/event/{id}``, ``/incidents``, ``/lineups`` (confirmed=false until
    ~1h before kickoff),
  * the cold pre-match bundle: ``/managers``, ``/h2h``, ``/h2h/events``,
    ``/pregame-form``, ``/team-streaks``,
  * odds/votes/winning-odds — gated automatically by ``match_center_policy``
    to the 24h pre-match hot window, so they only start fetching as kickoff
    nears (no wasted calls weeks out),

while live/finished-only edges (statistics, graph, best-players, player
stats/heatmap, highlights) stay correctly gated off for a notstarted event.

It reuses ``stream:etl:hydrate`` + the existing ``HydrateWorker`` fleet — no
new worker pool — exactly like ``FinalSyncPlannerDaemon``. It is bounded by a
per-tick publish cap AND consumer-group lag backpressure on ``cg:hydrate`` so
a cold-start sweep can neither flood the proxy pool nor breach the live SLO.
``season_id`` optionally scopes the daemon to a single competition (used for
the WC2026 test run); ``None`` means all football. The worker-side
``HydrateInFlightStore`` (300s TTL) de-dupes concurrent publishes for free, so
re-publishing an event already in flight is a no-op.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Protocol

from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_HYDRATE_EVENT_ROOT
from ..queue.streams import GROUP_HYDRATE, STREAM_HYDRATE
from ..workers._stream_jobs import encode_stream_job

logger = logging.getLogger(__name__)

# Non-live scope: keeps resolve_live_hydration_mode from downgrading "full"
# to a live-delta variant (it only rewrites live scopes), and is distinct
# from FINAL_SYNC_SCOPE so the terminal-freeze lock-stamp never fires here.
UPCOMING_HYDRATE_SCOPE = "prematch"


class _QueueLike(Protocol):
    def publish(self, stream: str, payload: Any) -> Any: ...

    def group_info(self, stream: str, group: str) -> Any: ...


class _RepoLike(Protocol):
    async def upcoming_notstarted_event_ids(
        self,
        executor: Any,
        *,
        horizon_seconds: int,
        limit: int,
        season_id: int | None = None,
    ) -> list[int]: ...


class _DatabaseLike(Protocol):
    def connection(self) -> Any: ...


@dataclass
class UpcomingHydratePlannerDaemon:
    """Periodic planner publishing pre-match hydrate jobs for future fixtures.

    Parameters
    ----------
    database
        Exposes ``connection()`` async context manager yielding an
        asyncpg-compatible executor.
    queue
        Redis-Streams facade with ``publish(stream, payload)`` and
        ``group_info(stream, group)`` (for lag backpressure).
    repository
        Exposes ``upcoming_notstarted_event_ids(...)``.
    horizon_days
        Look-ahead window: only events with
        ``now <= start_timestamp <= now + horizon_days`` are hydrated.
        Soonest kickoffs first. Keep small for general use; raise for a
        scoped (``season_id``) backfill of a whole future tournament.
    publish_per_tick_cap
        Hard cap on jobs published per tick — prevents a cold-start sweep
        from flooding the shared hydrate fleet / proxy pool.
    batch_size
        SQL ``LIMIT`` on the candidate query (>= cap).
    lag_threshold
        If ``cg:hydrate`` consumer-group lag is at/above this, skip the tick
        entirely — protects the live SLO (hydrate workers are shared).
        ``<= 0`` disables the check.
    tick_interval_seconds
        Sleep between ticks. Pre-match is not latency-sensitive.
    priority
        Job priority — below live (8) and below final-sync (4) so it never
        crowds latency-sensitive lanes; above historical (1).
    season_id
        Optional single-season scope (e.g. WC2026 = 58210). ``None`` = all
        football.
    """

    database: _DatabaseLike
    queue: _QueueLike
    repository: _RepoLike
    horizon_days: float = 2.0
    publish_per_tick_cap: int = 50
    batch_size: int = 500
    lag_threshold: int = 5000
    tick_interval_seconds: int = 60
    priority: int = 3
    season_id: int | None = None
    default_sport_slug: str = "football"
    stream: str = STREAM_HYDRATE
    group: str = GROUP_HYDRATE

    @property
    def horizon_seconds(self) -> int:
        return int(float(self.horizon_days) * 86400)

    def _is_blocked_by_backpressure(self) -> bool:
        """True when ``cg:hydrate`` LAG (unread tail, not XLEN) >= threshold.

        Fail-open: any probe error returns False so a transient Redis hiccup
        cannot wedge the planner. Reading lag (not stream length) is required
        because Streams retain ack'd entries — XLEN grows forever.
        """
        if self.lag_threshold <= 0:
            return False
        try:
            info = self.queue.group_info(self.stream, self.group)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "upcoming_hydrate_planner: group_info probe failed (fail-open): %s",
                exc,
            )
            return False
        if info is None or getattr(info, "lag", None) is None:
            return False
        return int(info.lag) >= self.lag_threshold

    async def tick(self, *, now_ms: int = 0) -> int:
        """One sweep: pick due upcoming events, publish hydrate jobs, return count."""
        del now_ms  # window is computed in SQL via now(); reserved for jitter
        if self._is_blocked_by_backpressure():
            logger.info(
                "upcoming_hydrate_planner: paused by backpressure stream=%s lag>=%s",
                self.stream,
                self.lag_threshold,
            )
            return 0

        async with self.database.connection() as connection:
            event_ids = await self.repository.upcoming_notstarted_event_ids(
                connection,
                horizon_seconds=self.horizon_seconds,
                limit=self.batch_size,
                season_id=self.season_id,
            )

        published = 0
        for event_id in event_ids:
            if published >= self.publish_per_tick_cap:
                logger.info(
                    "upcoming_hydrate_planner: per-tick cap %d reached (%d candidates this sweep)",
                    self.publish_per_tick_cap,
                    len(event_ids),
                )
                break
            envelope = JobEnvelope.create(
                job_type=JOB_HYDRATE_EVENT_ROOT,
                sport_slug=self.default_sport_slug,
                entity_type="event",
                entity_id=int(event_id),
                scope=UPCOMING_HYDRATE_SCOPE,
                params={"hydration_mode": "full"},
                priority=self.priority,
                trace_id=None,
            )
            try:
                self.queue.publish(self.stream, encode_stream_job(envelope))
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "upcoming_hydrate_planner: publish failed event_id=%s: %r",
                    event_id,
                    exc,
                )
                continue
            published += 1
            logger.info(
                "upcoming_hydrate_planner: published event_id=%s job_id=%s",
                event_id,
                envelope.job_id,
            )

        return published

    async def run_forever(self) -> None:
        logger.info(
            "upcoming_hydrate_planner: starting (horizon=%.1fd cap=%d batch=%d "
            "tick=%ds lag_threshold=%d season_id=%s)",
            self.horizon_days,
            self.publish_per_tick_cap,
            self.batch_size,
            self.tick_interval_seconds,
            self.lag_threshold,
            self.season_id,
        )
        while True:
            try:
                published = await self.tick()
            except Exception as exc:  # noqa: BLE001
                logger.exception("upcoming_hydrate_planner: tick failed: %r", exc)
                published = 0
            if published == 0:
                await asyncio.sleep(self.tick_interval_seconds)
            else:
                await asyncio.sleep(max(1, self.tick_interval_seconds // 6))
