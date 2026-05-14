"""Periodic sweep that removes finalized events from live polling lanes.

P0.B firebreak (2026-05-14): observability via ``/ops/live-freshness``
revealed ``zset:live:hot`` accumulating entries older than 90 minutes on
production. Without a sweeper events that finalised cleanly still occupy
the polling zsets, so workers burn proxy budget and DB queries polling
already-terminal events.

Contract:

* read ``event_terminal_state`` for events whose ``finalized_at`` is older
  than the configured grace window (default 5 minutes);
* intersect that set with the union of ``zset:live:hot/warm/cold`` members;
* ZREM the matches from each lane.

The grace window protects the late-tail final snapshot capture path —
right after an event finalises we still want one or two polling ticks to
land the closing payload. Anything older than the grace window is
unambiguously stale.

The sweeper is read-mostly (one SELECT + zero-to-three ZRANGEs + at most
three ZREMs). It does not delete the per-event hash ``live:event:{id}``
because that hash is consulted by other read paths and the cleanup of
hash state is intentionally out of scope until we know nothing else
depends on it.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiveStateSweepReport:
    """Outcome of one ``LiveStateSweeper.run_once`` invocation."""

    scanned_finalized_count: int
    removed_event_count: int
    elapsed_ms: int
    lane_removed_counts: dict[str, int]
    error: str | None = None


class LiveStateSweeper:
    """Removes finalised events from ``zset:live:hot/warm/cold``."""

    def __init__(
        self,
        *,
        live_state_store: Any,
        grace_seconds: int = 300,
        max_remove_per_tick: int = 500,
    ) -> None:
        if grace_seconds < 0:
            raise ValueError("grace_seconds must be >= 0")
        if max_remove_per_tick < 1:
            raise ValueError("max_remove_per_tick must be >= 1")
        self.live_state_store = live_state_store
        self.grace_seconds = int(grace_seconds)
        self.max_remove_per_tick = int(max_remove_per_tick)

    async def run_once(
        self,
        *,
        sql_executor: Any,
        now: datetime,
    ) -> LiveStateSweepReport:
        """Execute one sweep cycle. Best-effort: catches all exceptions and
        records them in the report so the planner tick never crashes
        because of a transient DB / Redis hiccup.
        """

        started_perf = time.perf_counter()
        lane_removed_counts = {"hot": 0, "warm": 0, "cold": 0}
        try:
            cutoff = now - timedelta(seconds=self.grace_seconds)
            finalized_ids = await self._fetch_finalized_event_ids(
                sql_executor, cutoff=cutoff
            )
            if not finalized_ids:
                return LiveStateSweepReport(
                    scanned_finalized_count=0,
                    removed_event_count=0,
                    elapsed_ms=_elapsed_ms(started_perf),
                    lane_removed_counts=lane_removed_counts,
                )
            removed_total = 0
            backend = self.live_state_store.backend
            lane_keys = {
                "hot": self.live_state_store.hot_zset_key,
                "warm": self.live_state_store.warm_zset_key,
                "cold": self.live_state_store.cold_zset_key,
            }
            for lane_name, lane_key in lane_keys.items():
                members = _zset_members(backend, lane_key)
                if not members:
                    continue
                to_remove = members & finalized_ids
                if not to_remove:
                    continue
                removed = _zrem_members(backend, lane_key, to_remove)
                lane_removed_counts[lane_name] = removed
                removed_total += removed
            return LiveStateSweepReport(
                scanned_finalized_count=len(finalized_ids),
                removed_event_count=removed_total,
                elapsed_ms=_elapsed_ms(started_perf),
                lane_removed_counts=lane_removed_counts,
            )
        except Exception as exc:  # noqa: BLE001 — best-effort sweep
            logger.warning("live_state_sweep error: %r", exc)
            return LiveStateSweepReport(
                scanned_finalized_count=0,
                removed_event_count=0,
                elapsed_ms=_elapsed_ms(started_perf),
                lane_removed_counts=lane_removed_counts,
                error=repr(exc)[:240],
            )

    async def _fetch_finalized_event_ids(
        self,
        sql_executor: Any,
        *,
        cutoff: datetime,
    ) -> set[int]:
        """Return event ids whose finalized_at is older than ``cutoff``.

        Only fetches the id column. The query is bounded by
        ``max_remove_per_tick`` so a one-off backlog cannot stall the
        sweeper for long.
        """

        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)
        rows = await sql_executor.fetch(
            """
            SELECT event_id
            FROM event_terminal_state
            WHERE terminal_status IS NOT NULL
              AND finalized_at IS NOT NULL
              AND finalized_at < $1
            ORDER BY finalized_at ASC
            LIMIT $2
            """,
            cutoff,
            self.max_remove_per_tick,
        )
        result: set[int] = set()
        for row in rows:
            try:
                result.add(int(_row_field(row, "event_id")))
            except (TypeError, ValueError):
                continue
        return result


def _zset_members(backend: Any, key: str) -> set[int]:
    zrange = getattr(backend, "zrange", None)
    if not callable(zrange):
        return set()
    try:
        raw = zrange(key, 0, -1)
    except Exception:
        return set()
    members: set[int] = set()
    for value in raw or ():
        try:
            members.add(int(value))
        except (TypeError, ValueError):
            continue
    return members


def _zrem_members(backend: Any, key: str, members: set[int]) -> int:
    if not members:
        return 0
    zrem = getattr(backend, "zrem", None)
    if not callable(zrem):
        return 0
    removed = 0
    for event_id in members:
        try:
            if int(zrem(key, str(event_id)) or 0) > 0:
                removed += 1
        except Exception:
            continue
    return removed


def _row_field(row: Any, name: str) -> Any:
    if isinstance(row, dict):
        return row.get(name)
    try:
        return row[name]
    except (KeyError, TypeError, IndexError):
        return None


def _elapsed_ms(started_perf: float) -> int:
    return int((time.perf_counter() - started_perf) * 1000)
