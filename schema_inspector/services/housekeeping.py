"""Housekeeping loop: retention sweeps + live-match zombie cleanup.

This module adds a second background loop alongside the existing reclaim
loop in MaintenanceWorker. Every ``interval_s`` seconds it runs a small,
well-defined set of janitor tasks in sequence:

  1. Retention on ``api_request_log`` (batched DELETE older than N hours).
  2. Retention on ``api_payload_snapshot`` (batched DELETE of legacy rows
     with NULL scope_key, older than N days, not referenced by
     ``api_snapshot_head``).
  3. Retention on ``event_live_state_history`` (batched DELETE older than
     N days).
  4. Live-zombie sweep: matches still in ``zset:live:hot`` / ``zset:live:warm``
     whose ``last_ingested_at`` is older than N minutes are removed from
     all lanes, their ``live:event:{id}`` hash is deleted, and
     ``event_terminal_state`` is stamped with ``terminal_status='zombie_stale'``
     via a DO-NOTHING insert (so a real finalization, if any, wins).

Every numeric knob is driven by environment variables so deployments can
tune retention windows without code changes. The canonical env names stay
the source of truth, but ``HousekeepingConfig.from_env()`` also accepts the
operational aliases currently used in deployed ``.env`` files for interval,
batch size, and zombie max age so rollout is backward-compatible.

Dry-run mode (``SOFASCORE_HOUSEKEEPING_DRY_RUN=true``) runs the count-only
queries for the retention tasks and logs the counts without touching data.
The sweeper, in dry-run, logs candidate event ids without performing any
Redis or DB writes.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

import orjson

from ..queue.live_state import (
    LIVE_COLD_ZSET,
    LIVE_HOT_ZSET,
    LIVE_WARM_ZSET,
    LiveEventStateStore,
)
from ..storage.live_state_repository import (
    EventTerminalStateRecord,
    LiveStateRepository,
)
from ..storage.retention_repository import RetentionRepository

logger = logging.getLogger(__name__)

ZOMBIE_TERMINAL_STATUS = "zombie_stale"

_ENV_PREFIX = "SOFASCORE_"

_ENV_ENABLED = _ENV_PREFIX + "HOUSEKEEPING_ENABLED"
_ENV_DRY_RUN = _ENV_PREFIX + "HOUSEKEEPING_DRY_RUN"
_ENV_INTERVAL_S = _ENV_PREFIX + "HOUSEKEEPING_INTERVAL_S"
_ENV_INTERVAL_ALIASES = (_ENV_PREFIX + "HOUSEKEEPING_INTERVAL_SECONDS",)
_ENV_BATCH_SIZE = _ENV_PREFIX + "RETENTION_DELETE_BATCH_SIZE"
_ENV_BATCH_SIZE_ALIASES = (_ENV_PREFIX + "HOUSEKEEPING_BATCH_SIZE",)
_ENV_MAX_BATCHES = _ENV_PREFIX + "RETENTION_MAX_BATCHES_PER_TICK"
_ENV_BATCH_SLEEP_MS = _ENV_PREFIX + "RETENTION_BATCH_SLEEP_MS"
_ENV_REQUEST_LOG_HOURS = _ENV_PREFIX + "RETENTION_REQUEST_LOG_HOURS"
_ENV_SNAPSHOT_DAYS = _ENV_PREFIX + "RETENTION_PAYLOAD_SNAPSHOT_DAYS"
_ENV_LIVE_HISTORY_DAYS = _ENV_PREFIX + "RETENTION_LIVE_HISTORY_DAYS"
_ENV_CAPABILITY_OBS_DAYS = _ENV_PREFIX + "RETENTION_CAPABILITY_OBSERVATION_DAYS"
_ENV_ZOMBIE_MAX_AGE_MIN = _ENV_PREFIX + "SWEEPER_ZOMBIE_MAX_AGE_MINUTES"
_ENV_ZOMBIE_MAX_AGE_ALIASES = (_ENV_PREFIX + "HOUSEKEEPING_ZOMBIE_MAX_AGE_MINUTES",)
_ENV_STALE_LIVE_ENABLED = _ENV_PREFIX + "HOUSEKEEPING_STALE_LIVE_ENABLED"
_ENV_STALE_UPDATED_MIN = _ENV_PREFIX + "HOUSEKEEPING_LIVE_UPDATED_STALE_MINUTES"
_ENV_STALE_BOOTSTRAP_GRACE_MIN = _ENV_PREFIX + "HOUSEKEEPING_BOOTSTRAP_GRACE_MINUTES"
_ENV_STALE_SURFACE_SUCCESS_LOOKBACK_MIN = _ENV_PREFIX + "HOUSEKEEPING_SURFACE_SUCCESS_LOOKBACK_MINUTES"
_ENV_STALE_MAX_RETIREMENTS = _ENV_PREFIX + "HOUSEKEEPING_MAX_STALE_RETIREMENTS_PER_TICK"

LIVE_STATUS_CODES: tuple[int, ...] = (6, 7, 8, 9, 30, 31, 32)


@dataclass(frozen=True)
class HousekeepingConfig:
    """All tunables for the housekeeping loop.

    Defaults are safe for production: retention windows follow the plan
    agreed in Fix #3, batches are small (5k), sleeps between batches give
    Postgres room to breathe, and the loop ticks every 5 minutes.
    """

    enabled: bool = False
    dry_run: bool = False
    interval_s: float = 300.0
    batch_size: int = 5_000
    max_batches_per_tick: int = 20
    batch_sleep_ms: int = 100
    request_log_retention_hours: int = 48
    payload_snapshot_retention_days: int = 7
    live_state_history_retention_days: int = 30
    capability_observation_retention_days: int = 7
    zombie_max_age_minutes: int = 120
    stale_live_enabled: bool = True
    live_updated_stale_minutes: int = 45
    bootstrap_grace_minutes: int = 15
    surface_success_lookback_minutes: int = 15
    max_stale_retirements_per_tick: int = 100

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "HousekeepingConfig":
        env = env if env is not None else dict(os.environ)
        return cls(
            enabled=_env_bool(env, _ENV_ENABLED, False),
            dry_run=_env_bool(env, _ENV_DRY_RUN, False),
            interval_s=_env_float_any(env, (_ENV_INTERVAL_S, *_ENV_INTERVAL_ALIASES), 300.0),
            batch_size=_env_int_any(env, (_ENV_BATCH_SIZE, *_ENV_BATCH_SIZE_ALIASES), 5_000, minimum=1),
            max_batches_per_tick=_env_int(env, _ENV_MAX_BATCHES, 20, minimum=1),
            batch_sleep_ms=_env_int(env, _ENV_BATCH_SLEEP_MS, 100, minimum=0),
            request_log_retention_hours=_env_int(env, _ENV_REQUEST_LOG_HOURS, 48, minimum=1),
            payload_snapshot_retention_days=_env_int(env, _ENV_SNAPSHOT_DAYS, 7, minimum=1),
            live_state_history_retention_days=_env_int(env, _ENV_LIVE_HISTORY_DAYS, 30, minimum=1),
            capability_observation_retention_days=_env_int(env, _ENV_CAPABILITY_OBS_DAYS, 7, minimum=1),
            zombie_max_age_minutes=_env_int_any(
                env,
                (_ENV_ZOMBIE_MAX_AGE_MIN, *_ENV_ZOMBIE_MAX_AGE_ALIASES),
                120,
                minimum=1,
            ),
            stale_live_enabled=_env_bool(env, _ENV_STALE_LIVE_ENABLED, True),
            live_updated_stale_minutes=_env_int(env, _ENV_STALE_UPDATED_MIN, 45, minimum=1),
            bootstrap_grace_minutes=_env_int(env, _ENV_STALE_BOOTSTRAP_GRACE_MIN, 15, minimum=1),
            surface_success_lookback_minutes=_env_int(env, _ENV_STALE_SURFACE_SUCCESS_LOOKBACK_MIN, 15, minimum=1),
            max_stale_retirements_per_tick=_env_int(env, _ENV_STALE_MAX_RETIREMENTS, 100, minimum=1),
        )


@dataclass
class HousekeepingTickReport:
    """Per-tick summary; logged after every loop iteration."""

    request_log_deleted: int = 0
    payload_snapshot_deleted: int = 0
    live_state_history_deleted: int = 0
    capability_observation_deleted: int = 0
    zombies_found: int = 0
    zombies_cleared: int = 0
    stale_live_found: int = 0
    stale_live_cleared: int = 0
    dry_run: bool = False
    # Populated only in dry-run mode — counts of rows that would be deleted.
    would_delete: dict[str, int] = field(default_factory=dict)
    duration_ms: int = 0


@dataclass(frozen=True)
class ZombieCandidate:
    event_id: int
    lane: str
    last_ingested_at: int | None
    sport_slug: str | None = None
    reason: str | None = None


ExecutorFactory = Callable[[], "AsyncExecutorContext"]


class AsyncExecutorContext:
    """Thin shim matching the `async with database.connection()` pattern.

    The housekeeping loop accepts any callable that yields an object with
    asyncpg-compatible ``execute`` / ``fetchval`` methods. The production
    wiring passes ``AsyncpgDatabase.connection`` directly.
    """

    async def __aenter__(self) -> Any:  # pragma: no cover - interface only
        raise NotImplementedError

    async def __aexit__(self, exc_type, exc, tb) -> None:  # pragma: no cover
        raise NotImplementedError


class HousekeepingLoop:
    """Runs retention + sweeper tasks on a fixed interval until shutdown."""

    def __init__(
        self,
        *,
        config: HousekeepingConfig,
        connection_factory: Callable[[], Any],
        retention_repository: RetentionRepository | None = None,
        live_state_store: LiveEventStateStore | None = None,
        live_state_repository: LiveStateRepository | None = None,
        redis_backend: Any | None = None,
        now_ms_factory: Callable[[], int] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self._connection_factory = connection_factory
        self.retention_repository = retention_repository or RetentionRepository()
        self.live_state_store = live_state_store
        self.live_state_repository = live_state_repository or LiveStateRepository()
        self.redis_backend = redis_backend
        self._now_ms = now_ms_factory or (lambda: int(time.time() * 1000))
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._shutdown = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def request_shutdown(self) -> None:
        self._shutdown = True

    async def run_forever(self) -> None:
        if not self.config.enabled:
            logger.info("housekeeping loop disabled; set %s=true to enable", _ENV_ENABLED)
            return
        logger.info(
            "housekeeping loop starting: dry_run=%s interval_s=%.0f batch=%d max_batches=%d",
            self.config.dry_run,
            self.config.interval_s,
            self.config.batch_size,
            self.config.max_batches_per_tick,
        )
        while not self._shutdown:
            try:
                report = await self.run_once()
                logger.info("housekeeping tick: %s", report_to_log(report))
            except Exception as exc:  # pragma: no cover - guard against one bad tick
                logger.exception("housekeeping tick failed: %s", exc)
            if self._shutdown:
                break
            await self._sleep_interruptible(self.config.interval_s)

    async def run_once(self) -> HousekeepingTickReport:
        started_perf = time.perf_counter()
        report = HousekeepingTickReport(dry_run=self.config.dry_run)
        if self.config.dry_run:
            await self._fill_dry_run_counts(report)
        else:
            report.request_log_deleted = await self._run_retention_safely(
                name="api_request_log",
                cutoff=self._cutoff(hours=self.config.request_log_retention_hours),
                delete_batch=self.retention_repository.delete_request_log_batch,
            )
            report.payload_snapshot_deleted = await self._run_retention_safely(
                name="api_payload_snapshot_legacy",
                cutoff=self._cutoff(days=self.config.payload_snapshot_retention_days),
                delete_batch=self.retention_repository.delete_legacy_snapshot_batch,
            )
            report.live_state_history_deleted = await self._run_retention_safely(
                name="event_live_state_history",
                cutoff=self._cutoff(days=self.config.live_state_history_retention_days),
                delete_batch=self.retention_repository.delete_live_state_history_batch,
            )
            report.capability_observation_deleted = await self._run_retention_safely(
                name="endpoint_capability_observation",
                cutoff=self._cutoff(days=self.config.capability_observation_retention_days),
                delete_batch=self.retention_repository.delete_capability_observation_batch,
            )
        await self._run_zombie_sweep(report)
        report.duration_ms = int((time.perf_counter() - started_perf) * 1000)
        return report

    # ------------------------------------------------------------------
    # Retention helpers
    # ------------------------------------------------------------------

    def _cutoff(self, *, hours: int = 0, days: int = 0) -> datetime:
        return self._clock() - timedelta(hours=hours, days=days)

    async def _run_retention(
        self,
        *,
        name: str,
        cutoff: datetime,
        delete_batch: Callable[..., Awaitable[int]],
    ) -> int:
        total_deleted = 0
        for batch_index in range(self.config.max_batches_per_tick):
            if self._shutdown:
                break
            async with self._connection_factory() as connection:
                deleted = await delete_batch(
                    connection,
                    cutoff=cutoff,
                    batch_size=self.config.batch_size,
                )
            total_deleted += int(deleted)
            if deleted < self.config.batch_size:
                # Exhausted — nothing more to clean this tick.
                logger.debug(
                    "retention %s exhausted after batch=%d deleted=%d total=%d",
                    name,
                    batch_index,
                    deleted,
                    total_deleted,
                )
                break
            await asyncio.sleep(self.config.batch_sleep_ms / 1000.0)
        if total_deleted:
            logger.info("retention %s deleted=%d cutoff=%s", name, total_deleted, cutoff.isoformat())
        return total_deleted

    async def _run_retention_safely(
        self,
        *,
        name: str,
        cutoff: datetime,
        delete_batch: Callable[..., Awaitable[int]],
    ) -> int:
        try:
            return await self._run_retention(name=name, cutoff=cutoff, delete_batch=delete_batch)
        except Exception:
            logger.exception("retention step failed: %s", name)
            return 0

    async def _fill_dry_run_counts(self, report: HousekeepingTickReport) -> None:
        request_log_cutoff = self._cutoff(hours=self.config.request_log_retention_hours)
        snapshot_cutoff = self._cutoff(days=self.config.payload_snapshot_retention_days)
        history_cutoff = self._cutoff(days=self.config.live_state_history_retention_days)
        capability_cutoff = self._cutoff(days=self.config.capability_observation_retention_days)
        async with self._connection_factory() as connection:
            report.would_delete = {
                "api_request_log": await self.retention_repository.count_expired_request_logs(
                    connection, cutoff=request_log_cutoff
                ),
                "api_payload_snapshot_legacy": await self.retention_repository.count_expired_legacy_snapshots(
                    connection, cutoff=snapshot_cutoff
                ),
                "event_live_state_history": await self.retention_repository.count_expired_live_state_history(
                    connection, cutoff=history_cutoff
                ),
                "endpoint_capability_observation": await self.retention_repository.count_expired_capability_observations(
                    connection, cutoff=capability_cutoff
                ),
            }

    # ------------------------------------------------------------------
    # Zombie sweep
    # ------------------------------------------------------------------

    async def _run_zombie_sweep(self, report: HousekeepingTickReport) -> None:
        if self.redis_backend is None:
            return
        lane_candidates = self._collect_zombie_candidates() if self.live_state_store is not None else []
        stale_live_candidates = await self._collect_stale_live_candidates()
        report.zombies_found = len(lane_candidates)
        report.stale_live_found = len(stale_live_candidates)
        candidates = self._merge_candidates(lane_candidates, stale_live_candidates)
        if not candidates:
            return
        if self.config.dry_run:
            logger.info(
                "zombie sweep dry-run candidates=%d lane=%d stale_live=%d (first 10: %s)",
                len(candidates),
                len(lane_candidates),
                len(stale_live_candidates),
                [c.event_id for c in candidates[:10]],
            )
            return
        now_iso = self._clock().isoformat()
        zombie_cleared = 0
        stale_live_cleared = 0
        for candidate in candidates:
            try:
                await self._retire_zombie(candidate, finalized_at_iso=now_iso)
                if candidate.reason == "surface_missing":
                    stale_live_cleared += 1
                else:
                    zombie_cleared += 1
            except Exception:  # pragma: no cover - best effort, continue sweep
                logger.exception("zombie sweep failed for event_id=%s", candidate.event_id)
        report.zombies_cleared = zombie_cleared
        report.stale_live_cleared = stale_live_cleared
        logger.info(
            "zombie sweep cleared=%d/%d stale_live=%d/%d found=%d",
            zombie_cleared,
            len(lane_candidates),
            stale_live_cleared,
            len(stale_live_candidates),
            len(candidates),
        )

    def _collect_zombie_candidates(self) -> list[ZombieCandidate]:
        assert self.live_state_store is not None
        cutoff_ms = self._now_ms() - self.config.zombie_max_age_minutes * 60 * 1000
        candidates: list[ZombieCandidate] = []
        seen: set[int] = set()
        for lane_name, lane_key in (("hot", LIVE_HOT_ZSET), ("warm", LIVE_WARM_ZSET)):
            members = self._zrange_all(lane_key)
            for raw in members:
                try:
                    event_id = int(raw)
                except (TypeError, ValueError):
                    continue
                if event_id in seen:
                    continue
                seen.add(event_id)
                state = self.live_state_store.fetch(event_id)
                if state is None:
                    # Orphan zset entry: lane has id but no hash. That's
                    # itself a zombie — clear it.
                    candidates.append(
                        ZombieCandidate(
                            event_id=event_id,
                            lane=lane_name,
                            last_ingested_at=None,
                            reason="orphan_live_state",
                        )
                    )
                    continue
                if state.is_finalized:
                    # Should never be in a live lane, but the finalize worker
                    # is the one who removes it — if it's here, let sweeper help.
                    candidates.append(
                        ZombieCandidate(event_id=event_id, lane=lane_name, last_ingested_at=state.last_ingested_at)
                    )
                    continue
                last = state.last_ingested_at
                if last is None or last < cutoff_ms:
                    candidates.append(
                        ZombieCandidate(
                            event_id=event_id,
                            lane=lane_name,
                            last_ingested_at=last,
                            reason="redis_live_zombie",
                        )
                    )
        return candidates

    async def _collect_stale_live_candidates(self) -> list[ZombieCandidate]:
        if not self.config.stale_live_enabled:
            return []
        rows = await self._fetch_stale_live_rows()
        if not rows:
            return []
        candidates: list[ZombieCandidate] = []
        rows_by_sport: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            sport_slug = str(row.get("sport_slug") or "").strip().lower()
            if not sport_slug:
                continue
            rows_by_sport.setdefault(sport_slug, []).append(row)
        for sport_slug, sport_rows in rows_by_sport.items():
            if not await self._has_recent_live_surface_success(sport_slug):
                logger.warning(
                    "stale-live sweep skipped sport=%s: no recent successful /events/live fetch",
                    sport_slug,
                )
                continue
            live_surface_ids = await self._fetch_live_surface_event_ids(sport_slug)
            if live_surface_ids is None:
                logger.warning(
                    "stale-live sweep skipped sport=%s: no recent live surface snapshot available",
                    sport_slug,
                )
                continue
            for row in sport_rows:
                event_id = int(row["event_id"])
                if event_id in live_surface_ids:
                    continue
                candidates.append(
                    ZombieCandidate(
                        event_id=event_id,
                        lane="stale_live",
                        last_ingested_at=None,
                        sport_slug=sport_slug,
                        reason="surface_missing",
                    )
                )
                if len(candidates) >= self.config.max_stale_retirements_per_tick:
                    return candidates
        return candidates

    async def _fetch_stale_live_rows(self) -> list[dict[str, Any]]:
        updated_cutoff = self._clock() - timedelta(minutes=self.config.live_updated_stale_minutes)
        bootstrap_cutoff = self._clock() - timedelta(minutes=self.config.bootstrap_grace_minutes)
        fetch_limit = max(
            self.config.max_stale_retirements_per_tick * 4,
            self.config.max_stale_retirements_per_tick,
        )
        async with self._connection_factory() as connection:
            rows = await connection.fetch(
                """
                SELECT
                    e.id AS event_id,
                    s.slug AS sport_slug,
                    e.updated_at,
                    e.live_bootstrap_done_at
                FROM event AS e
                JOIN tournament AS t
                    ON t.id = e.tournament_id
                JOIN category AS cat
                    ON cat.id = t.category_id
                JOIN sport AS s
                    ON s.id = cat.sport_id
                WHERE e.status_code = ANY($1::int[])
                  AND (
                    e.updated_at < $2
                    OR (
                        e.live_bootstrap_done_at IS NULL
                        AND e.updated_at < $3
                    )
                  )
                ORDER BY e.updated_at NULLS FIRST, e.id
                LIMIT $4
                """,
                list(LIVE_STATUS_CODES),
                updated_cutoff,
                bootstrap_cutoff,
                fetch_limit,
            )
        return [dict(row) for row in rows]

    async def _has_recent_live_surface_success(self, sport_slug: str) -> bool:
        cutoff = self._clock() - timedelta(minutes=self.config.surface_success_lookback_minutes)
        async with self._connection_factory() as connection:
            value = await connection.fetchval(
                """
                SELECT 1
                FROM api_payload_snapshot
                WHERE endpoint_pattern = $1
                  AND fetched_at >= $2
                ORDER BY fetched_at DESC
                LIMIT 1
                """,
                f"/api/v1/sport/{sport_slug}/events/live",
                cutoff,
            )
        return value is not None

    async def _fetch_live_surface_event_ids(self, sport_slug: str) -> set[int] | None:
        async with self._connection_factory() as connection:
            payload = await connection.fetchval(
                """
                SELECT payload
                FROM api_payload_snapshot
                WHERE endpoint_pattern = $1
                ORDER BY id DESC
                LIMIT 1
                """,
                f"/api/v1/sport/{sport_slug}/events/live",
            )
        if payload in (None, ""):
            return None
        decoded = _decode_payload(payload)
        if not isinstance(decoded, dict):
            return None
        raw_events = decoded.get("events")
        if not isinstance(raw_events, list):
            return set()
        event_ids: set[int] = set()
        for item in raw_events:
            if not isinstance(item, dict):
                continue
            raw_id = item.get("id")
            try:
                event_ids.add(int(raw_id))
            except (TypeError, ValueError):
                continue
        return event_ids

    def _merge_candidates(
        self,
        lane_candidates: list[ZombieCandidate],
        stale_live_candidates: list[ZombieCandidate],
    ) -> list[ZombieCandidate]:
        merged: list[ZombieCandidate] = []
        seen: set[int] = set()
        for candidate in (*lane_candidates, *stale_live_candidates):
            if candidate.event_id in seen:
                continue
            seen.add(candidate.event_id)
            merged.append(candidate)
        return merged

    def _zrange_all(self, lane_key: str) -> tuple[Any, ...]:
        """Read every member of a lane's zset. These zsets hold ~thousands of
        ids at most (hot/warm polling cohorts), so a single ZRANGE is fine.
        """

        assert self.redis_backend is not None
        try:
            raw = self.redis_backend.zrange(lane_key, 0, -1)
        except TypeError:
            # Some fakes use keyword-style; tolerate that.
            raw = self.redis_backend.zrange(lane_key, start=0, end=-1)
        return tuple(raw or ())

    async def _retire_zombie(self, candidate: ZombieCandidate, *, finalized_at_iso: str) -> None:
        assert self.redis_backend is not None
        event_key = f"live:event:{candidate.event_id}"
        member = str(candidate.event_id)
        # Remove from all three lanes — belt and braces.
        for lane_key in (LIVE_HOT_ZSET, LIVE_WARM_ZSET, LIVE_COLD_ZSET):
            self.redis_backend.zrem(lane_key, member)
        # Drop the live:event:{id} hash so no stale state lingers.
        self.redis_backend.delete(event_key)
        # Record a terminal state but only if one does not already exist.
        async with self._connection_factory() as connection:
            await self.live_state_repository.insert_terminal_state_if_missing(
                connection,
                EventTerminalStateRecord(
                    event_id=candidate.event_id,
                    terminal_status=ZOMBIE_TERMINAL_STATUS,
                    finalized_at=finalized_at_iso,
                    final_snapshot_id=None,
                ),
            )
            await self.live_state_repository.mark_event_stale_live_retired(
                connection,
                event_id=candidate.event_id,
                retired_at=finalized_at_iso,
            )

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    async def _sleep_interruptible(self, seconds: float) -> None:
        # Slice the sleep so request_shutdown takes effect within ~250 ms
        # instead of waiting the full interval.
        end_at = time.monotonic() + max(0.0, seconds)
        while not self._shutdown:
            remaining = end_at - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(0.25, remaining))


def report_to_log(report: HousekeepingTickReport) -> str:
    """Format a tick report for human-readable logging."""

    if report.dry_run:
        would = report.would_delete or {}
        return (
            f"dry_run would_delete "
            f"request_log={would.get('api_request_log', 0)} "
            f"snapshot={would.get('api_payload_snapshot_legacy', 0)} "
            f"live_history={would.get('event_live_state_history', 0)} "
            f"zombies_found={report.zombies_found} "
            f"stale_live_found={report.stale_live_found} "
            f"duration_ms={report.duration_ms}"
        )
    return (
        f"deleted request_log={report.request_log_deleted} "
        f"snapshot={report.payload_snapshot_deleted} "
        f"live_history={report.live_state_history_deleted} "
        f"zombies={report.zombies_cleared}/{report.zombies_found} "
        f"stale_live={report.stale_live_cleared}/{report.stale_live_found} "
        f"duration_ms={report.duration_ms}"
    )


def _decode_payload(payload: Any) -> Any:
    if payload is None:
        return None
    if isinstance(payload, (bytes, bytearray, memoryview)):
        try:
            return orjson.loads(bytes(payload))
        except orjson.JSONDecodeError:
            return None
    if isinstance(payload, str):
        try:
            return orjson.loads(payload)
        except orjson.JSONDecodeError:
            return None
    return payload


# ---------------------------------------------------------------------------
# Env parsing helpers
# ---------------------------------------------------------------------------


def _env_first(env: dict[str, str], names: tuple[str, ...]) -> tuple[str, str] | None:
    for name in names:
        raw = env.get(name)
        if raw is not None:
            return name, raw
    return None


def _env_bool(env: dict[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(env: dict[str, str], name: str, default: int, *, minimum: int = 0) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using default %d", name, raw, default)
        return default
    if value < minimum:
        logger.warning("%s must be >= %d; using default %d", name, minimum, default)
        return default
    return value


def _env_int_any(env: dict[str, str], names: tuple[str, ...], default: int, *, minimum: int = 0) -> int:
    selected = _env_first(env, names)
    if selected is None:
        return default
    name, raw = selected
    if raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using default %d", name, raw, default)
        return default
    if value < minimum:
        logger.warning("%s must be >= %d; using default %d", name, minimum, default)
        return default
    return value


def _env_float(env: dict[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using default %.3f", name, raw, default)
        return default


def _env_float_any(env: dict[str, str], names: tuple[str, ...], default: float) -> float:
    selected = _env_first(env, names)
    if selected is None:
        return default
    name, raw = selected
    if raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using default %.3f", name, raw, default)
        return default
