"""Periodic force-hydrate for stuck live events (A2 Phase 0, 2026-05-16).

The discovery → live-dispatch → hydrate pipeline can drop polling for an
event if:
  * upstream `/sport/{slug}/events/live` blinks (proxy flap, CDN miss)
    so live-discovery misses that event on a tick,
  * the live worker quarantines / circuit-breaks an event after a string
    of failures and the quarantine outlives the live span, or
  * bootstrap-hydrate failed and ``live_bootstrap_done_at`` stays NULL,
    so the event is in /events/live but in no live zset.

The housekeeping zombie-sweep eventually finalizes such events with
``terminal_status='zombie_stale'``, which loses the rest of the match.
This rescuer runs *between* those two failure modes — it scans
``/events/live`` (canonical upstream truth) once per ``interval_s`` and
publishes a force_rehydrate live job for any event whose
``live_state.last_ingested_at`` is older than ``stale_minutes`` or whose
live state is missing entirely. A cooldown key in Redis prevents
re-publishing the same event within ``cooldown_seconds`` if the rescue
didn't take effect yet.

Disabled by default (``SOFASCORE_LIVE_RESCUE_ENABLED=true`` to turn on).
Empty live-surface snapshot or missing snapshot table → no-op.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

import orjson

from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_HYDRATE_EVENT_ROOT
from ..live_dispatch_policy import LIVE_TIER_3, resolve_live_dispatch_tier
from ..queue.live_state import LiveEventStateStore
from ..queue.streams import (
    STREAM_LIVE_TIER_1,
    STREAM_LIVE_TIER_2,
    STREAM_LIVE_TIER_3,
)
from ..workers._stream_jobs import encode_stream_job

logger = logging.getLogger(__name__)

_ENV_PREFIX = "SOFASCORE_LIVE_RESCUE_"

_ENV_ENABLED = _ENV_PREFIX + "ENABLED"
_ENV_DRY_RUN = _ENV_PREFIX + "DRY_RUN"
_ENV_INTERVAL_S = _ENV_PREFIX + "INTERVAL_S"
_ENV_STALE_MINUTES = _ENV_PREFIX + "STALE_MINUTES"
_ENV_COOLDOWN_SECONDS = _ENV_PREFIX + "COOLDOWN_SECONDS"
_ENV_MAX_PER_TICK = _ENV_PREFIX + "MAX_PER_TICK"
_ENV_SPORT_SLUGS = _ENV_PREFIX + "SPORT_SLUGS"

_COOLDOWN_KEY_PREFIX = "live:rescue:cooldown:"


@dataclass(frozen=True)
class LiveRescueConfig:
    """Tunables for the live-rescue loop."""

    enabled: bool = False
    dry_run: bool = False
    interval_s: float = 60.0
    stale_minutes: int = 5
    cooldown_seconds: int = 300
    max_rescues_per_tick: int = 20
    sport_slugs: tuple[str, ...] = ("football",)

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "LiveRescueConfig":
        env = env if env is not None else dict(os.environ)
        sport_slugs_raw = (env.get(_ENV_SPORT_SLUGS) or "football").strip()
        sport_slugs: tuple[str, ...]
        if sport_slugs_raw:
            sport_slugs = tuple(
                slug.strip().lower()
                for slug in sport_slugs_raw.split(",")
                if slug and slug.strip()
            )
        else:
            sport_slugs = ("football",)
        return cls(
            enabled=_env_bool(env, _ENV_ENABLED, False),
            dry_run=_env_bool(env, _ENV_DRY_RUN, False),
            interval_s=_env_float(env, _ENV_INTERVAL_S, 60.0),
            stale_minutes=_env_int(env, _ENV_STALE_MINUTES, 5, minimum=1),
            cooldown_seconds=_env_int(env, _ENV_COOLDOWN_SECONDS, 300, minimum=10),
            max_rescues_per_tick=_env_int(env, _ENV_MAX_PER_TICK, 20, minimum=1),
            sport_slugs=sport_slugs or ("football",),
        )


@dataclass
class LiveRescueTickReport:
    """Per-tick summary."""

    scanned: int = 0
    candidates: int = 0
    rescued: int = 0
    skipped_cooldown: int = 0
    no_live_state: int = 0
    stale_polling: int = 0
    finalized: int = 0
    publish_failed: int = 0
    duration_ms: int = 0
    dry_run: bool = False
    per_sport: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class StuckEventCandidate:
    event_id: int
    sport_slug: str
    reason: str  # "no_live_state" | "stale_polling"
    last_ingested_at_ms: int | None
    dispatch_tier: str


class LiveRescueLoop:
    """Scan /events/live each tick and force-hydrate stuck events.

    The loop is *additive* — it only publishes new hydrate jobs, never
    deletes or mutates Redis live-state directly. If a rescue succeeds,
    the regular live worker will update ``last_ingested_at`` and future
    ticks will skip the event.
    """

    def __init__(
        self,
        *,
        config: LiveRescueConfig,
        connection_factory: Callable[[], Any],
        redis_backend: Any,
        live_state_store: LiveEventStateStore,
        stream_queue: Any,
        tier_override_registry: Any = None,
        now_ms_factory: Callable[[], int] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self._connection_factory = connection_factory
        self.redis_backend = redis_backend
        self.live_state_store = live_state_store
        self.stream_queue = stream_queue
        self.tier_override_registry = tier_override_registry
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
            logger.info(
                "live-rescue loop disabled; set %s=true to enable", _ENV_ENABLED
            )
            return
        logger.info(
            "live-rescue loop starting: dry_run=%s interval_s=%.0f stale_min=%d "
            "cooldown_s=%d max_per_tick=%d sports=%s",
            self.config.dry_run,
            self.config.interval_s,
            self.config.stale_minutes,
            self.config.cooldown_seconds,
            self.config.max_rescues_per_tick,
            ",".join(self.config.sport_slugs),
        )
        while not self._shutdown:
            try:
                report = await self.run_once()
                logger.info("live-rescue tick: %s", report_to_log(report))
            except Exception as exc:  # pragma: no cover — never break the loop
                logger.exception("live-rescue tick failed: %s", exc)
            if self._shutdown:
                break
            await self._sleep_interruptible(self.config.interval_s)

    async def run_once(self) -> LiveRescueTickReport:
        started_perf = time.perf_counter()
        report = LiveRescueTickReport(dry_run=self.config.dry_run)
        stale_cutoff_ms = self._now_ms() - self.config.stale_minutes * 60 * 1000

        for sport_slug in self.config.sport_slugs:
            if self._shutdown:
                break
            try:
                live_surface_ids = await self._fetch_live_surface_event_ids(sport_slug)
            except Exception:  # noqa: BLE001 — single sport must not poison others
                logger.exception("live-rescue: fetch_live_surface failed sport=%s", sport_slug)
                continue
            if not live_surface_ids:
                continue
            report.scanned += len(live_surface_ids)
            candidates = self._collect_candidates(
                sport_slug=sport_slug,
                event_ids=live_surface_ids,
                stale_cutoff_ms=stale_cutoff_ms,
                report=report,
            )
            report.candidates += len(candidates)
            if not candidates:
                continue
            rescued_this_sport = 0
            for candidate in candidates:
                if rescued_this_sport + report.rescued >= self.config.max_rescues_per_tick:
                    break
                if self._is_cooldown_active(candidate.event_id):
                    report.skipped_cooldown += 1
                    continue
                if self.config.dry_run:
                    logger.info(
                        "live-rescue dry-run: event_id=%d reason=%s tier=%s last_ingested_at=%s",
                        candidate.event_id,
                        candidate.reason,
                        candidate.dispatch_tier,
                        candidate.last_ingested_at_ms,
                    )
                    rescued_this_sport += 1
                    continue
                try:
                    self._publish_force_hydrate(candidate)
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "live-rescue publish failed: event_id=%d sport=%s",
                        candidate.event_id,
                        sport_slug,
                    )
                    report.publish_failed += 1
                    continue
                self._set_cooldown(candidate.event_id)
                rescued_this_sport += 1
            report.per_sport[sport_slug] = rescued_this_sport
            report.rescued += rescued_this_sport

        report.duration_ms = int((time.perf_counter() - started_perf) * 1000)
        return report

    # ------------------------------------------------------------------
    # Candidate selection
    # ------------------------------------------------------------------

    def _collect_candidates(
        self,
        *,
        sport_slug: str,
        event_ids: set[int],
        stale_cutoff_ms: int,
        report: LiveRescueTickReport,
    ) -> list[StuckEventCandidate]:
        candidates: list[StuckEventCandidate] = []
        for event_id in event_ids:
            state = None
            try:
                state = self.live_state_store.fetch(int(event_id))
            except Exception:  # noqa: BLE001 — fetch failure → treat as missing
                state = None
            if state is None:
                report.no_live_state += 1
                candidates.append(
                    StuckEventCandidate(
                        event_id=int(event_id),
                        sport_slug=sport_slug,
                        reason="no_live_state",
                        last_ingested_at_ms=None,
                        dispatch_tier=self._resolve_tier(sport_slug, None),
                    )
                )
                continue
            if state.is_finalized:
                # Upstream still emits the event in /events/live but our
                # pipeline already finalized it. This is the housekeeping
                # zombie-sweep territory — do NOT re-liven it.
                report.finalized += 1
                continue
            last = state.last_ingested_at
            if last is None or last < stale_cutoff_ms:
                report.stale_polling += 1
                candidates.append(
                    StuckEventCandidate(
                        event_id=int(event_id),
                        sport_slug=sport_slug,
                        reason="stale_polling",
                        last_ingested_at_ms=last,
                        dispatch_tier=self._resolve_tier(
                            sport_slug,
                            state.dispatch_tier,
                        ),
                    )
                )
        return candidates

    def _resolve_tier(self, sport_slug: str, hint: str | None) -> str:
        """Pick a dispatch tier for the rescue job.

        If the live state hash already carries a ``dispatch_tier`` we
        trust it (the live worker last classified the event that way).
        Otherwise we fall through to ``resolve_live_dispatch_tier`` with
        whatever signals we have — usually nothing, so it ends in tier_3,
        which is the safe default (5-min poll, no extra pressure on the
        hot lane).
        """
        normalized = str(hint or "").strip().lower()
        if normalized in {"tier_1", "tier_2", "tier_3"}:
            return normalized
        return resolve_live_dispatch_tier(
            sport_slug=sport_slug,
            detail_id=None,
            tournament_tier=None,
            tournament_user_count=None,
            unique_tournament_id=None,
            tier_override_registry=self.tier_override_registry,
        ) or LIVE_TIER_3

    # ------------------------------------------------------------------
    # Live-surface snapshot
    # ------------------------------------------------------------------

    async def _fetch_live_surface_event_ids(self, sport_slug: str) -> set[int]:
        """Read the latest /events/live snapshot for the sport.

        Returns an empty set if no snapshot exists (sport unsupported or
        endpoint never fetched). The freshness window for the snapshot
        is enforced via ``housekeeping._has_recent_live_surface_success``
        in the legacy retirement path; here we keep the rescuer additive
        — even an hour-old snapshot is enough to identify "event upstream
        believes is live", which is exactly when we want to re-hydrate.
        """
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
            return set()
        decoded = _decode_payload(payload)
        if not isinstance(decoded, dict):
            return set()
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

    # ------------------------------------------------------------------
    # Redis cooldown
    # ------------------------------------------------------------------

    def _cooldown_key(self, event_id: int) -> str:
        return f"{_COOLDOWN_KEY_PREFIX}{int(event_id)}"

    def _is_cooldown_active(self, event_id: int) -> bool:
        if self.redis_backend is None:
            return False
        exists = getattr(self.redis_backend, "exists", None)
        if not callable(exists):
            return False
        try:
            return bool(exists(self._cooldown_key(event_id)))
        except Exception:  # noqa: BLE001 — degrade open
            return False

    def _set_cooldown(self, event_id: int) -> None:
        if self.redis_backend is None:
            return
        setter = getattr(self.redis_backend, "set", None)
        if not callable(setter):
            return
        try:
            setter(
                self._cooldown_key(event_id),
                "1",
                ex=int(self.config.cooldown_seconds),
            )
        except TypeError:
            # Some fakes don't accept the ex= kwarg; fall back to bare set.
            try:
                setter(self._cooldown_key(event_id), "1")
            except Exception:  # noqa: BLE001
                logger.debug("live-rescue cooldown set fallback failed for event_id=%d", event_id)
        except Exception:  # noqa: BLE001
            logger.debug("live-rescue cooldown set failed for event_id=%d", event_id)

    # ------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------

    def _publish_force_hydrate(self, candidate: StuckEventCandidate) -> None:
        params: dict[str, object] = {
            "hydration_mode": "live_delta" if candidate.reason == "stale_polling" else "full",
            "live_bootstrap": candidate.reason == "no_live_state",
            "live_dispatch_tier": candidate.dispatch_tier,
            "force_rehydrate": True,
            "correction_reason": f"live_rescue:{candidate.reason}",
        }
        job = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug=candidate.sport_slug,
            entity_type="event",
            entity_id=int(candidate.event_id),
            scope="live",
            params=params,
            priority=10,
            trace_id=None,
        )
        stream = _stream_for_dispatch_tier(candidate.dispatch_tier)
        self.stream_queue.publish(stream, encode_stream_job(job))

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    async def _sleep_interruptible(self, seconds: float) -> None:
        end_at = time.monotonic() + max(0.0, seconds)
        while not self._shutdown:
            remaining = end_at - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(0.25, remaining))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stream_for_dispatch_tier(dispatch_tier: str | None) -> str:
    normalized = str(dispatch_tier or "").strip().lower()
    if normalized == "tier_1":
        return STREAM_LIVE_TIER_1
    if normalized == "tier_2":
        return STREAM_LIVE_TIER_2
    return STREAM_LIVE_TIER_3


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


def report_to_log(report: LiveRescueTickReport) -> str:
    return (
        f"scanned={report.scanned} candidates={report.candidates} "
        f"rescued={report.rescued} cooldown={report.skipped_cooldown} "
        f"no_state={report.no_live_state} stale={report.stale_polling} "
        f"finalized_skipped={report.finalized} publish_failed={report.publish_failed} "
        f"per_sport={report.per_sport} dry_run={int(report.dry_run)} "
        f"duration_ms={report.duration_ms}"
    )


# ---------------------------------------------------------------------------
# Env parsing helpers (mirrors housekeeping.py — kept local to avoid coupling)
# ---------------------------------------------------------------------------


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


def _env_float(env: dict[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using default %.3f", name, raw, default)
        return default
