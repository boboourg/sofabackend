"""Structural-sync planner.

Owns the cadence of the skeleton-only tournament/season sync contour.

Per tick, for each managed (sport_slug, unique_tournament_id) target:
    1. Read last-refresh timestamp from Redis.
    2. If never synced -> publish a bootstrap job (priority=30).
    3. Else, if enough time has elapsed since last refresh -> publish refresh job.
    4. After publish, write the current timestamp back to Redis.

Never touches live/historical/discovery queues — publishes only to
``STREAM_STRUCTURE_SYNC``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Awaitable
from dataclasses import dataclass, field

from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_SYNC_TOURNAMENT_STRUCTURE
from ..queue.streams import STREAM_STRUCTURE_SYNC
from ..sport_profiles import SportProfile, resolve_sport_profile
from ..storage.tournament_registry_repository import TournamentRegistryTarget
from ..workers._stream_jobs import encode_stream_job

STRUCTURE_SYNC_CURSOR_HASH = "hash:etl:structure_sync_cursor"
MANAGED_TOURNAMENTS_ENV_KEY = "SCHEMA_INSPECTOR_STRUCTURE_MANAGED_TOURNAMENTS"
MANAGED_TOURNAMENTS_FILE_ENV_KEY = "SCHEMA_INSPECTOR_STRUCTURE_MANAGED_FILE"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StructurePlanningTarget:
    sport_slug: str
    unique_tournament_id: int
    refresh_interval_seconds: float
    bootstrap_priority: int = 30
    refresh_priority: int = 50


class StructureCursorStore:
    def __init__(self, backend, *, hash_key: str = STRUCTURE_SYNC_CURSOR_HASH) -> None:
        self.backend = backend
        self.hash_key = hash_key

    def load_last_refresh_ms(self, sport_slug: str, unique_tournament_id: int) -> int:
        field_name = _cursor_field(sport_slug, unique_tournament_id)
        raw = self.backend.hgetall(self.hash_key).get(field_name)
        if raw in (None, ""):
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def save_last_refresh_ms(self, sport_slug: str, unique_tournament_id: int, when_ms: int) -> None:
        field_name = _cursor_field(sport_slug, unique_tournament_id)
        value = str(int(when_ms))
        try:
            self.backend.hset(self.hash_key, mapping={field_name: value})
        except TypeError:
            self.backend.hset(self.hash_key, {field_name: value})


def load_managed_tournaments(
    *,
    env: dict[str, str] | None = None,
    sport_slugs: tuple[str, ...] | None = None,
    registry_targets: tuple[TournamentRegistryTarget, ...] | None = None,
) -> tuple[StructurePlanningTarget, ...]:
    """Build structural-sync targets from (in order of precedence):

    1. Tournament registry rows keyed by sport/source.
    2. ``SCHEMA_INSPECTOR_STRUCTURE_MANAGED_TOURNAMENTS`` env JSON (mapping
       sport_slug -> list[int]).
    3. ``SCHEMA_INSPECTOR_STRUCTURE_MANAGED_FILE`` pointing at a JSON file
       with the same shape.
    4. Each sport's ``SportProfile.managed_unique_tournament_ids``.

    Precedence is per-sport: registry rows outrank env/file overrides, and
    env/file overrides outrank profile defaults.
    """

    resolved_env = env if env is not None else dict(os.environ)
    allowed_sports = set(sport_slugs or ())
    registry_by_sport: dict[str, list[int]] = {}
    for target in registry_targets or ():
        sport_slug = str(target.sport_slug).strip().lower()
        registry_by_sport.setdefault(sport_slug, []).append(int(target.unique_tournament_id))

    env_overrides = _parse_managed_json(resolved_env.get(MANAGED_TOURNAMENTS_ENV_KEY))
    file_path = resolved_env.get(MANAGED_TOURNAMENTS_FILE_ENV_KEY, "").strip()
    file_overrides: dict[str, list[int]] = {}
    if file_path:
        try:
            with open(file_path, "r", encoding="utf-8") as handle:
                file_overrides = _parse_managed_json(handle.read())
        except OSError as exc:
            logger.warning("structure planner: cannot read managed tournaments file %s: %s", file_path, exc)

    targets: list[StructurePlanningTarget] = []
    # Build a discriminated set of sport slugs: union of overrides + explicit arg.
    discovered_sports = set(registry_by_sport) | set(env_overrides) | set(file_overrides) | allowed_sports

    for sport_slug in sorted(discovered_sports):
        if allowed_sports and sport_slug not in allowed_sports:
            continue
        profile = resolve_sport_profile(sport_slug)
        if profile.structure_sync_mode == "disabled":
            continue
        if registry_by_sport.get(sport_slug):
            ids = _merge_ids(registry_by_sport[sport_slug])
        elif env_overrides.get(sport_slug) or file_overrides.get(sport_slug):
            ids = _merge_ids(
                env_overrides.get(sport_slug, []),
                file_overrides.get(sport_slug, []),
            )
        else:
            ids = _merge_ids(list(profile.managed_unique_tournament_ids))
        for unique_tournament_id in ids:
            targets.append(
                StructurePlanningTarget(
                    sport_slug=sport_slug,
                    unique_tournament_id=int(unique_tournament_id),
                    refresh_interval_seconds=float(profile.structure_refresh_interval_seconds),
                )
            )
    return tuple(targets)


class StructurePlannerDaemon:
    def __init__(
        self,
        *,
        queue,
        cursor_store: StructureCursorStore,
        targets: tuple[StructurePlanningTarget, ...],
        stream: str = STREAM_STRUCTURE_SYNC,
        loop_interval_s: float = 30.0,
        backpressure=None,
        now_ms_factory=None,
        target_loader=None,
    ) -> None:
        self.queue = queue
        self.cursor_store = cursor_store
        self._static_targets = tuple(targets)
        self.stream = stream
        self.loop_interval_s = float(loop_interval_s)
        self.backpressure = backpressure
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self._target_loader = target_loader
        self.shutdown_requested = False

    def request_shutdown(self) -> None:
        self.shutdown_requested = True

    async def run_forever(self) -> None:
        while not self.shutdown_requested:
            await self.tick()
            if self.shutdown_requested:
                break
            await asyncio.sleep(self.loop_interval_s)

    async def tick(self) -> int:
        if self.backpressure is not None:
            reason = self.backpressure.blocking_reason()
            if reason:
                logger.info("Structure planner paused by backpressure: %s", reason)
                return 0

        targets = await _maybe_await(self._target_loader()) if self._target_loader else self._static_targets
        published = 0
        now_ms = int(self.now_ms_factory())

        for target in targets:
            profile = resolve_sport_profile(target.sport_slug)
            if profile.structure_sync_mode == "disabled":
                continue
            last_ms = self.cursor_store.load_last_refresh_ms(
                target.sport_slug, target.unique_tournament_id
            )
            interval_ms = int(float(target.refresh_interval_seconds) * 1000)

            if last_ms <= 0:
                envelope = _build_envelope(target, profile, mode="bootstrap", priority=target.bootstrap_priority)
            elif now_ms - last_ms >= interval_ms:
                envelope = _build_envelope(target, profile, mode="refresh", priority=target.refresh_priority)
            else:
                continue

            self.queue.publish(self.stream, encode_stream_job(envelope))
            self.cursor_store.save_last_refresh_ms(
                target.sport_slug, target.unique_tournament_id, now_ms
            )
            published += 1

        if published:
            logger.info("Structure planner: published=%s", published)
        return published


def _build_envelope(
    target: StructurePlanningTarget,
    profile: SportProfile,
    *,
    mode: str,
    priority: int,
) -> JobEnvelope:
    return JobEnvelope.create(
        job_type=JOB_SYNC_TOURNAMENT_STRUCTURE,
        sport_slug=target.sport_slug,
        entity_type="unique_tournament",
        entity_id=int(target.unique_tournament_id),
        scope="structure",
        params={
            "phase": mode,
            "structure_mode": profile.structure_sync_mode,
        },
        priority=priority,
        trace_id=None,
        capability_hint="structure",
    )


def _parse_managed_json(raw: str | None) -> dict[str, list[int]]:
    if raw is None:
        return {}
    text = str(raw).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except ValueError as exc:
        logger.warning("structure planner: invalid managed JSON: %s", exc)
        return {}
    if not isinstance(parsed, dict):
        logger.warning("structure planner: managed JSON must be an object, got %r", type(parsed).__name__)
        return {}
    output: dict[str, list[int]] = {}
    for sport_slug, ids in parsed.items():
        if not isinstance(ids, (list, tuple)):
            continue
        cleaned: list[int] = []
        for item in ids:
            try:
                cleaned.append(int(item))
            except (TypeError, ValueError):
                continue
        output[str(sport_slug).strip().lower()] = cleaned
    return output


def _merge_ids(*sources: list[int]) -> list[int]:
    seen: set[int] = set()
    merged: list[int] = []
    for source in sources:
        for item in source:
            try:
                value = int(item)
            except (TypeError, ValueError):
                continue
            if value in seen:
                continue
            seen.add(value)
            merged.append(value)
    return merged


def _cursor_field(sport_slug: str, unique_tournament_id: int) -> str:
    return f"{str(sport_slug).strip().lower()}:{int(unique_tournament_id)}:last_refresh_ms"


async def _maybe_await(value):
    if isinstance(value, Awaitable):
        return await value
    return value


__all__ = [
    "StructureCursorStore",
    "StructurePlannerDaemon",
    "StructurePlanningTarget",
    "load_managed_tournaments",
    "STRUCTURE_SYNC_CURSOR_HASH",
    "MANAGED_TOURNAMENTS_ENV_KEY",
    "MANAGED_TOURNAMENTS_FILE_ENV_KEY",
]
