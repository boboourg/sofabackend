"""Historical planner with rolling horizons and persistent cursor support."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date as Date
from datetime import timedelta

from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_DISCOVER_SPORT_SURFACE
from ..queue.streams import STREAM_HISTORICAL_DISCOVERY
from ..storage.planner_cursor_repository import PlannerCursorRepository
from ..workers._stream_jobs import encode_stream_job

HISTORICAL_CURSOR_HASH = "hash:etl:historical_cursor"
HISTORICAL_PLANNER_NAME = "historical_planner"
logger = logging.getLogger(__name__)


def choose_recent_history_window(sport_slug: str) -> int:
    normalized = str(sport_slug).strip().lower()
    if normalized in {"football", "basketball", "ice-hockey", "baseball"}:
        return 730
    return 180


def default_archive_years(sport_slug: str) -> int:
    normalized = str(sport_slug).strip().lower()
    if normalized in {"football", "basketball", "ice-hockey", "baseball"}:
        return 30
    return 15


@dataclass(frozen=True)
class HistoricalHorizon:
    deep_from: Date
    deep_to: Date
    recent_from: Date
    recent_to: Date


def compute_historical_horizon(
    *,
    today: Date,
    sport_slug: str,
    start_override: Date | None = None,
    end_override: Date | None = None,
    recent_refresh_days: int | None = None,
) -> HistoricalHorizon:
    archive_years = default_archive_years(sport_slug)
    recent_days = max(1, int(recent_refresh_days or choose_recent_history_window(sport_slug)))
    bounded_end = min(end_override or today, today)
    recent_from = today - timedelta(days=recent_days)
    deep_from = start_override or Date(today.year - archive_years, 1, 1)
    deep_to = min(bounded_end, recent_from - timedelta(days=1))
    return HistoricalHorizon(
        deep_from=deep_from,
        deep_to=deep_to,
        recent_from=recent_from,
        recent_to=bounded_end,
    )


@dataclass(frozen=True)
class HistoricalPlanningTarget:
    sport_slug: str
    date_from: str
    date_to: str
    priority: int = 50
    source_slug: str = "sofascore"
    scope: str = "historical"
    repeat_from_start: bool = False
    scope_id: int | None = None


def build_historical_planning_targets(
    *,
    source_slug: str,
    sport_slug: str,
    horizon: HistoricalHorizon,
    priority: int = 50,
) -> tuple[HistoricalPlanningTarget, ...]:
    targets: list[HistoricalPlanningTarget] = []
    if horizon.deep_from <= horizon.deep_to:
        targets.append(
            HistoricalPlanningTarget(
                source_slug=source_slug,
                sport_slug=sport_slug,
                scope="historical_deep",
                date_from=horizon.deep_from.isoformat(),
                date_to=horizon.deep_to.isoformat(),
                priority=priority,
            )
        )
    if horizon.recent_from <= horizon.recent_to:
        targets.append(
            HistoricalPlanningTarget(
                source_slug=source_slug,
                sport_slug=sport_slug,
                scope="historical_recent_refresh",
                date_from=horizon.recent_from.isoformat(),
                date_to=horizon.recent_to.isoformat(),
                priority=priority,
                repeat_from_start=True,
            )
        )
    return tuple(targets)


@dataclass(frozen=True)
class HistoricalSaturationBudget:
    player_limit: int
    team_limit: int
    player_request_limit: int
    team_request_limit: int


def choose_saturation_budget(sport_slug: str) -> HistoricalSaturationBudget:
    normalized = str(sport_slug).strip().lower()
    if normalized in {"football", "basketball", "ice-hockey", "baseball"}:
        return HistoricalSaturationBudget(
            player_limit=400,
            team_limit=128,
            player_request_limit=400,
            team_request_limit=128,
        )
    return HistoricalSaturationBudget(
        player_limit=120,
        team_limit=48,
        player_request_limit=120,
        team_request_limit=48,
    )


def choose_event_detail_budget(sport_slug: str) -> int:
    normalized = str(sport_slug).strip().lower()
    if normalized in {"football", "basketball", "ice-hockey", "baseball"}:
        return 500
    return 180


class HistoricalCursorStore:
    """Legacy Redis-backed cursor store kept as a lightweight fallback."""

    def __init__(self, backend, *, hash_key: str = HISTORICAL_CURSOR_HASH) -> None:
        self.backend = backend
        self.hash_key = hash_key

    def load_next_date(self, sport_slug: str, date_from: str, date_to: str) -> str | None:
        raw = self.backend.hgetall(self.hash_key).get(_cursor_field(sport_slug, date_from, date_to))
        if raw in (None, ""):
            return None
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="ignore")
        return str(raw)

    def save_next_date(self, sport_slug: str, date_from: str, date_to: str, next_date: str) -> None:
        field = _cursor_field(sport_slug, date_from, date_to)
        try:
            self.backend.hset(self.hash_key, mapping={field: next_date})
        except TypeError:
            self.backend.hset(self.hash_key, {field: next_date})


class PostgresHistoricalCursorStore:
    def __init__(
        self,
        *,
        repository: PlannerCursorRepository,
        connection_factory: Callable[[], object],
        planner_name: str = HISTORICAL_PLANNER_NAME,
        default_source_slug: str = "sofascore",
    ) -> None:
        self.repository = repository
        self.connection_factory = connection_factory
        self.planner_name = str(planner_name).strip().lower()
        self.default_source_slug = str(default_source_slug).strip().lower()

    async def load_next_date(self, target: HistoricalPlanningTarget) -> str | None:
        async with self.connection_factory() as connection:
            record = await self.repository.load_cursor(
                connection,
                planner_name=self.planner_name,
                source_slug=_target_source_slug(target, self.default_source_slug),
                sport_slug=target.sport_slug,
                scope_type=target.scope,
                scope_id=target.scope_id,
            )
        if record is None or record.cursor_date is None:
            return None
        return record.cursor_date.isoformat()

    async def save_next_date(self, target: HistoricalPlanningTarget, next_date: str) -> None:
        async with self.connection_factory() as connection:
            await self.repository.upsert_cursor(
                connection,
                planner_name=self.planner_name,
                source_slug=_target_source_slug(target, self.default_source_slug),
                sport_slug=target.sport_slug,
                scope_type=target.scope,
                scope_id=target.scope_id,
                cursor_date=Date.fromisoformat(next_date),
                cursor_id=None,
            )


class HistoricalPlannerDaemon:
    def __init__(
        self,
        *,
        queue,
        cursor_store,
        targets: tuple[HistoricalPlanningTarget, ...] = (),
        target_loader: Callable[[], Awaitable[tuple[HistoricalPlanningTarget, ...]]] | None = None,
        stream: str = STREAM_HISTORICAL_DISCOVERY,
        dates_per_tick: int = 1,
        loop_interval_s: float = 5.0,
        backpressure=None,
    ) -> None:
        self.queue = queue
        self.cursor_store = cursor_store
        self.targets = tuple(targets)
        self.target_loader = target_loader
        self.stream = stream
        self.dates_per_tick = max(1, int(dates_per_tick))
        self.loop_interval_s = float(loop_interval_s)
        self.backpressure = backpressure
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
                logger.info("Historical planner paused by backpressure: %s", reason)
                return 0
        targets = await self._load_targets()
        published = 0
        for target in targets:
            current = await self._resolve_current_date(target)
            if current is None:
                continue
            for _ in range(self.dates_per_tick):
                if Date.fromisoformat(current) > Date.fromisoformat(target.date_to):
                    break
                job = JobEnvelope.create(
                    job_type=JOB_DISCOVER_SPORT_SURFACE,
                    sport_slug=target.sport_slug,
                    entity_type="sport",
                    entity_id=None,
                    scope=target.scope,
                    params={"date": current},
                    priority=target.priority,
                    trace_id=None,
                )
                self.queue.publish(self.stream, encode_stream_job(job))
                published += 1
                current = _advance_date(current)
                if target.repeat_from_start and Date.fromisoformat(current) > Date.fromisoformat(target.date_to):
                    current = target.date_from
                await self._save_next_date(target, current)
        return published

    async def _load_targets(self) -> tuple[HistoricalPlanningTarget, ...]:
        if self.target_loader is None:
            return self.targets
        return tuple(await self.target_loader())

    async def _resolve_current_date(self, target: HistoricalPlanningTarget) -> str | None:
        current = await self._load_next_date(target)
        if not current:
            return target.date_from
        if Date.fromisoformat(current) < Date.fromisoformat(target.date_from):
            return target.date_from
        if Date.fromisoformat(current) > Date.fromisoformat(target.date_to):
            if target.repeat_from_start:
                return target.date_from
            return None
        return current

    async def _load_next_date(self, target: HistoricalPlanningTarget) -> str | None:
        method = self.cursor_store.load_next_date
        try:
            return await _await_maybe(method(target))
        except TypeError as exc:
            try:
                return await _await_maybe(method(target.sport_slug, target.date_from, target.date_to))
            except TypeError:
                raise exc

    async def _save_next_date(self, target: HistoricalPlanningTarget, next_date: str) -> None:
        method = self.cursor_store.save_next_date
        try:
            await _await_maybe(method(target, next_date))
        except TypeError as exc:
            try:
                await _await_maybe(method(target.sport_slug, target.date_from, target.date_to, next_date))
            except TypeError:
                raise exc


async def _await_maybe(value: object) -> object:
    if isinstance(value, Awaitable):
        return await value
    return value


def _advance_date(value: str) -> str:
    observed = Date.fromisoformat(value)
    return Date.fromordinal(observed.toordinal() + 1).isoformat()


def _cursor_field(sport_slug: str, date_from: str, date_to: str) -> str:
    return f"{str(sport_slug).strip().lower()}:{date_from}:{date_to}"


def _target_source_slug(target: HistoricalPlanningTarget, default_source_slug: str) -> str:
    return str(target.source_slug or default_source_slug).strip().lower()
