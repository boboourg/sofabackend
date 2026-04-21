"""Periodic refresh loop for tournament_registry control-plane rows."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)

TOURNAMENT_REGISTRY_REFRESH_CURSOR_HASH = "hash:etl:tournament_registry_refresh_cursor"


@dataclass(frozen=True)
class TournamentRegistryRefreshResult:
    sport_slug: str
    category_count: int
    category_failures: int
    discovered_unique_tournament_count: int


class TournamentRegistryRefreshCursorStore:
    def __init__(self, backend, *, hash_key: str = TOURNAMENT_REGISTRY_REFRESH_CURSOR_HASH) -> None:
        self.backend = backend
        self.hash_key = hash_key

    def load_last_refresh_ms(self, sport_slug: str) -> int:
        raw = self.backend.hgetall(self.hash_key).get(_cursor_field(sport_slug))
        if raw in (None, ""):
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def save_last_refresh_ms(self, sport_slug: str, refresh_ms: int) -> None:
        field = _cursor_field(sport_slug)
        value = str(int(refresh_ms))
        try:
            self.backend.hset(self.hash_key, mapping={field: value})
        except TypeError:
            self.backend.hset(self.hash_key, {field: value})


async def refresh_tournament_registry_for_sport(
    *,
    category_job,
    sport_slug: str,
    timeout_s: float = 20.0,
) -> TournamentRegistryRefreshResult:
    categories_all_result = await category_job.run_categories_all(
        sport_slug=sport_slug,
        timeout=float(timeout_s),
    )
    category_ids = tuple(int(item) for item in categories_all_result.parsed.category_ids)
    category_failures = 0
    discovered_unique_tournament_ids: set[int] = set()

    for category_id in category_ids:
        try:
            result = await category_job.run_category_unique_tournaments(
                category_id,
                sport_slug=sport_slug,
                timeout=float(timeout_s),
            )
        except Exception as exc:
            category_failures += 1
            logger.warning(
                "Tournament registry refresh failed for sport=%s category_id=%s: %s",
                sport_slug,
                category_id,
                exc,
            )
            continue
        for unique_tournament_id in result.parsed.unique_tournament_ids:
            discovered_unique_tournament_ids.add(int(unique_tournament_id))

    refresh_result = TournamentRegistryRefreshResult(
        sport_slug=str(sport_slug).strip().lower(),
        category_count=len(category_ids),
        category_failures=category_failures,
        discovered_unique_tournament_count=len(discovered_unique_tournament_ids),
    )
    logger.info(
        "Tournament registry refresh completed: sport=%s categories=%s category_failures=%s tournaments=%s",
        refresh_result.sport_slug,
        refresh_result.category_count,
        refresh_result.category_failures,
        refresh_result.discovered_unique_tournament_count,
    )
    return refresh_result


class TournamentRegistryRefreshDaemon:
    def __init__(
        self,
        *,
        cursor_store: TournamentRegistryRefreshCursorStore,
        targets: tuple[str, ...],
        has_registry_rows: Callable[[str], Awaitable[bool] | bool],
        refresh_sport: Callable[[str], Awaitable[object] | object],
        refresh_interval_seconds: float = 86_400.0,
        sports_per_tick: int = 1,
        loop_interval_s: float = 300.0,
        now_ms_factory=None,
    ) -> None:
        self.cursor_store = cursor_store
        self.targets = tuple(str(item).strip().lower() for item in targets if str(item).strip())
        self.has_registry_rows = has_registry_rows
        self.refresh_sport = refresh_sport
        self.refresh_interval_s = float(refresh_interval_seconds)
        self.sports_per_tick = max(1, int(sports_per_tick))
        self.loop_interval_s = float(loop_interval_s)
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
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
        now_ms = int(self.now_ms_factory())
        refresh_interval_ms = int(self.refresh_interval_s * 1000)
        due_targets: list[tuple[int, int, int, str]] = []

        for target_index, sport_slug in enumerate(self.targets):
            try:
                has_rows = bool(await _await_maybe(self.has_registry_rows(sport_slug)))
            except Exception as exc:
                logger.warning(
                    "Tournament registry refresh skipped sport=%s while checking registry state: %s",
                    sport_slug,
                    exc,
                )
                continue
            last_refresh_ms = self.cursor_store.load_last_refresh_ms(sport_slug)
            is_bootstrap = (not has_rows) or last_refresh_ms <= 0
            is_due = is_bootstrap or (now_ms - last_refresh_ms >= refresh_interval_ms)
            if not is_due:
                continue
            due_targets.append((0 if is_bootstrap else 1, last_refresh_ms, target_index, sport_slug))

        if not due_targets:
            return 0

        due_targets.sort(key=lambda item: (item[0], item[1], item[2]))
        refreshed = 0
        refreshed_sports: list[str] = []

        for _, _, _, sport_slug in due_targets[: self.sports_per_tick]:
            try:
                await _await_maybe(self.refresh_sport(sport_slug))
            except Exception as exc:
                logger.warning("Tournament registry refresh failed sport=%s: %s", sport_slug, exc)
                continue
            self.cursor_store.save_last_refresh_ms(sport_slug, now_ms)
            refreshed += 1
            refreshed_sports.append(sport_slug)

        if refreshed:
            logger.info("Tournament registry refresh: refreshed=%s sports=%s", refreshed, ",".join(refreshed_sports))
        return refreshed


async def _await_maybe(value: object) -> object:
    if isinstance(value, Awaitable):
        return await value
    return value


def _cursor_field(sport_slug: str) -> str:
    return f"{str(sport_slug).strip().lower()}:last_refresh_ms"
