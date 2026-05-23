"""Historical tournament planner for season/tournament archival work."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from pathlib import Path

from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_SYNC_TOURNAMENT_ARCHIVE
from ..queue.streams import STREAM_HISTORICAL_TOURNAMENT
from ..workers._stream_jobs import encode_stream_job
from .backfill_priority_config import (
    BackfillPriorityConfig,
    ConfigValidationError,
)

HISTORICAL_TOURNAMENT_CURSOR_HASH = "hash:etl:historical_tournament_cursor"
TournamentSelector = Callable[..., Awaitable[tuple[int, ...]] | tuple[int, ...]]
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HistoricalTournamentPlanningTarget:
    sport_slug: str
    allowed_unique_tournament_ids: tuple[int, ...] = ()
    priority: int = 40


class HistoricalTournamentCursorStore:
    def __init__(self, backend, *, hash_key: str = HISTORICAL_TOURNAMENT_CURSOR_HASH) -> None:
        self.backend = backend
        self.hash_key = hash_key

    def load_last_unique_tournament_id(self, sport_slug: str) -> int:
        raw = self.backend.hgetall(self.hash_key).get(_cursor_field(sport_slug))
        if raw in (None, ""):
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def save_last_unique_tournament_id(self, sport_slug: str, unique_tournament_id: int) -> None:
        field = _cursor_field(sport_slug)
        value = str(int(unique_tournament_id))
        try:
            self.backend.hset(self.hash_key, mapping={field: value})
        except TypeError:
            self.backend.hset(self.hash_key, {field: value})


class HistoricalTournamentPlannerDaemon:
    def __init__(
        self,
        *,
        queue,
        cursor_store: HistoricalTournamentCursorStore,
        selector: TournamentSelector,
        targets: tuple[HistoricalTournamentPlanningTarget, ...],
        stream: str = STREAM_HISTORICAL_TOURNAMENT,
        tournaments_per_tick: int = 10,
        loop_interval_s: float = 10.0,
        backpressure=None,
        target_loader=None,
        backfill_cursor_selector: Callable[..., Awaitable[list[dict]]] | None = None,
        priority_config: BackfillPriorityConfig | None = None,
        # Phase 3.7(a) (2026-05-22): bootstrap surface for the
        # pending catalog rows. When provided, the planner publishes
        # an extra throttled batch of (UT, season) jobs taken from
        # the upstream-seasons catalog where bootstrap_state='pending'.
        # Workers see these jobs, look up the catalog state, and
        # dispatch in bootstrap_mode=True (lightweight event-list
        # only). Without this surface the catalog long-tail would
        # only drain via cursor advance — one UT at a time, days
        # to drain 19k pending rows.
        bootstrap_pending_selector: Callable[..., Awaitable[list[dict]]] | None = None,
        max_bootstrap_jobs_per_tick: int = 20,
        bootstrap_stream: str = STREAM_HISTORICAL_TOURNAMENT,
    ) -> None:
        self.queue = queue
        self.cursor_store = cursor_store
        self.selector = selector
        self._static_targets = tuple(targets)
        self.targets = self._static_targets
        self.stream = stream
        self.bootstrap_stream = bootstrap_stream
        self.tournaments_per_tick = max(1, int(tournaments_per_tick))
        self.loop_interval_s = float(loop_interval_s)
        self.backpressure = backpressure
        self._target_loader = target_loader
        # Phase 1 (2026-05-16): when provided, the planner pulls pending
        # (UT, season) pairs from the cursor selector first and publishes
        # per-season jobs. When the cursor selector returns nothing it
        # falls back to the legacy UT-only selector path so the daemon
        # keeps working before bootstrap and during gradual rollouts.
        self.backfill_cursor_selector = backfill_cursor_selector
        # C.3 (2026-05-18): operator-tunable per-sport / per-UT weights.
        # ``None`` keeps the pre-config behaviour (uniform target rotation).
        # See docs/BACKFILL_PRIORITIES.md.
        self.priority_config = priority_config
        # Phase 3.7(a) (2026-05-22): bootstrap surface (see __init__ doc).
        self.bootstrap_pending_selector = bootstrap_pending_selector
        self.max_bootstrap_jobs_per_tick = max(0, int(max_bootstrap_jobs_per_tick))
        self.shutdown_requested = False

    def request_shutdown(self) -> None:
        self.shutdown_requested = True

    def reload_priority_config(self, path: "Path | str") -> None:
        """Reload backfill priorities from ``path``.

        Safety rails:
          * ConfigValidationError (malformed YAML / bad value) is caught
            and the previous-good config is kept. A WARNING is logged so
            ``journalctl`` shows the operator typo.
          * Missing file resets the config to defaults (empty
            ``sport_weights``) — this lets an operator "remove the
            override" by deleting the file.
        """
        path = Path(path)
        try:
            new_config = BackfillPriorityConfig.load(path)
        except ConfigValidationError as exc:
            logger.warning(
                "reload_priority_config: keeping previous config — %s", exc,
            )
            return
        self.priority_config = new_config
        logger.info(
            "reload_priority_config: applied %s (sports=%d, ut_boost=%d)",
            path, len(new_config.sport_weights), len(new_config.ut_boost),
        )

    async def run_forever(self) -> None:
        while not self.shutdown_requested:
            await self.tick()
            if self.shutdown_requested:
                break
            await asyncio.sleep(self.loop_interval_s)

    def _compute_per_target_limits(
        self,
        targets,
    ) -> dict[str, int] | None:
        """Slice ``tournaments_per_tick`` across sport targets according
        to ``priority_config.sport_weights``. Returns ``None`` when no
        config is in effect — callers fall back to the legacy
        per-target full-budget behaviour.

        Sports with weight=0 (or missing from the map) get budget=0 and
        are filtered out by the caller. The rounding rule:
          * compute exact share = budget * weight / total_weight
          * floor each share to int
          * distribute the floor-remainder across the highest-share
            sports so the sum of integer slices == budget
        This guarantees every non-zero sport gets at least 1 slot when
        budget >= count(eligible_sports).
        """
        cfg = self.priority_config
        if cfg is None:
            return None

        weighted = []
        for target in targets:
            w = float(cfg.sport_weights.get(target.sport_slug, 0.0))
            if w > 0.0:
                weighted.append((target.sport_slug, w))
        if not weighted:
            return {t.sport_slug: 0 for t in targets}
        total_weight = sum(w for _, w in weighted)
        budget = self.tournaments_per_tick

        # Largest-remainder method (Hare quota) — stable distribution.
        exact_shares = [(s, budget * w / total_weight) for s, w in weighted]
        floor_shares = [(s, int(share), share - int(share)) for s, share in exact_shares]
        result = {s: floor for s, floor, _ in floor_shares}
        remainder = budget - sum(result.values())
        if remainder > 0:
            # hand out the remainder to the highest fractional parts.
            floor_shares.sort(key=lambda item: item[2], reverse=True)
            for s, _, _ in floor_shares[:remainder]:
                result[s] += 1
        # Fill paused sports with explicit 0.
        for target in targets:
            result.setdefault(target.sport_slug, 0)
        return result

    async def tick(self) -> int:
        # Phase 3.7(a) (2026-05-22): selective backpressure — cursor
        # publishes pause when enrichment lag is high (they generate
        # match-center fan-out + entities), but bootstrap publishes
        # keep going (bootstrap-mode workers skip enrichment children,
        # so they add zero downstream load). Without this split the
        # planner self-deadlocks on enrichment lag: cursor paused →
        # no advance → no events_loaded → no catalog drain → cursor
        # stays paused.
        cursor_paused_reason: str | None = None
        if self.backpressure is not None:
            cursor_paused_reason = self.backpressure.blocking_reason()
            if cursor_paused_reason:
                logger.info(
                    "Cursor publishes paused by backpressure: %s "
                    "(bootstrap publishes continue)",
                    cursor_paused_reason,
                )
        targets = await _await_maybe(self._target_loader()) if self._target_loader else self._static_targets
        # C.3: per-target slice of tournaments_per_tick when a
        # priority config is active. Targets with sport_weight=0 (or
        # missing) are skipped entirely. ut_boost is honoured downstream
        # by sorting fetched cursor_rows by weight_for_ut DESC.
        per_target_limit = self._compute_per_target_limits(targets)
        if per_target_limit is not None:
            # Drop sports with budget=0 from the rotation.
            targets = tuple(t for t in targets if per_target_limit.get(t.sport_slug, 0) > 0)
        published = 0
        for target in targets:
            # When cursor publishes are backpressure-paused, skip
            # the cursor + legacy walks but still try bootstrap
            # (it generates zero enrichment load — see Phase 3.7(a)
            # worker fix that skips JOB_ENRICH_TOURNAMENT_* children
            # for bootstrap_mode jobs).
            if cursor_paused_reason is not None:
                bootstrap_published, bootstrap_selector_returned = await self._publish_bootstrap_batch(target)
                published += bootstrap_published
                logger.info(
                    "bootstrap_tick: sport=%s published=%d selector_returned=%d cursor_paused=%s",
                    target.sport_slug,
                    bootstrap_published,
                    bootstrap_selector_returned,
                    cursor_paused_reason is not None,
                )
                continue

            # Phase 1 (2026-05-16): cursor-aware path first. The selector
            # returns rows like {unique_tournament_id, next_season_backfill_id,
            # priority_rank} ordered by priority. When it yields anything we
            # publish per-(UT, season) jobs and skip the legacy walk for
            # this target.
            if self.backfill_cursor_selector is not None:
                effective_limit = (
                    per_target_limit[target.sport_slug]
                    if per_target_limit is not None
                    else self.tournaments_per_tick
                )
                cursor_rows = await _await_maybe(
                    self.backfill_cursor_selector(
                        sport_slug=target.sport_slug,
                        limit=effective_limit,
                    )
                )
                if cursor_rows:
                    if self.priority_config is not None:
                        # ut_boost: sort rows so the boosted UTs publish
                        # first. weight_for_ut returns sport_weight *
                        # ut_multiplier (or just sport_weight when no
                        # boost) — sorting by it descending puts boosted
                        # rows at the head.
                        cursor_rows = sorted(
                            cursor_rows,
                            key=lambda r: self.priority_config.weight_for_ut(
                                ut_id=int(r["unique_tournament_id"]),
                                sport_slug=target.sport_slug,
                            ),
                            reverse=True,
                        )
                    for row in cursor_rows:
                        ut_id = int(row["unique_tournament_id"])
                        season_id = int(row["next_season_backfill_id"])
                        job = JobEnvelope.create(
                            job_type=JOB_SYNC_TOURNAMENT_ARCHIVE,
                            sport_slug=target.sport_slug,
                            entity_type="unique_tournament",
                            entity_id=ut_id,
                            scope="historical",
                            params={"target_season_id": season_id},
                            priority=target.priority,
                            trace_id=None,
                        )
                        self.queue.publish(self.stream, encode_stream_job(job))
                        published += 1
                    # Phase 3.7(a): bootstrap publishes alongside the
                    # cursor jobs. Both happen on the same tick — cursor
                    # advances current heads, bootstrap drains the
                    # long-tail pending catalog rows in parallel.
                    bootstrap_published, bootstrap_selector_returned = await self._publish_bootstrap_batch(target)
                    published += bootstrap_published
                    logger.info(
                        "bootstrap_tick: sport=%s published=%d selector_returned=%d cursor_paused=%s",
                        target.sport_slug,
                        bootstrap_published,
                        bootstrap_selector_returned,
                        cursor_paused_reason is not None,
                    )
                    continue

            # Legacy path — no cursor data yet, fall back to UT-only walk.
            after_unique_tournament_id = self.cursor_store.load_last_unique_tournament_id(target.sport_slug)
            selector_kwargs = {
                "sport_slug": target.sport_slug,
                "after_unique_tournament_id": after_unique_tournament_id,
                "limit": self.tournaments_per_tick,
            }
            if target.allowed_unique_tournament_ids:
                selector_kwargs["allowed_unique_tournament_ids"] = tuple(
                    int(item) for item in target.allowed_unique_tournament_ids
                )
            selected_ids = await _await_maybe(
                self.selector(**selector_kwargs)
            )
            if not selected_ids:
                # Phase 3.7(a): legacy selector empty — still try
                # bootstrap (catalog may have pending rows even
                # when tournament_registry walk is exhausted).
                bootstrap_published, bootstrap_selector_returned = await self._publish_bootstrap_batch(target)
                published += bootstrap_published
                logger.info(
                    "bootstrap_tick: sport=%s published=%d selector_returned=%d cursor_paused=%s",
                    target.sport_slug,
                    bootstrap_published,
                    bootstrap_selector_returned,
                    cursor_paused_reason is not None,
                )
                continue
            for unique_tournament_id in selected_ids:
                job = JobEnvelope.create(
                    job_type=JOB_SYNC_TOURNAMENT_ARCHIVE,
                    sport_slug=target.sport_slug,
                    entity_type="unique_tournament",
                    entity_id=int(unique_tournament_id),
                    scope="historical",
                    params={},
                    priority=target.priority,
                    trace_id=None,
                )
                self.queue.publish(self.stream, encode_stream_job(job))
                published += 1
            self.cursor_store.save_last_unique_tournament_id(target.sport_slug, int(selected_ids[-1]))
            # Phase 3.7(a): bootstrap publishes also run on the legacy
            # path so the catalog long tail drains even when cursor
            # rows are empty (e.g. registry not yet seeded).
            bootstrap_published, bootstrap_selector_returned = await self._publish_bootstrap_batch(target)
            published += bootstrap_published
            logger.info(
                "bootstrap_tick: sport=%s published=%d selector_returned=%d cursor_paused=%s",
                target.sport_slug,
                bootstrap_published,
                bootstrap_selector_returned,
                cursor_paused_reason is not None,
            )
        return published

    async def _publish_bootstrap_batch(self, target) -> tuple[int, int]:
        """Phase 3.7(a) (2026-05-22): publish a throttled batch of
        bootstrap jobs for ``pending`` catalog rows.

        Returns (published_count, selector_returned_count) so the caller
        can emit a structured per-tick log.

        No-op when:
          * ``bootstrap_pending_selector`` was not configured
            (backwards-compatible default).
          * ``max_bootstrap_jobs_per_tick == 0`` (operator dial-down).
          * Selector returns no rows (catalog drained for this sport).
        """

        if self.bootstrap_pending_selector is None:
            return 0, 0
        limit = int(self.max_bootstrap_jobs_per_tick)
        if limit <= 0:
            return 0, 0
        rows = await _await_maybe(
            self.bootstrap_pending_selector(
                sport_slug=target.sport_slug, limit=limit
            )
        )
        if not rows:
            return 0, 0
        published = 0
        for row in rows[:limit]:
            ut_id = int(row["unique_tournament_id"])
            season_id = int(row["season_id"])
            job = JobEnvelope.create(
                job_type=JOB_SYNC_TOURNAMENT_ARCHIVE,
                sport_slug=target.sport_slug,
                entity_type="unique_tournament",
                entity_id=ut_id,
                scope="historical",
                params={"target_season_id": season_id},
                priority=target.priority,
                trace_id=None,
            )
            self.queue.publish(self.bootstrap_stream, encode_stream_job(job))
            logger.debug(
                "bootstrap_publish: sport=%s ut=%s season=%s stream=%s",
                target.sport_slug, ut_id, season_id, self.bootstrap_stream,
            )
            published += 1
        return published, len(rows)


async def _await_maybe(value: object) -> object:
    if isinstance(value, Awaitable):
        return await value
    return value


def _cursor_field(sport_slug: str) -> str:
    return f"{str(sport_slug).strip().lower()}:last_unique_tournament_id"
