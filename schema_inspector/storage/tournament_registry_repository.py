"""PostgreSQL repository for tournament registry control-plane rows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Protocol

from ._temporal import coerce_timestamptz


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


class SqlBatchExecutor(SqlExecutor, Protocol):
    async def executemany(self, query: str, args: Iterable[tuple[object, ...]]) -> Any: ...


class SqlFetchExecutor(SqlExecutor, Protocol):
    async def fetch(self, query: str, *args: object) -> list[object]: ...


# Phase 2.A (2026-05-22): cursor advance now walks the upstream-seasons
# catalog (Stage 1 / 2026-05-19 walked the ``event`` table; that path
# silently jumped over upstream seasons we hadn't yet ingested events
# for, producing false ``backfill_completed_at`` stamps for cup-style
# tournaments with archive gaps — see commit message for full
# write-up).
#
# Exported as a module-level constant so the Phase 2.A unit test
# (test_phase_2a_upstream_catalog.py) can assert the predicate shape
# without spinning up Postgres — a single source of truth.
_ADVANCE_BACKFILL_CURSOR_SQL = """
    WITH next_season AS (
        SELECT tsuc.season_id
        FROM tournament_season_upstream_catalog tsuc
        WHERE tsuc.unique_tournament_id = $2
          AND tsuc.bootstrap_state <> 'fully_processed'
          AND tsuc.season_id <> $3
        ORDER BY tsuc.upstream_position ASC NULLS LAST,
                 tsuc.season_id DESC
        LIMIT 1
    )
    UPDATE tournament_registry tr
    SET next_season_backfill_id = COALESCE(
            (SELECT season_id FROM next_season),
            0
        ),
        backfill_last_advance_at = now(),
        backfill_completed_at = CASE
            WHEN (SELECT season_id FROM next_season) IS NULL
                THEN now()
            ELSE tr.backfill_completed_at
        END,
        updated_at = now()
    WHERE tr.sport_slug = $1
      AND tr.unique_tournament_id = $2
    RETURNING tr.next_season_backfill_id
"""


def _rows_affected(execute_result: Any) -> int:
    """asyncpg.execute() returns a CommandTag string like ``UPDATE 12``.
    Parse the trailing integer when possible — used by the bootstrap CLI
    to report how many cursors were seeded. Returns 0 when the executor
    returns something else (older fakes / mocks)."""
    if not isinstance(execute_result, str):
        return 0
    parts = execute_result.strip().split()
    if not parts:
        return 0
    try:
        return int(parts[-1])
    except ValueError:
        return 0


@dataclass(frozen=True)
class TournamentRegistryRecord:
    source_slug: str
    sport_slug: str
    category_id: int
    unique_tournament_id: int
    discovery_surface: str
    priority_rank: int
    is_active: bool
    first_seen_at: str | None = None
    last_seen_at: str | None = None


@dataclass(frozen=True)
class TournamentRegistryTarget:
    source_slug: str
    sport_slug: str
    unique_tournament_id: int
    structure_enabled: bool = True
    current_enabled: bool = True
    live_enabled: bool = True
    historical_enabled: bool = True
    enrichment_enabled: bool = True
    refresh_interval_seconds: int | None = None
    historical_backfill_start_date: date | None = None
    historical_backfill_end_date: date | None = None
    recent_refresh_days: int | None = None


@dataclass(frozen=True)
class HistoricalPlanningPolicy:
    source_slug: str
    sport_slug: str
    historical_backfill_start_date: date | None = None
    historical_backfill_end_date: date | None = None
    recent_refresh_days: int | None = None


class TournamentRegistryRepository:
    """Writes and reads registry-backed managed tournament targets."""

    _SURFACE_PREDICATES = {
        "structure": "structure_enabled = TRUE",
        "current": "current_enabled = TRUE",
        "live": "live_enabled = TRUE",
        "historical": "historical_enabled = TRUE",
        "enrichment": "enrichment_enabled = TRUE",
    }

    async def upsert_records(
        self,
        executor: SqlExecutor,
        records: Iterable[TournamentRegistryRecord],
    ) -> None:
        rows = [
            (
                record.source_slug,
                record.sport_slug,
                int(record.category_id),
                int(record.unique_tournament_id),
                record.discovery_surface,
                int(record.priority_rank),
                bool(record.is_active),
                coerce_timestamptz(record.first_seen_at),
                coerce_timestamptz(record.last_seen_at),
            )
            for record in records
        ]
        if not rows:
            return
        query = """
            INSERT INTO tournament_registry (
                source_slug,
                sport_slug,
                category_id,
                unique_tournament_id,
                discovery_surface,
                priority_rank,
                is_active,
                first_seen_at,
                last_seen_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                COALESCE($8, now()),
                COALESCE($9, now())
            )
            ON CONFLICT (source_slug, sport_slug, unique_tournament_id) DO UPDATE SET
                category_id = EXCLUDED.category_id,
                discovery_surface = EXCLUDED.discovery_surface,
                priority_rank = EXCLUDED.priority_rank,
                is_active = EXCLUDED.is_active,
                last_seen_at = EXCLUDED.last_seen_at,
                updated_at = now()
        """
        executemany = getattr(executor, "executemany", None)
        if callable(executemany):
            await executemany(query, rows)
            return
        for row in rows:
            await executor.execute(query, *row)

    async def list_active_targets(
        self,
        executor: SqlFetchExecutor,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        surface: str | None = None,
    ) -> tuple[TournamentRegistryTarget, ...]:
        normalized_sports = tuple(str(item).strip().lower() for item in (sport_slugs or ()) if str(item).strip())
        normalized_surface = str(surface or "").strip().lower()
        surface_predicate = self._SURFACE_PREDICATES.get(normalized_surface)
        if normalized_surface and surface_predicate is None:
            raise ValueError(f"Unsupported tournament registry surface: {surface}")
        rows = await executor.fetch(
            f"""
            SELECT
                source_slug,
                sport_slug,
                unique_tournament_id,
                structure_enabled,
                current_enabled,
                live_enabled,
                historical_enabled,
                enrichment_enabled,
                refresh_interval_seconds,
                historical_backfill_start_date,
                historical_backfill_end_date,
                recent_refresh_days
            FROM tournament_registry
            WHERE is_active = TRUE
              AND (
                $2::text = ''
                OR {surface_predicate or 'TRUE'}
              )
              AND (
                cardinality($1::text[]) = 0
                OR sport_slug = ANY($1::text[])
              )
            ORDER BY sport_slug ASC, priority_rank ASC, unique_tournament_id ASC
            """,
            list(normalized_sports),
            normalized_surface,
        )
        return tuple(
            TournamentRegistryTarget(
                source_slug=str(row["source_slug"]),
                sport_slug=str(row["sport_slug"]),
                unique_tournament_id=int(row["unique_tournament_id"]),
                structure_enabled=bool(row["structure_enabled"]),
                current_enabled=bool(row["current_enabled"]),
                live_enabled=bool(row["live_enabled"]),
                historical_enabled=bool(row["historical_enabled"]),
                enrichment_enabled=bool(row["enrichment_enabled"]),
                refresh_interval_seconds=(
                    None
                    if row["refresh_interval_seconds"] is None
                    else int(row["refresh_interval_seconds"])
                ),
                historical_backfill_start_date=row["historical_backfill_start_date"],
                historical_backfill_end_date=row["historical_backfill_end_date"],
                recent_refresh_days=(
                    None
                    if row["recent_refresh_days"] is None
                    else int(row["recent_refresh_days"])
                ),
            )
            for row in rows
        )

    async def has_rows(self, executor: SqlFetchExecutor, *, sport_slug: str) -> bool:
        rows = await executor.fetch(
            """
            SELECT 1
            FROM tournament_registry
            WHERE sport_slug = $1
            LIMIT 1
            """,
            str(sport_slug).strip().lower(),
        )
        return bool(rows)

    # ------------------------------------------------------------------
    # Phase 1 backfill cursor (2026-05-16). See:
    #   migrations/2026-05-16_backfill_cursor.sql
    #   docs/backfill-ordering-roadmap.md
    #
    # Semantics: next_season_backfill_id is the season the worker should
    # process **next**. Worker reads it, fetches that season, then calls
    # ``advance_backfill_cursor`` which moves the cursor to the next-older
    # season (or sets it to 0 when there is no older season left).
    # ------------------------------------------------------------------

    async def seed_backfill_cursors(
        self,
        executor: SqlExecutor,
        *,
        sport_slug: str | None = None,
        only_uninitialised: bool = True,
    ) -> int:
        """One-shot bootstrap: for every active UT in registry, set
        ``next_season_backfill_id`` to the UT's most-recently-played
        season — defined as the season with the latest
        ``MAX(event.start_timestamp)``.

        We deliberately do NOT use ``MAX(season_id)``: Sofascore re-uses
        old season ids and sometimes assigns a brand-new high id to a
        historical season (observed 2026-05-16 with LaLiga 1969/70
        receiving season_id 91246, higher than current 25/26 = 77559).
        Sorting by latest event start_timestamp is the only reliable
        "newest season" signal available cheaply in our schema.

        ``only_uninitialised=True`` (default) updates ONLY rows where
        the cursor is currently NULL — safe to re-run without clobbering
        in-progress backfills.

        UTs with **zero** ingested events get no seed (no event window
        to anchor on). Those become candidates for a future event-list
        bootstrap pass; the cursor stays NULL so the planner skips
        them via the pending-cursor filter.

        Returns the number of rows that received a new cursor value.
        """
        normalized_sport = (sport_slug or "").strip().lower() or None
        condition = "AND tr.next_season_backfill_id IS NULL" if only_uninitialised else ""
        query = f"""
            WITH latest_season AS (
                SELECT
                    e.unique_tournament_id,
                    e.season_id,
                    MAX(e.start_timestamp) AS max_start_ts,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.unique_tournament_id
                        ORDER BY MAX(e.start_timestamp) DESC NULLS LAST, e.season_id DESC
                    ) AS rn
                FROM event e
                WHERE e.unique_tournament_id IS NOT NULL
                  AND e.season_id IS NOT NULL
                  AND e.start_timestamp IS NOT NULL
                GROUP BY e.unique_tournament_id, e.season_id
            )
            UPDATE tournament_registry tr
            SET next_season_backfill_id = ls.season_id,
                backfill_started_at = COALESCE(tr.backfill_started_at, now()),
                updated_at = now()
            FROM latest_season ls
            WHERE ls.rn = 1
              AND tr.unique_tournament_id = ls.unique_tournament_id
              AND tr.is_active = TRUE
              AND tr.historical_enabled = TRUE
              AND ($1::text IS NULL OR tr.sport_slug = $1::text)
              {condition}
        """
        result = await executor.execute(query, normalized_sport)
        return _rows_affected(result)

    async def seed_backfill_cursors_to_newest_finished_season(
        self,
        executor: SqlExecutor,
        *,
        sport_slug: str | None = None,
        only_uninitialised: bool = True,
    ) -> int:
        """E.3 replacement seed semantics: target the newest **finished**
        season instead of the newest season by start_timestamp.

        The old ``seed_backfill_cursors`` pointed cursors at the newest
        season by ``MAX(event.start_timestamp)`` — fine for active
        leagues, but for cup-style competitions (FIFA WC, EURO,
        Olympic Games, ...) the newest event is the not_started future
        fixture. The worker has nothing to hydrate, the cursor never
        advances, and the entire cat=20 slice stalls.

        This variant constrains to ``status_code = 100`` (finished) so
        the cursor lands on the most recent edition that actually has
        match data to ingest.

        ``only_uninitialised=True`` (default) updates ONLY rows where
        the cursor is currently NULL — safe to re-run without
        clobbering in-progress backfills.

        UTs with zero finished events get no seed (the cursor stays
        NULL so the planner skips them via the pending-cursor filter).
        Those become candidates for a future structure-sync pass
        (E.2 — historical-structure-bootstrap CLI).

        Returns the number of rows that received a new cursor value.
        """
        normalized_sport = (sport_slug or "").strip().lower() or None
        condition = "AND tr.next_season_backfill_id IS NULL" if only_uninitialised else ""
        query = f"""
            WITH newest_finished AS (
                SELECT
                    e.unique_tournament_id,
                    e.season_id,
                    MAX(e.start_timestamp) AS max_start_ts,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.unique_tournament_id
                        ORDER BY MAX(e.start_timestamp) DESC NULLS LAST, e.season_id DESC
                    ) AS rn
                FROM event e
                WHERE e.status_code = 100
                  AND e.unique_tournament_id IS NOT NULL
                  AND e.season_id IS NOT NULL
                  AND e.start_timestamp IS NOT NULL
                GROUP BY e.unique_tournament_id, e.season_id
            )
            UPDATE tournament_registry tr
            SET next_season_backfill_id = nf.season_id,
                backfill_started_at = COALESCE(tr.backfill_started_at, now()),
                updated_at = now()
            FROM newest_finished nf
            WHERE nf.rn = 1
              AND tr.unique_tournament_id = nf.unique_tournament_id
              AND tr.is_active = TRUE
              AND tr.historical_enabled = TRUE
              AND ($1::text IS NULL OR tr.sport_slug = $1::text)
              {condition}
        """
        result = await executor.execute(query, normalized_sport)
        return _rows_affected(result)

    async def re_seed_stuck_cursors_to_newest_finished_season(
        self,
        executor: SqlExecutor,
        *,
        cat_priority_min: int = 0,
    ) -> int:
        """E.1 — one-shot operator method: for every UT whose current
        ``next_season_backfill_id`` points at a season with **zero**
        finished events, retarget the cursor to the newest finished
        season for the same UT.

        Scoping:
          * ``cat_priority_min`` — only re-seed UTs whose category has
            priority ≥ this threshold. ``0`` = all UTs (including
            amateur). ``6`` = top 5 European leagues + all international.

        Behaviour:
          * UTs whose category < cat_priority_min are untouched.
          * UTs where the cursor already points at a season with at
            least one finished event are untouched.
          * UTs with zero finished events in DB are untouched (no
            valid replacement value — these need structure-sync).

        Returns the number of rows actually updated.
        """
        query = """
            WITH newest_finished AS (
                SELECT
                    e.unique_tournament_id,
                    e.season_id,
                    MAX(e.start_timestamp) AS max_start_ts,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.unique_tournament_id
                        ORDER BY MAX(e.start_timestamp) DESC NULLS LAST, e.season_id DESC
                    ) AS rn
                FROM event e
                WHERE e.status_code = 100
                  AND e.unique_tournament_id IS NOT NULL
                  AND e.season_id IS NOT NULL
                  AND e.start_timestamp IS NOT NULL
                GROUP BY e.unique_tournament_id, e.season_id
            )
            UPDATE tournament_registry tr
            SET next_season_backfill_id = nf.season_id,
                backfill_last_advance_at = now(),
                updated_at = now()
            FROM newest_finished nf, unique_tournament ut, category c
            WHERE nf.rn = 1
              AND tr.unique_tournament_id = nf.unique_tournament_id
              AND ut.id = tr.unique_tournament_id
              AND c.id = ut.category_id
              AND c.priority >= $1
              AND tr.is_active = TRUE
              AND tr.historical_enabled = TRUE
              AND tr.next_season_backfill_id IS NOT NULL
              AND tr.next_season_backfill_id > 0
              -- "Stuck" = current cursor points at a season with no finished events
              AND NOT EXISTS (
                  SELECT 1 FROM event e2
                  WHERE e2.unique_tournament_id = tr.unique_tournament_id
                    AND e2.season_id = tr.next_season_backfill_id
                    AND e2.status_code = 100
              )
        """
        result = await executor.execute(query, int(cat_priority_min))
        return _rows_affected(result)

    async def fetch_backfill_cursor(
        self,
        executor: SqlFetchExecutor,
        *,
        sport_slug: str,
        unique_tournament_id: int,
    ) -> int | None:
        """Read the cursor for a (sport, UT). Returns the next season_id
        to fetch, ``0`` when exhausted, ``None`` when uninitialised."""
        rows = await executor.fetch(
            """
            SELECT next_season_backfill_id
            FROM tournament_registry
            WHERE sport_slug = $1 AND unique_tournament_id = $2
            LIMIT 1
            """,
            str(sport_slug).strip().lower(),
            int(unique_tournament_id),
        )
        if not rows:
            return None
        value = rows[0]["next_season_backfill_id"]
        return None if value is None else int(value)

    async def advance_backfill_cursor(
        self,
        executor: SqlExecutor,
        *,
        sport_slug: str,
        unique_tournament_id: int,
        completed_season_id: int,
    ) -> int | None:
        """Move the cursor past ``completed_season_id``.

        Phase 2.A (2026-05-22): walks the
        ``tournament_season_upstream_catalog`` table (the
        authoritative list of Sofascore-published seasons for this
        UT) rather than scanning the ``event`` table. This fixes the
        ghost-completion bug where cup-style competitions with gaps
        in event coverage would skip past missing seasons and stamp
        ``backfill_completed_at`` even though Sofascore still
        publishes those seasons.

        Behaviour:
          * The just-completed season is marked
            ``bootstrap_state = 'fully_processed'`` and
            ``processed_at = now()``.
          * The next cursor candidate is the lowest
            ``upstream_position`` row whose state is not
            ``fully_processed``. ``upstream_position`` matches
            Sofascore's newest-first ordering, so the walk stays
            current → past.
          * ``backfill_completed_at`` is stamped ONLY when every
            catalog row is ``fully_processed`` — no more false
            positives from gaps in ``event``.

        Returns the new cursor value (or ``None`` if the row is missing).
        """
        # Step 1: stamp the just-completed season as fully_processed.
        # Done unconditionally so the next CTE never re-selects it.
        await executor.execute(
            """
            UPDATE tournament_season_upstream_catalog
            SET bootstrap_state = 'fully_processed',
                processed_at = now(),
                last_observed_at = now()
            WHERE unique_tournament_id = $1 AND season_id = $2
            """,
            int(unique_tournament_id),
            int(completed_season_id),
        )

        query = _ADVANCE_BACKFILL_CURSOR_SQL
        # asyncpg returns rows via fetch only; some executors expose execute
        # with a RETURNING-aware return value (string CommandTag). To keep
        # the contract simple we go through fetch when available.
        fetch = getattr(executor, "fetch", None)
        if callable(fetch):
            rows = await fetch(
                query,
                str(sport_slug).strip().lower(),
                int(unique_tournament_id),
                int(completed_season_id),
            )
            if not rows:
                return None
            value = rows[0]["next_season_backfill_id"]
            return None if value is None else int(value)
        # Fallback for executors without fetch (older test fakes).
        await executor.execute(
            query,
            str(sport_slug).strip().lower(),
            int(unique_tournament_id),
            int(completed_season_id),
        )
        return None

    async def list_pending_backfill_cursors(
        self,
        executor: SqlFetchExecutor,
        *,
        sport_slug: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Return UTs whose cursor points at a real next season (>0), sorted
        by priority_rank then UT id. Useful for /ops/backfill-cursor.
        """
        normalized_sport = (sport_slug or "").strip().lower() or None
        rows = await executor.fetch(
            """
            SELECT
                tr.source_slug,
                tr.sport_slug,
                tr.unique_tournament_id,
                tr.priority_rank,
                tr.next_season_backfill_id,
                tr.backfill_started_at,
                tr.backfill_last_advance_at,
                tr.backfill_completed_at
            FROM tournament_registry tr
            WHERE tr.is_active = TRUE
              AND tr.historical_enabled = TRUE
              AND tr.next_season_backfill_id IS NOT NULL
              AND tr.next_season_backfill_id > 0
              AND ($1::text IS NULL OR tr.sport_slug = $1::text)
            ORDER BY tr.priority_rank, tr.unique_tournament_id
            LIMIT $2
            """,
            normalized_sport,
            int(limit),
        )
        return [dict(row) for row in rows]

    async def select_pending_cursors_by_top_category(
        self,
        executor: SqlFetchExecutor,
        *,
        sport_slug: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Return pending backfill cursors **only** from the highest
        ``category.priority`` bucket that still has work.

        Strict barrier semantics: as long as ANY UT in
        ``category.priority = MAX(...)`` has ``next_season_backfill_id > 0``,
        only those rows are returned. When that bucket is drained
        (every cursor cleared) the SQL's ``MAX(c.priority)`` falls to
        the next-highest cat.priority and that bucket becomes active.

        Within the active bucket the result is sorted by
        ``tr.priority_rank ASC`` (the per-category Sofascore-derived
        ranking) and ``ut.user_count DESC`` (popularity tie-break) so
        the most-watched UTs publish first.

        Use this selector instead of ``list_pending_backfill_cursors``
        for planner-side scheduling — the legacy method stays
        unchanged because /ops/backfill-cursor wants the full ordered
        view across all categories.
        """
        normalized_sport = (sport_slug or "").strip().lower()
        rows = await executor.fetch(
            """
            WITH pending AS (
                SELECT
                    tr.source_slug,
                    tr.sport_slug,
                    tr.unique_tournament_id,
                    tr.priority_rank,
                    tr.next_season_backfill_id,
                    c.priority AS category_priority,
                    c.name     AS category_name,
                    ut.user_count
                FROM tournament_registry tr
                JOIN unique_tournament ut ON ut.id = tr.unique_tournament_id
                JOIN category c ON c.id = ut.category_id
                WHERE tr.is_active = TRUE
                  AND tr.historical_enabled = TRUE
                  AND tr.next_season_backfill_id IS NOT NULL
                  AND tr.next_season_backfill_id > 0
                  AND tr.sport_slug = $1
            ),
            top_cat AS (
                SELECT MAX(c.priority) AS top_cat_priority
                FROM tournament_registry tr
                JOIN unique_tournament ut ON ut.id = tr.unique_tournament_id
                JOIN category c ON c.id = ut.category_id
                WHERE tr.is_active = TRUE
                  AND tr.historical_enabled = TRUE
                  AND tr.next_season_backfill_id IS NOT NULL
                  AND tr.next_season_backfill_id > 0
                  AND tr.sport_slug = $1
            )
            SELECT
                p.source_slug,
                p.sport_slug,
                p.unique_tournament_id,
                p.priority_rank,
                p.next_season_backfill_id,
                p.category_priority,
                p.category_name
            FROM pending p, top_cat
            WHERE p.category_priority = top_cat.top_cat_priority
            ORDER BY p.priority_rank ASC,
                     p.user_count DESC NULLS LAST,
                     p.unique_tournament_id ASC
            LIMIT $2
            """,
            normalized_sport,
            int(limit),
        )
        return [dict(row) for row in rows]

    async def list_pending_bootstrap_seasons(
        self,
        executor: SqlFetchExecutor,
        *,
        sport_slug: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Phase 3.7(a) (2026-05-22): return top-N pending catalog rows
        for a sport, ordered by tournament priority then upstream
        position (newest-first).

        Used by ``HistoricalTournamentPlannerDaemon`` to publish a
        throttled batch of bootstrap jobs alongside the cursor walk.
        Workers see these jobs, look up the catalog state, and
        dispatch ``run_historical_tournament_archive(bootstrap_mode=True)``
        — lightweight event-list-only path.

        The category barrier from
        ``select_pending_cursors_by_top_category`` is NOT applied
        here on purpose. Bootstrap is the long-tail drain, not the
        priority frontline; it should make steady progress across
        every active tournament in the sport, not stall behind a
        single top-cat bucket.
        """

        normalized_sport = (sport_slug or "").strip().lower()
        rows = await executor.fetch(
            """
            SELECT
                tsuc.unique_tournament_id,
                tsuc.season_id,
                tsuc.upstream_position,
                tr.priority_rank
            FROM tournament_season_upstream_catalog tsuc
            JOIN tournament_registry tr
                ON tr.unique_tournament_id = tsuc.unique_tournament_id
            WHERE tsuc.bootstrap_state = 'pending'
              AND tr.sport_slug = $1
              AND tr.is_active = TRUE
              AND tr.historical_enabled = TRUE
            ORDER BY tr.priority_rank ASC,
                     tsuc.upstream_position ASC NULLS LAST,
                     tsuc.season_id DESC
            LIMIT $2
            """,
            normalized_sport,
            int(limit),
        )
        return [dict(row) for row in rows]

    async def list_historical_planning_policies(
        self,
        executor: SqlFetchExecutor,
        *,
        sport_slugs: tuple[str, ...] | None = None,
    ) -> tuple[HistoricalPlanningPolicy, ...]:
        normalized_sports = tuple(str(item).strip().lower() for item in (sport_slugs or ()) if str(item).strip())
        rows = await executor.fetch(
            """
            SELECT
                source_slug,
                sport_slug,
                MIN(historical_backfill_start_date) AS historical_backfill_start_date,
                MAX(historical_backfill_end_date) AS historical_backfill_end_date,
                MAX(recent_refresh_days) AS recent_refresh_days
            FROM tournament_registry
            WHERE is_active = TRUE
              AND historical_enabled = TRUE
              AND (
                cardinality($1::text[]) = 0
                OR sport_slug = ANY($1::text[])
              )
            GROUP BY source_slug, sport_slug
            ORDER BY sport_slug ASC, source_slug ASC
            """,
            list(normalized_sports),
        )
        return tuple(
            HistoricalPlanningPolicy(
                source_slug=str(row["source_slug"]),
                sport_slug=str(row["sport_slug"]),
                historical_backfill_start_date=row["historical_backfill_start_date"],
                historical_backfill_end_date=row["historical_backfill_end_date"],
                recent_refresh_days=(
                    None
                    if row["recent_refresh_days"] is None
                    else int(row["recent_refresh_days"])
                ),
            )
            for row in rows
        )
