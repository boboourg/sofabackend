"""PostgreSQL repository for the League Capabilities Registry.

Phase 4 (2026-05-23): persists ``league_endpoint_capability`` rows that
encode which Sofascore endpoint patterns are ALLOWED / DISABLED / UNKNOWN
for each ``(unique_tournament_id, season_id, status_type, endpoint_pattern)``
quad — measured by the probe service rather than inferred from
hardcoded match_center_policy rules.

The orchestrator's hot path reads from a Redis cache (built on top
of this DB layer); the DB is the authoritative store and the source
of truth for the refresh daemon.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Protocol


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


class SqlFetchExecutor(SqlExecutor, Protocol):
    async def fetchrow(self, query: str, *args: object) -> Any: ...
    async def fetch(self, query: str, *args: object) -> list[Any]: ...


# Verdict states. These string constants MUST match the CHECK constraint
# in 2026-05-23_league_endpoint_capability.sql — don't translate.
STATE_ALLOWED = "allowed"
STATE_DISABLED = "disabled"
STATE_UNKNOWN = "unknown"

_KNOWN_STATES = frozenset({STATE_ALLOWED, STATE_DISABLED, STATE_UNKNOWN})

SOURCE_PROBE = "probe"
SOURCE_MANUAL_OVERRIDE = "manual_override"
SOURCE_LEGACY_SEED = "legacy_seed"


@dataclass(frozen=True)
class CapabilityRow:
    """Read-side view of one league_endpoint_capability row."""

    unique_tournament_id: int
    season_id: int | None
    status_type: str
    endpoint_pattern: str
    state: str
    probed_at: datetime | None
    probe_samples_total: int
    probe_samples_ok: int
    probe_samples_http_404: int
    probe_samples_empty: int
    probe_samples_error: int
    confidence_score: float | None
    expires_at: datetime
    source: str
    notes: str | None

    @property
    def is_allowed(self) -> bool:
        return self.state == STATE_ALLOWED

    @property
    def is_disabled(self) -> bool:
        return self.state == STATE_DISABLED

    @property
    def is_unknown(self) -> bool:
        return self.state == STATE_UNKNOWN


@dataclass(frozen=True)
class CapabilityUpsert:
    """Write-side payload for upsert_capability."""

    unique_tournament_id: int
    season_id: int | None
    status_type: str
    endpoint_pattern: str
    state: str
    probe_samples_total: int
    probe_samples_ok: int
    probe_samples_http_404: int
    probe_samples_empty: int
    probe_samples_error: int
    confidence_score: float | None
    expires_at: datetime
    source: str = SOURCE_PROBE
    notes: str | None = None

    def validate(self) -> None:
        if self.state not in _KNOWN_STATES:
            raise ValueError(f"state must be one of {_KNOWN_STATES}, got {self.state!r}")
        if self.probe_samples_total < 0:
            raise ValueError("probe_samples_total must be >= 0")
        if self.probe_samples_ok > self.probe_samples_total:
            raise ValueError("probe_samples_ok cannot exceed total")


class LeagueCapabilitiesRepository:
    """Phase 4 (2026-05-23): repository for league_endpoint_capability."""

    async def upsert_capability(
        self,
        executor: SqlExecutor,
        *,
        row: CapabilityUpsert,
    ) -> None:
        """Insert or update one capability row. ``probed_at`` is set to
        now() unconditionally on each upsert (so re-probes refresh the
        timestamp even when the verdict didn't change)."""

        row.validate()
        await executor.execute(
            """
            INSERT INTO league_endpoint_capability (
                unique_tournament_id, season_id, status_type, endpoint_pattern,
                state, probed_at,
                probe_samples_total, probe_samples_ok,
                probe_samples_http_404, probe_samples_empty, probe_samples_error,
                confidence_score, expires_at, source, notes,
                created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4,
                $5, now(),
                $6, $7,
                $8, $9, $10,
                $11, $12, $13, $14,
                now(), now()
            )
            ON CONFLICT (unique_tournament_id, season_id, status_type, endpoint_pattern)
            DO UPDATE SET
                state = EXCLUDED.state,
                probed_at = now(),
                probe_samples_total = EXCLUDED.probe_samples_total,
                probe_samples_ok = EXCLUDED.probe_samples_ok,
                probe_samples_http_404 = EXCLUDED.probe_samples_http_404,
                probe_samples_empty = EXCLUDED.probe_samples_empty,
                probe_samples_error = EXCLUDED.probe_samples_error,
                confidence_score = EXCLUDED.confidence_score,
                expires_at = EXCLUDED.expires_at,
                source = EXCLUDED.source,
                notes = EXCLUDED.notes,
                updated_at = now()
            -- Stage 1 anti-bloat guard: only update when the verdict or
            -- sample distribution actually changed. Re-probes that return
            -- identical results produce zero dead tuples.
            WHERE
                league_endpoint_capability.state IS DISTINCT FROM EXCLUDED.state
                OR league_endpoint_capability.probe_samples_total IS DISTINCT FROM EXCLUDED.probe_samples_total
                OR league_endpoint_capability.probe_samples_ok IS DISTINCT FROM EXCLUDED.probe_samples_ok
                OR league_endpoint_capability.source IS DISTINCT FROM EXCLUDED.source
            """,
            int(row.unique_tournament_id),
            row.season_id if row.season_id is None else int(row.season_id),
            str(row.status_type),
            str(row.endpoint_pattern),
            str(row.state),
            int(row.probe_samples_total),
            int(row.probe_samples_ok),
            int(row.probe_samples_http_404),
            int(row.probe_samples_empty),
            int(row.probe_samples_error),
            float(row.confidence_score) if row.confidence_score is not None else None,
            row.expires_at,
            str(row.source),
            row.notes,
        )

    async def fetch_capability(
        self,
        executor: SqlFetchExecutor,
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
        endpoint_pattern: str,
    ) -> CapabilityRow | None:
        """Lookup one row. Returns None if absent (orchestrator falls
        back to legacy match_center_policy)."""

        row = await executor.fetchrow(
            """
            SELECT *
            FROM league_endpoint_capability
            WHERE unique_tournament_id = $1
              AND season_id IS NOT DISTINCT FROM $2
              AND status_type = $3
              AND endpoint_pattern = $4
            """,
            int(unique_tournament_id),
            season_id if season_id is None else int(season_id),
            str(status_type),
            str(endpoint_pattern),
        )
        if row is None:
            return None
        return _row_to_capability(row)

    async def fetch_capabilities_for_quad(
        self,
        executor: SqlFetchExecutor,
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
    ) -> list[CapabilityRow]:
        """Phase 4.7.4 (2026-05-23): batch fetch for the whole
        ``(unique_tournament_id, season_id, status_type)`` triple in one
        SQL roundtrip. Replaces the 12 sequential
        ``fetch_capability`` calls that triggered asyncpg pool
        starvation under the Phase 4.8 production flip.

        No ``endpoint_pattern`` filter on purpose — a quad realistically
        has ≤12 rows (one per detail endpoint), so an unbounded fetch is
        cheaper than building an ``IN`` list with a dozen parameters.
        Caller picks the rows it cares about from the returned list."""

        rows = await executor.fetch(
            """
            SELECT *
            FROM league_endpoint_capability
            WHERE unique_tournament_id = $1
              AND season_id IS NOT DISTINCT FROM $2
              AND status_type = $3
            """,
            int(unique_tournament_id),
            season_id if season_id is None else int(season_id),
            str(status_type),
        )
        return [_row_to_capability(row) for row in rows]

    async def list_active_capabilities(
        self,
        executor: SqlFetchExecutor,
    ) -> list[CapabilityRow]:
        """Phase 4.7.5 (2026-05-23): bulk fetch of every capability row
        still considered "active" — either ``expires_at`` is in the
        future OR ``source = 'manual_override'`` (which gets a far-
        future expires_at by construction, but we belt-and-suspender it
        in the WHERE).

        Used by ``Registry.warm_cache_from_db`` at worker startup to
        pre-populate Redis. After warm the hot path is Redis-only — no
        per-quad DB lookups during match-center fetches, which is what
        kept blowing up the asyncpg pool in Phase 4.8.

        One unfiltered SELECT — ~600 rows today, can scale to tens of
        thousands without changing the design (Postgres index scan +
        single network roundtrip)."""

        rows = await executor.fetch(
            """
            SELECT *
            FROM league_endpoint_capability
            WHERE expires_at >= now()
               OR source = 'manual_override'
            """
        )
        return [_row_to_capability(row) for row in rows]

    async def list_capabilities_for_ut(
        self,
        executor: SqlFetchExecutor,
        *,
        unique_tournament_id: int,
        season_id: int | None = None,
    ) -> list[CapabilityRow]:
        """All capability rows for a UT, optionally narrowed to one
        season. Used by the /ops/league-capabilities endpoint and the
        cache-warmup pass on registry init."""

        if season_id is None:
            rows = await executor.fetch(
                """
                SELECT *
                FROM league_endpoint_capability
                WHERE unique_tournament_id = $1
                ORDER BY season_id NULLS FIRST, status_type, endpoint_pattern
                """,
                int(unique_tournament_id),
            )
        else:
            rows = await executor.fetch(
                """
                SELECT *
                FROM league_endpoint_capability
                WHERE unique_tournament_id = $1
                  AND season_id IS NOT DISTINCT FROM $2
                ORDER BY status_type, endpoint_pattern
                """,
                int(unique_tournament_id),
                int(season_id),
            )
        return [_row_to_capability(row) for row in rows]

    async def set_manual_override(
        self,
        executor: SqlExecutor,
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
        endpoint_pattern: str,
        state: str,
        note: str | None,
    ) -> None:
        """Operator-driven UPSERT with source='manual_override' and a
        far-future ``expires_at`` so the refresh daemon never
        re-probes this row. Used by /ops/league-capabilities/set and
        the ``cli league-capability set`` subcommand.

        State must be one of allowed/disabled/unknown (the same set
        the probe service uses) — invalid state raises ValueError
        before any SQL is emitted."""

        if state not in _KNOWN_STATES:
            raise ValueError(
                f"state must be one of {_KNOWN_STATES}, got {state!r}"
            )

        # Far-future expiry — manual override stays until operator
        # changes it (refresh daemon's list_expired filters out
        # source='manual_override' as a second safety net).
        far_future = datetime.now(timezone.utc).replace(year=2099)

        await executor.execute(
            """
            INSERT INTO league_endpoint_capability (
                unique_tournament_id, season_id, status_type, endpoint_pattern,
                state, probed_at,
                probe_samples_total, probe_samples_ok,
                probe_samples_http_404, probe_samples_empty, probe_samples_error,
                confidence_score, expires_at, source, notes,
                created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4,
                $5, now(),
                0, 0,
                0, 0, 0,
                NULL, $6, 'manual_override', $7,
                now(), now()
            )
            ON CONFLICT (unique_tournament_id, season_id, status_type, endpoint_pattern)
            DO UPDATE SET
                state = EXCLUDED.state,
                probed_at = now(),
                expires_at = EXCLUDED.expires_at,
                source = 'manual_override',
                notes = EXCLUDED.notes,
                updated_at = now()
            """,
            int(unique_tournament_id),
            season_id if season_id is None else int(season_id),
            str(status_type),
            str(endpoint_pattern),
            str(state),
            far_future,
            note,
        )

    async def list_expired(
        self,
        executor: SqlFetchExecutor,
        *,
        limit: int,
        now_factory=None,
    ) -> list[tuple[int, int | None, str]]:
        """Return up to ``limit`` (ut_id, season_id, status_type) tuples
        whose ``expires_at`` is past. Refresh daemon reads this list to
        schedule re-probes.

        The list is grouped by (ut, season, status) — one probe pass
        handles all endpoint_patterns for that quad in a single
        sampling, so iterating per-endpoint here would over-trigger
        probes."""

        now_ts = now_factory() if now_factory else datetime.now(timezone.utc)
        rows = await executor.fetch(
            """
            SELECT DISTINCT unique_tournament_id, season_id, status_type
            FROM league_endpoint_capability
            WHERE expires_at < $1
              AND source <> 'manual_override'
            ORDER BY unique_tournament_id, season_id NULLS FIRST, status_type
            LIMIT $2
            """,
            now_ts,
            int(limit),
        )
        return [
            (
                int(row["unique_tournament_id"]),
                None if row["season_id"] is None else int(row["season_id"]),
                str(row["status_type"]),
            )
            for row in rows
        ]


def _row_to_capability(row: Any) -> CapabilityRow:
    return CapabilityRow(
        unique_tournament_id=int(row["unique_tournament_id"]),
        season_id=None if row["season_id"] is None else int(row["season_id"]),
        status_type=str(row["status_type"]),
        endpoint_pattern=str(row["endpoint_pattern"]),
        state=str(row["state"]),
        probed_at=row["probed_at"],
        probe_samples_total=int(row["probe_samples_total"]),
        probe_samples_ok=int(row["probe_samples_ok"]),
        probe_samples_http_404=int(row["probe_samples_http_404"]),
        probe_samples_empty=int(row["probe_samples_empty"]),
        probe_samples_error=int(row["probe_samples_error"]),
        confidence_score=(
            None
            if row["confidence_score"] is None
            else float(row["confidence_score"])
        ),
        expires_at=row["expires_at"],
        source=str(row["source"]),
        notes=row["notes"],
    )
