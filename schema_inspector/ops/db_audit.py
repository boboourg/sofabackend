"""Database audit helpers for hybrid ETL runs."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DatabaseAuditReport:
    sport_slug: str
    event_count: int
    raw_requests: int
    raw_snapshots: int
    events: int
    statistics: int
    incidents: int
    lineup_sides: int
    lineup_players: int
    special_counts: dict[str, int]


SPECIAL_TABLES_BY_SPORT: dict[str, tuple[str, ...]] = {
    "tennis": ("tennis_point_by_point", "tennis_power"),
    "baseball": ("baseball_inning", "baseball_pitch"),
    "ice-hockey": ("shotmap_point",),
    "esports": ("esports_game",),
}


async def collect_db_audit(*, sql_executor, sport_slug: str, event_ids: tuple[int, ...]) -> DatabaseAuditReport:
    normalized_event_ids = tuple(int(item) for item in event_ids)
    return DatabaseAuditReport(
        sport_slug=str(sport_slug or ""),
        event_count=len(normalized_event_ids),
        raw_requests=await _count(
            sql_executor,
            "SELECT COUNT(DISTINCT arl.id) FROM api_request_log arl JOIN api_payload_snapshot aps ON aps.trace_id = arl.trace_id WHERE aps.context_event_id = ANY($1::bigint[])",
            normalized_event_ids,
        ),
        raw_snapshots=await _count(
            sql_executor,
            "SELECT COUNT(*) FROM api_payload_snapshot WHERE context_event_id = ANY($1::bigint[])",
            normalized_event_ids,
        ),
        events=await _count(
            sql_executor,
            "SELECT COUNT(*) FROM event WHERE id = ANY($1::bigint[])",
            normalized_event_ids,
        ),
        statistics=await _count(
            sql_executor,
            "SELECT COUNT(*) FROM event_statistic WHERE event_id = ANY($1::bigint[])",
            normalized_event_ids,
        ),
        incidents=await _count(
            sql_executor,
            "SELECT COUNT(*) FROM event_incident WHERE event_id = ANY($1::bigint[])",
            normalized_event_ids,
        ),
        lineup_sides=await _count(
            sql_executor,
            "SELECT COUNT(*) FROM event_lineup WHERE event_id = ANY($1::bigint[])",
            normalized_event_ids,
        ),
        lineup_players=await _count(
            sql_executor,
            "SELECT COUNT(*) FROM event_lineup_player WHERE event_id = ANY($1::bigint[])",
            normalized_event_ids,
        ),
        special_counts=await _collect_special_counts(
            sql_executor=sql_executor,
            sport_slug=str(sport_slug or ""),
            event_ids=normalized_event_ids,
        ),
    )


async def _collect_special_counts(*, sql_executor, sport_slug: str, event_ids: tuple[int, ...]) -> dict[str, int]:
    tables = SPECIAL_TABLES_BY_SPORT.get(str(sport_slug or ""), ())
    counts: dict[str, int] = {}
    for table_name in tables:
        counts[table_name] = await _count(
            sql_executor,
            f"SELECT COUNT(*) FROM {table_name} WHERE event_id = ANY($1::bigint[])",
            event_ids,
        )
    return counts


async def _count(sql_executor, query: str, event_ids: tuple[int, ...]) -> int:
    fetchval = getattr(sql_executor, "fetchval", None)
    if callable(fetchval):
        value = await fetchval(query, event_ids)
        return int(value or 0)
    rows = await sql_executor.fetch(query, event_ids)
    if not rows:
        return 0
    first = rows[0]
    if isinstance(first, dict):
        return int(next(iter(first.values())))
    return int(first[0])
