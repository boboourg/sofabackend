"""Database audit helpers for hybrid ETL runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from ..storage.coverage_repository import CoverageLedgerRecord, CoverageRepository


EVENT_RAW_REQUESTS_BY_EVENT_QUERY = (
    "SELECT aps.context_event_id AS event_id, COUNT(DISTINCT arl.id)::bigint AS count "
    "FROM api_request_log AS arl "
    "JOIN api_payload_snapshot AS aps ON aps.trace_id = arl.trace_id "
    "WHERE aps.context_event_id = ANY($1::bigint[]) "
    "GROUP BY aps.context_event_id"
)
EVENT_RAW_SNAPSHOTS_BY_EVENT_QUERY = (
    "SELECT context_event_id AS event_id, COUNT(*)::bigint AS count "
    "FROM api_payload_snapshot "
    "WHERE context_event_id = ANY($1::bigint[]) "
    "GROUP BY context_event_id"
)
EVENT_ROWS_BY_EVENT_QUERY = (
    "SELECT id AS event_id, COUNT(*)::bigint AS count "
    "FROM event "
    "WHERE id = ANY($1::bigint[]) "
    "GROUP BY id"
)
EVENT_STATISTICS_BY_EVENT_QUERY = (
    "SELECT event_id, COUNT(*)::bigint AS count "
    "FROM event_statistic "
    "WHERE event_id = ANY($1::bigint[]) "
    "GROUP BY event_id"
)
EVENT_INCIDENTS_BY_EVENT_QUERY = (
    "SELECT event_id, COUNT(*)::bigint AS count "
    "FROM event_incident "
    "WHERE event_id = ANY($1::bigint[]) "
    "GROUP BY event_id"
)
EVENT_LINEUP_SIDES_BY_EVENT_QUERY = (
    "SELECT event_id, COUNT(*)::bigint AS count "
    "FROM event_lineup "
    "WHERE event_id = ANY($1::bigint[]) "
    "GROUP BY event_id"
)
EVENT_LINEUP_PLAYERS_BY_EVENT_QUERY = (
    "SELECT event_id, COUNT(*)::bigint AS count "
    "FROM event_lineup_player "
    "WHERE event_id = ANY($1::bigint[]) "
    "GROUP BY event_id"
)


@dataclass(frozen=True)
class DatabaseAuditEventReport:
    event_id: int
    raw_requests: int
    raw_snapshots: int
    events: int
    statistics: int
    incidents: int
    lineup_sides: int
    lineup_players: int
    special_counts: dict[str, int]


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
    event_reports: tuple[DatabaseAuditEventReport, ...] = ()


SPECIAL_TABLES_BY_SPORT: dict[str, tuple[str, ...]] = {
    "tennis": ("tennis_point_by_point", "tennis_power"),
    "baseball": ("baseball_inning", "baseball_pitch"),
    "ice-hockey": ("shotmap_point",),
    "esports": ("esports_game",),
}


async def collect_db_audit(*, sql_executor, sport_slug: str, event_ids: tuple[int, ...]) -> DatabaseAuditReport:
    normalized_event_ids = tuple(int(item) for item in event_ids)
    event_reports = await _collect_event_reports(
        sql_executor=sql_executor,
        sport_slug=str(sport_slug or ""),
        event_ids=normalized_event_ids,
    )
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
        event_reports=event_reports,
    )


async def persist_audit_coverage(*, sql_executor, source_slug: str, report: DatabaseAuditReport, checked_at: str | None = None):
    repository = CoverageRepository()
    records = build_audit_coverage_records(
        source_slug=source_slug,
        report=report,
        checked_at=checked_at,
    )
    for record in records:
        await repository.upsert_coverage(sql_executor, record)
    return records


def build_audit_coverage_records(*, source_slug: str, report: DatabaseAuditReport, checked_at: str | None = None) -> tuple[CoverageLedgerRecord, ...]:
    normalized_checked_at = checked_at or datetime.now(timezone.utc).isoformat()
    records: list[CoverageLedgerRecord] = []
    for event_report in report.event_reports:
        surface_states = (
            ("event_core", *_core_coverage_state(event_report)),
            ("statistics", *_binary_coverage_state(event_report.statistics)),
            ("incidents", *_binary_coverage_state(event_report.incidents)),
            ("lineups", *_lineups_coverage_state(event_report)),
        )
        for surface_name, freshness_status, completeness_ratio in surface_states:
            records.append(
                CoverageLedgerRecord(
                    source_slug=str(source_slug or ""),
                    sport_slug=report.sport_slug,
                    surface_name=surface_name,
                    scope_type="event",
                    scope_id=int(event_report.event_id),
                    freshness_status=freshness_status,
                    completeness_ratio=completeness_ratio,
                    last_success_at=normalized_checked_at if freshness_status == "fresh" else None,
                    last_checked_at=normalized_checked_at,
                )
            )
    return tuple(records)


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


async def _collect_event_reports(*, sql_executor, sport_slug: str, event_ids: tuple[int, ...]) -> tuple[DatabaseAuditEventReport, ...]:
    event_maps = {
        "raw_requests": await _count_by_event(sql_executor, EVENT_RAW_REQUESTS_BY_EVENT_QUERY, event_ids),
        "raw_snapshots": await _count_by_event(sql_executor, EVENT_RAW_SNAPSHOTS_BY_EVENT_QUERY, event_ids),
        "events": await _count_by_event(sql_executor, EVENT_ROWS_BY_EVENT_QUERY, event_ids),
        "statistics": await _count_by_event(sql_executor, EVENT_STATISTICS_BY_EVENT_QUERY, event_ids),
        "incidents": await _count_by_event(sql_executor, EVENT_INCIDENTS_BY_EVENT_QUERY, event_ids),
        "lineup_sides": await _count_by_event(sql_executor, EVENT_LINEUP_SIDES_BY_EVENT_QUERY, event_ids),
        "lineup_players": await _count_by_event(sql_executor, EVENT_LINEUP_PLAYERS_BY_EVENT_QUERY, event_ids),
    }
    special_counts_by_event = await _collect_special_counts_by_event(
        sql_executor=sql_executor,
        sport_slug=sport_slug,
        event_ids=event_ids,
    )
    return tuple(
        DatabaseAuditEventReport(
            event_id=int(event_id),
            raw_requests=int(event_maps["raw_requests"].get(int(event_id), 0)),
            raw_snapshots=int(event_maps["raw_snapshots"].get(int(event_id), 0)),
            events=int(event_maps["events"].get(int(event_id), 0)),
            statistics=int(event_maps["statistics"].get(int(event_id), 0)),
            incidents=int(event_maps["incidents"].get(int(event_id), 0)),
            lineup_sides=int(event_maps["lineup_sides"].get(int(event_id), 0)),
            lineup_players=int(event_maps["lineup_players"].get(int(event_id), 0)),
            special_counts=dict(special_counts_by_event.get(int(event_id), {})),
        )
        for event_id in event_ids
    )


async def _collect_special_counts_by_event(*, sql_executor, sport_slug: str, event_ids: tuple[int, ...]) -> dict[int, dict[str, int]]:
    tables = SPECIAL_TABLES_BY_SPORT.get(str(sport_slug or ""), ())
    counts_by_event: dict[int, dict[str, int]] = {}
    for table_name in tables:
        query = (
            f"SELECT event_id, COUNT(*)::bigint AS count "
            f"FROM {table_name} "
            f"WHERE event_id = ANY($1::bigint[]) "
            f"GROUP BY event_id"
        )
        rows = await sql_executor.fetch(query, event_ids)
        for row in rows:
            event_id = int(row["event_id"])
            counts_by_event.setdefault(event_id, {})[table_name] = int(row["count"] or 0)
    for event_id in event_ids:
        counts_by_event.setdefault(int(event_id), {})
        for table_name in tables:
            counts_by_event[int(event_id)].setdefault(table_name, 0)
    return counts_by_event


async def _count_by_event(sql_executor, query: str, event_ids: tuple[int, ...]) -> dict[int, int]:
    rows = await sql_executor.fetch(query, event_ids)
    counts: dict[int, int] = {}
    for row in rows:
        event_id = row.get("event_id") if isinstance(row, dict) else row[0]
        count = row.get("count") if isinstance(row, dict) else row[1]
        if event_id is None:
            continue
        counts[int(event_id)] = int(count or 0)
    return counts


def _binary_coverage_state(count: int) -> tuple[str, float]:
    return ("fresh", 1.0) if int(count) > 0 else ("missing", 0.0)


def _core_coverage_state(report: DatabaseAuditEventReport) -> tuple[str, float]:
    signals = (int(report.raw_snapshots > 0), int(report.events > 0))
    completeness_ratio = sum(signals) / len(signals)
    freshness_status = "fresh" if completeness_ratio >= 1.0 else "missing"
    return freshness_status, completeness_ratio


def _lineups_coverage_state(report: DatabaseAuditEventReport) -> tuple[str, float]:
    signals = (int(report.lineup_sides > 0), int(report.lineup_players > 0))
    completeness_ratio = sum(signals) / len(signals)
    freshness_status = "fresh" if completeness_ratio >= 1.0 else ("partial" if completeness_ratio > 0 else "missing")
    return freshness_status, completeness_ratio


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
