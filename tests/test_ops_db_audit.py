from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest


class DatabaseAuditTests(unittest.IsolatedAsyncioTestCase):
    async def test_collect_db_audit_counts_core_and_special_tables(self) -> None:
        from schema_inspector.ops.db_audit import collect_db_audit

        executor = _FakeSqlExecutor(
            {
                (
                    "SELECT COUNT(DISTINCT arl.id) FROM api_request_log arl JOIN api_payload_snapshot aps ON aps.trace_id = arl.trace_id WHERE aps.context_event_id = ANY($1::bigint[])",
                    ((15632088, 15736636),),
                ): 9,
                ("SELECT COUNT(*) FROM api_payload_snapshot WHERE context_event_id = ANY($1::bigint[])", ((15632088, 15736636),)): 7,
                ("SELECT COUNT(*) FROM event WHERE id = ANY($1::bigint[])", ((15632088, 15736636),)): 2,
                ("SELECT COUNT(*) FROM event_statistic WHERE event_id = ANY($1::bigint[])", ((15632088, 15736636),)): 15,
                ("SELECT COUNT(*) FROM event_incident WHERE event_id = ANY($1::bigint[])", ((15632088, 15736636),)): 8,
                ("SELECT COUNT(*) FROM event_lineup WHERE event_id = ANY($1::bigint[])", ((15632088, 15736636),)): 4,
                ("SELECT COUNT(*) FROM event_lineup_player WHERE event_id = ANY($1::bigint[])", ((15632088, 15736636),)): 44,
                ("SELECT COUNT(*) FROM baseball_inning WHERE event_id = ANY($1::bigint[])", ((15632088, 15736636),)): 12,
                ("SELECT COUNT(*) FROM baseball_pitch WHERE event_id = ANY($1::bigint[])", ((15632088, 15736636),)): 33,
            }
        )

        report = await collect_db_audit(
            sql_executor=executor,
            sport_slug="baseball",
            event_ids=(15632088, 15736636),
        )

        self.assertEqual(report.sport_slug, "baseball")
        self.assertEqual(report.event_count, 2)
        self.assertEqual(report.raw_requests, 9)
        self.assertEqual(report.raw_snapshots, 7)
        self.assertEqual(report.events, 2)
        self.assertEqual(report.statistics, 15)
        self.assertEqual(report.incidents, 8)
        self.assertEqual(report.lineup_sides, 4)
        self.assertEqual(report.lineup_players, 44)
        self.assertEqual(report.special_counts, {"baseball_inning": 12, "baseball_pitch": 33})

    async def test_collect_db_audit_returns_zeroed_special_counts_for_supported_sport(self) -> None:
        from schema_inspector.ops.db_audit import collect_db_audit

        executor = _FakeSqlExecutor(
            {
                ("SELECT COUNT(DISTINCT arl.id) FROM api_request_log arl JOIN api_payload_snapshot aps ON aps.trace_id = arl.trace_id WHERE aps.context_event_id = ANY($1::bigint[])", ((15991729,),)): 1,
                ("SELECT COUNT(*) FROM api_payload_snapshot WHERE context_event_id = ANY($1::bigint[])", ((15991729,),)): 2,
                ("SELECT COUNT(*) FROM event WHERE id = ANY($1::bigint[])", ((15991729,),)): 1,
                ("SELECT COUNT(*) FROM event_statistic WHERE event_id = ANY($1::bigint[])", ((15991729,),)): 5,
                ("SELECT COUNT(*) FROM event_incident WHERE event_id = ANY($1::bigint[])", ((15991729,),)): 0,
                ("SELECT COUNT(*) FROM event_lineup WHERE event_id = ANY($1::bigint[])", ((15991729,),)): 0,
                ("SELECT COUNT(*) FROM event_lineup_player WHERE event_id = ANY($1::bigint[])", ((15991729,),)): 0,
                ("SELECT COUNT(*) FROM tennis_point_by_point WHERE event_id = ANY($1::bigint[])", ((15991729,),)): 6,
                ("SELECT COUNT(*) FROM tennis_power WHERE event_id = ANY($1::bigint[])", ((15991729,),)): 0,
            }
        )

        report = await collect_db_audit(
            sql_executor=executor,
            sport_slug="tennis",
            event_ids=(15991729,),
        )

        self.assertEqual(report.special_counts["tennis_point_by_point"], 6)
        self.assertEqual(report.special_counts["tennis_power"], 0)

    async def test_collect_db_audit_includes_event_level_breakdown(self) -> None:
        from schema_inspector.ops.db_audit import collect_db_audit

        executor = _FakeSqlExecutor(
            {
                (
                    "SELECT COUNT(DISTINCT arl.id) FROM api_request_log arl JOIN api_payload_snapshot aps ON aps.trace_id = arl.trace_id WHERE aps.context_event_id = ANY($1::bigint[])",
                    ((101, 202),),
                ): 9,
                ("SELECT COUNT(*) FROM api_payload_snapshot WHERE context_event_id = ANY($1::bigint[])", ((101, 202),)): 7,
                ("SELECT COUNT(*) FROM event WHERE id = ANY($1::bigint[])", ((101, 202),)): 2,
                ("SELECT COUNT(*) FROM event_statistic WHERE event_id = ANY($1::bigint[])", ((101, 202),)): 15,
                ("SELECT COUNT(*) FROM event_incident WHERE event_id = ANY($1::bigint[])", ((101, 202),)): 8,
                ("SELECT COUNT(*) FROM event_lineup WHERE event_id = ANY($1::bigint[])", ((101, 202),)): 4,
                ("SELECT COUNT(*) FROM event_lineup_player WHERE event_id = ANY($1::bigint[])", ((101, 202),)): 44,
            },
            rows_by_call={
                (
                    "SELECT aps.context_event_id AS event_id, COUNT(DISTINCT arl.id)::bigint AS count FROM api_request_log AS arl JOIN api_payload_snapshot AS aps ON aps.trace_id = arl.trace_id WHERE aps.context_event_id = ANY($1::bigint[]) GROUP BY aps.context_event_id",
                    ((101, 202),),
                ): [
                    {"event_id": 101, "count": 2},
                    {"event_id": 202, "count": 7},
                ],
                (
                    "SELECT context_event_id AS event_id, COUNT(*)::bigint AS count FROM api_payload_snapshot WHERE context_event_id = ANY($1::bigint[]) GROUP BY context_event_id",
                    ((101, 202),),
                ): [
                    {"event_id": 101, "count": 1},
                    {"event_id": 202, "count": 6},
                ],
                (
                    "SELECT id AS event_id, COUNT(*)::bigint AS count FROM event WHERE id = ANY($1::bigint[]) GROUP BY id",
                    ((101, 202),),
                ): [
                    {"event_id": 101, "count": 1},
                    {"event_id": 202, "count": 1},
                ],
                (
                    "SELECT event_id, COUNT(*)::bigint AS count FROM event_statistic WHERE event_id = ANY($1::bigint[]) GROUP BY event_id",
                    ((101, 202),),
                ): [
                    {"event_id": 202, "count": 15},
                ],
                (
                    "SELECT event_id, COUNT(*)::bigint AS count FROM event_incident WHERE event_id = ANY($1::bigint[]) GROUP BY event_id",
                    ((101, 202),),
                ): [
                    {"event_id": 202, "count": 8},
                ],
                (
                    "SELECT event_id, COUNT(*)::bigint AS count FROM event_lineup WHERE event_id = ANY($1::bigint[]) GROUP BY event_id",
                    ((101, 202),),
                ): [
                    {"event_id": 101, "count": 2},
                    {"event_id": 202, "count": 2},
                ],
                (
                    "SELECT event_id, COUNT(*)::bigint AS count FROM event_lineup_player WHERE event_id = ANY($1::bigint[]) GROUP BY event_id",
                    ((101, 202),),
                ): [
                    {"event_id": 101, "count": 22},
                    {"event_id": 202, "count": 22},
                ],
            },
        )

        report = await collect_db_audit(
            sql_executor=executor,
            sport_slug="football",
            event_ids=(101, 202),
        )

        self.assertEqual(tuple(item.event_id for item in report.event_reports), (101, 202))
        self.assertEqual(report.event_reports[0].raw_snapshots, 1)
        self.assertEqual(report.event_reports[0].events, 1)
        self.assertEqual(report.event_reports[0].statistics, 0)
        self.assertEqual(report.event_reports[0].lineup_players, 22)
        self.assertEqual(report.event_reports[1].raw_requests, 7)
        self.assertEqual(report.event_reports[1].statistics, 15)
        self.assertEqual(report.event_reports[1].incidents, 8)

    async def test_persist_audit_coverage_writes_event_surface_rows(self) -> None:
        from schema_inspector.ops.db_audit import (
            DatabaseAuditEventReport,
            DatabaseAuditReport,
            persist_audit_coverage,
        )

        executor = _FakeCoverageExecutor()
        report = DatabaseAuditReport(
            sport_slug="football",
            event_count=2,
            raw_requests=9,
            raw_snapshots=7,
            events=2,
            statistics=15,
            incidents=8,
            lineup_sides=4,
            lineup_players=44,
            special_counts={},
            event_reports=(
                DatabaseAuditEventReport(
                    event_id=101,
                    raw_requests=2,
                    raw_snapshots=1,
                    events=1,
                    statistics=0,
                    incidents=0,
                    lineup_sides=2,
                    lineup_players=22,
                    special_counts={},
                ),
                DatabaseAuditEventReport(
                    event_id=202,
                    raw_requests=7,
                    raw_snapshots=6,
                    events=1,
                    statistics=15,
                    incidents=8,
                    lineup_sides=2,
                    lineup_players=22,
                    special_counts={},
                ),
            ),
        )

        records = await persist_audit_coverage(
            sql_executor=executor,
            source_slug="sofascore",
            report=report,
            checked_at="2026-04-21T16:00:00+00:00",
        )

        self.assertEqual(len(records), 8)
        self.assertEqual(len(executor.execute_calls), 8)
        first_args = executor.execute_calls[0][1]
        self.assertEqual(first_args[0], "sofascore")
        self.assertEqual(first_args[1], "football")
        self.assertEqual(first_args[2], "event_core")
        self.assertEqual(first_args[3], "event")
        self.assertEqual(first_args[4], 101)
        self.assertEqual(first_args[5], "fresh")
        lineups_args = executor.execute_calls[3][1]
        self.assertEqual(lineups_args[2], "lineups")
        stats_args = executor.execute_calls[5][1]
        self.assertEqual(stats_args[2], "statistics")
        self.assertEqual(stats_args[5], "fresh")

    def test_build_audit_coverage_records_marks_early_lineups_as_possible(self) -> None:
        from schema_inspector.ops.db_audit import (
            DatabaseAuditEventReport,
            DatabaseAuditReport,
            build_audit_coverage_records,
        )

        checked_at = datetime(2026, 4, 21, 16, 0, tzinfo=timezone.utc)
        report = DatabaseAuditReport(
            sport_slug="football",
            event_count=1,
            raw_requests=1,
            raw_snapshots=1,
            events=1,
            statistics=0,
            incidents=0,
            lineup_sides=2,
            lineup_players=22,
            special_counts={},
            event_reports=(
                DatabaseAuditEventReport(
                    event_id=101,
                    raw_requests=1,
                    raw_snapshots=1,
                    events=1,
                    statistics=0,
                    incidents=0,
                    lineup_sides=2,
                    lineup_players=22,
                    special_counts={},
                    start_timestamp=int((checked_at + timedelta(minutes=120)).timestamp()),
                ),
            ),
        )

        records = build_audit_coverage_records(
            source_slug="sofascore",
            report=report,
            checked_at=checked_at.isoformat(),
        )

        lineups_record = next(item for item in records if item.surface_name == "lineups")
        self.assertEqual(lineups_record.freshness_status, "possible")
        self.assertEqual(lineups_record.completeness_ratio, 1.0)


class _FakeSqlExecutor:
    def __init__(
        self,
        values_by_call: dict[tuple[str, tuple[tuple[int, ...], ...]], int],
        *,
        rows_by_call: dict[tuple[str, tuple[tuple[int, ...], ...]], list[dict[str, int]]] | None = None,
    ) -> None:
        self.values_by_call = values_by_call
        self.rows_by_call = dict(rows_by_call or {})

    async def fetchval(self, query: str, *args):
        key = (query, tuple(args))
        return self.values_by_call[key]

    async def fetch(self, query: str, *args):
        key = (query, tuple(args))
        return list(self.rows_by_call.get(key, []))


class _FakeCoverageExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"


if __name__ == "__main__":
    unittest.main()
