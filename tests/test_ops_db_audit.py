from __future__ import annotations

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


class _FakeSqlExecutor:
    def __init__(self, values_by_call: dict[tuple[str, tuple[tuple[int, ...], ...]], int]) -> None:
        self.values_by_call = values_by_call

    async def fetchval(self, query: str, *args):
        key = (query, tuple(args))
        return self.values_by_call[key]


if __name__ == "__main__":
    unittest.main()
