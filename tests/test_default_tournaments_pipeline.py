from __future__ import annotations

import unittest
from types import SimpleNamespace

from schema_inspector.competition_parser import ApiPayloadSnapshotRecord, UniqueTournamentSeasonRecord
from schema_inspector.default_tournaments_pipeline_cli import (
    _load_season_event_ids,
    _select_season_ids,
    _select_unique_tournament_ids,
)
from schema_inspector.endpoints import UNIQUE_TOURNAMENT_SEASONS_ENDPOINT


class DefaultTournamentsPipelineTests(unittest.TestCase):
    def test_select_unique_tournament_ids_applies_filter_offset_and_limit(self) -> None:
        result = _select_unique_tournament_ids(
            (17, 8, 35, 17, 34),
            offset=1,
            limit=2,
            include_ids=(8, 35, 34),
        )

        self.assertEqual(result, (35, 34))

    def test_select_season_ids_prefers_payload_order(self) -> None:
        competition_result = SimpleNamespace(
            unique_tournament_id=17,
            parsed=SimpleNamespace(
                payload_snapshots=(
                    ApiPayloadSnapshotRecord(
                        endpoint_pattern=UNIQUE_TOURNAMENT_SEASONS_ENDPOINT.pattern,
                        source_url="https://example.test/seasons",
                        envelope_key="seasons",
                        context_entity_type="unique_tournament",
                        context_entity_id=17,
                        payload={"seasons": [{"id": 76986}, {"id": 72958}, {"id": 63515}]},
                        fetched_at="2026-04-12T10:00:00+00:00",
                    ),
                ),
                unique_tournament_seasons=(
                    UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=63515),
                    UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=72958),
                    UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=76986),
                ),
            ),
        )

        self.assertEqual(
            _select_season_ids(competition_result, seasons_per_tournament=2),
            (76986, 72958),
        )

    def test_select_season_ids_falls_back_to_all_when_limit_disabled(self) -> None:
        competition_result = SimpleNamespace(
            unique_tournament_id=17,
            parsed=SimpleNamespace(
                payload_snapshots=(),
                unique_tournament_seasons=(
                    UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=3),
                    UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=9),
                    UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=5),
                ),
            ),
        )

        self.assertEqual(
            _select_season_ids(competition_result, seasons_per_tournament=0),
            (9, 5, 3),
        )


class _FakeConnection:
    def __init__(self, rows) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args):
        self.calls.append((sql, args))
        return self.rows


class DefaultTournamentsPipelineSqlTests(unittest.IsolatedAsyncioTestCase):
    async def test_load_season_event_ids_orders_via_subquery(self) -> None:
        connection = _FakeConnection(rows=[{"id": 14025026}, {"id": 14025027}])

        result = await _load_season_event_ids(
            connection,
            unique_tournament_id=17,
            season_id=76986,
        )

        self.assertEqual(result, (14025026, 14025027))
        self.assertEqual(connection.calls[0][1], (17, 76986))
        self.assertIn("SELECT seed.id", connection.calls[0][0])
        self.assertIn("ORDER BY seed.start_timestamp NULLS LAST, seed.id", connection.calls[0][0])


if __name__ == "__main__":
    unittest.main()
