from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.entities_cli import _parse_triplet, _run


class EntitiesCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_uses_adapter_built_entities_job_and_passes_expected_parsed_args(self) -> None:
        fake_adapter = _FakeEntitiesAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.entities_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.entities_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.entities_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.entities_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
        ):
            exit_code = await _run(_build_args())

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.entities_build_calls, [fake_database])
        fake_adapter.entities_job.run.assert_awaited_once()
        call = fake_adapter.entities_job.run.await_args
        self.assertEqual(call.kwargs["player_ids"], [7])
        self.assertEqual(call.kwargs["player_statistics_ids"], [7])
        self.assertEqual(call.kwargs["team_ids"], [12])
        player_overall = call.kwargs["player_overall_requests"][0]
        self.assertEqual((player_overall.player_id, player_overall.unique_tournament_id, player_overall.season_id), (1, 2, 3))
        team_overall = call.kwargs["team_overall_requests"][0]
        self.assertEqual((team_overall.team_id, team_overall.unique_tournament_id, team_overall.season_id), (4, 5, 6))
        player_heatmap = call.kwargs["player_heatmap_requests"][0]
        self.assertEqual((player_heatmap.player_id, player_heatmap.unique_tournament_id, player_heatmap.season_id), (7, 8, 9))
        team_graph = call.kwargs["team_performance_graph_requests"][0]
        self.assertEqual((team_graph.team_id, team_graph.unique_tournament_id, team_graph.season_id), (10, 11, 12))
        self.assertEqual(call.kwargs["include_player_statistics"], True)
        self.assertEqual(call.kwargs["include_player_statistics_seasons"], False)
        self.assertEqual(call.kwargs["include_player_transfer_history"], True)
        self.assertEqual(call.kwargs["include_team_statistics_seasons"], False)
        self.assertEqual(call.kwargs["include_team_player_statistics_seasons"], True)
        self.assertEqual(call.kwargs["timeout"], 15.0)

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.entities_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.entities_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.entities_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.entities_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedEntitiesAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "entities enrichment is not wired for source secondary_source",
            ):
                await _run(_build_args())


class EntitiesCliParserTests(unittest.TestCase):
    def test_parse_triplet_parses_integer_triplets(self) -> None:
        self.assertEqual(_parse_triplet("1:2:3", "player-overall"), (1, 2, 3))

    def test_parse_triplet_raises_for_malformed_value(self) -> None:
        with self.assertRaisesRegex(SystemExit, "player-overall expects value in form"):
            _parse_triplet("1:2", "player-overall")


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeEntitiesAdapter:
    def __init__(self) -> None:
        self.entities_job = _FakeEntitiesJob()
        self.entities_build_calls: list[object] = []

    def build_entities_job(self, database):
        self.entities_build_calls.append(database)
        return self.entities_job


class _UnsupportedEntitiesAdapter:
    def build_entities_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "entities enrichment is not wired for source secondary_source"
        )


class _FakeEntitiesJob:
    def __init__(self) -> None:
        written = SimpleNamespace(
            player_rows=1,
            team_rows=1,
            transfer_history_rows=2,
            player_season_statistics_rows=3,
            entity_statistics_season_rows=4,
            entity_statistics_type_rows=5,
            season_statistics_type_rows=6,
            payload_snapshot_rows=7,
        )
        self.run = AsyncMock(return_value=SimpleNamespace(written=written))


def _build_args():
    return SimpleNamespace(
        player_id=[7],
        team_id=[12],
        player_overall=["1:2:3"],
        team_overall=["4:5:6"],
        player_heatmap=["7:8:9"],
        team_performance_graph=["10:11:12"],
        skip_player_statistics=False,
        skip_player_statistics_seasons=True,
        skip_player_transfer_history=False,
        skip_team_statistics_seasons=True,
        skip_team_player_statistics_seasons=False,
        timeout=15.0,
        proxy=[],
        user_agent=None,
        max_attempts=None,
        database_url=None,
        db_min_size=None,
        db_max_size=None,
        db_timeout=None,
    )


if __name__ == "__main__":
    unittest.main()
