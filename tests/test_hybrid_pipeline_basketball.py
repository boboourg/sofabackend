from __future__ import annotations

import json
import unittest

from schema_inspector.fetch_executor import FetchExecutor
from schema_inspector.normalizers.worker import NormalizeWorker
from schema_inspector.parsers.registry import ParserRegistry
from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator
from schema_inspector.planner.planner import Planner
from schema_inspector.runtime import TransportAttempt, TransportResult


class _FakeTransport:
    def __init__(self, responses: dict[str, TransportResult]) -> None:
        self.responses = responses
        self.seen_urls: list[str] = []

    async def fetch(self, url: str, *, headers=None, timeout: float = 20.0) -> TransportResult:
        del headers, timeout
        self.seen_urls.append(url)
        return self.responses[url]


class _FakeRawSnapshotStore:
    def __init__(self) -> None:
        self.snapshots_by_id = {}
        self._next_id = 1

    async def insert_request_log(self, executor, record) -> None:
        del executor, record

    async def insert_payload_snapshot_returning_id(self, executor, record) -> int:
        del executor
        snapshot_id = self._next_id
        self._next_id += 1
        self.snapshots_by_id[snapshot_id] = record
        return snapshot_id

    async def upsert_snapshot_head(self, executor, record) -> None:
        del executor, record

    def load_snapshot(self, snapshot_id: int):
        from schema_inspector.parsers.base import RawSnapshot

        record = self.snapshots_by_id[snapshot_id]
        return RawSnapshot(
            snapshot_id=snapshot_id,
            endpoint_pattern=record.endpoint_pattern,
            sport_slug=record.sport_slug,
            source_url=record.source_url,
            resolved_url=record.resolved_url,
            envelope_key=record.envelope_key,
            http_status=record.http_status,
            payload=record.payload,
            fetched_at=record.fetched_at,
            context_entity_type=record.context_entity_type,
            context_entity_id=record.context_entity_id,
            context_unique_tournament_id=record.context_unique_tournament_id,
            context_season_id=record.context_season_id,
            context_event_id=record.context_event_id,
        )


class _FakeCapabilityRepository:
    def __init__(self) -> None:
        self.rollups = []
        self.observations = []

    async def insert_observation(self, executor, record) -> None:
        del executor
        self.observations.append(record)

    async def upsert_rollup(self, executor, record) -> None:
        del executor
        self.rollups.append(record)


class BasketballHybridPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_basketball_pipeline_fetches_regular_season_widgets(self) -> None:
        event_url = "https://www.sofascore.com/api/v1/event/14439306"
        statistics_url = "https://www.sofascore.com/api/v1/event/14439306/statistics"
        lineups_url = "https://www.sofascore.com/api/v1/event/14439306/lineups"
        incidents_url = "https://www.sofascore.com/api/v1/event/14439306/incidents"
        team_home_url = "https://www.sofascore.com/api/v1/team/44"
        team_away_url = "https://www.sofascore.com/api/v1/team/45"
        player_home_url = "https://www.sofascore.com/api/v1/player/800"
        player_away_url = "https://www.sofascore.com/api/v1/player/801"
        manager_home_url = "https://www.sofascore.com/api/v1/manager/600"
        manager_away_url = "https://www.sofascore.com/api/v1/manager/601"
        top_players_url = "https://www.sofascore.com/api/v1/unique-tournament/132/season/84695/top-players/regularSeason"
        top_players_per_game_url = "https://www.sofascore.com/api/v1/unique-tournament/132/season/84695/top-players-per-game/all/regularSeason"
        top_teams_url = "https://www.sofascore.com/api/v1/unique-tournament/132/season/84695/top-teams/regularSeason"
        player_of_season_url = "https://www.sofascore.com/api/v1/unique-tournament/132/season/84695/player-of-the-season"

        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": 14439306,
                            "slug": "lakers-jazz",
                            "tournament": {
                                "id": 200,
                                "slug": "nba",
                                "name": "NBA",
                                "uniqueTournament": {"id": 132, "slug": "nba", "name": "NBA"},
                            },
                            "season": {"id": 84695, "name": "NBA 25/26", "year": "25/26"},
                            "status": {"type": "inprogress"},
                            "homeTeam": {"id": 44, "slug": "lakers", "name": "Lakers"},
                            "awayTeam": {"id": 45, "slug": "jazz", "name": "Jazz"},
                        }
                    },
                ),
                statistics_url: _json_result(statistics_url, {"statistics": []}),
                incidents_url: _json_result(
                    incidents_url,
                    {"incidents": [{"id": 1, "incidentType": "scoreChange", "time": 1, "player": {"id": 800, "name": "LeBron James"}}]},
                ),
                lineups_url: _json_result(
                    lineups_url,
                    {
                        "home": {"players": [{"position": "G", "teamId": 44, "player": {"id": 800, "slug": "lebron", "name": "LeBron James"}}]},
                        "away": {"players": [{"position": "G", "teamId": 45, "player": {"id": 801, "slug": "markkanen", "name": "Lauri Markkanen"}}]},
                    },
                ),
                team_home_url: _json_result(
                    team_home_url,
                    {"team": {"id": 44, "slug": "lakers", "name": "Lakers", "manager": {"id": 600, "slug": "reddick", "name": "JJ Redick"}}},
                ),
                team_away_url: _json_result(
                    team_away_url,
                    {"team": {"id": 45, "slug": "jazz", "name": "Jazz", "manager": {"id": 601, "slug": "hardy", "name": "Will Hardy"}}},
                ),
                player_home_url: _json_result(
                    player_home_url,
                    {"player": {"id": 800, "slug": "lebron", "name": "LeBron James", "team": {"id": 44, "slug": "lakers", "name": "Lakers"}}},
                ),
                player_away_url: _json_result(
                    player_away_url,
                    {"player": {"id": 801, "slug": "markkanen", "name": "Lauri Markkanen", "team": {"id": 45, "slug": "jazz", "name": "Jazz"}}},
                ),
                manager_home_url: _json_result(
                    manager_home_url,
                    {"manager": {"id": 600, "slug": "reddick", "name": "JJ Redick"}},
                ),
                manager_away_url: _json_result(
                    manager_away_url,
                    {"manager": {"id": 601, "slug": "hardy", "name": "Will Hardy"}},
                ),
                top_players_url: _json_result(top_players_url, {"topPlayers": {"points": []}}),
                top_players_per_game_url: _json_result(top_players_per_game_url, {"topPlayers": {"points": []}}),
                top_teams_url: _json_result(top_teams_url, {"topTeams": {"wins": []}}),
                player_of_season_url: _json_result(player_of_season_url, {"player": {"id": 800, "name": "LeBron James"}}),
            }
        )
        raw_store = _FakeRawSnapshotStore()
        capability_repository = _FakeCapabilityRepository()
        orchestrator = PilotOrchestrator(
            fetch_executor=FetchExecutor(transport=transport, raw_repository=raw_store, sql_executor=object()),
            snapshot_store=raw_store,
            normalize_worker=NormalizeWorker(ParserRegistry.default()),
            planner=Planner(
                capability_rollup={
                    "/api/v1/event/{event_id}/graph": "unsupported",
                    "/api/v1/event/{event_id}/incidents": "unsupported",
                }
            ),
            capability_repository=capability_repository,
            sql_executor=object(),
        )

        report = await orchestrator.run_event(event_id=14439306, sport_slug="basketball")

        self.assertIn(top_players_url, transport.seen_urls)
        self.assertIn(top_players_per_game_url, transport.seen_urls)
        self.assertIn(top_teams_url, transport.seen_urls)
        self.assertIn(player_of_season_url, transport.seen_urls)
        self.assertIn(incidents_url, transport.seen_urls)
        self.assertIn(team_home_url, transport.seen_urls)
        self.assertIn(team_away_url, transport.seen_urls)
        self.assertIn(player_home_url, transport.seen_urls)
        self.assertIn(player_away_url, transport.seen_urls)
        self.assertIn(manager_home_url, transport.seen_urls)
        self.assertIn(manager_away_url, transport.seen_urls)
        self.assertEqual(report.sport_slug, "basketball")
        self.assertTrue(any(item.endpoint_pattern.endswith("/top-players/regularSeason") for item in report.fetch_outcomes))
        self.assertTrue(any(item.support_level == "supported" for item in capability_repository.rollups))


def _json_result(url: str, payload: object, *, status_code: int = 200) -> TransportResult:
    return TransportResult(
        resolved_url=url,
        status_code=status_code,
        headers={"Content-Type": "application/json"},
        body_bytes=json.dumps(payload).encode("utf-8"),
        attempts=(TransportAttempt(1, "proxy_1", status_code, None, None),),
        final_proxy_name="proxy_1",
        challenge_reason=None,
    )


if __name__ == "__main__":
    unittest.main()
