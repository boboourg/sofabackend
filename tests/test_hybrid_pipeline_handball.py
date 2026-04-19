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

    async def insert_payload_snapshot_if_missing_returning_id(self, executor, record) -> int:
        return await self.insert_payload_snapshot_returning_id(executor, record)

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
    async def insert_observation(self, executor, record) -> None:
        del executor, record

    async def upsert_rollup(self, executor, record) -> None:
        del executor, record


class HandballHybridPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_handball_pipeline_uses_football_like_adapter(self) -> None:
        event_url = "https://www.sofascore.com/api/v1/event/13001"
        statistics_url = "https://www.sofascore.com/api/v1/event/13001/statistics"
        lineups_url = "https://www.sofascore.com/api/v1/event/13001/lineups"
        incidents_url = "https://www.sofascore.com/api/v1/event/13001/incidents"
        team_home_url = "https://www.sofascore.com/api/v1/team/501"
        team_away_url = "https://www.sofascore.com/api/v1/team/502"
        player_home_url = "https://www.sofascore.com/api/v1/player/901"
        player_away_url = "https://www.sofascore.com/api/v1/player/902"
        manager_home_url = "https://www.sofascore.com/api/v1/manager/701"
        manager_away_url = "https://www.sofascore.com/api/v1/manager/702"
        top_players_url = "https://www.sofascore.com/api/v1/unique-tournament/30/season/99001/top-players/overall"
        top_players_per_game_url = "https://www.sofascore.com/api/v1/unique-tournament/30/season/99001/top-players-per-game/all/overall"
        top_teams_url = "https://www.sofascore.com/api/v1/unique-tournament/30/season/99001/top-teams/overall"
        player_of_season_url = "https://www.sofascore.com/api/v1/unique-tournament/30/season/99001/player-of-the-season"

        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": 13001,
                            "slug": "barca-kiel",
                            "tournament": {
                                "id": 301,
                                "slug": "ehf-champions-league",
                                "name": "EHF Champions League",
                                "uniqueTournament": {"id": 30, "slug": "ehf-champions-league", "name": "EHF Champions League"},
                            },
                            "season": {"id": 99001, "name": "EHF Champions League 26", "year": "2026"},
                            "status": {"type": "inprogress"},
                            "homeTeam": {"id": 501, "slug": "barca", "name": "Barca"},
                            "awayTeam": {"id": 502, "slug": "kiel", "name": "Kiel"},
                        }
                    },
                ),
                statistics_url: _json_result(statistics_url, {"statistics": [{"period": "ALL", "groups": []}]}),
                lineups_url: _json_result(
                    lineups_url,
                    {
                        "home": {"players": [{"teamId": 501, "player": {"id": 901, "slug": "mem", "name": "Dika Mem"}}]},
                        "away": {"players": [{"teamId": 502, "player": {"id": 902, "slug": "bilyk", "name": "Nikola Bilyk"}}]},
                    },
                ),
                incidents_url: _json_result(
                    incidents_url,
                    {"incidents": [{"id": 1, "incidentType": "goal", "time": 1, "player": {"id": 901, "name": "Dika Mem"}}]},
                ),
                team_home_url: _json_result(
                    team_home_url,
                    {"team": {"id": 501, "slug": "barca", "name": "Barca", "manager": {"id": 701, "slug": "ortega", "name": "Carlos Ortega"}}},
                ),
                team_away_url: _json_result(
                    team_away_url,
                    {"team": {"id": 502, "slug": "kiel", "name": "Kiel", "manager": {"id": 702, "slug": "jicha", "name": "Filip Jicha"}}},
                ),
                player_home_url: _json_result(
                    player_home_url,
                    {"player": {"id": 901, "slug": "mem", "name": "Dika Mem", "team": {"id": 501, "slug": "barca", "name": "Barca"}}},
                ),
                player_away_url: _json_result(
                    player_away_url,
                    {"player": {"id": 902, "slug": "bilyk", "name": "Nikola Bilyk", "team": {"id": 502, "slug": "kiel", "name": "Kiel"}}},
                ),
                manager_home_url: _json_result(manager_home_url, {"manager": {"id": 701, "slug": "ortega", "name": "Carlos Ortega"}}),
                manager_away_url: _json_result(manager_away_url, {"manager": {"id": 702, "slug": "jicha", "name": "Filip Jicha"}}),
                top_players_url: _json_result(top_players_url, {"topPlayers": {"goals": []}}),
                top_players_per_game_url: _json_result(top_players_per_game_url, {"topPlayers": {"goals": []}}),
                top_teams_url: _json_result(top_teams_url, {"topTeams": {"wins": []}}),
                player_of_season_url: _json_result(player_of_season_url, {"player": {"id": 901, "name": "Dika Mem"}}),
            }
        )
        raw_store = _FakeRawSnapshotStore()
        orchestrator = PilotOrchestrator(
            fetch_executor=FetchExecutor(transport=transport, raw_repository=raw_store, sql_executor=object()),
            snapshot_store=raw_store,
            normalize_worker=NormalizeWorker(ParserRegistry.default()),
            planner=Planner(capability_rollup={"/api/v1/event/{event_id}/graph": "unsupported"}),
            capability_repository=_FakeCapabilityRepository(),
            sql_executor=object(),
        )

        report = await orchestrator.run_event(event_id=13001, sport_slug="handball")

        self.assertIn(incidents_url, transport.seen_urls)
        self.assertIn(manager_home_url, transport.seen_urls)
        self.assertIn(top_players_url, transport.seen_urls)
        self.assertEqual(report.sport_slug, "handball")
        self.assertIn("event_incidents", {item.parser_family for item in report.parse_results})


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
