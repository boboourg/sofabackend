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
    async def insert_observation(self, executor, record) -> None:
        del executor, record

    async def upsert_rollup(self, executor, record) -> None:
        del executor, record


class IceHockeyHybridPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_ice_hockey_pipeline_fetches_shotmap_special_route(self) -> None:
        event_url = "https://www.sofascore.com/api/v1/event/14201603"
        statistics_url = "https://www.sofascore.com/api/v1/event/14201603/statistics"
        lineups_url = "https://www.sofascore.com/api/v1/event/14201603/lineups"
        incidents_url = "https://www.sofascore.com/api/v1/event/14201603/incidents"
        shotmap_url = "https://www.sofascore.com/api/v1/event/14201603/shotmap"
        team_home_url = "https://www.sofascore.com/api/v1/team/701"
        team_away_url = "https://www.sofascore.com/api/v1/team/702"
        player_home_url = "https://www.sofascore.com/api/v1/player/1201"
        player_away_url = "https://www.sofascore.com/api/v1/player/1202"
        manager_home_url = "https://www.sofascore.com/api/v1/manager/901"
        manager_away_url = "https://www.sofascore.com/api/v1/manager/902"
        top_players_url = "https://www.sofascore.com/api/v1/unique-tournament/234/season/88001/top-players/regularSeason"
        top_players_per_game_url = "https://www.sofascore.com/api/v1/unique-tournament/234/season/88001/top-players-per-game/all/regularSeason"
        top_teams_url = "https://www.sofascore.com/api/v1/unique-tournament/234/season/88001/top-teams/regularSeason"
        player_of_season_url = "https://www.sofascore.com/api/v1/unique-tournament/234/season/88001/player-of-the-season"

        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": 14201603,
                            "slug": "rangers-bruins",
                            "tournament": {
                                "id": 601,
                                "slug": "nhl",
                                "name": "NHL",
                                "uniqueTournament": {"id": 234, "slug": "nhl", "name": "NHL"},
                            },
                            "season": {"id": 88001, "name": "NHL 2026", "year": "2026"},
                            "status": {"type": "inprogress"},
                            "homeTeam": {"id": 701, "slug": "rangers", "name": "Rangers"},
                            "awayTeam": {"id": 702, "slug": "bruins", "name": "Bruins"},
                        }
                    },
                ),
                statistics_url: _json_result(statistics_url, {"statistics": []}),
                lineups_url: _json_result(
                    lineups_url,
                    {
                        "home": {"players": [{"teamId": 701, "player": {"id": 1201, "slug": "panarin", "name": "Artemi Panarin"}}]},
                        "away": {"players": [{"teamId": 702, "player": {"id": 1202, "slug": "pasta", "name": "David Pastrnak"}}]},
                    },
                ),
                incidents_url: _json_result(incidents_url, {"incidents": [{"id": 1, "incidentType": "goal", "time": 7}]}),
                shotmap_url: _json_result(shotmap_url, {"shotmap": [{"x": 22.0, "y": 18.0, "shotType": "slap"}]}),
                team_home_url: _json_result(team_home_url, {"team": {"id": 701, "slug": "rangers", "name": "Rangers", "manager": {"id": 901, "slug": "laviolette", "name": "Peter Laviolette"}}}),
                team_away_url: _json_result(team_away_url, {"team": {"id": 702, "slug": "bruins", "name": "Bruins", "manager": {"id": 902, "slug": "montgomery", "name": "Jim Montgomery"}}}),
                player_home_url: _json_result(player_home_url, {"player": {"id": 1201, "slug": "panarin", "name": "Artemi Panarin", "team": {"id": 701, "slug": "rangers", "name": "Rangers"}}}),
                player_away_url: _json_result(player_away_url, {"player": {"id": 1202, "slug": "pasta", "name": "David Pastrnak", "team": {"id": 702, "slug": "bruins", "name": "Bruins"}}}),
                manager_home_url: _json_result(manager_home_url, {"manager": {"id": 901, "slug": "laviolette", "name": "Peter Laviolette"}}),
                manager_away_url: _json_result(manager_away_url, {"manager": {"id": 902, "slug": "montgomery", "name": "Jim Montgomery"}}),
                top_players_url: _json_result(top_players_url, {"topPlayers": {"points": []}}),
                top_players_per_game_url: _json_result(top_players_per_game_url, {"topPlayers": {"points": []}}),
                top_teams_url: _json_result(top_teams_url, {"topTeams": {"wins": []}}),
                player_of_season_url: _json_result(player_of_season_url, {"player": {"id": 1201, "name": "Artemi Panarin"}}),
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

        report = await orchestrator.run_event(event_id=14201603, sport_slug="ice-hockey")

        self.assertIn(incidents_url, transport.seen_urls)
        self.assertIn(shotmap_url, transport.seen_urls)
        self.assertIn(manager_home_url, transport.seen_urls)
        self.assertIn("shotmap", {item.parser_family for item in report.parse_results})


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
