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


class BaseballHybridPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_baseball_core_pipeline_still_fetches_innings_special(self) -> None:
        event_url = "https://www.sofascore.com/api/v1/event/15507996"
        statistics_url = "https://www.sofascore.com/api/v1/event/15507996/statistics"
        lineups_url = "https://www.sofascore.com/api/v1/event/15507996/lineups"
        incidents_url = "https://www.sofascore.com/api/v1/event/15507996/incidents"
        innings_url = "https://www.sofascore.com/api/v1/event/15507996/innings"

        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": 15507996,
                            "slug": "dodgers-yankees",
                            "tournament": {
                                "id": 401,
                                "slug": "mlb",
                                "name": "MLB",
                                "uniqueTournament": {"id": 11205, "slug": "mlb", "name": "MLB"},
                            },
                            "season": {"id": 84695, "name": "MLB 2026", "year": "2026"},
                            "status": {"type": "notstarted"},
                            "homeTeam": {"id": 601, "slug": "dodgers", "name": "Dodgers"},
                            "awayTeam": {"id": 602, "slug": "yankees", "name": "Yankees"},
                        }
                    },
                ),
                statistics_url: _json_result(statistics_url, {"statistics": []}),
                lineups_url: _json_result(lineups_url, {"home": {"players": []}, "away": {"players": []}}),
                incidents_url: _json_result(incidents_url, {"incidents": []}),
                innings_url: _json_result(innings_url, {"innings": [{"inning": 1, "homeScore": 0, "awayScore": 1}]}),
            }
        )
        raw_store = _FakeRawSnapshotStore()
        orchestrator = PilotOrchestrator(
            fetch_executor=FetchExecutor(transport=transport, raw_repository=raw_store, sql_executor=object()),
            snapshot_store=raw_store,
            normalize_worker=NormalizeWorker(ParserRegistry.default()),
            planner=Planner(capability_rollup={}),
            capability_repository=_FakeCapabilityRepository(),
            sql_executor=object(),
        )

        report = await orchestrator.run_event(event_id=15507996, sport_slug="baseball", hydration_mode="core")

        self.assertIn(incidents_url, transport.seen_urls)
        self.assertIn(innings_url, transport.seen_urls)
        self.assertNotIn("https://www.sofascore.com/api/v1/event/15507996/comments", transport.seen_urls)
        self.assertIn("baseball_innings", {item.parser_family for item in report.parse_results})

    async def test_baseball_pipeline_uses_regular_season_adapter_and_innings_special(self) -> None:
        event_url = "https://www.sofascore.com/api/v1/event/15507996"
        statistics_url = "https://www.sofascore.com/api/v1/event/15507996/statistics"
        lineups_url = "https://www.sofascore.com/api/v1/event/15507996/lineups"
        innings_url = "https://www.sofascore.com/api/v1/event/15507996/innings"
        pitches_url = "https://www.sofascore.com/api/v1/event/15507996/atbat/981540/pitches"
        team_home_url = "https://www.sofascore.com/api/v1/team/601"
        team_away_url = "https://www.sofascore.com/api/v1/team/602"
        player_home_url = "https://www.sofascore.com/api/v1/player/1001"
        player_away_url = "https://www.sofascore.com/api/v1/player/1002"
        manager_home_url = "https://www.sofascore.com/api/v1/manager/801"
        manager_away_url = "https://www.sofascore.com/api/v1/manager/802"
        top_players_url = "https://www.sofascore.com/api/v1/unique-tournament/11205/season/84695/top-players/regularSeason"
        top_players_per_game_url = "https://www.sofascore.com/api/v1/unique-tournament/11205/season/84695/top-players-per-game/all/regularSeason"
        top_teams_url = "https://www.sofascore.com/api/v1/unique-tournament/11205/season/84695/top-teams/regularSeason"
        player_of_season_url = "https://www.sofascore.com/api/v1/unique-tournament/11205/season/84695/player-of-the-season"
        incidents_url = "https://www.sofascore.com/api/v1/event/15507996/incidents"

        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": 15507996,
                            "slug": "dodgers-yankees",
                            "tournament": {
                                "id": 401,
                                "slug": "mlb",
                                "name": "MLB",
                                "uniqueTournament": {"id": 11205, "slug": "mlb", "name": "MLB"},
                            },
                            "season": {"id": 84695, "name": "MLB 2026", "year": "2026"},
                            "status": {"type": "inprogress"},
                            "homeTeam": {"id": 601, "slug": "dodgers", "name": "Dodgers"},
                            "awayTeam": {"id": 602, "slug": "yankees", "name": "Yankees"},
                        }
                    },
                ),
                statistics_url: _json_result(statistics_url, {"statistics": []}),
                lineups_url: _json_result(
                    lineups_url,
                    {
                        "home": {"players": [{"teamId": 601, "player": {"id": 1001, "slug": "betts", "name": "Mookie Betts"}}]},
                        "away": {"players": [{"teamId": 602, "player": {"id": 1002, "slug": "judge", "name": "Aaron Judge"}}]},
                    },
                ),
                incidents_url: _json_result(
                    incidents_url,
                    {"incidents": [{"id": 10, "incidentType": "atBat", "atBatId": 981540, "time": 7}]},
                ),
                innings_url: _json_result(innings_url, {"innings": [{"inning": 1, "homeScore": 0, "awayScore": 1}]}),
                pitches_url: _json_result(
                    pitches_url,
                    {
                        "pitches": [
                            {
                                "id": 5,
                                "pitchType": "SL",
                                "pitchSpeed": 87.2,
                                "pitcher": {"id": 7003, "name": "Pitcher Two"},
                                "hitter": {"id": 7004, "name": "Hitter Two"},
                            }
                        ]
                    },
                ),
                team_home_url: _json_result(
                    team_home_url,
                    {"team": {"id": 601, "slug": "dodgers", "name": "Dodgers", "manager": {"id": 801, "slug": "roberts", "name": "Dave Roberts"}}},
                ),
                team_away_url: _json_result(
                    team_away_url,
                    {"team": {"id": 602, "slug": "yankees", "name": "Yankees", "manager": {"id": 802, "slug": "boone", "name": "Aaron Boone"}}},
                ),
                player_home_url: _json_result(
                    player_home_url,
                    {"player": {"id": 1001, "slug": "betts", "name": "Mookie Betts", "team": {"id": 601, "slug": "dodgers", "name": "Dodgers"}}},
                ),
                player_away_url: _json_result(
                    player_away_url,
                    {"player": {"id": 1002, "slug": "judge", "name": "Aaron Judge", "team": {"id": 602, "slug": "yankees", "name": "Yankees"}}},
                ),
                manager_home_url: _json_result(manager_home_url, {"manager": {"id": 801, "slug": "roberts", "name": "Dave Roberts"}}),
                manager_away_url: _json_result(manager_away_url, {"manager": {"id": 802, "slug": "boone", "name": "Aaron Boone"}}),
                top_players_url: _json_result(top_players_url, {"topPlayers": {"homeRuns": []}}),
                top_players_per_game_url: _json_result(top_players_per_game_url, {"topPlayers": {"homeRuns": []}}),
                top_teams_url: _json_result(top_teams_url, {"topTeams": {"wins": []}}),
                player_of_season_url: _json_result(player_of_season_url, {"player": {"id": 1001, "name": "Mookie Betts"}}),
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

        report = await orchestrator.run_event(event_id=15507996, sport_slug="baseball")

        self.assertIn(innings_url, transport.seen_urls)
        self.assertIn(incidents_url, transport.seen_urls)
        self.assertIn(pitches_url, transport.seen_urls)
        self.assertIn(manager_home_url, transport.seen_urls)
        self.assertIn("baseball_innings", {item.parser_family for item in report.parse_results})
        self.assertIn("baseball_pitches", {item.parser_family for item in report.parse_results})


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
