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
        self.request_logs = []
        self.snapshot_heads = []
        self.snapshots_by_id = {}
        self._next_id = 1

    async def insert_request_log(self, executor, record) -> None:
        del executor
        self.request_logs.append(record)

    async def insert_payload_snapshot_returning_id(self, executor, record) -> int:
        del executor
        snapshot_id = self._next_id
        self._next_id += 1
        self.snapshots_by_id[snapshot_id] = record
        return snapshot_id

    async def upsert_snapshot_head(self, executor, record) -> None:
        del executor
        self.snapshot_heads.append(record)

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
        self.observations = []
        self.rollups = []

    async def insert_observation(self, executor, record) -> None:
        del executor
        self.observations.append(record)

    async def upsert_rollup(self, executor, record) -> None:
        del executor
        self.rollups.append(record)


class FootballHybridPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_football_core_mode_skips_heavy_followup_hydration(self) -> None:
        event_url = "https://www.sofascore.com/api/v1/event/14083191"
        statistics_url = "https://www.sofascore.com/api/v1/event/14083191/statistics"
        lineups_url = "https://www.sofascore.com/api/v1/event/14083191/lineups"
        incidents_url = "https://www.sofascore.com/api/v1/event/14083191/incidents"
        best_players_url = "https://www.sofascore.com/api/v1/event/14083191/best-players/summary"
        player_statistics_home_url = "https://www.sofascore.com/api/v1/event/14083191/player/700/statistics"
        team_home_url = "https://www.sofascore.com/api/v1/team/42"

        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": 14083191,
                            "slug": "arsenal-chelsea",
                            "tournament": {
                                "id": 100,
                                "slug": "premier-league",
                                "name": "Premier League",
                                "uniqueTournament": {"id": 17, "slug": "premier-league", "name": "Premier League"},
                            },
                            "season": {"id": 76986, "name": "Premier League 25/26", "year": "25/26"},
                            "status": {"type": "scheduled"},
                            "homeTeam": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                            "awayTeam": {"id": 43, "slug": "chelsea", "name": "Chelsea"},
                        }
                    },
                ),
                statistics_url: _json_result(
                    statistics_url,
                    {
                        "statistics": [
                            {
                                "period": "ALL",
                                "groups": [
                                    {
                                        "groupName": "Match overview",
                                        "statisticsItems": [
                                            {"name": "Possession", "home": "55%", "away": "45%"},
                                        ],
                                    }
                                ],
                            }
                        ]
                    },
                ),
                incidents_url: _json_result(incidents_url, {"incidents": []}),
                lineups_url: _json_result(
                    lineups_url,
                    {
                        "home": {
                            "formation": "4-3-3",
                            "players": [
                                {
                                    "avgRating": 7.9,
                                    "position": "F",
                                    "teamId": 42,
                                    "substitute": False,
                                    "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                                }
                            ],
                        },
                        "away": {
                            "formation": "4-2-3-1",
                            "players": [
                                {
                                    "avgRating": 7.1,
                                    "position": "M",
                                    "teamId": 43,
                                    "substitute": False,
                                    "player": {"id": 701, "slug": "palmer", "name": "Cole Palmer"},
                                }
                            ],
                        },
                    },
                ),
            }
        )
        raw_store = _FakeRawSnapshotStore()
        capability_repository = _FakeCapabilityRepository()
        fetch_executor = FetchExecutor(transport=transport, raw_repository=raw_store, sql_executor=object())
        orchestrator = PilotOrchestrator(
            fetch_executor=fetch_executor,
            snapshot_store=raw_store,
            normalize_worker=NormalizeWorker(ParserRegistry.default()),
            planner=Planner(
                capability_rollup={
                    "/api/v1/event/{event_id}/graph": "unsupported",
                }
            ),
            capability_repository=capability_repository,
            sql_executor=object(),
        )

        report = await orchestrator.run_event(event_id=14083191, sport_slug="football", hydration_mode="core")

        self.assertIn(event_url, transport.seen_urls)
        self.assertIn(statistics_url, transport.seen_urls)
        self.assertIn(lineups_url, transport.seen_urls)
        self.assertIn(incidents_url, transport.seen_urls)
        self.assertNotIn(best_players_url, transport.seen_urls)
        self.assertNotIn(player_statistics_home_url, transport.seen_urls)
        self.assertNotIn(team_home_url, transport.seen_urls)
        self.assertEqual(
            {item.parser_family for item in report.parse_results},
            {
                "event_root",
                "event_statistics",
                "event_lineups",
                "event_incidents",
            },
        )

    async def test_football_pipeline_fetches_root_statistics_and_lineups(self) -> None:
        event_url = "https://www.sofascore.com/api/v1/event/14083191"
        statistics_url = "https://www.sofascore.com/api/v1/event/14083191/statistics"
        lineups_url = "https://www.sofascore.com/api/v1/event/14083191/lineups"
        incidents_url = "https://www.sofascore.com/api/v1/event/14083191/incidents"
        best_players_url = "https://www.sofascore.com/api/v1/event/14083191/best-players/summary"
        player_statistics_home_url = "https://www.sofascore.com/api/v1/event/14083191/player/700/statistics"
        player_statistics_away_url = "https://www.sofascore.com/api/v1/event/14083191/player/701/statistics"
        player_breakdown_home_url = "https://www.sofascore.com/api/v1/event/14083191/player/700/rating-breakdown"
        player_breakdown_away_url = "https://www.sofascore.com/api/v1/event/14083191/player/701/rating-breakdown"
        team_home_url = "https://www.sofascore.com/api/v1/team/42"
        team_away_url = "https://www.sofascore.com/api/v1/team/43"
        player_home_url = "https://www.sofascore.com/api/v1/player/700"
        player_away_url = "https://www.sofascore.com/api/v1/player/701"
        manager_home_url = "https://www.sofascore.com/api/v1/manager/500"
        manager_away_url = "https://www.sofascore.com/api/v1/manager/501"

        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": 14083191,
                            "slug": "arsenal-chelsea",
                            "tournament": {
                                "id": 100,
                                "slug": "premier-league",
                                "name": "Premier League",
                                "uniqueTournament": {"id": 17, "slug": "premier-league", "name": "Premier League"},
                            },
                            "season": {"id": 76986, "name": "Premier League 25/26", "year": "25/26"},
                            "status": {"type": "inprogress"},
                            "homeTeam": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                            "awayTeam": {"id": 43, "slug": "chelsea", "name": "Chelsea"},
                        }
                    },
                ),
                statistics_url: _json_result(
                    statistics_url,
                    {
                        "statistics": [
                            {
                                "period": "ALL",
                                "groups": [
                                    {
                                        "groupName": "Match overview",
                                        "statisticsItems": [
                                            {"name": "Possession", "home": "55%", "away": "45%"},
                                        ],
                                    }
                                ],
                            }
                        ]
                    },
                ),
                incidents_url: _json_result(
                    incidents_url,
                    {
                        "incidents": [
                            {
                                "id": 1,
                                "incidentType": "goal",
                                "time": 17,
                                "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                                "team": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                                "homeScore": 1,
                                "awayScore": 0,
                            }
                        ]
                    },
                ),
                lineups_url: _json_result(
                    lineups_url,
                    {
                        "home": {
                            "formation": "4-3-3",
                            "players": [
                                {
                                    "avgRating": 7.9,
                                    "position": "F",
                                    "teamId": 42,
                                    "substitute": False,
                                    "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                                }
                            ],
                        },
                        "away": {
                            "formation": "4-2-3-1",
                            "players": [
                                {
                                    "avgRating": 7.1,
                                    "position": "M",
                                    "teamId": 43,
                                    "substitute": False,
                                    "player": {"id": 701, "slug": "palmer", "name": "Cole Palmer"},
                                }
                            ],
                        },
                    },
                ),
                best_players_url: _json_result(
                    best_players_url,
                    {
                        "bestHomeTeamPlayers": [
                            {
                                "label": "rating",
                                "value": "7.9",
                                "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                            }
                        ],
                        "bestAwayTeamPlayers": [
                            {
                                "label": "rating",
                                "value": "7.1",
                                "player": {"id": 701, "slug": "palmer", "name": "Cole Palmer"},
                            }
                        ],
                        "playerOfTheMatch": {
                            "label": "rating",
                            "value": "7.9",
                            "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                        },
                    },
                ),
                player_statistics_home_url: _json_result(
                    player_statistics_home_url,
                    {
                        "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                        "team": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                        "position": "F",
                        "statistics": {
                            "minutesPlayed": 90,
                            "goals": 1,
                            "rating": 7.9,
                            "ratingVersions": {"original": 7.8, "alternative": 7.6},
                            "statisticsType": {"statisticsType": "player", "sportSlug": "football"},
                        },
                        "extra": None,
                    },
                ),
                player_statistics_away_url: _json_result(
                    player_statistics_away_url,
                    {
                        "player": {"id": 701, "slug": "palmer", "name": "Cole Palmer"},
                        "team": {"id": 43, "slug": "chelsea", "name": "Chelsea"},
                        "position": "M",
                        "statistics": {
                            "minutesPlayed": 90,
                            "goals": 0,
                            "rating": 7.1,
                            "ratingVersions": {"original": 7.0, "alternative": 6.9},
                            "statisticsType": {"statisticsType": "player", "sportSlug": "football"},
                        },
                        "extra": None,
                    },
                ),
                player_breakdown_home_url: _json_result(
                    player_breakdown_home_url,
                    {
                        "passes": [
                            {
                                "eventActionType": "pass",
                                "isHome": True,
                                "keypass": False,
                                "outcome": True,
                                "playerCoordinates": {"x": 80.5, "y": 58.9},
                                "passEndCoordinates": {"x": 96.9, "y": 60.3},
                            }
                        ],
                        "dribbles": [],
                        "defensive": [],
                        "ball-carries": [],
                    },
                ),
                player_breakdown_away_url: _json_result(
                    player_breakdown_away_url,
                    {
                        "passes": [],
                        "dribbles": [
                            {
                                "eventActionType": "dribble",
                                "isHome": False,
                                "keypass": False,
                                "outcome": True,
                                "playerCoordinates": {"x": 60.0, "y": 40.0},
                            }
                        ],
                        "defensive": [],
                        "ball-carries": [],
                    },
                ),
                team_home_url: _json_result(
                    team_home_url,
                    {
                        "team": {
                            "id": 42,
                            "slug": "arsenal",
                            "name": "Arsenal",
                            "manager": {"id": 500, "slug": "arteta", "name": "Mikel Arteta"},
                            "venue": {"id": 55, "slug": "emirates", "name": "Emirates Stadium"},
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                        }
                    },
                ),
                team_away_url: _json_result(
                    team_away_url,
                    {
                        "team": {
                            "id": 43,
                            "slug": "chelsea",
                            "name": "Chelsea",
                            "manager": {"id": 501, "slug": "maresca", "name": "Enzo Maresca"},
                            "venue": {"id": 56, "slug": "bridge", "name": "Stamford Bridge"},
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                        }
                    },
                ),
                player_home_url: _json_result(
                    player_home_url,
                    {
                        "player": {
                            "id": 700,
                            "slug": "saka",
                            "name": "Bukayo Saka",
                            "team": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                        }
                    },
                ),
                player_away_url: _json_result(
                    player_away_url,
                    {
                        "player": {
                            "id": 701,
                            "slug": "palmer",
                            "name": "Cole Palmer",
                            "team": {"id": 43, "slug": "chelsea", "name": "Chelsea"},
                        }
                    },
                ),
                manager_home_url: _json_result(
                    manager_home_url,
                    {
                        "manager": {
                            "id": 500,
                            "slug": "arteta",
                            "name": "Mikel Arteta",
                        }
                    },
                ),
                manager_away_url: _json_result(
                    manager_away_url,
                    {
                        "manager": {
                            "id": 501,
                            "slug": "maresca",
                            "name": "Enzo Maresca",
                        }
                    },
                ),
            }
        )
        raw_store = _FakeRawSnapshotStore()
        capability_repository = _FakeCapabilityRepository()
        fetch_executor = FetchExecutor(transport=transport, raw_repository=raw_store, sql_executor=object())
        orchestrator = PilotOrchestrator(
            fetch_executor=fetch_executor,
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

        report = await orchestrator.run_event(event_id=14083191, sport_slug="football")

        self.assertIn(event_url, transport.seen_urls)
        self.assertIn(statistics_url, transport.seen_urls)
        self.assertIn(lineups_url, transport.seen_urls)
        self.assertIn(incidents_url, transport.seen_urls)
        self.assertIn(best_players_url, transport.seen_urls)
        self.assertIn(player_statistics_home_url, transport.seen_urls)
        self.assertIn(player_statistics_away_url, transport.seen_urls)
        self.assertIn(player_breakdown_home_url, transport.seen_urls)
        self.assertIn(player_breakdown_away_url, transport.seen_urls)
        self.assertIn(team_home_url, transport.seen_urls)
        self.assertIn(team_away_url, transport.seen_urls)
        self.assertIn(player_home_url, transport.seen_urls)
        self.assertIn(player_away_url, transport.seen_urls)
        self.assertIn(manager_home_url, transport.seen_urls)
        self.assertIn(manager_away_url, transport.seen_urls)
        self.assertEqual(report.sport_slug, "football")
        self.assertEqual(
            {item.parser_family for item in report.parse_results},
            {
                "event_root",
                "event_statistics",
                "event_lineups",
                "event_incidents",
                "event_best_players",
                "event_player_statistics",
                "event_player_rating_breakdown",
                "entity_profiles",
            },
        )
        observed_patterns = {item.endpoint_pattern for item in capability_repository.observations}
        self.assertIn("/api/v1/event/{event_id}", observed_patterns)
        self.assertIn("/api/v1/event/{event_id}/statistics", observed_patterns)
        self.assertIn("/api/v1/event/{event_id}/lineups", observed_patterns)
        self.assertIn("/api/v1/event/{event_id}/best-players/summary", observed_patterns)
        self.assertIn("/api/v1/event/{event_id}/player/{player_id}/statistics", observed_patterns)
        self.assertIn("/api/v1/event/{event_id}/player/{player_id}/rating-breakdown", observed_patterns)


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
