from __future__ import annotations

import json
import unittest

from schema_inspector.fetch_executor import FetchExecutor
from schema_inspector.normalizers.sink import DurableNormalizeSink
from schema_inspector.normalizers.worker import NormalizeWorker
from schema_inspector.parsers.registry import ParserRegistry
from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator
from schema_inspector.planner.planner import Planner
from schema_inspector.runtime import TransportAttempt, TransportResult
from schema_inspector.storage.normalize_repository import NormalizeRepository


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


class _FakeSqlExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"

    async def executemany(self, query: str, rows: list[tuple[object, ...]]) -> str:
        self.executemany_calls.append((query, rows))
        return "OK"

    async def fetch(self, query: str, *args: object):
        column = str(query).split("SELECT", 1)[1].split("FROM", 1)[0].strip()
        ids = list(args[0]) if args else []
        return [{column: item} for item in ids]


class HybridPipelineDurableSinkTests(unittest.IsolatedAsyncioTestCase):
    async def test_football_pipeline_persists_statistics_incidents_and_graph(self) -> None:
        event_id = 14083191
        event_url = f"https://www.sofascore.com/api/v1/event/{event_id}"
        statistics_url = f"https://www.sofascore.com/api/v1/event/{event_id}/statistics"
        lineups_url = f"https://www.sofascore.com/api/v1/event/{event_id}/lineups"
        incidents_url = f"https://www.sofascore.com/api/v1/event/{event_id}/incidents"
        graph_url = f"https://www.sofascore.com/api/v1/event/{event_id}/graph"
        best_players_url = f"https://www.sofascore.com/api/v1/event/{event_id}/best-players/summary"
        player_statistics_url = f"https://www.sofascore.com/api/v1/event/{event_id}/player/700/statistics"
        player_breakdown_url = f"https://www.sofascore.com/api/v1/event/{event_id}/player/700/rating-breakdown"
        team_url = "https://www.sofascore.com/api/v1/team/42"
        player_url = "https://www.sofascore.com/api/v1/player/700"
        manager_url = "https://www.sofascore.com/api/v1/manager/500"

        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": event_id,
                            "slug": "arsenal-chelsea",
                            "detailId": 1,
                            "season": {"id": 76986, "name": "Premier League 25/26", "year": "25/26"},
                            "status": {"type": "inprogress"},
                            "startTimestamp": 1_800_000_000,
                            "hasEventPlayerStatistics": True,
                            "hasEventPlayerHeatMap": False,
                            "hasXg": False,
                            "homeTeam": {"id": 42, "slug": "arsenal", "name": "Arsenal", "manager": {"id": 500, "slug": "arteta", "name": "Mikel Arteta"}},
                            "awayTeam": {"id": 43, "slug": "chelsea", "name": "Chelsea"},
                        }
                    },
                ),
                statistics_url: _json_result(
                    statistics_url,
                    {
                        "statistics": [
                            {"period": "ALL", "groups": [{"groupName": "Overview", "statisticsItems": [{"name": "Shots", "home": 5, "away": 3}]}]}
                        ]
                    },
                ),
                lineups_url: _json_result(
                    lineups_url,
                    {
                        "home": {"formation": "4-3-3", "players": [{"teamId": 42, "substitute": False, "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"}}]},
                        "away": {"formation": "4-2-3-1", "players": []},
                    },
                ),
                incidents_url: _json_result(incidents_url, {"incidents": [{"id": 1, "incidentType": "goal", "time": 17, "homeScore": 1, "awayScore": 0}]}),
                graph_url: _json_result(graph_url, {"periodTime": 74, "periodCount": 2, "overtimeLength": 0, "graphPoints": [{"minute": 17, "value": 1}]}),
                best_players_url: _json_result(best_players_url, {"bestHomeTeamPlayers": [{"label": "rating", "value": "7.9", "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"}}], "bestAwayTeamPlayers": [], "playerOfTheMatch": {"label": "rating", "value": "7.9", "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"}}}),
                player_statistics_url: _json_result(player_statistics_url, {"player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"}, "team": {"id": 42, "slug": "arsenal", "name": "Arsenal"}, "position": "F", "statistics": {"goals": 1, "rating": 7.9, "statisticsType": {"statisticsType": "player", "sportSlug": "football"}}, "extra": None}),
                player_breakdown_url: _json_result(player_breakdown_url, {"passes": [{"eventActionType": "pass", "isHome": True, "outcome": True, "playerCoordinates": {"x": 1.0, "y": 2.0}, "passEndCoordinates": {"x": 3.0, "y": 4.0}}]}),
                team_url: _json_result(team_url, {"team": {"id": 42, "slug": "arsenal", "name": "Arsenal", "manager": {"id": 500, "slug": "arteta", "name": "Mikel Arteta"}}}),
                player_url: _json_result(player_url, {"player": {"id": 700, "slug": "saka", "name": "Bukayo Saka", "team": {"id": 42, "slug": "arsenal", "name": "Arsenal"}}}),
                manager_url: _json_result(manager_url, {"manager": {"id": 500, "slug": "arteta", "name": "Mikel Arteta"}}),
            }
        )
        raw_store = _FakeRawSnapshotStore()
        sql_executor = _FakeSqlExecutor()
        orchestrator = PilotOrchestrator(
            fetch_executor=FetchExecutor(transport=transport, raw_repository=raw_store, sql_executor=sql_executor),
            snapshot_store=raw_store,
            normalize_worker=NormalizeWorker(
                ParserRegistry.default(),
                result_sink=DurableNormalizeSink(NormalizeRepository(), sql_executor),
            ),
            planner=Planner(
                capability_rollup={
                    "/api/v1/event/{event_id}/graph": "supported",
                    "/api/v1/event/{event_id}/incidents": "supported",
                    "/api/v1/event/{event_id}/best-players/summary": "supported",
                    "/api/v1/event/{event_id}/player/{player_id}/statistics": "supported",
                    "/api/v1/event/{event_id}/player/{player_id}/rating-breakdown": "supported",
                }
            ),
            capability_repository=_FakeCapabilityRepository(),
            sql_executor=sql_executor,
        )

        await orchestrator.run_event(event_id=event_id, sport_slug="football")

        statements = [sql for sql, _ in sql_executor.execute_calls] + [sql for sql, _ in sql_executor.executemany_calls]
        self.assertIn(graph_url, transport.seen_urls)
        self.assertTrue(any("INSERT INTO event_statistic" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_incident" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_graph" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_graph_point" in sql for sql in statements))

    async def test_core_pipeline_skips_root_entity_hierarchy_writes(self) -> None:
        event_id = 14083191
        event_url = f"https://www.sofascore.com/api/v1/event/{event_id}"
        statistics_url = f"https://www.sofascore.com/api/v1/event/{event_id}/statistics"
        lineups_url = f"https://www.sofascore.com/api/v1/event/{event_id}/lineups"
        incidents_url = f"https://www.sofascore.com/api/v1/event/{event_id}/incidents"
        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": event_id,
                            "slug": "arsenal-chelsea",
                            "detailId": 1,
                            "season": {"id": 76986, "name": "Premier League 25/26", "year": "25/26"},
                            "status": {"type": "inprogress"},
                            "startTimestamp": 1_800_000_000,
                            "hasEventPlayerStatistics": True,
                            "hasEventPlayerHeatMap": False,
                            "hasXg": False,
                            "tournament": {
                                "id": 100,
                                "slug": "premier-league",
                                "name": "Premier League",
                                "category": {
                                    "id": 1,
                                    "slug": "england",
                                    "name": "England",
                                    "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                                    "sport": {"id": 1, "slug": "football", "name": "Football"},
                                },
                                "uniqueTournament": {"id": 17, "slug": "premier-league", "name": "Premier League"},
                            },
                            "homeTeam": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                            "awayTeam": {"id": 43, "slug": "chelsea", "name": "Chelsea"},
                        }
                    },
                ),
                statistics_url: _json_result(
                    statistics_url,
                    {
                        "statistics": [
                            {"period": "ALL", "groups": [{"groupName": "Overview", "statisticsItems": [{"name": "Shots", "home": 5, "away": 3}]}]}
                        ]
                    },
                ),
                lineups_url: _json_result(
                    lineups_url,
                    {
                        "home": {"formation": "4-3-3", "players": [{"teamId": 42, "substitute": False, "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"}}]},
                        "away": {"formation": "4-2-3-1", "players": []},
                    },
                ),
                incidents_url: _json_result(
                    incidents_url,
                    {"incidents": [{"id": 1, "incidentType": "goal", "time": 17, "homeScore": 1, "awayScore": 0}]},
                ),
            }
        )
        raw_store = _FakeRawSnapshotStore()
        sql_executor = _FakeSqlExecutor()
        orchestrator = PilotOrchestrator(
            fetch_executor=FetchExecutor(transport=transport, raw_repository=raw_store, sql_executor=sql_executor),
            snapshot_store=raw_store,
            normalize_worker=NormalizeWorker(
                ParserRegistry.default(),
                result_sink=DurableNormalizeSink(
                    NormalizeRepository(),
                    sql_executor,
                    skip_entity_parser_families={"event_root"},
                ),
            ),
            planner=Planner(capability_rollup={}),
            capability_repository=_FakeCapabilityRepository(),
            sql_executor=sql_executor,
        )

        await orchestrator.run_event(event_id=event_id, sport_slug="football", hydration_mode="core")

        statements = [sql for sql, _ in sql_executor.executemany_calls]
        self.assertFalse(any("INSERT INTO sport" in sql for sql in statements))
        self.assertFalse(any("INSERT INTO country" in sql for sql in statements))
        self.assertFalse(any("INSERT INTO category" in sql for sql in statements))
        self.assertFalse(any("INSERT INTO unique_tournament" in sql for sql in statements))
        self.assertFalse(any("INSERT INTO season" in sql for sql in statements))
        self.assertFalse(any("INSERT INTO tournament" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_statistic" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_lineup" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_incident" in sql for sql in statements))

    async def test_special_routes_are_persisted_for_tennis_baseball_hockey_and_esports(self) -> None:
        sql_executor = _FakeSqlExecutor()
        raw_store = _FakeRawSnapshotStore()
        sink = DurableNormalizeSink(NormalizeRepository(), sql_executor)
        worker = NormalizeWorker(ParserRegistry.default(), result_sink=sink)

        snapshots = [
            _snapshot(
                1,
                "/api/v1/event/{event_id}/point-by-point",
                "tennis",
                "https://www.sofascore.com/api/v1/event/15981295/point-by-point",
                {"pointByPoint": [{"id": 1, "set": 1, "game": 1, "server": "home", "score": {"home": "15", "away": "0"}}]},
                15981295,
            ),
            _snapshot(
                2,
                "/api/v1/event/{event_id}/tennis-power",
                "tennis",
                "https://www.sofascore.com/api/v1/event/15981295/tennis-power",
                {"tennisPowerRankings": {"home": {"current": 0.61, "delta": 0.02}, "away": {"current": 0.39, "delta": -0.02}}},
                15981295,
            ),
            _snapshot(
                3,
                "/api/v1/event/{event_id}/innings",
                "baseball",
                "https://www.sofascore.com/api/v1/event/15308201/innings",
                {"innings": [{"inning": 1, "homeScore": 0, "awayScore": 1}]},
                15308201,
            ),
            _snapshot(
                4,
                "/api/v1/event/{event_id}/shotmap",
                "ice-hockey",
                "https://www.sofascore.com/api/v1/event/15929810/shotmap",
                {"shotmap": [{"x": 22.0, "y": 18.0, "shotType": "slap"}]},
                15929810,
            ),
            _snapshot(
                5,
                "/api/v1/event/{event_id}/esports-games",
                "esports",
                "https://www.sofascore.com/api/v1/event/16017074/esports-games",
                {"games": [{"id": 1, "status": "finished", "mapName": "Dust2"}]},
                16017074,
            ),
        ]

        for snapshot in snapshots:
            await worker.handle_async(snapshot)

        statements = [sql for sql, _ in sql_executor.execute_calls] + [sql for sql, _ in sql_executor.executemany_calls]
        self.assertTrue(any("INSERT INTO tennis_point_by_point" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO tennis_power" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO baseball_inning" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO shotmap_point" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO esports_game" in sql for sql in statements))


def _snapshot(snapshot_id: int, pattern: str, sport_slug: str, url: str, payload: object, event_id: int):
    from schema_inspector.parsers.base import RawSnapshot

    return RawSnapshot(
        snapshot_id=snapshot_id,
        endpoint_pattern=pattern,
        sport_slug=sport_slug,
        source_url=url,
        resolved_url=url,
        envelope_key="root",
        http_status=200,
        payload=payload,
        fetched_at="2026-04-17T10:00:00+00:00",
        context_entity_type="event",
        context_entity_id=event_id,
        context_event_id=event_id,
    )


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
