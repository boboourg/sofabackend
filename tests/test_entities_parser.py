from __future__ import annotations

import unittest

from schema_inspector.endpoints import (
    PLAYER_ENDPOINT,
    PLAYER_SEASON_HEATMAP_OVERALL_ENDPOINT,
    PLAYER_SEASON_OVERALL_STATISTICS_ENDPOINT,
    PLAYER_STATISTICS_ENDPOINT,
    PLAYER_STATISTICS_SEASONS_ENDPOINT,
    PLAYER_TRANSFER_HISTORY_ENDPOINT,
    TEAM_ENDPOINT,
    TEAM_PERFORMANCE_GRAPH_ENDPOINT,
    TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT,
    TEAM_SEASON_OVERALL_STATISTICS_ENDPOINT,
    TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT,
)
from schema_inspector.runtime import RuntimeConfig, TransportAttempt
from schema_inspector.sofascore_client import SofascoreClient, SofascoreHttpError, SofascoreResponse
from schema_inspector.entities_parser import (
    EntitiesParser,
    PlayerHeatmapRequest,
    PlayerOverallRequest,
    TeamOverallRequest,
    TeamPerformanceGraphRequest,
)


def _sport_payload() -> dict[str, object]:
    return {"id": 1, "slug": "football", "name": "Football"}


def _country_payload() -> dict[str, object]:
    return {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"}


def _category_payload() -> dict[str, object]:
    return {
        "id": 1,
        "slug": "england",
        "name": "England",
        "sport": _sport_payload(),
        "country": _country_payload(),
        "transferPeriod": [
            {"activeFrom": "2026-06-01", "activeTo": "2026-08-31"},
        ],
    }


def _unique_tournament_payload() -> dict[str, object]:
    return {
        "id": 17,
        "slug": "premier-league",
        "name": "Premier League",
        "category": _category_payload(),
    }


def _season_payload() -> dict[str, object]:
    return {"id": 76986, "name": "Premier League 25/26", "year": "25/26", "editor": False}


def _manager_payload(manager_id: int, slug: str, name: str) -> dict[str, object]:
    return {"id": manager_id, "slug": slug, "name": name}


def _team_payload(team_id: int, slug: str, name: str, manager_id: int) -> dict[str, object]:
    return {
        "id": team_id,
        "slug": slug,
        "name": name,
        "shortName": name,
        "sport": _sport_payload(),
        "country": _country_payload(),
        "category": _category_payload(),
        "primaryUniqueTournament": _unique_tournament_payload(),
        "tournament": {
            "id": 100,
            "slug": "premier-league",
            "name": "Premier League",
            "category": _category_payload(),
            "uniqueTournament": _unique_tournament_payload(),
            "competitionType": 1,
        },
        "manager": _manager_payload(manager_id, f"manager-{manager_id}", f"Manager {manager_id}"),
        "venue": {
            "id": 55,
            "slug": "emirates",
            "name": "Emirates Stadium",
            "country": _country_payload(),
            "city": {"name": "London"},
        },
        "gender": "M",
        "type": 0,
        "national": False,
        "disabled": False,
        "userCount": 1000,
        "teamColors": {"primary": "#cc0000", "secondary": "#ffffff", "text": "#ffffff"},
    }


def _player_payload(player_id: int) -> dict[str, object]:
    return {
        "id": player_id,
        "slug": "bukayo-saka",
        "name": "Bukayo Saka",
        "shortName": "B. Saka",
        "team": _team_payload(41, "arsenal", "Arsenal", 500),
        "country": _country_payload(),
        "position": "F",
        "preferredFoot": "Left",
        "jerseyNumber": "7",
        "gender": "M",
        "userCount": 9000,
    }


def _player_statistics_payload(team_id: int) -> dict[str, object]:
    return {
        "seasons": [
            {
                "year": "25/26",
                "startYear": 2025,
                "endYear": 2026,
                "team": _team_payload(team_id, "arsenal", "Arsenal", 500),
                "uniqueTournament": _unique_tournament_payload(),
                "season": _season_payload(),
                "statistics": {
                    "id": 0,
                    "type": "overall",
                    "appearances": 2,
                    "assists": 1,
                    "expectedGoals": 0.96,
                    "expectedAssists": 0.44,
                    "goals": 1,
                    "keyPasses": 7,
                    "rating": 7.15,
                    "accuratePasses": 90,
                    "accuratePassesPercentage": 84.11,
                    "minutesPlayed": 166,
                },
            }
        ],
        "typesMap": {"17": ["overall", "attack"]},
    }


class _FakeSofascoreClient(SofascoreClient):
    def __init__(self, responses: dict[str, object]) -> None:
        super().__init__(RuntimeConfig())
        self.responses = responses
        self.seen_urls: list[str] = []

    async def get_json(self, url: str, *, headers=None, timeout: float = 20.0) -> SofascoreResponse:
        del headers, timeout
        self.seen_urls.append(url)
        value = self.responses[url]
        if isinstance(value, Exception):
            raise value
        return SofascoreResponse(
            source_url=url,
            resolved_url=url,
            fetched_at="2026-04-10T10:00:00+00:00",
            status_code=200,
            headers={"Content-Type": "application/json"},
            body_bytes=b"{}",
            payload=value,
            attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
            final_proxy_name="proxy_1",
            challenge_reason=None,
        )


def _not_found_error(url: str) -> SofascoreHttpError:
    from schema_inspector.runtime import TransportResult

    return SofascoreHttpError(
        f"404 for {url}",
        transport_result=TransportResult(
            resolved_url=url,
            status_code=404,
            headers={},
            body_bytes=b"{}",
            attempts=(TransportAttempt(1, "proxy_1", 404, None, None),),
            final_proxy_name="proxy_1",
            challenge_reason=None,
        ),
    )


class EntitiesParserTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_entity_endpoint_templates_match_sofascore_paths(self) -> None:
        self.assertEqual(
            TEAM_ENDPOINT.build_url(team_id=41),
            "https://www.sofascore.com/api/v1/team/41",
        )
        self.assertEqual(
            PLAYER_ENDPOINT.build_url(player_id=1152),
            "https://www.sofascore.com/api/v1/player/1152",
        )
        self.assertEqual(
            PLAYER_STATISTICS_ENDPOINT.build_url(player_id=1152),
            "https://www.sofascore.com/api/v1/player/1152/statistics",
        )
        self.assertEqual(
            PLAYER_TRANSFER_HISTORY_ENDPOINT.build_url(player_id=1152),
            "https://www.sofascore.com/api/v1/player/1152/transfer-history",
        )
        self.assertEqual(
            TEAM_PERFORMANCE_GRAPH_ENDPOINT.build_url(team_id=42, unique_tournament_id=17, season_id=76986),
            "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/team/42/team-performance-graph-data",
        )

    async def test_entities_parser_builds_expected_bundle(self) -> None:
        player_id = 1152
        team_id = 41
        player_overall = PlayerOverallRequest(player_id=player_id, unique_tournament_id=17, season_id=76986)
        team_overall = TeamOverallRequest(team_id=team_id, unique_tournament_id=17, season_id=76986)
        player_heatmap = PlayerHeatmapRequest(player_id=player_id, unique_tournament_id=17, season_id=76986)
        team_graph = TeamPerformanceGraphRequest(team_id=42, unique_tournament_id=17, season_id=76986)

        responses = {
            TEAM_ENDPOINT.build_url(team_id=team_id): {"team": _team_payload(team_id, "arsenal", "Arsenal", 500)},
            PLAYER_ENDPOINT.build_url(player_id=player_id): {"player": _player_payload(player_id)},
            PLAYER_STATISTICS_ENDPOINT.build_url(player_id=player_id): _player_statistics_payload(team_id),
            PLAYER_TRANSFER_HISTORY_ENDPOINT.build_url(player_id=player_id): {
                "transferHistory": [
                    {
                        "id": 1,
                        "player": _player_payload(player_id),
                        "transferFrom": _team_payload(30, "academy", "Academy", 300),
                        "transferTo": _team_payload(team_id, "arsenal", "Arsenal", 500),
                        "fromTeamName": "Academy",
                        "toTeamName": "Arsenal",
                        "transferDateTimestamp": 1710000000,
                        "transferFee": 0,
                        "transferFeeDescription": "Free",
                        "transferFeeRaw": {"value": 0, "currency": "EUR"},
                        "type": 1,
                    }
                ]
            },
            PLAYER_STATISTICS_SEASONS_ENDPOINT.build_url(player_id=player_id): {
                "uniqueTournamentSeasons": [
                    {
                        "uniqueTournament": _unique_tournament_payload(),
                        "seasons": [_season_payload()],
                        "allTimeSeasonId": 999,
                    }
                ],
                "typesMap": {"17": {"76986": ["summary", "attack"]}},
            },
            TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT.build_url(team_id=team_id): {
                "uniqueTournamentSeasons": [
                    {
                        "uniqueTournament": _unique_tournament_payload(),
                        "seasons": [_season_payload()],
                        "allTimeSeasonId": 888,
                    }
                ],
                "typesMap": {"17": {"76986": ["overall", "attack"]}},
            },
            TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT.build_url(team_id=team_id): {
                "uniqueTournamentSeasons": [
                    {
                        "uniqueTournament": _unique_tournament_payload(),
                        "seasons": [_season_payload()],
                    }
                ],
                "typesMap": {"17": {"76986": ["summary", "passing"]}},
            },
            PLAYER_SEASON_OVERALL_STATISTICS_ENDPOINT.build_url(
                player_id=player_overall.player_id,
                unique_tournament_id=player_overall.unique_tournament_id,
                season_id=player_overall.season_id,
            ): {
                "statistics": {"id": 900, "rating": 7.6},
                "team": _team_payload(team_id, "arsenal", "Arsenal", 500),
            },
            TEAM_SEASON_OVERALL_STATISTICS_ENDPOINT.build_url(
                team_id=team_overall.team_id,
                unique_tournament_id=team_overall.unique_tournament_id,
                season_id=team_overall.season_id,
            ): {
                "statistics": {"id": 901, "goals": 70},
            },
            PLAYER_SEASON_HEATMAP_OVERALL_ENDPOINT.build_url(
                player_id=player_heatmap.player_id,
                unique_tournament_id=player_heatmap.unique_tournament_id,
                season_id=player_heatmap.season_id,
            ): {
                "heatmap": [{"x": 0.1, "y": 0.2}],
                "events": [],
            },
            TEAM_PERFORMANCE_GRAPH_ENDPOINT.build_url(
                team_id=team_graph.team_id,
                unique_tournament_id=team_graph.unique_tournament_id,
                season_id=team_graph.season_id,
            ): {
                "graphData": {"form": ["W", "D", "W"]},
            },
        }
        fake_client = _FakeSofascoreClient(responses)

        parser = EntitiesParser(fake_client)
        bundle = await parser.fetch_bundle(
            player_ids=(player_id,),
            team_ids=(team_id,),
            player_overall_requests=(player_overall,),
            team_overall_requests=(team_overall,),
            player_heatmap_requests=(player_heatmap,),
            team_performance_graph_requests=(team_graph,),
        )

        self.assertEqual(len(bundle.registry_entries), 11)
        self.assertEqual(len(bundle.payload_snapshots), 11)
        self.assertEqual({item.id for item in bundle.players}, {1152})
        self.assertEqual({item.id for item in bundle.teams}, {30, 41})
        self.assertEqual(len(bundle.category_transfer_periods), 1)
        self.assertEqual(len(bundle.transfer_histories), 1)
        self.assertEqual(len(bundle.player_season_statistics), 1)
        self.assertEqual(bundle.player_season_statistics[0].expected_goals, 0.96)
        self.assertEqual(bundle.player_season_statistics[0].assists, 1)
        self.assertEqual(len(bundle.entity_statistics_seasons), 2)
        self.assertEqual(len(bundle.entity_statistics_types), 4)
        self.assertEqual(len(bundle.season_statistics_types), 2)
        self.assertEqual(len(bundle.unique_tournament_seasons), 1)
        self.assertEqual(
            fake_client.seen_urls,
            [
                PLAYER_ENDPOINT.build_url(player_id=player_id),
                PLAYER_STATISTICS_ENDPOINT.build_url(player_id=player_id),
                PLAYER_STATISTICS_SEASONS_ENDPOINT.build_url(player_id=player_id),
                PLAYER_TRANSFER_HISTORY_ENDPOINT.build_url(player_id=player_id),
                TEAM_ENDPOINT.build_url(team_id=team_id),
                TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT.build_url(team_id=team_id),
                TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT.build_url(team_id=team_id),
                PLAYER_SEASON_OVERALL_STATISTICS_ENDPOINT.build_url(
                    player_id=player_overall.player_id,
                    unique_tournament_id=player_overall.unique_tournament_id,
                    season_id=player_overall.season_id,
                ),
                TEAM_SEASON_OVERALL_STATISTICS_ENDPOINT.build_url(
                    team_id=team_overall.team_id,
                    unique_tournament_id=team_overall.unique_tournament_id,
                    season_id=team_overall.season_id,
                ),
                PLAYER_SEASON_HEATMAP_OVERALL_ENDPOINT.build_url(
                    player_id=player_heatmap.player_id,
                    unique_tournament_id=player_heatmap.unique_tournament_id,
                    season_id=player_heatmap.season_id,
                ),
                TEAM_PERFORMANCE_GRAPH_ENDPOINT.build_url(
                    team_id=team_graph.team_id,
                    unique_tournament_id=team_graph.unique_tournament_id,
                    season_id=team_graph.season_id,
                ),
            ],
        )

    async def test_entities_parser_skips_optional_missing_seasonal_endpoints(self) -> None:
        player_id = 1152
        team_id = 41
        player_overall = PlayerOverallRequest(player_id=player_id, unique_tournament_id=17, season_id=76986)
        team_overall = TeamOverallRequest(team_id=team_id, unique_tournament_id=17, season_id=76986)
        player_heatmap = PlayerHeatmapRequest(player_id=player_id, unique_tournament_id=17, season_id=76986)
        team_graph = TeamPerformanceGraphRequest(team_id=42, unique_tournament_id=17, season_id=76986)

        player_overall_url = PLAYER_SEASON_OVERALL_STATISTICS_ENDPOINT.build_url(
            player_id=player_overall.player_id,
            unique_tournament_id=player_overall.unique_tournament_id,
            season_id=player_overall.season_id,
        )
        team_overall_url = TEAM_SEASON_OVERALL_STATISTICS_ENDPOINT.build_url(
            team_id=team_overall.team_id,
            unique_tournament_id=team_overall.unique_tournament_id,
            season_id=team_overall.season_id,
        )
        player_heatmap_url = PLAYER_SEASON_HEATMAP_OVERALL_ENDPOINT.build_url(
            player_id=player_heatmap.player_id,
            unique_tournament_id=player_heatmap.unique_tournament_id,
            season_id=player_heatmap.season_id,
        )
        team_graph_url = TEAM_PERFORMANCE_GRAPH_ENDPOINT.build_url(
            team_id=team_graph.team_id,
            unique_tournament_id=team_graph.unique_tournament_id,
            season_id=team_graph.season_id,
        )

        responses = {
            TEAM_ENDPOINT.build_url(team_id=team_id): {"team": _team_payload(team_id, "arsenal", "Arsenal", 500)},
            PLAYER_ENDPOINT.build_url(player_id=player_id): {"player": _player_payload(player_id)},
            PLAYER_STATISTICS_ENDPOINT.build_url(player_id=player_id): _not_found_error(
                PLAYER_STATISTICS_ENDPOINT.build_url(player_id=player_id)
            ),
            PLAYER_TRANSFER_HISTORY_ENDPOINT.build_url(player_id=player_id): {"transferHistory": []},
            PLAYER_STATISTICS_SEASONS_ENDPOINT.build_url(player_id=player_id): {
                "uniqueTournamentSeasons": [],
                "typesMap": {},
            },
            TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT.build_url(team_id=team_id): {
                "uniqueTournamentSeasons": [],
                "typesMap": {},
            },
            TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT.build_url(team_id=team_id): {
                "uniqueTournamentSeasons": [],
                "typesMap": {},
            },
            player_overall_url: _not_found_error(player_overall_url),
            team_overall_url: _not_found_error(team_overall_url),
            player_heatmap_url: _not_found_error(player_heatmap_url),
            team_graph_url: _not_found_error(team_graph_url),
        }
        fake_client = _FakeSofascoreClient(responses)
        parser = EntitiesParser(fake_client)

        bundle = await parser.fetch_bundle(
            player_ids=(player_id,),
            team_ids=(team_id,),
            player_overall_requests=(player_overall,),
            team_overall_requests=(team_overall,),
            player_heatmap_requests=(player_heatmap,),
            team_performance_graph_requests=(team_graph,),
        )

        self.assertEqual(len(bundle.payload_snapshots), 6)
        self.assertEqual(
            fake_client.seen_urls,
            [
                PLAYER_ENDPOINT.build_url(player_id=player_id),
                PLAYER_STATISTICS_ENDPOINT.build_url(player_id=player_id),
                PLAYER_STATISTICS_SEASONS_ENDPOINT.build_url(player_id=player_id),
                PLAYER_TRANSFER_HISTORY_ENDPOINT.build_url(player_id=player_id),
                TEAM_ENDPOINT.build_url(team_id=team_id),
                TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT.build_url(team_id=team_id),
                TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT.build_url(team_id=team_id),
                player_overall_url,
                team_overall_url,
                player_heatmap_url,
                team_graph_url,
            ],
        )

    async def test_entities_parser_skips_optional_missing_transfer_history(self) -> None:
        player_id = 1152

        responses = {
            PLAYER_ENDPOINT.build_url(player_id=player_id): {"player": _player_payload(player_id)},
            PLAYER_STATISTICS_ENDPOINT.build_url(player_id=player_id): {"seasons": [], "typesMap": {}},
            PLAYER_STATISTICS_SEASONS_ENDPOINT.build_url(player_id=player_id): {
                "uniqueTournamentSeasons": [],
                "typesMap": {},
            },
            PLAYER_TRANSFER_HISTORY_ENDPOINT.build_url(player_id=player_id): _not_found_error(
                PLAYER_TRANSFER_HISTORY_ENDPOINT.build_url(player_id=player_id)
            ),
        }
        fake_client = _FakeSofascoreClient(responses)
        parser = EntitiesParser(fake_client)

        bundle = await parser.fetch_bundle(
            player_ids=(player_id,),
            include_team_statistics_seasons=False,
            include_team_player_statistics_seasons=False,
        )

        self.assertEqual(len(bundle.payload_snapshots), 3)
        self.assertEqual(len(bundle.transfer_histories), 0)
        self.assertEqual(
            fake_client.seen_urls,
            [
                PLAYER_ENDPOINT.build_url(player_id=player_id),
                PLAYER_STATISTICS_ENDPOINT.build_url(player_id=player_id),
                PLAYER_STATISTICS_SEASONS_ENDPOINT.build_url(player_id=player_id),
                PLAYER_TRANSFER_HISTORY_ENDPOINT.build_url(player_id=player_id),
            ],
        )

    async def test_entities_parser_skips_optional_transfer_history_timeout(self) -> None:
        player_id = 1152

        responses = {
            PLAYER_ENDPOINT.build_url(player_id=player_id): {"player": _player_payload(player_id)},
            PLAYER_STATISTICS_ENDPOINT.build_url(player_id=player_id): {"seasons": [], "typesMap": {}},
            PLAYER_STATISTICS_SEASONS_ENDPOINT.build_url(player_id=player_id): {
                "uniqueTournamentSeasons": [],
                "typesMap": {},
            },
            PLAYER_TRANSFER_HISTORY_ENDPOINT.build_url(player_id=player_id): RuntimeError("proxy timeout"),
        }
        fake_client = _FakeSofascoreClient(responses)
        parser = EntitiesParser(fake_client)

        bundle = await parser.fetch_bundle(
            player_ids=(player_id,),
            include_team_statistics_seasons=False,
            include_team_player_statistics_seasons=False,
        )

        self.assertEqual(len(bundle.payload_snapshots), 3)
        self.assertEqual(len(bundle.transfer_histories), 0)
        self.assertEqual(
            fake_client.seen_urls,
            [
                PLAYER_ENDPOINT.build_url(player_id=player_id),
                PLAYER_STATISTICS_ENDPOINT.build_url(player_id=player_id),
                PLAYER_STATISTICS_SEASONS_ENDPOINT.build_url(player_id=player_id),
                PLAYER_TRANSFER_HISTORY_ENDPOINT.build_url(player_id=player_id),
            ],
        )


if __name__ == "__main__":
    unittest.main()
