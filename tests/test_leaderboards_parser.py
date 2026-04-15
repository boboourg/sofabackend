from __future__ import annotations

import unittest

from schema_inspector.endpoints import (
    TEAM_SCOPED_TOP_PLAYERS_OVERALL_ENDPOINT,
    UNIQUE_TOURNAMENT_GROUPS_ENDPOINT,
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT,
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_RACE_ENDPOINT,
    UNIQUE_TOURNAMENT_PLAYER_STATISTICS_TYPES_ENDPOINT,
    UNIQUE_TOURNAMENT_TEAM_EVENTS_ENDPOINT,
    UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT,
    UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_PERIODS_ENDPOINT,
    UNIQUE_TOURNAMENT_TEAM_STATISTICS_TYPES_ENDPOINT,
    UNIQUE_TOURNAMENT_TOP_PLAYERS_OVERALL_ENDPOINT,
    UNIQUE_TOURNAMENT_TOP_PLAYERS_PER_GAME_ENDPOINT,
    UNIQUE_TOURNAMENT_TOP_RATINGS_OVERALL_ENDPOINT,
    UNIQUE_TOURNAMENT_TOP_TEAMS_OVERALL_ENDPOINT,
    UNIQUE_TOURNAMENT_VENUES_ENDPOINT,
    team_scoped_top_players_endpoint,
    unique_tournament_top_players_endpoint,
    unique_tournament_top_players_per_game_endpoint,
    unique_tournament_top_teams_endpoint,
)
from schema_inspector.leaderboards_parser import LeaderboardsParser
from schema_inspector.runtime import RuntimeConfig, TransportAttempt
from schema_inspector.sofascore_client import SofascoreClient, SofascoreResponse


def _sport_payload() -> dict[str, object]:
    return {"id": 1, "slug": "football", "name": "Football"}


def _country_payload() -> dict[str, object]:
    return {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"}


def _category_payload() -> dict[str, object]:
    return {"id": 1, "slug": "england", "name": "England", "sport": _sport_payload(), "country": _country_payload()}


def _unique_tournament_payload() -> dict[str, object]:
    return {"id": 17, "slug": "premier-league", "name": "Premier League", "category": _category_payload()}


def _season_payload() -> dict[str, object]:
    return {"id": 76986, "name": "Premier League 25/26", "year": "25/26", "editor": False}


def _team_payload(team_id: int, slug: str, name: str) -> dict[str, object]:
    return {
        "id": team_id,
        "slug": slug,
        "name": name,
        "sport": _sport_payload(),
        "country": _country_payload(),
        "category": _category_payload(),
        "type": 0,
        "national": False,
        "teamColors": {"primary": "#cc0000", "secondary": "#ffffff", "text": "#ffffff"},
    }


def _player_payload(player_id: int, team_id: int) -> dict[str, object]:
    return {
        "id": player_id,
        "slug": f"player-{player_id}",
        "name": f"Player {player_id}",
        "gender": "M",
        "team": _team_payload(team_id, f"team-{team_id}", f"Team {team_id}"),
    }


def _event_payload() -> dict[str, object]:
    return {
        "id": 14083191,
        "slug": "arsenal-chelsea",
        "tournament": {
            "id": 100,
            "slug": "premier-league",
            "name": "Premier League",
            "category": _category_payload(),
            "uniqueTournament": _unique_tournament_payload(),
            "competitionType": 1,
        },
        "season": _season_payload(),
        "status": {"code": 100, "description": "1st half", "type": "inprogress"},
        "homeTeam": _team_payload(42, "arsenal", "Arsenal"),
        "awayTeam": _team_payload(43, "chelsea", "Chelsea"),
        "homeScore": {"current": 1, "display": 1},
        "awayScore": {"current": 0, "display": 0},
        "startTimestamp": 1710000000,
    }


class _FakeSofascoreClient(SofascoreClient):
    def __init__(self, responses: dict[str, object]) -> None:
        super().__init__(RuntimeConfig())
        self.responses = responses
        self.seen_urls: list[str] = []

    async def get_json(self, url: str, *, headers=None, timeout: float = 20.0) -> SofascoreResponse:
        del headers, timeout
        self.seen_urls.append(url)
        return SofascoreResponse(
            source_url=url,
            resolved_url=url,
            fetched_at="2026-04-10T10:00:00+00:00",
            status_code=200,
            headers={"Content-Type": "application/json"},
            body_bytes=b"{}",
            payload=self.responses[url],
            attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
            final_proxy_name="proxy_1",
            challenge_reason=None,
        )


class LeaderboardsParserTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_leaderboard_endpoint_templates_match_sofascore_paths(self) -> None:
        self.assertEqual(
            UNIQUE_TOURNAMENT_TOP_PLAYERS_OVERALL_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
            "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/top-players/overall",
        )
        self.assertEqual(
            UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT.build_url(
                unique_tournament_id=17,
                season_id=76986,
                period_id=19564,
            ),
            "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/team-of-the-week/19564",
        )

    async def test_leaderboards_parser_builds_expected_bundle(self) -> None:
        top_players_root = {
            "topPlayers": {
                "goals": [
                    {
                        "player": _player_payload(700, 42),
                        "team": _team_payload(42, "arsenal", "Arsenal"),
                        "playedEnough": True,
                        "statistics": {"id": 10, "goals": 11},
                    }
                ]
            },
            "statisticsType": {"code": "overall"},
        }
        top_ratings_root = {
            "topPlayers": {
                "avgRating": [
                    {
                        "player": _player_payload(700, 42),
                        "team": _team_payload(42, "arsenal", "Arsenal"),
                        "playedEnough": True,
                        "statistics": {"id": 11, "rating": 7.8},
                    }
                ]
            },
            "statisticsType": {"code": "overall"},
        }
        top_players_per_game_root = {
            "topPlayers": {
                "goals": [
                    {
                        "player": _player_payload(700, 42),
                        "team": _team_payload(42, "arsenal", "Arsenal"),
                        "event": _event_payload(),
                        "statistics": {"id": 12, "goals": 2},
                    }
                ]
            },
            "statisticsType": {"code": "perGame"},
        }
        top_teams_root = {
            "topTeams": {
                "goals": [
                    {
                        "team": _team_payload(42, "arsenal", "Arsenal"),
                        "statistics": {"id": 13, "goals": 70},
                    }
                ]
            }
        }
        team_of_week_periods_root = {
            "periods": [
                {
                    "id": 19564,
                    "periodName": "Round 1",
                    "type": "round",
                    "startDateTimestamp": 1710000000,
                    "createdAtTimestamp": 1710000100,
                    "round": {"name": "Round 1", "round": 1, "slug": "round-1"},
                }
            ]
        }
        team_of_week_root = {
            "formation": "4-3-3",
            "players": [
                {
                    "id": 1,
                    "order": 0,
                    "rating": "7.9",
                    "player": _player_payload(700, 42),
                    "team": _team_payload(42, "arsenal", "Arsenal"),
                }
            ],
        }

        responses = {
            UNIQUE_TOURNAMENT_TOP_PLAYERS_OVERALL_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986): top_players_root,
            UNIQUE_TOURNAMENT_TOP_RATINGS_OVERALL_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986): top_ratings_root,
            UNIQUE_TOURNAMENT_TOP_PLAYERS_PER_GAME_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986): top_players_per_game_root,
            UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_RACE_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986): top_players_root,
            UNIQUE_TOURNAMENT_TOP_TEAMS_OVERALL_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986): top_teams_root,
            UNIQUE_TOURNAMENT_VENUES_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986): {
                "venues": [
                    {
                        "id": 55,
                        "slug": "emirates",
                        "name": "Emirates Stadium",
                        "country": _country_payload(),
                        "city": {"name": "London"},
                        "stadium": {"name": "Emirates Stadium", "capacity": 60000},
                    }
                ]
            },
            UNIQUE_TOURNAMENT_GROUPS_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986): {
                "groups": [{"groupName": "Group A", "tournamentId": 100}]
            },
            UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986): {
                "player": _player_payload(700, 42),
                "team": _team_payload(42, "arsenal", "Arsenal"),
                "statistics": {"id": 15, "rating": 8.2},
                "playerOfTheTournament": True,
            },
            UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_PERIODS_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986): team_of_week_periods_root,
            UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986, period_id=19564): team_of_week_root,
            UNIQUE_TOURNAMENT_PLAYER_STATISTICS_TYPES_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986): {"types": ["summary", "attack"]},
            UNIQUE_TOURNAMENT_TEAM_STATISTICS_TYPES_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986): {"types": ["summary", "defence"]},
            UNIQUE_TOURNAMENT_TEAM_EVENTS_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986, scope="total"): {
                "tournamentTeamEvents": {"results": [_event_payload()]},
            },
            TEAM_SCOPED_TOP_PLAYERS_OVERALL_ENDPOINT.build_url(team_id=42, unique_tournament_id=17, season_id=76986): top_players_root,
        }
        fake_client = _FakeSofascoreClient(responses)

        parser = LeaderboardsParser(fake_client)
        bundle = await parser.fetch_bundle(
            17,
            76986,
            team_top_players_team_ids=(42,),
            team_event_scopes=("total",),
        )

        self.assertEqual(len(bundle.registry_entries), 15)
        self.assertEqual(len(bundle.payload_snapshots), 14)
        self.assertEqual(len(bundle.top_player_snapshots), 5)
        self.assertEqual(len(bundle.top_team_snapshots), 1)
        self.assertEqual(len(bundle.periods), 1)
        self.assertEqual(len(bundle.team_of_the_week), 1)
        self.assertEqual(len(bundle.team_of_the_week_players), 1)
        self.assertEqual(len(bundle.venues), 1)
        self.assertEqual(len(bundle.season_groups), 1)
        self.assertEqual(len(bundle.season_player_of_the_season), 1)
        self.assertEqual(len(bundle.season_statistics_types), 4)
        self.assertEqual(len(bundle.tournament_team_event_snapshots), 1)
        self.assertEqual(len(bundle.tournament_team_event_snapshots[0].buckets), 1)
        self.assertEqual({item.id for item in bundle.players}, {700})
        self.assertEqual({item.id for item in bundle.teams}, {42, 43})
        self.assertEqual({item.id for item in bundle.events}, {14083191})
        self.assertEqual(
            fake_client.seen_urls,
            [
                UNIQUE_TOURNAMENT_TOP_PLAYERS_OVERALL_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
                UNIQUE_TOURNAMENT_TOP_RATINGS_OVERALL_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
                UNIQUE_TOURNAMENT_TOP_PLAYERS_PER_GAME_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
                UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_RACE_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
                UNIQUE_TOURNAMENT_TOP_TEAMS_OVERALL_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
                UNIQUE_TOURNAMENT_VENUES_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
                UNIQUE_TOURNAMENT_GROUPS_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
                UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
                UNIQUE_TOURNAMENT_PLAYER_STATISTICS_TYPES_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
                UNIQUE_TOURNAMENT_TEAM_STATISTICS_TYPES_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
                TEAM_SCOPED_TOP_PLAYERS_OVERALL_ENDPOINT.build_url(team_id=42, unique_tournament_id=17, season_id=76986),
                UNIQUE_TOURNAMENT_TEAM_EVENTS_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986, scope="total"),
                UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_PERIODS_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
                UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986, period_id=19564),
            ],
        )

    async def test_leaderboards_parser_supports_basketball_regular_season_paths(self) -> None:
        top_players_endpoint = unique_tournament_top_players_endpoint("regularSeason")
        top_players_per_game_endpoint = unique_tournament_top_players_per_game_endpoint("all/regularSeason")
        top_teams_endpoint = unique_tournament_top_teams_endpoint("regularSeason")
        team_top_players_endpoint = team_scoped_top_players_endpoint("overall")

        responses = {
            top_players_endpoint.build_url(unique_tournament_id=132, season_id=80229): {
                "topPlayers": {
                    "points": [
                        {
                            "player": _player_payload(701, 3424),
                            "team": _team_payload(3424, "lakers", "Lakers"),
                            "statistics": {"id": 21, "points": 28},
                        }
                    ]
                },
                "statisticsType": {"code": "regularSeason"},
            },
            top_players_per_game_endpoint.build_url(unique_tournament_id=132, season_id=80229): {
                "topPlayers": {
                    "points": [
                        {
                            "player": _player_payload(701, 3424),
                            "team": _team_payload(3424, "lakers", "Lakers"),
                            "statistics": {"id": 22, "points": 31},
                        }
                    ]
                },
                "statisticsType": {"code": "regularSeason"},
            },
            top_teams_endpoint.build_url(unique_tournament_id=132, season_id=80229): {
                "topTeams": {
                    "points": [
                        {
                            "team": _team_payload(3424, "lakers", "Lakers"),
                            "statistics": {"id": 23, "points": 118},
                        }
                    ]
                }
            },
            UNIQUE_TOURNAMENT_VENUES_ENDPOINT.build_url(unique_tournament_id=132, season_id=80229): {"venues": []},
            UNIQUE_TOURNAMENT_GROUPS_ENDPOINT.build_url(unique_tournament_id=132, season_id=80229): {"groups": []},
            UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT.build_url(unique_tournament_id=132, season_id=80229): {
                "player": _player_payload(701, 3424),
                "team": _team_payload(3424, "lakers", "Lakers"),
                "statistics": {"id": 24, "rating": 8.0},
                "playerOfTheTournament": True,
            },
            UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_PERIODS_ENDPOINT.build_url(unique_tournament_id=132, season_id=80229): {
                "periods": []
            },
            UNIQUE_TOURNAMENT_PLAYER_STATISTICS_TYPES_ENDPOINT.build_url(unique_tournament_id=132, season_id=80229): {
                "types": ["summary"]
            },
            UNIQUE_TOURNAMENT_TEAM_STATISTICS_TYPES_ENDPOINT.build_url(unique_tournament_id=132, season_id=80229): {
                "types": ["summary"]
            },
            UNIQUE_TOURNAMENT_TEAM_EVENTS_ENDPOINT.build_url(unique_tournament_id=132, season_id=80229, scope="total"): {
                "tournamentTeamEvents": {}
            },
            team_top_players_endpoint.build_url(team_id=3424, unique_tournament_id=132, season_id=80229): {
                "topPlayers": {
                    "points": [
                        {
                            "player": _player_payload(701, 3424),
                            "team": _team_payload(3424, "lakers", "Lakers"),
                            "statistics": {"id": 25, "points": 28},
                        }
                    ]
                },
                "statisticsType": {"code": "overall"},
            },
        }
        fake_client = _FakeSofascoreClient(responses)

        parser = LeaderboardsParser(fake_client)
        bundle = await parser.fetch_bundle(
            132,
            80229,
            sport_slug="basketball",
            team_top_players_team_ids=(3424,),
            team_event_scopes=("total",),
        )

        self.assertEqual(bundle.registry_entries[0].path_template, "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/regularSeason")
        self.assertEqual(bundle.registry_entries[2].path_template, "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players-per-game/all/regularSeason")
        self.assertEqual(bundle.registry_entries[5].path_template, "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/regularSeason")
        self.assertEqual(len(bundle.top_player_snapshots), 3)
        self.assertEqual(len(bundle.top_team_snapshots), 1)
        self.assertEqual({item.id for item in bundle.players}, {701})
        self.assertEqual({item.id for item in bundle.teams}, {3424})
        self.assertNotIn(
            UNIQUE_TOURNAMENT_TOP_RATINGS_OVERALL_ENDPOINT.build_url(unique_tournament_id=132, season_id=80229),
            fake_client.seen_urls,
        )
        self.assertNotIn(
            UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_RACE_ENDPOINT.build_url(unique_tournament_id=132, season_id=80229),
            fake_client.seen_urls,
        )


if __name__ == "__main__":
    unittest.main()
