from __future__ import annotations

import unittest

from schema_inspector.event_list_parser import EventListParser, EventListParserError
from schema_inspector.endpoints import (
    SPORT_FOOTBALL_LIVE_EVENTS_ENDPOINT,
    SPORT_FOOTBALL_SCHEDULED_EVENTS_ENDPOINT,
    UNIQUE_TOURNAMENT_FEATURED_EVENTS_ENDPOINT,
    UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASON_BRACKETS_ENDPOINT,
    UNIQUE_TOURNAMENT_SCHEDULED_EVENTS_ENDPOINT,
    season_last_events_endpoint,
    season_next_events_endpoint,
    sport_live_events_endpoint,
    sport_scheduled_events_endpoint,
    team_last_events_endpoint,
    team_next_events_endpoint,
)
from schema_inspector.runtime import RuntimeConfig, TransportAttempt
from schema_inspector.sofascore_client import SofascoreClient, SofascoreResponse


class _FakeSofascoreClient(SofascoreClient):
    def __init__(self, responses: dict[str, object]) -> None:
        super().__init__(RuntimeConfig())
        self.responses = responses
        self.seen_urls: list[str] = []

    async def get_json(self, url: str, *, headers=None, timeout: float = 20.0) -> SofascoreResponse:
        del headers, timeout
        self.seen_urls.append(url)
        payload = self.responses[url]
        return SofascoreResponse(
            source_url=url,
            resolved_url=url,
            fetched_at="2026-04-10T10:00:00+00:00",
            status_code=200,
            headers={"Content-Type": "application/json"},
            body_bytes=b"{}",
            payload=payload,
            attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
            final_proxy_name="proxy_1",
            challenge_reason=None,
        )


class EventListParserTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_endpoint_templates_match_sofascore_paths(self) -> None:
        self.assertEqual(
            SPORT_FOOTBALL_SCHEDULED_EVENTS_ENDPOINT.build_url(date="2026-04-10"),
            "https://www.sofascore.com/api/v1/sport/football/scheduled-events/2026-04-10",
        )
        self.assertEqual(
            SPORT_FOOTBALL_LIVE_EVENTS_ENDPOINT.build_url(),
            "https://www.sofascore.com/api/v1/sport/football/events/live",
        )
        self.assertEqual(
            UNIQUE_TOURNAMENT_FEATURED_EVENTS_ENDPOINT.build_url(unique_tournament_id=17),
            "https://www.sofascore.com/api/v1/unique-tournament/17/featured-events",
        )
        self.assertEqual(
            UNIQUE_TOURNAMENT_SCHEDULED_EVENTS_ENDPOINT.build_url(
                unique_tournament_id=2423,
                date="2026-04-14",
            ),
            "https://www.sofascore.com/api/v1/unique-tournament/2423/scheduled-events/2026-04-14",
        )
        self.assertEqual(
            UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT.build_url(
                unique_tournament_id=17,
                season_id=76986,
                round_number=32,
            ),
            "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/events/round/32",
        )
        self.assertEqual(
            season_last_events_endpoint().build_url(unique_tournament_id=17, season_id=76986, page=0),
            "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/events/last/0",
        )
        self.assertEqual(
            season_next_events_endpoint().build_url(unique_tournament_id=17, season_id=76986, page=0),
            "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/events/next/0",
        )
        self.assertEqual(
            team_last_events_endpoint().build_url(team_id=2817, page=0),
            "https://www.sofascore.com/api/v1/team/2817/events/last/0",
        )
        self.assertEqual(
            team_next_events_endpoint().build_url(team_id=2817, page=0),
            "https://www.sofascore.com/api/v1/team/2817/events/next/0",
        )
        self.assertEqual(
            sport_scheduled_events_endpoint("basketball").build_url(date="2026-04-10"),
            "https://www.sofascore.com/api/v1/sport/basketball/scheduled-events/2026-04-10",
        )
        self.assertEqual(
            sport_live_events_endpoint("cricket").build_url(),
            "https://www.sofascore.com/api/v1/sport/cricket/events/live",
        )

    async def test_event_list_parser_builds_normalized_bundle_for_scheduled_payload(self) -> None:
        scheduled_url = SPORT_FOOTBALL_SCHEDULED_EVENTS_ENDPOINT.build_url(date="2026-04-10")
        fake_client = _FakeSofascoreClient(
            {
                scheduled_url: {
                    "events": [
                        {
                            "id": 15726260,
                            "slug": "arsenal-chelsea",
                            "customId": "abc123",
                            "tournament": {
                                "id": 1,
                                "slug": "premier-league",
                                "name": "Premier League",
                                "priority": 10,
                                "category": {
                                    "id": 1,
                                    "slug": "england",
                                    "name": "England",
                                    "alpha2": "EN",
                                    "flag": "england",
                                    "priority": 10,
                                    "sport": {"id": 1, "slug": "football", "name": "Football"},
                                    "country": {
                                        "alpha2": "EN",
                                        "alpha3": "ENG",
                                        "slug": "england",
                                        "name": "England",
                                    },
                                    "fieldTranslations": {"nameTranslation": {"ru": "Англия"}},
                                },
                                "uniqueTournament": {
                                    "id": 17,
                                    "slug": "premier-league",
                                    "name": "Premier League",
                                    "category": {
                                        "id": 1,
                                        "slug": "england",
                                        "name": "England",
                                        "alpha2": "EN",
                                        "sport": {"id": 1, "slug": "football", "name": "Football"},
                                    },
                                    "country": {
                                        "alpha2": "EN",
                                        "alpha3": "ENG",
                                        "slug": "england",
                                        "name": "England",
                                    },
                                    "displayInverseHomeAwayTeams": False,
                                    "fieldTranslations": {"nameTranslation": {"ru": "Премьер-лига"}},
                                    "hasEventPlayerStatistics": True,
                                    "hasPerformanceGraphFeature": True,
                                    "primaryColorHex": "#3c1c5a",
                                    "secondaryColorHex": "#f80158",
                                    "userCount": 1179222,
                                },
                                "fieldTranslations": {"nameTranslation": {"ru": "Премьер-лига"}},
                            },
                            "season": {
                                "id": 76986,
                                "name": "Premier League 25/26",
                                "year": "25/26",
                                "editor": False,
                                "seasonCoverageInfo": {"editorCoverageLevel": "full"},
                            },
                            "roundInfo": {"round": 32, "name": "Round 32", "cupRoundType": 1},
                            "status": {"code": 0, "description": "Not started", "type": "notstarted"},
                            "homeTeam": {
                                "id": 42,
                                "slug": "arsenal",
                                "name": "Arsenal",
                                "shortName": "Arsenal",
                                "nameCode": "ARS",
                                "sport": {"id": 1, "slug": "football", "name": "Football"},
                                "country": {
                                    "alpha2": "EN",
                                    "alpha3": "ENG",
                                    "slug": "england",
                                    "name": "England",
                                },
                                "teamColors": {"primary": "#ef0107", "secondary": "#ffffff"},
                                "fieldTranslations": {"nameTranslation": {"ru": "Арсенал"}},
                                "gender": "M",
                                "type": 0,
                                "national": False,
                                "disabled": False,
                                "userCount": 1000,
                            },
                            "awayTeam": {
                                "id": 43,
                                "slug": "chelsea",
                                "name": "Chelsea",
                                "shortName": "Chelsea",
                                "nameCode": "CHE",
                                "sport": {"id": 1, "slug": "football", "name": "Football"},
                                "country": {
                                    "alpha2": "EN",
                                    "alpha3": "ENG",
                                    "slug": "england",
                                    "name": "England",
                                },
                                "parentTeam": {
                                    "id": 430,
                                    "slug": "chelsea-root",
                                    "name": "Chelsea Root",
                                    "sport": {"id": 1, "slug": "football", "name": "Football"},
                                    "country": {"alpha2": "EN", "name": "England"},
                                },
                                "teamColors": {"primary": "#034694", "secondary": "#ffffff"},
                                "gender": "M",
                                "type": 0,
                                "national": False,
                                "disabled": False,
                                "userCount": 2000,
                            },
                            "homeScore": {},
                            "awayScore": {},
                            "eventFilters": {
                                "category": ["scheduled"],
                                "gender": ["men"],
                            },
                            "changes": {"changeTimestamp": 1712745600, "changes": ["status"]},
                            "seasonStatisticsType": "overall",
                            "startTimestamp": 1775779200,
                            "coverage": 1,
                            "winnerCode": 0,
                            "aggregatedWinnerCode": 0,
                            "homeRedCards": 0,
                            "awayRedCards": 0,
                            "defaultPeriodCount": 2,
                            "defaultPeriodLength": 45,
                            "defaultOvertimeLength": 15,
                            "correctAiInsight": False,
                            "correctHalftimeAiInsight": False,
                            "feedLocked": False,
                            "isEditor": False,
                            "showTotoPromo": False,
                            "crowdsourcingEnabled": True,
                            "crowdsourcingDataDisplayEnabled": True,
                            "finalResultOnly": False,
                            "hasEventPlayerStatistics": True,
                            "hasEventPlayerHeatMap": True,
                            "hasGlobalHighlights": False,
                            "hasXg": True,
                        }
                    ]
                }
            }
        )

        parser = EventListParser(fake_client)
        bundle = await parser.fetch_scheduled_events("2026-04-10")

        self.assertEqual(fake_client.seen_urls, [scheduled_url])
        self.assertEqual(len(bundle.registry_entries), 10)
        self.assertIn(
            UNIQUE_TOURNAMENT_SEASON_BRACKETS_ENDPOINT.path_template,
            {item.path_template for item in bundle.registry_entries},
        )
        self.assertEqual(len(bundle.payload_snapshots), 1)
        self.assertEqual({item.id for item in bundle.events}, {15726260})
        self.assertEqual({item.id for item in bundle.tournaments}, {1})
        self.assertEqual({item.id for item in bundle.unique_tournaments}, {17})
        self.assertEqual({item.id for item in bundle.seasons}, {76986})
        self.assertEqual({item.id for item in bundle.teams}, {42, 43, 430})
        self.assertEqual(bundle.event_statuses[0].description, "Not started")
        self.assertEqual(bundle.event_round_infos[0].round_number, 32)
        self.assertEqual(bundle.event_filter_values[0].filter_name, "category")
        self.assertEqual(bundle.event_change_items[0].change_value, "status")

    async def test_event_list_parser_builds_live_side_tables(self) -> None:
        live_url = SPORT_FOOTBALL_LIVE_EVENTS_ENDPOINT.build_url()
        fake_client = _FakeSofascoreClient(
            {
                live_url: {
                    "events": [
                        {
                            "id": 14083191,
                            "slug": "liverpool-manchester-city",
                            "tournament": {
                                "id": 1,
                                "slug": "premier-league",
                                "name": "Premier League",
                                "category": {
                                    "id": 1,
                                    "slug": "england",
                                    "name": "England",
                                    "alpha2": "EN",
                                    "sport": {"id": 1, "slug": "football", "name": "Football"},
                                },
                                "uniqueTournament": {
                                    "id": 17,
                                    "slug": "premier-league",
                                    "name": "Premier League",
                                    "category": {
                                        "id": 1,
                                        "slug": "england",
                                        "name": "England",
                                        "alpha2": "EN",
                                        "sport": {"id": 1, "slug": "football", "name": "Football"},
                                    },
                                },
                            },
                            "season": {"id": 76986, "name": "Premier League 25/26", "year": "25/26"},
                            "status": {"code": 100, "description": "1st half", "type": "inprogress"},
                            "homeTeam": {"id": 44, "slug": "liverpool", "name": "Liverpool"},
                            "awayTeam": {"id": 45, "slug": "manchester-city", "name": "Manchester City"},
                            "homeScore": {"current": 1, "display": 1, "period1": 1},
                            "awayScore": {"current": 0, "display": 0, "period1": 0},
                            "statusTime": {
                                "prefix": "'",
                                "timestamp": 1712745600,
                                "initial": 45,
                                "max": 45,
                                "extra": 2,
                            },
                            "time": {
                                "currentPeriodStartTimestamp": 1712742000,
                                "initial": 45,
                                "max": 45,
                                "extra": 2,
                                "overtimeLength": 15,
                                "periodLength": 45,
                                "totalPeriodCount": 2,
                            },
                            "varInProgress": {"homeTeam": True, "awayTeam": False},
                            "changes": {
                                "changeTimestamp": 1712745600,
                                "changes": ["score", "statusTime"],
                            },
                        }
                    ]
                }
            }
        )

        parser = EventListParser(fake_client)
        bundle = await parser.fetch_live_events()

        self.assertEqual(len(bundle.event_scores), 2)
        self.assertEqual(bundle.event_status_times[0].prefix, "'")
        self.assertEqual(bundle.event_times[0].period_length, 45)
        self.assertEqual(bundle.event_var_in_progress_items[0].home_team, True)
        self.assertEqual(
            {(item.event_id, item.side, item.current) for item in bundle.event_scores},
            {(14083191, "home", 1), (14083191, "away", 0)},
        )
        self.assertEqual(
            [item.change_value for item in bundle.event_change_items],
            ["score", "statusTime"],
        )

    async def test_event_list_parser_raises_on_missing_envelope(self) -> None:
        featured_url = UNIQUE_TOURNAMENT_FEATURED_EVENTS_ENDPOINT.build_url(unique_tournament_id=17)
        fake_client = _FakeSofascoreClient({featured_url: {"wrongKey": []}})
        parser = EventListParser(fake_client)

        with self.assertRaises(EventListParserError):
            await parser.fetch_featured_events(17)

    async def test_event_list_parser_supports_basketball_live_paths(self) -> None:
        live_url = sport_live_events_endpoint("basketball").build_url()
        fake_client = _FakeSofascoreClient(
            {
                live_url: {
                    "events": [
                        {
                            "id": 999,
                            "slug": "lakers-celtics",
                            "tournament": {
                                "id": 177,
                                "slug": "nba",
                                "name": "NBA",
                                "category": {
                                    "id": 15,
                                    "slug": "usa",
                                    "name": "USA",
                                    "sport": {"id": 2, "slug": "basketball", "name": "Basketball"},
                                },
                                "uniqueTournament": {
                                    "id": 132,
                                    "slug": "nba",
                                    "name": "NBA",
                                    "category": {
                                        "id": 15,
                                        "slug": "usa",
                                        "name": "USA",
                                        "sport": {"id": 2, "slug": "basketball", "name": "Basketball"},
                                    },
                                },
                            },
                            "season": {"id": 80229, "name": "NBA 25/26", "year": "25/26"},
                            "status": {"code": 100, "description": "Q1", "type": "inprogress"},
                            "homeTeam": {"id": 3424, "slug": "lakers", "name": "Lakers"},
                            "awayTeam": {"id": 3439, "slug": "celtics", "name": "Celtics"},
                            "homeScore": {"current": 32, "display": 32, "period1": 32},
                            "awayScore": {"current": 28, "display": 28, "period1": 28},
                        }
                    ]
                }
            }
        )

        parser = EventListParser(fake_client)
        bundle = await parser.fetch_live_events(sport_slug="basketball")

        self.assertEqual(fake_client.seen_urls, [live_url])
        self.assertEqual(bundle.registry_entries[0].path_template, "/api/v1/sport/basketball/scheduled-events/{date}")
        self.assertEqual(bundle.registry_entries[1].path_template, "/api/v1/sport/basketball/events/live")
        self.assertEqual({item.id for item in bundle.sports}, {2})
        self.assertEqual({item.id for item in bundle.events}, {999})

    async def test_event_list_parser_supports_tennis_tournament_day_paths_and_subteams(self) -> None:
        scheduled_url = UNIQUE_TOURNAMENT_SCHEDULED_EVENTS_ENDPOINT.build_url(
            unique_tournament_id=2423,
            date="2026-04-14",
        )
        fake_client = _FakeSofascoreClient(
            {
                scheduled_url: {
                    "events": [
                        {
                            "id": 16000905,
                            "slug": "arneodo-polmans-pavlasek-rikl",
                            "customId": "dcJjsjYKj",
                            "tournament": {
                                "id": 144350,
                                "slug": "barcelona-doubles-qualifying",
                                "name": "Barcelona, Spain, Doubles, Qualifying",
                                "category": {
                                    "id": 3,
                                    "slug": "atp",
                                    "name": "ATP",
                                    "flag": "atp",
                                    "sport": {"id": 5, "slug": "tennis", "name": "Tennis"},
                                },
                                "uniqueTournament": {
                                    "id": 2423,
                                    "slug": "barcelona-doubles",
                                    "name": "Barcelona, Doubles",
                                    "category": {
                                        "id": 3,
                                        "slug": "atp",
                                        "name": "ATP",
                                        "flag": "atp",
                                        "sport": {"id": 5, "slug": "tennis", "name": "Tennis"},
                                    },
                                    "displayInverseHomeAwayTeams": False,
                                    "hasEventPlayerStatistics": False,
                                    "hasPerformanceGraphFeature": False,
                                },
                            },
                            "season": {"id": 80012, "name": "Barcelona Doubles 2026", "year": "2026"},
                            "status": {"code": 0, "description": "Not started", "type": "notstarted"},
                            "homeTeam": {
                                "id": 1210041,
                                "slug": "arneodo-polmans",
                                "name": "Arneodo R / Polmans M",
                                "shortName": "Arneodo / Polmans",
                                "nameCode": "A/P",
                                "sport": {"id": 5, "slug": "tennis", "name": "Tennis"},
                                "country": {"alpha2": "MC", "name": "Monaco"},
                                "teamColors": {"primary": "#374df5"},
                                "subTeams": [
                                    {
                                        "id": 41475,
                                        "slug": "arneodo-romain",
                                        "name": "Romain Arneodo",
                                        "shortName": "R. Arneodo",
                                        "nameCode": "ARN",
                                        "sport": {"id": 5, "slug": "tennis", "name": "Tennis"},
                                        "country": {"alpha2": "MC", "name": "Monaco"},
                                        "teamColors": {"primary": "#374df5"},
                                        "gender": "M",
                                        "type": 1,
                                        "national": False,
                                        "userCount": 119,
                                        "subTeams": [],
                                    },
                                    {
                                        "id": 65072,
                                        "slug": "polmans-marc",
                                        "name": "Marc Polmans",
                                        "shortName": "M. Polmans",
                                        "nameCode": "POL",
                                        "sport": {"id": 5, "slug": "tennis", "name": "Tennis"},
                                        "country": {"alpha2": "AU", "name": "Australia"},
                                        "teamColors": {"primary": "#374df5"},
                                        "gender": "M",
                                        "type": 1,
                                        "national": False,
                                        "userCount": 201,
                                        "subTeams": [],
                                    },
                                ],
                                "gender": "M",
                                "type": 2,
                                "national": False,
                                "userCount": 12,
                            },
                            "awayTeam": {
                                "id": 1210103,
                                "slug": "pavlasek-rikl",
                                "name": "Pavlasek A / Rikl P",
                                "shortName": "Pavlasek / Rikl",
                                "nameCode": "P/R",
                                "sport": {"id": 5, "slug": "tennis", "name": "Tennis"},
                                "country": {"alpha2": "CZ", "name": "Czechia"},
                                "teamColors": {"primary": "#374df5"},
                                "subTeams": [
                                    {
                                        "id": 59324,
                                        "slug": "pavlasek-adam",
                                        "name": "Adam Pavlasek",
                                        "shortName": "A. Pavlasek",
                                        "nameCode": "PAV",
                                        "sport": {"id": 5, "slug": "tennis", "name": "Tennis"},
                                        "country": {"alpha2": "CZ", "name": "Czechia"},
                                        "teamColors": {"primary": "#374df5"},
                                        "gender": "M",
                                        "type": 1,
                                        "national": False,
                                        "userCount": 215,
                                        "subTeams": [],
                                    },
                                    {
                                        "id": 93641,
                                        "slug": "rikl-petr",
                                        "name": "Petr Rikl",
                                        "shortName": "P. Rikl",
                                        "nameCode": "RIK",
                                        "sport": {"id": 5, "slug": "tennis", "name": "Tennis"},
                                        "country": {"alpha2": "CZ", "name": "Czechia"},
                                        "teamColors": {"primary": "#374df5"},
                                        "gender": "M",
                                        "type": 1,
                                        "national": False,
                                        "userCount": 130,
                                        "subTeams": [],
                                    },
                                ],
                                "gender": "M",
                                "type": 2,
                                "national": False,
                                "userCount": 9,
                            },
                            "roundInfo": {"round": 1, "name": "Qualifying"},
                            "homeScore": {"current": 0, "period1": 5},
                            "awayScore": {"current": 2, "period1": 7},
                            "eventFilters": {"category": ["scheduled"], "gender": ["men"]},
                            "changes": {"changeTimestamp": 1776167193, "changes": ["score", "status"]},
                            "startTimestamp": 1776160800,
                            "coverage": 1,
                            "groundType": "Red clay",
                        }
                    ]
                }
            }
        )

        parser = EventListParser(fake_client)
        bundle = await parser.fetch_unique_tournament_scheduled_events(
            2423,
            "2026-04-14",
            sport_slug="tennis",
        )

        self.assertEqual(fake_client.seen_urls, [scheduled_url])
        self.assertEqual(bundle.registry_entries[2].path_template, "/api/v1/unique-tournament/{unique_tournament_id}/scheduled-events/{date}")
        self.assertEqual({item.id for item in bundle.sports}, {5})
        self.assertEqual({item.id for item in bundle.unique_tournaments}, {2423})
        self.assertEqual({item.id for item in bundle.events}, {16000905})
        self.assertEqual({item.id for item in bundle.teams}, {1210041, 1210103, 41475, 65072, 59324, 93641})
        child_parent_map = {item.id: item.parent_team_id for item in bundle.teams}
        self.assertEqual(child_parent_map[41475], 1210041)
        self.assertEqual(child_parent_map[59324], 1210103)

    async def test_event_list_parser_fetches_paginated_season_last_and_next_events(self) -> None:
        last_endpoint = season_last_events_endpoint()
        next_endpoint = season_next_events_endpoint()
        last_url = last_endpoint.build_url(unique_tournament_id=17, season_id=76986, page=0)
        next_url = next_endpoint.build_url(unique_tournament_id=17, season_id=76986, page=1)
        fake_client = _FakeSofascoreClient(
            {
                last_url: {"events": [_minimal_event_payload(15726260, status_type="finished")], "hasNextPage": True},
                next_url: {"events": [_minimal_event_payload(15726261, status_type="notstarted")], "hasNextPage": False},
            }
        )

        parser = EventListParser(fake_client)
        last_bundle = await parser.fetch_season_last_events(17, 76986, page=0)
        next_bundle = await parser.fetch_season_next_events(17, 76986, page=1)

        self.assertEqual(fake_client.seen_urls, [last_url, next_url])
        self.assertEqual(last_bundle.payload_snapshots[0].endpoint_pattern, last_endpoint.path_template)
        self.assertEqual(last_bundle.payload_snapshots[0].context_entity_type, "season")
        self.assertEqual(last_bundle.payload_snapshots[0].context_entity_id, 76986)
        self.assertEqual(last_bundle.payload_snapshots[0].payload["hasNextPage"], True)
        self.assertEqual({event.id for event in last_bundle.events}, {15726260})
        self.assertEqual(next_bundle.payload_snapshots[0].endpoint_pattern, next_endpoint.path_template)
        self.assertEqual(next_bundle.payload_snapshots[0].context_entity_id, 76986)
        self.assertEqual(next_bundle.payload_snapshots[0].payload["hasNextPage"], False)
        self.assertEqual({event.id for event in next_bundle.events}, {15726261})

    async def test_event_list_parser_fetches_paginated_team_last_and_next_events(self) -> None:
        last_endpoint = team_last_events_endpoint()
        next_endpoint = team_next_events_endpoint()
        last_url = last_endpoint.build_url(team_id=2817, page=0)
        next_url = next_endpoint.build_url(team_id=2817, page=1)
        fake_client = _FakeSofascoreClient(
            {
                last_url: {"events": [_minimal_event_payload(14023940, status_type="finished")], "hasNextPage": True},
                next_url: {"events": [_minimal_event_payload(14083495, status_type="notstarted")], "hasNextPage": False},
            }
        )

        parser = EventListParser(fake_client)
        last_bundle = await parser.fetch_team_last_events(2817, page=0)
        next_bundle = await parser.fetch_team_next_events(2817, page=1)

        self.assertEqual(fake_client.seen_urls, [last_url, next_url])
        self.assertEqual(last_bundle.payload_snapshots[0].endpoint_pattern, last_endpoint.path_template)
        self.assertEqual(last_bundle.payload_snapshots[0].context_entity_type, "team")
        self.assertEqual(last_bundle.payload_snapshots[0].context_entity_id, 2817)
        self.assertEqual(last_bundle.payload_snapshots[0].payload["hasNextPage"], True)
        self.assertEqual({event.id for event in last_bundle.events}, {14023940})
        self.assertEqual(next_bundle.payload_snapshots[0].endpoint_pattern, next_endpoint.path_template)
        self.assertEqual(next_bundle.payload_snapshots[0].context_entity_type, "team")
        self.assertEqual(next_bundle.payload_snapshots[0].context_entity_id, 2817)
        self.assertEqual(next_bundle.payload_snapshots[0].payload["hasNextPage"], False)
        self.assertEqual({event.id for event in next_bundle.events}, {14083495})


def _minimal_event_payload(event_id: int, *, status_type: str) -> dict[str, object]:
    return {
        "id": event_id,
        "slug": f"event-{event_id}",
        "status": {"code": 100 if status_type == "finished" else 0, "description": status_type, "type": status_type},
        "season": {"id": 76986, "name": "Premier League 25/26"},
        "startTimestamp": 1775779200,
    }


if __name__ == "__main__":
    unittest.main()
