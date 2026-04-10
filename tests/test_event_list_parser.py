from __future__ import annotations

import unittest

from schema_inspector.event_list_parser import EventListParser, EventListParserError
from schema_inspector.endpoints import (
    SPORT_FOOTBALL_LIVE_EVENTS_ENDPOINT,
    SPORT_FOOTBALL_SCHEDULED_EVENTS_ENDPOINT,
    UNIQUE_TOURNAMENT_FEATURED_EVENTS_ENDPOINT,
    UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT,
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
            UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT.build_url(
                unique_tournament_id=17,
                season_id=76986,
                round_number=32,
            ),
            "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/events/round/32",
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
        self.assertEqual(len(bundle.registry_entries), 4)
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


if __name__ == "__main__":
    unittest.main()
