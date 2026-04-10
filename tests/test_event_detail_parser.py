from __future__ import annotations

import unittest

from schema_inspector.endpoints import (
    EVENT_DETAIL_ENDPOINT,
    EVENT_H2H_ENDPOINT,
    EVENT_LINEUPS_ENDPOINT,
    EVENT_MANAGERS_ENDPOINT,
    EVENT_ODDS_ALL_ENDPOINT,
    EVENT_ODDS_FEATURED_ENDPOINT,
    EVENT_PREGAME_FORM_ENDPOINT,
    EVENT_VOTES_ENDPOINT,
    EVENT_WINNING_ODDS_ENDPOINT,
)
from schema_inspector.event_detail_parser import EventDetailParser
from schema_inspector.runtime import RuntimeConfig, TransportAttempt, TransportResult
from schema_inspector.sofascore_client import SofascoreClient, SofascoreHttpError, SofascoreResponse


def _not_found_error(url: str) -> SofascoreHttpError:
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


class EventDetailParserTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_endpoint_templates_match_sofascore_paths(self) -> None:
        self.assertEqual(
            EVENT_DETAIL_ENDPOINT.build_url(event_id=14083191),
            "https://www.sofascore.com/api/v1/event/14083191",
        )
        self.assertEqual(
            EVENT_LINEUPS_ENDPOINT.build_url(event_id=14083191),
            "https://www.sofascore.com/api/v1/event/14083191/lineups",
        )
        self.assertEqual(
            EVENT_MANAGERS_ENDPOINT.build_url(event_id=14083191),
            "https://www.sofascore.com/api/v1/event/14083191/managers",
        )
        self.assertEqual(
            EVENT_H2H_ENDPOINT.build_url(event_id=14083191),
            "https://www.sofascore.com/api/v1/event/14083191/h2h",
        )
        self.assertEqual(
            EVENT_PREGAME_FORM_ENDPOINT.build_url(event_id=14083191),
            "https://www.sofascore.com/api/v1/event/14083191/pregame-form",
        )
        self.assertEqual(
            EVENT_VOTES_ENDPOINT.build_url(event_id=14083191),
            "https://www.sofascore.com/api/v1/event/14083191/votes",
        )
        self.assertEqual(
            EVENT_ODDS_ALL_ENDPOINT.build_url(event_id=14083191, provider_id=1),
            "https://www.sofascore.com/api/v1/event/14083191/odds/1/all",
        )
        self.assertEqual(
            EVENT_ODDS_FEATURED_ENDPOINT.build_url(event_id=14083191, provider_id=1),
            "https://www.sofascore.com/api/v1/event/14083191/odds/1/featured",
        )
        self.assertEqual(
            EVENT_WINNING_ODDS_ENDPOINT.build_url(event_id=14083191, provider_id=1),
            "https://www.sofascore.com/api/v1/event/14083191/provider/1/winning-odds",
        )

    async def test_event_detail_parser_builds_bundle_and_skips_optional_404(self) -> None:
        event_url = EVENT_DETAIL_ENDPOINT.build_url(event_id=14083191)
        lineups_url = EVENT_LINEUPS_ENDPOINT.build_url(event_id=14083191)
        managers_url = EVENT_MANAGERS_ENDPOINT.build_url(event_id=14083191)
        h2h_url = EVENT_H2H_ENDPOINT.build_url(event_id=14083191)
        pregame_url = EVENT_PREGAME_FORM_ENDPOINT.build_url(event_id=14083191)
        votes_url = EVENT_VOTES_ENDPOINT.build_url(event_id=14083191)
        odds_all_url = EVENT_ODDS_ALL_ENDPOINT.build_url(event_id=14083191, provider_id=1)
        odds_featured_url = EVENT_ODDS_FEATURED_ENDPOINT.build_url(event_id=14083191, provider_id=1)
        winning_odds_url = EVENT_WINNING_ODDS_ENDPOINT.build_url(event_id=14083191, provider_id=1)

        fake_client = _FakeSofascoreClient(
            {
                event_url: {
                    "event": {
                        "id": 14083191,
                        "slug": "arsenal-chelsea",
                        "customId": "abc123",
                        "detailId": 9,
                        "tournament": {
                            "id": 1,
                            "slug": "premier-league",
                            "name": "Premier League",
                            "competitionType": 1,
                            "category": {
                                "id": 1,
                                "slug": "england",
                                "name": "England",
                                "alpha2": "EN",
                                "sport": {"id": 1, "slug": "football", "name": "Football"},
                                "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
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
                                "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                                "hasEventPlayerStatistics": True,
                            },
                        },
                        "season": {"id": 76986, "name": "Premier League 25/26", "year": "25/26", "editor": False},
                        "status": {"code": 100, "description": "1st half", "type": "inprogress"},
                        "venue": {
                            "id": 55,
                            "slug": "emirates-stadium",
                            "name": "Emirates Stadium",
                            "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                        },
                        "referee": {
                            "id": 99,
                            "slug": "michael-oliver",
                            "name": "Michael Oliver",
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                            "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                        },
                        "homeTeam": {
                            "id": 42,
                            "slug": "arsenal",
                            "name": "Arsenal",
                            "shortName": "Arsenal",
                            "fullName": "Arsenal FC",
                            "nameCode": "ARS",
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                            "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                            "manager": {"id": 500, "slug": "arteta", "name": "Mikel Arteta", "shortName": "M. Arteta"},
                        },
                        "awayTeam": {
                            "id": 43,
                            "slug": "chelsea",
                            "name": "Chelsea",
                            "shortName": "Chelsea",
                            "nameCode": "CHE",
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                            "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                            "manager": {"id": 501, "slug": "maresca", "name": "Enzo Maresca", "shortName": "E. Maresca"},
                        },
                        "homeScore": {"current": 1, "display": 1},
                        "awayScore": {"current": 0, "display": 0},
                        "roundInfo": {"round": 32, "name": "Round 32"},
                        "statusTime": {"prefix": "'", "timestamp": 1712745600, "initial": 45, "max": 45, "extra": 1},
                        "time": {"currentPeriodStartTimestamp": 1712742000, "periodLength": 45, "totalPeriodCount": 2},
                        "eventFilters": {"category": ["live"]},
                        "changes": {"changeTimestamp": 1712745600, "changes": ["status"]},
                        "seasonStatisticsType": "overall",
                        "startTimestamp": 1775779200,
                        "coverage": 1,
                        "winnerCode": 1,
                        "aggregatedWinnerCode": 1,
                        "homeRedCards": 0,
                        "awayRedCards": 0,
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
                },
                lineups_url: {
                    "home": {
                        "formation": "4-3-3",
                        "playerColor": {"primary": "#ff0000"},
                        "goalkeeperColor": {"primary": "#00ff00"},
                        "supportStaff": [],
                        "players": [
                            {
                                "avgRating": 7.1,
                                "jerseyNumber": "7",
                                "shirtNumber": 7,
                                "position": "F",
                                "substitute": False,
                                "teamId": 42,
                                "player": {
                                    "id": 700,
                                    "slug": "saka",
                                    "name": "Bukayo Saka",
                                    "shortName": "B. Saka",
                                    "firstName": "Bukayo",
                                    "lastName": "Saka",
                                    "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                                    "gender": "M",
                                    "position": "F",
                                    "jerseyNumber": "7",
                                    "sofascoreId": "saka7",
                                    "dateOfBirthTimestamp": 963273600,
                                    "height": 178,
                                    "marketValueCurrency": "EUR",
                                    "proposedMarketValueRaw": {"value": 100000000},
                                },
                            }
                        ],
                        "missingPlayers": [],
                    },
                    "away": {
                        "formation": "4-2-3-1",
                        "playerColor": {"primary": "#0000ff"},
                        "goalkeeperColor": {"primary": "#ffff00"},
                        "supportStaff": [],
                        "players": [],
                        "missingPlayers": [
                            {
                                "description": "Hamstring",
                                "expectedEndDate": "2026-05-01",
                                "externalType": 72,
                                "reason": 1,
                                "type": "missing",
                                "player": {
                                    "id": 701,
                                    "slug": "james",
                                    "name": "Reece James",
                                    "shortName": "R. James",
                                    "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                                    "gender": "M",
                                    "position": "D",
                                    "jerseyNumber": "24",
                                    "dateOfBirthTimestamp": 963273600,
                                    "height": 179,
                                    "marketValueCurrency": "EUR",
                                    "proposedMarketValueRaw": {"value": 45000000},
                                },
                            }
                        ],
                    },
                },
                managers_url: {
                    "homeManager": {"id": 500, "slug": "arteta", "name": "Mikel Arteta", "shortName": "M. Arteta"},
                    "awayManager": {"id": 501, "slug": "maresca", "name": "Enzo Maresca", "shortName": "E. Maresca"},
                },
                h2h_url: _not_found_error(h2h_url),
                pregame_url: {
                    "label": "Pts",
                    "homeTeam": {"avgRating": "6.73", "position": 2, "value": "70", "form": ["W", "W", "D"]},
                    "awayTeam": {"avgRating": "6.68", "position": 5, "value": "60", "form": ["L", "W", "W"]},
                },
                votes_url: {
                    "vote": {"vote1": 10, "voteX": 2, "vote2": 5},
                    "bothTeamsToScoreVote": {"voteYes": 7, "voteNo": 3},
                },
                odds_all_url: {
                    "eventId": 14083191,
                    "markets": [
                        {
                            "id": 289779151,
                            "fid": 191744484,
                            "marketId": 1,
                            "sourceId": 191744484,
                            "marketGroup": "1X2",
                            "marketName": "Full time",
                            "marketPeriod": "Full-time",
                            "structureType": 1,
                            "choiceGroup": "default",
                            "isLive": False,
                            "suspended": False,
                            "choices": [
                                {
                                    "sourceId": 11,
                                    "name": "1",
                                    "change": 0,
                                    "fractionalValue": "1/1",
                                    "initialFractionalValue": "11/10",
                                }
                            ],
                        }
                    ],
                },
                odds_featured_url: _not_found_error(odds_featured_url),
                winning_odds_url: {
                    "home": {"id": 1, "actual": 52, "expected": 49, "fractionalValue": "1/1"},
                    "away": {"id": 2, "actual": 24, "expected": 30, "fractionalValue": "3/1"},
                },
            }
        )

        parser = EventDetailParser(fake_client)
        bundle = await parser.fetch_bundle(14083191, provider_ids=(1,))

        self.assertEqual(len(bundle.registry_entries), 9)
        self.assertEqual({item.id for item in bundle.events}, {14083191})
        self.assertEqual({item.id for item in bundle.teams}, {42, 43})
        self.assertEqual({item.id for item in bundle.managers}, {500, 501})
        self.assertEqual({item.id for item in bundle.players}, {700, 701})
        self.assertEqual({item.id for item in bundle.providers}, {1})
        self.assertEqual(len(bundle.event_duels), 0)
        self.assertEqual(len(bundle.event_lineups), 2)
        self.assertEqual(len(bundle.event_lineup_players), 1)
        self.assertEqual(len(bundle.event_lineup_missing_players), 1)
        self.assertEqual(len(bundle.event_pregame_form_items), 6)
        self.assertEqual(len(bundle.event_vote_options), 5)
        self.assertEqual(len(bundle.event_markets), 1)
        self.assertEqual(len(bundle.event_market_choices), 1)
        self.assertEqual(len(bundle.event_winning_odds), 2)
        self.assertEqual(
            {(item.side, item.manager_id) for item in bundle.event_manager_assignments},
            {("home", 500), ("away", 501)},
        )
        self.assertIn(h2h_url, fake_client.seen_urls)


if __name__ == "__main__":
    unittest.main()
