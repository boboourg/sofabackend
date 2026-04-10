from __future__ import annotations

import unittest

from schema_inspector.endpoints import TOURNAMENT_STANDINGS_ENDPOINT, UNIQUE_TOURNAMENT_STANDINGS_ENDPOINT
from schema_inspector.runtime import RuntimeConfig, TransportAttempt
from schema_inspector.sofascore_client import SofascoreClient, SofascoreResponse
from schema_inspector.standings_parser import StandingsParser


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


class StandingsParserTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_standings_endpoint_templates_match_sofascore_paths(self) -> None:
        self.assertEqual(
            UNIQUE_TOURNAMENT_STANDINGS_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986, scope="total"),
            "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/standings/total",
        )
        self.assertEqual(
            TOURNAMENT_STANDINGS_ENDPOINT.build_url(tournament_id=1, season_id=76986, scope="home"),
            "https://www.sofascore.com/api/v1/tournament/1/season/76986/standings/home",
        )

    async def test_standings_parser_builds_bundle(self) -> None:
        url = UNIQUE_TOURNAMENT_STANDINGS_ENDPOINT.build_url(
            unique_tournament_id=17,
            season_id=76986,
            scope="total",
        )
        fake_client = _FakeSofascoreClient(
            {
                url: {
                    "standings": [
                        {
                            "id": 178737,
                            "name": "Premier League 25/26",
                            "type": "total",
                            "updatedAtTimestamp": 1775634161,
                            "descriptions": [],
                            "tieBreakingRule": {
                                "id": 2393,
                                "text": "Points, goal difference, goals scored.",
                            },
                            "tournament": {
                                "id": 1,
                                "slug": "premier-league",
                                "name": "Premier League",
                                "isGroup": False,
                                "isLive": False,
                                "priority": 701,
                                "fieldTranslations": {"nameTranslation": {"ru": "Премьер-лига"}},
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
                                    "displayInverseHomeAwayTeams": False,
                                    "hasPerformanceGraphFeature": True,
                                    "primaryColorHex": "#3c1c5a",
                                    "secondaryColorHex": "#f80158",
                                    "userCount": 1178753,
                                    "fieldTranslations": {"nameTranslation": {"ru": "Премьер-лига"}},
                                    "category": {
                                        "id": 1,
                                        "slug": "england",
                                        "name": "England",
                                        "alpha2": "EN",
                                        "flag": "england",
                                        "priority": 10,
                                        "sport": {"id": 1, "slug": "football", "name": "Football"},
                                    },
                                },
                            },
                            "rows": [
                                {
                                    "id": 1637254,
                                    "position": 1,
                                    "matches": 31,
                                    "wins": 21,
                                    "draws": 7,
                                    "losses": 3,
                                    "points": 70,
                                    "scoresFor": 61,
                                    "scoresAgainst": 22,
                                    "scoreDiffFormatted": "+39",
                                    "descriptions": [],
                                    "promotion": {"id": 804, "text": "Champions League"},
                                    "team": {
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
                                        "gender": "M",
                                        "type": 0,
                                        "national": False,
                                        "disabled": False,
                                        "userCount": 3322281,
                                        "teamColors": {
                                            "primary": "#cc0000",
                                            "secondary": "#ffffff",
                                            "text": "#ffffff",
                                        },
                                        "fieldTranslations": {"nameTranslation": {"ru": "Арсенал"}},
                                    },
                                }
                            ],
                        }
                    ]
                }
            }
        )

        parser = StandingsParser(fake_client)
        bundle = await parser.fetch_unique_tournament_standings(17, 76986, scopes=("total",))

        self.assertEqual(len(bundle.registry_entries), 2)
        self.assertEqual({item.id for item in bundle.sports}, {1})
        self.assertEqual({item.alpha2 for item in bundle.countries}, {"EN"})
        self.assertEqual({item.id for item in bundle.categories}, {1})
        self.assertEqual({item.id for item in bundle.unique_tournaments}, {17})
        self.assertEqual({item.id for item in bundle.tournaments}, {1})
        self.assertEqual({item.id for item in bundle.teams}, {42})
        self.assertEqual({item.id for item in bundle.tie_breaking_rules}, {2393})
        self.assertEqual({item.id for item in bundle.promotions}, {804})
        self.assertEqual({item.id for item in bundle.standings}, {178737})
        self.assertEqual({item.id for item in bundle.standing_rows}, {1637254})
        self.assertEqual(bundle.standing_rows[0].promotion_id, 804)
        self.assertEqual(fake_client.seen_urls, [url])


if __name__ == "__main__":
    unittest.main()
