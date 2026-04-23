from __future__ import annotations

import unittest

from schema_inspector.competition_parser import CompetitionParser, CompetitionParserError
from schema_inspector.endpoints import (
    COMPETITION_ENDPOINTS,
    UNIQUE_TOURNAMENT_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASON_INFO_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASONS_ENDPOINT,
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


class CompetitionParserTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_endpoint_templates_match_sofascore_paths(self) -> None:
        self.assertEqual(
            UNIQUE_TOURNAMENT_ENDPOINT.build_url(unique_tournament_id=17),
            "https://www.sofascore.com/api/v1/unique-tournament/17",
        )
        self.assertEqual(
            UNIQUE_TOURNAMENT_SEASONS_ENDPOINT.build_url(unique_tournament_id=17),
            "https://www.sofascore.com/api/v1/unique-tournament/17/seasons",
        )
        self.assertEqual(
            UNIQUE_TOURNAMENT_SEASON_INFO_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986),
            "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/info",
        )
        self.assertEqual(
            UNIQUE_TOURNAMENT_ENDPOINT.pattern,
            "/api/v1/unique-tournament/{unique_tournament_id}",
        )

    async def test_competition_parser_builds_normalized_bundle(self) -> None:
        tournament_url = UNIQUE_TOURNAMENT_ENDPOINT.build_url(unique_tournament_id=17)
        seasons_url = UNIQUE_TOURNAMENT_SEASONS_ENDPOINT.build_url(unique_tournament_id=17)
        info_url = UNIQUE_TOURNAMENT_SEASON_INFO_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986)

        fake_client = _FakeSofascoreClient(
            {
                tournament_url: {
                    "uniqueTournament": {
                        "id": 17,
                        "slug": "premier-league",
                        "name": "Premier League",
                        "category": {
                            "id": 1,
                            "slug": "england",
                            "name": "England",
                            "alpha2": "EN",
                            "flag": "england",
                            "priority": 10,
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                            "country": {"alpha2": "EN", "name": "England"},
                            "fieldTranslations": {"nameTranslation": {"ru": "Англия"}},
                        },
                        "country": {"alpha2": "EN", "name": "England"},
                        "logo": {"id": 1418035, "md5": "logo-md5"},
                        "darkLogo": {"id": 1418033, "md5": "dark-logo-md5"},
                        "titleHolder": {
                            "id": 42,
                            "slug": "liverpool",
                            "name": "Liverpool",
                            "shortName": "Liverpool",
                            "nameCode": "LIV",
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                            "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                            "teamColors": {"primary": "#dd0000", "secondary": "#ffffff", "text": "#ffffff"},
                            "fieldTranslations": {"nameTranslation": {"ru": "Ливерпуль"}},
                            "gender": "M",
                            "type": 0,
                            "national": False,
                            "disabled": False,
                            "userCount": 500000,
                        },
                        "mostTitlesTeams": [
                            {
                                "id": 42,
                                "slug": "liverpool",
                                "name": "Liverpool",
                                "sport": {"id": 1, "slug": "football", "name": "Football"},
                                "country": {"alpha2": "EN", "name": "England"},
                            }
                        ],
                        "linkedUniqueTournaments": [],
                        "upperDivisions": [],
                        "lowerDivisions": [
                            {
                                "id": 35,
                                "slug": "championship",
                                "name": "Championship",
                                "category": {
                                    "id": 1,
                                    "slug": "england",
                                    "name": "England",
                                    "alpha2": "EN",
                                    "flag": "england",
                                    "priority": 10,
                                    "sport": {"id": 1, "slug": "football", "name": "Football"},
                                    "country": {"alpha2": "EN", "name": "England"},
                                },
                                "country": {"alpha2": "EN", "name": "England"},
                                "logo": {"id": 2200, "md5": "championship-logo"},
                                "fieldTranslations": {"nameTranslation": {"ru": "Чемпионшип"}},
                                "periodLength": {"regular": 90},
                                "mostTitlesTeams": [],
                            }
                        ],
                        "titleHolderTitles": 20,
                        "mostTitles": 20,
                        "gender": "M",
                        "primaryColorHex": "#3c1c5a",
                        "secondaryColorHex": "#f80158",
                        "startDateTimestamp": 1755216000,
                        "endDateTimestamp": 1779580800,
                        "tier": 1,
                        "userCount": 1179222,
                        "hasRounds": True,
                        "hasGroups": False,
                        "hasPerformanceGraphFeature": True,
                        "hasPlayoffSeries": False,
                        "disabledHomeAwayStandings": False,
                        "displayInverseHomeAwayTeams": False,
                        "fieldTranslations": {"nameTranslation": {"ru": "Премьер-лига"}},
                        "periodLength": {"regular": 90},
                    }
                },
                seasons_url: {
                    "seasons": [
                        {"id": 76986, "name": "Premier League 25/26", "year": "25/26", "editor": False},
                        {"id": 72958, "name": "Premier League 24/25", "year": "24/25", "editor": False},
                    ]
                },
                info_url: {
                    "info": {
                        "id": 49510,
                        "season": {"id": 76986, "name": "Premier League 25/26", "year": "25/26", "editor": False},
                        "homeTeamWins": 129,
                        "awayTeamWins": 96,
                        "draws": 84,
                        "goals": 845,
                        "yellowCards": 1181,
                        "redCards": 39,
                        "numberOfCompetitors": 20,
                        "newcomersUpperDivision": [],
                        "newcomersOther": [],
                        "hostCountries": [],
                        "newcomersLowerDivision": [
                            {
                                "id": 34,
                                "slug": "leeds-united",
                                "name": "Leeds United",
                                "shortName": "Leeds",
                                "nameCode": "LEE",
                                "sport": {"id": 1, "slug": "football", "name": "Football"},
                                "country": {"alpha2": "EN", "alpha3": "ENG", "slug": "england", "name": "England"},
                                "teamColors": {"primary": "#ffffff", "secondary": "#1d4189", "text": "#1d4189"},
                                "fieldTranslations": {"nameTranslation": {"ru": "Лидс"}},
                                "gender": "M",
                                "type": 0,
                                "national": False,
                                "disabled": False,
                                "userCount": 240968,
                            }
                        ],
                    }
                },
            }
        )

        parser = CompetitionParser(fake_client)
        bundle = await parser.fetch_bundle(17, season_id=76986)

        self.assertEqual(fake_client.seen_urls, [tournament_url, seasons_url, info_url])
        self.assertEqual(len(bundle.registry_entries), len(COMPETITION_ENDPOINTS))
        self.assertEqual(len(bundle.payload_snapshots), 3)
        self.assertEqual(bundle.sports[0].slug, "football")
        self.assertEqual(bundle.categories[0].slug, "england")
        self.assertEqual(bundle.unique_tournaments[0].title_holder_team_id, 42)
        self.assertEqual(bundle.unique_tournaments[0].category_id, 1)
        self.assertEqual({team.id for team in bundle.teams}, {34, 42})
        self.assertEqual(
            {(item.unique_tournament_id, item.team_id) for item in bundle.unique_tournament_most_title_teams},
            {(17, 42)},
        )
        self.assertEqual({season.id for season in bundle.seasons}, {72958, 76986})
        self.assertEqual(
            {(item.unique_tournament_id, item.season_id) for item in bundle.unique_tournament_seasons},
            {(17, 72958), (17, 76986)},
        )
        self.assertIn(
            (17, 35, "lower_division"),
            {
                (item.unique_tournament_id, item.related_unique_tournament_id, item.relation_type)
                for item in bundle.unique_tournament_relations
            },
        )

    async def test_competition_parser_raises_on_missing_envelope(self) -> None:
        tournament_url = UNIQUE_TOURNAMENT_ENDPOINT.build_url(unique_tournament_id=17)
        fake_client = _FakeSofascoreClient({tournament_url: {"wrongKey": {}}})
        parser = CompetitionParser(fake_client)

        with self.assertRaises(CompetitionParserError):
            await parser.fetch_bundle(17, include_seasons=False, include_season_info=False)


if __name__ == "__main__":
    unittest.main()
