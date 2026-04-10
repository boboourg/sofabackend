from __future__ import annotations

import unittest

from schema_inspector.endpoints import (
    UNIQUE_TOURNAMENT_STATISTICS_ENDPOINT,
    UNIQUE_TOURNAMENT_STATISTICS_INFO_ENDPOINT,
)
from schema_inspector.runtime import RuntimeConfig, TransportAttempt
from schema_inspector.sofascore_client import SofascoreClient, SofascoreResponse
from schema_inspector.statistics_parser import StatisticsParser, StatisticsQuery


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


class StatisticsParserTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_statistics_endpoint_templates_match_sofascore_paths(self) -> None:
        info_url = UNIQUE_TOURNAMENT_STATISTICS_INFO_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986)
        self.assertEqual(
            info_url,
            "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/statistics/info",
        )

        query = StatisticsQuery(
            limit=20,
            order="-rating",
            accumulation="per90",
            group="summary",
            fields=("goals", "tackles"),
            filters=("appearances.GT.4", "position.in.G~D~M~F", "team.in.42~40"),
        )
        stats_url = UNIQUE_TOURNAMENT_STATISTICS_ENDPOINT.build_url_with_query(
            unique_tournament_id=17,
            season_id=76986,
            query_params=query.to_query_params(),
        )
        self.assertEqual(
            stats_url,
            "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/statistics"
            "?limit=20&order=-rating&accumulation=per90&group=summary&fields=goals%2Ctackles"
            "&filters=appearances.GT.4%2Cposition.in.G~D~M~F%2Cteam.in.42~40",
        )

    async def test_statistics_parser_builds_config_and_snapshot_bundle(self) -> None:
        query = StatisticsQuery(
            limit=20,
            order="-rating",
            accumulation="per90",
            group="summary",
            fields=("goals", "tackles"),
            filters=("appearances.GT.4", "position.in.G~D~M~F", "team.in.42~40"),
        )
        info_url = UNIQUE_TOURNAMENT_STATISTICS_INFO_ENDPOINT.build_url(unique_tournament_id=17, season_id=76986)
        stats_url = UNIQUE_TOURNAMENT_STATISTICS_ENDPOINT.build_url_with_query(
            unique_tournament_id=17,
            season_id=76986,
            query_params=query.to_query_params(),
        )
        fake_client = _FakeSofascoreClient(
            {
                info_url: {
                    "hideHomeAndAway": False,
                    "teams": [
                        {
                            "id": 42,
                            "slug": "arsenal",
                            "name": "Arsenal",
                            "shortName": "Arsenal",
                            "nameCode": "ARS",
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                            "gender": "M",
                            "type": 0,
                            "national": False,
                            "disabled": False,
                            "userCount": 3325428,
                            "teamColors": {"primary": "#cc0000", "secondary": "#ffffff", "text": "#ffffff"},
                            "fieldTranslations": {"nameTranslation": {"ru": "Арсенал"}},
                        },
                        {
                            "id": 40,
                            "slug": "liverpool",
                            "name": "Liverpool",
                            "shortName": "Liverpool",
                            "nameCode": "LIV",
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                            "gender": "M",
                            "type": 0,
                            "national": False,
                            "disabled": False,
                            "userCount": 2500000,
                            "teamColors": {"primary": "#aa0000", "secondary": "#ffffff", "text": "#ffffff"},
                            "fieldTranslations": {"nameTranslation": {"ru": "Ливерпуль"}},
                        },
                    ],
                    "statisticsGroups": {
                        "summary": ["rating", "goals"],
                        "passing": ["accuratePassesPercentage"],
                        "detailed": {"attack": ["goalsFromOutsideTheBox"]},
                    },
                    "nationalities": {"EN": "England", "UA": "Ukraine"},
                },
                stats_url: {
                    "page": 1,
                    "pages": 27,
                    "results": [
                        {
                            "rating": 7.6,
                            "goals": 6,
                            "tackles": 6,
                            "accuratePassesPercentage": 66.67,
                            "expectedGoals": 0.96,
                            "player": {
                                "id": 1445189,
                                "slug": "tom-edozie",
                                "name": "Tom Edozie",
                                "gender": "M",
                                "userCount": 630,
                                "fieldTranslations": {"nameTranslation": {"ru": "Том Эдози"}},
                            },
                            "team": {
                                "id": 42,
                                "slug": "arsenal",
                                "name": "Arsenal",
                                "national": False,
                                "sport": {"id": 1, "slug": "football", "name": "Football"},
                                "teamColors": {"primary": "#cc0000", "secondary": "#ffffff", "text": "#ffffff"},
                                "fieldTranslations": {"nameTranslation": {"ru": "Арсенал"}},
                                "type": 0,
                                "userCount": 3325428,
                            },
                        },
                        {
                            "rating": 7.1,
                            "goals": 4,
                            "tackles": 3,
                            "accuratePassesPercentage": 70.0,
                            "expectedGoals": None,
                            "player": {
                                "id": 2001,
                                "slug": "second-player",
                                "name": "Second Player",
                                "gender": "M",
                                "userCount": 101,
                                "fieldTranslations": {"nameTranslation": {"ru": "Второй Игрок"}},
                            },
                            "team": {
                                "id": 40,
                                "slug": "liverpool",
                                "name": "Liverpool",
                                "national": False,
                                "sport": {"id": 1, "slug": "football", "name": "Football"},
                                "teamColors": {"primary": "#aa0000", "secondary": "#ffffff", "text": "#ffffff"},
                                "fieldTranslations": {"nameTranslation": {"ru": "Ливерпуль"}},
                                "type": 0,
                                "userCount": 2500000,
                            },
                        },
                    ],
                },
            }
        )

        parser = StatisticsParser(fake_client)
        bundle = await parser.fetch_bundle(17, 76986, queries=(query,), include_info=True)

        self.assertEqual(len(bundle.registry_entries), 2)
        self.assertEqual(len(bundle.configs), 1)
        self.assertEqual(bundle.configs[0].hide_home_and_away, False)
        self.assertEqual({item.team_id for item in bundle.config_teams}, {40, 42})
        self.assertEqual(
            {(item.group_scope, item.group_name, item.stat_field) for item in bundle.group_items},
            {
                ("regular", "summary", "rating"),
                ("regular", "summary", "goals"),
                ("regular", "passing", "accuratePassesPercentage"),
                ("detailed", "attack", "goalsFromOutsideTheBox"),
            },
        )
        self.assertEqual({item.nationality_code for item in bundle.nationalities}, {"EN", "UA"})
        self.assertEqual({item.id for item in bundle.teams}, {40, 42})
        self.assertEqual({item.id for item in bundle.players}, {1445189, 2001})
        self.assertEqual(len(bundle.snapshots), 1)
        snapshot = bundle.snapshots[0]
        self.assertEqual(snapshot.page, 1)
        self.assertEqual(snapshot.pages, 27)
        self.assertEqual(snapshot.limit_value, 20)
        self.assertEqual(snapshot.order_code, "-rating")
        self.assertEqual(snapshot.accumulation, "per90")
        self.assertEqual(snapshot.group_code, "summary")
        self.assertEqual(snapshot.fields, ("goals", "tackles"))
        self.assertEqual(
            snapshot.filters,
            (
                {
                    "expression": "appearances.GT.4",
                    "field": "appearances",
                    "operator": "GT",
                    "raw_value": "4",
                    "values": (4,),
                },
                {
                    "expression": "position.in.G~D~M~F",
                    "field": "position",
                    "operator": "in",
                    "raw_value": "G~D~M~F",
                    "values": ("G", "D", "M", "F"),
                },
                {
                    "expression": "team.in.42~40",
                    "field": "team",
                    "operator": "in",
                    "raw_value": "42~40",
                    "values": (42, 40),
                },
            ),
        )
        self.assertEqual(len(snapshot.results), 2)
        self.assertEqual(snapshot.results[0].player_id, 1445189)
        self.assertEqual(snapshot.results[0].team_id, 42)
        self.assertEqual(snapshot.results[0].rating, 7.6)
        self.assertEqual(snapshot.results[0].goals, 6)
        self.assertEqual(snapshot.results[0].tackles, 6)
        self.assertEqual(snapshot.results[0].accurate_passes_percentage, 66.67)
        self.assertEqual(snapshot.results[0].expected_goals, 0.96)
        self.assertEqual(fake_client.seen_urls, [info_url, stats_url])


if __name__ == "__main__":
    unittest.main()
