from __future__ import annotations

import unittest

from schema_inspector.categories_seed_parser import CategoriesSeedParser, CategoriesSeedParserError
from schema_inspector.endpoints import SPORT_FOOTBALL_DATE_CATEGORIES_ENDPOINT, sport_date_categories_endpoint
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
            fetched_at="2026-04-11T10:55:07+00:00",
            status_code=200,
            headers={"Content-Type": "application/json"},
            body_bytes=b"{}",
            payload=payload,
            attempts=(TransportAttempt(1, "proxy_3", 200, None, None),),
            final_proxy_name="proxy_3",
            challenge_reason=None,
        )


class CategoriesSeedParserTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_endpoint_template_matches_sofascore_path(self) -> None:
        self.assertEqual(
            SPORT_FOOTBALL_DATE_CATEGORIES_ENDPOINT.build_url(
                date="2026-04-11",
                timezone_offset_seconds=10800,
            ),
            "https://www.sofascore.com/api/v1/sport/football/2026-04-11/10800/categories",
        )
        self.assertEqual(
            sport_date_categories_endpoint("basketball").build_url(
                date="2026-04-11",
                timezone_offset_seconds=10800,
            ),
            "https://www.sofascore.com/api/v1/sport/basketball/2026-04-11/10800/categories",
        )

    async def test_categories_seed_parser_builds_normalized_bundle(self) -> None:
        url = SPORT_FOOTBALL_DATE_CATEGORIES_ENDPOINT.build_url(
            date="2026-04-11",
            timezone_offset_seconds=10800,
        )
        fake_client = _FakeSofascoreClient(
            {
                url: {
                    "categories": [
                        {
                            "category": {
                                "id": 1,
                                "slug": "england",
                                "name": "England",
                                "alpha2": "EN",
                                "flag": "england",
                                "priority": 10,
                                "sport": {"id": 1, "slug": "football", "name": "Football"},
                                "fieldTranslations": {"nameTranslation": {"ru": "Англия"}},
                            },
                            "teamIds": [43, 44, 30],
                            "uniqueTournamentIds": [17, 173],
                            "totalEventPlayerStatistics": 9,
                            "totalEvents": 48,
                            "totalVideos": 0,
                        },
                        {
                            "category": {
                                "id": 2,
                                "slug": "spain",
                                "name": "Spain",
                                "alpha2": "ES",
                                "flag": "spain",
                                "priority": 9,
                                "sport": {"id": 1, "slug": "football", "name": "Football"},
                            },
                            "teamIds": [2817, 2829],
                            "totalEventPlayerStatistics": 4,
                            "totalEvents": 8,
                            "totalVideos": 1,
                        },
                    ]
                }
            }
        )

        parser = CategoriesSeedParser(fake_client)
        bundle = await parser.fetch_daily_categories("2026-04-11", 10800)

        self.assertEqual(fake_client.seen_urls, [url])
        self.assertEqual(len(bundle.registry_entries), 1)
        self.assertEqual(len(bundle.payload_snapshots), 1)
        self.assertEqual({item.id for item in bundle.categories}, {1, 2})
        self.assertEqual({item.id for item in bundle.sports}, {1})
        self.assertEqual(
            {(item.category_id, item.total_events) for item in bundle.daily_summaries},
            {(1, 48), (2, 8)},
        )
        self.assertEqual(
            {(item.category_id, item.unique_tournament_id) for item in bundle.daily_unique_tournaments},
            {(1, 17), (1, 173)},
        )
        self.assertEqual(
            {(item.category_id, item.team_id) for item in bundle.daily_teams},
            {(1, 43), (1, 44), (1, 30), (2, 2817), (2, 2829)},
        )

    async def test_categories_seed_parser_requires_categories_envelope(self) -> None:
        url = SPORT_FOOTBALL_DATE_CATEGORIES_ENDPOINT.build_url(
            date="2026-04-11",
            timezone_offset_seconds=10800,
        )
        fake_client = _FakeSofascoreClient({url: {"items": []}})

        parser = CategoriesSeedParser(fake_client)
        with self.assertRaises(CategoriesSeedParserError):
            await parser.fetch_daily_categories("2026-04-11", 10800)

    async def test_categories_seed_parser_supports_basketball_paths(self) -> None:
        url = sport_date_categories_endpoint("basketball").build_url(
            date="2026-04-11",
            timezone_offset_seconds=10800,
        )
        fake_client = _FakeSofascoreClient(
            {
                url: {
                    "categories": [
                        {
                            "category": {
                                "id": 15,
                                "slug": "usa",
                                "name": "USA",
                                "flag": "usa",
                                "sport": {"id": 2, "slug": "basketball", "name": "Basketball"},
                            },
                            "uniqueTournamentIds": [132],
                            "teamIds": [3424, 3439],
                            "totalEvents": 9,
                        }
                    ]
                }
            }
        )

        parser = CategoriesSeedParser(fake_client)
        bundle = await parser.fetch_daily_categories("2026-04-11", 10800, sport_slug="basketball")

        self.assertEqual(fake_client.seen_urls, [url])
        self.assertEqual(bundle.registry_entries[0].path_template, "/api/v1/sport/basketball/{date}/{timezone_offset_seconds}/categories")
        self.assertEqual({item.id for item in bundle.sports}, {2})
        self.assertEqual({item.category_id for item in bundle.daily_summaries}, {15})
