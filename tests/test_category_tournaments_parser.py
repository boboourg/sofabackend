from __future__ import annotations

import unittest

from schema_inspector.category_tournaments_parser import (
    CategoryTournamentsParser,
    CategoryTournamentsParserError,
)
from schema_inspector.endpoints import (
    CATEGORY_UNIQUE_TOURNAMENTS_ENDPOINT,
    sport_categories_all_endpoint,
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
            fetched_at="2026-04-14T20:13:12+00:00",
            status_code=200,
            headers={"Content-Type": "application/json"},
            body_bytes=b"{}",
            payload=payload,
            attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
            final_proxy_name="proxy_1",
            challenge_reason=None,
        )


class CategoryTournamentsParserTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_endpoint_templates_match_sofascore_paths(self) -> None:
        self.assertEqual(
            sport_categories_all_endpoint("tennis").build_url(),
            "https://www.sofascore.com/api/v1/sport/tennis/categories/all",
        )
        self.assertEqual(
            CATEGORY_UNIQUE_TOURNAMENTS_ENDPOINT.build_url(category_id=-101),
            "https://www.sofascore.com/api/v1/category/-101/unique-tournaments",
        )

    async def test_fetch_categories_all_builds_competition_compatible_bundle(self) -> None:
        url = sport_categories_all_endpoint("tennis").build_url()
        fake_client = _FakeSofascoreClient(
            {
                url: {
                    "categories": [
                        {
                            "id": 3,
                            "slug": "atp",
                            "name": "ATP",
                            "flag": "atp",
                            "priority": 7,
                            "sport": {"id": 5, "slug": "tennis", "name": "Tennis"},
                            "fieldTranslations": {"nameTranslation": {"ru": "ATP"}},
                        },
                        {
                            "id": 1012,
                            "slug": "iptl",
                            "name": "IPTL",
                            "alpha2": "CL",
                            "flag": "iptl",
                            "priority": 0,
                            "sport": {"id": 5, "slug": "tennis", "name": "Tennis"},
                            "fieldTranslations": {"nameTranslation": {"ru": "IPTL"}},
                        },
                    ]
                }
            }
        )

        parser = CategoryTournamentsParser(fake_client)
        bundle = await parser.fetch_categories_all(sport_slug="tennis")

        self.assertEqual(fake_client.seen_urls, [url])
        self.assertEqual(bundle.category_ids, (3, 1012))
        self.assertEqual(bundle.unique_tournament_ids, ())
        self.assertEqual(bundle.active_unique_tournament_ids, ())
        self.assertEqual(len(bundle.competition_bundle.payload_snapshots), 1)
        self.assertEqual({item.id for item in bundle.competition_bundle.categories}, {3, 1012})
        self.assertEqual({item.id for item in bundle.competition_bundle.sports}, {5})

    async def test_fetch_category_unique_tournaments_prefers_active_ids_metadata(self) -> None:
        url = CATEGORY_UNIQUE_TOURNAMENTS_ENDPOINT.build_url(category_id=-101)
        fake_client = _FakeSofascoreClient(
            {
                url: {
                    "activeUniqueTournamentIds": [2407, 2423],
                    "groups": [
                        {
                            "name": "ATP",
                            "isActive": True,
                            "uniqueTournaments": [
                                {
                                    "id": 2407,
                                    "slug": "barcelona",
                                    "name": "Barcelona",
                                    "userCount": 5271,
                                    "displayInverseHomeAwayTeams": False,
                                    "fieldTranslations": {"nameTranslation": {"ru": "Барселона"}},
                                    "category": {
                                        "id": 3,
                                        "slug": "atp",
                                        "name": "ATP",
                                        "flag": "atp",
                                        "priority": 7,
                                        "sport": {"id": 5, "slug": "tennis", "name": "Tennis"},
                                        "fieldTranslations": {"nameTranslation": {"ru": "ATP"}},
                                    },
                                },
                                {
                                    "id": 2423,
                                    "slug": "barcelona-doubles",
                                    "name": "Barcelona, Doubles",
                                    "userCount": 229,
                                    "displayInverseHomeAwayTeams": False,
                                    "fieldTranslations": {"nameTranslation": {"ru": "Барселона, пары"}},
                                    "category": {
                                        "id": 3,
                                        "slug": "atp",
                                        "name": "ATP",
                                        "flag": "atp",
                                        "priority": 7,
                                        "sport": {"id": 5, "slug": "tennis", "name": "Tennis"},
                                        "fieldTranslations": {"nameTranslation": {"ru": "ATP"}},
                                    },
                                },
                            ],
                        }
                    ],
                }
            }
        )

        parser = CategoryTournamentsParser(fake_client)
        bundle = await parser.fetch_category_unique_tournaments(-101, sport_slug="tennis")

        self.assertEqual(fake_client.seen_urls, [url])
        self.assertEqual(bundle.group_names, ("ATP",))
        self.assertEqual(bundle.active_unique_tournament_ids, (2407, 2423))
        self.assertEqual(bundle.unique_tournament_ids, (2407, 2423))
        self.assertEqual({item.id for item in bundle.competition_bundle.unique_tournaments}, {2407, 2423})
        self.assertEqual({item.id for item in bundle.competition_bundle.categories}, {3})

    async def test_fetch_category_unique_tournaments_requires_groups_envelope(self) -> None:
        url = CATEGORY_UNIQUE_TOURNAMENTS_ENDPOINT.build_url(category_id=-101)
        fake_client = _FakeSofascoreClient({url: {"items": []}})
        parser = CategoryTournamentsParser(fake_client)

        with self.assertRaises(CategoryTournamentsParserError):
            await parser.fetch_category_unique_tournaments(-101, sport_slug="tennis")


if __name__ == "__main__":
    unittest.main()
