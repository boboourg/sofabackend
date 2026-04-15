from __future__ import annotations

import unittest

from schema_inspector.default_tournaments_parser import DefaultTournamentListParser, DefaultTournamentListParserError
from schema_inspector.endpoints import DEFAULT_UNIQUE_TOURNAMENTS_ENDPOINT
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
            fetched_at="2026-04-12T10:00:00+00:00",
            status_code=200,
            headers={"Content-Type": "application/json"},
            body_bytes=b"{}",
            payload=payload,
            attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
            final_proxy_name="proxy_1",
            challenge_reason=None,
        )


class DefaultTournamentsParserTests(unittest.IsolatedAsyncioTestCase):
    def test_exact_endpoint_template_matches_sofascore_path(self) -> None:
        self.assertEqual(
            DEFAULT_UNIQUE_TOURNAMENTS_ENDPOINT.build_url(country_code="UA", sport_slug="football"),
            "https://www.sofascore.com/api/v1/config/default-unique-tournaments/UA/football",
        )

    async def test_parser_extracts_unique_tournament_ids_in_order(self) -> None:
        url = DEFAULT_UNIQUE_TOURNAMENTS_ENDPOINT.build_url(country_code="UA", sport_slug="football")
        fake_client = _FakeSofascoreClient(
            {
                url: {
                    "uniqueTournaments": [
                        {"id": 17, "name": "Premier League"},
                        {"id": 8, "name": "LaLiga"},
                        {"id": 17, "name": "Premier League"},
                    ]
                }
            }
        )

        parser = DefaultTournamentListParser(fake_client)
        result = await parser.fetch(country_code="ua", sport_slug="Football")

        self.assertEqual(fake_client.seen_urls, [url])
        self.assertEqual(result.country_code, "UA")
        self.assertEqual(result.sport_slug, "football")
        self.assertEqual(result.unique_tournament_ids, (17, 8))

    async def test_parser_requires_unique_tournaments_array(self) -> None:
        url = DEFAULT_UNIQUE_TOURNAMENTS_ENDPOINT.build_url(country_code="UA", sport_slug="football")
        fake_client = _FakeSofascoreClient({url: {"items": []}})

        parser = DefaultTournamentListParser(fake_client)
        with self.assertRaises(DefaultTournamentListParserError):
            await parser.fetch(country_code="UA", sport_slug="football")


if __name__ == "__main__":
    unittest.main()
