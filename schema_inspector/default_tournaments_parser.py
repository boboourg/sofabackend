"""Helpers for the default-unique-tournaments configuration endpoint."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from .endpoints import DEFAULT_UNIQUE_TOURNAMENTS_ENDPOINT
from .sofascore_client import SofascoreClient


@dataclass(frozen=True)
class DefaultTournamentList:
    source_url: str
    fetched_at: str
    country_code: str
    sport_slug: str
    unique_tournament_ids: tuple[int, ...]
    payload: Mapping[str, Any]


class DefaultTournamentListParserError(RuntimeError):
    """Raised when the default-tournaments payload is malformed."""


class DefaultTournamentListParser:
    """Fetches Sofascore's curated default-tournament list for one country and sport."""

    def __init__(self, client: SofascoreClient, *, logger: logging.Logger | None = None) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    async def fetch(
        self,
        *,
        country_code: str,
        sport_slug: str,
        timeout: float = 20.0,
    ) -> DefaultTournamentList:
        normalized_country = country_code.upper()
        normalized_sport = sport_slug.strip().lower()
        endpoint = DEFAULT_UNIQUE_TOURNAMENTS_ENDPOINT
        url = endpoint.build_url(country_code=normalized_country, sport_slug=normalized_sport)
        response = await self.client.get_json(url, timeout=timeout)
        root = _require_root_mapping(response.payload, url)
        unique_tournament_ids = _extract_unique_tournament_ids(root.get(endpoint.envelope_key))
        self.logger.info(
            "Default tournaments fetched: country=%s sport=%s count=%s",
            normalized_country,
            normalized_sport,
            len(unique_tournament_ids),
        )
        return DefaultTournamentList(
            source_url=response.source_url,
            fetched_at=response.fetched_at,
            country_code=normalized_country,
            sport_slug=normalized_sport,
            unique_tournament_ids=unique_tournament_ids,
            payload=root,
        )


def _extract_unique_tournament_ids(value: object) -> tuple[int, ...]:
    if not isinstance(value, list):
        raise DefaultTournamentListParserError("Missing 'uniqueTournaments' array in default tournaments payload")
    ids: list[int] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        tournament_id = item.get("id")
        if isinstance(tournament_id, int) and tournament_id not in ids:
            ids.append(tournament_id)
    return tuple(ids)


def _require_root_mapping(payload: object, source_url: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise DefaultTournamentListParserError(
            f"Expected object payload for {source_url}, got {type(payload).__name__}"
        )
    return payload
