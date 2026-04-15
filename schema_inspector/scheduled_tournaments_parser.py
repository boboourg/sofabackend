"""Parser for daily sport scheduled-tournament discovery endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .endpoints import scheduled_tournament_discovery_registry_entries, sport_scheduled_tournaments_endpoint
from .event_list_parser import EventListBundle, _EventListAccumulator, _as_bool, _as_mapping, _require_root_mapping
from .sofascore_client import SofascoreClient


@dataclass(frozen=True)
class ScheduledTournamentsBundle:
    event_list_bundle: EventListBundle
    observed_date: str
    page: int
    has_next_page: bool
    tournament_ids: tuple[int, ...]
    unique_tournament_ids: tuple[int, ...]


class ScheduledTournamentsParser:
    """Fetches /sport/{sport}/scheduled-tournaments/{date}/page/{page} and normalizes discovery data."""

    def __init__(self, client: SofascoreClient, *, logger: logging.Logger | None = None) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    async def fetch_page(
        self,
        observed_date: str,
        page: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> ScheduledTournamentsBundle:
        endpoint = sport_scheduled_tournaments_endpoint(sport_slug)
        url = endpoint.build_url(date=observed_date, page=page)
        response = await self.client.get_json(url, timeout=timeout)
        payload = _require_root_mapping(response.payload, url)
        scheduled_items = payload.get(endpoint.envelope_key)
        if not isinstance(scheduled_items, list):
            raise RuntimeError(f"Missing array envelope '{endpoint.envelope_key}' for {url}")

        state = _EventListAccumulator()
        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="scheduled_tournaments",
            context_entity_id=None,
            payload=payload,
        )
        for item in scheduled_items:
            item_mapping = _as_mapping(item)
            if not item_mapping:
                continue
            state.ingest_tournament(_as_mapping(item_mapping.get("tournament")))

        bundle = state.to_bundle(
            registry_entries=scheduled_tournament_discovery_registry_entries(sport_slug=sport_slug),
        )
        has_next_page = bool(_as_bool(payload.get("hasNextPage")))
        self.logger.info(
            "Scheduled tournaments collected: sport=%s date=%s page=%s tournaments=%s unique_tournaments=%s has_next_page=%s",
            sport_slug,
            observed_date,
            page,
            len(bundle.tournaments),
            len(bundle.unique_tournaments),
            has_next_page,
        )
        return ScheduledTournamentsBundle(
            event_list_bundle=bundle,
            observed_date=observed_date,
            page=page,
            has_next_page=has_next_page,
            tournament_ids=tuple(sorted(item.id for item in bundle.tournaments)),
            unique_tournament_ids=tuple(sorted(item.id for item in bundle.unique_tournaments)),
        )
