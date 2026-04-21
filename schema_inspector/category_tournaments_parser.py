"""Wide category/tournament discovery parser built on top of SofascoreClient."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from .competition_parser import (
    ApiPayloadSnapshotRecord,
    CategoryRecord,
    CompetitionBundle,
    CountryRecord,
    SportRecord,
    UniqueTournamentRecord,
)
from .endpoints import (
    CATEGORY_UNIQUE_TOURNAMENTS_ENDPOINT,
    category_tournament_discovery_registry_entries,
    sport_categories_all_endpoint,
)
from .sofascore_client import SofascoreClient, SofascoreResponse


@dataclass(frozen=True)
class CategoryTournamentsBundle:
    competition_bundle: CompetitionBundle
    category_ids: tuple[int, ...]
    unique_tournament_ids: tuple[int, ...]
    active_unique_tournament_ids: tuple[int, ...]
    group_names: tuple[str, ...]
    source_slug: str = "sofascore"
    discovery_surface: str = "category_unique_tournaments"


class CategoryTournamentsParserError(RuntimeError):
    """Raised when the category/tournament discovery payload is malformed."""


class CategoryTournamentsParser:
    """Fetches sport/category discovery endpoints that expose unique tournaments."""

    def __init__(self, client: SofascoreClient, *, logger: logging.Logger | None = None) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    async def fetch_categories_all(
        self,
        *,
        sport_slug: str = "tennis",
        timeout: float = 20.0,
    ) -> CategoryTournamentsBundle:
        endpoint = sport_categories_all_endpoint(sport_slug)
        url = endpoint.build_url()
        response = await self.client.get_json(url, timeout=timeout)
        payload = _require_root_mapping(response.payload, url)
        categories = _require_array(payload, endpoint.envelope_key, url)

        state = _CategoryTournamentAccumulator()
        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="sport",
            context_entity_id=None,
            payload=payload,
        )
        for item in categories:
            if isinstance(item, Mapping):
                state.ingest_category(item)

        self.logger.info(
            "Category catalog collected: sport=%s categories=%s",
            sport_slug,
            len(state.categories),
        )
        return state.to_bundle(
            sport_slug=sport_slug,
            source_slug=endpoint.source_slug,
            discovery_surface="categories_all",
        )

    async def fetch_category_unique_tournaments(
        self,
        category_id: int,
        *,
        sport_slug: str = "tennis",
        timeout: float = 20.0,
    ) -> CategoryTournamentsBundle:
        endpoint = CATEGORY_UNIQUE_TOURNAMENTS_ENDPOINT
        url = endpoint.build_url(category_id=category_id)
        response = await self.client.get_json(url, timeout=timeout)
        payload = _require_root_mapping(response.payload, url)
        groups = _require_array(payload, endpoint.envelope_key, url)

        state = _CategoryTournamentAccumulator()
        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="category",
            context_entity_id=category_id,
            payload=payload,
        )
        for item in payload.get("activeUniqueTournamentIds", []):
            if isinstance(item, int) and not isinstance(item, bool):
                state.active_unique_tournament_ids.add(item)
        for group in groups:
            if isinstance(group, Mapping):
                state.ingest_group(group)

        self.logger.info(
            "Category unique tournaments collected: category_id=%s groups=%s tournaments=%s active=%s",
            category_id,
            len(state.group_names),
            len(state.unique_tournaments),
            len(state.active_unique_tournament_ids),
        )
        return state.to_bundle(
            sport_slug=sport_slug,
            source_slug=endpoint.source_slug,
            discovery_surface="category_unique_tournaments",
        )


class _CategoryTournamentAccumulator:
    def __init__(self) -> None:
        self.payload_snapshots: list[ApiPayloadSnapshotRecord] = []
        self.sports: dict[int, dict[str, Any]] = {}
        self.countries: dict[str, dict[str, Any]] = {}
        self.categories: dict[int, dict[str, Any]] = {}
        self.unique_tournaments: dict[int, dict[str, Any]] = {}
        self.group_names: set[str] = set()
        self.active_unique_tournament_ids: set[int] = set()

    def add_payload_snapshot(
        self,
        *,
        endpoint_pattern: str,
        response: SofascoreResponse,
        envelope_key: str,
        context_entity_type: str | None,
        context_entity_id: int | None,
        payload: Mapping[str, Any],
    ) -> None:
        self.payload_snapshots.append(
            ApiPayloadSnapshotRecord(
                endpoint_pattern=endpoint_pattern,
                source_url=response.source_url,
                envelope_key=envelope_key,
                context_entity_type=context_entity_type,
                context_entity_id=context_entity_id,
                payload=dict(payload),
                fetched_at=response.fetched_at,
            )
        )

    def ingest_group(self, payload: Mapping[str, Any]) -> None:
        group_name = _as_str(payload.get("name"))
        if group_name:
            self.group_names.add(group_name)
        for tournament_payload in _iter_mappings(payload.get("uniqueTournaments")):
            self.ingest_unique_tournament(tournament_payload)

    def ingest_unique_tournament(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        unique_tournament_id = _as_int(payload.get("id"))
        category_id = self.ingest_category(_as_mapping(payload.get("category")))
        if unique_tournament_id is None or category_id is None:
            return unique_tournament_id

        self._merge(
            self.unique_tournaments,
            unique_tournament_id,
            {
                "id": unique_tournament_id,
                "slug": _as_str(payload.get("slug")),
                "name": _as_str(payload.get("name")),
                "category_id": category_id,
                "country_alpha2": self.ingest_country(_as_mapping(payload.get("country"))),
                "gender": _as_str(payload.get("gender")),
                "primary_color_hex": _as_str(payload.get("primaryColorHex")),
                "secondary_color_hex": _as_str(payload.get("secondaryColorHex")),
                "user_count": _as_int(payload.get("userCount")),
                "has_event_player_statistics": _as_bool(payload.get("hasEventPlayerStatistics")),
                "has_performance_graph_feature": _as_bool(payload.get("hasPerformanceGraphFeature")),
                "display_inverse_home_away_teams": _as_bool(payload.get("displayInverseHomeAwayTeams")),
                "field_translations": _as_mapping(payload.get("fieldTranslations")),
                "period_length": _as_mapping(payload.get("periodLength")),
            },
        )
        return unique_tournament_id

    def ingest_category(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        category_id = _as_int(payload.get("id"))
        sport_id = self.ingest_sport(_as_mapping(payload.get("sport")))
        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
        if country_alpha2 is None:
            fallback_alpha2 = _as_str(payload.get("alpha2"))
            if fallback_alpha2:
                self._merge(
                    self.countries,
                    fallback_alpha2,
                    {
                        "alpha2": fallback_alpha2,
                        "alpha3": None,
                        "slug": _as_str(payload.get("slug")),
                        "name": _as_str(payload.get("name")) or fallback_alpha2,
                    },
                )
                country_alpha2 = fallback_alpha2
        if category_id is None or sport_id is None:
            return category_id

        self._merge(
            self.categories,
            category_id,
            {
                "id": category_id,
                "slug": _as_str(payload.get("slug")),
                "name": _as_str(payload.get("name")),
                "sport_id": sport_id,
                "alpha2": _as_str(payload.get("alpha2")),
                "flag": _as_str(payload.get("flag")),
                "priority": _as_int(payload.get("priority")),
                "country_alpha2": country_alpha2 or _as_str(payload.get("alpha2")),
                "field_translations": _as_mapping(payload.get("fieldTranslations")),
            },
        )
        return category_id

    def ingest_sport(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        sport_id = _as_int(payload.get("id"))
        if sport_id is None:
            return None
        self._merge(
            self.sports,
            sport_id,
            {"id": sport_id, "slug": _as_str(payload.get("slug")), "name": _as_str(payload.get("name"))},
        )
        return sport_id

    def ingest_country(self, payload: Mapping[str, Any] | None) -> str | None:
        if not payload:
            return None
        alpha2 = _as_str(payload.get("alpha2"))
        if not alpha2:
            return None
        self._merge(
            self.countries,
            alpha2,
            {
                "alpha2": alpha2,
                "alpha3": _as_str(payload.get("alpha3")),
                "slug": _as_str(payload.get("slug")),
                "name": _as_str(payload.get("name")),
            },
        )
        return alpha2

    def to_bundle(
        self,
        *,
        sport_slug: str,
        source_slug: str = "sofascore",
        discovery_surface: str = "category_unique_tournaments",
    ) -> CategoryTournamentsBundle:
        competition_bundle = CompetitionBundle(
            registry_entries=category_tournament_discovery_registry_entries(sport_slug=sport_slug),
            payload_snapshots=tuple(self.payload_snapshots),
            image_assets=(),
            sports=tuple(SportRecord(**row) for _, row in sorted(self.sports.items())),
            countries=tuple(CountryRecord(**row) for _, row in sorted(self.countries.items())),
            categories=tuple(CategoryRecord(**row) for _, row in sorted(self.categories.items())),
            teams=(),
            unique_tournaments=tuple(
                UniqueTournamentRecord(**row) for _, row in sorted(self.unique_tournaments.items())
            ),
            unique_tournament_relations=(),
            unique_tournament_most_title_teams=(),
            seasons=(),
            unique_tournament_seasons=(),
        )
        return CategoryTournamentsBundle(
            competition_bundle=competition_bundle,
            category_ids=tuple(sorted(self.categories)),
            unique_tournament_ids=tuple(sorted(self.unique_tournaments)),
            active_unique_tournament_ids=tuple(sorted(self.active_unique_tournament_ids)),
            group_names=tuple(sorted(self.group_names)),
            source_slug=source_slug,
            discovery_surface=discovery_surface,
        )

    @staticmethod
    def _merge(store: dict[Any, dict[str, Any]], key: Any, row: dict[str, Any]) -> None:
        current = dict(store.get(key, {}))
        for field_name, value in row.items():
            if value is not None or field_name not in current:
                current[field_name] = value
        store[key] = current


def _require_root_mapping(payload: object, source_url: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise CategoryTournamentsParserError(
            f"Expected object payload for {source_url}, got {type(payload).__name__}"
        )
    return payload


def _require_array(payload: Mapping[str, Any], envelope_key: str, source_url: str) -> tuple[object, ...]:
    envelope = payload.get(envelope_key)
    if not isinstance(envelope, list):
        raise CategoryTournamentsParserError(f"Missing array envelope '{envelope_key}' for {source_url}")
    return tuple(envelope)


def _iter_mappings(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    return value if isinstance(value, int) else None


def _as_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None
