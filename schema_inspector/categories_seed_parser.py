"""Parser for the date/timezone sport categories discovery endpoint."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date as Date
from typing import Any, Mapping

from .competition_parser import ApiPayloadSnapshotRecord, CategoryRecord, CountryRecord, SportRecord
from .endpoints import (
    EndpointRegistryEntry,
    categories_seed_registry_entries,
    sport_date_categories_endpoint,
)
from .sofascore_client import SofascoreClient, SofascoreResponse


@dataclass(frozen=True)
class CategoryDailySummaryRecord:
    observed_date: Date
    timezone_offset_seconds: int
    category_id: int
    total_events: int | None = None
    total_event_player_statistics: int | None = None
    total_videos: int | None = None


@dataclass(frozen=True)
class CategoryDailyUniqueTournamentRecord:
    observed_date: Date
    timezone_offset_seconds: int
    category_id: int
    unique_tournament_id: int
    ordinal: int


@dataclass(frozen=True)
class CategoryDailyTeamRecord:
    observed_date: Date
    timezone_offset_seconds: int
    category_id: int
    team_id: int
    ordinal: int


@dataclass(frozen=True)
class CategoriesSeedBundle:
    registry_entries: tuple[EndpointRegistryEntry, ...]
    payload_snapshots: tuple[ApiPayloadSnapshotRecord, ...]
    sports: tuple[SportRecord, ...]
    countries: tuple[CountryRecord, ...]
    categories: tuple[CategoryRecord, ...]
    daily_summaries: tuple[CategoryDailySummaryRecord, ...]
    daily_unique_tournaments: tuple[CategoryDailyUniqueTournamentRecord, ...]
    daily_teams: tuple[CategoryDailyTeamRecord, ...]


class CategoriesSeedParserError(RuntimeError):
    """Raised when the categories discovery payload misses its expected envelope."""


class CategoriesSeedParser:
    """Fetches and normalizes /sport/{sport_slug}/{date}/{offset}/categories."""

    def __init__(self, client: SofascoreClient, *, logger: logging.Logger | None = None) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    async def fetch_daily_categories(
        self,
        observed_date: str,
        timezone_offset_seconds: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> CategoriesSeedBundle:
        endpoint = sport_date_categories_endpoint(sport_slug)
        url = endpoint.build_url(
            date=observed_date,
            timezone_offset_seconds=timezone_offset_seconds,
        )
        response = await self.client.get_json(url, timeout=timeout)
        payload = _require_root_mapping(response.payload, url)
        category_items = _require_category_array(payload, endpoint.envelope_key, url)

        state = _CategoriesSeedAccumulator(
            observed_date=Date.fromisoformat(observed_date),
            timezone_offset_seconds=timezone_offset_seconds,
        )
        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            payload=payload,
        )
        for item in category_items:
            if isinstance(item, Mapping):
                state.ingest_category_entry(item)

        self.logger.info(
            "Categories seed bundle collected: categories=%s unique_tournaments=%s teams=%s",
            len(state.categories),
            len(state.daily_unique_tournaments),
            len(state.daily_teams),
        )
        return state.to_bundle(registry_entries=categories_seed_registry_entries(sport_slug=sport_slug))


class _CategoriesSeedAccumulator:
    def __init__(self, *, observed_date: Date, timezone_offset_seconds: int) -> None:
        self.observed_date = observed_date
        self.timezone_offset_seconds = timezone_offset_seconds
        self.payload_snapshots: list[ApiPayloadSnapshotRecord] = []
        self.sports: dict[int, dict[str, Any]] = {}
        self.countries: dict[str, dict[str, Any]] = {}
        self.categories: dict[int, dict[str, Any]] = {}
        self.daily_summaries: dict[tuple[Date, int, int], dict[str, Any]] = {}
        self.daily_unique_tournaments: dict[tuple[Date, int, int, int], dict[str, Any]] = {}
        self.daily_teams: dict[tuple[Date, int, int, int], dict[str, Any]] = {}

    def add_payload_snapshot(
        self,
        *,
        endpoint_pattern: str,
        response: SofascoreResponse,
        envelope_key: str,
        payload: Mapping[str, Any],
    ) -> None:
        self.payload_snapshots.append(
            ApiPayloadSnapshotRecord(
                endpoint_pattern=endpoint_pattern,
                source_url=response.source_url,
                envelope_key=envelope_key,
                context_entity_type=None,
                context_entity_id=None,
                payload=dict(payload),
                fetched_at=response.fetched_at,
            )
        )

    def ingest_category_entry(self, payload: Mapping[str, Any]) -> int | None:
        category_id = self.ingest_category(_as_mapping(payload.get("category")))
        if category_id is None:
            return None

        self._merge(
            self.daily_summaries,
            (self.observed_date, self.timezone_offset_seconds, category_id),
            {
                "observed_date": self.observed_date,
                "timezone_offset_seconds": self.timezone_offset_seconds,
                "category_id": category_id,
                "total_events": _as_int(payload.get("totalEvents")),
                "total_event_player_statistics": _as_int(payload.get("totalEventPlayerStatistics")),
                "total_videos": _as_int(payload.get("totalVideos")),
            },
        )

        unique_tournament_ids = payload.get("uniqueTournamentIds")
        if isinstance(unique_tournament_ids, list):
            for ordinal, unique_tournament_id in enumerate(unique_tournament_ids):
                if isinstance(unique_tournament_id, int) and not isinstance(unique_tournament_id, bool):
                    self._merge(
                        self.daily_unique_tournaments,
                        (self.observed_date, self.timezone_offset_seconds, category_id, unique_tournament_id),
                        {
                            "observed_date": self.observed_date,
                            "timezone_offset_seconds": self.timezone_offset_seconds,
                            "category_id": category_id,
                            "unique_tournament_id": unique_tournament_id,
                            "ordinal": ordinal,
                        },
                    )

        team_ids = payload.get("teamIds")
        if isinstance(team_ids, list):
            for ordinal, team_id in enumerate(team_ids):
                if isinstance(team_id, int) and not isinstance(team_id, bool):
                    self._merge(
                        self.daily_teams,
                        (self.observed_date, self.timezone_offset_seconds, category_id, team_id),
                        {
                            "observed_date": self.observed_date,
                            "timezone_offset_seconds": self.timezone_offset_seconds,
                            "category_id": category_id,
                            "team_id": team_id,
                            "ordinal": ordinal,
                        },
                    )
        return category_id

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

    def to_bundle(self, *, registry_entries: tuple[EndpointRegistryEntry, ...]) -> CategoriesSeedBundle:
        return CategoriesSeedBundle(
            registry_entries=registry_entries,
            payload_snapshots=tuple(self.payload_snapshots),
            sports=tuple(SportRecord(**row) for _, row in sorted(self.sports.items())),
            countries=tuple(CountryRecord(**row) for _, row in sorted(self.countries.items())),
            categories=tuple(CategoryRecord(**row) for _, row in sorted(self.categories.items())),
            daily_summaries=tuple(
                CategoryDailySummaryRecord(**row) for _, row in sorted(self.daily_summaries.items())
            ),
            daily_unique_tournaments=tuple(
                CategoryDailyUniqueTournamentRecord(**row)
                for _, row in sorted(self.daily_unique_tournaments.items())
            ),
            daily_teams=tuple(CategoryDailyTeamRecord(**row) for _, row in sorted(self.daily_teams.items())),
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
        raise CategoriesSeedParserError(
            f"Expected object payload for {source_url}, got {type(payload).__name__}"
        )
    return payload


def _require_category_array(payload: Mapping[str, Any], envelope_key: str, source_url: str) -> tuple[object, ...]:
    envelope = payload.get(envelope_key)
    if not isinstance(envelope, list):
        raise CategoriesSeedParserError(f"Missing array envelope '{envelope_key}' for {source_url}")
    return tuple(envelope)


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    return value if isinstance(value, int) else None
