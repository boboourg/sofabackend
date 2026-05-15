"""Async competition parser built on top of SofascoreClient."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .endpoints import (
    EndpointRegistryEntry,
    UNIQUE_TOURNAMENT_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASON_INFO_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASONS_ENDPOINT,
    competition_registry_entries,
)
from .sofascore_client import SofascoreClient, SofascoreHttpError, SofascoreResponse


@dataclass(frozen=True)
class ApiPayloadSnapshotRecord:
    endpoint_pattern: str
    source_url: str
    envelope_key: str
    context_entity_type: str | None
    context_entity_id: int | None
    payload: Mapping[str, Any]
    fetched_at: str


@dataclass(frozen=True)
class ImageAssetRecord:
    id: int
    md5: str


@dataclass(frozen=True)
class SportRecord:
    id: int
    slug: str
    name: str


@dataclass(frozen=True)
class CountryRecord:
    alpha2: str
    name: str
    alpha3: str | None = None
    slug: str | None = None


@dataclass(frozen=True)
class CategoryRecord:
    id: int
    slug: str
    name: str
    sport_id: int
    alpha2: str | None = None
    flag: str | None = None
    priority: int | None = None
    country_alpha2: str | None = None
    field_translations: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class TeamRecord:
    id: int
    slug: str
    name: str
    short_name: str | None = None
    name_code: str | None = None
    sport_id: int | None = None
    country_alpha2: str | None = None
    gender: str | None = None
    type: int | None = None
    national: bool | None = None
    disabled: bool | None = None
    user_count: int | None = None
    field_translations: Mapping[str, Any] | None = None
    team_colors: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class UniqueTournamentRecord:
    id: int
    slug: str
    name: str
    category_id: int
    country_alpha2: str | None = None
    logo_asset_id: int | None = None
    dark_logo_asset_id: int | None = None
    title_holder_team_id: int | None = None
    title_holder_titles: int | None = None
    most_titles: int | None = None
    gender: str | None = None
    primary_color_hex: str | None = None
    secondary_color_hex: str | None = None
    start_date_timestamp: int | None = None
    end_date_timestamp: int | None = None
    tier: int | None = None
    user_count: int | None = None
    has_rounds: bool | None = None
    has_groups: bool | None = None
    has_event_player_statistics: bool | None = None
    has_performance_graph_feature: bool | None = None
    has_playoff_series: bool | None = None
    disabled_home_away_standings: bool | None = None
    display_inverse_home_away_teams: bool | None = None
    field_translations: Mapping[str, Any] | None = None
    period_length: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class UniqueTournamentRelationRecord:
    unique_tournament_id: int
    related_unique_tournament_id: int
    relation_type: str


@dataclass(frozen=True)
class UniqueTournamentMostTitleTeamRecord:
    unique_tournament_id: int
    team_id: int


@dataclass(frozen=True)
class SeasonRecord:
    id: int
    name: str | None = None
    year: str | None = None
    editor: bool | None = None


@dataclass(frozen=True)
class UniqueTournamentSeasonRecord:
    unique_tournament_id: int
    season_id: int


@dataclass(frozen=True)
class SeasonRoundRecord:
    """One row of /unique-tournament/{ut}/season/{s}/rounds payload.

    Task 3 (2026-05-15): structural rounds get parsed inline during
    competition ingest because the normalize stream path is reserved
    for hydrate snapshots — structural snapshots are written directly
    to api_payload_snapshot and normalised via competition_repository.
    """

    unique_tournament_id: int
    season_id: int
    round_number: int
    round_name: str | None = None
    round_slug: str | None = None
    round_prefix: str | None = None
    is_current: bool = False


@dataclass(frozen=True)
class CompetitionBundle:
    registry_entries: tuple[EndpointRegistryEntry, ...]
    payload_snapshots: tuple[ApiPayloadSnapshotRecord, ...]
    image_assets: tuple[ImageAssetRecord, ...]
    sports: tuple[SportRecord, ...]
    countries: tuple[CountryRecord, ...]
    categories: tuple[CategoryRecord, ...]
    teams: tuple[TeamRecord, ...]
    unique_tournaments: tuple[UniqueTournamentRecord, ...]
    unique_tournament_relations: tuple[UniqueTournamentRelationRecord, ...]
    unique_tournament_most_title_teams: tuple[UniqueTournamentMostTitleTeamRecord, ...]
    seasons: tuple[SeasonRecord, ...]
    unique_tournament_seasons: tuple[UniqueTournamentSeasonRecord, ...]
    season_rounds: tuple[SeasonRoundRecord, ...] = ()


class CompetitionParserError(RuntimeError):
    """Raised when a competition payload misses its expected envelope."""


class CompetitionParser:
    """Fetches competition-family endpoints and normalizes their entities."""

    def __init__(self, client: SofascoreClient, *, logger: logging.Logger | None = None) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    async def fetch_bundle(
        self,
        unique_tournament_id: int,
        *,
        season_id: int | None = None,
        include_seasons: bool = True,
        include_season_info: bool | None = None,
        include_season_rounds: bool | None = None,
        timeout: float = 20.0,
    ) -> CompetitionBundle:
        """Fetch a normalized competition bundle from exact Sofascore paths."""

        if include_season_info is None:
            include_season_info = season_id is not None
        # Task 3 (2026-05-15): season rounds leaf — fan-out from
        # structure-sync. Auto-on when a season_id is provided. Failure-
        # tolerant (404 swallowed) for sports without rounds.
        if include_season_rounds is None:
            include_season_rounds = season_id is not None

        state = _CompetitionAccumulator()

        await self._fetch_unique_tournament(unique_tournament_id, state, timeout=timeout)
        if include_seasons:
            await self._fetch_seasons(unique_tournament_id, state, timeout=timeout)
        if include_season_info:
            if season_id is None:
                raise CompetitionParserError("season_id is required when include_season_info=True")
            await self._fetch_season_info(unique_tournament_id, season_id, state, timeout=timeout)
        if include_season_rounds:
            if season_id is None:
                raise CompetitionParserError("season_id is required when include_season_rounds=True")
            await self._fetch_season_rounds(unique_tournament_id, season_id, state, timeout=timeout)

        self.logger.info(
            "Competition bundle collected: tournaments=%s seasons=%s teams=%s",
            len(state.unique_tournaments),
            len(state.seasons),
            len(state.teams),
        )
        return state.to_bundle()

    async def _fetch_unique_tournament(
        self,
        unique_tournament_id: int,
        state: "_CompetitionAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = UNIQUE_TOURNAMENT_ENDPOINT
        url = endpoint.build_url(unique_tournament_id=unique_tournament_id)
        response = await self.client.get_json(url, timeout=timeout)
        envelope = _require_mapping(response.payload, endpoint.envelope_key, url)

        # Persist the full upstream response (``{"uniqueTournament": {...}}``)
        # so the snapshot stays 1:1 with sofascore. Use the unwrapped envelope
        # only for downstream normalization, not for snapshot storage.
        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="unique_tournament",
            context_entity_id=unique_tournament_id,
            payload=_full_response_payload(response, fallback_key=endpoint.envelope_key, fallback_value=envelope),
        )
        state.ingest_unique_tournament(envelope)

    async def _fetch_seasons(
        self,
        unique_tournament_id: int,
        state: "_CompetitionAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = UNIQUE_TOURNAMENT_SEASONS_ENDPOINT
        url = endpoint.build_url(unique_tournament_id=unique_tournament_id)
        try:
            response = await self.client.get_json(url, timeout=timeout)
        except SofascoreHttpError as exc:
            status_code = exc.transport_result.status_code if exc.transport_result is not None else None
            if status_code == 404:
                self.logger.info(
                    "Competition optional 404: context=unique_tournament:%s endpoint=%s url=%s",
                    unique_tournament_id,
                    endpoint.pattern,
                    url,
                )
                return
            raise
        seasons = _require_sequence(response.payload, endpoint.envelope_key, url)

        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="unique_tournament",
            context_entity_id=unique_tournament_id,
            payload={endpoint.envelope_key: list(seasons)},
        )
        for season in seasons:
            if isinstance(season, Mapping):
                state.ingest_season(season, unique_tournament_id=unique_tournament_id)

    async def _fetch_season_info(
        self,
        unique_tournament_id: int,
        season_id: int,
        state: "_CompetitionAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = UNIQUE_TOURNAMENT_SEASON_INFO_ENDPOINT
        url = endpoint.build_url(unique_tournament_id=unique_tournament_id, season_id=season_id)
        try:
            response = await self.client.get_json(url, timeout=timeout)
        except SofascoreHttpError as exc:
            status_code = exc.transport_result.status_code if exc.transport_result is not None else None
            if status_code == 404:
                self.logger.info(
                    "Competition optional 404: context=unique_tournament:%s season:%s endpoint=%s url=%s",
                    unique_tournament_id,
                    season_id,
                    endpoint.pattern,
                    url,
                )
                return
            raise
        envelope = _require_mapping(response.payload, endpoint.envelope_key, url)

        # Persist the full upstream response (``{"info": {...}}``) so the
        # snapshot stays 1:1 with sofascore. Use the unwrapped envelope only
        # for downstream normalization.
        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="season",
            context_entity_id=season_id,
            payload=_full_response_payload(response, fallback_key=endpoint.envelope_key, fallback_value=envelope),
        )
        state.ingest_season_info(envelope, unique_tournament_id=unique_tournament_id)

    async def _fetch_season_rounds(
        self,
        unique_tournament_id: int,
        season_id: int,
        state: "_CompetitionAccumulator",
        *,
        timeout: float,
    ) -> None:
        """Fetch /unique-tournament/{ut}/season/{s}/rounds (Task 3 fan-out).

        Stores snapshot only. The normalize stream picks it up via the
        SeasonRoundsParser family — populates the season_round table with
        ``{round_number, round_name, round_slug, round_prefix, is_current}``.

        Sports without round structure (tennis, table-tennis, etc.) return
        404 and the fan-out is silently skipped. Other HTTP errors are
        re-raised so the orchestrator can surface them.

        Unlike the other competition endpoints, the rounds payload uses an
        ARRAY envelope (``{"rounds": [...]}``) rather than an object — so
        we do not call ``_require_mapping`` on it. We still validate that
        the top-level response is a mapping and that the envelope key is
        present.
        """

        endpoint = UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT
        url = endpoint.build_url(unique_tournament_id=unique_tournament_id, season_id=season_id)
        try:
            response = await self.client.get_json(url, timeout=timeout)
        except SofascoreHttpError as exc:
            status_code = exc.transport_result.status_code if exc.transport_result is not None else None
            if status_code == 404:
                self.logger.info(
                    "Competition optional 404: context=unique_tournament:%s season:%s endpoint=%s url=%s",
                    unique_tournament_id,
                    season_id,
                    endpoint.pattern,
                    url,
                )
                return
            raise

        # Soft validation only: the response must be a mapping carrying
        # the envelope key. The value of envelope_key="rounds" is an
        # array, which is fine — the normalize parser handles array
        # payloads natively.
        if not isinstance(response.payload, Mapping):
            self.logger.warning(
                "Season rounds payload not a mapping: context=unique_tournament:%s season:%s url=%s type=%s",
                unique_tournament_id,
                season_id,
                url,
                type(response.payload).__name__,
            )
            return
        if endpoint.envelope_key not in response.payload:
            self.logger.warning(
                "Season rounds payload missing envelope key '%s': context=unique_tournament:%s season:%s url=%s",
                endpoint.envelope_key,
                unique_tournament_id,
                season_id,
                url,
            )
            return

        # Persist the full upstream response so /api/v1/.../rounds keeps
        # 1:1 shape with sofascore via the raw snapshot waterfall.
        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="season",
            context_entity_id=season_id,
            payload=_full_response_payload(
                response,
                fallback_key=endpoint.envelope_key,
                fallback_value=response.payload.get(endpoint.envelope_key, []),
            ),
        )

        # Inline normalize. We cannot rely on the normalize stream here
        # because structural snapshots are persisted directly via
        # competition_repository (the stream path is reserved for
        # hydrate-side snapshots). Parse the rounds array right now and
        # let the repository upsert them in the same transaction.
        rounds_payload = response.payload.get(endpoint.envelope_key)
        if not isinstance(rounds_payload, list):
            return
        current_round_block = response.payload.get("currentRound")
        current_round_number: int | None = None
        current_round_meta: Mapping[str, Any] | None = None
        if isinstance(current_round_block, Mapping):
            candidate = current_round_block.get("round")
            if isinstance(candidate, bool):
                candidate = None
            if isinstance(candidate, int):
                current_round_number = candidate
            elif isinstance(candidate, float) and candidate.is_integer():
                current_round_number = int(candidate)
            current_round_meta = current_round_block

        seen_round_numbers: set[int] = set()
        for item in rounds_payload:
            if not isinstance(item, Mapping):
                continue
            raw = item.get("round")
            if isinstance(raw, bool):
                continue
            if isinstance(raw, int):
                round_number = raw
            elif isinstance(raw, float) and raw.is_integer():
                round_number = int(raw)
            else:
                continue
            if round_number in seen_round_numbers:
                continue
            seen_round_numbers.add(round_number)
            name = item.get("name") if isinstance(item.get("name"), str) else None
            slug = item.get("slug") if isinstance(item.get("slug"), str) else None
            prefix = item.get("prefix") if isinstance(item.get("prefix"), str) else None
            is_current = current_round_number is not None and round_number == current_round_number
            # currentRound block can carry name/slug/prefix that the
            # rounds[] list omits — copy them in for the matching row.
            if is_current and current_round_meta is not None:
                if name is None and isinstance(current_round_meta.get("name"), str):
                    name = current_round_meta["name"]
                if slug is None and isinstance(current_round_meta.get("slug"), str):
                    slug = current_round_meta["slug"]
                if prefix is None and isinstance(current_round_meta.get("prefix"), str):
                    prefix = current_round_meta["prefix"]
            state.season_rounds.append(
                SeasonRoundRecord(
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    round_number=round_number,
                    round_name=name,
                    round_slug=slug,
                    round_prefix=prefix,
                    is_current=is_current,
                )
            )

        # If currentRound has a round_number not represented in the
        # rounds[] list (rare but possible for cup tournaments), add it
        # so the row carries is_current=True.
        if (
            current_round_number is not None
            and current_round_number not in seen_round_numbers
        ):
            name = None
            slug = None
            prefix = None
            if current_round_meta is not None:
                name = current_round_meta.get("name") if isinstance(current_round_meta.get("name"), str) else None
                slug = current_round_meta.get("slug") if isinstance(current_round_meta.get("slug"), str) else None
                prefix = current_round_meta.get("prefix") if isinstance(current_round_meta.get("prefix"), str) else None
            state.season_rounds.append(
                SeasonRoundRecord(
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    round_number=current_round_number,
                    round_name=name,
                    round_slug=slug,
                    round_prefix=prefix,
                    is_current=True,
                )
            )


class _CompetitionAccumulator:
    def __init__(self) -> None:
        self.payload_snapshots: list[ApiPayloadSnapshotRecord] = []
        self.image_assets: dict[int, dict[str, Any]] = {}
        self.sports: dict[int, dict[str, Any]] = {}
        self.countries: dict[str, dict[str, Any]] = {}
        self.categories: dict[int, dict[str, Any]] = {}
        self.teams: dict[int, dict[str, Any]] = {}
        self.unique_tournaments: dict[int, dict[str, Any]] = {}
        self.unique_tournament_relations: set[tuple[int, int, str]] = set()
        self.unique_tournament_most_title_teams: set[tuple[int, int]] = set()
        self.seasons: dict[int, dict[str, Any]] = {}
        self.unique_tournament_seasons: set[tuple[int, int]] = set()
        # Task 3 (2026-05-15): structural rounds parsed inline (no
        # normalize stream involvement).
        self.season_rounds: list[SeasonRoundRecord] = []

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

    def ingest_unique_tournament(self, payload: Mapping[str, Any]) -> int | None:
        tournament_id = _as_int(payload.get("id"))
        category_id = self.ingest_category(_as_mapping(payload.get("category")))
        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
        logo_asset_id = self.ingest_image(_as_mapping(payload.get("logo")))
        dark_logo_asset_id = self.ingest_image(_as_mapping(payload.get("darkLogo")))
        title_holder_team_id = self.ingest_team(_as_mapping(payload.get("titleHolder")))

        for relation_type, field_name in (
            ("linked", "linkedUniqueTournaments"),
            ("upper_division", "upperDivisions"),
            ("lower_division", "lowerDivisions"),
        ):
            for child in _iter_mappings(payload.get(field_name)):
                child_id = self.ingest_unique_tournament(child)
                if tournament_id is not None and child_id is not None:
                    self.unique_tournament_relations.add((tournament_id, child_id, relation_type))

        for team_payload in _iter_mappings(payload.get("mostTitlesTeams")):
            team_id = self.ingest_team(team_payload)
            if tournament_id is not None and team_id is not None:
                self.unique_tournament_most_title_teams.add((tournament_id, team_id))

        if tournament_id is None or category_id is None:
            return tournament_id

        self._merge(
            self.unique_tournaments,
            tournament_id,
            {
                "id": tournament_id,
                "slug": _as_str(payload.get("slug")),
                "name": _as_str(payload.get("name")),
                "category_id": category_id,
                "country_alpha2": country_alpha2,
                "logo_asset_id": logo_asset_id,
                "dark_logo_asset_id": dark_logo_asset_id,
                "title_holder_team_id": title_holder_team_id,
                "title_holder_titles": _as_int(payload.get("titleHolderTitles")),
                "most_titles": _as_int(payload.get("mostTitles")),
                "gender": _as_str(payload.get("gender")),
                "primary_color_hex": _as_str(payload.get("primaryColorHex")),
                "secondary_color_hex": _as_str(payload.get("secondaryColorHex")),
                "start_date_timestamp": _as_int(payload.get("startDateTimestamp")),
                "end_date_timestamp": _as_int(payload.get("endDateTimestamp")),
                "tier": _as_int(payload.get("tier")),
                "user_count": _as_int(payload.get("userCount")),
                "has_rounds": _as_bool(payload.get("hasRounds")),
                "has_groups": _as_bool(payload.get("hasGroups")),
                "has_event_player_statistics": _as_bool(payload.get("hasEventPlayerStatistics")),
                "has_performance_graph_feature": _as_bool(payload.get("hasPerformanceGraphFeature")),
                "has_playoff_series": _as_bool(payload.get("hasPlayoffSeries")),
                "disabled_home_away_standings": _as_bool(payload.get("disabledHomeAwayStandings")),
                "display_inverse_home_away_teams": _as_bool(payload.get("displayInverseHomeAwayTeams")),
                "field_translations": _as_mapping(payload.get("fieldTranslations")),
                "period_length": _as_mapping(payload.get("periodLength")),
            },
        )
        return tournament_id

    def ingest_season(self, payload: Mapping[str, Any], *, unique_tournament_id: int | None = None) -> int | None:
        season_id = _as_int(payload.get("id"))
        if season_id is None:
            return None

        self._merge(
            self.seasons,
            season_id,
            {
                "id": season_id,
                "name": _as_str(payload.get("name")),
                "year": _as_str(payload.get("year")),
                "editor": _as_bool(payload.get("editor")),
            },
        )
        if unique_tournament_id is not None:
            self.unique_tournament_seasons.add((unique_tournament_id, season_id))
        return season_id

    def ingest_season_info(self, payload: Mapping[str, Any], *, unique_tournament_id: int) -> None:
        season_id = self.ingest_season(_as_mapping(payload.get("season")), unique_tournament_id=unique_tournament_id)
        if season_id is None:
            return

        for field_name in ("newcomersLowerDivision", "newcomersUpperDivision", "newcomersOther"):
            for team_payload in _iter_mappings(payload.get(field_name)):
                self.ingest_team(team_payload)

    def ingest_image(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        image_id = _as_int(payload.get("id"))
        if image_id is None:
            return None
        self._merge(self.image_assets, image_id, {"id": image_id, "md5": _as_str(payload.get("md5"))})
        return image_id

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

    def ingest_category(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        category_id = _as_int(payload.get("id"))
        sport_id = self.ingest_sport(_as_mapping(payload.get("sport")))
        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
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

    def ingest_team(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        team_id = _as_int(payload.get("id"))
        if team_id is None:
            return None

        sport_id = self.ingest_sport(_as_mapping(payload.get("sport")))
        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
        self._merge(
            self.teams,
            team_id,
            {
                "id": team_id,
                "slug": _as_str(payload.get("slug")),
                "name": _as_str(payload.get("name")),
                "short_name": _as_str(payload.get("shortName")),
                "name_code": _as_str(payload.get("nameCode")),
                "sport_id": sport_id,
                "country_alpha2": country_alpha2,
                "gender": _as_str(payload.get("gender")),
                "type": _as_int(payload.get("type")),
                "national": _as_bool(payload.get("national")),
                "disabled": _as_bool(payload.get("disabled")),
                "user_count": _as_int(payload.get("userCount")),
                "field_translations": _as_mapping(payload.get("fieldTranslations")),
                "team_colors": _as_mapping(payload.get("teamColors")),
            },
        )
        return team_id

    def to_bundle(self) -> CompetitionBundle:
        return CompetitionBundle(
            registry_entries=competition_registry_entries(),
            payload_snapshots=tuple(self.payload_snapshots),
            image_assets=tuple(ImageAssetRecord(**row) for _, row in sorted(self.image_assets.items())),
            sports=tuple(SportRecord(**row) for _, row in sorted(self.sports.items())),
            countries=tuple(CountryRecord(**row) for _, row in sorted(self.countries.items())),
            categories=tuple(CategoryRecord(**row) for _, row in sorted(self.categories.items())),
            teams=tuple(TeamRecord(**row) for _, row in sorted(self.teams.items())),
            unique_tournaments=tuple(
                UniqueTournamentRecord(**row) for _, row in sorted(self.unique_tournaments.items())
            ),
            unique_tournament_relations=tuple(
                UniqueTournamentRelationRecord(
                    unique_tournament_id=unique_tournament_id,
                    related_unique_tournament_id=related_unique_tournament_id,
                    relation_type=relation_type,
                )
                for unique_tournament_id, related_unique_tournament_id, relation_type in sorted(
                    self.unique_tournament_relations
                )
            ),
            unique_tournament_most_title_teams=tuple(
                UniqueTournamentMostTitleTeamRecord(
                    unique_tournament_id=unique_tournament_id,
                    team_id=team_id,
                )
                for unique_tournament_id, team_id in sorted(self.unique_tournament_most_title_teams)
            ),
            seasons=tuple(SeasonRecord(**row) for _, row in sorted(self.seasons.items())),
            unique_tournament_seasons=tuple(
                UniqueTournamentSeasonRecord(unique_tournament_id=unique_tournament_id, season_id=season_id)
                for unique_tournament_id, season_id in sorted(self.unique_tournament_seasons)
            ),
            season_rounds=tuple(
                sorted(
                    self.season_rounds,
                    key=lambda row: (row.unique_tournament_id, row.season_id, row.round_number),
                )
            ),
        )

    @staticmethod
    def _merge(store: dict[Any, dict[str, Any]], key: Any, row: dict[str, Any]) -> None:
        current = dict(store.get(key, {}))
        for field_name, value in row.items():
            if value is not None or field_name not in current:
                current[field_name] = value
        store[key] = current


def _require_mapping(payload: object, envelope_key: str, source_url: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise CompetitionParserError(f"Expected object payload for {source_url}, got {type(payload).__name__}")
    envelope = payload.get(envelope_key)
    if not isinstance(envelope, Mapping):
        raise CompetitionParserError(f"Missing object envelope '{envelope_key}' for {source_url}")
    return envelope


def _full_response_payload(
    response: SofascoreResponse,
    *,
    fallback_key: str,
    fallback_value: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Return the full upstream JSON body for snapshot storage.

    The competition pipeline used to persist only the unwrapped envelope
    (``payload[envelope_key]``), which broke 1:1 contract with sofascore for
    ``/api/v1/unique-tournament/{id}`` and ``.../season/{id}/info``. This
    helper preserves the full ``{"<envelope_key>": {...}}`` shape and falls
    back to a synthetic wrapper if the upstream response was a non-mapping
    (defensive — should not happen in practice since ``_require_mapping``
    already validated the envelope).
    """

    payload = response.payload
    if isinstance(payload, Mapping):
        return payload
    return {fallback_key: fallback_value}


def _require_sequence(payload: object, envelope_key: str, source_url: str) -> tuple[object, ...]:
    if not isinstance(payload, Mapping):
        raise CompetitionParserError(f"Expected object payload for {source_url}, got {type(payload).__name__}")
    envelope = payload.get(envelope_key)
    if not isinstance(envelope, list):
        raise CompetitionParserError(f"Missing array envelope '{envelope_key}' for {source_url}")
    return tuple(envelope)


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _iter_mappings(value: object) -> Iterable[Mapping[str, Any]]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    return value if isinstance(value, int) else None


def _as_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None
