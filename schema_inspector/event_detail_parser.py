"""Async parser for Sofascore event-detail endpoints."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from .competition_parser import ApiPayloadSnapshotRecord, CategoryRecord, CountryRecord, SportRecord, UniqueTournamentRecord
from .endpoints import (
    EndpointRegistryEntry,
    EVENT_DETAIL_ENDPOINT,
    EVENT_H2H_ENDPOINT,
    EVENT_LINEUPS_ENDPOINT,
    EVENT_MANAGERS_ENDPOINT,
    EVENT_ODDS_ALL_ENDPOINT,
    EVENT_ODDS_FEATURED_ENDPOINT,
    EVENT_PREGAME_FORM_ENDPOINT,
    EVENT_VOTES_ENDPOINT,
    EVENT_WINNING_ODDS_ENDPOINT,
    event_detail_registry_entries,
)
from .event_list_parser import (
    EventChangeItemRecord,
    EventFilterValueRecord,
    EventRoundInfoRecord,
    EventScoreRecord,
    EventSeasonRecord,
    EventStatusRecord,
    EventStatusTimeRecord,
    EventTimeRecord,
    EventVarInProgressRecord,
)
from .sofascore_client import SofascoreClient, SofascoreHttpError, SofascoreResponse


@dataclass(frozen=True)
class EventDetailTournamentRecord:
    id: int
    slug: str | None
    name: str
    category_id: int
    unique_tournament_id: int | None = None
    competition_type: int | None = None
    group_name: str | None = None
    group_sign: str | None = None
    is_group: bool | None = None
    is_live: bool | None = None
    priority: int | None = None
    field_translations: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class EventDetailTeamRecord:
    id: int
    slug: str
    name: str
    short_name: str | None = None
    full_name: str | None = None
    name_code: str | None = None
    sport_id: int | None = None
    category_id: int | None = None
    country_alpha2: str | None = None
    manager_id: int | None = None
    venue_id: int | None = None
    tournament_id: int | None = None
    primary_unique_tournament_id: int | None = None
    parent_team_id: int | None = None
    gender: str | None = None
    type: int | None = None
    class_value: int | None = None
    ranking: int | None = None
    national: bool | None = None
    disabled: bool | None = None
    foundation_date_timestamp: int | None = None
    user_count: int | None = None
    team_colors: Mapping[str, Any] | None = None
    field_translations: Mapping[str, Any] | None = None
    time_active: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class VenueRecord:
    id: int
    slug: str | None
    name: str
    capacity: int | None = None
    hidden: bool | None = None
    country_alpha2: str | None = None
    city_name: str | None = None
    stadium_name: str | None = None
    stadium_capacity: int | None = None
    latitude: float | None = None
    longitude: float | None = None
    field_translations: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class RefereeRecord:
    id: int
    slug: str | None
    name: str
    sport_id: int | None = None
    country_alpha2: str | None = None
    games: int | None = None
    yellow_cards: int | None = None
    yellow_red_cards: int | None = None
    red_cards: int | None = None
    field_translations: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class ManagerRecord:
    id: int
    slug: str | None
    name: str
    short_name: str | None = None
    sport_id: int | None = None
    country_alpha2: str | None = None
    team_id: int | None = None
    former_player_id: int | None = None
    nationality: str | None = None
    nationality_iso2: str | None = None
    date_of_birth_timestamp: int | None = None
    deceased: bool | None = None
    preferred_formation: str | None = None
    field_translations: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class ManagerPerformanceRecord:
    manager_id: int
    total: int | None = None
    wins: int | None = None
    draws: int | None = None
    losses: int | None = None
    goals_scored: int | None = None
    goals_conceded: int | None = None
    total_points: int | None = None


@dataclass(frozen=True)
class ManagerTeamMembershipRecord:
    manager_id: int
    team_id: int


@dataclass(frozen=True)
class PlayerRecord:
    id: int
    slug: str | None
    name: str
    short_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    team_id: int | None = None
    country_alpha2: str | None = None
    manager_id: int | None = None
    gender: str | None = None
    position: str | None = None
    positions_detailed: tuple[str, ...] | None = None
    preferred_foot: str | None = None
    jersey_number: str | None = None
    sofascore_id: str | None = None
    date_of_birth: str | None = None
    date_of_birth_timestamp: int | None = None
    height: int | None = None
    weight: int | None = None
    market_value_currency: str | None = None
    proposed_market_value_raw: Mapping[str, Any] | None = None
    rating: str | None = None
    retired: bool | None = None
    deceased: bool | None = None
    user_count: int | None = None
    order_value: int | None = None
    field_translations: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class EventDetailEventRecord:
    id: int
    slug: str | None
    custom_id: str | None
    detail_id: int | None
    tournament_id: int | None
    unique_tournament_id: int | None
    season_id: int | None
    home_team_id: int | None
    away_team_id: int | None
    venue_id: int | None
    referee_id: int | None
    status_code: int | None
    season_statistics_type: str | None
    start_timestamp: int | None
    coverage: int | None
    winner_code: int | None
    aggregated_winner_code: int | None
    home_red_cards: int | None
    away_red_cards: int | None
    previous_leg_event_id: int | None
    cup_matches_in_round: int | None
    default_period_count: int | None
    default_period_length: int | None
    default_overtime_length: int | None
    last_period: str | None
    correct_ai_insight: bool | None
    correct_halftime_ai_insight: bool | None
    feed_locked: bool | None
    is_editor: bool | None
    show_toto_promo: bool | None
    crowdsourcing_enabled: bool | None
    crowdsourcing_data_display_enabled: bool | None
    final_result_only: bool | None
    has_event_player_statistics: bool | None
    has_event_player_heat_map: bool | None
    has_global_highlights: bool | None
    has_xg: bool | None


@dataclass(frozen=True)
class EventManagerAssignmentRecord:
    event_id: int
    side: str
    manager_id: int


@dataclass(frozen=True)
class EventDuelRecord:
    event_id: int
    duel_type: str
    home_wins: int
    away_wins: int
    draws: int


@dataclass(frozen=True)
class EventPregameFormRecord:
    event_id: int
    label: str | None = None


@dataclass(frozen=True)
class EventPregameFormSideRecord:
    event_id: int
    side: str
    avg_rating: str | None = None
    position: int | None = None
    value: str | None = None


@dataclass(frozen=True)
class EventPregameFormItemRecord:
    event_id: int
    side: str
    ordinal: int
    form_value: str


@dataclass(frozen=True)
class EventVoteOptionRecord:
    event_id: int
    vote_type: str
    option_name: str
    vote_count: int


@dataclass(frozen=True)
class ProviderRecord:
    id: int
    slug: str | None = None
    name: str | None = None
    country: str | None = None
    default_bet_slip_link: str | None = None
    colors: Mapping[str, Any] | None = None
    odds_from_provider_id: int | None = None
    live_odds_from_provider_id: int | None = None


@dataclass(frozen=True)
class EventMarketRecord:
    id: int
    event_id: int
    provider_id: int | None
    fid: int
    market_id: int
    source_id: int | None
    market_group: str
    market_name: str
    market_period: str
    structure_type: int
    choice_group: str | None
    is_live: bool
    suspended: bool


@dataclass(frozen=True)
class EventMarketChoiceRecord:
    source_id: int
    event_market_id: int
    name: str
    change_value: int
    fractional_value: str
    initial_fractional_value: str


@dataclass(frozen=True)
class EventWinningOddsRecord:
    event_id: int
    provider_id: int
    side: str
    odds_id: int | None = None
    actual: int | None = None
    expected: int | None = None
    fractional_value: str | None = None


@dataclass(frozen=True)
class EventLineupRecord:
    event_id: int
    side: str
    formation: str | None = None
    player_color: Mapping[str, Any] | None = None
    goalkeeper_color: Mapping[str, Any] | None = None
    support_staff: Sequence[Any] | None = None


@dataclass(frozen=True)
class EventLineupPlayerRecord:
    event_id: int
    side: str
    player_id: int
    team_id: int | None = None
    position: str | None = None
    substitute: bool | None = None
    shirt_number: int | None = None
    jersey_number: str | None = None
    avg_rating: float | None = None


@dataclass(frozen=True)
class EventLineupMissingPlayerRecord:
    event_id: int
    side: str
    player_id: int
    description: str | None = None
    expected_end_date: str | None = None
    external_type: int | None = None
    reason: int | None = None
    type: str | None = None


@dataclass(frozen=True)
class EventDetailBundle:
    registry_entries: tuple[EndpointRegistryEntry, ...]
    payload_snapshots: tuple[ApiPayloadSnapshotRecord, ...]
    sports: tuple[SportRecord, ...]
    countries: tuple[CountryRecord, ...]
    categories: tuple[CategoryRecord, ...]
    unique_tournaments: tuple[UniqueTournamentRecord, ...]
    seasons: tuple[EventSeasonRecord, ...]
    tournaments: tuple[EventDetailTournamentRecord, ...]
    teams: tuple[EventDetailTeamRecord, ...]
    venues: tuple[VenueRecord, ...]
    referees: tuple[RefereeRecord, ...]
    managers: tuple[ManagerRecord, ...]
    manager_performances: tuple[ManagerPerformanceRecord, ...]
    manager_team_memberships: tuple[ManagerTeamMembershipRecord, ...]
    players: tuple[PlayerRecord, ...]
    event_statuses: tuple[EventStatusRecord, ...]
    events: tuple[EventDetailEventRecord, ...]
    event_round_infos: tuple[EventRoundInfoRecord, ...]
    event_status_times: tuple[EventStatusTimeRecord, ...]
    event_times: tuple[EventTimeRecord, ...]
    event_var_in_progress_items: tuple[EventVarInProgressRecord, ...]
    event_scores: tuple[EventScoreRecord, ...]
    event_filter_values: tuple[EventFilterValueRecord, ...]
    event_change_items: tuple[EventChangeItemRecord, ...]
    event_manager_assignments: tuple[EventManagerAssignmentRecord, ...]
    event_duels: tuple[EventDuelRecord, ...]
    event_pregame_forms: tuple[EventPregameFormRecord, ...]
    event_pregame_form_sides: tuple[EventPregameFormSideRecord, ...]
    event_pregame_form_items: tuple[EventPregameFormItemRecord, ...]
    event_vote_options: tuple[EventVoteOptionRecord, ...]
    providers: tuple[ProviderRecord, ...]
    event_markets: tuple[EventMarketRecord, ...]
    event_market_choices: tuple[EventMarketChoiceRecord, ...]
    event_winning_odds: tuple[EventWinningOddsRecord, ...]
    event_lineups: tuple[EventLineupRecord, ...]
    event_lineup_players: tuple[EventLineupPlayerRecord, ...]
    event_lineup_missing_players: tuple[EventLineupMissingPlayerRecord, ...]


class EventDetailParserError(RuntimeError):
    """Raised when an event-detail payload misses its expected structure."""


class EventDetailParser:
    """Fetches and normalizes event-detail endpoints around one event_id."""

    def __init__(self, client: SofascoreClient, *, logger: logging.Logger | None = None) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    async def fetch_bundle(
        self,
        event_id: int,
        *,
        provider_ids: Iterable[int] = (1,),
        timeout: float = 20.0,
    ) -> EventDetailBundle:
        state = _EventDetailAccumulator()
        await self._fetch_event_root(event_id, state, timeout=timeout)

        tasks = [
            self._fetch_lineups(event_id, state, timeout=timeout),
            self._fetch_managers(event_id, state, timeout=timeout),
            self._fetch_h2h(event_id, state, timeout=timeout),
            self._fetch_pregame_form(event_id, state, timeout=timeout),
            self._fetch_votes(event_id, state, timeout=timeout),
        ]
        for provider_id in tuple(dict.fromkeys(provider_ids)):
            tasks.extend(
                (
                    self._fetch_odds_all(event_id, provider_id, state, timeout=timeout),
                    self._fetch_odds_featured(event_id, provider_id, state, timeout=timeout),
                    self._fetch_winning_odds(event_id, provider_id, state, timeout=timeout),
                )
            )

        await asyncio.gather(*tasks)
        self.logger.info(
            "Event detail bundle collected: event_id=%s players=%s markets=%s lineups=%s",
            event_id,
            len(state.players),
            len(state.event_markets),
            len(state.event_lineups),
        )
        return state.to_bundle()

    async def _fetch_event_root(
        self,
        event_id: int,
        state: "_EventDetailAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = EVENT_DETAIL_ENDPOINT
        url = endpoint.build_url(event_id=event_id)
        response = await self.client.get_json(url, timeout=timeout)
        payload = _require_root_mapping(response.payload, url)
        envelope = _require_mapping(payload.get("event"), endpoint.envelope_key, url)
        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="event",
            context_entity_id=event_id,
            payload=envelope,
        )
        state.ingest_event_root(envelope)

    async def _fetch_lineups(
        self,
        event_id: int,
        state: "_EventDetailAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = EVENT_LINEUPS_ENDPOINT
        payload = await self._fetch_optional_root_payload(
            endpoint,
            event_id=event_id,
            state=state,
            timeout=timeout,
        )
        if payload is not None:
            state.ingest_lineups(event_id, payload)

    async def _fetch_managers(
        self,
        event_id: int,
        state: "_EventDetailAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = EVENT_MANAGERS_ENDPOINT
        payload = await self._fetch_optional_root_payload(
            endpoint,
            event_id=event_id,
            state=state,
            timeout=timeout,
        )
        if payload is not None:
            state.ingest_managers_endpoint(event_id, payload)

    async def _fetch_h2h(
        self,
        event_id: int,
        state: "_EventDetailAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = EVENT_H2H_ENDPOINT
        payload = await self._fetch_optional_root_payload(
            endpoint,
            event_id=event_id,
            state=state,
            timeout=timeout,
        )
        if payload is not None:
            state.ingest_h2h(event_id, payload)

    async def _fetch_pregame_form(
        self,
        event_id: int,
        state: "_EventDetailAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = EVENT_PREGAME_FORM_ENDPOINT
        payload = await self._fetch_optional_root_payload(
            endpoint,
            event_id=event_id,
            state=state,
            timeout=timeout,
        )
        if payload is not None:
            state.ingest_pregame_form(event_id, payload)

    async def _fetch_votes(
        self,
        event_id: int,
        state: "_EventDetailAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = EVENT_VOTES_ENDPOINT
        payload = await self._fetch_optional_root_payload(
            endpoint,
            event_id=event_id,
            state=state,
            timeout=timeout,
        )
        if payload is not None:
            state.ingest_votes(event_id, payload)

    async def _fetch_odds_all(
        self,
        event_id: int,
        provider_id: int,
        state: "_EventDetailAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = EVENT_ODDS_ALL_ENDPOINT
        payload = await self._fetch_optional_root_payload(
            endpoint,
            event_id=event_id,
            provider_id=provider_id,
            state=state,
            timeout=timeout,
        )
        if payload is not None:
            state.ingest_provider(provider_id)
            state.ingest_odds_all(event_id, provider_id, payload)

    async def _fetch_odds_featured(
        self,
        event_id: int,
        provider_id: int,
        state: "_EventDetailAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = EVENT_ODDS_FEATURED_ENDPOINT
        payload = await self._fetch_optional_root_payload(
            endpoint,
            event_id=event_id,
            provider_id=provider_id,
            state=state,
            timeout=timeout,
        )
        if payload is not None:
            state.ingest_provider(provider_id)
            state.ingest_odds_featured(event_id, provider_id, payload)

    async def _fetch_winning_odds(
        self,
        event_id: int,
        provider_id: int,
        state: "_EventDetailAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = EVENT_WINNING_ODDS_ENDPOINT
        payload = await self._fetch_optional_root_payload(
            endpoint,
            event_id=event_id,
            provider_id=provider_id,
            state=state,
            timeout=timeout,
        )
        if payload is not None:
            state.ingest_provider(provider_id)
            state.ingest_winning_odds(event_id, provider_id, payload)

    async def _fetch_optional_root_payload(
        self,
        endpoint,
        *,
        event_id: int,
        state: "_EventDetailAccumulator",
        timeout: float,
        **path_params: object,
    ) -> Mapping[str, Any] | None:
        url = endpoint.build_url(event_id=event_id, **path_params)
        try:
            response = await self.client.get_json(url, timeout=timeout)
        except SofascoreHttpError as exc:
            status_code = exc.transport_result.status_code if exc.transport_result is not None else None
            if status_code == 404:
                self.logger.info("Event detail endpoint missing for event_id=%s url=%s", event_id, url)
                return None
            raise

        payload = _require_root_mapping(response.payload, url)
        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="event",
            context_entity_id=event_id,
            payload=payload,
        )
        return payload


class _EventDetailAccumulator:
    def __init__(self) -> None:
        self.payload_snapshots: list[ApiPayloadSnapshotRecord] = []
        self.sports: dict[int, dict[str, Any]] = {}
        self.countries: dict[str, dict[str, Any]] = {}
        self.categories: dict[int, dict[str, Any]] = {}
        self.unique_tournaments: dict[int, dict[str, Any]] = {}
        self.seasons: dict[int, dict[str, Any]] = {}
        self.tournaments: dict[int, dict[str, Any]] = {}
        self.teams: dict[int, dict[str, Any]] = {}
        self.venues: dict[int, dict[str, Any]] = {}
        self.referees: dict[int, dict[str, Any]] = {}
        self.managers: dict[int, dict[str, Any]] = {}
        self.manager_performances: dict[int, dict[str, Any]] = {}
        self.manager_team_memberships: set[tuple[int, int]] = set()
        self.players: dict[int, dict[str, Any]] = {}
        self.event_statuses: dict[int, dict[str, Any]] = {}
        self.events: dict[int, dict[str, Any]] = {}
        self.event_round_infos: dict[int, dict[str, Any]] = {}
        self.event_status_times: dict[int, dict[str, Any]] = {}
        self.event_times: dict[int, dict[str, Any]] = {}
        self.event_var_in_progress_items: dict[int, dict[str, Any]] = {}
        self.event_scores: dict[tuple[int, str], dict[str, Any]] = {}
        self.event_filter_values: dict[tuple[int, str, int], dict[str, Any]] = {}
        self.event_change_items: dict[tuple[int, int], dict[str, Any]] = {}
        self.event_manager_assignments: dict[tuple[int, str], dict[str, Any]] = {}
        self.event_duels: dict[tuple[int, str], dict[str, Any]] = {}
        self.event_pregame_forms: dict[int, dict[str, Any]] = {}
        self.event_pregame_form_sides: dict[tuple[int, str], dict[str, Any]] = {}
        self.event_pregame_form_items: dict[tuple[int, str, int], dict[str, Any]] = {}
        self.event_vote_options: dict[tuple[int, str, str], dict[str, Any]] = {}
        self.providers: dict[int, dict[str, Any]] = {}
        self.event_markets: dict[int, dict[str, Any]] = {}
        self.event_market_choices: dict[int, dict[str, Any]] = {}
        self.event_winning_odds: dict[tuple[int, int, str], dict[str, Any]] = {}
        self.event_lineups: dict[tuple[int, str], dict[str, Any]] = {}
        self.event_lineup_players: dict[tuple[int, str, int], dict[str, Any]] = {}
        self.event_lineup_missing_players: dict[tuple[int, str, int], dict[str, Any]] = {}

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

    def ingest_event_root(self, payload: Mapping[str, Any]) -> int | None:
        event_id = _as_int(payload.get("id"))
        if event_id is None:
            return None

        tournament_id = self.ingest_tournament(_as_mapping(payload.get("tournament")))
        season_id = self.ingest_season(_as_mapping(payload.get("season")))
        status_code = self.ingest_status(_as_mapping(payload.get("status")))
        venue_id = self.ingest_venue(_as_mapping(payload.get("venue")))
        referee_id = self.ingest_referee(_as_mapping(payload.get("referee")))
        home_team_id = self.ingest_team(_as_mapping(payload.get("homeTeam")), team_side="home", event_id=event_id)
        away_team_id = self.ingest_team(_as_mapping(payload.get("awayTeam")), team_side="away", event_id=event_id)

        self.ingest_round_info(event_id, _as_mapping(payload.get("roundInfo")))
        self.ingest_status_time(event_id, _as_mapping(payload.get("statusTime")))
        self.ingest_time(event_id, _as_mapping(payload.get("time")))
        self.ingest_var_in_progress(event_id, _as_mapping(payload.get("varInProgress")))
        self.ingest_score(event_id, "home", _as_mapping(payload.get("homeScore")))
        self.ingest_score(event_id, "away", _as_mapping(payload.get("awayScore")))
        self.ingest_event_filters(event_id, _as_mapping(payload.get("eventFilters")))
        self.ingest_changes(event_id, _as_mapping(payload.get("changes")))

        unique_tournament_id = None
        if tournament_id is not None and tournament_id in self.tournaments:
            unique_tournament_id = self.tournaments[tournament_id].get("unique_tournament_id")

        self._merge(
            self.events,
            event_id,
            {
                "id": event_id,
                "slug": _as_str(payload.get("slug")),
                "custom_id": _as_str(payload.get("customId")),
                "detail_id": _as_int(payload.get("detailId")),
                "tournament_id": tournament_id,
                "unique_tournament_id": unique_tournament_id,
                "season_id": season_id,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "venue_id": venue_id,
                "referee_id": referee_id,
                "status_code": status_code,
                "season_statistics_type": _as_str(payload.get("seasonStatisticsType")),
                "start_timestamp": _as_int(payload.get("startTimestamp")),
                "coverage": _as_int(payload.get("coverage")),
                "winner_code": _as_int(payload.get("winnerCode")),
                "aggregated_winner_code": _as_int(payload.get("aggregatedWinnerCode")),
                "home_red_cards": _as_int(payload.get("homeRedCards")),
                "away_red_cards": _as_int(payload.get("awayRedCards")),
                "previous_leg_event_id": _as_int(payload.get("previousLegEventId")),
                "cup_matches_in_round": _as_int(payload.get("cupMatchesInRound")),
                "default_period_count": _as_int(payload.get("defaultPeriodCount")),
                "default_period_length": _as_int(payload.get("defaultPeriodLength")),
                "default_overtime_length": _as_int(payload.get("defaultOvertimeLength")),
                "last_period": _as_str(payload.get("lastPeriod")),
                "correct_ai_insight": _as_bool(payload.get("correctAiInsight")),
                "correct_halftime_ai_insight": _as_bool(payload.get("correctHalftimeAiInsight")),
                "feed_locked": _as_bool(payload.get("feedLocked")),
                "is_editor": _as_bool(payload.get("isEditor")),
                "show_toto_promo": _as_bool(payload.get("showTotoPromo")),
                "crowdsourcing_enabled": _as_bool(payload.get("crowdsourcingEnabled")),
                "crowdsourcing_data_display_enabled": _as_bool(payload.get("crowdsourcingDataDisplayEnabled")),
                "final_result_only": _as_bool(payload.get("finalResultOnly")),
                "has_event_player_statistics": _as_bool(payload.get("hasEventPlayerStatistics")),
                "has_event_player_heat_map": _as_bool(payload.get("hasEventPlayerHeatMap")),
                "has_global_highlights": _as_bool(payload.get("hasGlobalHighlights")),
                "has_xg": _as_bool(payload.get("hasXg")),
            },
        )
        return event_id

    def ingest_tournament(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        tournament_id = _as_int(payload.get("id"))
        category_id = self.ingest_category(_as_mapping(payload.get("category")))
        unique_tournament_id = self.ingest_unique_tournament(_as_mapping(payload.get("uniqueTournament")))
        if tournament_id is None or category_id is None:
            return tournament_id

        self._merge(
            self.tournaments,
            tournament_id,
            {
                "id": tournament_id,
                "slug": _as_str(payload.get("slug")),
                "name": _as_str(payload.get("name")),
                "category_id": category_id,
                "unique_tournament_id": unique_tournament_id,
                "competition_type": _as_int(payload.get("competitionType")),
                "group_name": _as_str(payload.get("groupName")),
                "group_sign": _as_str(payload.get("groupSign")),
                "is_group": _as_bool(payload.get("isGroup")),
                "is_live": _as_bool(payload.get("isLive")),
                "priority": _as_int(payload.get("priority")),
                "field_translations": _as_mapping(payload.get("fieldTranslations")),
            },
        )
        return tournament_id

    def ingest_season(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
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
                "season_coverage_info": _as_mapping(payload.get("seasonCoverageInfo")),
            },
        )
        return season_id

    def ingest_status(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        code = _as_int(payload.get("code"))
        if code is None:
            return None
        self._merge(
            self.event_statuses,
            code,
            {"code": code, "description": _as_str(payload.get("description")), "type": _as_str(payload.get("type"))},
        )
        return code

    def ingest_team(
        self,
        payload: Mapping[str, Any] | None,
        *,
        team_side: str | None = None,
        event_id: int | None = None,
    ) -> int | None:
        if not payload:
            return None
        team_id = _as_int(payload.get("id"))
        if team_id is None:
            return None

        parent_team_id = self.ingest_team(_as_mapping(payload.get("parentTeam")))
        sport_id = self.ingest_sport(_as_mapping(payload.get("sport")))
        category_id = self.ingest_category(_as_mapping(payload.get("category")))
        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
        venue_id = self.ingest_venue(_as_mapping(payload.get("venue")))
        primary_unique_tournament_id = self.ingest_unique_tournament(_as_mapping(payload.get("primaryUniqueTournament")))
        tournament_id = self.ingest_tournament(_as_mapping(payload.get("tournament")))

        manager_id = self.ingest_manager(_as_mapping(payload.get("manager")), team_id=team_id)
        if manager_id is not None:
            self.manager_team_memberships.add((manager_id, team_id))
            if event_id is not None and team_side is not None:
                self.event_manager_assignments[(event_id, team_side)] = {
                    "event_id": event_id,
                    "side": team_side,
                    "manager_id": manager_id,
                }

        self._merge(
            self.teams,
            team_id,
            {
                "id": team_id,
                "slug": _as_str(payload.get("slug")),
                "name": _as_str(payload.get("name")),
                "short_name": _as_str(payload.get("shortName")),
                "full_name": _as_str(payload.get("fullName")),
                "name_code": _as_str(payload.get("nameCode")),
                "sport_id": sport_id,
                "category_id": category_id,
                "country_alpha2": country_alpha2,
                "manager_id": manager_id,
                "venue_id": venue_id,
                "tournament_id": tournament_id,
                "primary_unique_tournament_id": primary_unique_tournament_id,
                "parent_team_id": parent_team_id,
                "gender": _as_str(payload.get("gender")),
                "type": _as_int(payload.get("type")),
                "class_value": _as_int(payload.get("class")),
                "ranking": _as_int(payload.get("ranking")),
                "national": _as_bool(payload.get("national")),
                "disabled": _as_bool(payload.get("disabled")),
                "foundation_date_timestamp": _as_int(payload.get("foundationDateTimestamp")),
                "user_count": _as_int(payload.get("userCount")),
                "team_colors": _as_mapping(payload.get("teamColors")),
                "field_translations": _as_mapping(payload.get("fieldTranslations")),
                "time_active": _as_mapping(payload.get("timeActive")),
            },
        )
        return team_id

    def ingest_venue(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        venue_id = _as_int(payload.get("id"))
        if venue_id is None:
            return None
        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
        self._merge(
            self.venues,
            venue_id,
            {
                "id": venue_id,
                "slug": _as_str(payload.get("slug")),
                "name": _as_str(payload.get("name")),
                "capacity": _as_int(payload.get("capacity")),
                "hidden": _as_bool(payload.get("hidden")),
                "country_alpha2": country_alpha2,
                "city_name": _as_str(payload.get("cityName")),
                "stadium_name": _as_str(payload.get("stadiumName")),
                "stadium_capacity": _as_int(payload.get("stadiumCapacity")),
                "latitude": _as_float(payload.get("latitude")),
                "longitude": _as_float(payload.get("longitude")),
                "field_translations": _as_mapping(payload.get("fieldTranslations")),
            },
        )
        return venue_id

    def ingest_referee(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        referee_id = _as_int(payload.get("id"))
        if referee_id is None:
            return None
        sport_id = self.ingest_sport(_as_mapping(payload.get("sport")))
        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
        self._merge(
            self.referees,
            referee_id,
            {
                "id": referee_id,
                "slug": _as_str(payload.get("slug")),
                "name": _as_str(payload.get("name")),
                "sport_id": sport_id,
                "country_alpha2": country_alpha2,
                "games": _as_int(payload.get("games")),
                "yellow_cards": _as_int(payload.get("yellowCards")),
                "yellow_red_cards": _as_int(payload.get("yellowRedCards")),
                "red_cards": _as_int(payload.get("redCards")),
                "field_translations": _as_mapping(payload.get("fieldTranslations")),
            },
        )
        return referee_id

    def ingest_manager(self, payload: Mapping[str, Any] | None, *, team_id: int | None = None) -> int | None:
        if not payload:
            return None
        manager_id = _as_int(payload.get("id"))
        if manager_id is None:
            return None
        sport_id = self.ingest_sport(_as_mapping(payload.get("sport")))
        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
        nationality_iso2 = _as_str(payload.get("nationalityIso2"))
        if nationality_iso2 and nationality_iso2 not in self.countries:
            nationality_iso2 = None
        self._merge(
            self.managers,
            manager_id,
            {
                "id": manager_id,
                "slug": _as_str(payload.get("slug")),
                "name": _as_str(payload.get("name")),
                "short_name": _as_str(payload.get("shortName")),
                "sport_id": sport_id,
                "country_alpha2": country_alpha2,
                "team_id": team_id,
                "former_player_id": _as_int(payload.get("formerPlayerId")),
                "nationality": _as_str(payload.get("nationality")),
                "nationality_iso2": nationality_iso2,
                "date_of_birth_timestamp": _as_int(payload.get("dateOfBirthTimestamp")),
                "deceased": _as_bool(payload.get("deceased")),
                "preferred_formation": _as_str(payload.get("preferredFormation")),
                "field_translations": _as_mapping(payload.get("fieldTranslations")),
            },
        )
        self.ingest_manager_performance(manager_id, _as_mapping(payload.get("performance")))
        return manager_id

    def ingest_manager_performance(self, manager_id: int, payload: Mapping[str, Any] | None) -> None:
        if not payload:
            return
        self._merge(
            self.manager_performances,
            manager_id,
            {
                "manager_id": manager_id,
                "total": _as_int(payload.get("total")),
                "wins": _as_int(payload.get("wins")),
                "draws": _as_int(payload.get("draws")),
                "losses": _as_int(payload.get("losses")),
                "goals_scored": _as_int(payload.get("goalsScored")),
                "goals_conceded": _as_int(payload.get("goalsConceded")),
                "total_points": _as_int(payload.get("totalPoints")),
            },
        )

    def ingest_player(self, payload: Mapping[str, Any] | None, *, team_id: int | None = None) -> int | None:
        if not payload:
            return None
        player_id = _as_int(payload.get("id"))
        if player_id is None:
            return None
        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
        manager_id = self.ingest_manager(_as_mapping(payload.get("manager")))
        positions = _as_string_sequence(payload.get("positions"))
        self._merge(
            self.players,
            player_id,
            {
                "id": player_id,
                "slug": _as_str(payload.get("slug")),
                "name": _as_str(payload.get("name")),
                "short_name": _as_str(payload.get("shortName")),
                "first_name": _as_str(payload.get("firstName")),
                "last_name": _as_str(payload.get("lastName")),
                "team_id": team_id,
                "country_alpha2": country_alpha2,
                "manager_id": manager_id,
                "gender": _as_str(payload.get("gender")),
                "position": _as_str(payload.get("position")),
                "positions_detailed": positions,
                "preferred_foot": _as_str(payload.get("preferredFoot")),
                "jersey_number": _as_str(payload.get("jerseyNumber")),
                "sofascore_id": _as_str(payload.get("sofascoreId")),
                "date_of_birth": _as_str(payload.get("dateOfBirth")),
                "date_of_birth_timestamp": _as_int(payload.get("dateOfBirthTimestamp")),
                "height": _as_int(payload.get("height")),
                "weight": _as_int(payload.get("weight")),
                "market_value_currency": _as_str(payload.get("marketValueCurrency")),
                "proposed_market_value_raw": _as_mapping(payload.get("proposedMarketValueRaw")),
                "rating": _as_scalar_text(payload.get("rating")),
                "retired": _as_bool(payload.get("retired")),
                "deceased": _as_bool(payload.get("deceased")),
                "user_count": _as_int(payload.get("userCount")),
                "order_value": _as_int(payload.get("order")),
                "field_translations": _as_mapping(payload.get("fieldTranslations")),
            },
        )
        return player_id

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

    def ingest_unique_tournament(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        unique_tournament_id = _as_int(payload.get("id"))
        category_id = self.ingest_category(_as_mapping(payload.get("category")))
        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
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
                "country_alpha2": country_alpha2,
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

    def ingest_round_info(self, event_id: int, payload: Mapping[str, Any] | None) -> None:
        if not payload:
            return
        row = {
            "event_id": event_id,
            "round_number": _as_int(payload.get("round")),
            "slug": _as_str(payload.get("slug")),
            "name": _as_str(payload.get("name")),
            "cup_round_type": _as_int(payload.get("cupRoundType")),
        }
        if any(value is not None for key, value in row.items() if key != "event_id"):
            self._merge(self.event_round_infos, event_id, row)

    def ingest_status_time(self, event_id: int, payload: Mapping[str, Any] | None) -> None:
        if not payload:
            return
        row = {
            "event_id": event_id,
            "prefix": _as_str(payload.get("prefix")),
            "timestamp": _as_int(payload.get("timestamp")),
            "initial": _as_int(payload.get("initial")),
            "max": _as_int(payload.get("max")),
            "extra": _as_int(payload.get("extra")),
        }
        if any(value is not None for key, value in row.items() if key != "event_id"):
            self._merge(self.event_status_times, event_id, row)

    def ingest_time(self, event_id: int, payload: Mapping[str, Any] | None) -> None:
        if not payload:
            return
        row = {
            "event_id": event_id,
            "current_period_start_timestamp": _as_int(payload.get("currentPeriodStartTimestamp")),
            "initial": _as_int(payload.get("initial")),
            "max": _as_int(payload.get("max")),
            "extra": _as_int(payload.get("extra")),
            "injury_time1": _as_int(payload.get("injuryTime1")),
            "injury_time2": _as_int(payload.get("injuryTime2")),
            "injury_time3": _as_int(payload.get("injuryTime3")),
            "injury_time4": _as_int(payload.get("injuryTime4")),
            "overtime_length": _as_int(payload.get("overtimeLength")),
            "period_length": _as_int(payload.get("periodLength")),
            "total_period_count": _as_int(payload.get("totalPeriodCount")),
        }
        if any(value is not None for key, value in row.items() if key != "event_id"):
            self._merge(self.event_times, event_id, row)

    def ingest_var_in_progress(self, event_id: int, payload: Mapping[str, Any] | None) -> None:
        if not payload:
            return
        row = {
            "event_id": event_id,
            "home_team": _as_bool(payload.get("homeTeam")),
            "away_team": _as_bool(payload.get("awayTeam")),
        }
        if any(value is not None for key, value in row.items() if key != "event_id"):
            self._merge(self.event_var_in_progress_items, event_id, row)

    def ingest_score(self, event_id: int, side: str, payload: Mapping[str, Any] | None) -> None:
        if not payload:
            return
        row = {
            "event_id": event_id,
            "side": side,
            "current": _as_int(payload.get("current")),
            "display": _as_int(payload.get("display")),
            "aggregated": _as_int(payload.get("aggregated")),
            "normaltime": _as_int(payload.get("normaltime")),
            "overtime": _as_int(payload.get("overtime")),
            "penalties": _as_int(payload.get("penalties")),
            "period1": _as_int(payload.get("period1")),
            "period2": _as_int(payload.get("period2")),
            "period3": _as_int(payload.get("period3")),
            "period4": _as_int(payload.get("period4")),
            "extra1": _as_int(payload.get("extra1")),
            "extra2": _as_int(payload.get("extra2")),
            "series": _as_int(payload.get("series")),
        }
        if any(value is not None for key, value in row.items() if key not in {"event_id", "side"}):
            self._merge(self.event_scores, (event_id, side), row)

    def ingest_event_filters(self, event_id: int, payload: Mapping[str, Any] | None) -> None:
        if not payload:
            return
        for filter_name, values in payload.items():
            if not isinstance(values, list):
                continue
            for ordinal, item in enumerate(values):
                text_value = _as_scalar_text(item)
                if text_value is None:
                    continue
                self.event_filter_values[(event_id, filter_name, ordinal)] = {
                    "event_id": event_id,
                    "filter_name": filter_name,
                    "ordinal": ordinal,
                    "filter_value": text_value,
                }

    def ingest_changes(self, event_id: int, payload: Mapping[str, Any] | None) -> None:
        if not payload:
            return
        change_timestamp = _as_int(payload.get("changeTimestamp"))
        values = payload.get("changes")
        if not isinstance(values, list):
            return
        for ordinal, item in enumerate(values):
            text_value = _as_scalar_text(item)
            if text_value is None:
                continue
            self.event_change_items[(event_id, ordinal)] = {
                "event_id": event_id,
                "change_timestamp": change_timestamp,
                "ordinal": ordinal,
                "change_value": text_value,
            }

    def ingest_lineups(self, event_id: int, payload: Mapping[str, Any]) -> None:
        for side, key in (("home", "home"), ("away", "away")):
            lineup = _as_mapping(payload.get(key))
            if not lineup:
                continue
            self._merge(
                self.event_lineups,
                (event_id, side),
                {
                    "event_id": event_id,
                    "side": side,
                    "formation": _as_str(lineup.get("formation")),
                    "player_color": _as_mapping(lineup.get("playerColor")),
                    "goalkeeper_color": _as_mapping(lineup.get("goalkeeperColor")),
                    "support_staff": _as_sequence(lineup.get("supportStaff")),
                },
            )
            for item in _iter_mappings(lineup.get("players")):
                player_id = self.ingest_player(_as_mapping(item.get("player")), team_id=_as_int(item.get("teamId")))
                if player_id is None:
                    continue
                self._merge(
                    self.event_lineup_players,
                    (event_id, side, player_id),
                    {
                        "event_id": event_id,
                        "side": side,
                        "player_id": player_id,
                        "team_id": _as_int(item.get("teamId")),
                        "position": _as_str(item.get("position")),
                        "substitute": _as_bool(item.get("substitute")),
                        "shirt_number": _as_int(item.get("shirtNumber")),
                        "jersey_number": _as_str(item.get("jerseyNumber")),
                        "avg_rating": _as_float(item.get("avgRating")),
                    },
                )
            for item in _iter_mappings(lineup.get("missingPlayers")):
                player_id = self.ingest_player(_as_mapping(item.get("player")))
                if player_id is None:
                    continue
                self._merge(
                    self.event_lineup_missing_players,
                    (event_id, side, player_id),
                    {
                        "event_id": event_id,
                        "side": side,
                        "player_id": player_id,
                        "description": _as_str(item.get("description")),
                        "expected_end_date": _as_str(item.get("expectedEndDate")),
                        "external_type": _as_int(item.get("externalType")),
                        "reason": _as_int(item.get("reason")),
                        "type": _as_str(item.get("type")),
                    },
                )

    def ingest_managers_endpoint(self, event_id: int, payload: Mapping[str, Any]) -> None:
        for side, key in (("home", "homeManager"), ("away", "awayManager")):
            manager_id = self.ingest_manager(_as_mapping(payload.get(key)))
            if manager_id is None:
                continue
            self.event_manager_assignments[(event_id, side)] = {
                "event_id": event_id,
                "side": side,
                "manager_id": manager_id,
            }

    def ingest_h2h(self, event_id: int, payload: Mapping[str, Any]) -> None:
        for duel_type, key in (("team", "teamDuel"), ("manager", "managerDuel")):
            duel = _as_mapping(payload.get(key))
            if not duel:
                continue
            home_wins = _as_int(duel.get("homeWins"))
            away_wins = _as_int(duel.get("awayWins"))
            draws = _as_int(duel.get("draws"))
            if home_wins is None or away_wins is None or draws is None:
                continue
            self.event_duels[(event_id, duel_type)] = {
                "event_id": event_id,
                "duel_type": duel_type,
                "home_wins": home_wins,
                "away_wins": away_wins,
                "draws": draws,
            }

    def ingest_pregame_form(self, event_id: int, payload: Mapping[str, Any]) -> None:
        self.event_pregame_forms[event_id] = {"event_id": event_id, "label": _as_str(payload.get("label"))}
        for side, key in (("home", "homeTeam"), ("away", "awayTeam")):
            item = _as_mapping(payload.get(key))
            if not item:
                continue
            self.event_pregame_form_sides[(event_id, side)] = {
                "event_id": event_id,
                "side": side,
                "avg_rating": _as_scalar_text(item.get("avgRating")),
                "position": _as_int(item.get("position")),
                "value": _as_scalar_text(item.get("value")),
            }
            for ordinal, form_item in enumerate(_as_sequence(item.get("form"))):
                text_value = _as_scalar_text(form_item)
                if text_value is None:
                    continue
                self.event_pregame_form_items[(event_id, side, ordinal)] = {
                    "event_id": event_id,
                    "side": side,
                    "ordinal": ordinal,
                    "form_value": text_value,
                }

    def ingest_votes(self, event_id: int, payload: Mapping[str, Any]) -> None:
        for vote_type, vote_payload in payload.items():
            vote_mapping = _as_mapping(vote_payload)
            if not vote_mapping:
                continue
            for option_name, vote_count in vote_mapping.items():
                count = _as_int(vote_count)
                if count is None:
                    continue
                self.event_vote_options[(event_id, vote_type, option_name)] = {
                    "event_id": event_id,
                    "vote_type": vote_type,
                    "option_name": option_name,
                    "vote_count": count,
                }

    def ingest_provider(self, provider_id: int) -> None:
        self._merge(self.providers, provider_id, {"id": provider_id})

    def ingest_odds_all(self, event_id: int, provider_id: int, payload: Mapping[str, Any]) -> None:
        for market_payload in _iter_mappings(payload.get("markets")):
            self.ingest_market(event_id, provider_id, market_payload)

    def ingest_odds_featured(self, event_id: int, provider_id: int, payload: Mapping[str, Any]) -> None:
        featured = _as_mapping(payload.get("featured"))
        if not featured:
            return
        for market_payload in featured.values():
            if isinstance(market_payload, Mapping):
                self.ingest_market(event_id, provider_id, market_payload)

    def ingest_market(self, event_id: int, provider_id: int, payload: Mapping[str, Any]) -> int | None:
        market_id = _as_int(payload.get("id"))
        fid = _as_int(payload.get("fid"))
        source_id = _as_int(payload.get("sourceId"))
        market_key = _as_int(payload.get("marketId"))
        market_group = _as_str(payload.get("marketGroup"))
        market_name = _as_str(payload.get("marketName"))
        market_period = _as_str(payload.get("marketPeriod"))
        structure_type = _as_int(payload.get("structureType"))
        is_live = _as_bool(payload.get("isLive"))
        suspended = _as_bool(payload.get("suspended"))
        if (
            market_id is None
            or fid is None
            or market_key is None
            or market_group is None
            or market_name is None
            or market_period is None
            or structure_type is None
            or is_live is None
            or suspended is None
        ):
            return None

        self._merge(
            self.event_markets,
            market_id,
            {
                "id": market_id,
                "event_id": event_id,
                "provider_id": provider_id,
                "fid": fid,
                "market_id": market_key,
                "source_id": source_id,
                "market_group": market_group,
                "market_name": market_name,
                "market_period": market_period,
                "structure_type": structure_type,
                "choice_group": _as_str(payload.get("choiceGroup")),
                "is_live": is_live,
                "suspended": suspended,
            },
        )

        for choice_payload in _iter_mappings(payload.get("choices")):
            choice_source_id = _as_int(choice_payload.get("sourceId"))
            name = _as_str(choice_payload.get("name"))
            change_value = _as_int(choice_payload.get("change"))
            fractional_value = _as_scalar_text(choice_payload.get("fractionalValue"))
            initial_fractional_value = _as_scalar_text(choice_payload.get("initialFractionalValue"))
            if (
                choice_source_id is None
                or name is None
                or change_value is None
                or fractional_value is None
                or initial_fractional_value is None
            ):
                continue
            self._merge(
                self.event_market_choices,
                choice_source_id,
                {
                    "source_id": choice_source_id,
                    "event_market_id": market_id,
                    "name": name,
                    "change_value": change_value,
                    "fractional_value": fractional_value,
                    "initial_fractional_value": initial_fractional_value,
                },
            )
        return market_id

    def ingest_winning_odds(self, event_id: int, provider_id: int, payload: Mapping[str, Any]) -> None:
        for side, key in (("home", "home"), ("away", "away")):
            item = _as_mapping(payload.get(key))
            if not item:
                continue
            self.event_winning_odds[(event_id, provider_id, side)] = {
                "event_id": event_id,
                "provider_id": provider_id,
                "side": side,
                "odds_id": _as_int(item.get("id")),
                "actual": _as_int(item.get("actual")),
                "expected": _as_int(item.get("expected")),
                "fractional_value": _as_scalar_text(item.get("fractionalValue")),
            }

    def to_bundle(self) -> EventDetailBundle:
        return EventDetailBundle(
            registry_entries=event_detail_registry_entries(),
            payload_snapshots=tuple(self.payload_snapshots),
            sports=tuple(SportRecord(**row) for _, row in sorted(self.sports.items())),
            countries=tuple(CountryRecord(**row) for _, row in sorted(self.countries.items())),
            categories=tuple(CategoryRecord(**row) for _, row in sorted(self.categories.items())),
            unique_tournaments=tuple(
                UniqueTournamentRecord(**row) for _, row in sorted(self.unique_tournaments.items())
            ),
            seasons=tuple(EventSeasonRecord(**row) for _, row in sorted(self.seasons.items())),
            tournaments=tuple(EventDetailTournamentRecord(**row) for _, row in sorted(self.tournaments.items())),
            teams=tuple(EventDetailTeamRecord(**row) for _, row in sorted(self.teams.items())),
            venues=tuple(VenueRecord(**row) for _, row in sorted(self.venues.items())),
            referees=tuple(RefereeRecord(**row) for _, row in sorted(self.referees.items())),
            managers=tuple(ManagerRecord(**row) for _, row in sorted(self.managers.items())),
            manager_performances=tuple(
                ManagerPerformanceRecord(**row) for _, row in sorted(self.manager_performances.items())
            ),
            manager_team_memberships=tuple(
                ManagerTeamMembershipRecord(manager_id=manager_id, team_id=team_id)
                for manager_id, team_id in sorted(self.manager_team_memberships)
            ),
            players=tuple(PlayerRecord(**row) for _, row in sorted(self.players.items())),
            event_statuses=tuple(EventStatusRecord(**row) for _, row in sorted(self.event_statuses.items())),
            events=tuple(EventDetailEventRecord(**row) for _, row in sorted(self.events.items())),
            event_round_infos=tuple(EventRoundInfoRecord(**row) for _, row in sorted(self.event_round_infos.items())),
            event_status_times=tuple(
                EventStatusTimeRecord(**row) for _, row in sorted(self.event_status_times.items())
            ),
            event_times=tuple(EventTimeRecord(**row) for _, row in sorted(self.event_times.items())),
            event_var_in_progress_items=tuple(
                EventVarInProgressRecord(**row) for _, row in sorted(self.event_var_in_progress_items.items())
            ),
            event_scores=tuple(EventScoreRecord(**row) for _, row in sorted(self.event_scores.items())),
            event_filter_values=tuple(
                EventFilterValueRecord(**row) for _, row in sorted(self.event_filter_values.items())
            ),
            event_change_items=tuple(
                EventChangeItemRecord(**row) for _, row in sorted(self.event_change_items.items())
            ),
            event_manager_assignments=tuple(
                EventManagerAssignmentRecord(**row) for _, row in sorted(self.event_manager_assignments.items())
            ),
            event_duels=tuple(EventDuelRecord(**row) for _, row in sorted(self.event_duels.items())),
            event_pregame_forms=tuple(
                EventPregameFormRecord(**row) for _, row in sorted(self.event_pregame_forms.items())
            ),
            event_pregame_form_sides=tuple(
                EventPregameFormSideRecord(**row) for _, row in sorted(self.event_pregame_form_sides.items())
            ),
            event_pregame_form_items=tuple(
                EventPregameFormItemRecord(**row) for _, row in sorted(self.event_pregame_form_items.items())
            ),
            event_vote_options=tuple(
                EventVoteOptionRecord(**row) for _, row in sorted(self.event_vote_options.items())
            ),
            providers=tuple(ProviderRecord(**row) for _, row in sorted(self.providers.items())),
            event_markets=tuple(EventMarketRecord(**row) for _, row in sorted(self.event_markets.items())),
            event_market_choices=tuple(
                EventMarketChoiceRecord(**row) for _, row in sorted(self.event_market_choices.items())
            ),
            event_winning_odds=tuple(
                EventWinningOddsRecord(**row) for _, row in sorted(self.event_winning_odds.items())
            ),
            event_lineups=tuple(EventLineupRecord(**row) for _, row in sorted(self.event_lineups.items())),
            event_lineup_players=tuple(
                EventLineupPlayerRecord(**row) for _, row in sorted(self.event_lineup_players.items())
            ),
            event_lineup_missing_players=tuple(
                EventLineupMissingPlayerRecord(**row)
                for _, row in sorted(self.event_lineup_missing_players.items())
            ),
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
        raise EventDetailParserError(f"Expected object payload for {source_url}, got {type(payload).__name__}")
    return payload


def _require_mapping(payload: object, envelope_key: str, source_url: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise EventDetailParserError(f"Missing object envelope '{envelope_key}' for {source_url}")
    return payload


def _iter_mappings(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_sequence(value: object) -> tuple[Any, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(value)


def _as_string_sequence(value: object) -> tuple[str, ...] | None:
    if not isinstance(value, list):
        return None
    result = tuple(item for item in value if isinstance(item, str))
    return result or None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    return value if isinstance(value, int) else None


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _as_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _as_scalar_text(value: object) -> str | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (str, int, float)):
        return str(value)
    return None
