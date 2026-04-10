"""Async parser for Sofascore event-list endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping

from .competition_parser import ApiPayloadSnapshotRecord, CategoryRecord, CountryRecord, SportRecord, UniqueTournamentRecord
from .endpoints import (
    EndpointRegistryEntry,
    SPORT_FOOTBALL_LIVE_EVENTS_ENDPOINT,
    SPORT_FOOTBALL_SCHEDULED_EVENTS_ENDPOINT,
    UNIQUE_TOURNAMENT_FEATURED_EVENTS_ENDPOINT,
    UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT,
    event_list_registry_entries,
)
from .sofascore_client import SofascoreClient, SofascoreResponse


@dataclass(frozen=True)
class EventTeamRecord:
    id: int
    slug: str
    name: str
    short_name: str | None = None
    name_code: str | None = None
    sport_id: int | None = None
    country_alpha2: str | None = None
    parent_team_id: int | None = None
    gender: str | None = None
    type: int | None = None
    national: bool | None = None
    disabled: bool | None = None
    user_count: int | None = None
    field_translations: Mapping[str, Any] | None = None
    team_colors: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class EventSeasonRecord:
    id: int
    name: str | None = None
    year: str | None = None
    editor: bool | None = None
    season_coverage_info: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class TournamentRecord:
    id: int
    slug: str
    name: str
    category_id: int
    unique_tournament_id: int | None = None
    group_name: str | None = None
    group_sign: str | None = None
    is_group: bool | None = None
    is_live: bool | None = None
    priority: int | None = None
    field_translations: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class EventStatusRecord:
    code: int
    description: str
    type: str


@dataclass(frozen=True)
class EventRecord:
    id: int
    slug: str
    custom_id: str | None
    detail_id: int | None
    tournament_id: int | None
    unique_tournament_id: int | None
    season_id: int | None
    home_team_id: int | None
    away_team_id: int | None
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
class EventRoundInfoRecord:
    event_id: int
    round_number: int | None = None
    slug: str | None = None
    name: str | None = None
    cup_round_type: int | None = None


@dataclass(frozen=True)
class EventStatusTimeRecord:
    event_id: int
    prefix: str | None = None
    timestamp: int | None = None
    initial: int | None = None
    max: int | None = None
    extra: int | None = None


@dataclass(frozen=True)
class EventTimeRecord:
    event_id: int
    current_period_start_timestamp: int | None = None
    initial: int | None = None
    max: int | None = None
    extra: int | None = None
    injury_time1: int | None = None
    injury_time2: int | None = None
    injury_time3: int | None = None
    injury_time4: int | None = None
    overtime_length: int | None = None
    period_length: int | None = None
    total_period_count: int | None = None


@dataclass(frozen=True)
class EventVarInProgressRecord:
    event_id: int
    home_team: bool | None = None
    away_team: bool | None = None


@dataclass(frozen=True)
class EventScoreRecord:
    event_id: int
    side: str
    current: int | None = None
    display: int | None = None
    aggregated: int | None = None
    normaltime: int | None = None
    overtime: int | None = None
    penalties: int | None = None
    period1: int | None = None
    period2: int | None = None
    period3: int | None = None
    period4: int | None = None
    extra1: int | None = None
    extra2: int | None = None
    series: int | None = None


@dataclass(frozen=True)
class EventFilterValueRecord:
    event_id: int
    filter_name: str
    ordinal: int
    filter_value: str


@dataclass(frozen=True)
class EventChangeItemRecord:
    event_id: int
    change_timestamp: int | None
    ordinal: int
    change_value: str


@dataclass(frozen=True)
class EventListBundle:
    registry_entries: tuple[EndpointRegistryEntry, ...]
    payload_snapshots: tuple[ApiPayloadSnapshotRecord, ...]
    sports: tuple[SportRecord, ...]
    countries: tuple[CountryRecord, ...]
    categories: tuple[CategoryRecord, ...]
    teams: tuple[EventTeamRecord, ...]
    unique_tournaments: tuple[UniqueTournamentRecord, ...]
    seasons: tuple[EventSeasonRecord, ...]
    tournaments: tuple[TournamentRecord, ...]
    event_statuses: tuple[EventStatusRecord, ...]
    events: tuple[EventRecord, ...]
    event_round_infos: tuple[EventRoundInfoRecord, ...]
    event_status_times: tuple[EventStatusTimeRecord, ...]
    event_times: tuple[EventTimeRecord, ...]
    event_var_in_progress_items: tuple[EventVarInProgressRecord, ...]
    event_scores: tuple[EventScoreRecord, ...]
    event_filter_values: tuple[EventFilterValueRecord, ...]
    event_change_items: tuple[EventChangeItemRecord, ...]


class EventListParserError(RuntimeError):
    """Raised when an event-list endpoint payload misses its expected envelope."""


class EventListParser:
    """Fetches and normalizes Sofascore list-style event endpoints."""

    def __init__(self, client: SofascoreClient, *, logger: logging.Logger | None = None) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    async def fetch_scheduled_events(self, date: str, *, timeout: float = 20.0) -> EventListBundle:
        return await self._fetch_event_collection(
            SPORT_FOOTBALL_SCHEDULED_EVENTS_ENDPOINT,
            timeout=timeout,
            context_entity_type=None,
            context_entity_id=None,
            date=date,
        )

    async def fetch_live_events(self, *, timeout: float = 20.0) -> EventListBundle:
        return await self._fetch_event_collection(
            SPORT_FOOTBALL_LIVE_EVENTS_ENDPOINT,
            timeout=timeout,
            context_entity_type=None,
            context_entity_id=None,
        )

    async def fetch_featured_events(self, unique_tournament_id: int, *, timeout: float = 20.0) -> EventListBundle:
        return await self._fetch_event_collection(
            UNIQUE_TOURNAMENT_FEATURED_EVENTS_ENDPOINT,
            timeout=timeout,
            context_entity_type="unique_tournament",
            context_entity_id=unique_tournament_id,
            unique_tournament_id=unique_tournament_id,
        )

    async def fetch_round_events(
        self,
        unique_tournament_id: int,
        season_id: int,
        round_number: int,
        *,
        timeout: float = 20.0,
    ) -> EventListBundle:
        return await self._fetch_event_collection(
            UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT,
            timeout=timeout,
            context_entity_type="season",
            context_entity_id=season_id,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            round_number=round_number,
        )

    async def _fetch_event_collection(
        self,
        endpoint,
        *,
        timeout: float,
        context_entity_type: str | None,
        context_entity_id: int | None,
        **path_params: object,
    ) -> EventListBundle:
        url = endpoint.build_url(**path_params)
        response = await self.client.get_json(url, timeout=timeout)
        payload = _require_root_mapping(response.payload, url)
        events = _require_event_array(payload, endpoint.envelope_key, url)

        state = _EventListAccumulator()
        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type=context_entity_type,
            context_entity_id=context_entity_id,
            payload=payload,
        )
        for event_payload in events:
            if isinstance(event_payload, Mapping):
                state.ingest_event(event_payload)

        self.logger.info(
            "Event list bundle collected: events=%s tournaments=%s teams=%s",
            len(state.events),
            len(state.tournaments),
            len(state.teams),
        )
        return state.to_bundle()


class _EventListAccumulator:
    def __init__(self) -> None:
        self.payload_snapshots: list[ApiPayloadSnapshotRecord] = []
        self.sports: dict[int, dict[str, Any]] = {}
        self.countries: dict[str, dict[str, Any]] = {}
        self.categories: dict[int, dict[str, Any]] = {}
        self.teams: dict[int, dict[str, Any]] = {}
        self.unique_tournaments: dict[int, dict[str, Any]] = {}
        self.seasons: dict[int, dict[str, Any]] = {}
        self.tournaments: dict[int, dict[str, Any]] = {}
        self.event_statuses: dict[int, dict[str, Any]] = {}
        self.events: dict[int, dict[str, Any]] = {}
        self.event_round_infos: dict[int, dict[str, Any]] = {}
        self.event_status_times: dict[int, dict[str, Any]] = {}
        self.event_times: dict[int, dict[str, Any]] = {}
        self.event_var_in_progress_items: dict[int, dict[str, Any]] = {}
        self.event_scores: dict[tuple[int, str], dict[str, Any]] = {}
        self.event_filter_values: dict[tuple[int, str, int], dict[str, Any]] = {}
        self.event_change_items: dict[tuple[int, int], dict[str, Any]] = {}

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

    def ingest_event(self, payload: Mapping[str, Any]) -> int | None:
        event_id = _as_int(payload.get("id"))
        if event_id is None:
            return None

        tournament_id = self.ingest_tournament(_as_mapping(payload.get("tournament")))
        season_id = self.ingest_season(_as_mapping(payload.get("season")))
        status_code = self.ingest_status(_as_mapping(payload.get("status")))
        home_team_id = self.ingest_team(_as_mapping(payload.get("homeTeam")))
        away_team_id = self.ingest_team(_as_mapping(payload.get("awayTeam")))

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

    def ingest_team(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        team_id = _as_int(payload.get("id"))
        if team_id is None:
            return None

        parent_team_id = self.ingest_team(_as_mapping(payload.get("parentTeam")))
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
                "parent_team_id": parent_team_id,
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
                if isinstance(item, str):
                    self.event_filter_values[(event_id, filter_name, ordinal)] = {
                        "event_id": event_id,
                        "filter_name": filter_name,
                        "ordinal": ordinal,
                        "filter_value": item,
                    }

    def ingest_changes(self, event_id: int, payload: Mapping[str, Any] | None) -> None:
        if not payload:
            return
        change_timestamp = _as_int(payload.get("changeTimestamp"))
        values = payload.get("changes")
        if not isinstance(values, list):
            return
        for ordinal, item in enumerate(values):
            if isinstance(item, str):
                self.event_change_items[(event_id, ordinal)] = {
                    "event_id": event_id,
                    "change_timestamp": change_timestamp,
                    "ordinal": ordinal,
                    "change_value": item,
                }

    def to_bundle(self) -> EventListBundle:
        return EventListBundle(
            registry_entries=event_list_registry_entries(),
            payload_snapshots=tuple(self.payload_snapshots),
            sports=tuple(SportRecord(**row) for _, row in sorted(self.sports.items())),
            countries=tuple(CountryRecord(**row) for _, row in sorted(self.countries.items())),
            categories=tuple(CategoryRecord(**row) for _, row in sorted(self.categories.items())),
            teams=tuple(EventTeamRecord(**row) for _, row in sorted(self.teams.items())),
            unique_tournaments=tuple(UniqueTournamentRecord(**row) for _, row in sorted(self.unique_tournaments.items())),
            seasons=tuple(EventSeasonRecord(**row) for _, row in sorted(self.seasons.items())),
            tournaments=tuple(TournamentRecord(**row) for _, row in sorted(self.tournaments.items())),
            event_statuses=tuple(EventStatusRecord(**row) for _, row in sorted(self.event_statuses.items())),
            events=tuple(EventRecord(**row) for _, row in sorted(self.events.items())),
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
        raise EventListParserError(f"Expected object payload for {source_url}, got {type(payload).__name__}")
    return payload


def _require_event_array(payload: Mapping[str, Any], envelope_key: str, source_url: str) -> tuple[object, ...]:
    envelope = payload.get(envelope_key)
    if not isinstance(envelope, list):
        raise EventListParserError(f"Missing array envelope '{envelope_key}' for {source_url}")
    return tuple(envelope)


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
