"""Async parser for seasonal leaderboard and aggregate endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .competition_parser import ApiPayloadSnapshotRecord, CategoryRecord, CountryRecord, UniqueTournamentRecord, SportRecord
from .endpoints import (
    EndpointRegistryEntry,
    UNIQUE_TOURNAMENT_GROUPS_ENDPOINT,
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT,
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_RACE_ENDPOINT,
    UNIQUE_TOURNAMENT_PLAYER_STATISTICS_TYPES_ENDPOINT,
    UNIQUE_TOURNAMENT_TEAM_EVENTS_ENDPOINT,
    UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT,
    UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_PERIODS_ENDPOINT,
    UNIQUE_TOURNAMENT_TEAM_STATISTICS_TYPES_ENDPOINT,
    UNIQUE_TOURNAMENT_TOP_RATINGS_OVERALL_ENDPOINT,
    UNIQUE_TOURNAMENT_VENUES_ENDPOINT,
    leaderboards_registry_entries,
    sport_trending_top_players_endpoint,
    team_scoped_top_players_endpoint,
    unique_tournament_top_players_endpoint,
    unique_tournament_top_players_per_game_endpoint,
    unique_tournament_top_teams_endpoint,
)
from .sport_profiles import resolve_sport_profile
from .entities_parser import SeasonStatisticsTypeRecord
from .event_detail_parser import (
    EventDetailEventRecord,
    EventDetailTeamRecord,
    EventDetailTournamentRecord,
    PlayerRecord,
    VenueRecord,
    _EventDetailAccumulator,
    _as_bool,
    _as_int,
    _as_mapping,
    _as_sequence,
    _as_str,
    _iter_mappings,
    _require_root_mapping,
)
from .event_list_parser import (
    EventFilterValueRecord,
    EventChangeItemRecord,
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
class TopPlayerEntryRecord:
    metric_name: str
    ordinal: int
    player_id: int | None
    team_id: int | None
    event_id: int | None
    played_enough: bool | None
    statistic: float | int | None
    statistics_id: int | None
    statistics_payload: Mapping[str, Any] | None


@dataclass(frozen=True)
class TopPlayerSnapshotRecord:
    endpoint_pattern: str
    unique_tournament_id: int
    season_id: int
    source_url: str
    statistics_type: Mapping[str, Any] | None
    fetched_at: str
    entries: tuple[TopPlayerEntryRecord, ...]


@dataclass(frozen=True)
class TopTeamEntryRecord:
    metric_name: str
    ordinal: int
    team_id: int
    statistics_id: int | None
    statistics_payload: Mapping[str, Any] | None


@dataclass(frozen=True)
class TopTeamSnapshotRecord:
    endpoint_pattern: str
    unique_tournament_id: int
    season_id: int
    source_url: str
    fetched_at: str
    entries: tuple[TopTeamEntryRecord, ...]


@dataclass(frozen=True)
class PeriodRecord:
    id: int
    unique_tournament_id: int
    season_id: int
    period_name: str
    type: str
    start_date_timestamp: int
    created_at_timestamp: int
    round_name: str | None = None
    round_number: int | None = None
    round_slug: str | None = None


@dataclass(frozen=True)
class TeamOfTheWeekRecord:
    period_id: int
    formation: str


@dataclass(frozen=True)
class TeamOfTheWeekPlayerRecord:
    period_id: int
    entry_id: int
    player_id: int
    team_id: int
    order_value: int
    rating: str


@dataclass(frozen=True)
class SeasonGroupRecord:
    unique_tournament_id: int
    season_id: int
    tournament_id: int
    group_name: str


@dataclass(frozen=True)
class SeasonPlayerOfTheSeasonRecord:
    unique_tournament_id: int
    season_id: int
    player_id: int
    team_id: int | None
    player_of_the_tournament: bool | None
    statistics_id: int | None
    statistics_payload: Mapping[str, Any] | None


@dataclass(frozen=True)
class TournamentTeamEventBucketRecord:
    level_1_key: str
    level_2_key: str
    event_id: int
    ordinal: int | None = None


@dataclass(frozen=True)
class TournamentTeamEventSnapshotRecord:
    endpoint_pattern: str
    unique_tournament_id: int
    season_id: int
    scope: str
    source_url: str
    fetched_at: str
    buckets: tuple[TournamentTeamEventBucketRecord, ...]


@dataclass(frozen=True)
class LeaderboardsBundle:
    registry_entries: tuple[EndpointRegistryEntry, ...]
    payload_snapshots: tuple[ApiPayloadSnapshotRecord, ...]
    sports: tuple[SportRecord, ...]
    countries: tuple[CountryRecord, ...]
    categories: tuple[CategoryRecord, ...]
    unique_tournaments: tuple[UniqueTournamentRecord, ...]
    seasons: tuple[EventSeasonRecord, ...]
    tournaments: tuple[EventDetailTournamentRecord, ...]
    venues: tuple[VenueRecord, ...]
    teams: tuple[EventDetailTeamRecord, ...]
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
    top_player_snapshots: tuple[TopPlayerSnapshotRecord, ...]
    top_team_snapshots: tuple[TopTeamSnapshotRecord, ...]
    periods: tuple[PeriodRecord, ...]
    team_of_the_week: tuple[TeamOfTheWeekRecord, ...]
    team_of_the_week_players: tuple[TeamOfTheWeekPlayerRecord, ...]
    season_groups: tuple[SeasonGroupRecord, ...]
    season_player_of_the_season: tuple[SeasonPlayerOfTheSeasonRecord, ...]
    season_statistics_types: tuple[SeasonStatisticsTypeRecord, ...]
    tournament_team_event_snapshots: tuple[TournamentTeamEventSnapshotRecord, ...]


class LeaderboardsParserError(RuntimeError):
    """Raised when leaderboard payloads are malformed."""


class LeaderboardsParser:
    """Fetches and normalizes seasonal leaderboard endpoints."""

    def __init__(self, client: SofascoreClient, *, logger: logging.Logger | None = None) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    async def fetch_bundle(
        self,
        unique_tournament_id: int,
        season_id: int,
        *,
        sport_slug: str = "football",
        include_top_players: bool = True,
        include_top_ratings: bool = True,
        include_top_players_per_game: bool = True,
        include_top_teams: bool = True,
        include_player_of_the_season_race: bool = True,
        include_player_of_the_season: bool = True,
        include_venues: bool = True,
        include_groups: bool = True,
        include_team_of_the_week: bool = True,
        team_of_the_week_period_ids: Sequence[int] = (),
        include_statistics_types: bool = True,
        team_event_scopes: Sequence[str] = ("home", "away", "total"),
        team_top_players_team_ids: Sequence[int] = (),
        include_trending_top_players: bool = False,
        timeout: float = 20.0,
    ) -> LeaderboardsBundle:
        sport_profile = resolve_sport_profile(sport_slug)
        state = _LeaderboardsAccumulator()

        if include_top_players and sport_profile.top_players_suffix is not None:
            await self._fetch_top_players_root(
                unique_tournament_top_players_endpoint(sport_profile.top_players_suffix),
                state,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                timeout=timeout,
            )
        if include_top_ratings and sport_profile.include_top_ratings:
            await self._fetch_top_players_root(
                UNIQUE_TOURNAMENT_TOP_RATINGS_OVERALL_ENDPOINT,
                state,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                timeout=timeout,
            )
        if include_top_players_per_game and sport_profile.top_players_per_game_suffix is not None:
            await self._fetch_top_players_root(
                unique_tournament_top_players_per_game_endpoint(sport_profile.top_players_per_game_suffix),
                state,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                timeout=timeout,
            )
        if include_player_of_the_season_race and sport_profile.include_player_of_the_season_race:
            await self._fetch_top_players_root(
                UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_RACE_ENDPOINT,
                state,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                timeout=timeout,
            )
        if include_top_teams and sport_profile.top_teams_suffix is not None:
            await self._fetch_top_teams(
                state,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                path_suffix=sport_profile.top_teams_suffix,
                timeout=timeout,
            )
        if include_venues and sport_profile.include_venues:
            await self._fetch_venues(
                state,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                timeout=timeout,
            )
        if include_groups and sport_profile.include_groups:
            await self._fetch_groups(
                state,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                timeout=timeout,
            )
        if include_player_of_the_season and sport_profile.include_player_of_the_season:
            await self._fetch_player_of_the_season(
                state,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                timeout=timeout,
            )
        if include_statistics_types and sport_profile.include_statistics_types:
            await self._fetch_statistics_types(
                state,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                subject_type="player",
                timeout=timeout,
            )
            await self._fetch_statistics_types(
                state,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                subject_type="team",
                timeout=timeout,
            )
        if sport_profile.team_top_players_suffix is not None:
            for team_id in sorted(set(team_top_players_team_ids)):
                await self._fetch_team_scoped_top_players(
                    state,
                    team_id=team_id,
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    path_suffix=sport_profile.team_top_players_suffix,
                    timeout=timeout,
                )
        if include_trending_top_players and sport_profile.include_trending_top_players:
            await self._fetch_trending_top_players(state, sport_slug=sport_slug, timeout=timeout)

        if sport_profile.include_team_events:
            for scope in _normalize_scopes(team_event_scopes):
                await self._fetch_team_events(
                    state,
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    scope=scope,
                    timeout=timeout,
                )

        period_ids = tuple(sorted(set(team_of_the_week_period_ids)))
        if include_team_of_the_week and sport_profile.include_team_of_the_week:
            fetched_period_ids = await self._fetch_team_of_the_week_periods(
                state,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                timeout=timeout,
            )
            if not period_ids:
                period_ids = fetched_period_ids

        if sport_profile.include_team_of_the_week:
            for period_id in period_ids:
                await self._fetch_team_of_the_week(
                    state,
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    period_id=period_id,
                    timeout=timeout,
                )

        bundle = state.to_bundle(
            registry_entries=leaderboards_registry_entries(
                sport_slug=sport_slug,
                top_players_suffix=sport_profile.top_players_suffix or "overall",
                top_players_per_game_suffix=sport_profile.top_players_per_game_suffix or "all/overall",
                team_top_players_suffix=sport_profile.team_top_players_suffix or "overall",
                top_teams_suffix=sport_profile.top_teams_suffix or "overall",
            )
        )
        self.logger.debug(
            "Leaderboards bundle collected: top_player_snapshots=%s top_team_snapshots=%s periods=%s groups=%s",
            len(bundle.top_player_snapshots),
            len(bundle.top_team_snapshots),
            len(bundle.periods),
            len(bundle.season_groups),
        )
        return bundle

    async def _fetch_top_players_root(
        self,
        endpoint,
        state: "_LeaderboardsAccumulator",
        *,
        unique_tournament_id: int,
        season_id: int,
        timeout: float,
    ) -> None:
        response, root = await self._fetch_optional_root_payload(
            endpoint,
            state=state,
            context_entity_type="season",
            context_entity_id=season_id,
            timeout=timeout,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
        )
        if response is None or root is None:
            return
        top_players = _as_mapping(root.get("topPlayers"))
        if not top_players:
            raise LeaderboardsParserError(f"Missing 'topPlayers' payload for {response.source_url}")
        state.ingest_top_players(
            endpoint_pattern=endpoint.pattern,
            response=response,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            top_players=top_players,
            statistics_type=_as_mapping(root.get("statisticsType")),
        )

    async def _fetch_top_teams(
        self,
        state: "_LeaderboardsAccumulator",
        *,
        unique_tournament_id: int,
        season_id: int,
        path_suffix: str,
        timeout: float,
    ) -> None:
        endpoint = unique_tournament_top_teams_endpoint(path_suffix)
        response, root = await self._fetch_optional_root_payload(
            endpoint,
            state=state,
            context_entity_type="season",
            context_entity_id=season_id,
            timeout=timeout,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
        )
        if response is None or root is None:
            return
        top_teams = _as_mapping(root.get("topTeams"))
        if not top_teams:
            raise LeaderboardsParserError(f"Missing 'topTeams' payload for {response.source_url}")
        state.ingest_top_teams(
            endpoint_pattern=endpoint.pattern,
            response=response,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            top_teams=top_teams,
        )

    async def _fetch_venues(
        self,
        state: "_LeaderboardsAccumulator",
        *,
        unique_tournament_id: int,
        season_id: int,
        timeout: float,
    ) -> None:
        endpoint = UNIQUE_TOURNAMENT_VENUES_ENDPOINT
        response, root = await self._fetch_optional_root_payload(
            endpoint,
            state=state,
            context_entity_type="season",
            context_entity_id=season_id,
            timeout=timeout,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
        )
        if response is None or root is None:
            return
        state.ingest_venues(root)

    async def _fetch_groups(
        self,
        state: "_LeaderboardsAccumulator",
        *,
        unique_tournament_id: int,
        season_id: int,
        timeout: float,
    ) -> None:
        endpoint = UNIQUE_TOURNAMENT_GROUPS_ENDPOINT
        response, root = await self._fetch_optional_root_payload(
            endpoint,
            state=state,
            context_entity_type="season",
            context_entity_id=season_id,
            timeout=timeout,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
        )
        if response is None or root is None:
            return
        state.ingest_groups(unique_tournament_id, season_id, _iter_mappings(root.get("groups")))

    async def _fetch_player_of_the_season(
        self,
        state: "_LeaderboardsAccumulator",
        *,
        unique_tournament_id: int,
        season_id: int,
        timeout: float,
    ) -> None:
        endpoint = UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT
        response, root = await self._fetch_optional_root_payload(
            endpoint,
            state=state,
            context_entity_type="season",
            context_entity_id=season_id,
            timeout=timeout,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
        )
        if response is None or root is None:
            return
        state.ingest_player_of_the_season(unique_tournament_id, season_id, root)

    async def _fetch_team_of_the_week_periods(
        self,
        state: "_LeaderboardsAccumulator",
        *,
        unique_tournament_id: int,
        season_id: int,
        timeout: float,
    ) -> tuple[int, ...]:
        endpoint = UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_PERIODS_ENDPOINT
        response, root = await self._fetch_optional_root_payload(
            endpoint,
            state=state,
            context_entity_type="season",
            context_entity_id=season_id,
            timeout=timeout,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
        )
        if response is None or root is None:
            return ()
        periods = _iter_mappings(root.get("periods"))
        return state.ingest_periods(unique_tournament_id, season_id, periods)

    async def _fetch_team_of_the_week(
        self,
        state: "_LeaderboardsAccumulator",
        *,
        unique_tournament_id: int,
        season_id: int,
        period_id: int,
        timeout: float,
    ) -> None:
        endpoint = UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT
        response, root = await self._fetch_optional_root_payload(
            endpoint,
            state=state,
            context_entity_type="period",
            context_entity_id=period_id,
            timeout=timeout,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            period_id=period_id,
        )
        if response is None or root is None:
            return
        state.ingest_team_of_the_week(period_id, root)

    async def _fetch_statistics_types(
        self,
        state: "_LeaderboardsAccumulator",
        *,
        unique_tournament_id: int,
        season_id: int,
        subject_type: str,
        timeout: float,
    ) -> None:
        endpoint = (
            UNIQUE_TOURNAMENT_PLAYER_STATISTICS_TYPES_ENDPOINT
            if subject_type == "player"
            else UNIQUE_TOURNAMENT_TEAM_STATISTICS_TYPES_ENDPOINT
        )
        response, root = await self._fetch_optional_root_payload(
            endpoint,
            state=state,
            context_entity_type="season",
            context_entity_id=season_id,
            timeout=timeout,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
        )
        if response is None or root is None:
            return
        state.ingest_statistics_types(
            subject_type=subject_type,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            root=root,
        )

    async def _fetch_team_events(
        self,
        state: "_LeaderboardsAccumulator",
        *,
        unique_tournament_id: int,
        season_id: int,
        scope: str,
        timeout: float,
    ) -> None:
        endpoint = UNIQUE_TOURNAMENT_TEAM_EVENTS_ENDPOINT
        response, root = await self._fetch_optional_root_payload(
            endpoint,
            state=state,
            context_entity_type="season",
            context_entity_id=season_id,
            timeout=timeout,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            scope=scope,
        )
        if response is None or root is None:
            return
        state.ingest_team_events(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            scope=scope,
            response=response,
            root=root,
        )

    async def _fetch_team_scoped_top_players(
        self,
        state: "_LeaderboardsAccumulator",
        *,
        team_id: int,
        unique_tournament_id: int,
        season_id: int,
        path_suffix: str,
        timeout: float,
    ) -> None:
        endpoint = team_scoped_top_players_endpoint(path_suffix)
        response, root = await self._fetch_optional_root_payload(
            endpoint,
            state=state,
            context_entity_type="team",
            context_entity_id=team_id,
            timeout=timeout,
            team_id=team_id,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
        )
        if response is None or root is None:
            return
        top_players = _as_mapping(root.get("topPlayers"))
        if not top_players:
            raise LeaderboardsParserError(f"Missing 'topPlayers' payload for {response.source_url}")
        state.ingest_top_players(
            endpoint_pattern=endpoint.pattern,
            response=response,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            top_players=top_players,
            statistics_type=_as_mapping(root.get("statisticsType")),
        )

    async def _fetch_trending_top_players(
        self,
        state: "_LeaderboardsAccumulator",
        *,
        sport_slug: str,
        timeout: float,
    ) -> None:
        endpoint = sport_trending_top_players_endpoint(sport_slug)
        response, root = await self._fetch_optional_root_payload(
            endpoint,
            state=state,
            context_entity_type=None,
            context_entity_id=None,
            timeout=timeout,
        )
        if response is None or root is None:
            return

    async def _fetch_optional_root_payload(
        self,
        endpoint,
        *,
        state: "_LeaderboardsAccumulator",
        context_entity_type: str | None,
        context_entity_id: int | None,
        timeout: float,
        **path_params: object,
    ) -> tuple[SofascoreResponse | None, Mapping[str, Any] | None]:
        url = endpoint.build_url(**path_params)
        try:
            response = await self.client.get_json(url, timeout=timeout)
        except SofascoreHttpError as exc:
            status_code = exc.transport_result.status_code if exc.transport_result is not None else None
            if status_code == 404:
                self.logger.info(
                    "Leaderboards optional 404: context=%s:%s endpoint=%s target=%s url=%s",
                    context_entity_type,
                    context_entity_id,
                    endpoint.pattern,
                    endpoint.target_table,
                    url,
                )
                return None, None
            raise

        root = _require_root_mapping(response.payload, url)
        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type=context_entity_type,
            context_entity_id=context_entity_id,
            payload=root,
        )
        return response, root


class _LeaderboardsAccumulator:
    def __init__(self) -> None:
        self.core = _EventDetailAccumulator()
        self.payload_snapshots: list[ApiPayloadSnapshotRecord] = []
        self.top_player_snapshots: list[TopPlayerSnapshotRecord] = []
        self.top_team_snapshots: list[TopTeamSnapshotRecord] = []
        self.periods: dict[int, dict[str, Any]] = {}
        self.team_of_the_week: dict[int, dict[str, Any]] = {}
        self.team_of_the_week_players: dict[tuple[int, int], dict[str, Any]] = {}
        self.season_groups: dict[tuple[int, int, int], dict[str, Any]] = {}
        self.season_player_of_the_season: dict[tuple[int, int], dict[str, Any]] = {}
        self.season_statistics_types: set[tuple[str, int, int, str]] = set()
        self.tournament_team_event_snapshots: list[TournamentTeamEventSnapshotRecord] = []

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

    def ingest_top_players(
        self,
        *,
        endpoint_pattern: str,
        response: SofascoreResponse,
        unique_tournament_id: int,
        season_id: int,
        top_players: Mapping[str, Any],
        statistics_type: Mapping[str, Any] | None,
    ) -> None:
        entries: list[TopPlayerEntryRecord] = []
        for metric_name, raw_items in top_players.items():
            for ordinal, item in enumerate(_iter_mappings(raw_items)):
                player_id = self.core.ingest_player(_as_mapping(item.get("player")))
                team_id = self.core.ingest_team(_as_mapping(item.get("team")))
                event_payload = _as_mapping(item.get("event"))
                event_id = self.core.ingest_event_root(event_payload) if event_payload else None
                statistics_payload = _as_mapping(item.get("statistics"))
                statistics_id = _as_int(statistics_payload.get("id")) if statistics_payload else None
                entries.append(
                    TopPlayerEntryRecord(
                        metric_name=metric_name,
                        ordinal=ordinal,
                        player_id=player_id,
                        team_id=team_id,
                        event_id=event_id,
                        played_enough=_as_bool(item.get("playedEnough")),
                        statistic=_metric_value(metric_name, item, statistics_payload),
                        statistics_id=statistics_id,
                        statistics_payload=statistics_payload,
                    )
                )

        self.top_player_snapshots.append(
            TopPlayerSnapshotRecord(
                endpoint_pattern=endpoint_pattern,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                source_url=response.source_url,
                statistics_type=statistics_type,
                fetched_at=response.fetched_at,
                entries=tuple(entries),
            )
        )

    def ingest_top_teams(
        self,
        *,
        endpoint_pattern: str,
        response: SofascoreResponse,
        unique_tournament_id: int,
        season_id: int,
        top_teams: Mapping[str, Any],
    ) -> None:
        entries: list[TopTeamEntryRecord] = []
        for metric_name, raw_items in top_teams.items():
            for ordinal, item in enumerate(_iter_mappings(raw_items)):
                team_id = self.core.ingest_team(_as_mapping(item.get("team")))
                statistics_payload = _as_mapping(item.get("statistics"))
                if team_id is None:
                    continue
                entries.append(
                    TopTeamEntryRecord(
                        metric_name=metric_name,
                        ordinal=ordinal,
                        team_id=team_id,
                        statistics_id=_as_int(statistics_payload.get("id")) if statistics_payload else None,
                        statistics_payload=statistics_payload,
                    )
                )

        self.top_team_snapshots.append(
            TopTeamSnapshotRecord(
                endpoint_pattern=endpoint_pattern,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                source_url=response.source_url,
                fetched_at=response.fetched_at,
                entries=tuple(entries),
            )
        )

    def ingest_periods(
        self,
        unique_tournament_id: int,
        season_id: int,
        periods: Sequence[Mapping[str, Any]],
    ) -> tuple[int, ...]:
        period_ids: list[int] = []
        for item in periods:
            period_id = _as_int(item.get("id"))
            period_name = _as_str(item.get("periodName"))
            period_type = _as_str(item.get("type"))
            start_date_timestamp = _as_int(item.get("startDateTimestamp"))
            created_at_timestamp = _as_int(item.get("createdAtTimestamp"))
            if (
                period_id is None
                or period_name is None
                or period_type is None
                or start_date_timestamp is None
                or created_at_timestamp is None
            ):
                continue
            round_payload = _as_mapping(item.get("round"))
            self.periods[period_id] = {
                "id": period_id,
                "unique_tournament_id": unique_tournament_id,
                "season_id": season_id,
                "period_name": period_name,
                "type": period_type,
                "start_date_timestamp": start_date_timestamp,
                "created_at_timestamp": created_at_timestamp,
                "round_name": _as_str(round_payload.get("name")) if round_payload else None,
                "round_number": _as_int(round_payload.get("round")) if round_payload else None,
                "round_slug": _as_str(round_payload.get("slug")) if round_payload else None,
            }
            period_ids.append(period_id)
        return tuple(period_ids)

    def ingest_team_of_the_week(self, period_id: int, root: Mapping[str, Any]) -> None:
        formation = _as_str(root.get("formation"))
        if formation:
            self.team_of_the_week[period_id] = {"period_id": period_id, "formation": formation}

        for item in _iter_mappings(root.get("players")):
            entry_id = _as_int(item.get("id"))
            order_value = _as_int(item.get("order"))
            rating = _as_str(item.get("rating"))
            player_id = self.core.ingest_player(_as_mapping(item.get("player")))
            team_id = self.core.ingest_team(_as_mapping(item.get("team")))
            if None in {entry_id, order_value, rating, player_id, team_id}:
                continue
            self.team_of_the_week_players[(period_id, entry_id)] = {
                "period_id": period_id,
                "entry_id": entry_id,
                "player_id": player_id,
                "team_id": team_id,
                "order_value": order_value,
                "rating": rating,
            }

    def ingest_venues(self, root: Mapping[str, Any]) -> None:
        for item in _iter_mappings(root.get("venues")):
            self.core.ingest_venue(item)

    def ingest_groups(
        self,
        unique_tournament_id: int,
        season_id: int,
        groups: Sequence[Mapping[str, Any]],
    ) -> None:
        for item in groups:
            tournament_id = _as_int(item.get("tournamentId"))
            group_name = _as_str(item.get("groupName"))
            if tournament_id is None or group_name is None:
                continue
            self.season_groups[(unique_tournament_id, season_id, tournament_id)] = {
                "unique_tournament_id": unique_tournament_id,
                "season_id": season_id,
                "tournament_id": tournament_id,
                "group_name": group_name,
            }

    def ingest_player_of_the_season(
        self,
        unique_tournament_id: int,
        season_id: int,
        root: Mapping[str, Any],
    ) -> None:
        player_id = self.core.ingest_player(_as_mapping(root.get("player")))
        team_id = self.core.ingest_team(_as_mapping(root.get("team")))
        if player_id is None:
            return
        statistics_payload = _as_mapping(root.get("statistics"))
        self.season_player_of_the_season[(unique_tournament_id, season_id)] = {
            "unique_tournament_id": unique_tournament_id,
            "season_id": season_id,
            "player_id": player_id,
            "team_id": team_id,
            "player_of_the_tournament": _as_bool(root.get("playerOfTheTournament")),
            "statistics_id": _as_int(statistics_payload.get("id")) if statistics_payload else None,
            "statistics_payload": statistics_payload,
        }

    def ingest_statistics_types(
        self,
        *,
        subject_type: str,
        unique_tournament_id: int,
        season_id: int,
        root: Mapping[str, Any],
    ) -> None:
        for value in _as_sequence(root.get("types")):
            stat_type = _as_str(value)
            if stat_type:
                self.season_statistics_types.add((subject_type, unique_tournament_id, season_id, stat_type))

    def ingest_team_events(
        self,
        *,
        unique_tournament_id: int,
        season_id: int,
        scope: str,
        response: SofascoreResponse,
        root: Mapping[str, Any],
    ) -> None:
        tournament_team_events = _as_mapping(root.get("tournamentTeamEvents"))
        if not tournament_team_events:
            return
        buckets = tuple(self._walk_team_events(tournament_team_events))
        self.tournament_team_event_snapshots.append(
            TournamentTeamEventSnapshotRecord(
                endpoint_pattern=UNIQUE_TOURNAMENT_TEAM_EVENTS_ENDPOINT.pattern,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                scope=scope,
                source_url=response.source_url,
                fetched_at=response.fetched_at,
                buckets=buckets,
            )
        )

    def _walk_team_events(
        self,
        payload: Mapping[str, Any],
        path: tuple[str, ...] = (),
    ) -> tuple[TournamentTeamEventBucketRecord, ...]:
        buckets: list[TournamentTeamEventBucketRecord] = []
        for key, value in payload.items():
            if isinstance(value, list):
                level_1_key, level_2_key = _team_event_bucket_keys(path + (str(key),))
                for ordinal, item in enumerate(_iter_mappings(value)):
                    event_id = self.core.ingest_event_root(item)
                    if event_id is None:
                        continue
                    buckets.append(
                        TournamentTeamEventBucketRecord(
                            level_1_key=level_1_key,
                            level_2_key=level_2_key,
                            event_id=event_id,
                            ordinal=ordinal,
                        )
                    )
            else:
                nested = _as_mapping(value)
                if nested:
                    buckets.extend(self._walk_team_events(nested, path + (str(key),)))
        return tuple(buckets)

    def to_bundle(self, *, registry_entries: tuple[EndpointRegistryEntry, ...]) -> LeaderboardsBundle:
        return LeaderboardsBundle(
            registry_entries=registry_entries,
            payload_snapshots=tuple(self.payload_snapshots),
            sports=tuple(SportRecord(**row) for _, row in sorted(self.core.sports.items())),
            countries=tuple(CountryRecord(**row) for _, row in sorted(self.core.countries.items())),
            categories=tuple(CategoryRecord(**row) for _, row in sorted(self.core.categories.items())),
            unique_tournaments=tuple(
                UniqueTournamentRecord(**row) for _, row in sorted(self.core.unique_tournaments.items())
            ),
            seasons=tuple(EventSeasonRecord(**row) for _, row in sorted(self.core.seasons.items())),
            tournaments=tuple(
                EventDetailTournamentRecord(**row) for _, row in sorted(self.core.tournaments.items())
            ),
            venues=tuple(VenueRecord(**row) for _, row in sorted(self.core.venues.items())),
            teams=tuple(EventDetailTeamRecord(**row) for _, row in sorted(self.core.teams.items())),
            players=tuple(PlayerRecord(**row) for _, row in sorted(self.core.players.items())),
            event_statuses=tuple(EventStatusRecord(**row) for _, row in sorted(self.core.event_statuses.items())),
            events=tuple(EventDetailEventRecord(**row) for _, row in sorted(self.core.events.items())),
            event_round_infos=tuple(
                EventRoundInfoRecord(**row) for _, row in sorted(self.core.event_round_infos.items())
            ),
            event_status_times=tuple(
                EventStatusTimeRecord(**row) for _, row in sorted(self.core.event_status_times.items())
            ),
            event_times=tuple(EventTimeRecord(**row) for _, row in sorted(self.core.event_times.items())),
            event_var_in_progress_items=tuple(
                EventVarInProgressRecord(**row) for _, row in sorted(self.core.event_var_in_progress_items.items())
            ),
            event_scores=tuple(EventScoreRecord(**row) for _, row in sorted(self.core.event_scores.items())),
            event_filter_values=tuple(
                EventFilterValueRecord(**row) for _, row in sorted(self.core.event_filter_values.items())
            ),
            event_change_items=tuple(
                EventChangeItemRecord(**row) for _, row in sorted(self.core.event_change_items.items())
            ),
            top_player_snapshots=tuple(self.top_player_snapshots),
            top_team_snapshots=tuple(self.top_team_snapshots),
            periods=tuple(PeriodRecord(**row) for _, row in sorted(self.periods.items())),
            team_of_the_week=tuple(TeamOfTheWeekRecord(**row) for _, row in sorted(self.team_of_the_week.items())),
            team_of_the_week_players=tuple(
                TeamOfTheWeekPlayerRecord(**row) for _, row in sorted(self.team_of_the_week_players.items())
            ),
            season_groups=tuple(SeasonGroupRecord(**row) for _, row in sorted(self.season_groups.items())),
            season_player_of_the_season=tuple(
                SeasonPlayerOfTheSeasonRecord(**row)
                for _, row in sorted(self.season_player_of_the_season.items())
            ),
            season_statistics_types=tuple(
                SeasonStatisticsTypeRecord(
                    subject_type=subject_type,
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    stat_type=stat_type,
                )
                for subject_type, unique_tournament_id, season_id, stat_type in sorted(self.season_statistics_types)
            ),
            tournament_team_event_snapshots=tuple(self.tournament_team_event_snapshots),
        )


def _metric_value(
    metric_name: str,
    item_payload: Mapping[str, Any],
    statistics_payload: Mapping[str, Any] | None,
) -> float | int | None:
    for key in _metric_aliases(metric_name):
        value = _numeric_value((statistics_payload or {}).get(key))
        if value is not None:
            return value
    value = _numeric_value(item_payload.get(metric_name))
    if value is not None:
        return value
    return None


def _metric_aliases(metric_name: str) -> tuple[str, ...]:
    aliases = {
        "goalAssist": ("goalAssist", "goalsAssistsSum"),
        "avgRating": ("avgRating", "rating"),
    }
    return aliases.get(metric_name, (metric_name,))


def _numeric_value(value: object) -> float | int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    return None


def _normalize_scopes(scopes: Sequence[str]) -> tuple[str, ...]:
    allowed = {"home", "away", "total"}
    normalized: list[str] = []
    for value in scopes:
        scope = value.strip()
        if scope in allowed and scope not in normalized:
            normalized.append(scope)
    return tuple(normalized)


def _team_event_bucket_keys(path: tuple[str, ...]) -> tuple[str, str]:
    if not path:
        return ("root", "__items__")
    if len(path) == 1:
        return (path[0], "__items__")
    return (path[0], "/".join(path[1:]))
