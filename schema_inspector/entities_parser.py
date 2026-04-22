"""Async parser for player/team entity and enrichment endpoints."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

from .competition_parser import (
    ApiPayloadSnapshotRecord,
    CategoryRecord,
    CountryRecord,
    SportRecord,
    UniqueTournamentRecord,
    UniqueTournamentSeasonRecord,
)
from .endpoints import (
    EndpointRegistryEntry,
    PLAYER_ENDPOINT,
    PLAYER_SEASON_HEATMAP_OVERALL_ENDPOINT,
    PLAYER_SEASON_OVERALL_STATISTICS_ENDPOINT,
    PLAYER_STATISTICS_ENDPOINT,
    PLAYER_STATISTICS_SEASONS_ENDPOINT,
    PLAYER_TRANSFER_HISTORY_ENDPOINT,
    TEAM_ENDPOINT,
    TEAM_PERFORMANCE_GRAPH_ENDPOINT,
    TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT,
    TEAM_SEASON_OVERALL_STATISTICS_ENDPOINT,
    TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT,
    entities_registry_entries,
)
from .event_detail_parser import (
    EventDetailTeamRecord,
    EventDetailTournamentRecord,
    ManagerRecord,
    ManagerTeamMembershipRecord,
    PlayerRecord,
    VenueRecord,
    _EventDetailAccumulator,
    _as_int,
    _as_mapping,
    _as_sequence,
    _as_str,
    _iter_mappings,
    _require_root_mapping,
)
from .event_list_parser import EventSeasonRecord
from .sofascore_client import SofascoreClient, SofascoreResponse
from .sofascore_client import SofascoreHttpError


@dataclass(frozen=True)
class CategoryTransferPeriodRecord:
    category_id: int
    active_from: str
    active_to: str


@dataclass(frozen=True)
class PlayerTransferHistoryRecord:
    id: int
    player_id: int
    transfer_from_team_id: int | None
    transfer_to_team_id: int | None
    from_team_name: str
    to_team_name: str
    transfer_date_timestamp: int
    transfer_fee: int
    transfer_fee_description: str
    transfer_fee_raw: Mapping[str, Any]
    type: int


@dataclass(frozen=True)
class PlayerSeasonStatisticsRecord:
    player_id: int
    unique_tournament_id: int
    season_id: int
    team_id: int
    stat_type: str
    statistics_id: int | None = None
    season_year: str | None = None
    start_year: int | None = None
    end_year: int | None = None
    accurate_crosses: int | None = None
    accurate_crosses_percentage: int | float | None = None
    accurate_long_balls: int | None = None
    accurate_long_balls_percentage: int | float | None = None
    accurate_passes: int | None = None
    accurate_passes_percentage: int | float | None = None
    aerial_duels_won: int | None = None
    appearances: int | None = None
    assists: int | None = None
    big_chances_created: int | None = None
    big_chances_missed: int | None = None
    blocked_shots: int | None = None
    clean_sheet: int | None = None
    count_rating: int | None = None
    dribbled_past: int | None = None
    error_lead_to_goal: int | None = None
    expected_assists: int | float | None = None
    expected_goals: int | float | None = None
    goals: int | None = None
    goals_assists_sum: int | None = None
    goals_conceded: int | None = None
    goals_prevented: int | float | None = None
    interceptions: int | None = None
    key_passes: int | None = None
    minutes_played: int | None = None
    outfielder_blocks: int | None = None
    pass_to_assist: int | None = None
    rating: int | float | None = None
    red_cards: int | None = None
    saves: int | None = None
    shots_from_inside_the_box: int | None = None
    shots_on_target: int | None = None
    successful_dribbles: int | None = None
    tackles: int | None = None
    total_cross: int | None = None
    total_long_balls: int | None = None
    total_passes: int | None = None
    total_rating: int | float | None = None
    total_shots: int | None = None
    yellow_cards: int | None = None
    statistics_payload: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class EntityStatisticsSeasonRecord:
    subject_type: str
    subject_id: int
    unique_tournament_id: int
    season_id: int
    all_time_season_id: int | None = None


@dataclass(frozen=True)
class EntityStatisticsTypeRecord:
    subject_type: str
    subject_id: int
    unique_tournament_id: int
    season_id: int
    stat_type: str


@dataclass(frozen=True)
class SeasonStatisticsTypeRecord:
    subject_type: str
    unique_tournament_id: int
    season_id: int
    stat_type: str


@dataclass(frozen=True)
class PlayerOverallRequest:
    player_id: int
    unique_tournament_id: int
    season_id: int


@dataclass(frozen=True)
class TeamOverallRequest:
    team_id: int
    unique_tournament_id: int
    season_id: int


@dataclass(frozen=True)
class PlayerHeatmapRequest:
    player_id: int
    unique_tournament_id: int
    season_id: int


@dataclass(frozen=True)
class TeamPerformanceGraphRequest:
    team_id: int
    unique_tournament_id: int
    season_id: int


@dataclass(frozen=True)
class EntitiesBundle:
    registry_entries: tuple[EndpointRegistryEntry, ...]
    payload_snapshots: tuple[ApiPayloadSnapshotRecord, ...]
    sports: tuple[SportRecord, ...]
    countries: tuple[CountryRecord, ...]
    categories: tuple[CategoryRecord, ...]
    category_transfer_periods: tuple[CategoryTransferPeriodRecord, ...]
    unique_tournaments: tuple[UniqueTournamentRecord, ...]
    seasons: tuple[EventSeasonRecord, ...]
    unique_tournament_seasons: tuple[UniqueTournamentSeasonRecord, ...]
    tournaments: tuple[EventDetailTournamentRecord, ...]
    teams: tuple[EventDetailTeamRecord, ...]
    venues: tuple[VenueRecord, ...]
    managers: tuple[ManagerRecord, ...]
    manager_team_memberships: tuple[ManagerTeamMembershipRecord, ...]
    players: tuple[PlayerRecord, ...]
    transfer_histories: tuple[PlayerTransferHistoryRecord, ...]
    player_season_statistics: tuple[PlayerSeasonStatisticsRecord, ...]
    entity_statistics_seasons: tuple[EntityStatisticsSeasonRecord, ...]
    entity_statistics_types: tuple[EntityStatisticsTypeRecord, ...]
    season_statistics_types: tuple[SeasonStatisticsTypeRecord, ...]


class EntitiesParserError(RuntimeError):
    """Raised when entity/enrichment payloads are malformed."""


class EntitiesParser:
    """Fetches and normalizes team/player enrichment endpoints."""

    def __init__(
        self,
        client: SofascoreClient,
        *,
        logger: logging.Logger | None = None,
        max_concurrency: int = 12,
    ) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)
        self.max_concurrency = max(1, int(max_concurrency))

    async def fetch_bundle(
        self,
        *,
        player_ids: Sequence[int] = (),
        player_statistics_ids: Sequence[int] = (),
        team_ids: Sequence[int] = (),
        player_overall_requests: Sequence[PlayerOverallRequest] = (),
        team_overall_requests: Sequence[TeamOverallRequest] = (),
        player_heatmap_requests: Sequence[PlayerHeatmapRequest] = (),
        team_performance_graph_requests: Sequence[TeamPerformanceGraphRequest] = (),
        include_player_statistics: bool = True,
        include_player_statistics_seasons: bool = True,
        include_player_transfer_history: bool = True,
        include_team_statistics_seasons: bool = True,
        include_team_player_statistics_seasons: bool = True,
        timeout: float = 20.0,
    ) -> EntitiesBundle:
        state = _EntitiesAccumulator()
        progress_state: dict[str, float] = {}

        resolved_player_ids = _dedupe_ints(player_ids)
        extra_player_statistics_ids = tuple(
            player_id for player_id in _dedupe_ints(player_statistics_ids) if player_id not in resolved_player_ids
        )

        self.logger.info(
            "Entities fetch start: players=%s extra_player_statistics=%s teams=%s player_overall=%s team_overall=%s player_heatmaps=%s team_graphs=%s",
            len(resolved_player_ids),
            len(extra_player_statistics_ids),
            len(_dedupe_ints(team_ids)),
            len(_dedupe_requests(player_overall_requests)),
            len(_dedupe_requests(team_overall_requests)),
            len(_dedupe_requests(player_heatmap_requests)),
            len(_dedupe_requests(team_performance_graph_requests)),
        )

        await self._run_stage(
            resolved_player_ids,
            stage="players",
            progress_state=progress_state,
            item_label=lambda player_id: f"player_id={player_id}",
            worker=lambda player_id: self._fetch_player_bundle(
                player_id,
                state,
                timeout=timeout,
                include_player_statistics=include_player_statistics,
                include_player_statistics_seasons=include_player_statistics_seasons,
                include_player_transfer_history=include_player_transfer_history,
            ),
        )

        if include_player_statistics:
            await self._run_stage(
                extra_player_statistics_ids,
                stage="extra_player_statistics",
                progress_state=progress_state,
                item_label=lambda player_id: f"player_id={player_id}",
                worker=lambda player_id: self._fetch_player_statistics(player_id, state, timeout=timeout),
            )

        resolved_team_ids = _dedupe_ints(team_ids)
        await self._run_stage(
            resolved_team_ids,
            stage="teams",
            progress_state=progress_state,
            item_label=lambda team_id: f"team_id={team_id}",
            worker=lambda team_id: self._fetch_team_bundle(
                team_id,
                state,
                timeout=timeout,
                include_team_statistics_seasons=include_team_statistics_seasons,
                include_team_player_statistics_seasons=include_team_player_statistics_seasons,
            ),
        )

        resolved_player_overall_requests = _dedupe_requests(player_overall_requests)
        await self._run_stage(
            resolved_player_overall_requests,
            stage="player_overall",
            progress_state=progress_state,
            item_label=lambda request: (
                f"player_id={request.player_id} "
                f"unique_tournament_id={request.unique_tournament_id} season_id={request.season_id}"
            ),
            worker=lambda request: self._fetch_player_overall(request, state, timeout=timeout),
        )

        resolved_team_overall_requests = _dedupe_requests(team_overall_requests)
        await self._run_stage(
            resolved_team_overall_requests,
            stage="team_overall",
            progress_state=progress_state,
            item_label=lambda request: (
                f"team_id={request.team_id} "
                f"unique_tournament_id={request.unique_tournament_id} season_id={request.season_id}"
            ),
            worker=lambda request: self._fetch_team_overall(request, state, timeout=timeout),
        )

        resolved_player_heatmap_requests = _dedupe_requests(player_heatmap_requests)
        await self._run_stage(
            resolved_player_heatmap_requests,
            stage="player_heatmaps",
            progress_state=progress_state,
            item_label=lambda request: (
                f"player_id={request.player_id} "
                f"unique_tournament_id={request.unique_tournament_id} season_id={request.season_id}"
            ),
            worker=lambda request: self._fetch_player_heatmap(request, state, timeout=timeout),
        )

        resolved_team_graph_requests = _dedupe_requests(team_performance_graph_requests)
        await self._run_stage(
            resolved_team_graph_requests,
            stage="team_graphs",
            progress_state=progress_state,
            item_label=lambda request: (
                f"team_id={request.team_id} "
                f"unique_tournament_id={request.unique_tournament_id} season_id={request.season_id}"
            ),
            worker=lambda request: self._fetch_team_performance_graph(request, state, timeout=timeout),
        )

        bundle = state.to_bundle()
        self.logger.debug(
            "Entities bundle collected: players=%s teams=%s transfers=%s player_season_stats=%s season_links=%s",
            len(bundle.players),
            len(bundle.teams),
            len(bundle.transfer_histories),
            len(bundle.player_season_statistics),
            len(bundle.entity_statistics_seasons),
        )
        return bundle

    async def _run_stage(
        self,
        values: Sequence[object],
        *,
        stage: str,
        progress_state: dict[str, float],
        item_label,
        worker,
    ) -> None:
        total = len(values)
        if total <= 0:
            return
        semaphore = asyncio.Semaphore(min(self.max_concurrency, total))

        async def _run_one(index: int, value: object) -> None:
            async with semaphore:
                _maybe_log_entities_progress(
                    self.logger,
                    progress_state,
                    stage=stage,
                    index=index,
                    total=total,
                    item_label=str(item_label(value)),
                )
                await worker(value)

        await asyncio.gather(*(_run_one(index, value) for index, value in enumerate(values, start=1)))

    async def _fetch_player_bundle(
        self,
        player_id: int,
        state: "_EntitiesAccumulator",
        *,
        timeout: float,
        include_player_statistics: bool,
        include_player_statistics_seasons: bool,
        include_player_transfer_history: bool,
    ) -> None:
        await self._fetch_player(player_id, state, timeout=timeout)
        if include_player_statistics:
            await self._fetch_player_statistics(player_id, state, timeout=timeout)
        if include_player_statistics_seasons:
            await self._fetch_player_statistics_seasons(player_id, state, timeout=timeout)
        if include_player_transfer_history:
            await self._fetch_player_transfer_history(player_id, state, timeout=timeout)

    async def _fetch_team_bundle(
        self,
        team_id: int,
        state: "_EntitiesAccumulator",
        *,
        timeout: float,
        include_team_statistics_seasons: bool,
        include_team_player_statistics_seasons: bool,
    ) -> None:
        await self._fetch_team(team_id, state, timeout=timeout)
        if include_team_statistics_seasons:
            await self._fetch_team_statistics_seasons(team_id, state, timeout=timeout)
        if include_team_player_statistics_seasons:
            await self._fetch_team_player_statistics_seasons(team_id, state, timeout=timeout)

    async def _fetch_player(self, player_id: int, state: "_EntitiesAccumulator", *, timeout: float) -> None:
        endpoint = PLAYER_ENDPOINT
        url = endpoint.build_url(player_id=player_id)
        response = await self.client.get_json(url, timeout=timeout)
        root = _require_root_mapping(response.payload, url)
        player = _as_mapping(root.get("player"))
        if not player:
            raise EntitiesParserError(f"Missing 'player' payload for {url}")

        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="player",
            context_entity_id=player_id,
            payload=root,
        )
        state.ingest_player_root(player)

    async def _fetch_team(self, team_id: int, state: "_EntitiesAccumulator", *, timeout: float) -> None:
        endpoint = TEAM_ENDPOINT
        url = endpoint.build_url(team_id=team_id)
        response = await self.client.get_json(url, timeout=timeout)
        root = _require_root_mapping(response.payload, url)
        team = _as_mapping(root.get("team"))
        if not team:
            raise EntitiesParserError(f"Missing 'team' payload for {url}")

        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="team",
            context_entity_id=team_id,
            payload=root,
        )
        state.ingest_team_root(team)

    async def _fetch_player_transfer_history(
        self,
        player_id: int,
        state: "_EntitiesAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = PLAYER_TRANSFER_HISTORY_ENDPOINT
        url = endpoint.build_url(player_id=player_id)
        try:
            response = await self.client.get_json(url, timeout=timeout)
        except SofascoreHttpError as exc:
            status_code = exc.transport_result.status_code if exc.transport_result is not None else None
            if status_code == 404:
                self.logger.info(
                    "Entities optional 404: context=player:%s endpoint=%s target=%s url=%s",
                    player_id,
                    endpoint.pattern,
                    endpoint.target_table,
                    url,
                )
                return
            raise
        except Exception as exc:
            self.logger.warning(
                "Entities optional fetch failed: context=player:%s endpoint=%s target=%s url=%s error=%s",
                player_id,
                endpoint.pattern,
                endpoint.target_table,
                url,
                exc,
            )
            return
        root = _require_root_mapping(response.payload, url)
        transfer_history = _iter_mappings(root.get("transferHistory"))

        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="player",
            context_entity_id=player_id,
            payload=root,
        )
        state.ingest_transfer_history(player_id, transfer_history)

    async def _fetch_player_statistics(
        self,
        player_id: int,
        state: "_EntitiesAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = PLAYER_STATISTICS_ENDPOINT
        url = endpoint.build_url(player_id=player_id)
        try:
            response = await self.client.get_json(url, timeout=timeout)
        except SofascoreHttpError as exc:
            status_code = exc.transport_result.status_code if exc.transport_result is not None else None
            if status_code == 404:
                self.logger.info(
                    "Entities optional 404: context=player:%s endpoint=%s target=%s url=%s",
                    player_id,
                    endpoint.pattern,
                    endpoint.target_table,
                    url,
                )
                return
            raise
        except Exception as exc:
            self.logger.warning(
                "Entities optional fetch failed: context=player:%s endpoint=%s target=%s url=%s error=%s",
                player_id,
                endpoint.pattern,
                endpoint.target_table,
                url,
                exc,
            )
            return
        root = _require_root_mapping(response.payload, url)

        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="player",
            context_entity_id=player_id,
            payload=root,
        )
        state.ingest_player_statistics(player_id, root)

    async def _fetch_player_statistics_seasons(
        self,
        player_id: int,
        state: "_EntitiesAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = PLAYER_STATISTICS_SEASONS_ENDPOINT
        url = endpoint.build_url(player_id=player_id)
        try:
            response = await self.client.get_json(url, timeout=timeout)
        except Exception as exc:
            self.logger.warning(
                "Entities optional fetch failed: context=player:%s endpoint=%s target=%s url=%s error=%s",
                player_id,
                endpoint.pattern,
                endpoint.target_table,
                url,
                exc,
            )
            return
        root = _require_root_mapping(response.payload, url)

        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="player",
            context_entity_id=player_id,
            payload=root,
        )
        state.ingest_entity_statistics_seasons(
            subject_type="player",
            subject_id=player_id,
            unique_tournament_seasons=_iter_mappings(root.get("uniqueTournamentSeasons")),
            types_map=_as_mapping(root.get("typesMap")),
        )

    async def _fetch_team_statistics_seasons(
        self,
        team_id: int,
        state: "_EntitiesAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT
        url = endpoint.build_url(team_id=team_id)
        try:
            response = await self.client.get_json(url, timeout=timeout)
        except Exception as exc:
            self.logger.warning(
                "Entities optional fetch failed: context=team:%s endpoint=%s target=%s url=%s error=%s",
                team_id,
                endpoint.pattern,
                endpoint.target_table,
                url,
                exc,
            )
            return
        root = _require_root_mapping(response.payload, url)

        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="team",
            context_entity_id=team_id,
            payload=root,
        )
        state.ingest_entity_statistics_seasons(
            subject_type="team",
            subject_id=team_id,
            unique_tournament_seasons=_iter_mappings(root.get("uniqueTournamentSeasons")),
            types_map=_as_mapping(root.get("typesMap")),
        )

    async def _fetch_team_player_statistics_seasons(
        self,
        team_id: int,
        state: "_EntitiesAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT
        url = endpoint.build_url(team_id=team_id)
        try:
            response = await self.client.get_json(url, timeout=timeout)
        except Exception as exc:
            self.logger.warning(
                "Entities optional fetch failed: context=team:%s endpoint=%s target=%s url=%s error=%s",
                team_id,
                endpoint.pattern,
                endpoint.target_table,
                url,
                exc,
            )
            return
        root = _require_root_mapping(response.payload, url)

        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="team",
            context_entity_id=team_id,
            payload=root,
        )
        state.ingest_season_statistics_types(
            subject_type="player",
            unique_tournament_seasons=_iter_mappings(root.get("uniqueTournamentSeasons")),
            types_map=_as_mapping(root.get("typesMap")),
        )

    async def _fetch_player_overall(
        self,
        request: PlayerOverallRequest,
        state: "_EntitiesAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = PLAYER_SEASON_OVERALL_STATISTICS_ENDPOINT
        root = await self._fetch_optional_root_payload(
            endpoint,
            context_entity_type="player",
            context_entity_id=request.player_id,
            timeout=timeout,
            player_id=request.player_id,
            unique_tournament_id=request.unique_tournament_id,
            season_id=request.season_id,
            state=state,
        )
        if root is None:
            return

        team = _as_mapping(root.get("team"))
        if team:
            state.core.ingest_team(team)

    async def _fetch_team_overall(
        self,
        request: TeamOverallRequest,
        state: "_EntitiesAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = TEAM_SEASON_OVERALL_STATISTICS_ENDPOINT
        root = await self._fetch_optional_root_payload(
            endpoint,
            context_entity_type="team",
            context_entity_id=request.team_id,
            timeout=timeout,
            team_id=request.team_id,
            unique_tournament_id=request.unique_tournament_id,
            season_id=request.season_id,
            state=state,
        )
        if root is None:
            return

    async def _fetch_player_heatmap(
        self,
        request: PlayerHeatmapRequest,
        state: "_EntitiesAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = PLAYER_SEASON_HEATMAP_OVERALL_ENDPOINT
        root = await self._fetch_optional_root_payload(
            endpoint,
            context_entity_type="player",
            context_entity_id=request.player_id,
            timeout=timeout,
            player_id=request.player_id,
            unique_tournament_id=request.unique_tournament_id,
            season_id=request.season_id,
            state=state,
        )
        if root is None:
            return

    async def _fetch_team_performance_graph(
        self,
        request: TeamPerformanceGraphRequest,
        state: "_EntitiesAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = TEAM_PERFORMANCE_GRAPH_ENDPOINT
        root = await self._fetch_optional_root_payload(
            endpoint,
            context_entity_type="team",
            context_entity_id=request.team_id,
            timeout=timeout,
            team_id=request.team_id,
            unique_tournament_id=request.unique_tournament_id,
            season_id=request.season_id,
            state=state,
        )
        if root is None:
            return

    async def _fetch_optional_root_payload(
        self,
        endpoint,
        *,
        state: "_EntitiesAccumulator",
        context_entity_type: str,
        context_entity_id: int,
        timeout: float,
        **path_params: object,
    ) -> Mapping[str, Any] | None:
        url = endpoint.build_url(**path_params)
        try:
            response = await self.client.get_json(url, timeout=timeout)
        except SofascoreHttpError as exc:
            status_code = exc.transport_result.status_code if exc.transport_result is not None else None
            if status_code == 404:
                self.logger.info(
                    "Entities optional 404: context=%s:%s endpoint=%s target=%s url=%s",
                    context_entity_type,
                    context_entity_id,
                    endpoint.pattern,
                    endpoint.target_table,
                    url,
                )
                return None
            raise
        except Exception as exc:
            self.logger.warning(
                "Entities optional fetch failed: context=%s:%s endpoint=%s target=%s url=%s error=%s",
                context_entity_type,
                context_entity_id,
                endpoint.pattern,
                endpoint.target_table,
                url,
                exc,
            )
            return None

        root = _require_root_mapping(response.payload, url)
        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type=context_entity_type,
            context_entity_id=context_entity_id,
            payload=root,
        )
        return root


class _EntitiesAccumulator:
    def __init__(self) -> None:
        self.core = _EventDetailAccumulator()
        self.payload_snapshots: list[ApiPayloadSnapshotRecord] = []
        self.category_transfer_periods: set[tuple[int, str, str]] = set()
        self.unique_tournament_seasons: set[tuple[int, int]] = set()
        self.transfer_histories: dict[int, dict[str, Any]] = {}
        self.player_season_statistics: dict[tuple[int, int, int, int, str], dict[str, Any]] = {}
        self.entity_statistics_seasons: dict[tuple[str, int, int, int], dict[str, Any]] = {}
        self.entity_statistics_types: set[tuple[str, int, int, int, str]] = set()
        self.season_statistics_types: set[tuple[str, int, int, str]] = set()

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

    def ingest_team_root(self, payload: Mapping[str, Any]) -> int | None:
        team_id = self.core.ingest_team(payload)
        self._collect_transfer_periods_from_team(payload)
        return team_id

    def ingest_player_root(self, payload: Mapping[str, Any]) -> int | None:
        team_id = self.core.ingest_team(_as_mapping(payload.get("team")))
        return self.core.ingest_player(payload, team_id=team_id)

    def ingest_transfer_history(self, player_id: int, items: Sequence[Mapping[str, Any]]) -> None:
        for item in items:
            transfer_id = _as_int(item.get("id"))
            transfer_date_timestamp = _as_int(item.get("transferDateTimestamp"))
            transfer_fee = _as_int(item.get("transferFee"))
            transfer_type = _as_int(item.get("type"))
            from_team_name = _as_str(item.get("fromTeamName"))
            to_team_name = _as_str(item.get("toTeamName"))
            transfer_fee_description = _as_str(item.get("transferFeeDescription"))
            transfer_fee_raw = _as_mapping(item.get("transferFeeRaw"))
            if (
                transfer_id is None
                or transfer_date_timestamp is None
                or transfer_fee is None
                or transfer_type is None
                or from_team_name is None
                or to_team_name is None
                or transfer_fee_description is None
                or transfer_fee_raw is None
            ):
                continue

            self.core.ingest_player(_as_mapping(item.get("player")))
            transfer_from_team_id = self.core.ingest_team(_as_mapping(item.get("transferFrom")))
            transfer_to_team_id = self.core.ingest_team(_as_mapping(item.get("transferTo")))
            self.transfer_histories[transfer_id] = {
                "id": transfer_id,
                "player_id": player_id,
                "transfer_from_team_id": transfer_from_team_id,
                "transfer_to_team_id": transfer_to_team_id,
                "from_team_name": from_team_name,
                "to_team_name": to_team_name,
                "transfer_date_timestamp": transfer_date_timestamp,
                "transfer_fee": transfer_fee,
                "transfer_fee_description": transfer_fee_description,
                "transfer_fee_raw": transfer_fee_raw,
                "type": transfer_type,
            }

    def ingest_player_statistics(self, player_id: int, payload: Mapping[str, Any]) -> None:
        for item in _iter_mappings(payload.get("seasons")):
            unique_tournament_id = self.core.ingest_unique_tournament(_as_mapping(item.get("uniqueTournament")))
            team_id = self.core.ingest_team(_as_mapping(item.get("team")))
            season_id = self.core.ingest_season(_as_mapping(item.get("season")))
            statistics = _as_mapping(item.get("statistics"))
            stat_type = None if statistics is None else _as_str(statistics.get("type"))
            if (
                unique_tournament_id is None
                or team_id is None
                or season_id is None
                or statistics is None
                or stat_type is None
            ):
                continue

            self.unique_tournament_seasons.add((unique_tournament_id, season_id))
            row = {
                "player_id": player_id,
                "unique_tournament_id": unique_tournament_id,
                "season_id": season_id,
                "team_id": team_id,
                "stat_type": stat_type,
                "statistics_id": _as_int(statistics.get("id")),
                "season_year": _as_str(item.get("year")),
                "start_year": _as_int(item.get("startYear")),
                "end_year": _as_int(item.get("endYear")),
                "statistics_payload": statistics,
            }
            for json_field, column_name in _PLAYER_STATISTICS_METRIC_COLUMN_MAP.items():
                row[column_name] = _coerce_player_statistics_metric(column_name, statistics.get(json_field))
            self.player_season_statistics[(player_id, unique_tournament_id, season_id, team_id, stat_type)] = row

    def ingest_entity_statistics_seasons(
        self,
        *,
        subject_type: str,
        subject_id: int,
        unique_tournament_seasons: Sequence[Mapping[str, Any]],
        types_map: Mapping[str, Any] | None,
    ) -> None:
        for item in unique_tournament_seasons:
            unique_tournament = _as_mapping(item.get("uniqueTournament"))
            unique_tournament_id = self.core.ingest_unique_tournament(unique_tournament)
            all_time_season_id = _as_int(item.get("allTimeSeasonId"))
            if unique_tournament_id is None:
                continue
            for season_payload in _iter_mappings(item.get("seasons")):
                season_id = self.core.ingest_season(season_payload)
                if season_id is None:
                    continue
                self.unique_tournament_seasons.add((unique_tournament_id, season_id))
                key = (subject_type, subject_id, unique_tournament_id, season_id)
                self.entity_statistics_seasons[key] = {
                    "subject_type": subject_type,
                    "subject_id": subject_id,
                    "unique_tournament_id": unique_tournament_id,
                    "season_id": season_id,
                    "all_time_season_id": all_time_season_id,
                }

        for unique_tournament_id, season_id, stat_type in _iter_types_map(types_map):
            self.entity_statistics_types.add((subject_type, subject_id, unique_tournament_id, season_id, stat_type))

    def ingest_season_statistics_types(
        self,
        *,
        subject_type: str,
        unique_tournament_seasons: Sequence[Mapping[str, Any]],
        types_map: Mapping[str, Any] | None,
    ) -> None:
        for item in unique_tournament_seasons:
            unique_tournament_id = self.core.ingest_unique_tournament(_as_mapping(item.get("uniqueTournament")))
            if unique_tournament_id is None:
                continue
            for season_payload in _iter_mappings(item.get("seasons")):
                season_id = self.core.ingest_season(season_payload)
                if season_id is not None:
                    self.unique_tournament_seasons.add((unique_tournament_id, season_id))

        for unique_tournament_id, season_id, stat_type in _iter_types_map(types_map):
            self.season_statistics_types.add((subject_type, unique_tournament_id, season_id, stat_type))

    def _collect_transfer_periods_from_team(self, team_payload: Mapping[str, Any]) -> None:
        self._collect_transfer_periods_from_category(_as_mapping(team_payload.get("category")))
        tournament = _as_mapping(team_payload.get("tournament"))
        if tournament:
            self._collect_transfer_periods_from_category(_as_mapping(tournament.get("category")))
            tournament_unique_tournament = _as_mapping(tournament.get("uniqueTournament"))
            if tournament_unique_tournament:
                self._collect_transfer_periods_from_category(_as_mapping(tournament_unique_tournament.get("category")))
        primary_unique_tournament = _as_mapping(team_payload.get("primaryUniqueTournament"))
        if primary_unique_tournament:
            self._collect_transfer_periods_from_category(_as_mapping(primary_unique_tournament.get("category")))

    def _collect_transfer_periods_from_category(self, payload: Mapping[str, Any] | None) -> None:
        if not payload:
            return
        category_id = self.core.ingest_category(payload)
        if category_id is None:
            return
        for item in _iter_mappings(payload.get("transferPeriod")):
            active_from = _as_str(item.get("activeFrom"))
            active_to = _as_str(item.get("activeTo"))
            if active_from and active_to:
                self.category_transfer_periods.add((category_id, active_from, active_to))

    def to_bundle(self) -> EntitiesBundle:
        return EntitiesBundle(
            registry_entries=entities_registry_entries(),
            payload_snapshots=tuple(self.payload_snapshots),
            sports=tuple(SportRecord(**row) for _, row in sorted(self.core.sports.items())),
            countries=tuple(CountryRecord(**row) for _, row in sorted(self.core.countries.items())),
            categories=tuple(CategoryRecord(**row) for _, row in sorted(self.core.categories.items())),
            category_transfer_periods=tuple(
                CategoryTransferPeriodRecord(category_id=item[0], active_from=item[1], active_to=item[2])
                for item in sorted(self.category_transfer_periods)
            ),
            unique_tournaments=tuple(
                UniqueTournamentRecord(**row) for _, row in sorted(self.core.unique_tournaments.items())
            ),
            seasons=tuple(EventSeasonRecord(**row) for _, row in sorted(self.core.seasons.items())),
            unique_tournament_seasons=tuple(
                UniqueTournamentSeasonRecord(unique_tournament_id=unique_tournament_id, season_id=season_id)
                for unique_tournament_id, season_id in sorted(self.unique_tournament_seasons)
            ),
            tournaments=tuple(
                EventDetailTournamentRecord(**row) for _, row in sorted(self.core.tournaments.items())
            ),
            teams=tuple(EventDetailTeamRecord(**row) for _, row in sorted(self.core.teams.items())),
            venues=tuple(VenueRecord(**row) for _, row in sorted(self.core.venues.items())),
            managers=tuple(ManagerRecord(**row) for _, row in sorted(self.core.managers.items())),
            manager_team_memberships=tuple(
                ManagerTeamMembershipRecord(manager_id=manager_id, team_id=team_id)
                for manager_id, team_id in sorted(self.core.manager_team_memberships)
            ),
            players=tuple(PlayerRecord(**row) for _, row in sorted(self.core.players.items())),
            transfer_histories=tuple(
                PlayerTransferHistoryRecord(**row) for _, row in sorted(self.transfer_histories.items())
            ),
            player_season_statistics=tuple(
                PlayerSeasonStatisticsRecord(**row)
                for _, row in sorted(self.player_season_statistics.items())
            ),
            entity_statistics_seasons=tuple(
                EntityStatisticsSeasonRecord(**row) for _, row in sorted(self.entity_statistics_seasons.items())
            ),
            entity_statistics_types=tuple(
                EntityStatisticsTypeRecord(
                    subject_type=subject_type,
                    subject_id=subject_id,
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    stat_type=stat_type,
                )
                for subject_type, subject_id, unique_tournament_id, season_id, stat_type in sorted(
                    self.entity_statistics_types
                )
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
        )


def _iter_types_map(types_map: Mapping[str, Any] | None) -> tuple[tuple[int, int, str], ...]:
    if not types_map:
        return ()
    items: list[tuple[int, int, str]] = []
    for unique_tournament_key, seasons_payload in types_map.items():
        unique_tournament_id = _parse_int_key(unique_tournament_key)
        if unique_tournament_id is None:
            continue
        seasons_mapping = _as_mapping(seasons_payload)
        if not seasons_mapping:
            continue
        for season_key, types_payload in seasons_mapping.items():
            season_id = _parse_int_key(season_key)
            if season_id is None:
                continue
            for stat_type in _as_sequence(types_payload):
                text_value = _as_str(stat_type)
                if text_value:
                    items.append((unique_tournament_id, season_id, text_value))
    return tuple(items)


def _parse_int_key(value: object) -> int | None:
    direct = _as_int(value)
    if direct is not None:
        return direct
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _dedupe_ints(values: Iterable[int]) -> tuple[int, ...]:
    return tuple(sorted({value for value in values}))


def _dedupe_requests(values: Sequence[object]) -> tuple[object, ...]:
    seen: list[object] = []
    for value in values:
        if value not in seen:
            seen.append(value)
    return tuple(seen)


def _maybe_log_entities_progress(
    logger: logging.Logger,
    progress_state: dict[str, float],
    *,
    stage: str,
    index: int,
    total: int,
    item_label: str,
    min_interval_seconds: float = 15.0,
) -> None:
    if total <= 0:
        return
    now = time.monotonic()
    last_logged_at = progress_state.get(stage)
    should_log = (
        index == 1
        or index == total
        or last_logged_at is None
        or (now - last_logged_at) >= min_interval_seconds
    )
    if not should_log:
        return
    progress_state[stage] = now
    logger.info("Entities progress [%s]: %s/%s %s", stage, index, total, item_label)


_PLAYER_STATISTICS_METRIC_COLUMN_MAP: dict[str, str] = {
    "accurateCrosses": "accurate_crosses",
    "accurateCrossesPercentage": "accurate_crosses_percentage",
    "accurateLongBalls": "accurate_long_balls",
    "accurateLongBallsPercentage": "accurate_long_balls_percentage",
    "accuratePasses": "accurate_passes",
    "accuratePassesPercentage": "accurate_passes_percentage",
    "aerialDuelsWon": "aerial_duels_won",
    "appearances": "appearances",
    "assists": "assists",
    "bigChancesCreated": "big_chances_created",
    "bigChancesMissed": "big_chances_missed",
    "blockedShots": "blocked_shots",
    "cleanSheet": "clean_sheet",
    "countRating": "count_rating",
    "dribbledPast": "dribbled_past",
    "errorLeadToGoal": "error_lead_to_goal",
    "expectedAssists": "expected_assists",
    "expectedGoals": "expected_goals",
    "goals": "goals",
    "goalsAssistsSum": "goals_assists_sum",
    "goalsConceded": "goals_conceded",
    "goalsPrevented": "goals_prevented",
    "interceptions": "interceptions",
    "keyPasses": "key_passes",
    "minutesPlayed": "minutes_played",
    "outfielderBlocks": "outfielder_blocks",
    "passToAssist": "pass_to_assist",
    "rating": "rating",
    "redCards": "red_cards",
    "saves": "saves",
    "shotsFromInsideTheBox": "shots_from_inside_the_box",
    "shotsOnTarget": "shots_on_target",
    "successfulDribbles": "successful_dribbles",
    "tackles": "tackles",
    "totalCross": "total_cross",
    "totalLongBalls": "total_long_balls",
    "totalPasses": "total_passes",
    "totalRating": "total_rating",
    "totalShots": "total_shots",
    "yellowCards": "yellow_cards",
}

_PLAYER_STATISTICS_INTEGER_COLUMNS = {
    "accurate_crosses",
    "accurate_long_balls",
    "accurate_passes",
    "aerial_duels_won",
    "appearances",
    "assists",
    "big_chances_created",
    "big_chances_missed",
    "blocked_shots",
    "clean_sheet",
    "count_rating",
    "dribbled_past",
    "error_lead_to_goal",
    "goals",
    "goals_assists_sum",
    "goals_conceded",
    "interceptions",
    "key_passes",
    "minutes_played",
    "outfielder_blocks",
    "pass_to_assist",
    "red_cards",
    "saves",
    "shots_from_inside_the_box",
    "shots_on_target",
    "successful_dribbles",
    "tackles",
    "total_cross",
    "total_long_balls",
    "total_passes",
    "total_shots",
    "yellow_cards",
}


def _coerce_player_statistics_metric(column_name: str, value: object) -> int | float | None:
    if value is None or isinstance(value, bool):
        return None
    if column_name in _PLAYER_STATISTICS_INTEGER_COLUMNS:
        int_value = _as_int(value)
        if int_value is not None:
            return int_value
        float_value = _as_float(value)
        if float_value is not None and float_value.is_integer():
            return int(float_value)
        return None
    float_value = _as_float(value)
    if float_value is not None:
        return float_value
    int_value = _as_int(value)
    if int_value is not None:
        return int_value
    return None


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None
