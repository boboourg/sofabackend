"""Async parser for Sofascore season-statistics endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .competition_parser import ApiPayloadSnapshotRecord, SportRecord
from .endpoints import (
    EndpointRegistryEntry,
    UNIQUE_TOURNAMENT_STATISTICS_ENDPOINT,
    UNIQUE_TOURNAMENT_STATISTICS_INFO_ENDPOINT,
    statistics_registry_entries,
)
from .sofascore_client import SofascoreClient, SofascoreResponse

_METRIC_COLUMN_MAP: dict[str, str] = {
    "accuratePasses": "accurate_passes",
    "accuratePassesPercentage": "accurate_passes_percentage",
    "assists": "assists",
    "bigChancesCreated": "big_chances_created",
    "bigChancesMissed": "big_chances_missed",
    "blockedShots": "blocked_shots",
    "cleanSheet": "clean_sheet",
    "clearances": "clearances",
    "dribbledPast": "dribbled_past",
    "errorLeadToGoal": "error_lead_to_goal",
    "errorLeadToShot": "error_lead_to_shot",
    "expectedGoals": "expected_goals",
    "goalConversionPercentage": "goal_conversion_percentage",
    "goals": "goals",
    "goalsFromOutsideTheBox": "goals_from_outside_the_box",
    "hitWoodwork": "hit_woodwork",
    "interceptions": "interceptions",
    "keyPasses": "key_passes",
    "outfielderBlocks": "outfielder_blocks",
    "ownGoals": "own_goals",
    "penaltyConceded": "penalty_conceded",
    "penaltySave": "penalty_save",
    "penaltyWon": "penalty_won",
    "rating": "rating",
    "runsOut": "runs_out",
    "savedShotsFromInsideTheBox": "saved_shots_from_inside_the_box",
    "saves": "saves",
    "successfulDribbles": "successful_dribbles",
    "successfulDribblesPercentage": "successful_dribbles_percentage",
    "tackles": "tackles",
    "totalShots": "total_shots",
}

_INTEGER_METRIC_COLUMNS = {
    "accurate_passes",
    "big_chances_created",
    "big_chances_missed",
    "clearances",
    "dribbled_past",
    "error_lead_to_goal",
    "interceptions",
    "key_passes",
    "outfielder_blocks",
    "own_goals",
    "penalty_conceded",
    "total_shots",
}


@dataclass(frozen=True)
class StatisticsQuery:
    """One exact Sofascore statistics query definition."""

    limit: int | None = None
    offset: int | None = None
    order: str | None = None
    accumulation: str | None = None
    group: str | None = None
    fields: tuple[str, ...] = ()
    filters: tuple[str, ...] = ()

    def to_query_params(self) -> dict[str, object]:
        params: dict[str, object] = {}
        if self.limit is not None:
            params["limit"] = self.limit
        if self.offset is not None:
            params["offset"] = self.offset
        if self.order is not None:
            params["order"] = self.order
        if self.accumulation is not None:
            params["accumulation"] = self.accumulation
        if self.group is not None:
            params["group"] = self.group
        if self.fields:
            params["fields"] = ",".join(self.fields)
        if self.filters:
            params["filters"] = ",".join(self.filters)
        return params

    def parsed_fields(self) -> tuple[str, ...] | None:
        if not self.fields:
            return None
        return tuple(self.fields)

    def parsed_filters(self) -> tuple[Mapping[str, Any], ...] | None:
        if not self.filters:
            return None
        return tuple(_parse_filter_expression(item) for item in self.filters)


@dataclass(frozen=True)
class StatisticsTeamRecord:
    id: int
    slug: str
    name: str
    short_name: str | None = None
    name_code: str | None = None
    sport_id: int | None = None
    gender: str | None = None
    type: int | None = None
    national: bool | None = None
    disabled: bool | None = None
    user_count: int | None = None
    field_translations: Mapping[str, Any] | None = None
    team_colors: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class StatisticsPlayerRecord:
    id: int
    slug: str | None
    name: str
    short_name: str | None = None
    team_id: int | None = None
    gender: str | None = None
    user_count: int | None = None
    field_translations: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class SeasonStatisticsConfigRecord:
    unique_tournament_id: int
    season_id: int
    hide_home_and_away: bool


@dataclass(frozen=True)
class SeasonStatisticsConfigTeamRecord:
    unique_tournament_id: int
    season_id: int
    team_id: int
    ordinal: int | None = None


@dataclass(frozen=True)
class SeasonStatisticsNationalityRecord:
    unique_tournament_id: int
    season_id: int
    nationality_code: str
    nationality_name: str


@dataclass(frozen=True)
class SeasonStatisticsGroupItemRecord:
    unique_tournament_id: int
    season_id: int
    group_scope: str
    group_name: str
    stat_field: str
    ordinal: int | None = None


@dataclass(frozen=True)
class SeasonStatisticsResultRecord:
    row_number: int
    player_id: int | None = None
    team_id: int | None = None
    accurate_passes: int | None = None
    accurate_passes_percentage: int | float | None = None
    assists: int | float | None = None
    big_chances_created: int | None = None
    big_chances_missed: int | None = None
    blocked_shots: int | float | None = None
    clean_sheet: int | float | None = None
    clearances: int | None = None
    dribbled_past: int | None = None
    error_lead_to_goal: int | None = None
    error_lead_to_shot: int | float | None = None
    expected_goals: int | float | None = None
    goal_conversion_percentage: int | float | None = None
    goals: int | float | None = None
    goals_from_outside_the_box: int | float | None = None
    hit_woodwork: int | float | None = None
    interceptions: int | None = None
    key_passes: int | None = None
    outfielder_blocks: int | None = None
    own_goals: int | None = None
    penalty_conceded: int | None = None
    penalty_save: int | float | None = None
    penalty_won: int | float | None = None
    rating: int | float | None = None
    runs_out: int | float | None = None
    saved_shots_from_inside_the_box: int | float | None = None
    saves: int | float | None = None
    successful_dribbles: int | float | None = None
    successful_dribbles_percentage: int | float | None = None
    tackles: int | float | None = None
    total_shots: int | None = None


@dataclass(frozen=True)
class SeasonStatisticsSnapshotRecord:
    endpoint_pattern: str
    unique_tournament_id: int
    season_id: int
    source_url: str
    page: int | None
    pages: int | None
    limit_value: int | None
    offset_value: int | None
    order_code: str | None
    accumulation: str | None
    group_code: str | None
    fields: tuple[str, ...] | None
    filters: tuple[Mapping[str, Any], ...] | None
    fetched_at: str
    results: tuple[SeasonStatisticsResultRecord, ...]


@dataclass(frozen=True)
class StatisticsBundle:
    registry_entries: tuple[EndpointRegistryEntry, ...]
    payload_snapshots: tuple[ApiPayloadSnapshotRecord, ...]
    sports: tuple[SportRecord, ...]
    teams: tuple[StatisticsTeamRecord, ...]
    players: tuple[StatisticsPlayerRecord, ...]
    configs: tuple[SeasonStatisticsConfigRecord, ...]
    config_teams: tuple[SeasonStatisticsConfigTeamRecord, ...]
    nationalities: tuple[SeasonStatisticsNationalityRecord, ...]
    group_items: tuple[SeasonStatisticsGroupItemRecord, ...]
    snapshots: tuple[SeasonStatisticsSnapshotRecord, ...]


class StatisticsParserError(RuntimeError):
    """Raised when a statistics payload misses its expected structure."""


class StatisticsParser:
    """Fetches and normalizes season-statistics endpoints."""

    def __init__(self, client: SofascoreClient, *, logger: logging.Logger | None = None) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    async def fetch_bundle(
        self,
        unique_tournament_id: int,
        season_id: int,
        *,
        queries: Iterable[StatisticsQuery] = (),
        include_info: bool = True,
        timeout: float = 20.0,
    ) -> StatisticsBundle:
        state = _StatisticsAccumulator()

        if include_info:
            await self._fetch_statistics_info(unique_tournament_id, season_id, state, timeout=timeout)

        for query in tuple(queries):
            await self._fetch_statistics_query(unique_tournament_id, season_id, query, state, timeout=timeout)

        self.logger.info(
            "Statistics bundle collected: configs=%s snapshots=%s players=%s teams=%s",
            len(state.configs),
            len(state.snapshots),
            len(state.players),
            len(state.teams),
        )
        return state.to_bundle()

    async def _fetch_statistics_info(
        self,
        unique_tournament_id: int,
        season_id: int,
        state: "_StatisticsAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = UNIQUE_TOURNAMENT_STATISTICS_INFO_ENDPOINT
        url = endpoint.build_url(unique_tournament_id=unique_tournament_id, season_id=season_id)
        response = await self.client.get_json(url, timeout=timeout)
        payload = _require_root_mapping(response.payload, url)

        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="season",
            context_entity_id=season_id,
            payload=payload,
        )
        state.ingest_statistics_info(unique_tournament_id, season_id, payload)

    async def _fetch_statistics_query(
        self,
        unique_tournament_id: int,
        season_id: int,
        query: StatisticsQuery,
        state: "_StatisticsAccumulator",
        *,
        timeout: float,
    ) -> None:
        endpoint = UNIQUE_TOURNAMENT_STATISTICS_ENDPOINT
        url = endpoint.build_url_with_query(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            query_params=query.to_query_params(),
        )
        response = await self.client.get_json(url, timeout=timeout)
        payload = _require_root_mapping(response.payload, url)

        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type="season",
            context_entity_id=season_id,
            payload=payload,
        )
        state.ingest_statistics_snapshot(unique_tournament_id, season_id, query, response, payload)


class _StatisticsAccumulator:
    def __init__(self) -> None:
        self.payload_snapshots: list[ApiPayloadSnapshotRecord] = []
        self.sports: dict[int, dict[str, Any]] = {}
        self.teams: dict[int, dict[str, Any]] = {}
        self.players: dict[int, dict[str, Any]] = {}
        self.configs: dict[tuple[int, int], dict[str, Any]] = {}
        self.config_teams: dict[tuple[int, int, int], dict[str, Any]] = {}
        self.nationalities: dict[tuple[int, int, str], dict[str, Any]] = {}
        self.group_items: dict[tuple[int, int, str, str, str], dict[str, Any]] = {}
        self.snapshots: list[SeasonStatisticsSnapshotRecord] = []

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

    def ingest_statistics_info(self, unique_tournament_id: int, season_id: int, payload: Mapping[str, Any]) -> None:
        hide_home_and_away = _as_bool(payload.get("hideHomeAndAway"))
        if hide_home_and_away is None:
            raise StatisticsParserError("statistics/info payload is missing boolean 'hideHomeAndAway'")

        self.configs[(unique_tournament_id, season_id)] = {
            "unique_tournament_id": unique_tournament_id,
            "season_id": season_id,
            "hide_home_and_away": hide_home_and_away,
        }

        for ordinal, team_payload in enumerate(_iter_mappings(payload.get("teams"))):
            team_id = self.ingest_team(team_payload)
            if team_id is None:
                continue
            self.config_teams[(unique_tournament_id, season_id, team_id)] = {
                "unique_tournament_id": unique_tournament_id,
                "season_id": season_id,
                "team_id": team_id,
                "ordinal": ordinal,
            }

        statistics_groups = _as_mapping(payload.get("statisticsGroups"))
        if statistics_groups:
            for group_name, fields in statistics_groups.items():
                if group_name == "detailed":
                    continue
                for ordinal, stat_field in enumerate(_iter_strings(fields)):
                    self.group_items[(unique_tournament_id, season_id, "regular", group_name, stat_field)] = {
                        "unique_tournament_id": unique_tournament_id,
                        "season_id": season_id,
                        "group_scope": "regular",
                        "group_name": group_name,
                        "stat_field": stat_field,
                        "ordinal": ordinal,
                    }

            detailed = _as_mapping(statistics_groups.get("detailed"))
            if detailed:
                for group_name, fields in detailed.items():
                    for ordinal, stat_field in enumerate(_iter_strings(fields)):
                        self.group_items[(unique_tournament_id, season_id, "detailed", group_name, stat_field)] = {
                            "unique_tournament_id": unique_tournament_id,
                            "season_id": season_id,
                            "group_scope": "detailed",
                            "group_name": group_name,
                            "stat_field": stat_field,
                            "ordinal": ordinal,
                        }

        nationalities = _as_mapping(payload.get("nationalities"))
        if nationalities:
            for nationality_code, nationality_name in nationalities.items():
                if not isinstance(nationality_name, str):
                    continue
                self.nationalities[(unique_tournament_id, season_id, nationality_code)] = {
                    "unique_tournament_id": unique_tournament_id,
                    "season_id": season_id,
                    "nationality_code": nationality_code,
                    "nationality_name": nationality_name,
                }

    def ingest_statistics_snapshot(
        self,
        unique_tournament_id: int,
        season_id: int,
        query: StatisticsQuery,
        response: SofascoreResponse,
        payload: Mapping[str, Any],
    ) -> None:
        results_payload = payload.get("results")
        if not isinstance(results_payload, list):
            raise StatisticsParserError(f"Missing array envelope 'results' for {response.source_url}")

        results: list[SeasonStatisticsResultRecord] = []
        for row_number, item in enumerate(results_payload, start=1):
            if not isinstance(item, Mapping):
                continue
            results.append(self.ingest_result_row(row_number, item))

        self.snapshots.append(
            SeasonStatisticsSnapshotRecord(
                endpoint_pattern=UNIQUE_TOURNAMENT_STATISTICS_ENDPOINT.pattern,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                source_url=response.source_url,
                page=_as_int(payload.get("page")),
                pages=_as_int(payload.get("pages")),
                limit_value=query.limit,
                offset_value=query.offset,
                order_code=query.order,
                accumulation=query.accumulation,
                group_code=query.group,
                fields=query.parsed_fields(),
                filters=query.parsed_filters(),
                fetched_at=response.fetched_at,
                results=tuple(results),
            )
        )

    def ingest_result_row(self, row_number: int, payload: Mapping[str, Any]) -> SeasonStatisticsResultRecord:
        team_id = self.ingest_team(_as_mapping(payload.get("team")))
        player_id = self.ingest_player(_as_mapping(payload.get("player")), team_id=team_id)

        row: dict[str, Any] = {
            "row_number": row_number,
            "player_id": player_id,
            "team_id": team_id,
        }
        for json_field, column_name in _METRIC_COLUMN_MAP.items():
            row[column_name] = _coerce_metric(column_name, payload.get(json_field))

        return SeasonStatisticsResultRecord(**row)

    def ingest_team(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        team_id = _as_int(payload.get("id"))
        slug = _as_str(payload.get("slug"))
        name = _as_str(payload.get("name"))
        if team_id is None or slug is None or name is None:
            return team_id

        sport_id = self.ingest_sport(_as_mapping(payload.get("sport")))
        self._merge(
            self.teams,
            team_id,
            {
                "id": team_id,
                "slug": slug,
                "name": name,
                "short_name": _as_str(payload.get("shortName")),
                "name_code": _as_str(payload.get("nameCode")),
                "sport_id": sport_id,
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

    def ingest_player(self, payload: Mapping[str, Any] | None, *, team_id: int | None = None) -> int | None:
        if not payload:
            return None
        player_id = _as_int(payload.get("id"))
        name = _as_str(payload.get("name"))
        if player_id is None or name is None:
            return player_id

        self._merge(
            self.players,
            player_id,
            {
                "id": player_id,
                "slug": _as_str(payload.get("slug")),
                "name": name,
                "short_name": _as_str(payload.get("shortName")),
                "team_id": team_id,
                "gender": _as_str(payload.get("gender")),
                "user_count": _as_int(payload.get("userCount")),
                "field_translations": _as_mapping(payload.get("fieldTranslations")),
            },
        )
        return player_id

    def ingest_sport(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        sport_id = _as_int(payload.get("id"))
        slug = _as_str(payload.get("slug"))
        name = _as_str(payload.get("name"))
        if sport_id is None or slug is None or name is None:
            return sport_id

        self._merge(self.sports, sport_id, {"id": sport_id, "slug": slug, "name": name})
        return sport_id

    def to_bundle(self) -> StatisticsBundle:
        return StatisticsBundle(
            registry_entries=statistics_registry_entries(),
            payload_snapshots=tuple(self.payload_snapshots),
            sports=tuple(SportRecord(**row) for _, row in sorted(self.sports.items())),
            teams=tuple(StatisticsTeamRecord(**row) for _, row in sorted(self.teams.items())),
            players=tuple(StatisticsPlayerRecord(**row) for _, row in sorted(self.players.items())),
            configs=tuple(SeasonStatisticsConfigRecord(**row) for _, row in sorted(self.configs.items())),
            config_teams=tuple(
                SeasonStatisticsConfigTeamRecord(**row) for _, row in sorted(self.config_teams.items())
            ),
            nationalities=tuple(
                SeasonStatisticsNationalityRecord(**row) for _, row in sorted(self.nationalities.items())
            ),
            group_items=tuple(
                SeasonStatisticsGroupItemRecord(**row) for _, row in sorted(self.group_items.items())
            ),
            snapshots=tuple(self.snapshots),
        )

    @staticmethod
    def _merge(store: dict[Any, dict[str, Any]], key: Any, row: dict[str, Any]) -> None:
        current = dict(store.get(key, {}))
        for field_name, value in row.items():
            if value is not None or field_name not in current:
                current[field_name] = value
        store[key] = current


def _parse_filter_expression(expression: str) -> dict[str, Any]:
    field_name, operator, raw_value = _split_filter_expression(expression)
    values = tuple(_parse_filter_value(item) for item in raw_value.split("~")) if raw_value else ()
    return {
        "expression": expression,
        "field": field_name,
        "operator": operator,
        "raw_value": raw_value,
        "values": values,
    }


def _split_filter_expression(expression: str) -> tuple[str | None, str | None, str]:
    parts = expression.split(".", 2)
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        return parts[0], parts[1], ""
    return None, None, expression


def _parse_filter_value(value: str) -> int | float | str:
    int_value = _as_int(value)
    if int_value is not None:
        return int_value
    float_value = _as_float(value)
    if float_value is not None:
        return float_value
    return value


def _coerce_metric(column_name: str, value: object) -> int | float | None:
    if value is None or isinstance(value, bool):
        return None
    if column_name in _INTEGER_METRIC_COLUMNS:
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        return None
    if isinstance(value, (int, float)):
        return value
    return None


def _require_root_mapping(payload: object, source_url: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise StatisticsParserError(f"Expected object payload for {source_url}, got {type(payload).__name__}")
    return payload


def _iter_mappings(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _iter_strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
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


def _as_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None
