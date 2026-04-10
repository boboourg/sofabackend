"""Async parser for Sofascore standings endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .competition_parser import CategoryRecord, CountryRecord, SportRecord, UniqueTournamentRecord
from .endpoints import (
    EndpointRegistryEntry,
    TOURNAMENT_STANDINGS_ENDPOINT,
    UNIQUE_TOURNAMENT_STANDINGS_ENDPOINT,
    standings_registry_entries,
)
from .event_list_parser import EventTeamRecord, TournamentRecord
from .sofascore_client import SofascoreClient, SofascoreResponse


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
class StandingTieBreakingRuleRecord:
    id: int
    text: str


@dataclass(frozen=True)
class StandingPromotionRecord:
    id: int
    text: str


@dataclass(frozen=True)
class StandingRecord:
    id: int
    season_id: int
    tournament_id: int | None
    name: str
    type: str
    updated_at_timestamp: int | None = None
    tie_breaking_rule_id: int | None = None
    descriptions: tuple[Any, ...] | None = None


@dataclass(frozen=True)
class StandingRowRecord:
    id: int
    standing_id: int
    team_id: int
    position: int
    matches: int
    wins: int
    draws: int
    losses: int
    points: int
    scores_for: int
    scores_against: int
    score_diff_formatted: str
    promotion_id: int | None = None
    descriptions: tuple[Any, ...] | None = None


@dataclass(frozen=True)
class StandingsBundle:
    registry_entries: tuple[EndpointRegistryEntry, ...]
    payload_snapshots: tuple[ApiPayloadSnapshotRecord, ...]
    sports: tuple[SportRecord, ...]
    countries: tuple[CountryRecord, ...]
    categories: tuple[CategoryRecord, ...]
    unique_tournaments: tuple[UniqueTournamentRecord, ...]
    tournaments: tuple[TournamentRecord, ...]
    teams: tuple[EventTeamRecord, ...]
    tie_breaking_rules: tuple[StandingTieBreakingRuleRecord, ...]
    promotions: tuple[StandingPromotionRecord, ...]
    standings: tuple[StandingRecord, ...]
    standing_rows: tuple[StandingRowRecord, ...]


class StandingsParserError(RuntimeError):
    """Raised when a standings payload misses its expected structure."""


class StandingsParser:
    """Fetches and normalizes standings endpoints."""

    def __init__(self, client: SofascoreClient, *, logger: logging.Logger | None = None) -> None:
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    async def fetch_unique_tournament_standings(
        self,
        unique_tournament_id: int,
        season_id: int,
        *,
        scopes: Iterable[str] = ("total",),
        timeout: float = 20.0,
    ) -> StandingsBundle:
        state = _StandingsAccumulator()
        for scope in tuple(dict.fromkeys(scopes)):
            await self._fetch_standings_collection(
                UNIQUE_TOURNAMENT_STANDINGS_ENDPOINT,
                state,
                timeout=timeout,
                context_entity_type="season",
                context_entity_id=season_id,
                season_id=season_id,
                unique_tournament_id=unique_tournament_id,
                scope=scope,
            )
        return state.to_bundle()

    async def fetch_tournament_standings(
        self,
        tournament_id: int,
        season_id: int,
        *,
        scopes: Iterable[str] = ("total",),
        timeout: float = 20.0,
    ) -> StandingsBundle:
        state = _StandingsAccumulator()
        for scope in tuple(dict.fromkeys(scopes)):
            await self._fetch_standings_collection(
                TOURNAMENT_STANDINGS_ENDPOINT,
                state,
                timeout=timeout,
                context_entity_type="season",
                context_entity_id=season_id,
                season_id=season_id,
                tournament_id=tournament_id,
                scope=scope,
            )
        return state.to_bundle()

    async def _fetch_standings_collection(
        self,
        endpoint,
        state: "_StandingsAccumulator",
        *,
        timeout: float,
        context_entity_type: str | None,
        context_entity_id: int | None,
        **path_params: object,
    ) -> None:
        url = endpoint.build_url(**path_params)
        response = await self.client.get_json(url, timeout=timeout)
        payload = _require_root_mapping(response.payload, url)
        standings = _require_standings_array(payload, endpoint.envelope_key, url)

        state.add_payload_snapshot(
            endpoint_pattern=endpoint.pattern,
            response=response,
            envelope_key=endpoint.envelope_key,
            context_entity_type=context_entity_type,
            context_entity_id=context_entity_id,
            payload=payload,
        )
        season_id = path_params.get("season_id")
        if not isinstance(season_id, int):
            raise StandingsParserError("season_id is required for standings parsing")
        for standing_payload in standings:
            if isinstance(standing_payload, Mapping):
                state.ingest_standing(standing_payload, season_id=season_id)

        self.logger.info(
            "Standings bundle collected: standings=%s rows=%s teams=%s",
            len(state.standings),
            len(state.standing_rows),
            len(state.teams),
        )


class _StandingsAccumulator:
    def __init__(self) -> None:
        self.payload_snapshots: list[ApiPayloadSnapshotRecord] = []
        self.sports: dict[int, dict[str, Any]] = {}
        self.countries: dict[str, dict[str, Any]] = {}
        self.categories: dict[int, dict[str, Any]] = {}
        self.unique_tournaments: dict[int, dict[str, Any]] = {}
        self.tournaments: dict[int, dict[str, Any]] = {}
        self.teams: dict[int, dict[str, Any]] = {}
        self.tie_breaking_rules: dict[int, dict[str, Any]] = {}
        self.promotions: dict[int, dict[str, Any]] = {}
        self.standings: dict[int, dict[str, Any]] = {}
        self.standing_rows: dict[int, dict[str, Any]] = {}

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

    def ingest_standing(self, payload: Mapping[str, Any], *, season_id: int) -> int | None:
        standing_id = _as_int(payload.get("id"))
        name = _as_str(payload.get("name"))
        standing_type = _as_str(payload.get("type"))
        if standing_id is None or name is None or standing_type is None:
            return standing_id

        tie_breaking_rule_id = self.ingest_tie_breaking_rule(_as_mapping(payload.get("tieBreakingRule")))
        tournament_id = self.ingest_tournament(_as_mapping(payload.get("tournament")))
        self._merge(
            self.standings,
            standing_id,
            {
                "id": standing_id,
                "season_id": season_id,
                "tournament_id": tournament_id,
                "name": name,
                "type": standing_type,
                "updated_at_timestamp": _as_int(payload.get("updatedAtTimestamp")),
                "tie_breaking_rule_id": tie_breaking_rule_id,
                "descriptions": _as_sequence(payload.get("descriptions")),
            },
        )

        for row_payload in _iter_mappings(payload.get("rows")):
            self.ingest_standing_row(row_payload, standing_id=standing_id)

        return standing_id

    def ingest_tie_breaking_rule(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        rule_id = _as_int(payload.get("id"))
        text = _as_str(payload.get("text"))
        if rule_id is None or text is None:
            return rule_id
        self._merge(self.tie_breaking_rules, rule_id, {"id": rule_id, "text": text})
        return rule_id

    def ingest_promotion(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        promotion_id = _as_int(payload.get("id"))
        text = _as_str(payload.get("text"))
        if promotion_id is None or text is None:
            return promotion_id
        self._merge(self.promotions, promotion_id, {"id": promotion_id, "text": text})
        return promotion_id

    def ingest_standing_row(self, payload: Mapping[str, Any], *, standing_id: int) -> int | None:
        row_id = _as_int(payload.get("id"))
        team_id = self.ingest_team(_as_mapping(payload.get("team")))
        position = _as_int(payload.get("position"))
        matches = _as_int(payload.get("matches"))
        wins = _as_int(payload.get("wins"))
        draws = _as_int(payload.get("draws"))
        losses = _as_int(payload.get("losses"))
        points = _as_int(payload.get("points"))
        scores_for = _as_int(payload.get("scoresFor"))
        scores_against = _as_int(payload.get("scoresAgainst"))
        score_diff_formatted = _as_str(payload.get("scoreDiffFormatted"))
        if None in {
            row_id,
            team_id,
            position,
            matches,
            wins,
            draws,
            losses,
            points,
            scores_for,
            scores_against,
        } or score_diff_formatted is None:
            return row_id

        self._merge(
            self.standing_rows,
            row_id,
            {
                "id": row_id,
                "standing_id": standing_id,
                "team_id": team_id,
                "position": position,
                "matches": matches,
                "wins": wins,
                "draws": draws,
                "losses": losses,
                "points": points,
                "scores_for": scores_for,
                "scores_against": scores_against,
                "score_diff_formatted": score_diff_formatted,
                "promotion_id": self.ingest_promotion(_as_mapping(payload.get("promotion"))),
                "descriptions": _as_sequence(payload.get("descriptions")),
            },
        )
        return row_id

    def ingest_tournament(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        tournament_id = _as_int(payload.get("id"))
        slug = _as_str(payload.get("slug"))
        name = _as_str(payload.get("name"))
        category_id = self.ingest_category(_as_mapping(payload.get("category")))
        if tournament_id is None or slug is None or name is None or category_id is None:
            return tournament_id

        unique_tournament_id = self.ingest_unique_tournament(_as_mapping(payload.get("uniqueTournament")))
        self._merge(
            self.tournaments,
            tournament_id,
            {
                "id": tournament_id,
                "slug": slug,
                "name": name,
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

    def ingest_unique_tournament(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        unique_tournament_id = _as_int(payload.get("id"))
        slug = _as_str(payload.get("slug"))
        name = _as_str(payload.get("name"))
        category_id = self.ingest_category(_as_mapping(payload.get("category")))
        if unique_tournament_id is None or slug is None or name is None or category_id is None:
            return unique_tournament_id

        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
        self._merge(
            self.unique_tournaments,
            unique_tournament_id,
            {
                "id": unique_tournament_id,
                "slug": slug,
                "name": name,
                "category_id": category_id,
                "country_alpha2": country_alpha2,
                "gender": _as_str(payload.get("gender")),
                "primary_color_hex": _as_str(payload.get("primaryColorHex")),
                "secondary_color_hex": _as_str(payload.get("secondaryColorHex")),
                "user_count": _as_int(payload.get("userCount")),
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
        slug = _as_str(payload.get("slug"))
        name = _as_str(payload.get("name"))
        sport_id = self.ingest_sport(_as_mapping(payload.get("sport")))
        if category_id is None or slug is None or name is None or sport_id is None:
            return category_id

        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
        self._merge(
            self.categories,
            category_id,
            {
                "id": category_id,
                "slug": slug,
                "name": name,
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
        slug = _as_str(payload.get("slug"))
        name = _as_str(payload.get("name"))
        if sport_id is None or slug is None or name is None:
            return sport_id
        self._merge(self.sports, sport_id, {"id": sport_id, "slug": slug, "name": name})
        return sport_id

    def ingest_country(self, payload: Mapping[str, Any] | None) -> str | None:
        if not payload:
            return None
        alpha2 = _as_str(payload.get("alpha2"))
        name = _as_str(payload.get("name"))
        if not alpha2 or name is None:
            return alpha2
        self._merge(
            self.countries,
            alpha2,
            {
                "alpha2": alpha2,
                "alpha3": _as_str(payload.get("alpha3")),
                "slug": _as_str(payload.get("slug")),
                "name": name,
            },
        )
        return alpha2

    def ingest_team(self, payload: Mapping[str, Any] | None) -> int | None:
        if not payload:
            return None
        team_id = _as_int(payload.get("id"))
        slug = _as_str(payload.get("slug"))
        name = _as_str(payload.get("name"))
        if team_id is None or slug is None or name is None:
            return team_id

        sport_id = self.ingest_sport(_as_mapping(payload.get("sport")))
        country_alpha2 = self.ingest_country(_as_mapping(payload.get("country")))
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

    def to_bundle(self) -> StandingsBundle:
        return StandingsBundle(
            registry_entries=standings_registry_entries(),
            payload_snapshots=tuple(self.payload_snapshots),
            sports=tuple(SportRecord(**row) for _, row in sorted(self.sports.items())),
            countries=tuple(CountryRecord(**row) for _, row in sorted(self.countries.items())),
            categories=tuple(CategoryRecord(**row) for _, row in sorted(self.categories.items())),
            unique_tournaments=tuple(
                UniqueTournamentRecord(**row) for _, row in sorted(self.unique_tournaments.items())
            ),
            tournaments=tuple(TournamentRecord(**row) for _, row in sorted(self.tournaments.items())),
            teams=tuple(EventTeamRecord(**row) for _, row in sorted(self.teams.items())),
            tie_breaking_rules=tuple(
                StandingTieBreakingRuleRecord(**row) for _, row in sorted(self.tie_breaking_rules.items())
            ),
            promotions=tuple(StandingPromotionRecord(**row) for _, row in sorted(self.promotions.items())),
            standings=tuple(StandingRecord(**row) for _, row in sorted(self.standings.items())),
            standing_rows=tuple(StandingRowRecord(**row) for _, row in sorted(self.standing_rows.items())),
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
        raise StandingsParserError(f"Expected object payload for {source_url}, got {type(payload).__name__}")
    return payload


def _require_standings_array(payload: Mapping[str, Any], envelope_key: str, source_url: str) -> tuple[object, ...]:
    envelope = payload.get(envelope_key)
    if not isinstance(envelope, list):
        raise StandingsParserError(f"Missing array envelope '{envelope_key}' for {source_url}")
    return tuple(envelope)


def _iter_mappings(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_sequence(value: object) -> tuple[Any, ...] | None:
    if not isinstance(value, list):
        return None
    return tuple(value)


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    return value if isinstance(value, int) else None


def _as_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None
