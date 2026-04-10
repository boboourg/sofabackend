"""Exact Sofascore endpoint templates used by parser jobs."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlencode

SOFASCORE_BASE_URL = "https://www.sofascore.com"


@dataclass(frozen=True)
class EndpointRegistryEntry:
    """Registry row for an exact Sofascore path/query template."""

    pattern: str
    path_template: str
    query_template: str | None
    envelope_key: str
    target_table: str | None = None
    notes: str | None = None


@dataclass(frozen=True)
class SofascoreEndpoint:
    """One exact Sofascore endpoint definition."""

    path_template: str
    envelope_key: str
    target_table: str | None = None
    query_template: str | None = None
    notes: str | None = None

    @property
    def pattern(self) -> str:
        if self.query_template:
            return f"{self.path_template}?{self.query_template}"
        return self.path_template

    def build_url(self, **path_params: object) -> str:
        return f"{SOFASCORE_BASE_URL}{self.path_template.format(**path_params)}"

    def build_url_with_query(self, *, query_params: dict[str, object] | None = None, **path_params: object) -> str:
        url = self.build_url(**path_params)
        if not query_params:
            return url
        encoded_query = urlencode(query_params, doseq=True)
        return f"{url}?{encoded_query}"

    def registry_entry(self) -> EndpointRegistryEntry:
        return EndpointRegistryEntry(
            pattern=self.pattern,
            path_template=self.path_template,
            query_template=self.query_template,
            envelope_key=self.envelope_key,
            target_table=self.target_table,
            notes=self.notes,
        )


UNIQUE_TOURNAMENT_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}",
    envelope_key="uniqueTournament",
    target_table="unique_tournament",
)

UNIQUE_TOURNAMENT_SEASONS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/seasons",
    envelope_key="seasons",
    target_table="season",
)

UNIQUE_TOURNAMENT_SEASON_INFO_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/info",
    envelope_key="info",
    target_table="api_payload_snapshot",
    notes="Hydrates season metadata and newcomer teams; raw payload should also be snapshotted.",
)

COMPETITION_ENDPOINTS = (
    UNIQUE_TOURNAMENT_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASONS_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASON_INFO_ENDPOINT,
)

SPORT_FOOTBALL_SCHEDULED_EVENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/sport/football/scheduled-events/{date}",
    envelope_key="events",
    target_table="event",
)

SPORT_FOOTBALL_LIVE_EVENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/sport/football/events/live",
    envelope_key="events",
    target_table="event",
)

UNIQUE_TOURNAMENT_FEATURED_EVENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/featured-events",
    envelope_key="featuredEvents",
    target_table="event",
)

UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round_number}",
    envelope_key="events",
    target_table="event",
)

EVENT_LIST_ENDPOINTS = (
    SPORT_FOOTBALL_SCHEDULED_EVENTS_ENDPOINT,
    SPORT_FOOTBALL_LIVE_EVENTS_ENDPOINT,
    UNIQUE_TOURNAMENT_FEATURED_EVENTS_ENDPOINT,
    UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT,
)

EVENT_DETAIL_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}",
    envelope_key="event",
    target_table="event",
)

EVENT_LINEUPS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/lineups",
    envelope_key="home,away",
    target_table="event_lineup",
)

EVENT_MANAGERS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/managers",
    envelope_key="homeManager,awayManager",
    target_table="event_manager_assignment",
)

EVENT_H2H_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/h2h",
    envelope_key="teamDuel,managerDuel",
    target_table="event_duel",
)

EVENT_PREGAME_FORM_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/pregame-form",
    envelope_key="homeTeam,awayTeam",
    target_table="event_pregame_form",
)

EVENT_VOTES_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/votes",
    envelope_key="vote",
    target_table="event_vote_option",
)

EVENT_ODDS_ALL_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/odds/{provider_id}/all",
    envelope_key="markets",
    target_table="event_market",
)

EVENT_ODDS_FEATURED_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/odds/{provider_id}/featured",
    envelope_key="featured",
    target_table="event_market",
)

EVENT_WINNING_ODDS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/provider/{provider_id}/winning-odds",
    envelope_key="home,away",
    target_table="event_winning_odds",
)

EVENT_DETAIL_ENDPOINTS = (
    EVENT_DETAIL_ENDPOINT,
    EVENT_LINEUPS_ENDPOINT,
    EVENT_MANAGERS_ENDPOINT,
    EVENT_H2H_ENDPOINT,
    EVENT_PREGAME_FORM_ENDPOINT,
    EVENT_VOTES_ENDPOINT,
    EVENT_ODDS_ALL_ENDPOINT,
    EVENT_ODDS_FEATURED_ENDPOINT,
    EVENT_WINNING_ODDS_ENDPOINT,
)


def competition_registry_entries() -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for the competition parser family."""

    return tuple(endpoint.registry_entry() for endpoint in COMPETITION_ENDPOINTS)


def event_list_registry_entries() -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for the event-list parser family."""

    return tuple(endpoint.registry_entry() for endpoint in EVENT_LIST_ENDPOINTS)


def event_detail_registry_entries() -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for the event-detail parser family."""

    return tuple(endpoint.registry_entry() for endpoint in EVENT_DETAIL_ENDPOINTS)
