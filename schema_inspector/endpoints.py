"""Exact Sofascore endpoint templates used by parser jobs."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlencode

from .sport_profiles import SUPPORTED_SPORT_SLUGS, resolve_sport_profile

SOFASCORE_BASE_URL = "https://www.sofascore.com"

LOCAL_API_SUPPORTED_SPORTS = SUPPORTED_SPORT_SLUGS


@dataclass(frozen=True)
class EndpointRegistryEntry:
    """Registry row for an exact Sofascore path/query template."""

    pattern: str
    path_template: str
    query_template: str | None
    envelope_key: str
    target_table: str | None = None
    notes: str | None = None
    source_slug: str = "sofascore"
    contract_version: str = "v1"


@dataclass(frozen=True)
class SofascoreEndpoint:
    """One exact Sofascore endpoint definition."""

    path_template: str
    envelope_key: str
    target_table: str | None = None
    query_template: str | None = None
    notes: str | None = None
    source_slug: str = "sofascore"
    contract_version: str = "v1"

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
            source_slug=self.source_slug,
            contract_version=self.contract_version,
        )


def _normalize_sport_slug(sport_slug: str) -> str:
    value = str(sport_slug).strip().lower()
    if not value:
        raise ValueError("sport_slug cannot be empty")
    return value


def _normalize_path_suffix(value: str) -> str:
    suffix = str(value).strip().strip("/")
    if not suffix:
        raise ValueError("path suffix cannot be empty")
    return suffix


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

def sport_date_categories_endpoint(sport_slug: str = "football") -> SofascoreEndpoint:
    normalized_sport_slug = _normalize_sport_slug(sport_slug)
    return SofascoreEndpoint(
        path_template=f"/api/v1/sport/{normalized_sport_slug}/{{date}}/{{timezone_offset_seconds}}/categories",
        envelope_key="categories",
        target_table="category_daily_summary",
        notes=(
            f"Daily {normalized_sport_slug} category discovery endpoint. Seeds categories plus "
            "discovered unique_tournament_id and team_id lists for follow-on ingestion."
        ),
    )


SPORT_FOOTBALL_DATE_CATEGORIES_ENDPOINT = sport_date_categories_endpoint("football")

CATEGORIES_SEED_ENDPOINTS = (SPORT_FOOTBALL_DATE_CATEGORIES_ENDPOINT,)


def sport_categories_endpoint(sport_slug: str = "football") -> SofascoreEndpoint:
    normalized_sport_slug = _normalize_sport_slug(sport_slug)
    return SofascoreEndpoint(
        path_template=f"/api/v1/sport/{normalized_sport_slug}/categories",
        envelope_key="categories",
        target_table="category",
        notes=f"Sport-level category listing for {normalized_sport_slug}.",
    )


def sport_categories_all_endpoint(sport_slug: str = "football") -> SofascoreEndpoint:
    normalized_sport_slug = _normalize_sport_slug(sport_slug)
    return SofascoreEndpoint(
        path_template=f"/api/v1/sport/{normalized_sport_slug}/categories/all",
        envelope_key="categories",
        target_table="category",
        notes=(
            f"Full category catalog for {normalized_sport_slug}; used as a wide discovery seed "
            "before category-specific tournament expansion."
        ),
    )


SPORT_FOOTBALL_CATEGORIES_ENDPOINT = sport_categories_endpoint("football")

SPORT_FOOTBALL_CATEGORIES_ALL_ENDPOINT = sport_categories_all_endpoint("football")


def sport_scheduled_tournaments_endpoint(sport_slug: str = "football") -> SofascoreEndpoint:
    normalized_sport_slug = _normalize_sport_slug(sport_slug)
    return SofascoreEndpoint(
        path_template=f"/api/v1/sport/{normalized_sport_slug}/scheduled-tournaments/{{date}}/page/{{page}}",
        envelope_key="scheduled",
        target_table="tournament",
        notes=(
            f"Daily scheduled tournament catalog for {normalized_sport_slug}. Useful as a wide discovery "
            "surface for active tournaments before event hydration."
        ),
    )


def sport_live_categories_endpoint(sport_slug: str = "football") -> SofascoreEndpoint:
    normalized_sport_slug = _normalize_sport_slug(sport_slug)
    return SofascoreEndpoint(
        path_template=f"/api/v1/sport/{normalized_sport_slug}/live-categories",
        envelope_key="categories",
        target_table="api_payload_snapshot",
        notes=f"Live category discovery surface for {normalized_sport_slug}.",
    )


def sport_live_tournaments_endpoint(sport_slug: str = "football") -> SofascoreEndpoint:
    normalized_sport_slug = _normalize_sport_slug(sport_slug)
    return SofascoreEndpoint(
        path_template=f"/api/v1/sport/{normalized_sport_slug}/live-tournaments",
        envelope_key="tournaments",
        target_table="api_payload_snapshot",
        notes=f"Live tournament discovery surface for {normalized_sport_slug}.",
    )


def sport_finished_upcoming_tournaments_endpoint(sport_slug: str = "football") -> SofascoreEndpoint:
    normalized_sport_slug = _normalize_sport_slug(sport_slug)
    return SofascoreEndpoint(
        path_template=f"/api/v1/sport/{normalized_sport_slug}/finished-upcoming-tournaments",
        envelope_key="tournaments",
        target_table="api_payload_snapshot",
        notes=f"Finished/upcoming tournament window for {normalized_sport_slug}.",
    )


CATEGORY_UNIQUE_TOURNAMENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/category/{category_id}/unique-tournaments",
    envelope_key="groups",
    target_table="unique_tournament",
    notes=(
        "Category-level unique tournament discovery endpoint. For tennis this can expose a much "
        "wider active tournament set than config/default-unique-tournaments."
    ),
)

DEFAULT_UNIQUE_TOURNAMENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/config/default-unique-tournaments/{country_code}/{sport_slug}",
    envelope_key="uniqueTournaments",
    target_table=None,
    notes=(
        "Configuration endpoint used to discover a curated set of default unique tournaments "
        "for one country/sport combination."
    ),
)

def sport_scheduled_events_endpoint(sport_slug: str = "football") -> SofascoreEndpoint:
    normalized_sport_slug = _normalize_sport_slug(sport_slug)
    return SofascoreEndpoint(
        path_template=f"/api/v1/sport/{normalized_sport_slug}/scheduled-events/{{date}}",
        envelope_key="events",
        target_table="event",
    )


def sport_live_events_endpoint(sport_slug: str = "football") -> SofascoreEndpoint:
    normalized_sport_slug = _normalize_sport_slug(sport_slug)
    return SofascoreEndpoint(
        path_template=f"/api/v1/sport/{normalized_sport_slug}/events/live",
        envelope_key="events",
        target_table="event",
    )


SPORT_FOOTBALL_SCHEDULED_EVENTS_ENDPOINT = sport_scheduled_events_endpoint("football")

SPORT_FOOTBALL_LIVE_EVENTS_ENDPOINT = sport_live_events_endpoint("football")

UNIQUE_TOURNAMENT_FEATURED_EVENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/featured-events",
    envelope_key="featuredEvents",
    target_table="event",
)

UNIQUE_TOURNAMENT_SCHEDULED_EVENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/scheduled-events/{date}",
    envelope_key="events",
    target_table="event",
    notes=(
        "Tournament/day event listing. Particularly useful for tennis, where featured-events can "
        "underrepresent the active draw while scheduled-events exposes the actual daily slate."
    ),
)

UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round_number}",
    envelope_key="events",
    target_table="event",
)

UNIQUE_TOURNAMENT_SEASON_BRACKETS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/brackets",
    envelope_key="brackets",
    target_table="event",
    notes="Bracket/playoff tree payload; event-like nodes are recursively extracted for skeleton event ingestion.",
)


def season_rounds_endpoint() -> SofascoreEndpoint:
    return SofascoreEndpoint(
        path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/rounds",
        envelope_key="rounds",
        target_table="season_round",
    )


def season_cuptrees_endpoint() -> SofascoreEndpoint:
    return SofascoreEndpoint(
        path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/cuptrees",
        envelope_key="cupTrees",
        target_table="season_cup_tree",
        notes="Cup tree / playoff structure payload with nested rounds, blocks and participants.",
    )


UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT = season_rounds_endpoint()

UNIQUE_TOURNAMENT_SEASON_CUPTREES_ENDPOINT = season_cuptrees_endpoint()


COMPETITION_ENDPOINTS = (
    UNIQUE_TOURNAMENT_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASONS_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASON_INFO_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASON_CUPTREES_ENDPOINT,
)


def season_last_events_endpoint() -> SofascoreEndpoint:
    return SofascoreEndpoint(
        path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/last/{page}",
        envelope_key="events",
        target_table="event",
    )


def season_next_events_endpoint() -> SofascoreEndpoint:
    return SofascoreEndpoint(
        path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/next/{page}",
        envelope_key="events",
        target_table="event",
    )


def calendar_months_with_events_endpoint() -> SofascoreEndpoint:
    return SofascoreEndpoint(
        path_template="/api/v1/calendar/unique-tournament/{unique_tournament_id}/season/{season_id}/months-with-events",
        envelope_key="months",
        target_table="api_payload_snapshot",
    )


def category_live_events_count_endpoint() -> SofascoreEndpoint:
    return SofascoreEndpoint(
        path_template="/api/v1/category/{category_id}/events/live-count",
        envelope_key="count",
        target_table="api_payload_snapshot",
    )

def event_list_endpoints(sport_slug: str = "football") -> tuple[SofascoreEndpoint, ...]:
    return (
        sport_scheduled_events_endpoint(sport_slug),
        sport_live_events_endpoint(sport_slug),
        UNIQUE_TOURNAMENT_SCHEDULED_EVENTS_ENDPOINT,
        UNIQUE_TOURNAMENT_FEATURED_EVENTS_ENDPOINT,
        UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT,
        UNIQUE_TOURNAMENT_SEASON_BRACKETS_ENDPOINT,
    )


EVENT_LIST_ENDPOINTS = event_list_endpoints("football")


def sport_local_leaderboard_endpoints(sport_slug: str) -> tuple[SofascoreEndpoint, ...]:
    profile = resolve_sport_profile(sport_slug)
    return tuple(
        endpoint
        for endpoint in (
            unique_tournament_top_players_endpoint(profile.top_players_suffix)
            if profile.top_players_suffix
            else None,
            UNIQUE_TOURNAMENT_TOP_RATINGS_OVERALL_ENDPOINT if profile.include_top_ratings else None,
            unique_tournament_top_players_per_game_endpoint(profile.top_players_per_game_suffix)
            if profile.top_players_per_game_suffix
            else None,
            team_scoped_top_players_endpoint(profile.team_top_players_suffix)
            if profile.team_top_players_suffix
            else None,
            UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_RACE_ENDPOINT if profile.include_player_of_the_season_race else None,
            unique_tournament_top_teams_endpoint(profile.top_teams_suffix) if profile.top_teams_suffix else None,
            UNIQUE_TOURNAMENT_VENUES_ENDPOINT if profile.include_venues else None,
            UNIQUE_TOURNAMENT_GROUPS_ENDPOINT if profile.include_groups else None,
            UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT if profile.include_player_of_the_season else None,
            UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_PERIODS_ENDPOINT if profile.include_team_of_the_week else None,
            UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT if profile.include_team_of_the_week else None,
            UNIQUE_TOURNAMENT_PLAYER_STATISTICS_TYPES_ENDPOINT if profile.include_statistics_types else None,
            UNIQUE_TOURNAMENT_TEAM_STATISTICS_TYPES_ENDPOINT if profile.include_statistics_types else None,
            UNIQUE_TOURNAMENT_TEAM_EVENTS_ENDPOINT if profile.include_team_events else None,
            sport_trending_top_players_endpoint(profile.sport_slug) if profile.include_trending_top_players else None,
        )
        if endpoint is not None
    )


def local_api_endpoints(sport_slugs: tuple[str, ...] = LOCAL_API_SUPPORTED_SPORTS) -> tuple[SofascoreEndpoint, ...]:
    endpoints: list[SofascoreEndpoint] = []
    seen_patterns: set[str] = set()

    def add(endpoint: SofascoreEndpoint) -> None:
        if endpoint.pattern in seen_patterns:
            return
        seen_patterns.add(endpoint.pattern)
        endpoints.append(endpoint)

    for sport_slug in sport_slugs:
        add(sport_date_categories_endpoint(sport_slug))
        add(sport_categories_endpoint(sport_slug))
        add(sport_categories_all_endpoint(sport_slug))
        add(sport_scheduled_tournaments_endpoint(sport_slug))
        for endpoint in event_list_endpoints(sport_slug):
            add(endpoint)
        for endpoint in event_detail_endpoints(sport_slug=sport_slug):
            add(endpoint)
        for endpoint in sport_local_leaderboard_endpoints(sport_slug):
            add(endpoint)

    add(CATEGORY_UNIQUE_TOURNAMENTS_ENDPOINT)

    for endpoint in COMPETITION_ENDPOINTS + STANDINGS_ENDPOINTS + STATISTICS_ENDPOINTS + ENTITIES_ENDPOINTS:
        add(endpoint)

    return tuple(endpoints)

EVENT_DETAIL_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}",
    envelope_key="event",
    target_table="event",
)

EVENT_STATISTICS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/statistics",
    envelope_key="statistics",
    target_table="event_statistic",
)

EVENT_LINEUPS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/lineups",
    envelope_key="home,away",
    target_table="event_lineup",
)
EVENT_INCIDENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/incidents",
    envelope_key="incidents",
    target_table="event_incident",
)

EVENT_BEST_PLAYERS_SUMMARY_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/best-players/summary",
    envelope_key="bestHomeTeamPlayers,bestAwayTeamPlayers,playerOfTheMatch",
    target_table="event_best_player_entry",
    notes="Best-player summary including player of the match / MVP.",
)

EVENT_PLAYER_STATISTICS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/player/{player_id}/statistics",
    envelope_key="player,team,position,statistics,extra",
    target_table="event_player_statistics",
    notes="Per-player event statistics payload for football-style detailed analytics.",
)

EVENT_PLAYER_RATING_BREAKDOWN_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/player/{player_id}/rating-breakdown",
    envelope_key="passes,dribbles,defensive,ball-carries",
    target_table="event_player_rating_breakdown_action",
    notes="Per-player rating-breakdown action feed used to explain Sofascore ratings.",
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

EVENT_COMMENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/comments",
    envelope_key="comments,home,away",
    target_table="event_comment",
)

EVENT_GRAPH_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/graph",
    envelope_key="graphPoints",
    target_table="event_graph",
)

EVENT_GRAPH_SEQUENCE_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/graph/sequence",
    envelope_key="graphPoints",
    target_table="api_payload_snapshot",
)

EVENT_POINT_BY_POINT_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/point-by-point",
    envelope_key="root",
    target_table="tennis_point_by_point",
    notes="Tennis-specific event progression payload.",
)

EVENT_TENNIS_POWER_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/tennis-power",
    envelope_key="tennisPowerRankings",
    target_table="tennis_power",
    notes="Tennis-specific momentum/power payload.",
)

EVENT_BASEBALL_INNINGS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/innings",
    envelope_key="innings",
    target_table="baseball_inning",
    notes="Baseball-specific innings/score progression payload.",
)

EVENT_BASEBALL_PITCHES_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/atbat/{at_bat_id}/pitches",
    envelope_key="pitches",
    target_table="baseball_pitch",
    notes="Baseball-specific at-bat pitch breakdown payload.",
)

EVENT_SHOTMAP_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/shotmap",
    envelope_key="shotmap",
    target_table="shotmap_point",
    notes="Shotmap/event-map style payload used by hockey-style sports.",
)

EVENT_ESPORTS_GAMES_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/esports-games",
    envelope_key="games",
    target_table="esports_game",
    notes="Esports match sub-games payload.",
)

EVENT_HEATMAP_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/heatmap/{team_id}",
    envelope_key="playerPoints,goalkeeperPoints",
    target_table="event_team_heatmap",
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

EVENT_WEATHER_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/weather",
    envelope_key="weather",
    target_table="api_payload_snapshot",
)

EVENT_DETAIL_BASE_ENDPOINTS = (
    EVENT_DETAIL_ENDPOINT,
    EVENT_STATISTICS_ENDPOINT,
    EVENT_LINEUPS_ENDPOINT,
    EVENT_INCIDENTS_ENDPOINT,
    EVENT_MANAGERS_ENDPOINT,
    EVENT_H2H_ENDPOINT,
    EVENT_PREGAME_FORM_ENDPOINT,
    EVENT_VOTES_ENDPOINT,
    EVENT_COMMENTS_ENDPOINT,
    EVENT_GRAPH_ENDPOINT,
    EVENT_GRAPH_SEQUENCE_ENDPOINT,
    EVENT_HEATMAP_ENDPOINT,
    EVENT_ODDS_ALL_ENDPOINT,
    EVENT_ODDS_FEATURED_ENDPOINT,
    EVENT_WINNING_ODDS_ENDPOINT,
    EVENT_WEATHER_ENDPOINT,
)

EVENT_DETAIL_TENNIS_ENDPOINTS = (
    EVENT_POINT_BY_POINT_ENDPOINT,
    EVENT_TENNIS_POWER_ENDPOINT,
)

EVENT_DETAIL_BASEBALL_ENDPOINTS = (
    EVENT_BASEBALL_INNINGS_ENDPOINT,
    EVENT_BASEBALL_PITCHES_ENDPOINT,
)

EVENT_DETAIL_ICE_HOCKEY_ENDPOINTS = (
    EVENT_SHOTMAP_ENDPOINT,
)

EVENT_DETAIL_ESPORTS_ENDPOINTS = (
    EVENT_ESPORTS_GAMES_ENDPOINT,
)

EVENT_DETAIL_FOOTBALL_ANALYTICS_ENDPOINTS = (
    EVENT_BEST_PLAYERS_SUMMARY_ENDPOINT,
    EVENT_PLAYER_STATISTICS_ENDPOINT,
    EVENT_PLAYER_RATING_BREAKDOWN_ENDPOINT,
)


def event_detail_endpoints(*, sport_slug: str | None = None) -> tuple[SofascoreEndpoint, ...]:
    normalized_sport_slug = str(sport_slug or "").strip().lower()
    if normalized_sport_slug == "tennis":
        return EVENT_DETAIL_BASE_ENDPOINTS + EVENT_DETAIL_TENNIS_ENDPOINTS
    if normalized_sport_slug == "baseball":
        return EVENT_DETAIL_BASE_ENDPOINTS + EVENT_DETAIL_BASEBALL_ENDPOINTS
    if normalized_sport_slug == "ice-hockey":
        return EVENT_DETAIL_BASE_ENDPOINTS + EVENT_DETAIL_ICE_HOCKEY_ENDPOINTS
    if normalized_sport_slug == "esports":
        return EVENT_DETAIL_BASE_ENDPOINTS + EVENT_DETAIL_ESPORTS_ENDPOINTS
    if normalized_sport_slug == "football":
        return EVENT_DETAIL_BASE_ENDPOINTS + EVENT_DETAIL_FOOTBALL_ANALYTICS_ENDPOINTS
    return EVENT_DETAIL_BASE_ENDPOINTS


EVENT_DETAIL_ENDPOINTS = event_detail_endpoints()

UNIQUE_TOURNAMENT_STATISTICS_INFO_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/info",
    envelope_key="hideHomeAndAway,teams,statisticsGroups,nationalities",
    target_table="season_statistics_config",
)

UNIQUE_TOURNAMENT_STATISTICS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics",
    query_template=(
        "limit={limit}&offset={offset}&order={order}&accumulation={accumulation}"
        "&group={group}&fields={fields}&filters={filters}"
    ),
    envelope_key="results",
    target_table="season_statistics_snapshot",
    notes="Optional query params are omitted when absent; fields and filters preserve exact Sofascore query grammar.",
)

STATISTICS_ENDPOINTS = (
    UNIQUE_TOURNAMENT_STATISTICS_INFO_ENDPOINT,
    UNIQUE_TOURNAMENT_STATISTICS_ENDPOINT,
)

UNIQUE_TOURNAMENT_STANDINGS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/{scope}",
    envelope_key="standings",
    target_table="standing",
)

TOURNAMENT_STANDINGS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/tournament/{tournament_id}/season/{season_id}/standings/{scope}",
    envelope_key="standings",
    target_table="standing",
)

STANDINGS_ENDPOINTS = (
    UNIQUE_TOURNAMENT_STANDINGS_ENDPOINT,
    TOURNAMENT_STANDINGS_ENDPOINT,
)

TEAM_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/team/{team_id}",
    envelope_key="team",
    target_table="team",
)

PLAYER_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}",
    envelope_key="player",
    target_table="player",
)

MANAGER_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/manager/{manager_id}",
    envelope_key="manager",
    target_table="manager",
)

PLAYER_STATISTICS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}/statistics",
    envelope_key="seasons,typesMap",
    target_table="player_season_statistics",
)

PLAYER_TRANSFER_HISTORY_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}/transfer-history",
    envelope_key="transferHistory",
    target_table="player_transfer_history",
)

PLAYER_STATISTICS_SEASONS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}/statistics/seasons",
    envelope_key="uniqueTournamentSeasons,typesMap",
    target_table="entity_statistics_season",
)

TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/team/{team_id}/team-statistics/seasons",
    envelope_key="uniqueTournamentSeasons,typesMap",
    target_table="entity_statistics_season",
)

TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/team/{team_id}/player-statistics/seasons",
    envelope_key="uniqueTournamentSeasons,typesMap",
    target_table="season_statistics_type",
)

PLAYER_SEASON_OVERALL_STATISTICS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall",
    envelope_key="statistics,team",
    target_table="api_payload_snapshot",
)

TEAM_SEASON_OVERALL_STATISTICS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall",
    envelope_key="statistics",
    target_table="api_payload_snapshot",
)

PLAYER_SEASON_HEATMAP_OVERALL_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/heatmap/overall",
    envelope_key="heatmap,events",
    target_table="api_payload_snapshot",
)

TEAM_PERFORMANCE_GRAPH_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team/{team_id}/team-performance-graph-data",
    envelope_key="graphData",
    target_table="api_payload_snapshot",
)

ENTITIES_ENDPOINTS = (
    TEAM_ENDPOINT,
    PLAYER_ENDPOINT,
    MANAGER_ENDPOINT,
    PLAYER_STATISTICS_ENDPOINT,
    PLAYER_TRANSFER_HISTORY_ENDPOINT,
    PLAYER_STATISTICS_SEASONS_ENDPOINT,
    TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT,
    TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT,
    PLAYER_SEASON_OVERALL_STATISTICS_ENDPOINT,
    TEAM_SEASON_OVERALL_STATISTICS_ENDPOINT,
    PLAYER_SEASON_HEATMAP_OVERALL_ENDPOINT,
    TEAM_PERFORMANCE_GRAPH_ENDPOINT,
)

def unique_tournament_top_players_endpoint(path_suffix: str = "overall") -> SofascoreEndpoint:
    normalized_suffix = _normalize_path_suffix(path_suffix)
    return SofascoreEndpoint(
        path_template=(
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/"
            f"{normalized_suffix}"
        ),
        envelope_key="topPlayers",
        target_table="top_player_snapshot",
    )


UNIQUE_TOURNAMENT_TOP_PLAYERS_OVERALL_ENDPOINT = unique_tournament_top_players_endpoint("overall")

UNIQUE_TOURNAMENT_TOP_RATINGS_OVERALL_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-ratings/overall",
    envelope_key="topPlayers",
    target_table="top_player_snapshot",
)

def unique_tournament_top_players_per_game_endpoint(path_suffix: str = "all/overall") -> SofascoreEndpoint:
    normalized_suffix = _normalize_path_suffix(path_suffix)
    return SofascoreEndpoint(
        path_template=(
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players-per-game/"
            f"{normalized_suffix}"
        ),
        envelope_key="topPlayers",
        target_table="top_player_snapshot",
    )


UNIQUE_TOURNAMENT_TOP_PLAYERS_PER_GAME_ENDPOINT = unique_tournament_top_players_per_game_endpoint("all/overall")

def team_scoped_top_players_endpoint(path_suffix: str = "overall") -> SofascoreEndpoint:
    normalized_suffix = _normalize_path_suffix(path_suffix)
    return SofascoreEndpoint(
        path_template=(
            "/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/"
            f"{normalized_suffix}"
        ),
        envelope_key="topPlayers",
        target_table="top_player_snapshot",
    )


TEAM_SCOPED_TOP_PLAYERS_OVERALL_ENDPOINT = team_scoped_top_players_endpoint("overall")

UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_RACE_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season-race",
    envelope_key="topPlayers,statisticsType",
    target_table="top_player_snapshot",
)

def unique_tournament_top_teams_endpoint(path_suffix: str = "overall") -> SofascoreEndpoint:
    normalized_suffix = _normalize_path_suffix(path_suffix)
    return SofascoreEndpoint(
        path_template=(
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/"
            f"{normalized_suffix}"
        ),
        envelope_key="topTeams",
        target_table="top_team_snapshot",
    )


UNIQUE_TOURNAMENT_TOP_TEAMS_OVERALL_ENDPOINT = unique_tournament_top_teams_endpoint("overall")

UNIQUE_TOURNAMENT_VENUES_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/venues",
    envelope_key="venues",
    target_table="venue",
)

UNIQUE_TOURNAMENT_GROUPS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/groups",
    envelope_key="groups",
    target_table="season_group",
)

UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season",
    envelope_key="player,team,statistics,playerOfTheTournament",
    target_table="season_player_of_the_season",
)

UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_PERIODS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-of-the-week/periods",
    envelope_key="periods",
    target_table="period",
)

UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-of-the-week/{period_id}",
    envelope_key="formation,players",
    target_table="team_of_the_week",
)

UNIQUE_TOURNAMENT_PLAYER_STATISTICS_TYPES_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-statistics/types",
    envelope_key="types",
    target_table="season_statistics_type",
)

UNIQUE_TOURNAMENT_TEAM_STATISTICS_TYPES_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-statistics/types",
    envelope_key="types",
    target_table="season_statistics_type",
)

UNIQUE_TOURNAMENT_TEAM_EVENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/{scope}",
    envelope_key="tournamentTeamEvents",
    target_table="api_payload_snapshot",
)

def sport_trending_top_players_endpoint(sport_slug: str = "football") -> SofascoreEndpoint:
    normalized_sport_slug = _normalize_sport_slug(sport_slug)
    return SofascoreEndpoint(
        path_template=f"/api/v1/sport/{normalized_sport_slug}/trending-top-players",
        envelope_key="topPlayers",
        target_table="api_payload_snapshot",
    )


SPORT_FOOTBALL_TRENDING_TOP_PLAYERS_ENDPOINT = sport_trending_top_players_endpoint("football")

LEADERBOARDS_ENDPOINTS = (
    UNIQUE_TOURNAMENT_TOP_PLAYERS_OVERALL_ENDPOINT,
    UNIQUE_TOURNAMENT_TOP_RATINGS_OVERALL_ENDPOINT,
    UNIQUE_TOURNAMENT_TOP_PLAYERS_PER_GAME_ENDPOINT,
    TEAM_SCOPED_TOP_PLAYERS_OVERALL_ENDPOINT,
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_RACE_ENDPOINT,
    UNIQUE_TOURNAMENT_TOP_TEAMS_OVERALL_ENDPOINT,
    UNIQUE_TOURNAMENT_VENUES_ENDPOINT,
    UNIQUE_TOURNAMENT_GROUPS_ENDPOINT,
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT,
    UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_PERIODS_ENDPOINT,
    UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT,
    UNIQUE_TOURNAMENT_PLAYER_STATISTICS_TYPES_ENDPOINT,
    UNIQUE_TOURNAMENT_TEAM_STATISTICS_TYPES_ENDPOINT,
    UNIQUE_TOURNAMENT_TEAM_EVENTS_ENDPOINT,
    SPORT_FOOTBALL_TRENDING_TOP_PLAYERS_ENDPOINT,
)


def competition_registry_entries() -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for the competition parser family."""

    return tuple(endpoint.registry_entry() for endpoint in COMPETITION_ENDPOINTS)


def categories_seed_registry_entries(*, sport_slug: str = "football") -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for daily category seed discovery."""

    return (sport_date_categories_endpoint(sport_slug).registry_entry(),)


def category_tournament_discovery_registry_entries(
    *,
    sport_slug: str = "football",
) -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for wide category/tournament discovery."""

    return (
        sport_categories_endpoint(sport_slug).registry_entry(),
        sport_categories_all_endpoint(sport_slug).registry_entry(),
        CATEGORY_UNIQUE_TOURNAMENTS_ENDPOINT.registry_entry(),
    )


def scheduled_tournament_discovery_registry_entries(
    *,
    sport_slug: str = "football",
) -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for daily scheduled-tournament discovery."""

    return (sport_scheduled_tournaments_endpoint(sport_slug).registry_entry(),)


def event_list_registry_entries(*, sport_slug: str = "football") -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for the event-list parser family."""

    return tuple(endpoint.registry_entry() for endpoint in event_list_endpoints(sport_slug))


def event_detail_registry_entries(*, sport_slug: str | None = None) -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for the event-detail parser family."""

    return tuple(endpoint.registry_entry() for endpoint in event_detail_endpoints(sport_slug=sport_slug))


def statistics_registry_entries() -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for the statistics parser family."""

    return tuple(endpoint.registry_entry() for endpoint in STATISTICS_ENDPOINTS)


def standings_registry_entries() -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for the standings parser family."""

    return tuple(endpoint.registry_entry() for endpoint in STANDINGS_ENDPOINTS)


def entities_registry_entries() -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for the entities/enrichment parser family."""

    return tuple(endpoint.registry_entry() for endpoint in ENTITIES_ENDPOINTS)


def leaderboards_registry_entries(
    *,
    sport_slug: str = "football",
    top_players_suffix: str = "overall",
    top_players_per_game_suffix: str = "all/overall",
    team_top_players_suffix: str = "overall",
    top_teams_suffix: str = "overall",
) -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows for leaderboard and seasonal aggregate parser family."""

    endpoints = (
        unique_tournament_top_players_endpoint(top_players_suffix),
        UNIQUE_TOURNAMENT_TOP_RATINGS_OVERALL_ENDPOINT,
        unique_tournament_top_players_per_game_endpoint(top_players_per_game_suffix),
        team_scoped_top_players_endpoint(team_top_players_suffix),
        UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_RACE_ENDPOINT,
        unique_tournament_top_teams_endpoint(top_teams_suffix),
        UNIQUE_TOURNAMENT_VENUES_ENDPOINT,
        UNIQUE_TOURNAMENT_GROUPS_ENDPOINT,
        UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT,
        UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_PERIODS_ENDPOINT,
        UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT,
        UNIQUE_TOURNAMENT_PLAYER_STATISTICS_TYPES_ENDPOINT,
        UNIQUE_TOURNAMENT_TEAM_STATISTICS_TYPES_ENDPOINT,
        UNIQUE_TOURNAMENT_TEAM_EVENTS_ENDPOINT,
        sport_trending_top_players_endpoint(sport_slug),
    )
    return tuple(endpoint.registry_entry() for endpoint in endpoints)


def leaderboards_registry_entries_for_sport(sport_slug: str) -> tuple[EndpointRegistryEntry, ...]:
    profile = resolve_sport_profile(sport_slug)
    return tuple(endpoint.registry_entry() for endpoint in sport_local_leaderboard_endpoints(profile.sport_slug))


def hybrid_runtime_registry_entries_for_sport(sport_slug: str) -> tuple[EndpointRegistryEntry, ...]:
    """Registry rows required by the hybrid cutover runtime before workers start fetching."""

    entries: list[EndpointRegistryEntry] = []
    seen_patterns: set[str] = set()

    def add_many(items: tuple[EndpointRegistryEntry, ...]) -> None:
        for item in items:
            if item.pattern in seen_patterns:
                continue
            seen_patterns.add(item.pattern)
            entries.append(item)

    add_many(event_detail_registry_entries(sport_slug=sport_slug))
    add_many(entities_registry_entries())
    add_many(leaderboards_registry_entries_for_sport(sport_slug))
    return tuple(entries)
