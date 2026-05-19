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
    """One exact Sofascore endpoint definition.

    The optional ``refresh_*`` fields drive the generic Resource Refresh Loop
    (see ``schema_inspector.services.resource_planner``). When ``refresh_*``
    fields are ``None`` the endpoint is *not* auto-refreshed; callers must
    fetch it via ad-hoc CLI / structure-sync / live discovery. This default
    keeps every existing endpoint constant binary-compatible -- only
    endpoints that explicitly opt in get periodic refresh.
    """

    path_template: str
    envelope_key: str
    target_table: str | None = None
    query_template: str | None = None
    notes: str | None = None
    source_slug: str = "sofascore"
    contract_version: str = "v1"
    # Resource Refresh Loop metadata (optional, opt-in).
    refresh_interval_seconds: int | None = None
    refresh_priority: int = 50
    scope_kind: str | None = None
    freshness_ttl_seconds: int | None = None
    # Worker-side auto-pagination (Stage 8 / C.4). When ``auto_paginate``
    # is True the resource refresh worker chains page=K+1 immediately on
    # ``hasNextPage=true`` until ``hasNextPage=false`` (initial walk) OR
    # ``max_pages`` (safety fuse). On completion the worker records the
    # entity in ``PaginationDoneStore`` and the planner skips re-walking
    # for ``audit_interval_seconds``. Page=0 keeps refreshing on the
    # endpoint's normal ``refresh_interval_seconds`` cadence regardless.
    auto_paginate: bool = False
    audit_interval_seconds: int | None = None
    max_pages: int = 50
    # D6 empty-data marker. Some endpoints return HTTP 200 with an empty
    # body for entities that have no data yet/anymore (e.g. last-year-summary
    # for retired players). ``empty_predicate`` names a registered predicate
    # in the resource refresh worker that inspects the freshly stored
    # payload and decides whether to mark the entity as "empty". When
    # marked, the planner skips re-publishing for ``empty_data_ttl_seconds``.
    # ``None`` disables the empty-marker path -- safe default for endpoints
    # whose 200 always carries useful data.
    empty_predicate: str | None = None
    empty_data_ttl_seconds: int | None = None
    # P0.2 — HEAD-probe-first opt-in. When ``prefer_head_probe`` is True
    # the FetchExecutor first issues an HTTP HEAD against the resolved
    # URL. Sofascore's HEAD endpoint mirrors GET status reliably (live
    # probe 2026-05-07: 38/38 match) so we can decide whether to issue
    # the body-heavy GET without paying for the response payload. Only
    # endpoints with high "structurally-not-applicable" 404 rates
    # benefit (e.g. /top-players/regularSeason for football, ~1.9 MB
    # GET vs. ~0 byte HEAD). For each pattern with this flag the worker
    # also performs a defence-in-depth ``recent_200`` lookup: if the
    # HEAD says 4xx but ``api_payload_snapshot`` has a real 200 within
    # ``head_probe_recent_200_window_days`` (default 30), we still
    # issue the GET in case the HEAD endpoint is temporarily lying.
    prefer_head_probe: bool = False

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
    # D13.2: globalised. RoundOfRegistryFootballResolver expands every
    # registry-active football (ut, season) pair into (ut, season,
    # round_number) triples by parsing the cached /rounds snapshot, and
    # filters to ``round_number < currentRound`` so we never fetch the
    # round still in progress. Refresh weekly — round results don't
    # change once played.
    refresh_interval_seconds=7 * 24 * 3600,
    refresh_priority=65,
    scope_kind="round-of-registry-football",
    freshness_ttl_seconds=6 * 24 * 3600,
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
        notes=(
            "Cup tree / playoff structure payload with nested rounds, blocks and participants. "
            "Pre-D5 probe: upstream returns 200 only for tournaments that actually carry a cup "
            "tree (UCL, FA Cup, DFB Pokal, Coppa Italia, Copa del Rey, Supercoppa…) and 404 for "
            "league tournaments (LaLiga, PL, Serie A, Saudi PL, MLS…). The local field "
            "`unique_tournament.has_playoff_series` is unreliable as a gate (Sofascore returns "
            "false for cup tournaments too), so we lean on ResourceNegativeCache: leagues hit "
            "404 once and the negative cache suppresses re-publishing for 7 days. After the "
            "first sweep only the actual cup tournaments contribute traffic."
        ),
        # D5: weekly refresh, low priority. Scope is the broad registry-driven
        # (ut, season) set so cup tournaments without standings still land here.
        refresh_interval_seconds=7 * 24 * 3600,
        refresh_priority=70,
        scope_kind="season-of-registry-ut",
        freshness_ttl_seconds=6 * 24 * 3600,
    )


UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT = season_rounds_endpoint()

UNIQUE_TOURNAMENT_SEASON_CUPTREES_ENDPOINT = season_cuptrees_endpoint()


UNIQUE_TOURNAMENT_MEDIA_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/unique-tournament/{unique_tournament_id}/media",
    envelope_key="media",
    target_table="api_payload_snapshot",
    notes=(
        "Synthetic league media feed aggregated from already-ingested "
        "/api/v1/event/{event_id}/highlights snapshots. DB-only, no upstream fetch."
    ),
)

COMPETITION_ENDPOINTS = (
    UNIQUE_TOURNAMENT_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASONS_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASON_INFO_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASON_CUPTREES_ENDPOINT,
    UNIQUE_TOURNAMENT_MEDIA_ENDPOINT,
)


def season_last_events_endpoint() -> SofascoreEndpoint:
    return SofascoreEndpoint(
        path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/last/{page}",
        envelope_key="events",
        target_table="event",
        # Stage 4: opt into Resource Refresh Loop. Page=0 is auto-refreshed
        # every 30 minutes for the (ut, season) pairs produced by
        # SeasonOfActiveUTEventsResolver. Generic _fetch_snapshot_payload
        # then serves the raw upstream payload (1:1 with sofascore.com)
        # once the refresh worker has populated api_payload_snapshot.
        refresh_interval_seconds=30 * 60,
        refresh_priority=45,
        scope_kind="season-of-active-ut-events",
        freshness_ttl_seconds=25 * 60,
    )


def season_next_events_endpoint() -> SofascoreEndpoint:
    return SofascoreEndpoint(
        path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/next/{page}",
        envelope_key="events",
        target_table="event",
        refresh_interval_seconds=30 * 60,
        refresh_priority=45,
        scope_kind="season-of-active-ut-events",
        freshness_ttl_seconds=25 * 60,
    )


def team_last_events_endpoint() -> SofascoreEndpoint:
    return SofascoreEndpoint(
        path_template="/api/v1/team/{team_id}/events/last/{page}",
        envelope_key="events",
        target_table="event",
        # Phase 1 (2026-05-19, docs/REDUNDANT_ENDPOINTS_AUDIT.md §A.1):
        # refresh disabled. ``target_table="event"`` means every row this
        # endpoint produces is already provided by the canonical UT-level
        # discovery path (``unique-tournament/{ut}/season/{s}/events/...``)
        # which the historical/scheduled planners already walk. Stage 5's
        # 6h team-of-active-ut-first-page refresh added ~160 redundant HTTP
        # per EPL-sized league per day with zero unique event rows. The
        # endpoint constant is preserved so on-demand callers (CLI,
        # team_detail_cli, local API passthrough for legacy frontend probes)
        # still resolve, but the Resource Refresh Loop no longer picks it up.
    )


def team_next_events_endpoint() -> SofascoreEndpoint:
    return SofascoreEndpoint(
        path_template="/api/v1/team/{team_id}/events/next/{page}",
        envelope_key="events",
        target_table="event",
        # Phase 1 (2026-05-19): refresh disabled — see team_last_events_endpoint
        # for the full rationale.
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


SPORT_ALL_EVENT_COUNT_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/sport/0/event-count",
    envelope_key="sports",
    target_table="event",
    notes="Local synthetic route: current-day total/live event counts grouped by sport from PostgreSQL.",
)


def event_list_endpoints(sport_slug: str = "football") -> tuple[SofascoreEndpoint, ...]:
    return (
        sport_scheduled_events_endpoint(sport_slug),
        sport_live_events_endpoint(sport_slug),
        UNIQUE_TOURNAMENT_SCHEDULED_EVENTS_ENDPOINT,
        UNIQUE_TOURNAMENT_FEATURED_EVENTS_ENDPOINT,
        UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT,
        UNIQUE_TOURNAMENT_SEASON_BRACKETS_ENDPOINT,
        season_last_events_endpoint(),
        season_next_events_endpoint(),
        team_last_events_endpoint(),
        team_next_events_endpoint(),
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

    add(SPORT_ALL_EVENT_COUNT_ENDPOINT)
    add(CATEGORY_UNIQUE_TOURNAMENTS_ENDPOINT)

    # Phase 2.1 (2026-05-19, docs/REDUNDANT_ENDPOINTS_AUDIT.md §H):
    # ``/calendar/.../months-with-events`` was a phantom endpoint constant
    # — declared but never registered. It is now synthesized from
    # ``event.start_timestamp`` in the local API server; registering it
    # here puts the route under the same dispatcher as every other
    # endpoint and prevents 404s for clients that ask Sofascore-style.
    add(calendar_months_with_events_endpoint())

    for endpoint in COMPETITION_ENDPOINTS + STANDINGS_ENDPOINTS + STATISTICS_ENDPOINTS + ENTITIES_ENDPOINTS:
        add(endpoint)

    # D11: keep deprecated/empirically-broken endpoints routable in
    # Swagger / route registry (so existing client code doesn't break)
    # but DON'T put them into ``event_detail_endpoints`` -> they no
    # longer participate in the hydrate-worker per-event fetch loop.
    # See ``EVENT_DETAIL_DEPRECATED_ENDPOINTS`` for the concrete list.
    for endpoint in EVENT_DETAIL_DEPRECATED_ENDPOINTS:
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

EVENT_HIGHLIGHTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/highlights",
    envelope_key="highlights",
    target_table="api_payload_snapshot",
    notes="Event highlight clips/media feed when the root payload advertises hasGlobalHighlights.",
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

EVENT_H2H_EVENTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{custom_id}/h2h/events",
    envelope_key="events",
    target_table="api_payload_snapshot",
    notes=(
        "CustomId-based H2H events feed. Path uses {custom_id} (string) "
        "rather than {event_id} — only endpoint in registry that does so. "
        "D13.2: CustomIdOfRegistryEventsResolver yields custom_id targets "
        "for every finished football event in the rolling event window "
        "whose tournament is active+current_enabled in tournament_registry."
    ),
    refresh_interval_seconds=7 * 24 * 3600,
    refresh_priority=70,
    scope_kind="custom-id-of-registry-events",
    freshness_ttl_seconds=6 * 24 * 3600,
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

EVENT_OFFICIAL_TWEETS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/official-tweets",
    envelope_key="tweets",
    target_table="api_payload_snapshot",
)

EVENT_HIGHLIGHTS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/highlights",
    envelope_key="highlights",
    target_table="api_payload_snapshot",
    notes="Event highlights payload gated by hasGlobalHighlights and finished-match age.",
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

EVENT_AVERAGE_POSITIONS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/average-positions",
    envelope_key="players",
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

# P0.2 — /innings is cricket-only on prod (live probe 2026-05-07 confirmed
# baseball /innings returns 100% soft-error 404; cricket events return real
# innings payload with shape {innings: [{number, battingTeam, bowlingTeam,
# score, wickets, overs, extra, wide, noBall, bye, legBye, penalty}]}, which
# is incompatible with BaseballInningsParser's (event_id, ordinal, inning,
# home_score, away_score) schema). Stored as raw passthrough until a proper
# CricketInningsParser is wired. ``baseball_inning`` table is preserved for
# legacy snapshots; classifier remains sport-aware so a stray baseball
# /innings 200 (if upstream restores the route) can still parse via
# BaseballInningsParser.
EVENT_INNINGS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/innings",
    envelope_key="innings",
    target_table="api_payload_snapshot",
    notes="Cricket innings payload (raw passthrough). Baseball events 404 here.",
    # P0.2 — HEAD probe gates the body fetch; baseball events HEAD=404
    # so we skip GET entirely.
    prefer_head_probe=True,
)
# Backwards-compat alias — many call sites still import the old symbol.
EVENT_BASEBALL_INNINGS_ENDPOINT = EVENT_INNINGS_ENDPOINT

EVENT_BASEBALL_AT_BATS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/at-bats",
    envelope_key="atBats",
    target_table="api_payload_snapshot",
    notes=(
        "Baseball game at-bats list; parent of /atbat/{at_bat_id}/pitches. Raw passthrough. "
        "Pre-D2 probe: 200 OK for both finished (status_code=100/110) AND inprogress baseball "
        "events (status_code=23/24/29/30 — innings/pause); the live payload grows as the game "
        "progresses. Scope covers BOTH finished and live games. Refresh interval is 10 minutes "
        "so live games get sub-10-minute lag; finished games are deduped by FreshnessStore until "
        "they drop out of the rolling baseball window."
    ),
    # D7: refresh every 10 min, freshness_ttl 8 min so a duplicate publish
    # within the same window gets dedup'd. For finished events the snapshot
    # is stable -> FreshnessStore blocks; for live events the snapshot
    # legitimately changes between refreshes.
    refresh_interval_seconds=10 * 60,
    refresh_priority=55,
    scope_kind="event-of-active-baseball",
    freshness_ttl_seconds=8 * 60,
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

EVENT_PLAYER_SHOTMAP_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/shotmap/player/{player_id}",
    envelope_key="shotmap",
    target_table="api_payload_snapshot",
)

EVENT_GOALKEEPER_SHOTMAP_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/goalkeeper-shotmap/player/{player_id}",
    envelope_key="shotmap",
    target_table="api_payload_snapshot",
    prefer_head_probe=True,  # P0.2 — only goalkeepers; HEAD gates body
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

EVENT_PLAYER_HEATMAP_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/player/{player_id}/heatmap",
    envelope_key="heatmap",
    target_table="api_payload_snapshot",
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

EVENT_TEAM_STREAKS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/team-streaks",
    envelope_key="streaks",
    target_table="api_payload_snapshot",
)

EVENT_TEAM_STREAKS_BETTING_ODDS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/event/{event_id}/team-streaks/betting-odds/{provider_id}",
    envelope_key="home,away",
    target_table="api_payload_snapshot",
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
    EVENT_HIGHLIGHTS_ENDPOINT,
    EVENT_MANAGERS_ENDPOINT,
    EVENT_H2H_ENDPOINT,
    EVENT_PREGAME_FORM_ENDPOINT,
    EVENT_VOTES_ENDPOINT,
    EVENT_COMMENTS_ENDPOINT,
    EVENT_GRAPH_ENDPOINT,
    EVENT_HEATMAP_ENDPOINT,
    EVENT_ODDS_ALL_ENDPOINT,
    EVENT_ODDS_FEATURED_ENDPOINT,
    EVENT_WINNING_ODDS_ENDPOINT,
    # D11 removed from BASE: EVENT_GRAPH_SEQUENCE_ENDPOINT and
    # EVENT_WEATHER_ENDPOINT. Pre-D11 audit established that both return
    # 100% 4xx for football, baseball, ice-hockey, tennis events that we
    # tested (652/652 4xx for the 2-league force-fetch and 0 successful
    # snapshots in the entire DB across all sports). Keeping them in
    # BASE caused a guaranteed 404 per event per hydrate cycle — wasted
    # proxy budget. They remain reachable for Swagger via
    # ``EVENT_DETAIL_DEPRECATED_ENDPOINTS`` so frontend probing still
    # works; only the auto-fetch chain is severed.
)

EVENT_DETAIL_DEPRECATED_ENDPOINTS = (
    EVENT_GRAPH_SEQUENCE_ENDPOINT,
    EVENT_WEATHER_ENDPOINT,
)

EVENT_DETAIL_TENNIS_ENDPOINTS = (
    EVENT_POINT_BY_POINT_ENDPOINT,
    EVENT_TENNIS_POWER_ENDPOINT,
)

EVENT_DETAIL_BASEBALL_ENDPOINTS = (
    EVENT_BASEBALL_AT_BATS_ENDPOINT,
    EVENT_BASEBALL_PITCHES_ENDPOINT,
)

EVENT_DETAIL_CRICKET_ENDPOINTS = (
    EVENT_INNINGS_ENDPOINT,
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
    EVENT_H2H_EVENTS_ENDPOINT,
    EVENT_OFFICIAL_TWEETS_ENDPOINT,
    EVENT_AVERAGE_POSITIONS_ENDPOINT,
    EVENT_PLAYER_SHOTMAP_ENDPOINT,
    EVENT_GOALKEEPER_SHOTMAP_ENDPOINT,
    EVENT_PLAYER_HEATMAP_ENDPOINT,
    EVENT_TEAM_STREAKS_ENDPOINT,
    EVENT_TEAM_STREAKS_BETTING_ODDS_ENDPOINT,
)


def event_detail_endpoints(*, sport_slug: str | None = None) -> tuple[SofascoreEndpoint, ...]:
    normalized_sport_slug = str(sport_slug or "").strip().lower()
    if normalized_sport_slug == "tennis":
        return EVENT_DETAIL_BASE_ENDPOINTS + EVENT_DETAIL_TENNIS_ENDPOINTS
    if normalized_sport_slug == "baseball":
        return EVENT_DETAIL_BASE_ENDPOINTS + EVENT_DETAIL_BASEBALL_ENDPOINTS
    if normalized_sport_slug == "cricket":
        return EVENT_DETAIL_BASE_ENDPOINTS + EVENT_DETAIL_CRICKET_ENDPOINTS
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
    # Stage 4: opt-in. SeasonOfActiveUTStandingsResolver fans out per (ut,
    # season) pair into three targets (scope ∈ total/home/away), so a single
    # endpoint pattern covers all three buckets via per-target cursors.
    refresh_interval_seconds=30 * 60,
    refresh_priority=50,
    scope_kind="season-of-active-ut-standings",
    freshness_ttl_seconds=25 * 60,
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
    # Stage C.1: refresh once per day for every player on a recently-fetched
    # active-squad team roster. PlayerOfActiveSquadResolver derives the scope
    # from successful /api/v1/team/{team_id}/players snapshots so we never
    # touch player_ids attached to ghost / stale team rosters.
    refresh_interval_seconds=24 * 3600,
    refresh_priority=50,
    scope_kind="player-of-active-squad",
    freshness_ttl_seconds=22 * 3600,
)

PLAYER_TRANSFER_HISTORY_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}/transfer-history",
    envelope_key="transferHistory",
    target_table="player_transfer_history",
    # Stage C.2: transfers change rarely; once per week is plenty. Same
    # active-squad scope as PLAYER_STATISTICS so any player on a recently
    # ingested team roster gets a fresh transfer history.
    refresh_interval_seconds=7 * 24 * 3600,
    refresh_priority=60,
    scope_kind="player-of-active-squad",
    freshness_ttl_seconds=6 * 24 * 3600,
)

PLAYER_STATISTICS_SEASONS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}/statistics/seasons",
    envelope_key="uniqueTournamentSeasons,typesMap",
    target_table="entity_statistics_season",
    # D9: opt into Resource Refresh Loop. Was previously fed only by the
    # legacy structure-sync path so coverage was sparse (697 snapshots
    # vs 51k active players). Same active-squad scope as /statistics so
    # any player on a recently-fetched team roster gets auto-refreshed.
    refresh_interval_seconds=7 * 24 * 3600,
    refresh_priority=60,
    scope_kind="player-of-active-squad",
    freshness_ttl_seconds=6 * 24 * 3600,
)

PLAYER_STATISTICS_MATCH_TYPE_OVERALL_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}/statistics/match-type/overall",
    envelope_key="seasons,typesMap",
    target_table="api_payload_snapshot",
    notes=(
        "Per-match-type aggregated season statistics. Raw passthrough. Pre-D2 probe: "
        "200 OK for football, basketball (extra `all` key) and baseball; 404 for handball, "
        "ice-hockey, rugby. The active-squad scope below is sport-agnostic; the negative "
        "cache absorbs the 4xx for non-applicable sports after the first attempt."
    ),
    # D2: ~100 KB per player; same active-squad scope as /statistics. Retired
    # players return 4xx upstream — ResourceNegativeCache catches them after
    # the first 404 and skips re-publishing for 7 days.
    refresh_interval_seconds=24 * 3600,
    refresh_priority=55,
    scope_kind="player-of-active-squad",
    freshness_ttl_seconds=22 * 3600,
)

PLAYER_NATIONAL_TEAM_STATISTICS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}/national-team-statistics",
    envelope_key="statistics",
    target_table="api_payload_snapshot",
    notes=(
        "Player career national-team aggregate. Raw passthrough; small payload (~600B "
        "with data, 17 B empty). Pre-D2 probe: 200 always, even for retired legends and "
        "players without national-team history (empty `statistics: []`). Scope is the "
        "broad active-squad set — D6 empty-data marker (30 days) absorbs the ~30% "
        "empty-body responses so the cost stays bounded while celebrity players whose "
        "national lineups are not yet hydrated (e.g. CR7) still get covered."
    ),
    # D8: switched scope from player-of-national-team-history (narrow,
    # ~8k) to player-of-active-squad (broad, ~51k) so well-known players
    # whose national-team match lineups have not been hydrated yet are
    # still picked up. The D6 EmptyDataMarker keeps the empty-body
    # responses from re-firing weekly.
    refresh_interval_seconds=7 * 24 * 3600,
    refresh_priority=70,
    scope_kind="player-of-active-squad",
    freshness_ttl_seconds=6 * 24 * 3600,
    empty_predicate="national_team_statistics",
    empty_data_ttl_seconds=30 * 24 * 3600,
)

PLAYER_LAST_YEAR_SUMMARY_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}/last-year-summary",
    envelope_key="summary,uniqueTournamentsMap",
    target_table="api_payload_snapshot",
    notes=(
        "Player rolling 12-month per-tournament summary. Pre-D2 probe: 200 always, but "
        "empty body (~40 B, summary=[], uniqueTournamentsMap={}) for ~70% of the active "
        "squad scope (older actives, retired, youth/reserves). D6 empty-data marker is "
        "REQUIRED here — without it ~35k useless 40 B requests would fire daily."
    ),
    # D2: rolling-12-month feed; refresh daily. Retired players return 200
    # with empty arrays — D6 empty-data marker absorbs those.
    refresh_interval_seconds=24 * 3600,
    refresh_priority=55,
    scope_kind="player-of-active-squad",
    freshness_ttl_seconds=22 * 3600,
    # D6: marker TTL = 14 days. Shorter than national-team-statistics
    # because rolling-12mo windows can refill faster (a junior player
    # getting their first senior appearance, an older player coming back).
    empty_predicate="last_year_summary",
    empty_data_ttl_seconds=14 * 24 * 3600,
)

TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/team/{team_id}/team-statistics/seasons",
    envelope_key="uniqueTournamentSeasons,typesMap",
    target_table="entity_statistics_season",
)

TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/team/{team_id}/player-statistics/seasons",
    envelope_key="uniqueTournamentSeasons,typesMap",
    target_table="team_player_statistics_season",
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

TEAM_SEASON_GOAL_DISTRIBUTIONS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/goal-distributions",
    envelope_key="goalDistributions",
    target_table="api_payload_snapshot",
    notes="Football team per-season goal distribution by minute buckets. Raw passthrough.",
    # D2: small payload (~1.7 KB), refresh once per day. Scope is the same
    # standings-driven (team, ut, season) triple the active leagues use.
    refresh_interval_seconds=24 * 3600,
    refresh_priority=55,
    scope_kind="team-of-active-ut-season",
    freshness_ttl_seconds=22 * 3600,
    prefer_head_probe=True,  # P0.2 — 96.3% soft-error on prod
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

# --- Raw-passthrough D-category endpoints. ---
# These have no equivalent normalized table either because the upstream payload
# is sofascore-derived synthetic data we cannot reconstruct (featured-players,
# attribute-overviews) or because the normalized projection would lose
# upstream-only fields (team/players roster sections, player events
# playedForTeamMap / statisticsMap). target_table="api_payload_snapshot"
# signals the generic handler to skip and defer to a specialized passthrough
# handler in local_api_server.

TEAM_PLAYERS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/team/{team_id}/players",
    envelope_key="players,foreignPlayers,nationalPlayers",
    target_table="api_payload_snapshot",
    notes=(
        "Sofascore team roster. Raw passthrough -- preserves foreign/national player subsets. "
        "Scope source: D3 registry-driven resolver (event window in registry-active UTs) so "
        "leagues without recent standings ingest (Saudi Pro League, MLS, qualifiers, cups) "
        "are still covered. Pilot list controlled by "
        "SCHEMA_INSPECTOR_RESOURCE_REGISTRY_PILOT_UTS; empty env = full registry."
    ),
    # D3 activation: switched from "team-of-active-ut" (standings-driven) to
    # "team-of-registry-ut" (event-window over tournament_registry). The
    # registry resolver is wider — it covers Saudi/MLS/qualifiers/cups whose
    # standings worker is currently parked under historical_enrichment
    # backpressure. The standings-driven resolver is still registered and
    # used by sibling endpoints (FEATURED_PLAYERS, TRANSFERS) so the rollout
    # is one endpoint at a time.
    refresh_interval_seconds=12 * 3600,
    refresh_priority=40,
    scope_kind="team-of-registry-ut",
    freshness_ttl_seconds=11 * 3600,
)

TEAM_FEATURED_PLAYERS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/team/{team_id}/featured-players",
    envelope_key="featuredPlayers",
    target_table="api_payload_snapshot",
    notes="Sofascore editorial featured players keyed by event id. Raw passthrough only.",
    # Stage 5: featured-players is an editorial pick keyed by upcoming/recent
    # eventId. The set rotates every match, so 24h cadence keeps it fresh
    # without flooding (most teams play 1-2 matches per week).
    refresh_interval_seconds=24 * 3600,
    refresh_priority=55,
    scope_kind="team-of-active-ut",
    freshness_ttl_seconds=22 * 3600,
    prefer_head_probe=True,  # P0.2 — 94.5% soft-error on prod
)


TEAM_TRANSFERS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/team/{team_id}/transfers",
    envelope_key="transfersIn,transfersOut",
    target_table="api_payload_snapshot",
    notes="Sofascore team transfer history (in + out). No pagination, no query params. Raw passthrough.",
    # Stage 5: transfers change only inside transfer windows -- 30 days is
    # plenty. freshness_ttl 25d so a stray duplicate publish from the
    # planner gets deduped at FreshnessStore before hitting upstream.
    refresh_interval_seconds=30 * 24 * 3600,
    refresh_priority=65,
    scope_kind="team-of-active-ut",
    freshness_ttl_seconds=25 * 24 * 3600,
)

PLAYER_ATTRIBUTE_OVERVIEWS_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}/attribute-overviews",
    envelope_key="playerAttributeOverviews,averageAttributeOverviews",
    target_table="api_payload_snapshot",
    notes="Sofascore-derived synthetic attribute scores. Raw passthrough only.",
    # Stage C.2: ratings refresh daily; same active-squad scope as
    # PLAYER_STATISTICS. Sofascore-derived synthetic, no normalized fallback.
    refresh_interval_seconds=24 * 3600,
    refresh_priority=55,
    scope_kind="player-of-active-squad",
    freshness_ttl_seconds=22 * 3600,
)

PLAYER_EVENTS_LAST_ENDPOINT = SofascoreEndpoint(
    path_template="/api/v1/player/{player_id}/events/last/{page}",
    envelope_key="events,hasNextPage",
    target_table="api_payload_snapshot",
    notes="Player's recent events with pagination. Raw passthrough preserves playedForTeamMap and statisticsMap.",
    # Stage C.3: page=0 auto-refreshes every 6h (planner) -- this is what
    # the frontend "Matches" tab reads.
    refresh_interval_seconds=6 * 3600,
    refresh_priority=45,
    scope_kind="player-of-active-squad-first-page",
    freshness_ttl_seconds=5 * 3600,
    # Phase 1 (2026-05-19, docs/REDUNDANT_ENDPOINTS_AUDIT.md §A.2):
    # auto-pagination disabled. ``page>=1`` is fully derivable from
    # ``event_player_statistics JOIN event``: ``events[]`` are already in
    # ``event``, ``playedForTeamMap`` from ``event_player_statistics.team_id``,
    # ``statisticsMap`` from ``event_player_statistics`` rows. Chaining 50
    # pages × 500 active players × every 6h was ~10K HTTP/day per
    # EPL-sized league for zero new event_id rows. The local API server
    # already synthesizes pages>=1 via ``scheduled_events_synthesizer``
    # ``_FETCH_QUERY_PLAYER_EVENTS_LAST`` -- the worker chain was redundant.
    #
    # Stage 8 / C.4 chain mechanism stays in the worker code (generic, no
    # endpoint references it now) so any future endpoint can opt back in.
    # PaginationDoneStore entries written before this change become inert
    # because no chain ever consults them again.
)

ENTITIES_ENDPOINTS = (
    TEAM_ENDPOINT,
    PLAYER_ENDPOINT,
    MANAGER_ENDPOINT,
    PLAYER_STATISTICS_ENDPOINT,
    PLAYER_TRANSFER_HISTORY_ENDPOINT,
    PLAYER_STATISTICS_SEASONS_ENDPOINT,
    PLAYER_STATISTICS_MATCH_TYPE_OVERALL_ENDPOINT,
    PLAYER_NATIONAL_TEAM_STATISTICS_ENDPOINT,
    PLAYER_LAST_YEAR_SUMMARY_ENDPOINT,
    TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT,
    TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT,
    PLAYER_SEASON_OVERALL_STATISTICS_ENDPOINT,
    TEAM_SEASON_OVERALL_STATISTICS_ENDPOINT,
    TEAM_SEASON_GOAL_DISTRIBUTIONS_ENDPOINT,
    PLAYER_SEASON_HEATMAP_OVERALL_ENDPOINT,
    TEAM_PERFORMANCE_GRAPH_ENDPOINT,
    TEAM_PLAYERS_ENDPOINT,
    TEAM_FEATURED_PLAYERS_ENDPOINT,
    TEAM_TRANSFERS_ENDPOINT,
    PLAYER_ATTRIBUTE_OVERVIEWS_ENDPOINT,
    PLAYER_EVENTS_LAST_ENDPOINT,
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
        # P0.2 — body is huge (1.9 MB on PL active season) and 80%+ of
        # sub-tier (ut, season) pairs return 4xx. HEAD probe gates the
        # body fetch.
        prefer_head_probe=True,
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
        prefer_head_probe=True,  # P0.2 — same rationale as top-players
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
        prefer_head_probe=True,  # P0.2
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
        prefer_head_probe=True,  # P0.2
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
    # P0.2 — POS endpoint returns 200 only after season completes (live
    # probe: PL 24/25, PL 23/24 → 200; PL 25/26 active → 404). Most
    # active-season probes are wasted; HEAD-probe gating eliminates the
    # body fetch on those.
    prefer_head_probe=True,
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
    # D13.2: globalised. PeriodOfRegistryFootballResolver yields (ut,
    # season, period_id) triples for every registry-active football
    # league whose `period` table has rows. Combined with the D13.3
    # Smart-404 blacklist (hash:resource_refresh:totw_season_status) the
    # planner skips seasons proven not to support ToTW so we don't waste
    # a request per (ut, season) on the 95% that 404.
    refresh_interval_seconds=14 * 24 * 3600,
    refresh_priority=75,
    scope_kind="period-of-registry-football",
    freshness_ttl_seconds=13 * 24 * 3600,
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
