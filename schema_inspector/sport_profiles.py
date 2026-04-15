"""Sport-specific endpoint and stage defaults for Sofascore ETL flows."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SportProfile:
    sport_slug: str
    standings_scopes: tuple[str, ...] = ("total",)
    team_event_scopes: tuple[str, ...] = ("total",)
    top_players_suffix: str | None = None
    top_players_per_game_suffix: str | None = None
    team_top_players_suffix: str | None = None
    top_teams_suffix: str | None = None
    include_top_ratings: bool = False
    include_player_of_the_season_race: bool = False
    include_player_of_the_season: bool = False
    include_venues: bool = False
    include_groups: bool = False
    include_team_of_the_week: bool = False
    include_statistics_types: bool = False
    include_team_events: bool = False
    include_trending_top_players: bool = False
    use_daily_categories_seed: bool = False
    use_scheduled_tournaments: bool = False
    discovery_category_seed_ids: tuple[int, ...] = ()
    include_categories_all_discovery: bool = False


FOOTBALL_PROFILE = SportProfile(
    sport_slug="football",
    standings_scopes=("total", "home", "away"),
    team_event_scopes=("home", "away", "total"),
    top_players_suffix="overall",
    top_players_per_game_suffix="all/overall",
    team_top_players_suffix="overall",
    top_teams_suffix="overall",
    include_top_ratings=True,
    include_player_of_the_season_race=True,
    include_player_of_the_season=True,
    include_venues=True,
    include_groups=True,
    include_team_of_the_week=True,
    include_statistics_types=True,
    include_team_events=True,
    include_trending_top_players=True,
    use_daily_categories_seed=True,
)


BASKETBALL_PROFILE = SportProfile(
    sport_slug="basketball",
    standings_scopes=("total", "home", "away"),
    team_event_scopes=("total",),
    top_players_suffix="regularSeason",
    top_players_per_game_suffix="all/regularSeason",
    team_top_players_suffix="overall",
    top_teams_suffix="regularSeason",
    include_top_ratings=False,
    include_player_of_the_season_race=False,
    include_player_of_the_season=True,
    include_venues=True,
    include_groups=True,
    include_team_of_the_week=True,
    include_statistics_types=True,
    include_team_events=True,
    include_trending_top_players=True,
    use_scheduled_tournaments=True,
)


TENNIS_PROFILE = SportProfile(
    sport_slug="tennis",
    standings_scopes=(),
    team_event_scopes=(),
    top_players_suffix=None,
    top_players_per_game_suffix=None,
    team_top_players_suffix=None,
    top_teams_suffix=None,
    include_top_ratings=False,
    include_player_of_the_season_race=False,
    include_player_of_the_season=False,
    include_venues=False,
    include_groups=False,
    include_team_of_the_week=False,
    include_statistics_types=False,
    include_team_events=False,
    include_trending_top_players=False,
    use_scheduled_tournaments=True,
    discovery_category_seed_ids=(-101,),
    include_categories_all_discovery=False,
)


CRICKET_PROFILE = SportProfile(
    sport_slug="cricket",
    standings_scopes=("total",),
    team_event_scopes=("total",),
    top_players_suffix=None,
    top_players_per_game_suffix=None,
    team_top_players_suffix=None,
    top_teams_suffix=None,
    include_top_ratings=False,
    include_player_of_the_season_race=False,
    include_player_of_the_season=False,
    include_venues=False,
    include_groups=False,
    include_team_of_the_week=False,
    include_statistics_types=False,
    include_team_events=False,
    include_trending_top_players=False,
    use_scheduled_tournaments=True,
)


GENERIC_PROFILE = SportProfile(
    sport_slug="generic",
    standings_scopes=("total",),
    team_event_scopes=("total",),
    top_players_suffix="overall",
    top_players_per_game_suffix="all/overall",
    team_top_players_suffix="overall",
    top_teams_suffix="overall",
    include_top_ratings=False,
    include_player_of_the_season_race=False,
    include_player_of_the_season=True,
    include_venues=True,
    include_groups=True,
    include_team_of_the_week=False,
    include_statistics_types=True,
    include_team_events=True,
    include_trending_top_players=False,
    use_scheduled_tournaments=True,
)


_PROFILES: dict[str, SportProfile] = {
    FOOTBALL_PROFILE.sport_slug: FOOTBALL_PROFILE,
    BASKETBALL_PROFILE.sport_slug: BASKETBALL_PROFILE,
    TENNIS_PROFILE.sport_slug: TENNIS_PROFILE,
    CRICKET_PROFILE.sport_slug: CRICKET_PROFILE,
}


def normalize_sport_slug(sport_slug: str) -> str:
    value = str(sport_slug).strip().lower()
    if not value:
        raise ValueError("sport_slug cannot be empty")
    return value


def resolve_sport_profile(sport_slug: str) -> SportProfile:
    normalized = normalize_sport_slug(sport_slug)
    if normalized in _PROFILES:
        return _PROFILES[normalized]
    return SportProfile(
        sport_slug=normalized,
        standings_scopes=GENERIC_PROFILE.standings_scopes,
        team_event_scopes=GENERIC_PROFILE.team_event_scopes,
        top_players_suffix=GENERIC_PROFILE.top_players_suffix,
        top_players_per_game_suffix=GENERIC_PROFILE.top_players_per_game_suffix,
        team_top_players_suffix=GENERIC_PROFILE.team_top_players_suffix,
        top_teams_suffix=GENERIC_PROFILE.top_teams_suffix,
        include_top_ratings=GENERIC_PROFILE.include_top_ratings,
        include_player_of_the_season_race=GENERIC_PROFILE.include_player_of_the_season_race,
        include_player_of_the_season=GENERIC_PROFILE.include_player_of_the_season,
        include_venues=GENERIC_PROFILE.include_venues,
        include_groups=GENERIC_PROFILE.include_groups,
        include_team_of_the_week=GENERIC_PROFILE.include_team_of_the_week,
        include_statistics_types=GENERIC_PROFILE.include_statistics_types,
        include_team_events=GENERIC_PROFILE.include_team_events,
        include_trending_top_players=GENERIC_PROFILE.include_trending_top_players,
        use_daily_categories_seed=GENERIC_PROFILE.use_daily_categories_seed,
        use_scheduled_tournaments=GENERIC_PROFILE.use_scheduled_tournaments,
        discovery_category_seed_ids=GENERIC_PROFILE.discovery_category_seed_ids,
        include_categories_all_discovery=GENERIC_PROFILE.include_categories_all_discovery,
    )
