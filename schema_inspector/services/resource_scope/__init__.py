"""Scope resolvers for the Resource Refresh Loop.

Each ``SofascoreEndpoint`` with ``refresh_interval_seconds`` set declares a
``scope_kind`` (string). The planner asks the resolver matching that kind
for a list of ``ResourceTarget``s; for each one it publishes a
``JOB_REFRESH_RESOURCE`` envelope.

This split keeps endpoint metadata declarative ("scope_kind=team-of-active-ut")
and the actual scope query (SQL, env, cached redis set, ...) localized to
small resolver classes.
"""

from .base import ResourceTarget, ScopeResolver
from .custom_id_of_managed_events import CustomIdOfManagedEventsResolver
from .custom_id_of_registry_events import CustomIdOfRegistryEventsResolver
from .event_of_finished_baseball import EventOfFinishedBaseballResolver
from .managed import ManagedScopeResolver
from .managed_football_pairs import (
    ENV_KEY as MANAGED_FOOTBALL_PAIRS_ENV_KEY,
    load_managed_pairs,
    parse_managed_pairs,
)
from .period_of_managed_pairs import PeriodOfManagedPairsResolver
from .period_of_registry_football import PeriodOfRegistryFootballResolver
from .player_of_active_squad import PlayerOfActiveSquadResolver
from .player_of_active_squad_first_page import PlayerOfActiveSquadFirstPageResolver
from .player_of_national_team_history import PlayerOfNationalTeamHistoryResolver
from .round_of_managed_pairs import RoundOfManagedPairsResolver
from .round_of_registry_football import RoundOfRegistryFootballResolver
from .season_of_active_ut import (
    SeasonOfActiveUTBaseResolver,
    SeasonOfActiveUTEventsResolver,
    SeasonOfActiveUTStandingsResolver,
)
from .season_of_registry_ut import SeasonOfRegistryUTResolver
from .team_of_active_ut import TeamOfActiveUTResolver
from .team_of_active_ut_first_page import TeamOfActiveUTFirstPageResolver
from .team_of_active_ut_season import TeamOfActiveUTSeasonResolver
from .team_of_registry_ut import TeamOfRegistryUTResolver

__all__ = [
    "ResourceTarget",
    "ScopeResolver",
    "CustomIdOfManagedEventsResolver",
    "CustomIdOfRegistryEventsResolver",
    "EventOfFinishedBaseballResolver",
    "ManagedScopeResolver",
    "MANAGED_FOOTBALL_PAIRS_ENV_KEY",
    "PeriodOfManagedPairsResolver",
    "PeriodOfRegistryFootballResolver",
    "PlayerOfActiveSquadResolver",
    "PlayerOfActiveSquadFirstPageResolver",
    "PlayerOfNationalTeamHistoryResolver",
    "RoundOfManagedPairsResolver",
    "RoundOfRegistryFootballResolver",
    "SeasonOfActiveUTBaseResolver",
    "SeasonOfActiveUTEventsResolver",
    "SeasonOfActiveUTStandingsResolver",
    "SeasonOfRegistryUTResolver",
    "TeamOfActiveUTResolver",
    "TeamOfActiveUTFirstPageResolver",
    "TeamOfActiveUTSeasonResolver",
    "TeamOfRegistryUTResolver",
    "load_managed_pairs",
    "parse_managed_pairs",
]
