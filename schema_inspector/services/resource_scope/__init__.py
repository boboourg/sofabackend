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
from .managed import ManagedScopeResolver
from .player_of_active_squad import PlayerOfActiveSquadResolver
from .player_of_active_squad_first_page import PlayerOfActiveSquadFirstPageResolver
from .team_of_active_ut import TeamOfActiveUTResolver

__all__ = [
    "ResourceTarget",
    "ScopeResolver",
    "ManagedScopeResolver",
    "PlayerOfActiveSquadResolver",
    "PlayerOfActiveSquadFirstPageResolver",
    "TeamOfActiveUTResolver",
]
