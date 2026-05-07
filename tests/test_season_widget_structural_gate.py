from __future__ import annotations

import asyncio
import unittest

from schema_inspector.endpoints import (
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT,
    unique_tournament_top_players_endpoint,
    unique_tournament_top_teams_endpoint,
)
from schema_inspector.services.season_widget_structural_gate import (
    DEFAULT_REGULAR_SEASON_ALLOWLIST,
    SeasonWidgetStructuralGate,
)


class _FakeConn:
    def __init__(self, *, has_recent_or_upcoming: bool, already_stored: bool):
        self.calls: list[tuple[str, tuple]] = []
        self._has_recent_or_upcoming = has_recent_or_upcoming
        self._already_stored = already_stored

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        return {
            "has_recent_or_upcoming": self._has_recent_or_upcoming,
            "already_stored": self._already_stored,
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _FakeDatabase:
    def __init__(self, *, has_recent_or_upcoming: bool = False, already_stored: bool = False):
        self._has = has_recent_or_upcoming
        self._stored = already_stored
        self.connection_calls = 0

    def connection(self):
        self.connection_calls += 1
        return _FakeConn(has_recent_or_upcoming=self._has, already_stored=self._stored)


def _resolve(gate, *, ut, season, candidates):
    return tuple(
        asyncio.run(
            gate.blocked_patterns(
                unique_tournament_id=ut,
                season_id=season,
                candidate_patterns=candidates,
            )
        )
    )


class StructuralGateTests(unittest.TestCase):
    def test_pos_blocked_when_season_active(self) -> None:
        gate = SeasonWidgetStructuralGate(
            database=_FakeDatabase(has_recent_or_upcoming=True, already_stored=False),
            env={},
        )
        candidates = (UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT.pattern,)
        blocked = _resolve(gate, ut=17, season=76986, candidates=candidates)
        self.assertEqual(blocked, candidates)

    def test_pos_blocked_when_already_stored_one_shot(self) -> None:
        gate = SeasonWidgetStructuralGate(
            database=_FakeDatabase(has_recent_or_upcoming=False, already_stored=True),
            env={},
        )
        candidates = (UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT.pattern,)
        blocked = _resolve(gate, ut=17, season=61627, candidates=candidates)
        self.assertEqual(blocked, candidates)

    def test_pos_allowed_when_season_completed_and_not_yet_stored(self) -> None:
        gate = SeasonWidgetStructuralGate(
            database=_FakeDatabase(has_recent_or_upcoming=False, already_stored=False),
            env={},
        )
        candidates = (UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT.pattern,)
        blocked = _resolve(gate, ut=17, season=61627, candidates=candidates)
        self.assertEqual(blocked, ())  # not blocked -> publish goes through

    def test_regular_season_blocked_for_non_allowlist_ut(self) -> None:
        gate = SeasonWidgetStructuralGate(
            database=_FakeDatabase(),  # never queried
            env={},
        )
        reg_pattern = unique_tournament_top_players_endpoint("regularSeason").pattern
        team_reg_pattern = unique_tournament_top_teams_endpoint("regularSeason").pattern
        overall_pattern = unique_tournament_top_players_endpoint("overall").pattern
        candidates = (reg_pattern, team_reg_pattern, overall_pattern)
        # PL UT 17 not in NBA/MLB/NHL/NFL allowlist
        blocked = _resolve(gate, ut=17, season=76986, candidates=candidates)
        self.assertIn(reg_pattern, blocked)
        self.assertIn(team_reg_pattern, blocked)
        self.assertNotIn(overall_pattern, blocked)

    def test_regular_season_allowed_for_nba(self) -> None:
        gate = SeasonWidgetStructuralGate(
            database=_FakeDatabase(),
            env={},
        )
        reg_pattern = unique_tournament_top_players_endpoint("regularSeason").pattern
        candidates = (reg_pattern,)
        blocked = _resolve(gate, ut=132, season=80229, candidates=candidates)  # NBA
        self.assertEqual(blocked, ())

    def test_env_override_for_allowlist(self) -> None:
        gate = SeasonWidgetStructuralGate(
            database=_FakeDatabase(),
            env={"SCHEMA_INSPECTOR_REGULAR_SEASON_UTS": "999, 1000"},
        )
        # Default allowlist (NBA=132 etc.) is overridden — 132 should now be blocked.
        reg_pattern = unique_tournament_top_players_endpoint("regularSeason").pattern
        blocked = _resolve(gate, ut=132, season=80229, candidates=(reg_pattern,))
        self.assertIn(reg_pattern, blocked)
        # And 999 should pass.
        blocked = _resolve(gate, ut=999, season=1, candidates=(reg_pattern,))
        self.assertEqual(blocked, ())

    def test_default_allowlist_includes_known_uts(self) -> None:
        # NBA=132, MLB=11205, NHL=234, NFL=9464 — confirmed via live probe.
        for ut in (132, 11205, 234, 9464):
            self.assertIn(ut, DEFAULT_REGULAR_SEASON_ALLOWLIST)


if __name__ == "__main__":
    unittest.main()
