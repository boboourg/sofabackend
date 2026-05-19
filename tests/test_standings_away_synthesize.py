"""TDD tests for Phase 2.5 — synthesize ``standings/away`` from
``standings/total`` and ``standings/home`` rows that are already in our
DB.

Sofascore exposes three scopes per (UT, season): total, home, away.
For additive metrics (matches, wins, draws, losses, scoresFor,
scoresAgainst, points), they satisfy:

    total[team] = home[team] + away[team]

so ``away`` is fully derivable from the other two. We were fetching all
three every 30 minutes — Phase 2.5 drops ``away`` from the active-UT
refresh list (1/3 HTTP saving on standings) and synthesizes the away
envelope on demand.

Pure-function tests pin the arithmetic and the position re-ranking;
integration tests pin the dispatcher wiring.
"""

from __future__ import annotations

import unittest


class SynthesizeAwayStandingRowsArithmeticTests(unittest.TestCase):
    """``synthesize_away_standing_rows(total_rows, home_rows)`` returns a
    list of away rows with ``away = total - home`` arithmetic for every
    additive field and re-ranks position by points DESC."""

    def _make_row(
        self,
        *,
        team_id: int,
        matches: int,
        wins: int,
        draws: int,
        losses: int,
        scores_for: int,
        scores_against: int,
        points: int,
        position: int = 1,
        team_name: str = "Team",
    ) -> dict:
        return {
            "team_id": team_id,
            "team_name": team_name,
            "team_slug": team_name.lower(),
            "team_short_name": team_name[:3],
            "matches": matches,
            "wins": wins,
            "draws": draws,
            "losses": losses,
            "scores_for": scores_for,
            "scores_against": scores_against,
            "points": points,
            "position": position,
        }

    def test_simple_two_team_arithmetic(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            synthesize_away_standing_rows,
        )

        # Team A: total = 10 matches (5W 3D 2L, gf=15 ga=8, 18pts).
        #         home  = 5 matches  (3W 1D 1L, gf=8  ga=3, 10pts).
        # Expected away = 5 matches  (2W 2D 1L, gf=7  ga=5,  8pts).
        total_rows = [
            self._make_row(
                team_id=1, matches=10, wins=5, draws=3, losses=2,
                scores_for=15, scores_against=8, points=18, team_name="A",
            ),
            self._make_row(
                team_id=2, matches=10, wins=4, draws=2, losses=4,
                scores_for=12, scores_against=10, points=14, team_name="B",
            ),
        ]
        home_rows = [
            self._make_row(
                team_id=1, matches=5, wins=3, draws=1, losses=1,
                scores_for=8, scores_against=3, points=10, team_name="A",
            ),
            self._make_row(
                team_id=2, matches=5, wins=3, draws=0, losses=2,
                scores_for=6, scores_against=4, points=9, team_name="B",
            ),
        ]
        away = synthesize_away_standing_rows(total_rows, home_rows)
        by_team = {row["team_id"]: row for row in away}
        # Team A
        self.assertEqual(by_team[1]["matches"], 5)
        self.assertEqual(by_team[1]["wins"], 2)
        self.assertEqual(by_team[1]["draws"], 2)
        self.assertEqual(by_team[1]["losses"], 1)
        self.assertEqual(by_team[1]["scores_for"], 7)
        self.assertEqual(by_team[1]["scores_against"], 5)
        self.assertEqual(by_team[1]["points"], 8)
        # Team B
        self.assertEqual(by_team[2]["matches"], 5)
        self.assertEqual(by_team[2]["wins"], 1)
        self.assertEqual(by_team[2]["draws"], 2)
        self.assertEqual(by_team[2]["losses"], 2)
        self.assertEqual(by_team[2]["scores_for"], 6)
        self.assertEqual(by_team[2]["scores_against"], 6)
        self.assertEqual(by_team[2]["points"], 5)

    def test_position_recomputed_by_points_desc(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            synthesize_away_standing_rows,
        )

        # Team A scores 8pts away, Team B scores 12pts away — B must be 1st.
        total_rows = [
            self._make_row(
                team_id=1, matches=10, wins=5, draws=3, losses=2,
                scores_for=15, scores_against=8, points=18, team_name="A",
            ),
            self._make_row(
                team_id=2, matches=10, wins=6, draws=2, losses=2,
                scores_for=18, scores_against=10, points=20, team_name="B",
            ),
        ]
        home_rows = [
            self._make_row(
                team_id=1, matches=5, wins=3, draws=1, losses=1,
                scores_for=8, scores_against=3, points=10, team_name="A",
            ),
            self._make_row(
                team_id=2, matches=5, wins=2, draws=2, losses=1,
                scores_for=6, scores_against=4, points=8, team_name="B",
            ),
        ]
        away = synthesize_away_standing_rows(total_rows, home_rows)
        # B should be position 1 (12 pts away), A position 2 (8 pts away).
        by_team = {row["team_id"]: row for row in away}
        self.assertEqual(by_team[2]["points"], 12)
        self.assertEqual(by_team[1]["points"], 8)
        self.assertEqual(by_team[2]["position"], 1)
        self.assertEqual(by_team[1]["position"], 2)

    def test_tiebreaker_goal_difference_then_goals_for(self) -> None:
        """When points are equal, sort by (scoresFor - scoresAgainst) DESC,
        then scoresFor DESC — Sofascore's standard tie-breaker."""
        from schema_inspector.scheduled_events_synthesizer import (
            synthesize_away_standing_rows,
        )

        # Both teams: 10pts away, but A has gf-ga=+5, B has gf-ga=+2.
        total_rows = [
            self._make_row(
                team_id=1, matches=5, wins=3, draws=1, losses=1,
                scores_for=10, scores_against=5, points=10, team_name="A",
            ),
            self._make_row(
                team_id=2, matches=5, wins=3, draws=1, losses=1,
                scores_for=8, scores_against=6, points=10, team_name="B",
            ),
        ]
        home_rows = [
            self._make_row(
                team_id=1, matches=0, wins=0, draws=0, losses=0,
                scores_for=0, scores_against=0, points=0, team_name="A",
            ),
            self._make_row(
                team_id=2, matches=0, wins=0, draws=0, losses=0,
                scores_for=0, scores_against=0, points=0, team_name="B",
            ),
        ]
        away = synthesize_away_standing_rows(total_rows, home_rows)
        by_team = {row["team_id"]: row for row in away}
        self.assertEqual(by_team[1]["position"], 1)  # +5 GD beats +2
        self.assertEqual(by_team[2]["position"], 2)

    def test_team_missing_from_home_treated_as_zero(self) -> None:
        """If a team appears in total but not home (no home matches yet),
        away simply equals total. Defensive against partial data."""
        from schema_inspector.scheduled_events_synthesizer import (
            synthesize_away_standing_rows,
        )

        total_rows = [
            self._make_row(
                team_id=1, matches=3, wins=2, draws=0, losses=1,
                scores_for=5, scores_against=2, points=6, team_name="A",
            ),
        ]
        home_rows: list[dict] = []
        away = synthesize_away_standing_rows(total_rows, home_rows)
        self.assertEqual(len(away), 1)
        self.assertEqual(away[0]["matches"], 3)
        self.assertEqual(away[0]["wins"], 2)
        self.assertEqual(away[0]["points"], 6)

    def test_team_in_home_only_not_emitted(self) -> None:
        """If a team appears in home but NOT in total — data integrity
        anomaly, no event ever produced a 'total' row for this team.
        Skip them; can't compute away without a total baseline."""
        from schema_inspector.scheduled_events_synthesizer import (
            synthesize_away_standing_rows,
        )

        total_rows: list[dict] = []
        home_rows = [
            self._make_row(
                team_id=99, matches=2, wins=1, draws=1, losses=0,
                scores_for=3, scores_against=1, points=4, team_name="X",
            ),
        ]
        away = synthesize_away_standing_rows(total_rows, home_rows)
        self.assertEqual(away, [])

    def test_score_diff_formatted_sign(self) -> None:
        """``scoreDiffFormatted`` follows Sofascore convention: '+N' for
        positive, '-N' for negative, '0' for zero."""
        from schema_inspector.scheduled_events_synthesizer import (
            synthesize_away_standing_rows,
        )

        total_rows = [
            self._make_row(
                team_id=1, matches=2, wins=1, draws=0, losses=1,
                scores_for=4, scores_against=4, points=3, team_name="A",
            ),
        ]
        home_rows = [
            self._make_row(
                team_id=1, matches=1, wins=0, draws=0, losses=1,
                scores_for=1, scores_against=3, points=0, team_name="A",
            ),
        ]
        # away = matches=1, wins=1, draws=0, losses=0, gf=3, ga=1, pts=3 → GD=+2
        away = synthesize_away_standing_rows(total_rows, home_rows)
        self.assertEqual(away[0]["score_diff_formatted"], "+2")


class FootballProfileAwayScopeDroppedTests(unittest.TestCase):
    """Phase 2.5 — football's active-UT standings refresh drops ``away``
    from the scope list. ``total`` + ``home`` are sufficient to
    synthesize ``away`` arithmetically."""

    def test_football_standings_scopes_drops_away(self) -> None:
        from schema_inspector.sport_profiles import FOOTBALL_PROFILE

        self.assertNotIn(
            "away",
            FOOTBALL_PROFILE.standings_scopes,
            "Phase 2.5 drops 'away' — synthesized from total - home in "
            "local_api_server. See docs/REDUNDANT_ENDPOINTS_AUDIT.md §B.",
        )

    def test_football_standings_scopes_keeps_total_and_home(self) -> None:
        from schema_inspector.sport_profiles import FOOTBALL_PROFILE

        # Total + home are the inputs to the away synthesize. Both must
        # stay fetched.
        self.assertIn("total", FOOTBALL_PROFILE.standings_scopes)
        self.assertIn("home", FOOTBALL_PROFILE.standings_scopes)


if __name__ == "__main__":
    unittest.main()
