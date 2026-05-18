"""Verify that HistoricalTournamentPlannerDaemon can reload its
BackfillPriorityConfig from disk at runtime without restarting."""
from __future__ import annotations
import tempfile
import textwrap
import unittest
from pathlib import Path
from typing import Any

from schema_inspector.services.backfill_priority_config import (
    BackfillPriorityConfig,
)
from schema_inspector.services.historical_tournament_planner import (
    HistoricalTournamentCursorStore,
    HistoricalTournamentPlannerDaemon,
    HistoricalTournamentPlanningTarget,
)


class FakeQueue:
    def __init__(self) -> None:
        self.published: list[Any] = []

    def publish(self, *_a: Any, **_kw: Any) -> None:
        self.published.append(_a)


class FakeBackend:
    def __init__(self) -> None:
        self._h: dict[str, str] = {}

    def hgetall(self, _: str) -> dict[str, str]:
        return dict(self._h)

    def hset(self, *_a: Any, **_kw: Any) -> int:
        return 1


def _write_yaml(text: str) -> Path:
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8")
    f.write(textwrap.dedent(text))
    f.close()
    return Path(f.name)


def _make_planner() -> HistoricalTournamentPlannerDaemon:
    async def selector(*, sport_slug: str, **_: Any) -> tuple[int, ...]:
        return ()

    return HistoricalTournamentPlannerDaemon(
        queue=FakeQueue(),
        cursor_store=HistoricalTournamentCursorStore(FakeBackend()),
        selector=selector,
        targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
        tournaments_per_tick=10,
    )


class ReloadConfigTests(unittest.TestCase):
    def test_reload_replaces_priority_config(self) -> None:
        planner = _make_planner()
        self.assertIsNone(planner.priority_config)

        path = _write_yaml("""
            sport_weights:
              football: 10
              tennis: 5
        """)
        planner.reload_priority_config(path)

        self.assertIsNotNone(planner.priority_config)
        self.assertEqual(planner.priority_config.sport_weights["football"], 10.0)

    def test_reload_with_malformed_yaml_keeps_previous_config(self) -> None:
        """Operator typo must NOT clear the running config — log + skip."""
        planner = _make_planner()
        good = _write_yaml("""
            sport_weights:
              football: 7
        """)
        planner.reload_priority_config(good)
        baseline = planner.priority_config

        bad = _write_yaml("""
            sport_weights:
              football: -1   # invalid
        """)
        planner.reload_priority_config(bad)

        self.assertIs(planner.priority_config, baseline)
        self.assertEqual(planner.priority_config.sport_weights["football"], 7.0)

    def test_reload_with_missing_file_resets_to_default_empty(self) -> None:
        """Missing file is a legitimate operator state ('I removed the
        override') — reset to empty BackfillPriorityConfig."""
        planner = _make_planner()
        good = _write_yaml("""
            sport_weights:
              football: 7
        """)
        planner.reload_priority_config(good)
        self.assertEqual(planner.priority_config.sport_weights["football"], 7.0)

        # File now missing.
        planner.reload_priority_config(Path("/tmp/definitely-not-here-987.yaml"))
        # Empty config restored — planner falls back to uniform behaviour.
        self.assertIsInstance(planner.priority_config, BackfillPriorityConfig)
        self.assertEqual(planner.priority_config.sport_weights, {})


if __name__ == "__main__":
    unittest.main()
