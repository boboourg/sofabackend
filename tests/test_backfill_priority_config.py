"""Tests for BackfillPriorityConfig — operator-facing YAML that controls
how the historical-tournament planner distributes work across sports
and individual unique tournaments.
"""
from __future__ import annotations
import tempfile
import textwrap
import unittest
from pathlib import Path


def _write_tmp(yaml: str) -> Path:
    f = tempfile.NamedTemporaryFile(
        "w", suffix=".yaml", delete=False, encoding="utf-8"
    )
    f.write(textwrap.dedent(yaml))
    f.close()
    return Path(f.name)


class LoadTests(unittest.TestCase):
    def test_load_minimal_valid(self) -> None:
        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
        )

        path = _write_tmp("""
            sport_weights:
              football: 10
              tennis: 5
        """)
        cfg = BackfillPriorityConfig.load(path)
        self.assertEqual(cfg.sport_weights["football"], 10.0)
        self.assertEqual(cfg.sport_weights["tennis"], 5.0)
        self.assertEqual(cfg.ut_boost, {})
        self.assertEqual(cfg.sport_concurrency_caps, {})

    def test_load_full_config(self) -> None:
        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
        )

        path = _write_tmp("""
            sport_weights:
              football: 20
              basketball: 5
            ut_boost:
              - ut_id: 17
                multiplier: 5.0
              - ut_id: 1
                multiplier: 10.0
            sport_concurrency_caps:
              football: 50
        """)
        cfg = BackfillPriorityConfig.load(path)
        self.assertEqual(cfg.sport_weights, {"football": 20.0, "basketball": 5.0})
        self.assertEqual(cfg.ut_boost, {17: 5.0, 1: 10.0})
        self.assertEqual(cfg.sport_concurrency_caps, {"football": 50})

    def test_load_missing_file_returns_defaults(self) -> None:
        """If the config file is absent the planner must keep running
        with uniform weights (no sport gets dropped)."""
        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
        )

        cfg = BackfillPriorityConfig.load(Path("/tmp/nonexistent-12345.yaml"))
        self.assertEqual(cfg.sport_weights, {})
        self.assertEqual(cfg.ut_boost, {})

    def test_load_empty_yaml_returns_defaults(self) -> None:
        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
        )

        path = _write_tmp("")
        cfg = BackfillPriorityConfig.load(path)
        self.assertEqual(cfg.sport_weights, {})
        self.assertEqual(cfg.ut_boost, {})

    def test_load_rejects_negative_weight(self) -> None:
        """Negative weight is operator error — raise so SIGHUP keeps
        the previous config instead of silently dropping a sport."""
        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
            ConfigValidationError,
        )

        path = _write_tmp("""
            sport_weights:
              football: -1
        """)
        with self.assertRaises(ConfigValidationError):
            BackfillPriorityConfig.load(path)

    def test_load_rejects_non_numeric_weight(self) -> None:
        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
            ConfigValidationError,
        )

        path = _write_tmp("""
            sport_weights:
              football: "high"
        """)
        with self.assertRaises(ConfigValidationError):
            BackfillPriorityConfig.load(path)

    def test_load_rejects_bad_ut_boost_shape(self) -> None:
        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
            ConfigValidationError,
        )

        path = _write_tmp("""
            ut_boost:
              - multiplier: 2.0
              # missing ut_id
        """)
        with self.assertRaises(ConfigValidationError):
            BackfillPriorityConfig.load(path)


class WeightLookupTests(unittest.TestCase):
    def test_weight_for_ut_combines_sport_and_boost(self) -> None:
        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
        )

        cfg = BackfillPriorityConfig(
            sport_weights={"football": 10.0},
            ut_boost={17: 5.0},
            sport_concurrency_caps={},
        )
        # boost = sport_weight * multiplier
        self.assertEqual(cfg.weight_for_ut(ut_id=17, sport_slug="football"), 50.0)
        # no boost — fall back to sport weight
        self.assertEqual(cfg.weight_for_ut(ut_id=99, sport_slug="football"), 10.0)
        # unknown sport — 0
        self.assertEqual(cfg.weight_for_ut(ut_id=99, sport_slug="cricket"), 0.0)

    def test_zero_weight_pauses_sport(self) -> None:
        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
        )

        cfg = BackfillPriorityConfig(
            sport_weights={"cricket": 0.0, "football": 10.0},
            ut_boost={},
            sport_concurrency_caps={},
        )
        self.assertEqual(cfg.weight_for_ut(ut_id=99, sport_slug="cricket"), 0.0)


class WeightedSelectionTests(unittest.TestCase):
    def test_select_with_weighted_distribution_over_many_samples(self) -> None:
        """Verify the planner-level helper distributes selections
        proportionally to the configured weights."""
        from collections import Counter

        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
            weighted_select,
        )

        cfg = BackfillPriorityConfig(
            sport_weights={"football": 4.0, "tennis": 1.0},
            ut_boost={},
            sport_concurrency_caps={},
        )
        # One football vs one tennis candidate — weights 4:1.
        candidates = [
            (101, "football"),
            (201, "tennis"),
        ]
        # Run 10K selections — ratio should land near 4:1 favouring football.
        counts: Counter = Counter()
        for i in range(10_000):
            picked = weighted_select(candidates, cfg, seed=i)
            counts[picked[1]] += 1
        # football should be roughly 80 %, tennis 20 % — allow some noise.
        self.assertGreater(counts["football"] / 10_000, 0.75)
        self.assertLess(counts["football"] / 10_000, 0.85)

    def test_weighted_select_skips_zero_weight(self) -> None:
        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
            weighted_select,
        )

        cfg = BackfillPriorityConfig(
            sport_weights={"football": 10.0, "cricket": 0.0},
            ut_boost={},
            sport_concurrency_caps={},
        )
        candidates = [(1, "cricket"), (2, "football"), (3, "cricket")]
        for seed in range(20):
            picked = weighted_select(candidates, cfg, seed=seed)
            self.assertEqual(picked[1], "football")

    def test_weighted_select_returns_none_when_no_eligible(self) -> None:
        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
            weighted_select,
        )

        cfg = BackfillPriorityConfig(
            sport_weights={"cricket": 0.0},
            ut_boost={},
            sport_concurrency_caps={},
        )
        self.assertIsNone(weighted_select([(1, "cricket")], cfg, seed=42))


class ConcurrencyCapTests(unittest.TestCase):
    def test_cap_for_sport_returns_value_or_none(self) -> None:
        from schema_inspector.services.backfill_priority_config import (
            BackfillPriorityConfig,
        )

        cfg = BackfillPriorityConfig(
            sport_weights={},
            ut_boost={},
            sport_concurrency_caps={"football": 50},
        )
        self.assertEqual(cfg.cap_for_sport("football"), 50)
        self.assertIsNone(cfg.cap_for_sport("tennis"))


if __name__ == "__main__":
    unittest.main()
