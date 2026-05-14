"""Tests for monitoring/signals.py — classification, snapshot, formatting.

Pins the contract:
- ``classify_signal`` returns OK when value is None (no data, no alert)
- "max" direction triggers on value > threshold
- "min" direction triggers on value < threshold
- CRIT supersedes WARN even when thresholds are equal
- ``make_snapshot`` carries overridden thresholds, classifies, preserves note
- ``format_alert_message`` is plain-text, includes name/value/threshold/time
- RESOLVED messages distinct from alert messages
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from schema_inspector.monitoring.signals import (
    SIGNAL_DEFINITIONS,
    SIGNAL_OLDEST_HOT_AGE,
    SIGNAL_REFRESH_SUCCESS,
    SIGNAL_TIER_1_BLOCKED,
    SignalDefinition,
    classify_signal,
    format_alert_message,
    make_snapshot,
)


class ClassifySignalTests(unittest.TestCase):
    def test_none_value_is_ok(self) -> None:
        self.assertEqual(
            classify_signal(
                value=None, threshold_warn=100, threshold_crit=200, direction="max"
            ),
            "OK",
        )

    def test_max_direction_below_warn_is_ok(self) -> None:
        self.assertEqual(
            classify_signal(
                value=50, threshold_warn=100, threshold_crit=200, direction="max"
            ),
            "OK",
        )

    def test_max_direction_above_warn_is_warn(self) -> None:
        self.assertEqual(
            classify_signal(
                value=150, threshold_warn=100, threshold_crit=200, direction="max"
            ),
            "WARN",
        )

    def test_max_direction_above_crit_is_crit(self) -> None:
        self.assertEqual(
            classify_signal(
                value=250, threshold_warn=100, threshold_crit=200, direction="max"
            ),
            "CRIT",
        )

    def test_max_direction_exact_at_threshold_is_ok(self) -> None:
        """`value > threshold` is strict — exact equality stays OK."""

        self.assertEqual(
            classify_signal(
                value=100, threshold_warn=100, threshold_crit=200, direction="max"
            ),
            "OK",
        )

    def test_min_direction_above_warn_is_ok(self) -> None:
        self.assertEqual(
            classify_signal(
                value=0.99,
                threshold_warn=0.95,
                threshold_crit=0.85,
                direction="min",
            ),
            "OK",
        )

    def test_min_direction_below_warn_is_warn(self) -> None:
        self.assertEqual(
            classify_signal(
                value=0.90,
                threshold_warn=0.95,
                threshold_crit=0.85,
                direction="min",
            ),
            "WARN",
        )

    def test_min_direction_below_crit_is_crit(self) -> None:
        self.assertEqual(
            classify_signal(
                value=0.80,
                threshold_warn=0.95,
                threshold_crit=0.85,
                direction="min",
            ),
            "CRIT",
        )

    def test_crit_supersedes_warn_when_thresholds_equal(self) -> None:
        self.assertEqual(
            classify_signal(
                value=150, threshold_warn=100, threshold_crit=100, direction="max"
            ),
            "CRIT",
        )


class MakeSnapshotTests(unittest.TestCase):
    def test_classifies_value_against_definition_defaults(self) -> None:
        snapshot = make_snapshot(
            definition=SIGNAL_OLDEST_HOT_AGE,
            value=500,
        )
        self.assertEqual(snapshot.severity, "CRIT")
        self.assertEqual(snapshot.name, SIGNAL_OLDEST_HOT_AGE.name)
        self.assertEqual(snapshot.threshold_warn, SIGNAL_OLDEST_HOT_AGE.threshold_warn)
        self.assertEqual(snapshot.threshold_crit, SIGNAL_OLDEST_HOT_AGE.threshold_crit)

    def test_override_thresholds_change_classification(self) -> None:
        # Default crit=300; override crit=600 — value 500 should be WARN now.
        snapshot = make_snapshot(
            definition=SIGNAL_OLDEST_HOT_AGE,
            value=500,
            threshold_warn=120,
            threshold_crit=600,
        )
        self.assertEqual(snapshot.severity, "WARN")
        self.assertEqual(snapshot.threshold_crit, 600)

    def test_none_value_yields_ok(self) -> None:
        snapshot = make_snapshot(
            definition=SIGNAL_TIER_1_BLOCKED,
            value=None,
        )
        self.assertEqual(snapshot.severity, "OK")
        self.assertIsNone(snapshot.value)

    def test_note_and_extra_preserved(self) -> None:
        snapshot = make_snapshot(
            definition=SIGNAL_OLDEST_HOT_AGE,
            value=150,
            note="custom note",
            extra={"tier_1_active_events": 5},
        )
        self.assertEqual(snapshot.note, "custom note")
        self.assertEqual(snapshot.extra["tier_1_active_events"], 5)

    def test_timestamp_default_is_utc_now(self) -> None:
        before = datetime.now(timezone.utc)
        snapshot = make_snapshot(definition=SIGNAL_OLDEST_HOT_AGE, value=10)
        after = datetime.now(timezone.utc)
        self.assertGreaterEqual(snapshot.timestamp, before)
        self.assertLessEqual(snapshot.timestamp, after)


class FormatAlertMessageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.snapshot_crit = make_snapshot(
            definition=SIGNAL_OLDEST_HOT_AGE,
            value=412,
            timestamp=datetime(2026, 5, 14, 18, 24, 0, tzinfo=timezone.utc),
        )

    def test_message_includes_signal_name(self) -> None:
        msg = format_alert_message(self.snapshot_crit, host_label="prod")
        self.assertIn(SIGNAL_OLDEST_HOT_AGE.name, msg)

    def test_crit_message_marked_crit(self) -> None:
        msg = format_alert_message(self.snapshot_crit, host_label="prod")
        self.assertIn("CRIT", msg)

    def test_message_includes_value_and_threshold(self) -> None:
        msg = format_alert_message(self.snapshot_crit, host_label="prod")
        self.assertIn("412", msg)
        # crit threshold = 300 from default definition
        self.assertIn("300", msg)

    def test_message_includes_host_label(self) -> None:
        msg = format_alert_message(self.snapshot_crit, host_label="sofascore-prod")
        self.assertIn("sofascore-prod", msg)

    def test_repeat_count_when_provided(self) -> None:
        first = datetime(2026, 5, 14, 18, 0, 0, tzinfo=timezone.utc)
        msg = format_alert_message(
            self.snapshot_crit,
            host_label="prod",
            repeat_count=3,
            first_alerted_at=first,
        )
        self.assertIn("Repeat #3", msg)
        self.assertIn(first.isoformat(), msg)

    def test_resolved_message_is_distinct(self) -> None:
        ok_snapshot = make_snapshot(
            definition=SIGNAL_OLDEST_HOT_AGE,
            value=98,
            timestamp=datetime(2026, 5, 14, 18, 48, 0, tzinfo=timezone.utc),
        )
        msg = format_alert_message(
            ok_snapshot, host_label="prod", resolved=True
        )
        self.assertIn("RESOLVED", msg)
        self.assertIn("98", msg)
        self.assertNotIn("threshold", msg)

    def test_warn_message_marked_warn(self) -> None:
        warn_snapshot = make_snapshot(
            definition=SIGNAL_OLDEST_HOT_AGE,
            value=150,
            timestamp=datetime(2026, 5, 14, 18, 24, 0, tzinfo=timezone.utc),
        )
        msg = format_alert_message(warn_snapshot, host_label="prod")
        self.assertIn("WARN", msg)
        self.assertIn("150", msg)
        # WARN message uses warn threshold = 120 from default definition.
        self.assertIn("120", msg)

    def test_extra_fields_appear_in_message(self) -> None:
        snapshot = make_snapshot(
            definition=SIGNAL_TIER_1_BLOCKED,
            value=0.75,
            extra={"tier_1_active_events": 4},
            timestamp=datetime(2026, 5, 14, 18, 0, 0, tzinfo=timezone.utc),
        )
        msg = format_alert_message(snapshot, host_label="prod")
        self.assertIn("tier_1_active_events: 4", msg)

    def test_message_is_plain_text(self) -> None:
        """No Markdown — avoids parse_mode accidents from dynamic notes."""

        snapshot = make_snapshot(
            definition=SIGNAL_OLDEST_HOT_AGE,
            value=412,
            note="contains *star* and _underscore_",
            timestamp=datetime(2026, 5, 14, 18, 24, 0, tzinfo=timezone.utc),
        )
        msg = format_alert_message(snapshot, host_label="prod")
        # The literal asterisks and underscores remain as-is — we don't
        # escape them; Telegram is told parse_mode=None server-side.
        self.assertIn("*star*", msg)
        self.assertIn("_underscore_", msg)


class SignalDefinitionsRegistryTests(unittest.TestCase):
    def test_registry_contains_phase_1_slo_signals(self) -> None:
        # Phase 1 SLO signals must remain in the registry across phases.
        slo_names = {
            SIGNAL_OLDEST_HOT_AGE.name,
            SIGNAL_TIER_1_BLOCKED.name,
            SIGNAL_REFRESH_SUCCESS.name,
        }
        self.assertTrue(slo_names.issubset(set(SIGNAL_DEFINITIONS)))

    def test_registry_contains_phase_2_queue_signals(self) -> None:
        # Phase 2 queue signals (XLEN per stream).
        queue_names = {
            "hydrate_xlen",
            "live_hot_xlen",
            "live_warm_xlen",
            "live_discovery_xlen",
            "discovery_xlen",
        }
        self.assertTrue(queue_names.issubset(set(SIGNAL_DEFINITIONS)))

    def test_definitions_are_well_formed(self) -> None:
        for definition in SIGNAL_DEFINITIONS.values():
            self.assertIsInstance(definition, SignalDefinition)
            self.assertIn(definition.direction, {"max", "min"})


if __name__ == "__main__":
    unittest.main()
