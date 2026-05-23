"""Tests for monitoring/config.py — env-backed configuration loader.

Pins the contract:
- Empty env → defaults are sensible (60s interval, 600/1800 ttls)
- SOFASCORE_MONITORING_ENABLED honours 0/1/true/false
- All thresholds env-overridable
- has_telegram() returns True only when both token AND chat_id set
- ``from_env(env)`` is pure (no global os.environ side effects)
"""

from __future__ import annotations

import unittest

from schema_inspector.monitoring.config import MonitoringConfig


class FromEnvDefaultsTests(unittest.TestCase):
    def test_empty_env_uses_defaults(self) -> None:
        config = MonitoringConfig.from_env(env={})
        self.assertTrue(config.enabled)
        self.assertEqual(config.interval_seconds, 60.0)
        self.assertEqual(config.warn_ttl_seconds, 600)
        self.assertEqual(config.crit_ttl_seconds, 1800)
        self.assertEqual(config.oldest_hot_age_warn_seconds, 120)
        self.assertEqual(config.oldest_hot_age_crit_seconds, 300)

    def test_enabled_zero_disables(self) -> None:
        config = MonitoringConfig.from_env(env={"SOFASCORE_MONITORING_ENABLED": "0"})
        self.assertFalse(config.enabled)

    def test_enabled_false_disables(self) -> None:
        config = MonitoringConfig.from_env(
            env={"SOFASCORE_MONITORING_ENABLED": "false"}
        )
        self.assertFalse(config.enabled)

    def test_enabled_yes_enables(self) -> None:
        config = MonitoringConfig.from_env(
            env={"SOFASCORE_MONITORING_ENABLED": "yes"}
        )
        self.assertTrue(config.enabled)


class FromEnvOverridesTests(unittest.TestCase):
    def test_interval_override(self) -> None:
        config = MonitoringConfig.from_env(
            env={"SOFASCORE_MONITORING_INTERVAL_SECONDS": "120"}
        )
        self.assertEqual(config.interval_seconds, 120.0)

    def test_ttl_overrides(self) -> None:
        env = {
            "SOFASCORE_MONITORING_DEDUPE_WARN_TTL_SECONDS": "300",
            "SOFASCORE_MONITORING_DEDUPE_CRIT_TTL_SECONDS": "900",
        }
        config = MonitoringConfig.from_env(env=env)
        self.assertEqual(config.warn_ttl_seconds, 300)
        self.assertEqual(config.crit_ttl_seconds, 900)

    def test_threshold_overrides(self) -> None:
        env = {
            "SOFASCORE_MONITORING_OLDEST_HOT_AGE_WARN_SECONDS": "180",
            "SOFASCORE_MONITORING_OLDEST_HOT_AGE_CRIT_SECONDS": "600",
            "SOFASCORE_MONITORING_TIER_1_BLOCKED_WARN_RATE": "0.35",
            "SOFASCORE_MONITORING_TIER_1_BLOCKED_CRIT_RATE": "0.60",
            "SOFASCORE_MONITORING_REFRESH_SUCCESS_WARN_RATE": "0.90",
            "SOFASCORE_MONITORING_REFRESH_SUCCESS_CRIT_RATE": "0.80",
        }
        config = MonitoringConfig.from_env(env=env)
        self.assertEqual(config.oldest_hot_age_warn_seconds, 180)
        self.assertEqual(config.oldest_hot_age_crit_seconds, 600)
        self.assertEqual(config.tier_1_blocked_warn_rate, 0.35)
        self.assertEqual(config.tier_1_blocked_crit_rate, 0.60)
        self.assertEqual(config.refresh_success_warn_rate, 0.90)
        self.assertEqual(config.refresh_success_crit_rate, 0.80)

    def test_telegram_credentials_loaded(self) -> None:
        env = {
            "SOFASCORE_MONITORING_TELEGRAM_BOT_TOKEN": "abc:def",
            "SOFASCORE_MONITORING_TELEGRAM_CHAT_ID": "42",
        }
        config = MonitoringConfig.from_env(env=env)
        self.assertEqual(config.telegram_bot_token, "abc:def")
        self.assertEqual(config.telegram_chat_id, "42")

    def test_queue_thresholds_overridable(self) -> None:
        env = {
            "SOFASCORE_MONITORING_HYDRATE_LAG_WARN": "2500",
            "SOFASCORE_MONITORING_HYDRATE_LAG_CRIT": "10000",
            "SOFASCORE_MONITORING_LIVE_HOT_LAG_WARN": "750",
            "SOFASCORE_MONITORING_LIVE_HOT_LAG_CRIT": "3000",
        }
        config = MonitoringConfig.from_env(env=env)
        self.assertEqual(config.hydrate_lag_warn, 2500)
        self.assertEqual(config.hydrate_lag_crit, 10000)
        self.assertEqual(config.live_hot_lag_warn, 750)
        self.assertEqual(config.live_hot_lag_crit, 3000)

    def test_invalid_int_falls_back_to_default(self) -> None:
        config = MonitoringConfig.from_env(
            env={"SOFASCORE_MONITORING_INTERVAL_SECONDS": "not-a-number"}
        )
        self.assertEqual(config.interval_seconds, 60.0)


class HasTelegramTests(unittest.TestCase):
    def test_returns_true_when_both_set(self) -> None:
        config = MonitoringConfig(
            telegram_bot_token="abc",
            telegram_chat_id="42",
        )
        self.assertTrue(config.has_telegram())

    def test_returns_false_when_token_missing(self) -> None:
        config = MonitoringConfig(telegram_chat_id="42")
        self.assertFalse(config.has_telegram())

    def test_returns_false_when_chat_id_missing(self) -> None:
        config = MonitoringConfig(telegram_bot_token="abc")
        self.assertFalse(config.has_telegram())

    def test_returns_false_when_both_missing(self) -> None:
        config = MonitoringConfig()
        self.assertFalse(config.has_telegram())


if __name__ == "__main__":
    unittest.main()
