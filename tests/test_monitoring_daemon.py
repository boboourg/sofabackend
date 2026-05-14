"""Tests for monitoring/daemon.py — MonitoringDaemon tick orchestration.

Pins the contract:
- A WARN snapshot → fires alert, marks dedupe
- A second WARN within TTL → suppressed (dedupe blocks)
- A WARN → OK transition fires RESOLVED, clears dedupe
- A WARN → CRIT escalation fires CRIT even if dedupe would otherwise block
- Sink failure → suppresses dedupe mark, ensures retry next tick
- signal_source exception → no crash, empty tick report
- daemon.config.enabled=False → run_forever exits immediately
"""

from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone

from schema_inspector.monitoring.alerter import NullAlertSink
from schema_inspector.monitoring.config import MonitoringConfig
from schema_inspector.monitoring.daemon import MonitoringDaemon
from schema_inspector.monitoring.dedupe import NullDedupeStore
from schema_inspector.monitoring.signals import (
    SIGNAL_OLDEST_HOT_AGE,
    SIGNAL_REFRESH_SUCCESS,
    SignalSnapshot,
    make_snapshot,
)


class _RecordingSink:
    def __init__(self, *, succeed: bool = True) -> None:
        self.messages: list[str] = []
        self.succeed = succeed
        self.send_calls = 0

    async def send(self, message: str) -> bool:
        self.send_calls += 1
        self.messages.append(message)
        return self.succeed


class _CountingDedupeStore:
    """Replays a deterministic should_send schedule and tracks mark_sent."""

    def __init__(self, schedule: list[bool] | None = None) -> None:
        self.schedule = list(schedule or [])
        self.mark_calls: list[tuple[str, str]] = []
        self.resolved_signals: list[str] = []
        self._counts: dict[tuple[str, str], int] = {}
        self._first_alerted: dict[tuple[str, str], float] = {}

    def should_send(
        self, signal_name: str, severity: str, ttl_seconds: int
    ) -> bool:
        del signal_name, severity, ttl_seconds
        if not self.schedule:
            return True
        return self.schedule.pop(0)

    def mark_sent(self, signal_name: str, severity: str, ttl_seconds: int) -> None:
        del ttl_seconds
        self.mark_calls.append((signal_name, severity))
        key = (signal_name, severity)
        self._counts[key] = self._counts.get(key, 0) + 1
        self._first_alerted.setdefault(key, 1000.0)

    def record_resolved(self, signal_name: str) -> None:
        self.resolved_signals.append(signal_name)
        for severity in ("WARN", "CRIT"):
            self._counts.pop((signal_name, severity), None)
            self._first_alerted.pop((signal_name, severity), None)

    def get_repeat_count(self, signal_name: str, severity: str) -> int:
        return self._counts.get((signal_name, severity), 0)

    def get_first_alerted_at_epoch(
        self, signal_name: str, severity: str
    ) -> float | None:
        return self._first_alerted.get((signal_name, severity))


def _make_warn_snapshot() -> SignalSnapshot:
    return make_snapshot(
        definition=SIGNAL_OLDEST_HOT_AGE,
        value=150,
        timestamp=datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc),
    )


def _make_crit_snapshot() -> SignalSnapshot:
    return make_snapshot(
        definition=SIGNAL_OLDEST_HOT_AGE,
        value=500,
        timestamp=datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc),
    )


def _make_ok_snapshot() -> SignalSnapshot:
    return make_snapshot(
        definition=SIGNAL_OLDEST_HOT_AGE,
        value=10,
        timestamp=datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc),
    )


def _make_daemon(
    *,
    snapshots: list[SignalSnapshot] | None = None,
    sink: _RecordingSink | None = None,
    dedupe: _CountingDedupeStore | None = None,
    config: MonitoringConfig | None = None,
) -> tuple[MonitoringDaemon, _RecordingSink, _CountingDedupeStore]:
    sink = sink or _RecordingSink()
    dedupe = dedupe or _CountingDedupeStore()
    config = config or MonitoringConfig(
        enabled=True,
        interval_seconds=1.0,
        warn_ttl_seconds=600,
        crit_ttl_seconds=1800,
    )
    snapshots = snapshots or []

    async def signal_source() -> list[SignalSnapshot]:
        return list(snapshots)

    return (
        MonitoringDaemon(
            config=config,
            signal_source=signal_source,
            sink=sink,
            dedupe=dedupe,
        ),
        sink,
        dedupe,
    )


class TickAlertFireSuppressTests(unittest.IsolatedAsyncioTestCase):
    async def test_warn_snapshot_fires_alert(self) -> None:
        daemon, sink, dedupe = _make_daemon(snapshots=[_make_warn_snapshot()])
        report = await daemon._tick()
        self.assertEqual(report.alerts_fired, 1)
        self.assertEqual(sink.send_calls, 1)
        self.assertEqual(
            dedupe.mark_calls,
            [(SIGNAL_OLDEST_HOT_AGE.name, "WARN")],
        )

    async def test_dedupe_suppression_no_alert(self) -> None:
        sink = _RecordingSink()
        dedupe = _CountingDedupeStore(schedule=[False])
        daemon, _, _ = _make_daemon(
            snapshots=[_make_warn_snapshot()], sink=sink, dedupe=dedupe
        )
        report = await daemon._tick()
        self.assertEqual(report.alerts_fired, 0)
        self.assertEqual(report.alerts_suppressed, 1)
        self.assertEqual(sink.send_calls, 0)

    async def test_sink_failure_does_not_mark_dedupe(self) -> None:
        sink = _RecordingSink(succeed=False)
        dedupe = _CountingDedupeStore()
        daemon, _, _ = _make_daemon(
            snapshots=[_make_warn_snapshot()], sink=sink, dedupe=dedupe
        )
        report = await daemon._tick()
        # Sink failed → not counted as fired, no dedupe mark → will retry.
        self.assertEqual(report.alerts_fired, 0)
        self.assertEqual(dedupe.mark_calls, [])

    async def test_resolved_alert_fired_on_warn_to_ok(self) -> None:
        sink = _RecordingSink()
        dedupe = _CountingDedupeStore()
        config = MonitoringConfig(enabled=True)
        warn_snap = _make_warn_snapshot()
        ok_snap = _make_ok_snapshot()

        snapshots_iter: list[SignalSnapshot] = [warn_snap]

        async def signal_source() -> list[SignalSnapshot]:
            return list(snapshots_iter)

        daemon = MonitoringDaemon(
            config=config,
            signal_source=signal_source,
            sink=sink,
            dedupe=dedupe,
        )
        await daemon._tick()  # WARN fired
        snapshots_iter[:] = [ok_snap]
        report = await daemon._tick()  # OK → RESOLVED
        self.assertEqual(report.resolved_signals, 1)
        self.assertIn("RESOLVED", sink.messages[-1])
        self.assertIn(SIGNAL_OLDEST_HOT_AGE.name, dedupe.resolved_signals)

    async def test_warn_to_crit_escalation_fires_even_with_dedupe(self) -> None:
        """Escalation should bypass dedupe — user wants immediate notice."""

        sink = _RecordingSink()
        # Force CRIT path even if dedupe says "block":
        dedupe = _CountingDedupeStore(schedule=[False])  # would suppress
        config = MonitoringConfig(enabled=True)
        warn_snap = _make_warn_snapshot()
        crit_snap = _make_crit_snapshot()
        snapshots_iter: list[SignalSnapshot] = [warn_snap]

        async def signal_source() -> list[SignalSnapshot]:
            return list(snapshots_iter)

        daemon = MonitoringDaemon(
            config=config, signal_source=signal_source, sink=sink, dedupe=dedupe
        )
        await daemon._tick()  # WARN
        snapshots_iter[:] = [crit_snap]
        report = await daemon._tick()  # WARN → CRIT escalation
        self.assertEqual(report.alerts_fired, 1)
        self.assertEqual(sink.messages[-1].split("\n")[0][:6], "[CRIT]")

    async def test_ok_value_does_not_alert(self) -> None:
        daemon, sink, dedupe = _make_daemon(snapshots=[_make_ok_snapshot()])
        report = await daemon._tick()
        self.assertEqual(report.alerts_fired, 0)
        self.assertEqual(sink.send_calls, 0)
        self.assertEqual(dedupe.mark_calls, [])


class TickResilienceTests(unittest.IsolatedAsyncioTestCase):
    async def test_signal_source_exception_does_not_crash(self) -> None:
        sink = _RecordingSink()
        dedupe = _CountingDedupeStore()
        config = MonitoringConfig(enabled=True)

        async def broken_source() -> list[SignalSnapshot]:
            raise RuntimeError("forced failure")

        daemon = MonitoringDaemon(
            config=config, signal_source=broken_source, sink=sink, dedupe=dedupe
        )
        report = await daemon._tick()
        self.assertEqual(report.snapshots_received, 0)
        self.assertEqual(report.alerts_fired, 0)
        self.assertIsNotNone(report.error)

    async def test_empty_snapshot_list_no_alerts(self) -> None:
        daemon, sink, dedupe = _make_daemon(snapshots=[])
        report = await daemon._tick()
        self.assertEqual(report.snapshots_received, 0)
        self.assertEqual(report.alerts_fired, 0)


class RunForeverTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_config_exits_immediately(self) -> None:
        sink = _RecordingSink()
        dedupe = _CountingDedupeStore()
        config = MonitoringConfig(enabled=False)

        async def source() -> list[SignalSnapshot]:
            return []

        daemon = MonitoringDaemon(
            config=config, signal_source=source, sink=sink, dedupe=dedupe
        )
        # Should return promptly, not loop forever.
        await asyncio.wait_for(daemon.run_forever(), timeout=2.0)

    async def test_cancellation_propagates_cleanly(self) -> None:
        sink = _RecordingSink()
        dedupe = _CountingDedupeStore()
        config = MonitoringConfig(enabled=True, interval_seconds=0.01)

        async def source() -> list[SignalSnapshot]:
            return [_make_ok_snapshot()]

        daemon = MonitoringDaemon(
            config=config, signal_source=source, sink=sink, dedupe=dedupe
        )
        task = asyncio.create_task(daemon.run_forever())
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task


class CounterAccountingTests(unittest.IsolatedAsyncioTestCase):
    async def test_total_alerts_fired_increments(self) -> None:
        daemon, _, _ = _make_daemon(snapshots=[_make_warn_snapshot()])
        await daemon._tick()
        self.assertEqual(daemon.total_alerts_fired, 1)
        self.assertEqual(daemon.total_ticks, 1)


if __name__ == "__main__":
    unittest.main()
