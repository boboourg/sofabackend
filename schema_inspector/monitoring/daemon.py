"""MonitoringDaemon — orchestrator that pulls signals and fires alerts.

Architecture mirrors :class:`PlannerDaemon` in style:

* ``run_forever`` is the public entry point (called from CLI).
* ``_tick`` is one cycle: fetch signal snapshots → classify → for each
  alert candidate, check dedupe → render message → send via sink.

The daemon is intentionally side-effect-light: it accepts a
``signal_source`` callable that returns a list of :class:`SignalSnapshot`
each tick. That allows Phase 1 to wire ``fetch_slo_signals_from_api`` and
Phase 2 to wire a composed source that also reads queue signals — no
restructure required.

Resilience: every step is wrapped in try/except. A failing signal source
results in an empty tick, not a crashed daemon. A failing alert sink
results in dedupe not getting marked, so the alert will retry next tick.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from .alerter import AlertSink
from .config import MonitoringConfig
from .dedupe import DedupeStore
from .signals import SignalSnapshot, format_alert_message


logger = logging.getLogger(__name__)


SignalSource = Callable[[], Awaitable[list[SignalSnapshot]]]
Clock = Callable[[], datetime]


@dataclass(frozen=True)
class TickReport:
    """Diagnostic snapshot of one tick. Returned by ``_tick`` and used in tests."""

    snapshots_received: int
    alerts_fired: int
    alerts_suppressed: int
    resolved_signals: int
    error: str | None = None


def _default_clock() -> datetime:
    return datetime.now(timezone.utc)


class MonitoringDaemon:
    def __init__(
        self,
        *,
        config: MonitoringConfig,
        signal_source: SignalSource,
        sink: AlertSink,
        dedupe: DedupeStore,
        clock: Clock = _default_clock,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.config = config
        self.signal_source = signal_source
        self.sink = sink
        self.dedupe = dedupe
        self.clock = clock
        self.sleep = sleep
        # Remembered last severity per signal — used to detect transitions
        # from breach back to OK so we can fire RESOLVED alerts even if the
        # dedupe store would otherwise consider the signal "still alerted".
        # In-memory only — daemon restarts forget state but the next tick
        # will re-classify from scratch and pick up where it left off.
        self._last_severity: dict[str, str] = {}
        # Diagnostic counters surfaced for ops smoke tests.
        self.total_ticks = 0
        self.total_alerts_fired = 0
        self.total_alerts_suppressed = 0
        self.total_resolved_alerts = 0

    async def run_forever(self) -> None:
        """Loop until cancelled. Each iteration runs ``_tick`` and sleeps."""

        if not self.config.enabled:
            logger.info(
                "MonitoringDaemon: SOFASCORE_MONITORING_ENABLED=0, exiting"
            )
            return
        logger.info(
            "MonitoringDaemon: starting interval=%.1fs base_url=%s host_label=%s",
            self.config.interval_seconds,
            self.config.base_url,
            self.config.host_label,
        )
        try:
            while True:
                await self._tick()
                await self.sleep(self.config.interval_seconds)
        except asyncio.CancelledError:
            logger.info("MonitoringDaemon: cancelled, exiting cleanly")
            raise

    async def _tick(self) -> TickReport:
        self.total_ticks += 1
        snapshots: list[SignalSnapshot]
        try:
            snapshots = await self.signal_source()
        except Exception as exc:  # noqa: BLE001 — never crash daemon
            logger.warning("MonitoringDaemon: signal_source failed: %r", exc)
            return TickReport(
                snapshots_received=0,
                alerts_fired=0,
                alerts_suppressed=0,
                resolved_signals=0,
                error=repr(exc),
            )
        if not snapshots:
            return TickReport(
                snapshots_received=0,
                alerts_fired=0,
                alerts_suppressed=0,
                resolved_signals=0,
            )
        alerts_fired = 0
        alerts_suppressed = 0
        resolved_signals = 0
        for snapshot in snapshots:
            try:
                outcome = await self._process_snapshot(snapshot)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "MonitoringDaemon: process_snapshot failed signal=%s err=%r",
                    snapshot.name,
                    exc,
                )
                continue
            if outcome == "fired":
                alerts_fired += 1
                self.total_alerts_fired += 1
            elif outcome == "suppressed":
                alerts_suppressed += 1
                self.total_alerts_suppressed += 1
            elif outcome == "resolved":
                resolved_signals += 1
                self.total_resolved_alerts += 1
        return TickReport(
            snapshots_received=len(snapshots),
            alerts_fired=alerts_fired,
            alerts_suppressed=alerts_suppressed,
            resolved_signals=resolved_signals,
        )

    async def _process_snapshot(self, snapshot: SignalSnapshot) -> str:
        severity = snapshot.severity
        previous_severity = self._last_severity.get(snapshot.name, "OK")
        self._last_severity[snapshot.name] = severity

        if severity == "OK":
            if previous_severity in ("WARN", "CRIT"):
                # Transition back to healthy — fire RESOLVED and clear dedupe
                # so the next breach starts a fresh series.
                first_seen_epoch = self.dedupe.get_first_alerted_at_epoch(
                    snapshot.name, previous_severity
                )
                self.dedupe.record_resolved(snapshot.name)
                message = format_alert_message(
                    snapshot,
                    host_label=self.config.host_label,
                    resolved=True,
                    first_alerted_at=_epoch_to_datetime(first_seen_epoch),
                )
                ok = await self.sink.send(message)
                return "resolved" if ok else "suppressed"
            return "ok"

        # severity in (WARN, CRIT)
        ttl_seconds = (
            self.config.crit_ttl_seconds
            if severity == "CRIT"
            else self.config.warn_ttl_seconds
        )
        # An escalation (WARN→CRIT) always sends — the user wants to see
        # the severity bump immediately rather than wait for crit_ttl.
        force_send = previous_severity in ("OK", "WARN") and severity == "CRIT" and previous_severity != "CRIT"
        # A WARN that follows OK→CRIT→OK→WARN should also send (no dedupe
        # record at this severity yet → should_send returns True naturally).
        if force_send or self.dedupe.should_send(
            snapshot.name, severity, ttl_seconds
        ):
            repeat_count = self.dedupe.get_repeat_count(snapshot.name, severity) + 1
            first_seen_epoch = self.dedupe.get_first_alerted_at_epoch(
                snapshot.name, severity
            )
            message = format_alert_message(
                snapshot,
                host_label=self.config.host_label,
                repeat_count=repeat_count,
                first_alerted_at=_epoch_to_datetime(first_seen_epoch),
            )
            ok = await self.sink.send(message)
            if ok:
                self.dedupe.mark_sent(snapshot.name, severity, ttl_seconds)
                return "fired"
            return "suppressed"
        return "suppressed"


def _epoch_to_datetime(epoch: float | None) -> datetime | None:
    if epoch is None:
        return None
    try:
        return datetime.fromtimestamp(float(epoch), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None
