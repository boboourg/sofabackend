"""Live polling lane decisions for the hybrid ETL planner."""

from __future__ import annotations

from dataclasses import dataclass

ACTIVE_LIVE_STATUS_TYPES = frozenset(
    {
        "inprogress",
        "live",
        "halftime",
        "paused",
        "pause",
        "break",
        "interrupted",
        "overtime",
        "extra",
        "awaitingextra",
        "awaitingpenalties",
        "penalties",
    }
)
TERMINAL_STATUS_TYPES = frozenset({"finished", "afterextra", "afterpen", "cancelled", "postponed"})


@dataclass(frozen=True)
class LivePollingDecision:
    lane: str
    next_poll_seconds: int | None
    terminal: bool


def classify_live_polling(*, status_type: str | None, minutes_to_start: int | None) -> LivePollingDecision:
    normalized = str(status_type or "").strip().lower()
    if normalized in ACTIVE_LIVE_STATUS_TYPES:
        return LivePollingDecision(lane="hot", next_poll_seconds=10, terminal=False)
    if normalized in TERMINAL_STATUS_TYPES:
        return LivePollingDecision(lane="terminal", next_poll_seconds=None, terminal=True)
    if minutes_to_start is not None and minutes_to_start <= 30:
        return LivePollingDecision(lane="warm", next_poll_seconds=60, terminal=False)
    return LivePollingDecision(lane="cold", next_poll_seconds=300, terminal=False)
