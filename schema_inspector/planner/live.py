"""Live polling lane decisions for the hybrid ETL planner."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LivePollingDecision:
    lane: str
    next_poll_seconds: int | None
    terminal: bool


def classify_live_polling(*, status_type: str | None, minutes_to_start: int | None) -> LivePollingDecision:
    normalized = str(status_type or "").strip().lower()
    if normalized in {"inprogress", "live"}:
        return LivePollingDecision(lane="hot", next_poll_seconds=10, terminal=False)
    if normalized in {"finished", "afterextra", "afterpen", "cancelled", "postponed"}:
        return LivePollingDecision(lane="terminal", next_poll_seconds=None, terminal=True)
    if minutes_to_start is not None and minutes_to_start <= 30:
        return LivePollingDecision(lane="warm", next_poll_seconds=60, terminal=False)
    return LivePollingDecision(lane="cold", next_poll_seconds=300, terminal=False)
