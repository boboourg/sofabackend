"""Thin worker wrapper for live jobs."""

from __future__ import annotations

from ..planner.live import classify_live_polling


class LiveWorker:
    def handle(self, *, status_type: str | None, minutes_to_start: int | None):
        return classify_live_polling(status_type=status_type, minutes_to_start=minutes_to_start)
