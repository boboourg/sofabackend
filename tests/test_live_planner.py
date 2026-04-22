from __future__ import annotations

import unittest

from schema_inspector.planner.live import LivePollingDecision, classify_live_polling


class LivePlannerTests(unittest.TestCase):
    def test_live_events_are_hot(self) -> None:
        decision = classify_live_polling(status_type="inprogress", minutes_to_start=None)

        self.assertEqual(
            decision,
            LivePollingDecision(lane="hot", next_poll_seconds=10, terminal=False),
        )

    def test_starting_soon_events_are_warm(self) -> None:
        decision = classify_live_polling(status_type="scheduled", minutes_to_start=20)

        self.assertEqual(
            decision,
            LivePollingDecision(lane="warm", next_poll_seconds=600, terminal=False),
        )

    def test_distant_scheduled_events_are_warm(self) -> None:
        decision = classify_live_polling(status_type="scheduled", minutes_to_start=240)

        self.assertEqual(
            decision,
            LivePollingDecision(lane="warm", next_poll_seconds=300, terminal=False),
        )

    def test_break_status_stays_hot_with_120s(self) -> None:
        decision = classify_live_polling(status_type="halftime", minutes_to_start=None)

        self.assertEqual(
            decision,
            LivePollingDecision(lane="hot", next_poll_seconds=120, terminal=False),
        )

    def test_tennis_inprogress_uses_sport_specific_hot_poll_seconds(self) -> None:
        decision = classify_live_polling(
            status_type="inprogress",
            minutes_to_start=None,
            sport_slug="tennis",
        )

        self.assertEqual(
            decision,
            LivePollingDecision(lane="hot", next_poll_seconds=15, terminal=False),
        )

    def test_finished_events_are_terminal(self) -> None:
        decision = classify_live_polling(status_type="finished", minutes_to_start=None)

        self.assertEqual(
            decision,
            LivePollingDecision(lane="terminal", next_poll_seconds=None, terminal=True),
        )

    def test_canceled_events_are_terminal(self) -> None:
        decision = classify_live_polling(status_type="canceled", minutes_to_start=None)

        self.assertEqual(
            decision,
            LivePollingDecision(lane="terminal", next_poll_seconds=None, terminal=True),
        )


if __name__ == "__main__":
    unittest.main()
