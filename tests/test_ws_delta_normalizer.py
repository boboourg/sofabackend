"""Tests for the WebSocket delta normalizer.

The normalizer is a pure function that takes a single NATS-pushed
event-type or odds-type delta (already JSON-decoded) and produces a
NormalizedDelta bundle: per-table UPSERT rows that the consumer's
writer applies in a single transaction.

No DB, no asyncio — these are unit tests of the dot-notation mapping
and the JSON-shaped fields (status, statusTime, time.playedLastUpdated).
"""
from __future__ import annotations
import unittest
from datetime import datetime


class NormalizeEventDeltaTests(unittest.TestCase):
    def test_empty_payload_returns_no_writes(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({})
        self.assertIsNone(result)

    def test_payload_without_id_returns_none(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        # Without an event id, we cannot route the upsert.
        result = normalize_event_delta({"homeScore.current": 1})
        self.assertIsNone(result)

    def test_id_only_payload_produces_empty_bundle(self) -> None:
        """An "id-only" delta carries no state — the normalizer should
        produce a NormalizedDelta with no row writes (but still record
        the event_id, so downstream cache invalidation can fire)."""
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({"id": 16167494, "changes.changeTimestamp": 1779063160})
        self.assertIsNotNone(result)
        self.assertEqual(result.event_id, 16167494)
        # change_timestamp goes through change_items, not into a column.
        self.assertEqual(result.change_timestamp, 1779063160)
        # No score / status / time writes.
        self.assertEqual(result.event_score_rows, [])
        self.assertEqual(result.event_status_fields, {})
        self.assertEqual(result.event_time_fields, {})

    def test_home_score_current_maps_to_event_score_home(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 16167494,
            "homeScore.current": 2,
            "homeScore.display": 2,
        })
        self.assertEqual(result.event_score_rows, [
            {"side": "home", "current": 2, "display": 2}
        ])

    def test_away_score_period_maps_to_event_score_away(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 100,
            "awayScore.period1": 3,
            "awayScore.period2": 1,
        })
        self.assertEqual(result.event_score_rows, [
            {"side": "away", "period1": 3, "period2": 1}
        ])

    def test_home_and_away_in_same_delta_produce_two_rows(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 100,
            "homeScore.current": 1,
            "awayScore.current": 0,
        })
        # rows ordered home, away
        sides = [r["side"] for r in result.event_score_rows]
        self.assertEqual(sorted(sides), ["away", "home"])

    def test_tennis_point_score_is_preserved_as_string(self) -> None:
        """awayScore.point is "15"/"30"/"40"/"AD" — keep it as string."""
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 100,
            "awayScore.point": "15",
        })
        # event_score.point doesn't exist as a column — we route via
        # the tennis-specific field. For Stage 1 we capture it as a
        # ``string_extras`` map so the writer can decide how to store it.
        self.assertIn("point", result.event_score_rows[0])
        self.assertEqual(result.event_score_rows[0]["point"], "15")

    def test_status_code_maps_to_event_table(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 100,
            "status.code": 6,
            "status.description": "1st half",
            "status.type": "inprogress",
        })
        self.assertEqual(result.event_status_fields, {
            "code": 6,
            "description": "1st half",
            "type": "inprogress",
        })

    def test_winner_code_maps_to_event_table(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({"id": 100, "winnerCode": 1})
        self.assertEqual(result.event_fields, {"winner_code": 1})

    def test_last_period_maps_to_event_table(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({"id": 100, "lastPeriod": "period2"})
        self.assertEqual(result.event_fields, {"last_period": "period2"})

    def test_first_to_serve_maps_to_event_table(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({"id": 100, "firstToServe": 2})
        self.assertEqual(result.event_fields, {"first_to_serve": 2})

    def test_time_fields_map_to_event_time(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 100,
            "time.initial": 2700,
            "time.max": 5400,
            "time.extra": 540,
            "time.currentPeriodStartTimestamp": 1779063160,
            "time.played": 600,
        })
        self.assertEqual(result.event_time_fields, {
            "initial": 2700,
            "max": 5400,
            "extra": 540,
            "current_period_start_timestamp": 1779063160,
            "played": 600,
        })

    def test_time_played_last_updated_parses_pgsql_dict(self) -> None:
        """time.playedLastUpdated comes as
        {"date": "2026-04-25 11:07:05.802240", "timezone_type": 3, "timezone": "UTC"}
        — extract the date into a datetime."""
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 100,
            "time.playedLastUpdated": {
                "date": "2026-04-25 11:07:05.802240",
                "timezone_type": 3,
                "timezone": "UTC",
            },
        })
        dt = result.event_time_fields["played_last_updated"]
        self.assertIsInstance(dt, datetime)
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 4)
        self.assertEqual(dt.day, 25)

    def test_clock_running_maps_with_timestamp(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 100,
            "time.clockRunning": True,
            "time.clockRunningLastUpdated": {
                "date": "2026-04-25 11:07:05.802240",
                "timezone_type": 3,
                "timezone": "UTC",
            },
        })
        self.assertEqual(result.event_time_fields["clock_running"], True)
        self.assertIsInstance(
            result.event_time_fields["clock_running_last_updated"], datetime
        )

    def test_statusTime_object_decomposes_into_event_status_time_fields(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 100,
            "statusTime": {
                "prefix": "",
                "initial": 2700,
                "max": 5400,
                "timestamp": 1777115225,
                "extra": 540,
            },
        })
        self.assertEqual(result.event_status_time_fields, {
            "prefix": "",
            "initial": 2700,
            "max": 5400,
            "timestamp": 1777115225,
            "extra": 540,
        })

    def test_currentPeriodStartTimestamp_top_level_maps_to_event_time(self) -> None:
        """Sometimes the delta carries currentPeriodStartTimestamp at
        the top level too (in addition to time.currentPeriodStartTimestamp).
        Both must point at the same column — last one wins (they are
        identical in practice)."""
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 100,
            "currentPeriodStartTimestamp": 1779063160,
        })
        self.assertEqual(result.event_time_fields["current_period_start_timestamp"], 1779063160)

    def test_status_description_redundant_top_level(self) -> None:
        """statusDescription (top-level) duplicates status.description.
        Both should land on event_status_fields[description] — taking
        the more authoritative ``status.description`` if both present."""
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 100,
            "status.description": "2nd half",
            "statusDescription": "46",   # human label, less authoritative
        })
        # status.description wins; we capture statusDescription as
        # status_description_label in event_status_fields so the writer
        # can decide which column it maps to.
        self.assertEqual(result.event_status_fields.get("description"), "2nd half")

    def test_complete_realworld_delta_yields_full_bundle(self) -> None:
        """A real archive sample with multiple fields at once."""
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "status.code": 7,
            "status.description": "2nd half",
            "homeScore.period2": 0,
            "awayScore.period2": 0,
            "statusDescription": "46",
            "time.currentPeriodStartTimestamp": 1777115225,
            "time.initial": 2700,
            "time.max": 5400,
            "time.extra": 540,
            "currentPeriodStartTimestamp": 1777115225,
            "statusTime": {
                "prefix": "",
                "initial": 2700,
                "max": 5400,
                "timestamp": 1777115225,
                "extra": 540,
            },
            "lastPeriod": "period2",
            "changes.changeTimestamp": 1777115225,
            "id": 16046462,
        })
        self.assertEqual(result.event_id, 16046462)
        self.assertEqual(result.change_timestamp, 1777115225)
        self.assertEqual(result.event_status_fields["code"], 7)
        self.assertEqual(result.event_status_fields["description"], "2nd half")
        self.assertEqual(result.event_fields["last_period"], "period2")
        self.assertEqual(result.event_time_fields["initial"], 2700)
        self.assertEqual(result.event_time_fields["max"], 5400)
        self.assertEqual(result.event_status_time_fields["timestamp"], 1777115225)
        # Both home and away period2 → two event_score rows.
        sides = {r["side"]: r for r in result.event_score_rows}
        self.assertEqual(sides["home"]["period2"], 0)
        self.assertEqual(sides["away"]["period2"], 0)


class VarAndCardsTests(unittest.TestCase):
    """varInProgress + cardsCode coverage (closes the last 0.7% of
    archive event-delta keys)."""

    def test_var_in_progress_maps_to_event_var_in_progress(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 15372969,
            "varInProgress": {"homeTeam": False, "awayTeam": True},
        })
        self.assertEqual(result.event_var_in_progress, {
            "home_team": False,
            "away_team": True,
        })

    def test_cards_code_maps_to_red_card_columns(self) -> None:
        """cardsCode "01" -> home_red_cards=0, away_red_cards=1."""
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({"id": 1, "cardsCode": "01"})
        self.assertEqual(result.event_fields["home_red_cards"], 0)
        self.assertEqual(result.event_fields["away_red_cards"], 1)

    def test_cards_code_supports_double_digits(self) -> None:
        """Sofascore can occasionally ship two-digit per-side counts,
        e.g. cardsCode "10" -> home=1, away=0. We split half/half so
        anything beyond one digit per side falls back to leaving the
        whole string in extras (and the writer logs the unknown shape).
        """
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({"id": 1, "cardsCode": "10"})
        self.assertEqual(result.event_fields["home_red_cards"], 1)
        self.assertEqual(result.event_fields["away_red_cards"], 0)

    def test_cards_code_invalid_format_dropped(self) -> None:
        """An unparseable cardsCode (e.g. "1-2") should be ignored
        instead of polluting the bundle."""
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({"id": 1, "cardsCode": "1-2"})
        self.assertNotIn("home_red_cards", result.event_fields)
        self.assertNotIn("away_red_cards", result.event_fields)

    def test_event_state_timestamp_and_indicator_ignored(self) -> None:
        """eventState.* is derived from varInProgress + side; no
        independent table. We drop these keys silently."""
        from schema_inspector.ws_delta_normalizer import normalize_event_delta

        result = normalize_event_delta({
            "id": 1,
            "eventState.timestamp": 1777104868,
            "eventState.statusIndicator": "varInProgressHome",
        })
        # No fields produced (event_var_in_progress map remains None/empty).
        self.assertEqual(result.event_var_in_progress, None)
        self.assertEqual(result.event_fields, {})


class NormalizeOddsDeltaTests(unittest.TestCase):
    """Odds deltas are offer-scoped (the id is the bookmaker offer id,
    not event_id). For Stage 1 we capture them as a separate bundle and
    skip writing — Stage 2 will wire them into the odds tables."""

    def test_empty_payload(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_odds_delta

        self.assertIsNone(normalize_odds_delta({}))

    def test_offer_id_required(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_odds_delta

        self.assertIsNone(normalize_odds_delta({"choice1.fractionalValue": "1/2"}))

    def test_parses_choices(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_odds_delta

        bundle = normalize_odds_delta({
            "id": 306234958,
            "choice1.fractionalValue": "5/6",
            "choice2.fractionalValue": "7/2",
            "choice3.fractionalValue": "11/5",
        })
        self.assertEqual(bundle.offer_id, 306234958)
        self.assertEqual(bundle.choices, {
            1: {"fractionalValue": "5/6"},
            2: {"fractionalValue": "7/2"},
            3: {"fractionalValue": "11/5"},
        })

    def test_initial_fractional_value(self) -> None:
        from schema_inspector.ws_delta_normalizer import normalize_odds_delta

        bundle = normalize_odds_delta({
            "id": 306234958,
            "choice1.initialFractionalValue": "1/2",
            "choice1.fractionalValue": "5/6",
        })
        self.assertEqual(bundle.choices[1], {
            "initialFractionalValue": "1/2",
            "fractionalValue": "5/6",
        })


if __name__ == "__main__":
    unittest.main()
