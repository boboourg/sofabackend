"""Tests for the synthesizer that builds Sofascore-style scheduled-events
payloads from our normalized tables.

The synthesizer lets the local API answer
``/api/v1/sport/{slug}/scheduled-events/{date}`` for any date covered by
historical-tournament backfill (typically months ahead), without needing
a raw ``api_payload_snapshot`` for that specific date.

Pure function over a list of joined-row dicts — DB is the JOIN driver,
this module is the JSON assembler. Tests pass row dicts in directly so
we can isolate payload-shape correctness from SQL correctness.
"""

from __future__ import annotations

import unittest


class BuildPayloadEmptyTests(unittest.TestCase):
    """Empty / missing input cases."""

    def test_empty_rows_returns_empty_events_envelope(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([])

        self.assertEqual(result, {"events": []})


def _minimal_row(**overrides: object) -> dict[str, object]:
    """Build a row dict that satisfies every scalar field the synthesizer
    expects. Tests then override only the fields they care about."""
    base = {
        # ---- event scalars ----
        "event_id": 13981512,
        "event_slug": "as-roma-parma",
        "custom_id": "Pdbsceb",
        "detail_id": 1,
        "start_timestamp": 1761762961,
        "winner_code": 1,
        "aggregated_winner_code": None,
        "has_xg": True,
        "has_global_highlights": False,
        "has_event_player_statistics": True,
        "has_event_player_heat_map": True,
        "feed_locked": False,
        "is_editor": False,
        "show_toto_promo": False,
        "crowdsourcing_enabled": False,
        "crowdsourcing_data_display_enabled": False,
        "final_result_only": False,
        "coverage": None,
        "home_red_cards": 0,
        "away_red_cards": 0,
        "previous_leg_event_id": None,
        "cup_matches_in_round": None,
        "default_period_count": 2,
        "default_period_length": 45,
        "default_overtime_length": 15,
        "last_period": None,
        # ---- status (FK lookup) ----
        "status_code": 100,
        "status_type": "finished",
        "status_description": "Ended",
        # ---- season ----
        "season_id": 76457,
        "season_name": "Serie A 25/26",
        "season_year": "25/26",
        "season_editor": False,
        # ---- home team ----
        "home_team_id": 2702,
        "home_team_name": "AS Roma",
        "home_team_slug": "roma",
        "home_team_short_name": "Roma",
        "home_team_full_name": None,
        "home_team_name_code": "ROM",
        "home_team_gender": "M",
        "home_team_type": 0,
        "home_team_national": False,
        "home_team_disabled": False,
        "home_team_user_count": 774899,
        "home_team_team_colors": {"text": "#ffffff", "primary": "#811117", "secondary": "#fcb826"},
        "home_team_field_translations": {"nameTranslation": {"ru": "Рома"}, "shortNameTranslation": {}},
        "home_team_country_alpha2": "IT",
        "home_team_country_name": "Italy",
        "home_team_country_slug": "italy",
        "home_team_country_alpha3": "ITA",
        "home_team_sport_id": 1,
        "home_team_sport_name": "Football",
        "home_team_sport_slug": "football",
        # ---- away team ----
        "away_team_id": 2690,
        "away_team_name": "Parma",
        "away_team_slug": "parma",
        "away_team_short_name": "Parma",
        "away_team_full_name": None,
        "away_team_name_code": "PAR",
        "away_team_gender": "M",
        "away_team_type": 0,
        "away_team_national": False,
        "away_team_disabled": False,
        "away_team_user_count": 105201,
        "away_team_team_colors": {"text": "#ffffff", "primary": "#374df5", "secondary": "#374df5"},
        "away_team_field_translations": {"nameTranslation": {"ru": "Парма Кальчо"}, "shortNameTranslation": {}},
        "away_team_country_alpha2": "IT",
        "away_team_country_name": "Italy",
        "away_team_country_slug": "italy",
        "away_team_country_alpha3": "ITA",
        "away_team_sport_id": 1,
        "away_team_sport_name": "Football",
        "away_team_sport_slug": "football",
        # ---- tournament ----
        "tournament_id": 23,
        "tournament_name": "Serie A",
        "tournament_slug": "serie-a",
        "tournament_priority": 305,
        "tournament_is_group": False,
        "tournament_is_live": False,
        "tournament_competition_type": 1,
        "tournament_group_name": None,
        "tournament_group_sign": None,
        "tournament_field_translations": None,
        # ---- unique tournament ----
        "ut_id": 23,
        "ut_name": "Serie A",
        "ut_slug": "serie-a",
        "ut_user_count": 1234567,
        "ut_has_event_player_statistics": True,
        "ut_has_performance_graph_feature": True,
        "ut_tier": 1,
        "ut_primary_color_hex": "#1c5b9f",
        "ut_secondary_color_hex": "#1c5b9f",
        # ---- category ----
        "category_id": 31,
        "category_name": "Italy",
        "category_slug": "italy",
        "category_priority": 350,
        "category_flag": "italy",
        "category_sport_id": 1,
        "category_sport_name": "Football",
        "category_sport_slug": "football",
        # ---- score (left-join, may be NULL for not-started) ----
        "home_score_current": 1,
        "home_score_display": 1,
        "home_score_aggregated": None,
        "home_score_normaltime": 1,
        "home_score_overtime": None,
        "home_score_penalties": None,
        "home_score_period1": 0,
        "home_score_period2": 1,
        "away_score_current": 0,
        "away_score_display": 0,
        "away_score_aggregated": None,
        "away_score_normaltime": 0,
        "away_score_overtime": None,
        "away_score_penalties": None,
        "away_score_period1": 0,
        "away_score_period2": 0,
        # ---- round info ----
        "round_number": 10,
        "round_slug": None,
        "round_name": None,
        "round_cup_round_type": None,
        # ---- changes (jsonb pre-aggregated from event_change_item) ----
        "changes_payload": None,
        # ---- time (event_status_time) ----
        "time_injury_time_1": None,
        "time_injury_time_2": None,
        "time_current_period_start_timestamp": None,
    }
    base.update(overrides)
    return base


class BuildPayloadEventScalarsTests(unittest.TestCase):
    """Event-level scalar fields appear in the emitted dict."""

    def test_single_event_has_top_level_scalar_fields(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        self.assertEqual(len(result["events"]), 1)
        event = result["events"][0]
        self.assertEqual(event["id"], 13981512)
        self.assertEqual(event["slug"], "as-roma-parma")
        self.assertEqual(event["customId"], "Pdbsceb")
        self.assertEqual(event["detailId"], 1)
        self.assertEqual(event["startTimestamp"], 1761762961)
        self.assertEqual(event["winnerCode"], 1)
        self.assertTrue(event["hasXg"])
        self.assertFalse(event["hasGlobalHighlights"])
        self.assertTrue(event["hasEventPlayerStatistics"])
        self.assertTrue(event["hasEventPlayerHeatMap"])
        self.assertFalse(event["feedLocked"])
        self.assertFalse(event["isEditor"])
        self.assertFalse(event["crowdsourcingEnabled"])
        self.assertFalse(event["finalResultOnly"])


class BuildPayloadTeamTests(unittest.TestCase):
    """Verify homeTeam / awayTeam are full Sofascore-shape team objects."""

    def test_home_team_has_top_level_scalars(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        home = result["events"][0]["homeTeam"]
        self.assertEqual(home["id"], 2702)
        self.assertEqual(home["name"], "AS Roma")
        self.assertEqual(home["slug"], "roma")
        self.assertEqual(home["shortName"], "Roma")
        self.assertEqual(home["nameCode"], "ROM")
        self.assertEqual(home["gender"], "M")
        self.assertEqual(home["type"], 0)
        self.assertFalse(home["national"])
        self.assertFalse(home["disabled"])
        self.assertEqual(home["userCount"], 774899)
        # sub-teams empty array is the Sofascore convention for regular teams.
        self.assertEqual(home["subTeams"], [])

    def test_home_team_has_nested_sport_object(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        sport = result["events"][0]["homeTeam"]["sport"]
        self.assertEqual(sport, {"id": 1, "name": "Football", "slug": "football"})

    def test_home_team_has_nested_country_object(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        country = result["events"][0]["homeTeam"]["country"]
        self.assertEqual(country, {"name": "Italy", "slug": "italy", "alpha2": "IT", "alpha3": "ITA"})

    def test_home_team_includes_team_colors_and_field_translations(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        home = result["events"][0]["homeTeam"]
        self.assertEqual(
            home["teamColors"],
            {"text": "#ffffff", "primary": "#811117", "secondary": "#fcb826"},
        )
        self.assertEqual(
            home["fieldTranslations"],
            {"nameTranslation": {"ru": "Рома"}, "shortNameTranslation": {}},
        )

    def test_away_team_uses_away_columns_not_home(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        away = result["events"][0]["awayTeam"]
        self.assertEqual(away["id"], 2690)
        self.assertEqual(away["name"], "Parma")
        self.assertEqual(away["nameCode"], "PAR")
        self.assertEqual(away["userCount"], 105201)

    def test_team_country_omitted_when_country_alpha2_is_null(self) -> None:
        """Some teams (rare) lack country_alpha2 — payload should still be valid."""
        from schema_inspector.scheduled_events_synthesizer import build_payload

        row = _minimal_row(
            home_team_country_alpha2=None,
            home_team_country_name=None,
            home_team_country_slug=None,
            home_team_country_alpha3=None,
        )
        result = build_payload([row])

        home = result["events"][0]["homeTeam"]
        # When country data is missing we still emit the key with an empty
        # dict so frontend type-checkers don't crash.
        self.assertEqual(home["country"], {})


class BuildPayloadTournamentTests(unittest.TestCase):
    """Tournament + nested uniqueTournament + nested category."""

    def test_tournament_has_top_level_fields(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        t = result["events"][0]["tournament"]
        self.assertEqual(t["id"], 23)
        self.assertEqual(t["name"], "Serie A")
        self.assertEqual(t["slug"], "serie-a")
        self.assertEqual(t["priority"], 305)
        self.assertEqual(t["competitionType"], 1)
        self.assertFalse(t["isGroup"])
        self.assertFalse(t["isLive"])

    def test_tournament_includes_unique_tournament_nested(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        ut = result["events"][0]["tournament"]["uniqueTournament"]
        self.assertEqual(ut["id"], 23)
        self.assertEqual(ut["name"], "Serie A")
        self.assertEqual(ut["slug"], "serie-a")
        self.assertEqual(ut["userCount"], 1234567)
        self.assertTrue(ut["hasEventPlayerStatistics"])
        self.assertTrue(ut["hasPerformanceGraphFeature"])
        self.assertEqual(ut["primaryColorHex"], "#1c5b9f")

    def test_tournament_includes_category_nested(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        cat = result["events"][0]["tournament"]["category"]
        self.assertEqual(cat["id"], 31)
        self.assertEqual(cat["name"], "Italy")
        self.assertEqual(cat["slug"], "italy")
        self.assertEqual(cat["flag"], "italy")
        self.assertEqual(cat["sport"], {"id": 1, "name": "Football", "slug": "football"})


class BuildPayloadSeasonTests(unittest.TestCase):
    def test_event_includes_season_nested(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        season = result["events"][0]["season"]
        self.assertEqual(season, {"id": 76457, "name": "Serie A 25/26", "year": "25/26", "editor": False})


class BuildPayloadScoreTests(unittest.TestCase):
    def test_home_and_away_scores_with_period_breakdown(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        home_score = result["events"][0]["homeScore"]
        away_score = result["events"][0]["awayScore"]
        self.assertEqual(home_score["current"], 1)
        self.assertEqual(home_score["display"], 1)
        self.assertEqual(home_score["normaltime"], 1)
        self.assertEqual(home_score["period1"], 0)
        self.assertEqual(home_score["period2"], 1)
        self.assertEqual(away_score["current"], 0)
        self.assertEqual(away_score["period2"], 0)

    def test_scores_default_to_zero_when_event_score_row_is_missing(self) -> None:
        """For not-started events we still emit a homeScore / awayScore
        envelope, matching Sofascore's behavior (they return
        ``{"current": 0, "display": 0}`` for upcoming matches)."""
        from schema_inspector.scheduled_events_synthesizer import build_payload

        row = _minimal_row(
            home_score_current=None,
            home_score_display=None,
            home_score_period1=None,
            home_score_period2=None,
            home_score_normaltime=None,
            away_score_current=None,
            away_score_display=None,
            away_score_period1=None,
            away_score_period2=None,
            away_score_normaltime=None,
        )
        result = build_payload([row])

        home = result["events"][0]["homeScore"]
        away = result["events"][0]["awayScore"]
        self.assertEqual(home["current"], 0)
        self.assertEqual(home["display"], 0)
        self.assertEqual(away["current"], 0)
        self.assertEqual(away["display"], 0)


class BuildPayloadRoundInfoTests(unittest.TestCase):
    def test_event_includes_round_info_when_round_number_present(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        round_info = result["events"][0]["roundInfo"]
        self.assertEqual(round_info, {"round": 10})

    def test_event_includes_round_info_with_slug_name_for_cup_round(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        row = _minimal_row(
            round_number=None,
            round_slug="quarterfinal",
            round_name="Quarter-final",
            round_cup_round_type=4,
        )
        result = build_payload([row])

        round_info = result["events"][0]["roundInfo"]
        self.assertEqual(round_info["slug"], "quarterfinal")
        self.assertEqual(round_info["name"], "Quarter-final")
        self.assertEqual(round_info["cupRoundType"], 4)

    def test_round_info_omitted_when_all_fields_null(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        row = _minimal_row(
            round_number=None,
            round_slug=None,
            round_name=None,
            round_cup_round_type=None,
        )
        result = build_payload([row])

        # Sofascore drops the key entirely when there is no round info.
        self.assertNotIn("roundInfo", result["events"][0])


class BuildPayloadTimeTests(unittest.TestCase):
    def test_time_included_for_in_progress_event(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        row = _minimal_row(
            time_injury_time_1=4,
            time_injury_time_2=3,
            time_current_period_start_timestamp=1761762961,
        )
        result = build_payload([row])

        time = result["events"][0]["time"]
        self.assertEqual(time["injuryTime1"], 4)
        self.assertEqual(time["injuryTime2"], 3)
        self.assertEqual(time["currentPeriodStartTimestamp"], 1761762961)

    def test_time_omitted_for_not_started_event(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        # All time_* in _minimal_row are None by default.
        result = build_payload([_minimal_row()])

        self.assertNotIn("time", result["events"][0])


class BuildPayloadChangesTests(unittest.TestCase):
    def test_changes_block_included_when_changes_payload_present(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        row = _minimal_row(
            changes_payload={
                "changes": ["status.code", "status.type"],
                "changeTimestamp": 1761765853,
            },
        )
        result = build_payload([row])

        self.assertEqual(
            result["events"][0]["changes"],
            {"changes": ["status.code", "status.type"], "changeTimestamp": 1761765853},
        )

    def test_changes_omitted_when_payload_is_null(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        self.assertNotIn("changes", result["events"][0])


class BuildPayloadOrderingTests(unittest.TestCase):
    def test_events_are_emitted_in_row_order_caller_decides_ordering(self) -> None:
        """The synthesizer does not re-sort. SQL ORDER BY drives the order."""
        from schema_inspector.scheduled_events_synthesizer import build_payload

        rows = [
            _minimal_row(event_id=1, start_timestamp=2000),
            _minimal_row(event_id=2, start_timestamp=1000),
            _minimal_row(event_id=3, start_timestamp=3000),
        ]
        result = build_payload(rows)

        ids = [event["id"] for event in result["events"]]
        self.assertEqual(ids, [1, 2, 3])


class BuildPayloadStatusTests(unittest.TestCase):
    def test_event_status_nested_object_with_code_type_description(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        result = build_payload([_minimal_row()])

        status = result["events"][0]["status"]
        self.assertEqual(status, {"code": 100, "type": "finished", "description": "Ended"})

    def test_event_status_for_not_started_event(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_payload

        row = _minimal_row(
            status_code=0,
            status_type="notstarted",
            status_description="Not started",
        )
        result = build_payload([row])

        status = result["events"][0]["status"]
        self.assertEqual(status["code"], 0)
        self.assertEqual(status["type"], "notstarted")
        self.assertEqual(status["description"], "Not started")


class BuildSingleEventPayloadTests(unittest.TestCase):
    """build_single_event_payload wraps one event in {"event": {...}}
    envelope — the Sofascore shape for /api/v1/event/{event_id}."""

    def test_single_event_envelope_uses_event_key_not_events(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_single_event_payload

        result = build_single_event_payload(_minimal_row())

        self.assertIn("event", result)
        self.assertNotIn("events", result)
        self.assertEqual(result["event"]["id"], 13981512)

    def test_single_event_includes_full_team_and_tournament_shape(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_single_event_payload

        result = build_single_event_payload(_minimal_row())

        event = result["event"]
        # All the nested objects we tested for the list endpoint should
        # also appear in the single-event envelope — same _build_event.
        self.assertEqual(event["homeTeam"]["country"]["alpha2"], "IT")
        self.assertEqual(event["tournament"]["uniqueTournament"]["id"], 23)
        self.assertEqual(event["status"]["type"], "finished")


class FetchSingleEventRowContractTests(unittest.IsolatedAsyncioTestCase):
    """fetch_single_event_row pins one event by id and returns the same
    column shape as the list fetchers (so build_event works unchanged)."""

    async def test_fetch_single_event_row_passes_event_id_as_first_param(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import fetch_single_event_row

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetchrow(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return None

        result = await fetch_single_event_row(_StubConn(), event_id=14023956)

        self.assertIsNone(result)
        self.assertEqual(captured["args"], (14023956,))
        self.assertIn("WHERE e.id = $1", str(captured["query"]))

    async def test_fetch_single_event_row_decodes_jsonb(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import fetch_single_event_row

        class _StubConn:
            async def fetchrow(self, query: str, *args: object):
                return {
                    "home_team_team_colors": '{"primary": "#1c5b9f"}',
                    "home_team_field_translations": None,
                    "away_team_team_colors": None,
                    "away_team_field_translations": None,
                    "tournament_field_translations": None,
                    "changes_payload": None,
                }

        result = await fetch_single_event_row(_StubConn(), event_id=1)

        self.assertEqual(result["home_team_team_colors"], {"primary": "#1c5b9f"})


class FetchSeasonEventsRowsContractTests(unittest.IsolatedAsyncioTestCase):
    """fetch_season_events_rows powers
    /unique-tournament/{ut_id}/season/{season_id}/events/last|next/{page}.
    Same column projection as the other fetchers, filtered by UT+season,
    paginated by start_timestamp DESC (last) or ASC (next)."""

    async def test_last_direction_filters_finished_orders_desc(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import fetch_season_events_rows

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_season_events_rows(
            _StubConn(),
            unique_tournament_id=17,
            season_id=76986,
            direction="last",
            page=2,
            page_size=30,
        )
        # UT id is $1, season is $2, offset+limit are $3 + $4.
        self.assertEqual(captured["args"][:2], (17, 76986))
        # OFFSET = page * page_size = 60, LIMIT = page_size + 1 = 31
        # (the +1 lets the caller detect hasNextPage cheaply).
        self.assertEqual(captured["args"][2], 60)
        self.assertEqual(captured["args"][3], 31)
        query = str(captured["query"])
        self.assertIn("unique_tournament_id = $1", query)
        self.assertIn("season_id = $2", query)
        self.assertIn("ORDER BY e.start_timestamp DESC", query)
        # Last (finished) direction filters by finished status types.
        self.assertIn("finished", query)

    async def test_next_direction_filters_notstarted_orders_asc(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import fetch_season_events_rows

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_season_events_rows(
            _StubConn(),
            unique_tournament_id=17,
            season_id=76986,
            direction="next",
            page=0,
            page_size=30,
        )
        query = str(captured["query"])
        self.assertIn("ORDER BY e.start_timestamp ASC", query)
        self.assertIn("notstarted", query)


class FetchRoundEventsRowsContractTests(unittest.IsolatedAsyncioTestCase):
    """fetch_round_events_rows powers
    /unique-tournament/{ut_id}/season/{sid}/events/round/{round_number}.
    Filter: UT + season + event_round_info.round_number = $3."""

    async def test_filter_passes_ut_season_and_round(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import fetch_round_events_rows

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_round_events_rows(
            _StubConn(),
            unique_tournament_id=17,
            season_id=76986,
            round_number=10,
        )
        self.assertEqual(captured["args"], (17, 76986, 10))
        query = str(captured["query"])
        self.assertIn("unique_tournament_id = $1", query)
        self.assertIn("season_id = $2", query)
        # round_number is on event_round_info table, joined via eri alias.
        self.assertIn("round_number = $3", query)


class FetchUtScheduledEventsRowsContractTests(unittest.IsolatedAsyncioTestCase):
    """fetch_ut_scheduled_events_rows powers
    /unique-tournament/{ut_id}/scheduled-events/{date} — date-bounded
    list of events filtered to a single unique tournament."""

    async def test_filter_passes_ut_id_and_timestamp_range(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import fetch_ut_scheduled_events_rows

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_ut_scheduled_events_rows(
            _StubConn(),
            unique_tournament_id=17,
            start_ts=1779235200,
            end_ts=1779321600,
        )
        self.assertEqual(captured["args"], (17, 1779235200, 1779321600))
        query = str(captured["query"])
        self.assertIn("unique_tournament_id = $1", query)
        self.assertIn("start_timestamp >= $2", query)
        self.assertIn("start_timestamp <  $3", query)


class FetchTeamEventsRowsContractTests(unittest.IsolatedAsyncioTestCase):
    """fetch_team_events_rows powers /team/{team_id}/events/last|next/{page}.
    Filter: event.home_team_id = $1 OR event.away_team_id = $1. Same
    pagination contract as season events (page * page_size offset,
    page_size + 1 limit for hasNextPage detection)."""

    async def test_last_direction_filters_team_and_orders_desc(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import fetch_team_events_rows

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_team_events_rows(
            _StubConn(),
            team_id=42,
            direction="last",
            page=1,
            page_size=20,
        )
        self.assertEqual(captured["args"][0], 42)
        # offset=page*page_size=20, limit=page_size+1=21
        self.assertEqual(captured["args"][1], 20)
        self.assertEqual(captured["args"][2], 21)
        query = str(captured["query"])
        self.assertIn("home_team_id = $1", query)
        self.assertIn("away_team_id = $1", query)
        self.assertIn("ORDER BY e.start_timestamp DESC", query)

    async def test_next_direction_orders_asc_and_filters_upcoming(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import fetch_team_events_rows

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_team_events_rows(
            _StubConn(),
            team_id=42,
            direction="next",
            page=0,
        )
        query = str(captured["query"])
        self.assertIn("ORDER BY e.start_timestamp ASC", query)
        self.assertIn("notstarted", query)


class FetchLiveRowsContractTests(unittest.IsolatedAsyncioTestCase):
    """fetch_live_rows is the live-events counterpart of fetch_rows:
    same column projection, different WHERE clause (active-live status,
    last-12h window, no terminal_state, no editor events).

    These tests pin the *contract* of the function (params, return shape,
    delegation to build_payload). They do not exercise the SQL — that is
    verified via a manual prod smoke after deploy.
    """

    async def test_fetch_live_rows_passes_sport_slug_and_lookback_to_fetch(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import fetch_live_rows

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        rows = await fetch_live_rows(
            _StubConn(),
            sport_slug="football",
            lookback_hours=12,
        )

        self.assertEqual(rows, [])
        args = captured["args"]
        # Sport slug is the first parameter; the second is the list of
        # active-live status types we filter on.
        self.assertEqual(args[0], "football")
        self.assertIn("inprogress", args[1])
        self.assertIn("halftime", args[1])
        query = str(captured["query"])
        # Discriminating WHERE clauses so the query cannot silently regress
        # into the scheduled-events date-range filter.
        self.assertIn("event_terminal_state", query)
        self.assertIn("is_editor", query)
        self.assertIn("12 hours", query)

    async def test_fetch_live_rows_decodes_jsonb_columns_like_fetch_rows(self) -> None:
        """Both fetchers should reuse the same JSONB decoder so a future
        bug-fix in one path applies to the other."""
        from schema_inspector.scheduled_events_synthesizer import fetch_live_rows

        class _StubRow(dict):
            pass

        sample_row = _StubRow({
            "home_team_team_colors": '{"primary": "#1c5b9f"}',
            "home_team_field_translations": '{"nameTranslation": {}}',
            "away_team_team_colors": None,
            "away_team_field_translations": None,
            "tournament_field_translations": None,
            "changes_payload": None,
        })

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                return [sample_row]

        rows = await fetch_live_rows(_StubConn(), sport_slug="football")

        self.assertEqual(rows[0]["home_team_team_colors"], {"primary": "#1c5b9f"})


if __name__ == "__main__":
    unittest.main()
