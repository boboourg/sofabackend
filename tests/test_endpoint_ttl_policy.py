"""Tests for B1 Phase 0 per-endpoint × per-status TTL policy."""

from __future__ import annotations

import unittest

from schema_inspector.endpoint_ttl_policy import (
    STATUS_FINISHED,
    STATUS_LIVE,
    STATUS_NOT_STARTED,
    EndpointTtlSpec,
    known_football_endpoint_patterns,
    normalize_status_phase,
    resolve_endpoint_ttl,
)
from schema_inspector.endpoints import (
    EVENT_COMMENTS_ENDPOINT,
    EVENT_INCIDENTS_ENDPOINT,
    EVENT_LINEUPS_ENDPOINT,
    EVENT_MANAGERS_ENDPOINT,
    EVENT_OFFICIAL_TWEETS_ENDPOINT,
    EVENT_PLAYER_STATISTICS_ENDPOINT,
)
from schema_inspector.pipeline.pilot_orchestrator import (
    EVENT_PLAYER_DETAIL_FRESHNESS_TTL_SECONDS,
    EVENT_STATIC_DETAIL_FRESHNESS_TTL_SECONDS,
    _event_detail_freshness_fields,
)


SECOND = 1
MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR


class EndpointTtlSpecTests(unittest.TestCase):
    def test_for_phase_normalizes_aliases(self) -> None:
        spec = EndpointTtlSpec(not_started=300, live=60, finished=86_400)
        # canonical phases
        self.assertEqual(spec.for_phase("not_started"), 300)
        self.assertEqual(spec.for_phase("live"), 60)
        self.assertEqual(spec.for_phase("finished"), 86_400)
        # legacy aliases that come straight off the upstream status_type
        self.assertEqual(spec.for_phase("notstarted"), 300)
        self.assertEqual(spec.for_phase("scheduled"), 300)
        self.assertEqual(spec.for_phase("inprogress"), 60)
        self.assertEqual(spec.for_phase("aet"), 86_400)
        self.assertEqual(spec.for_phase("afterextra"), 86_400)
        # unknown
        self.assertIsNone(spec.for_phase("garbage"))
        self.assertIsNone(spec.for_phase(""))

    def test_none_phase_means_no_per_phase_override(self) -> None:
        spec = EndpointTtlSpec(not_started=None, live=60, finished=None)
        self.assertIsNone(spec.for_phase("not_started"))
        self.assertEqual(spec.for_phase("live"), 60)
        self.assertIsNone(spec.for_phase("finished"))


class ResolveEndpointTtlTests(unittest.TestCase):
    def test_football_comments_matches_bobur_matrix(self) -> None:
        # docs/football-matrix.md §7: comments live=60s, finished=24h.
        self.assertEqual(
            resolve_endpoint_ttl(
                sport_slug="football",
                endpoint_pattern=EVENT_COMMENTS_ENDPOINT.pattern,
                status_phase=STATUS_LIVE,
            ),
            60,
        )
        self.assertEqual(
            resolve_endpoint_ttl(
                sport_slug="football",
                endpoint_pattern=EVENT_COMMENTS_ENDPOINT.pattern,
                status_phase=STATUS_FINISHED,
            ),
            DAY,
        )
        # comments не whitelisted на NOT_STARTED → None
        self.assertIsNone(
            resolve_endpoint_ttl(
                sport_slug="football",
                endpoint_pattern=EVENT_COMMENTS_ENDPOINT.pattern,
                status_phase=STATUS_NOT_STARTED,
            )
        )

    def test_football_official_tweets_live_is_20_minutes(self) -> None:
        # Bobur: "20×60s" → 1200 s every live refresh
        self.assertEqual(
            resolve_endpoint_ttl(
                sport_slug="football",
                endpoint_pattern=EVENT_OFFICIAL_TWEETS_ENDPOINT.pattern,
                status_phase=STATUS_LIVE,
            ),
            20 * MINUTE,
        )

    def test_lineups_has_pregame_ttl(self) -> None:
        # Lineups confirmed=true становится стабильным за ~5min до старта.
        self.assertEqual(
            resolve_endpoint_ttl(
                sport_slug="football",
                endpoint_pattern=EVENT_LINEUPS_ENDPOINT.pattern,
                status_phase=STATUS_NOT_STARTED,
            ),
            5 * MINUTE,
        )

    def test_incidents_not_started_is_none(self) -> None:
        # Incidents не fetched до старта матча.
        self.assertIsNone(
            resolve_endpoint_ttl(
                sport_slug="football",
                endpoint_pattern=EVENT_INCIDENTS_ENDPOINT.pattern,
                status_phase=STATUS_NOT_STARTED,
            )
        )

    def test_unknown_sport_returns_none(self) -> None:
        # Other sports keep legacy behaviour — matrix is football-only.
        self.assertIsNone(
            resolve_endpoint_ttl(
                sport_slug="basketball",
                endpoint_pattern=EVENT_COMMENTS_ENDPOINT.pattern,
                status_phase=STATUS_LIVE,
            )
        )
        self.assertIsNone(
            resolve_endpoint_ttl(
                sport_slug="tennis",
                endpoint_pattern=EVENT_COMMENTS_ENDPOINT.pattern,
                status_phase=STATUS_LIVE,
            )
        )

    def test_unknown_endpoint_returns_none(self) -> None:
        self.assertIsNone(
            resolve_endpoint_ttl(
                sport_slug="football",
                endpoint_pattern="/api/v1/event/{event_id}/not-in-matrix",
                status_phase=STATUS_LIVE,
            )
        )

    def test_empty_inputs_return_none(self) -> None:
        self.assertIsNone(
            resolve_endpoint_ttl(
                sport_slug="football",
                endpoint_pattern="",
                status_phase=STATUS_LIVE,
            )
        )

    def test_normalize_status_phase(self) -> None:
        self.assertEqual(normalize_status_phase("notstarted"), STATUS_NOT_STARTED)
        self.assertEqual(normalize_status_phase("scheduled"), STATUS_NOT_STARTED)
        self.assertEqual(normalize_status_phase("inprogress"), STATUS_LIVE)
        self.assertEqual(normalize_status_phase("live"), STATUS_LIVE)
        self.assertEqual(normalize_status_phase("finished"), STATUS_FINISHED)
        self.assertEqual(normalize_status_phase("aet"), STATUS_FINISHED)
        # unknown → conservative live (re-fetch sooner)
        self.assertEqual(normalize_status_phase("zombie_stale"), STATUS_LIVE)
        self.assertEqual(normalize_status_phase(None), STATUS_LIVE)

    def test_matrix_listing_is_non_empty(self) -> None:
        patterns = known_football_endpoint_patterns()
        self.assertGreater(len(patterns), 10)
        # spot-check: comments + tweets must be present
        self.assertIn(EVENT_COMMENTS_ENDPOINT.pattern, patterns)
        self.assertIn(EVENT_OFFICIAL_TWEETS_ENDPOINT.pattern, patterns)


class EventDetailFreshnessFieldsTests(unittest.TestCase):
    """Tests for the B1-aware ``_event_detail_freshness_fields`` resolver.

    The function is the integration point between the matrix and the
    actual fetch path; these tests pin both the *new* per-status TTL
    branch and the *legacy* default branch so future refactors cannot
    silently regress one or the other.
    """

    def test_legacy_static_endpoint_without_context_keeps_24h(self) -> None:
        # Legacy callers (older tests, replay path) pass no sport / phase.
        key, ttl = _event_detail_freshness_fields(
            EVENT_MANAGERS_ENDPOINT,
            {"event_id": 99},
        )
        self.assertIsNotNone(key)
        self.assertEqual(ttl, EVENT_STATIC_DETAIL_FRESHNESS_TTL_SECONDS)

    def test_legacy_player_endpoint_without_context_keeps_5min(self) -> None:
        key, ttl = _event_detail_freshness_fields(
            EVENT_PLAYER_STATISTICS_ENDPOINT,
            {"event_id": 99, "player_id": 7},
        )
        self.assertIsNotNone(key)
        self.assertEqual(ttl, EVENT_PLAYER_DETAIL_FRESHNESS_TTL_SECONDS)

    def test_managers_pregame_uses_one_hour_from_matrix(self) -> None:
        # Bobur: managers NOT_STARTED → 1h.
        key, ttl = _event_detail_freshness_fields(
            EVENT_MANAGERS_ENDPOINT,
            {"event_id": 42},
            sport_slug="football",
            status_phase="not_started",
        )
        self.assertEqual(key, "freshness:event-detail:42:" + EVENT_MANAGERS_ENDPOINT.pattern)
        self.assertEqual(ttl, HOUR)

    def test_comments_live_uses_60s_from_matrix(self) -> None:
        # Comments is NOT in the legacy whitelist sets — only B1 surfaces it.
        key, ttl = _event_detail_freshness_fields(
            EVENT_COMMENTS_ENDPOINT,
            {"event_id": 42},
            sport_slug="football",
            status_phase="live",
        )
        self.assertEqual(key, "freshness:event-detail:42:" + EVENT_COMMENTS_ENDPOINT.pattern)
        self.assertEqual(ttl, 60)

    def test_comments_not_started_returns_no_gate(self) -> None:
        # not_started=None in the matrix → no freshness gate to enforce.
        key, ttl = _event_detail_freshness_fields(
            EVENT_COMMENTS_ENDPOINT,
            {"event_id": 42},
            sport_slug="football",
            status_phase="not_started",
        )
        self.assertIsNone(key)
        self.assertIsNone(ttl)

    def test_player_endpoint_live_uses_matrix_value(self) -> None:
        # Player stats live = 60s (was 5min legacy).
        key, ttl = _event_detail_freshness_fields(
            EVENT_PLAYER_STATISTICS_ENDPOINT,
            {"event_id": 42, "player_id": 7},
            sport_slug="football",
            status_phase="live",
        )
        self.assertEqual(ttl, 60)
        self.assertIn("freshness:event-player:42:7:", key)

    def test_unknown_sport_falls_through_to_legacy(self) -> None:
        # Other sports keep the legacy TTL.
        key, ttl = _event_detail_freshness_fields(
            EVENT_MANAGERS_ENDPOINT,
            {"event_id": 42},
            sport_slug="basketball",
            status_phase="live",
        )
        self.assertEqual(ttl, EVENT_STATIC_DETAIL_FRESHNESS_TTL_SECONDS)

    def test_missing_event_id_returns_none(self) -> None:
        key, ttl = _event_detail_freshness_fields(
            EVENT_COMMENTS_ENDPOINT,
            {},
            sport_slug="football",
            status_phase="live",
        )
        self.assertIsNone(key)
        self.assertIsNone(ttl)


if __name__ == "__main__":
    unittest.main()
