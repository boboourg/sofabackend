"""TDD tests for ``EndpointOrigin`` — Fix 2 of audit follow-ups.

Adds a single enum + field to ``SofascoreEndpoint`` that classifies
each endpoint by *where the response payload comes from*:

* ``UPSTREAM``  — fetched from Sofascore (default for ~100 endpoints)
* ``SYNTHETIC`` — built locally from our DB (Phase 2.1 calendar, 2.4 h2h)
* ``FEDERATED`` — snapshot primary + synthesize fallback (Phase 2.5
  standings/away once dispatcher wiring lands)

Why this matters
----------------
Right now "this endpoint is independent of Sofascore" is implicit
knowledge in the comments around ``calendar_months_with_events_endpoint``
and ``EVENT_H2H_EVENTS_ENDPOINT``. The marker makes it explicit:

* health checks can report which endpoints survive a Sofascore outage;
* monitoring can split metrics by origin (proxy budget belongs to
  UPSTREAM only);
* future engineers see the categorization at the endpoint declaration
  site, not via grep through phase audit docs.

The change is intentionally additive — default ``UPSTREAM`` preserves
binary compatibility for every existing endpoint constant. Only the
two known synthesized endpoints get re-labelled.
"""

from __future__ import annotations

import unittest


class EndpointOriginEnumTests(unittest.TestCase):
    """The enum itself: three stable values, string-valued for JSON
    serialization in ``/ops/`` reports."""

    def test_upstream_value_is_string(self) -> None:
        from schema_inspector.endpoints import EndpointOrigin

        self.assertEqual(EndpointOrigin.UPSTREAM.value, "upstream")

    def test_synthetic_value_is_string(self) -> None:
        from schema_inspector.endpoints import EndpointOrigin

        self.assertEqual(EndpointOrigin.SYNTHETIC.value, "synthetic")

    def test_federated_value_is_string(self) -> None:
        from schema_inspector.endpoints import EndpointOrigin

        self.assertEqual(EndpointOrigin.FEDERATED.value, "federated")

    def test_three_distinct_members(self) -> None:
        from schema_inspector.endpoints import EndpointOrigin

        members = {member.name for member in EndpointOrigin}
        self.assertEqual(members, {"UPSTREAM", "SYNTHETIC", "FEDERATED"})


class SofascoreEndpointOriginFieldTests(unittest.TestCase):
    """``SofascoreEndpoint`` gains an ``origin`` field defaulting to
    ``UPSTREAM`` so every existing endpoint declaration stays binary
    compatible — only endpoints that explicitly opt out get a different
    classification."""

    def test_default_origin_is_upstream(self) -> None:
        from schema_inspector.endpoints import EndpointOrigin, SofascoreEndpoint

        endpoint = SofascoreEndpoint(
            path_template="/api/v1/sample",
            envelope_key="data",
        )
        self.assertEqual(endpoint.origin, EndpointOrigin.UPSTREAM)

    def test_origin_can_be_set_to_synthetic(self) -> None:
        from schema_inspector.endpoints import EndpointOrigin, SofascoreEndpoint

        endpoint = SofascoreEndpoint(
            path_template="/api/v1/sample",
            envelope_key="data",
            origin=EndpointOrigin.SYNTHETIC,
        )
        self.assertEqual(endpoint.origin, EndpointOrigin.SYNTHETIC)

    def test_origin_can_be_set_to_federated(self) -> None:
        from schema_inspector.endpoints import EndpointOrigin, SofascoreEndpoint

        endpoint = SofascoreEndpoint(
            path_template="/api/v1/sample",
            envelope_key="data",
            origin=EndpointOrigin.FEDERATED,
        )
        self.assertEqual(endpoint.origin, EndpointOrigin.FEDERATED)


class CalendarMonthsEndpointMarkedSyntheticTests(unittest.TestCase):
    """Phase 2.1: ``/calendar/.../months-with-events`` is built locally
    from ``event.start_timestamp`` — no upstream fetch path. Marker
    documents this at the declaration site."""

    def test_calendar_months_endpoint_origin_is_synthetic(self) -> None:
        from schema_inspector.endpoints import (
            EndpointOrigin,
            calendar_months_with_events_endpoint,
        )

        endpoint = calendar_months_with_events_endpoint()
        self.assertEqual(endpoint.origin, EndpointOrigin.SYNTHETIC)


class H2HEventsEndpointMarkedSyntheticTests(unittest.TestCase):
    """Phase 2.4: ``/event/{custom_id}/h2h/events`` is synthesized via
    the anchor-pair CTE over ``event``. No upstream traffic on serve."""

    def test_h2h_events_endpoint_origin_is_synthetic(self) -> None:
        from schema_inspector.endpoints import (
            EVENT_H2H_EVENTS_ENDPOINT,
            EndpointOrigin,
        )

        self.assertEqual(EVENT_H2H_EVENTS_ENDPOINT.origin, EndpointOrigin.SYNTHETIC)


class UpstreamEndpointsKeepDefaultTests(unittest.TestCase):
    """Spot-check that endpoints we did NOT migrate keep the default
    ``UPSTREAM`` origin — protects against accidental sweeps."""

    def test_event_detail_endpoint_remains_upstream(self) -> None:
        from schema_inspector.endpoints import EVENT_DETAIL_ENDPOINT, EndpointOrigin

        self.assertEqual(EVENT_DETAIL_ENDPOINT.origin, EndpointOrigin.UPSTREAM)

    def test_unique_tournament_endpoint_remains_upstream(self) -> None:
        from schema_inspector.endpoints import (
            UNIQUE_TOURNAMENT_ENDPOINT,
            EndpointOrigin,
        )

        self.assertEqual(UNIQUE_TOURNAMENT_ENDPOINT.origin, EndpointOrigin.UPSTREAM)


class OpsEndpointsByOriginRouteTests(unittest.TestCase):
    """``/ops/endpoints/by-origin`` reports the count + listing of all
    registered endpoints grouped by their ``EndpointOrigin``. Used by
    deploy checklists and monitoring."""

    def test_payload_has_total_count(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)
        payload = application._fetch_ops_endpoints_by_origin_payload()
        self.assertIn("total", payload)
        self.assertIsInstance(payload["total"], int)
        self.assertGreater(payload["total"], 50)

    def test_payload_groups_by_three_origins(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)
        payload = application._fetch_ops_endpoints_by_origin_payload()
        self.assertEqual(
            set(payload["counts"].keys()),
            {"upstream", "synthetic", "federated"},
        )

    def test_upstream_count_largest(self) -> None:
        """~100 endpoints are UPSTREAM, only a handful are synthesized."""
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)
        payload = application._fetch_ops_endpoints_by_origin_payload()
        self.assertGreater(payload["counts"]["upstream"], 50)

    def test_synthetic_count_at_least_two(self) -> None:
        """At minimum: calendar/months-with-events + h2h/events."""
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)
        payload = application._fetch_ops_endpoints_by_origin_payload()
        self.assertGreaterEqual(payload["counts"]["synthetic"], 2)

    def test_synthetic_list_contains_calendar_months(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)
        payload = application._fetch_ops_endpoints_by_origin_payload()
        synthetic_paths = [
            item["path_template"] for item in payload["endpoints"]["synthetic"]
        ]
        self.assertIn(
            "/api/v1/calendar/unique-tournament/{unique_tournament_id}"
            "/season/{season_id}/months-with-events",
            synthetic_paths,
        )

    def test_synthetic_list_contains_h2h_events(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)
        payload = application._fetch_ops_endpoints_by_origin_payload()
        synthetic_paths = [
            item["path_template"] for item in payload["endpoints"]["synthetic"]
        ]
        self.assertIn("/api/v1/event/{custom_id}/h2h/events", synthetic_paths)


if __name__ == "__main__":
    unittest.main()
