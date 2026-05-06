from __future__ import annotations

import unittest

from schema_inspector.compare_endpoint import (
    DiffReport,
    FetchedPayload,
    build_report,
    collect_shape_keypaths,
    diff_keypaths,
    format_report,
)


class CollectShapeKeypathsTests(unittest.TestCase):
    def test_flat_dict(self) -> None:
        result = collect_shape_keypaths({"a": 1, "b": "x", "c": True})
        self.assertEqual(
            result,
            {"a": "int", "b": "str", "c": "bool"},
        )

    def test_nested_dict(self) -> None:
        result = collect_shape_keypaths({"event": {"id": 1, "status": {"code": 100}}})
        self.assertEqual(result["event.id"], "int")
        self.assertEqual(result["event.status.code"], "int")
        # Intermediate nodes are not emitted as leaves.
        self.assertNotIn("event", result)
        self.assertNotIn("event.status", result)

    def test_list_treated_as_homogeneous_first_element(self) -> None:
        result = collect_shape_keypaths(
            {"events": [{"id": 1, "score": {"current": 0}}, {"id": 2, "score": {"current": 1}}]}
        )
        self.assertIn("events[*].id", result)
        self.assertIn("events[*].score.current", result)
        # Index segment is rendered as [*].
        self.assertNotIn("events[0].id", result)
        self.assertNotIn("events[1].id", result)

    def test_empty_list_emits_marker(self) -> None:
        result = collect_shape_keypaths({"events": []})
        self.assertEqual(result.get("events"), "(empty list)")

    def test_empty_dict_emits_marker(self) -> None:
        result = collect_shape_keypaths({"info": {}})
        self.assertEqual(result.get("info"), "(empty dict)")

    def test_root_is_a_list(self) -> None:
        result = collect_shape_keypaths([{"id": 1}, {"id": 2}])
        self.assertIn("[*].id", result)

    def test_root_scalar(self) -> None:
        result = collect_shape_keypaths(42)
        self.assertEqual(result, {"<root>": "int"})


class DiffKeypathsTests(unittest.TestCase):
    def test_missing_extra_common(self) -> None:
        upstream = {"a": "int", "b.x": "str", "c": "bool"}
        local = {"a": "int", "c": "bool", "d": "list"}
        missing, extra, common = diff_keypaths(upstream, local)
        self.assertEqual(missing, ["b.x"])
        self.assertEqual(extra, ["d"])
        self.assertEqual(sorted(common), ["a", "c"])

    def test_no_diff(self) -> None:
        same = {"x.y": "int"}
        missing, extra, common = diff_keypaths(same, same)
        self.assertEqual(missing, [])
        self.assertEqual(extra, [])
        self.assertEqual(common, ["x.y"])


class BuildReportTests(unittest.TestCase):
    def test_realistic_event_list_diff(self) -> None:
        # Mirrors the real UT8/season 77559 diff: upstream has homeScore/awayScore
        # and friends; local synthesizer-only output drops them.
        upstream = FetchedPayload(
            url="https://www.sofascore.com/...events/last/0",
            status=200,
            bytes_total=148_000,
            payload={
                "events": [
                    {
                        "id": 1,
                        "homeTeam": {"id": 10, "name": "X"},
                        "awayTeam": {"id": 11, "name": "Y"},
                        "homeScore": {"current": 1, "period1": 0},
                        "awayScore": {"current": 1, "period1": 0},
                        "status": {"code": 100, "type": "finished"},
                        "changes": {"changes": [], "changeTimestamp": 0},
                    }
                ],
                "hasNextPage": True,
            },
        )
        local = FetchedPayload(
            url="http://127.0.0.1:8000/...events/last/0",
            status=200,
            bytes_total=17_000,
            payload={
                "events": [
                    {
                        "id": 1,
                        "homeTeam": {"id": 10, "name": "X"},
                        "awayTeam": {"id": 11, "name": "Y"},
                        "status": {"code": 100, "type": "finished"},
                    }
                ],
                "hasNextPage": True,
            },
        )

        report = build_report(upstream, local)

        # homeScore + awayScore + changes leaves all missing in local
        self.assertIn("events[*].homeScore.current", report.missing_in_local)
        self.assertIn("events[*].homeScore.period1", report.missing_in_local)
        self.assertIn("events[*].awayScore.current", report.missing_in_local)
        self.assertIn("events[*].changes.changeTimestamp", report.missing_in_local)
        self.assertNotIn("events[*].homeTeam.id", report.missing_in_local)  # present on both sides
        # Nothing extra on local.
        self.assertEqual(report.extra_in_local, [])

    def test_format_report_renders_summary(self) -> None:
        upstream = FetchedPayload(
            url="u", status=200, bytes_total=200, payload={"a": 1, "b": 2}
        )
        local = FetchedPayload(
            url="l", status=200, bytes_total=100, payload={"a": 1}
        )
        report = build_report(upstream, local)
        text = format_report(report)
        self.assertIn("UPSTREAM", text)
        self.assertIn("LOCAL", text)
        self.assertIn("MISSING in local (1)", text)
        self.assertIn("size ratio (local / upstream): 50.0%", text)


class FetchErrorPathsTests(unittest.TestCase):
    """``build_report`` must not crash when a fetch returned no payload."""

    def test_handles_upstream_error(self) -> None:
        upstream = FetchedPayload(url="u", status=0, bytes_total=0, payload=None, error="boom")
        local = FetchedPayload(url="l", status=200, bytes_total=10, payload={"x": 1})
        report = build_report(upstream, local)
        # Local has 1 keypath, upstream has 0 -> all paths are "extra".
        self.assertEqual(report.missing_in_local, [])
        self.assertEqual(report.extra_in_local, ["x"])

    def test_handles_local_error(self) -> None:
        upstream = FetchedPayload(url="u", status=200, bytes_total=10, payload={"x": 1})
        local = FetchedPayload(url="l", status=500, bytes_total=0, payload=None, error="500")
        report = build_report(upstream, local)
        self.assertEqual(report.missing_in_local, ["x"])
        self.assertEqual(report.extra_in_local, [])


if __name__ == "__main__":
    unittest.main()
