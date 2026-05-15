"""Standalone handler test for /unique-tournament/{ut}/season/{s}/rounds.

Lives in its own module because tests/test_local_api_server.py is hitting
a pre-existing class-collection issue where LocalApiOperationsTests is
truncated after ~17 methods — unrelated to season_rounds, but it means
the original test_handle_api_get_rebuilds_rounds_without_raw_snapshot
(and the new cup variant) cannot be exercised from that file. Adding
them here gives pytest a clean entry point for both shapes.
"""

from __future__ import annotations

import unittest
from typing import Any, Iterable

from schema_inspector.local_api_server import LocalApiApplication, build_route_specs


class _RoundsFakeConn:
    def __init__(self, rounds: Iterable[dict[str, Any]]) -> None:
        self.rounds = list(rounds)
        self.closed = False
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any):
        self.fetch_calls.append((query, args))
        if "FROM season_round" in query:
            return self.rounds
        return []

    async def close(self) -> None:
        self.closed = True


class _SnapshotEmptyConn:
    def __init__(self) -> None:
        self.closed = False

    async def fetch(self, *_args: Any, **_kwargs: Any):
        return []

    async def fetchrow(self, *_args: Any, **_kwargs: Any):
        return None

    async def execute(self, *_args: Any, **_kwargs: Any):
        return None

    async def close(self) -> None:
        self.closed = True


def _sequence_connector(connections: list[Any]):
    iterator = iter(connections)

    async def _connect():
        return next(iterator)

    return _connect


class LeagueRoundsHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_league_rounds_emit_only_round_number_when_name_and_slug_null(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snap = _SnapshotEmptyConn()
        norm = _RoundsFakeConn(
            rounds=[
                {"round_number": 1, "round_name": None, "round_slug": None, "round_prefix": None, "is_current": False},
                {"round_number": 37, "round_name": None, "round_slug": None, "round_prefix": None, "is_current": True},
            ],
        )
        application._connect = _sequence_connector([snap, norm])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/8/season/77559/rounds", ""
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload["rounds"], [{"round": 1}, {"round": 37}])
        self.assertEqual(response.payload["currentRound"], {"round": 37})


class CupRoundsHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_cup_rounds_include_name_slug_and_optional_prefix(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snap = _SnapshotEmptyConn()
        norm = _RoundsFakeConn(
            rounds=[
                {
                    "round_number": 1,
                    "round_name": "Qualification Round 1",
                    "round_slug": "qualification-round-1",
                    "round_prefix": None,
                    "is_current": False,
                },
                {
                    "round_number": 28,
                    "round_name": "Semifinals",
                    "round_slug": "semifinals",
                    "round_prefix": None,
                    "is_current": True,
                },
                {
                    "round_number": 636,
                    "round_name": "Playoff round",
                    "round_slug": "playoff-round",
                    "round_prefix": "Qualification",
                    "is_current": False,
                },
            ],
        )
        application._connect = _sequence_connector([snap, norm])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/7/season/76953/rounds", ""
        )

        self.assertEqual(response.status_code, 200)

        # Plain Qualification Round 1 — name+slug, no prefix
        self.assertEqual(
            response.payload["rounds"][0],
            {"round": 1, "name": "Qualification Round 1", "slug": "qualification-round-1"},
        )

        # Semifinals = currentRound, with name+slug, no prefix
        self.assertEqual(
            response.payload["rounds"][1],
            {"round": 28, "name": "Semifinals", "slug": "semifinals"},
        )
        self.assertEqual(
            response.payload["currentRound"],
            {"round": 28, "name": "Semifinals", "slug": "semifinals"},
        )

        # Playoff round — includes prefix
        self.assertEqual(
            response.payload["rounds"][2],
            {
                "round": 636,
                "name": "Playoff round",
                "slug": "playoff-round",
                "prefix": "Qualification",
            },
        )

    async def test_cup_rounds_omit_prefix_field_when_db_value_is_null(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snap = _SnapshotEmptyConn()
        norm = _RoundsFakeConn(
            rounds=[
                {
                    "round_number": 1,
                    "round_name": "Round 1",
                    "round_slug": "round-1",
                    "round_prefix": None,
                    "is_current": False,
                },
            ],
        )
        application._connect = _sequence_connector([snap, norm])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/7/season/76953/rounds", ""
        )

        self.assertEqual(response.status_code, 200)
        # No "prefix" key when round_prefix is NULL
        self.assertNotIn("prefix", response.payload["rounds"][0])


if __name__ == "__main__":
    unittest.main()
