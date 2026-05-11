"""P0d write-avoidance cache integration tests.

Verifies that team + player UPSERTs in three repositories skip the
DB write when the row's written columns match the process-local cache,
and that the cache is only populated AFTER a transaction commits
(via ``register_post_commit_hook``) so an aborted transaction does
not poison the cache.

The deadlock pattern (discovery ↔ hydrate cross-deadlock on country/
team/player ON CONFLICT row locks) is the original motivation — see
``EventListRepository.__init__`` block-comment for context. This test
file is a regression guard against accidentally regressing the cache
short-circuit.
"""

from __future__ import annotations

import unittest
from dataclasses import fields, replace as dc_replace
from typing import Any, Callable

from schema_inspector.db import _POST_COMMIT_HOOKS
from schema_inspector.event_detail_parser import (
    EventDetailBundle,
    EventDetailTeamRecord,
    PlayerRecord,
)
from schema_inspector.event_detail_repository import EventDetailRepository
from schema_inspector.event_list_parser import EventListBundle, EventTeamRecord
from schema_inspector.event_list_repository import EventListRepository
from schema_inspector.statistics_parser import (
    StatisticsBundle,
    StatisticsPlayerRecord,
    StatisticsTeamRecord,
)
from schema_inspector.statistics_repository import StatisticsRepository


class _FakeExecutor:
    """Minimal SqlExecutor stub that records executemany calls.

    Mirrors the helper used by ``test_event_list_storage.py``.
    """

    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    async def executemany(self, command: str, args):
        self.executemany_calls.append((command, list(args)))
        return None


def _team_upsert_calls(executor: _FakeExecutor) -> list[list[tuple[Any, ...]]]:
    return [
        rows
        for (command, rows) in executor.executemany_calls
        if "INSERT INTO team" in command
    ]


def _player_upsert_calls(executor: _FakeExecutor) -> list[list[tuple[Any, ...]]]:
    return [
        rows
        for (command, rows) in executor.executemany_calls
        if "INSERT INTO player" in command
    ]


def _empty_bundle_kwargs(bundle_cls: type) -> dict[str, tuple]:
    """Build a kwargs dict with empty tuples for every dataclass field
    of ``bundle_cls`` so we can override only the fields we care about."""
    return {field.name: () for field in fields(bundle_cls)}


def _event_list_bundle(teams: tuple[EventTeamRecord, ...] = ()) -> EventListBundle:
    return EventListBundle(**(_empty_bundle_kwargs(EventListBundle) | {"teams": teams}))


def _event_detail_bundle(
    *,
    teams: tuple[EventDetailTeamRecord, ...] = (),
    players: tuple[PlayerRecord, ...] = (),
) -> EventDetailBundle:
    return EventDetailBundle(
        **(_empty_bundle_kwargs(EventDetailBundle) | {"teams": teams, "players": players})
    )


def _statistics_bundle(
    *,
    teams: tuple[StatisticsTeamRecord, ...] = (),
    players: tuple[StatisticsPlayerRecord, ...] = (),
) -> StatisticsBundle:
    return StatisticsBundle(
        **(_empty_bundle_kwargs(StatisticsBundle) | {"teams": teams, "players": players})
    )


def _make_team_record(team_id: int = 7001) -> EventTeamRecord:
    return EventTeamRecord(
        id=team_id,
        slug="bayern-munich",
        name="Bayern Munich",
        short_name="Bayern",
        name_code="BAY",
        sport_id=1,
        country_alpha2="DE",
        parent_team_id=None,
        gender="M",
        type=0,
        national=False,
        disabled=False,
        user_count=120000,
        team_colors=None,
        field_translations=None,
    )


def _make_detail_team_record(team_id: int = 7001) -> EventDetailTeamRecord:
    return EventDetailTeamRecord(
        id=team_id,
        slug="bayern-munich",
        name="Bayern Munich",
        short_name="Bayern",
        full_name="FC Bayern Munich",
        name_code="BAY",
        sport_id=1,
        category_id=1,
        country_alpha2="DE",
        manager_id=None,
        venue_id=None,
        tournament_id=None,
        primary_unique_tournament_id=None,
        parent_team_id=None,
        gender="M",
        type=0,
        class_value=None,
        ranking=None,
        national=False,
        disabled=False,
        foundation_date_timestamp=None,
        user_count=120000,
        team_colors=None,
        field_translations=None,
        time_active=None,
    )


def _make_player_record(player_id: int = 12345, team_id: int = 7001) -> PlayerRecord:
    return PlayerRecord(
        id=player_id,
        slug="harry-kane",
        name="Harry Kane",
        short_name="H. Kane",
        first_name="Harry",
        last_name="Kane",
        team_id=team_id,
        country_alpha2="GB",
        manager_id=None,
        gender="M",
        position="F",
        positions_detailed=("F",),
        preferred_foot="R",
        jersey_number="9",
        sofascore_id="kane-id",
        date_of_birth=None,
        date_of_birth_timestamp=None,
        height=188,
        weight=86,
        market_value_currency="EUR",
        proposed_market_value_raw=None,
        rating="8.5",
        retired=False,
        deceased=False,
        user_count=50000,
        order_value=1,
        field_translations=None,
    )


def _make_stats_team_record(team_id: int = 7001) -> StatisticsTeamRecord:
    return StatisticsTeamRecord(
        id=team_id,
        slug="bayern",
        name="Bayern Munich",
        short_name="Bayern",
        name_code="BAY",
        sport_id=1,
        gender="M",
        type=0,
        national=False,
        disabled=False,
        user_count=120000,
        team_colors=None,
        field_translations=None,
    )


def _make_stats_player_record(player_id: int = 12345, team_id: int = 7001) -> StatisticsPlayerRecord:
    return StatisticsPlayerRecord(
        id=player_id,
        slug="harry-kane",
        name="Harry Kane",
        short_name="H. Kane",
        team_id=team_id,
        gender="M",
        user_count=50000,
        field_translations=None,
    )


async def _run_with_post_commit_capture(coro_factory: Callable[[], Any]) -> list[Callable[[], None]]:
    """Run an async repository method with a fresh post-commit-hook
    queue installed, return captured hooks. Mimics how
    ``DatabaseClient`` sets up the ContextVar inside ``transaction()``.
    Caller can then invoke the hooks manually to simulate the
    post-commit step."""
    captured: list[Callable[[], None]] = []
    token = _POST_COMMIT_HOOKS.set(captured)
    try:
        await coro_factory()
    finally:
        _POST_COMMIT_HOOKS.reset(token)
    return captured


# ---------------------------------------------------------------------------
# 1. EventListRepository._upsert_teams
# ---------------------------------------------------------------------------
class EventListTeamCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_first_call_writes_then_second_skips_when_unchanged(self) -> None:
        repo = EventListRepository()
        bundle = _event_list_bundle(teams=(_make_team_record(),))
        executor = _FakeExecutor()

        # First call — cache miss → INSERT happens AND post-commit hook
        # registered.
        hooks = await _run_with_post_commit_capture(
            lambda: repo._upsert_teams(executor, bundle)
        )
        self.assertEqual(len(_team_upsert_calls(executor)), 1)
        self.assertEqual(len(hooks), 1)

        # Simulate post-commit by firing the hook.
        for hook in hooks:
            hook()

        # Second call with same content — cache hit → no executemany call.
        await repo._upsert_teams(executor, bundle)
        self.assertEqual(len(_team_upsert_calls(executor)), 1)  # still 1, not 2

    async def test_changed_team_fingerprint_triggers_upsert(self) -> None:
        repo = EventListRepository()
        executor = _FakeExecutor()

        bundle_v1 = _event_list_bundle(teams=(_make_team_record(),))
        hooks = await _run_with_post_commit_capture(
            lambda: repo._upsert_teams(executor, bundle_v1)
        )
        for hook in hooks:
            hook()

        # Modify one column → fingerprint changes → INSERT happens.
        team_v2 = dc_replace(_make_team_record(), user_count=999999)
        bundle_v2 = _event_list_bundle(teams=(team_v2,))
        await repo._upsert_teams(executor, bundle_v2)
        self.assertEqual(len(_team_upsert_calls(executor)), 2)


# ---------------------------------------------------------------------------
# 2. EventDetailRepository._upsert_teams_base + _upsert_players
# ---------------------------------------------------------------------------
class EventDetailCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_team_cache_short_circuits_second_call(self) -> None:
        repo = EventDetailRepository()
        executor = _FakeExecutor()
        bundle = _event_detail_bundle(teams=(_make_detail_team_record(),))

        hooks = await _run_with_post_commit_capture(
            lambda: repo._upsert_teams_base(executor, bundle)
        )
        self.assertEqual(len(_team_upsert_calls(executor)), 1)
        for hook in hooks:
            hook()

        await repo._upsert_teams_base(executor, bundle)
        self.assertEqual(len(_team_upsert_calls(executor)), 1)  # cache hit

    async def test_player_cache_short_circuits_second_call(self) -> None:
        repo = EventDetailRepository()
        executor = _FakeExecutor()
        team = _make_detail_team_record()
        player = _make_player_record(team_id=team.id)
        bundle = _event_detail_bundle(teams=(team,), players=(player,))

        hooks = await _run_with_post_commit_capture(
            lambda: repo._upsert_players(executor, bundle)
        )
        self.assertEqual(len(_player_upsert_calls(executor)), 1)
        for hook in hooks:
            hook()

        await repo._upsert_players(executor, bundle)
        self.assertEqual(len(_player_upsert_calls(executor)), 1)

    async def test_cache_not_populated_when_hook_not_fired(self) -> None:
        """If transaction rolls back, the post-commit hook is never
        called, so cache stays empty and the next call re-attempts the
        UPSERT. Verifies that we did not accidentally populate cache
        synchronously inside the repository method (which would
        poison the cache on rollback)."""
        repo = EventDetailRepository()
        executor = _FakeExecutor()
        bundle = _event_detail_bundle(teams=(_make_detail_team_record(),))

        hooks = await _run_with_post_commit_capture(
            lambda: repo._upsert_teams_base(executor, bundle)
        )
        self.assertEqual(len(hooks), 1)
        # DO NOT fire hooks — simulate transaction rollback.

        # Cache must still be empty → second call triggers INSERT.
        await _run_with_post_commit_capture(
            lambda: repo._upsert_teams_base(executor, bundle)
        )
        self.assertEqual(len(_team_upsert_calls(executor)), 2)

    async def test_no_executemany_when_all_rows_cache_hit(self) -> None:
        """If every row matches the cache, executemany is not called
        at all (early return path). This is the cluster-wide win —
        most calls during a steady stream of repeat hydrates hit this
        short-circuit."""
        repo = EventDetailRepository()
        executor = _FakeExecutor()
        bundle = _event_detail_bundle(teams=(_make_detail_team_record(),))

        hooks = await _run_with_post_commit_capture(
            lambda: repo._upsert_teams_base(executor, bundle)
        )
        for hook in hooks:
            hook()
        executor.executemany_calls.clear()

        await repo._upsert_teams_base(executor, bundle)
        # ZERO executemany calls — fully short-circuited.
        self.assertEqual(executor.executemany_calls, [])

    async def test_no_hook_context_falls_back_to_immediate_set(self) -> None:
        """When no transaction context is active,
        ``register_post_commit_hook`` returns False and the repository
        falls back to populating cache synchronously. Verify the cache
        IS populated even without an explicit hook fire."""
        repo = EventDetailRepository()
        executor = _FakeExecutor()
        bundle = _event_detail_bundle(teams=(_make_detail_team_record(),))

        # NO _run_with_post_commit_capture — context var unset.
        await repo._upsert_teams_base(executor, bundle)
        self.assertEqual(len(_team_upsert_calls(executor)), 1)

        # Second call — cache should be populated from fallback path.
        await repo._upsert_teams_base(executor, bundle)
        self.assertEqual(len(_team_upsert_calls(executor)), 1)


# ---------------------------------------------------------------------------
# 3. StatisticsRepository._upsert_teams + _upsert_players
# ---------------------------------------------------------------------------
class StatisticsCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_team_cache_short_circuits_repeat_calls(self) -> None:
        repo = StatisticsRepository()
        executor = _FakeExecutor()
        bundle = _statistics_bundle(teams=(_make_stats_team_record(),))

        hooks = await _run_with_post_commit_capture(
            lambda: repo._upsert_teams(executor, bundle)
        )
        self.assertEqual(len(_team_upsert_calls(executor)), 1)
        for hook in hooks:
            hook()

        await repo._upsert_teams(executor, bundle)
        self.assertEqual(len(_team_upsert_calls(executor)), 1)

    async def test_player_cache_short_circuits_repeat_calls(self) -> None:
        repo = StatisticsRepository()
        executor = _FakeExecutor()
        bundle = _statistics_bundle(players=(_make_stats_player_record(),))

        hooks = await _run_with_post_commit_capture(
            lambda: repo._upsert_players(executor, bundle)
        )
        self.assertEqual(len(_player_upsert_calls(executor)), 1)
        for hook in hooks:
            hook()

        await repo._upsert_players(executor, bundle)
        self.assertEqual(len(_player_upsert_calls(executor)), 1)


# ---------------------------------------------------------------------------
# 4. SQL contains WHERE IS DISTINCT FROM (defense-in-depth)
# ---------------------------------------------------------------------------
class WhereClauseGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_event_list_team_upsert_sql_contains_where_distinct_from(self) -> None:
        repo = EventListRepository()
        executor = _FakeExecutor()
        bundle = _event_list_bundle(teams=(_make_team_record(),))
        await repo._upsert_teams(executor, bundle)
        team_calls = _team_upsert_calls(executor)
        self.assertEqual(len(team_calls), 1)
        command = executor.executemany_calls[0][0]
        self.assertIn("WHERE team.slug IS DISTINCT FROM EXCLUDED.slug", command)

    async def test_event_detail_team_upsert_sql_contains_where_distinct_from(self) -> None:
        repo = EventDetailRepository()
        executor = _FakeExecutor()
        bundle = _event_detail_bundle(teams=(_make_detail_team_record(),))
        await repo._upsert_teams_base(executor, bundle)
        command = executor.executemany_calls[0][0]
        self.assertIn("WHERE team.slug IS DISTINCT FROM EXCLUDED.slug", command)
        # Spot-check that several other distinct-from clauses are present.
        self.assertIn("team.full_name IS DISTINCT FROM EXCLUDED.full_name", command)
        self.assertIn(
            "team.foundation_date_timestamp IS DISTINCT FROM EXCLUDED.foundation_date_timestamp",
            command,
        )

    async def test_event_detail_player_upsert_sql_contains_where_distinct_from(self) -> None:
        repo = EventDetailRepository()
        executor = _FakeExecutor()
        team = _make_detail_team_record()
        player = _make_player_record(team_id=team.id)
        bundle = _event_detail_bundle(teams=(team,), players=(player,))
        await repo._upsert_players(executor, bundle)
        commands = [c for c, _ in executor.executemany_calls if "INSERT INTO player" in c]
        self.assertEqual(len(commands), 1)
        self.assertIn("WHERE player.slug IS DISTINCT FROM EXCLUDED.slug", commands[0])
        self.assertIn(
            "player.jersey_number IS DISTINCT FROM EXCLUDED.jersey_number", commands[0]
        )


if __name__ == "__main__":
    unittest.main()
