from __future__ import annotations

import asyncio
import unittest

from schema_inspector.services.resource_scope import (
    ResourceTarget,
    TeamOfActiveUTFirstPageResolver,
    TeamOfActiveUTResolver,
)


class _StaticBaseResolver:
    """Stand-in for TeamOfActiveUTResolver: returns a fixed list of
    ``ResourceTarget(entity_type='team', entity_id=N, path_params={'team_id': N})``
    so the wrapper can be tested without DB / Redis.
    """

    kind = TeamOfActiveUTResolver.kind

    def __init__(self, team_ids: list[int]) -> None:
        self.team_ids = list(team_ids)
        self.resolve_calls = 0

    async def resolve(self):
        self.resolve_calls += 1
        return [
            ResourceTarget(
                entity_type="team",
                entity_id=tid,
                path_params={"team_id": tid},
            )
            for tid in self.team_ids
        ]


def _resolve(resolver) -> tuple[ResourceTarget, ...]:
    return tuple(asyncio.run(resolver.resolve()))


class TeamOfActiveUTFirstPageResolverTests(unittest.TestCase):
    def test_kind_constant_matches_endpoint_metadata(self) -> None:
        # team_last_events_endpoint() and team_next_events_endpoint() read
        # this string verbatim.
        self.assertEqual(
            TeamOfActiveUTFirstPageResolver.kind,
            "team-of-active-ut-first-page",
        )

    def test_each_target_carries_page_zero(self) -> None:
        base = _StaticBaseResolver([42, 2672, 17])
        wrapper = TeamOfActiveUTFirstPageResolver(base=base)

        targets = _resolve(wrapper)

        self.assertEqual(len(targets), 3)
        for target, expected_tid in zip(targets, [42, 2672, 17]):
            self.assertEqual(target.entity_type, "team")
            self.assertEqual(target.entity_id, expected_tid)
            self.assertEqual(target.path_params, {"team_id": expected_tid, "page": 0})

    def test_does_not_duplicate_base_query(self) -> None:
        # The wrapper is just metadata: each .resolve() invokes base exactly
        # once, regardless of how many call sites consume the team scope.
        base = _StaticBaseResolver([42])
        wrapper = TeamOfActiveUTFirstPageResolver(base=base)
        _resolve(wrapper)
        _resolve(wrapper)
        self.assertEqual(base.resolve_calls, 2)

    def test_empty_base_yields_empty(self) -> None:
        wrapper = TeamOfActiveUTFirstPageResolver(base=_StaticBaseResolver([]))
        self.assertEqual(_resolve(wrapper), ())


if __name__ == "__main__":
    unittest.main()
