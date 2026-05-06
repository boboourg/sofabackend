from __future__ import annotations

import asyncio
import unittest

from schema_inspector.services.resource_scope import (
    PlayerOfActiveSquadFirstPageResolver,
    PlayerOfActiveSquadResolver,
    ResourceTarget,
)


class _StaticBaseResolver:
    """Stand-in for PlayerOfActiveSquadResolver: returns a fixed list of
    ``ResourceTarget(entity_type='player', entity_id=N, path_params={'player_id': N})``
    so the wrapper resolver can be tested without DB / Redis.
    """

    kind = PlayerOfActiveSquadResolver.kind

    def __init__(self, player_ids: list[int]) -> None:
        self.player_ids = list(player_ids)
        self.resolve_calls = 0

    async def resolve(self):
        self.resolve_calls += 1
        return [
            ResourceTarget(
                entity_type="player",
                entity_id=pid,
                path_params={"player_id": pid},
            )
            for pid in self.player_ids
        ]


def _resolve(resolver) -> tuple[ResourceTarget, ...]:
    return tuple(asyncio.run(resolver.resolve()))


class PlayerOfActiveSquadFirstPageResolverTests(unittest.TestCase):
    def test_kind_constant_matches_endpoint_metadata(self) -> None:
        # PLAYER_EVENTS_LAST_ENDPOINT.scope_kind reads this string verbatim.
        self.assertEqual(
            PlayerOfActiveSquadFirstPageResolver.kind,
            "player-of-active-squad-first-page",
        )

    def test_each_target_carries_page_zero(self) -> None:
        base = _StaticBaseResolver([100, 200, 300])
        wrapper = PlayerOfActiveSquadFirstPageResolver(base=base)

        targets = _resolve(wrapper)

        self.assertEqual(len(targets), 3)
        for target, expected_pid in zip(targets, [100, 200, 300]):
            self.assertEqual(target.entity_type, "player")
            self.assertEqual(target.entity_id, expected_pid)
            self.assertEqual(target.path_params, {"player_id": expected_pid, "page": 0})

    def test_resolver_reuses_base_cache_one_call_per_resolve(self) -> None:
        # The wrapper must NOT trigger a second DB query: it just adds page=0
        # to each ResourceTarget produced by the shared base resolver.
        base = _StaticBaseResolver([42])
        wrapper = PlayerOfActiveSquadFirstPageResolver(base=base)

        _resolve(wrapper)
        _resolve(wrapper)

        # Each wrapper.resolve() invokes base.resolve() exactly once -- which
        # in production hits the cached Redis snapshot. The point is that the
        # wrapper does not duplicate the SQL.
        self.assertEqual(base.resolve_calls, 2)

    def test_empty_base_yields_empty(self) -> None:
        base = _StaticBaseResolver([])
        wrapper = PlayerOfActiveSquadFirstPageResolver(base=base)
        self.assertEqual(_resolve(wrapper), ())


if __name__ == "__main__":
    unittest.main()
