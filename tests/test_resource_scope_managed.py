from __future__ import annotations

import asyncio
import unittest

from schema_inspector.services.resource_scope import ManagedScopeResolver, ResourceTarget


def _resolve(resolver: ManagedScopeResolver) -> tuple[ResourceTarget, ...]:
    return tuple(asyncio.run(_run(resolver)))


async def _run(resolver: ManagedScopeResolver):
    return list(await resolver.resolve())


class ManagedScopeResolverTests(unittest.TestCase):
    def test_kind_matches_endpoint_scope_kind_constant(self) -> None:
        # Stage A pilot endpoints (TEAM_PLAYERS_ENDPOINT) declare scope_kind="managed".
        self.assertEqual(ManagedScopeResolver.kind, "managed")

    def test_unset_env_yields_empty(self) -> None:
        resolver = ManagedScopeResolver(env={})
        self.assertEqual(_resolve(resolver), ())

    def test_empty_value_yields_empty(self) -> None:
        resolver = ManagedScopeResolver(
            env={"SCHEMA_INSPECTOR_RESOURCE_PILOT_TEAMS": ""}
        )
        self.assertEqual(_resolve(resolver), ())

    def test_parses_comma_separated_list(self) -> None:
        resolver = ManagedScopeResolver(
            env={"SCHEMA_INSPECTOR_RESOURCE_PILOT_TEAMS": "42, 2672 ,17,44,2829"}
        )
        targets = _resolve(resolver)
        team_ids = [t.entity_id for t in targets]
        self.assertEqual(team_ids, [42, 2672, 17, 44, 2829])
        for target in targets:
            self.assertEqual(target.entity_type, "team")
            self.assertEqual(target.path_params, {"team_id": target.entity_id})

    def test_drops_invalid_and_duplicate_entries(self) -> None:
        resolver = ManagedScopeResolver(
            env={"SCHEMA_INSPECTOR_RESOURCE_PILOT_TEAMS": "42, oops, 42, 0, -1, 17"}
        )
        team_ids = [t.entity_id for t in _resolve(resolver)]
        self.assertEqual(team_ids, [42, 17])

    def test_supports_custom_env_key(self) -> None:
        resolver = ManagedScopeResolver(
            env_key="ALT_KEY",
            env={"ALT_KEY": "100, 200"},
        )
        team_ids = [t.entity_id for t in _resolve(resolver)]
        self.assertEqual(team_ids, [100, 200])


if __name__ == "__main__":
    unittest.main()
