"""Durable normalize sink for parser results."""

from __future__ import annotations

from dataclasses import replace


# Fix E (2026-05-20 architecture audit, anomaly E): event_incidents and
# event_statistics now forward team / player blocks to the repository
# layer. Sofascore embeds full nested ``team`` / ``player`` objects
# inside incident-payload (scorer, assister, substituted player, team
# of card) and stats-payload (home / away team meta). Previously the
# allowlist was {"event"} only — extract_entities lifted the blocks
# out of the JSON but the sink discarded them before
# persist_parse_result, leaving the parent FK targets missing and
# forcing a permanent FK violation if a stub player was later
# referenced by event_player_statistics / breakdown / best_players in
# the same transaction.
#
# Pairing:
#   * Fix D in NormalizeRepository._upsert_child_pass introduces a
#     stub-upsert (ON CONFLICT DO NOTHING) for nameless players so the
#     FK target physically exists even when Sofascore returns only an
#     ``{"id": X}`` skeleton.
#   * This Fix E in sink.py lets that stub-upsert ACTUALLY see the
#     player row in the first place; without the allowlist extension,
#     the stub-track never gets a chance to fire for incidents-only
#     payloads.
#
# event_statistics deliberately excludes player — the schema for
# event_statistic has no player FK, and per-event statistics blocks
# never carry individual player entries. team is added so home/away
# team meta lands in the parent pass before any sibling snapshot
# (best_players, incidents) tries to reference it.
DEFAULT_ENTITY_KIND_ALLOWLIST_BY_PARSER_FAMILY = {
    "event_statistics": frozenset({"event", "team"}),
    "event_incidents": frozenset({"event", "team", "player"}),
    "event_graph": frozenset({"event"}),
    "event_best_players": frozenset({"event", "player"}),
    "event_lineups": frozenset({"event", "team", "player"}),
    "event_player_statistics": frozenset({"event", "team", "player"}),
}


class DurableNormalizeSink:
    def __init__(
        self,
        repository,
        sql_executor,
        *,
        skip_entity_parser_families=(),
        entity_kind_allowlist_by_parser_family=None,
    ) -> None:
        self.repository = repository
        self.sql_executor = sql_executor
        self.skip_entity_parser_families = frozenset(str(item) for item in skip_entity_parser_families)
        configured_allowlist = entity_kind_allowlist_by_parser_family or DEFAULT_ENTITY_KIND_ALLOWLIST_BY_PARSER_FAMILY
        self.entity_kind_allowlist_by_parser_family = {
            str(parser_family): frozenset(str(entity_kind) for entity_kind in entity_kinds)
            for parser_family, entity_kinds in configured_allowlist.items()
        }

    async def __call__(self, result) -> None:
        normalized_result = result
        skip_entity_upserts = result.parser_family in self.skip_entity_parser_families
        if not skip_entity_upserts:
            allowlist = self.entity_kind_allowlist_by_parser_family.get(str(result.parser_family))
            if allowlist is not None:
                normalized_result = replace(
                    result,
                    entity_upserts={
                        entity_kind: tuple(rows)
                        for entity_kind, rows in result.entity_upserts.items()
                        if entity_kind in allowlist
                    },
                )
        await self.repository.persist_parse_result(
            self.sql_executor,
            normalized_result,
            skip_entity_upserts=skip_entity_upserts,
        )
