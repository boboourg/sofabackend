"""Durable normalize sink for parser results."""

from __future__ import annotations

from dataclasses import replace


DEFAULT_ENTITY_KIND_ALLOWLIST_BY_PARSER_FAMILY = {
    "event_statistics": frozenset({"event"}),
    "event_incidents": frozenset({"event"}),
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
