"""Durable normalize sink for parser results."""

from __future__ import annotations


class DurableNormalizeSink:
    def __init__(self, repository, sql_executor, *, skip_entity_parser_families=()) -> None:
        self.repository = repository
        self.sql_executor = sql_executor
        self.skip_entity_parser_families = frozenset(str(item) for item in skip_entity_parser_families)

    async def __call__(self, result) -> None:
        await self.repository.persist_parse_result(
            self.sql_executor,
            result,
            skip_entity_upserts=result.parser_family in self.skip_entity_parser_families,
        )
