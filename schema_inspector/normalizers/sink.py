"""Durable normalize sink for parser results."""

from __future__ import annotations


class DurableNormalizeSink:
    def __init__(self, repository, sql_executor) -> None:
        self.repository = repository
        self.sql_executor = sql_executor

    async def __call__(self, result) -> None:
        await self.repository.persist_parse_result(self.sql_executor, result)
