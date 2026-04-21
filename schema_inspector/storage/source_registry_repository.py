from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


@dataclass(frozen=True)
class SourceRegistryRecord:
    source_slug: str
    display_name: str
    transport_kind: str
    trust_rank: int = 100
    is_active: bool = True


class SourceRegistryRepository:
    async def upsert_source(self, executor: SqlExecutor, record: SourceRegistryRecord) -> None:
        await executor.execute(
            """
            INSERT INTO provider_source (
                source_slug, display_name, transport_kind, trust_rank, is_active
            )
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (source_slug) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                transport_kind = EXCLUDED.transport_kind,
                trust_rank = EXCLUDED.trust_rank,
                is_active = EXCLUDED.is_active
            """,
            record.source_slug,
            record.display_name,
            record.transport_kind,
            record.trust_rank,
            record.is_active,
        )
