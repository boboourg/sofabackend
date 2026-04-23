from __future__ import annotations

import asyncio

from schema_inspector.db import AsyncpgDatabase, load_database_config
from schema_inspector.storage.endpoint_negative_cache_repository import EndpointNegativeCacheRepository


async def _main() -> None:
    repository = EndpointNegativeCacheRepository()
    async with AsyncpgDatabase(load_database_config()) as database:
        async with database.transaction() as connection:
            await repository.rebuild_state_from_log(connection)


if __name__ == "__main__":
    asyncio.run(_main())
