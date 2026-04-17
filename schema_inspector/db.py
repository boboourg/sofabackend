"""Async PostgreSQL access layer for ETL jobs."""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Any


@dataclass(frozen=True)
class DatabaseConfig:
    """Connection settings for PostgreSQL."""

    dsn: str
    min_size: int = 20
    max_size: int = 50
    command_timeout: float = 60.0


class AsyncpgDatabase:
    """Small asyncpg wrapper with explicit connection and transaction boundaries."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self._pool: Any | None = None

    async def connect(self) -> None:
        if self._pool is not None:
            return

        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError(
                "asyncpg is required for PostgreSQL ingestion. Add it to the environment before running ETL jobs."
            ) from exc

        self._pool = await asyncpg.create_pool(
            dsn=self.config.dsn,
            min_size=self.config.min_size,
            max_size=self.config.max_size,
            command_timeout=self.config.command_timeout,
        )

    async def close(self) -> None:
        if self._pool is None:
            return
        await self._pool.close()
        self._pool = None

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        if self._pool is None:
            raise RuntimeError("Database pool is not connected.")
        connection = await self._pool.acquire()
        try:
            yield connection
        finally:
            await self._pool.release(connection)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Any]:
        async with self.connection() as connection:
            transaction = connection.transaction()
            await transaction.start()
            try:
                yield connection
            except Exception:
                await transaction.rollback()
                raise
            else:
                await transaction.commit()

    async def __aenter__(self) -> "AsyncpgDatabase":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        await self.close()


def load_database_config(
    *,
    env: dict[str, str] | None = None,
    dsn: str | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
    command_timeout: float | None = None,
) -> DatabaseConfig:
    """Build database config from arguments and environment."""

    env = env or _load_project_env()
    resolved_dsn = dsn or env.get("SOFASCORE_DATABASE_URL") or env.get("DATABASE_URL") or env.get("POSTGRES_DSN")
    if not resolved_dsn:
        raise RuntimeError("Database DSN is required. Set SOFASCORE_DATABASE_URL, DATABASE_URL or POSTGRES_DSN.")

    return DatabaseConfig(
        dsn=resolved_dsn,
        min_size=min_size or _env_int(env, "SOFASCORE_PG_MIN_SIZE", 20),
        max_size=max_size or _env_int(env, "SOFASCORE_PG_MAX_SIZE", 50),
        command_timeout=command_timeout or _env_float(env, "SOFASCORE_PG_COMMAND_TIMEOUT", 60.0),
    )


def _load_project_env() -> dict[str, str]:
    merged = dict(os.environ)
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return merged

    for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        merged.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return merged


def _env_int(env: dict[str, str], name: str, default: int) -> int:
    value = env.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(env: dict[str, str], name: str, default: float) -> float:
    value = env.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default
