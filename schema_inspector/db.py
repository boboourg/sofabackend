"""Async PostgreSQL access layer for ETL jobs."""

from __future__ import annotations

import os
import re
import sys
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Any, Callable
from urllib.parse import parse_qs, urlsplit


_POST_COMMIT_HOOKS: ContextVar[list[Callable[[], None]] | None] = ContextVar(
    "_POST_COMMIT_HOOKS",
    default=None,
)


def register_post_commit_hook(callback: Callable[[], None]) -> bool:
    hooks = _POST_COMMIT_HOOKS.get()
    if hooks is None:
        return False
    hooks.append(callback)
    return True


def _reset_registry_sync_caches() -> None:
    try:
        from .storage.raw_repository import reset_all_registry_sync_caches
    except ImportError:
        return
    reset_all_registry_sync_caches()


@dataclass(frozen=True)
class DatabaseConfig:
    """Connection settings for PostgreSQL."""

    dsn: str
    min_size: int = 20
    max_size: int = 50
    command_timeout: float = 60.0
    application_name: str | None = None
    unix_socket_dir: str | None = None

    def connect_kwargs(
        self,
        *,
        platform: str | None = None,
        socket_dir: str | None = None,
        prefer_unix_socket: bool = True,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "dsn": self.dsn,
            "command_timeout": self.command_timeout,
        }
        if prefer_unix_socket:
            resolved_socket_dir = socket_dir if socket_dir is not None else self.unix_socket_dir
            socket_host = _resolve_unix_socket_host(self.dsn, platform=platform, socket_dir=resolved_socket_dir)
            if socket_host is not None:
                kwargs["host"] = socket_host
                parsed = urlsplit(self.dsn)
                if parsed.port is not None:
                    kwargs["port"] = parsed.port
        if self.application_name:
            kwargs["server_settings"] = {"application_name": self.application_name}
        return kwargs

    def pool_kwargs(
        self,
        *,
        platform: str | None = None,
        socket_dir: str | None = None,
        prefer_unix_socket: bool = True,
    ) -> dict[str, Any]:
        kwargs = self.connect_kwargs(
            platform=platform,
            socket_dir=socket_dir,
            prefer_unix_socket=prefer_unix_socket,
        )
        kwargs["min_size"] = self.min_size
        kwargs["max_size"] = self.max_size
        return kwargs


class AsyncpgDatabase:
    """Small asyncpg wrapper with explicit connection and transaction boundaries."""

    def __init__(self, config: DatabaseConfig) -> None:
        self.config = config
        self._pool: Any | None = None

    async def connect(self) -> None:
        if self._pool is not None:
            return

        self._pool = await create_pool_with_fallback(self.config)
        _reset_registry_sync_caches()

    async def close(self) -> None:
        if self._pool is None:
            return
        await self._pool.close()
        _reset_registry_sync_caches()
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
            post_commit_hooks: list[Callable[[], None]] = []
            hooks_token = _POST_COMMIT_HOOKS.set(post_commit_hooks)
            await transaction.start()
            try:
                yield connection
            except Exception:
                await transaction.rollback()
                raise
            else:
                await transaction.commit()
                for hook in post_commit_hooks:
                    hook()
            finally:
                _POST_COMMIT_HOOKS.reset(hooks_token)

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
    application_name: str | None = None,
    unix_socket_dir: str | None = None,
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
        application_name=application_name
        or env.get("SOFASCORE_PG_APPLICATION_NAME")
        or _default_application_name(),
        unix_socket_dir=unix_socket_dir or env.get("SOFASCORE_PG_SOCKET_DIR") or "/var/run/postgresql",
    )


async def connect_with_fallback(config: DatabaseConfig) -> Any:
    asyncpg = _require_asyncpg()
    preferred_kwargs = config.connect_kwargs()
    tcp_kwargs = config.connect_kwargs(prefer_unix_socket=False)
    return await _connect_with_optional_fallback(asyncpg.connect, preferred_kwargs, tcp_kwargs)


async def create_pool_with_fallback(config: DatabaseConfig) -> Any:
    asyncpg = _require_asyncpg()
    preferred_kwargs = config.pool_kwargs()
    tcp_kwargs = config.pool_kwargs(prefer_unix_socket=False)
    return await _connect_with_optional_fallback(asyncpg.create_pool, preferred_kwargs, tcp_kwargs)


def _default_application_name(argv: list[str] | None = None) -> str:
    raw_argv = list(sys.argv if argv is None else argv)
    if len(raw_argv) <= 1:
        return "schema-inspector"
    remaining = raw_argv[1:]
    if remaining and remaining[0] == "-m":
        remaining = remaining[1:]
    parts: list[str] = []
    for token in remaining:
        if token.startswith("-"):
            break
        parts.append(token)
        if len(parts) == 2:
            break
    candidate = "-".join(parts) if parts else (Path(raw_argv[0]).stem or "schema-inspector")
    sanitized = re.sub(r"[^A-Za-z0-9_.-]+", "-", candidate).strip("-")
    return sanitized or "schema-inspector"


def _resolve_unix_socket_host(
    dsn: str,
    *,
    platform: str | None = None,
    socket_dir: str | None = None,
) -> str | None:
    normalized_platform = (platform or sys.platform).lower()
    if normalized_platform.startswith("win"):
        return None
    if not socket_dir:
        return None
    parsed = urlsplit(dsn)
    query = parse_qs(parsed.query)
    if any(str(value).startswith("/") for value in query.get("host", ())):
        return None
    hostname = (parsed.hostname or "").strip().lower()
    if hostname not in {"localhost", "127.0.0.1", "::1"}:
        return None
    return socket_dir


async def _connect_with_optional_fallback(
    factory: Callable[..., Any],
    preferred_kwargs: dict[str, Any],
    tcp_kwargs: dict[str, Any],
) -> Any:
    try:
        return await factory(**preferred_kwargs)
    except Exception as exc:
        if not _should_retry_with_tcp(exc, preferred_kwargs=preferred_kwargs, tcp_kwargs=tcp_kwargs):
            raise
        return await factory(**tcp_kwargs)


def _should_retry_with_tcp(
    exc: Exception,
    *,
    preferred_kwargs: dict[str, Any],
    tcp_kwargs: dict[str, Any],
) -> bool:
    if preferred_kwargs == tcp_kwargs:
        return False
    host = preferred_kwargs.get("host")
    if not isinstance(host, str) or not host.startswith("/"):
        return False
    return exc.__class__.__name__ in {
        "InvalidAuthorizationSpecificationError",
        "ConnectionDoesNotExistError",
        "CannotConnectNowError",
        "ConnectionFailureError",
    } or isinstance(exc, OSError)


def _require_asyncpg() -> Any:
    try:
        import asyncpg
    except ImportError as exc:
        raise RuntimeError(
            "asyncpg is required for PostgreSQL ingestion. Add it to the environment before running ETL jobs."
        ) from exc
    return asyncpg


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
