"""Long-running Sofascore WebSocket consumer.

Connects to ``wss://ws.sofascore.com:9222/`` (NATS over WebSocket),
subscribes to ``sport.{slug}`` and ``odds.{slug}.1`` for the configured
sports, parses delta pushes via ``ws_nats_parser`` + ``ws_delta_normalizer``,
applies them through ``ws_delta_writer`` and (transitively) invalidates
``event_payload_cache``.

Goal: knock down the polling-induced latency floor (`event.updated_at`
trails the live world by ~5 s in the polling path) so the persistent
payload cache stays warm with sub-second-old state.

This module owns three layers:
  * :func:`dispatch_message` — pure, testable: takes one parsed NATS
    message + a DB pool, runs normalize→write→invalidate.
  * :class:`WSConsumerService` — IO loop: ws_connect, handshake,
    subscribe, recv→parse→dispatch, reconnect on error.
  * ``cli_main`` (in :mod:`schema_inspector.cli`) — argparse entry
    that wires everything together for systemd.

Note: the consumer does **not** use the per-route hydrate workers — it
writes directly into the existing normalised tables. The polling-based
hydrate path keeps running unchanged; WS just shortens the time
between an upstream change and our cache invalidation from "next poll
cycle" (~5 s) to "next NATS push" (~100 ms).
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from ..ws_delta_normalizer import normalize_event_delta, normalize_odds_delta
from ..ws_delta_writer import apply_event_delta, apply_odds_delta
from ..ws_nats_parser import (
    build_subscribe_commands,
    parse_nats_frames,
    subject_to_msg_type,
)

logger = logging.getLogger(__name__)


WS_URI = "wss://ws.sofascore.com:9222/"

_CONNECT_CMD = (
    'CONNECT {"protocol":1,"version":"3.1.0","lang":"nats.ws",'
    '"verbose":false,"pedantic":false,"user":"none","pass":"none",'
    '"headers":true,"no_responders":true}\r\n'
)

_BROWSER_HEADERS = {
    "Origin": "https://www.sofascore.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

DEFAULT_SPORTS = (
    "football", "basketball", "tennis", "table-tennis", "volleyball",
    "handball", "ice-hockey", "baseball", "american-football",
    "rugby", "cricket", "futsal", "esports",
)


async def dispatch_message(pool: Any, msg: tuple[str, Any]) -> None:
    """Route one parsed NATS message to the normalize→write pipeline.

    Pure function modulo the asyncpg pool. Tested separately with a
    fake pool — see ``tests/test_ws_consumer_dispatch.py``.
    """
    kind, payload = msg
    if kind != "MSG":
        return
    subject, _sid, data_str = payload
    msg_type = subject_to_msg_type(subject)
    if msg_type is None:
        return

    try:
        data = json.loads(data_str)
    except json.JSONDecodeError:
        # Upstream sometimes ships truncated chunks during reconnects.
        # Log only at debug to keep the hot loop quiet under bursts.
        logger.debug("Skipping malformed payload on subject=%s", subject)
        return

    if msg_type == "event":
        delta = normalize_event_delta(data)
        if delta is None:
            return
        async with pool.acquire() as conn:
            async with conn.transaction():
                await apply_event_delta(conn, delta)
        return

    if msg_type == "odds":
        bundle = normalize_odds_delta(data)
        if bundle is None:
            return
        async with pool.acquire() as conn:
            await apply_odds_delta(conn, bundle)
        return


class WSConsumerService:
    """The IO half: ws_connect → handshake → SUB → recv loop with
    automatic reconnect on transport errors.

    Instantiated by ``schema_inspector.cli`` for the ``ws-consumer``
    subcommand. Not unit-tested directly — its parts are exercised
    by the dispatch / parser tests.
    """

    def __init__(
        self,
        pool: Any,
        *,
        sports: tuple[str, ...] = DEFAULT_SPORTS,
        include_odds: bool = True,
        reconnect_delay_seconds: float = 10.0,
        ws_uri: str = WS_URI,
    ) -> None:
        self.pool = pool
        self.sports = sports
        self.include_odds = include_odds
        self.reconnect_delay_seconds = reconnect_delay_seconds
        self.ws_uri = ws_uri
        self._stop = asyncio.Event()
        # rolling counters for /ops integration
        self.metrics = {
            "frames_in": 0,
            "msgs_dispatched": 0,
            "reconnects": 0,
            "json_errors": 0,
            "dispatch_errors": 0,
        }

    def request_stop(self) -> None:
        self._stop.set()

    async def run_forever(self) -> None:
        # Lazy import keeps unit tests free of curl_cffi.
        from curl_cffi import CurlError  # type: ignore
        from curl_cffi.requests import AsyncSession  # type: ignore

        while not self._stop.is_set():
            ws = None
            try:
                logger.info("WS consumer connecting to %s", self.ws_uri)
                async with AsyncSession(impersonate="chrome120") as session:
                    ws = await session.ws_connect(self.ws_uri, headers=_BROWSER_HEADERS)
                    await ws.send(_CONNECT_CMD.encode("utf-8"))
                    sub_cmds = build_subscribe_commands(
                        self.sports, include_odds=self.include_odds,
                    )
                    logger.info(
                        "WS consumer subscribing: %d streams (sports=%s, odds=%s)",
                        len(sub_cmds), ",".join(self.sports), self.include_odds,
                    )
                    for cmd in sub_cmds:
                        await ws.send(cmd)
                        await asyncio.sleep(0.05)
                    await self._recv_loop(ws)
            except CurlError as e:
                logger.warning(
                    "WS consumer transport error: %s. Reconnecting in %ss.",
                    e, self.reconnect_delay_seconds,
                )
                self.metrics["reconnects"] += 1
                await self._sleep_or_stop(self.reconnect_delay_seconds)
            except (ConnectionError, OSError) as e:
                logger.warning(
                    "WS consumer network error: %s. Reconnecting in %ss.",
                    e, self.reconnect_delay_seconds,
                )
                self.metrics["reconnects"] += 1
                await self._sleep_or_stop(self.reconnect_delay_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "WS consumer unexpected error. Reconnecting in %ss.",
                    self.reconnect_delay_seconds,
                )
                self.metrics["reconnects"] += 1
                await self._sleep_or_stop(self.reconnect_delay_seconds)
            finally:
                if ws is not None:
                    try:
                        await ws.close()
                    except Exception:
                        pass

    async def _recv_loop(self, ws: Any) -> None:
        buffer = ""
        while not self._stop.is_set():
            chunk, _ = await ws.recv()
            if isinstance(chunk, bytes):
                chunk = chunk.decode("utf-8", errors="replace")
            buffer += chunk
            messages, buffer = parse_nats_frames(buffer)
            for msg in messages:
                self.metrics["frames_in"] += 1
                kind, payload = msg
                if kind == "PING":
                    try:
                        await ws.send(b"PONG\r\n")
                    except Exception:
                        logger.exception("PONG send failed; closing loop")
                        return
                    continue
                if kind == "ERR":
                    logger.warning("NATS ERR: %s", payload)
                    continue
                if kind != "MSG":
                    continue
                try:
                    await dispatch_message(self.pool, msg)
                    self.metrics["msgs_dispatched"] += 1
                except Exception:
                    self.metrics["dispatch_errors"] += 1
                    logger.exception("dispatch failed (subject=%s)", payload[0])

    async def _sleep_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass
