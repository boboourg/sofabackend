"""Mirror Sofascore WebSocket server.

Speaks the same NATS subset the upstream feed at
``wss://ws.sofascore.com:9222`` does, but with our consumer's deltas
re-broadcast via redis pub/sub. Mobile clients can ``SUB sport.football``
or ``SUB event.16167494`` and get fresh updates without polling.

Threading model:
  * one FastAPI app with a single WebSocket route (``/ws/v1``)
  * per-connection asyncio task that owns the recv loop
  * one **shared** redis subscriber task per process that fans messages
    out to all connected clients via the SubscriptionManager
  * fire-and-forget — no replay buffer, no auth, no rate limiting

Connection lifecycle:
  client ← INFO {server_id, version}\r\n
  client → CONNECT {...}\r\n        (ack, body ignored)
  client → SUB <subject> <sid>\r\n
  client ← MSG <subject> <sid> <bytes>\r\n<payload>\r\n  ...
  client → PING\r\n
  client ← PONG\r\n
  client → UNSUB <sid>\r\n
  (or socket closes)
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from ..ws_server_protocol import (
    SubscriptionManager,
    format_info_frame,
    format_msg_frame,
    parse_client_frames,
    subject_to_channel,
)

logger = logging.getLogger(__name__)


class MirrorWSServer:
    """Holds the shared SubscriptionManager + the redis listener task.

    One instance lives for the lifetime of the FastAPI process. The
    WebSocket endpoint refers to it via a closure captured at
    ``build_app``.
    """

    def __init__(self, redis: Any, *, server_id: str | None = None) -> None:
        self.redis = redis
        self.server_id = server_id or f"sofascore-mirror-{uuid.uuid4().hex[:8]}"
        self.sub_manager = SubscriptionManager()
        # client_id → asyncio.Queue[str]  (frames waiting to be sent)
        self._client_queues: dict[str, asyncio.Queue[str]] = {}
        # background listener task
        self._listener_task: asyncio.Task[None] | None = None
        # the redis pubsub object we read from
        self._pubsub: Any | None = None
        # subscribed channels (we sub/unsub lazily as clients add subjects)
        self._listening: set[str] = set()
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if self._pubsub is None:
            self._pubsub = self.redis.pubsub()
            # redis-py async PubSub.get_message() raises "pubsub
            # connection not set" if no channels were ever subscribed.
            # Subscribe to a placeholder so the connection initialises
            # before any client SUB arrives. Real subscriptions are
            # added lazily by _sync_redis_subscriptions().
            try:
                await self._pubsub.subscribe("ws:fanout:_warmup_")
                self._listening.add("ws:fanout:_warmup_")
            except Exception:
                logger.exception("ws-server warmup subscribe failed")
        if self._listener_task is None or self._listener_task.done():
            self._listener_task = asyncio.create_task(self._listen_redis())

    async def stop(self) -> None:
        if self._listener_task is not None:
            self._listener_task.cancel()
            with contextlib.suppress(Exception):
                await self._listener_task
            self._listener_task = None
        if self._pubsub is not None:
            with contextlib.suppress(Exception):
                await self._pubsub.close()
            self._pubsub = None

    async def add_client(self, client_id: str) -> asyncio.Queue[str]:
        q: asyncio.Queue[str] = asyncio.Queue(maxsize=1024)
        self._client_queues[client_id] = q
        return q

    async def remove_client(self, client_id: str) -> None:
        self.sub_manager.disconnect(client_id)
        self._client_queues.pop(client_id, None)
        # Compact: drop redis subs that no client cares about now.
        await self._sync_redis_subscriptions()

    async def subscribe(self, client_id: str, *, subject: str, sid: int) -> bool:
        ok = self.sub_manager.subscribe(client_id, subject=subject, sid=sid)
        if ok:
            await self._sync_redis_subscriptions()
        return ok

    async def unsubscribe(self, client_id: str, *, sid: int) -> None:
        self.sub_manager.unsubscribe(client_id, sid=sid)
        await self._sync_redis_subscriptions()

    async def _sync_redis_subscriptions(self) -> None:
        """Keep our redis SUBSCRIBE set equal to the union of channels
        across all live subscriptions."""
        async with self._lock:
            if self._pubsub is None:
                return
            desired = self.sub_manager.channels_to_listen()
            to_add = desired - self._listening
            to_remove = self._listening - desired
            for channel in to_add:
                try:
                    await self._pubsub.subscribe(channel)
                    self._listening.add(channel)
                except Exception:
                    logger.exception("redis subscribe failed for channel=%s", channel)
            for channel in to_remove:
                try:
                    await self._pubsub.unsubscribe(channel)
                    self._listening.discard(channel)
                except Exception:
                    logger.exception("redis unsubscribe failed for channel=%s", channel)

    async def _listen_redis(self) -> None:
        """Read messages off the shared redis pubsub object and fan out
        to matching client queues."""
        assert self._pubsub is not None
        while True:
            try:
                msg = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("redis listener get_message failed")
                await asyncio.sleep(0.5)
                continue
            if msg is None:
                continue
            channel = msg.get("channel")
            data = msg.get("data")
            if isinstance(channel, bytes):
                channel = channel.decode("utf-8", errors="replace")
            if isinstance(data, bytes):
                data = data.decode("utf-8", errors="replace")
            if not isinstance(channel, str) or not isinstance(data, str):
                continue
            try:
                envelope = json.loads(data)
            except json.JSONDecodeError:
                continue
            subject = envelope.get("subject")
            payload = envelope.get("payload")
            if not isinstance(subject, str) or payload is None:
                continue
            payload_str = json.dumps(payload, separators=(",", ":"))
            for client_id, sid, _ in self.sub_manager.matches_for(channel):
                q = self._client_queues.get(client_id)
                if q is None:
                    continue
                frame = format_msg_frame(subject=subject, sid=sid, payload=payload_str)
                # Drop on full queue rather than block — fire-and-forget.
                try:
                    q.put_nowait(frame)
                except asyncio.QueueFull:
                    pass


def build_app(redis: Any) -> FastAPI:
    server = MirrorWSServer(redis)
    app = FastAPI(title="Sofascore Mirror WS", lifespan=_lifespan_factory(server))

    @app.websocket("/ws/v1")
    async def websocket_endpoint(ws: WebSocket) -> None:
        await ws.accept()
        client_id = uuid.uuid4().hex
        outbound = await server.add_client(client_id)
        # Greet
        try:
            await ws.send_text(
                format_info_frame(
                    server_id=server.server_id,
                    version="0.1",
                    max_payload=1048576,
                    proto=1,
                )
            )
        except Exception:
            await server.remove_client(client_id)
            return

        async def sender() -> None:
            while True:
                frame = await outbound.get()
                try:
                    await ws.send_text(frame)
                except Exception:
                    return

        send_task = asyncio.create_task(sender())
        buffer = ""
        try:
            while True:
                try:
                    chunk = await ws.receive_text()
                except WebSocketDisconnect:
                    break
                buffer += chunk
                frames, buffer = parse_client_frames(buffer)
                for kind, payload in frames:
                    if kind == "PING":
                        await outbound.put("PONG\r\n")
                    elif kind == "PONG":
                        pass
                    elif kind == "CONNECT":
                        # ignore body, just ack via PONG-style noop
                        pass
                    elif kind == "SUB":
                        subject, sid = payload
                        ok = await server.subscribe(
                            client_id, subject=subject, sid=sid,
                        )
                        if not ok:
                            await outbound.put(
                                f"-ERR 'Permission Violated for Subject {subject}'\r\n"
                            )
                    elif kind == "UNSUB":
                        await server.unsubscribe(client_id, sid=payload)
        finally:
            send_task.cancel()
            with contextlib.suppress(Exception):
                await send_task
            await server.remove_client(client_id)

    return app


def _lifespan_factory(server: MirrorWSServer):
    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI):
        await server.start()
        try:
            yield
        finally:
            await server.stop()

    return lifespan
