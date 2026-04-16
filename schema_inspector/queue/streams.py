"""Redis Streams queue facade for ETL job movement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

STREAM_DISCOVERY = "stream:etl:discovery"
STREAM_HYDRATE = "stream:etl:hydrate"
STREAM_NORMALIZE = "stream:etl:normalize"
STREAM_LIVE_HOT = "stream:etl:live_hot"
STREAM_LIVE_WARM = "stream:etl:live_warm"
STREAM_MAINTENANCE = "stream:etl:maintenance"
STREAM_DLQ = "stream:etl:dlq"


@dataclass(frozen=True)
class StreamEntry:
    stream: str
    message_id: str
    values: dict[str, str]


class RedisStreamQueue:
    """Thin queue wrapper over a redis-py compatible Streams backend."""

    def __init__(self, backend: Any) -> None:
        self.backend = backend

    def publish(self, stream: str, values: Mapping[str, object]) -> str:
        payload = {str(key): _stringify(value) for key, value in values.items()}
        return str(self.backend.xadd(stream, payload))

    def read_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int = 1,
        block_ms: int | None = None,
    ) -> tuple[StreamEntry, ...]:
        rows = self.backend.xreadgroup(group, consumer, {stream: ">"}, count=count, block=block_ms)
        entries: list[StreamEntry] = []
        for returned_stream, messages in rows:
            for message_id, values in messages:
                entries.append(
                    StreamEntry(
                        stream=str(returned_stream),
                        message_id=str(message_id),
                        values={str(key): _stringify(value) for key, value in values.items()},
                    )
                )
        return tuple(entries)

    def ack(self, stream: str, group: str, message_ids: tuple[str, ...]) -> int:
        if not message_ids:
            return 0
        return int(self.backend.xack(stream, group, *message_ids))


def _stringify(value: object) -> str:
    if value is None:
        return ""
    return str(value)
