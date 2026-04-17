"""Redis Streams queue facade for ETL job movement."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

STREAM_DISCOVERY = "stream:etl:discovery"
STREAM_HYDRATE = "stream:etl:hydrate"
STREAM_NORMALIZE = "stream:etl:normalize"
STREAM_HISTORICAL_DISCOVERY = "stream:etl:historical_discovery"
STREAM_HISTORICAL_TOURNAMENT = "stream:etl:historical_tournament"
STREAM_HISTORICAL_ENRICHMENT = "stream:etl:historical_enrichment"
STREAM_HISTORICAL_HYDRATE = "stream:etl:historical_hydrate"
STREAM_HISTORICAL_MAINTENANCE = "stream:etl:historical_maintenance"
STREAM_LIVE_HOT = "stream:etl:live_hot"
STREAM_LIVE_WARM = "stream:etl:live_warm"
STREAM_MAINTENANCE = "stream:etl:maintenance"
STREAM_DLQ = "stream:etl:dlq"


@dataclass(frozen=True)
class StreamEntry:
    stream: str
    message_id: str
    values: dict[str, str]


@dataclass(frozen=True)
class PendingSummary:
    total: int
    smallest_id: str | None
    largest_id: str | None
    consumers: dict[str, int]


@dataclass(frozen=True)
class PendingEntry:
    message_id: str
    consumer: str
    idle_ms: int
    delivery_count: int


@dataclass(frozen=True)
class ConsumerGroupInfo:
    consumers: int
    pending: int
    last_delivered_id: str | None
    entries_read: int | None
    lag: int | None


class RedisStreamQueue:
    """Thin queue wrapper over a redis-py compatible Streams backend."""

    def __init__(self, backend: Any) -> None:
        self.backend = backend

    def publish(self, stream: str, values: Mapping[str, object]) -> str:
        payload = {str(key): _stringify(value) for key, value in values.items()}
        return str(self.backend.xadd(stream, payload))

    def ensure_group(self, stream: str, group: str, *, start_id: str = "0-0") -> None:
        try:
            self.backend.xgroup_create(stream, group, id=start_id, mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

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

    def pending_summary(self, stream: str, group: str) -> PendingSummary:
        raw = self.backend.xpending(stream, group)
        if isinstance(raw, Mapping):
            consumers = {str(key): int(value) for key, value in dict(raw.get("consumers", {})).items()}
            return PendingSummary(
                total=int(raw.get("pending", 0)),
                smallest_id=_optional_string(raw.get("min")),
                largest_id=_optional_string(raw.get("max")),
                consumers=consumers,
            )
        if isinstance(raw, (tuple, list)) and len(raw) >= 4:
            consumer_rows = raw[3]
            consumers: dict[str, int] = {}
            for consumer_row in consumer_rows:
                if isinstance(consumer_row, (tuple, list)) and len(consumer_row) >= 2:
                    consumers[str(consumer_row[0])] = int(consumer_row[1])
            return PendingSummary(
                total=int(raw[0]),
                smallest_id=_optional_string(raw[1]),
                largest_id=_optional_string(raw[2]),
                consumers=consumers,
            )
        raise TypeError(f"Unsupported XPENDING response: {raw!r}")

    def pending_entries(
        self,
        stream: str,
        group: str,
        *,
        start_id: str = "-",
        end_id: str = "+",
        count: int = 100,
        consumer: str | None = None,
    ) -> tuple[PendingEntry, ...]:
        rows = self.backend.xpending_range(stream, group, start_id, end_id, count, consumername=consumer)
        entries: list[PendingEntry] = []
        for row in rows:
            if isinstance(row, Mapping):
                entries.append(
                    PendingEntry(
                        message_id=str(row.get("message_id")),
                        consumer=str(row.get("consumer")),
                        idle_ms=int(row.get("time_since_delivered", 0)),
                        delivery_count=int(row.get("times_delivered", 0)),
                    )
                )
                continue
            if isinstance(row, (tuple, list)) and len(row) >= 4:
                entries.append(
                    PendingEntry(
                        message_id=str(row[0]),
                        consumer=str(row[1]),
                        idle_ms=int(row[2]),
                        delivery_count=int(row[3]),
                    )
                )
                continue
            raise TypeError(f"Unsupported XPENDING range row: {row!r}")
        return tuple(entries)

    def stream_length(self, stream: str) -> int:
        return int(self.backend.xlen(stream))

    def group_info(self, stream: str, group: str) -> ConsumerGroupInfo | None:
        rows = self.backend.xinfo_groups(stream)
        for row in rows:
            if str(_mapping_get(row, "name") or "") != str(group):
                continue
            return ConsumerGroupInfo(
                consumers=int(_mapping_get(row, "consumers", 0) or 0),
                pending=int(_mapping_get(row, "pending", 0) or 0),
                last_delivered_id=_optional_string(
                    _mapping_get(row, "last-delivered-id") or _mapping_get(row, "last_delivered_id")
                ),
                entries_read=_optional_int(_mapping_get(row, "entries-read") or _mapping_get(row, "entries_read")),
                lag=_optional_int(_mapping_get(row, "lag")),
            )
        return None

    def claim_stale(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        min_idle_ms: int,
        start_id: str = "0-0",
        count: int = 100,
    ) -> tuple[StreamEntry, ...]:
        raw = self.backend.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_ms,
            start_id,
            count=count,
        )
        rows: object
        if isinstance(raw, (tuple, list)) and len(raw) >= 2:
            rows = raw[1]
        else:
            raise TypeError(f"Unsupported XAUTOCLAIM response: {raw!r}")
        entries: list[StreamEntry] = []
        for message_id, values in rows:
            entries.append(
                StreamEntry(
                    stream=str(stream),
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


def _optional_string(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _mapping_get(row: object, key: str, default: object = None) -> object:
    if isinstance(row, Mapping):
        return row.get(key, default)
    return default
