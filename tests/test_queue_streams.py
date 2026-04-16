from __future__ import annotations

import unittest

from schema_inspector.queue import (
    DELAYED_JOBS_KEY,
    STREAM_DISCOVERY,
    STREAM_DLQ,
    STREAM_HYDRATE,
    STREAM_LIVE_HOT,
    STREAM_LIVE_WARM,
    STREAM_MAINTENANCE,
    STREAM_NORMALIZE,
)
from schema_inspector.queue.delayed import DelayedJobScheduler
from schema_inspector.queue.streams import RedisStreamQueue


class _FakeStreamBackend:
    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.group_offsets: dict[tuple[str, str], int] = {}
        self.pending: dict[tuple[str, str], set[str]] = {}
        self.counters: dict[str, int] = {}

    def xadd(self, stream: str, fields: dict[str, str]) -> str:
        counter = self.counters.get(stream, 0) + 1
        self.counters[stream] = counter
        message_id = f"1-{counter}"
        self.streams.setdefault(stream, []).append((message_id, dict(fields)))
        return message_id

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        del consumer, block
        results: list[tuple[str, list[tuple[str, dict[str, str]]]]] = []
        for stream in streams:
            offset = self.group_offsets.get((stream, group), 0)
            items = self.streams.get(stream, [])[offset : offset + (count or len(self.streams.get(stream, [])))]
            if items:
                self.group_offsets[(stream, group)] = offset + len(items)
                self.pending.setdefault((stream, group), set()).update(message_id for message_id, _ in items)
                results.append((stream, items))
        return results

    def xack(self, stream: str, group: str, *message_ids: str) -> int:
        pending = self.pending.setdefault((stream, group), set())
        acknowledged = 0
        for message_id in message_ids:
            if message_id in pending:
                pending.remove(message_id)
                acknowledged += 1
        return acknowledged


class _FakeSortedSetBackend:
    def __init__(self) -> None:
        self.values: dict[str, dict[str, float]] = {}

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.values.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            if member not in bucket:
                added += 1
            bucket[member] = float(score)
        return added

    def zrangebyscore(
        self,
        key: str,
        min_score: float,
        max_score: float,
        *,
        start: int = 0,
        num: int | None = None,
        withscores: bool = False,
    ) -> list[tuple[str, float]] | list[str]:
        items = [
            (member, score)
            for member, score in sorted(self.values.get(key, {}).items(), key=lambda item: (item[1], item[0]))
            if min_score <= score <= max_score
        ]
        sliced = items[start : start + num if num is not None else None]
        if withscores:
            return sliced
        return [member for member, _ in sliced]

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.values.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                removed += 1
                del bucket[member]
        return removed


class QueueStreamsTests(unittest.TestCase):
    def test_stream_constants_match_planned_queues(self) -> None:
        self.assertEqual(STREAM_DISCOVERY, "stream:etl:discovery")
        self.assertEqual(STREAM_HYDRATE, "stream:etl:hydrate")
        self.assertEqual(STREAM_NORMALIZE, "stream:etl:normalize")
        self.assertEqual(STREAM_LIVE_HOT, "stream:etl:live_hot")
        self.assertEqual(STREAM_LIVE_WARM, "stream:etl:live_warm")
        self.assertEqual(STREAM_MAINTENANCE, "stream:etl:maintenance")
        self.assertEqual(STREAM_DLQ, "stream:etl:dlq")
        self.assertEqual(DELAYED_JOBS_KEY, "zset:etl:delayed")

    def test_stream_queue_can_publish_read_and_ack(self) -> None:
        queue = RedisStreamQueue(_FakeStreamBackend())

        message_id = queue.publish(STREAM_DISCOVERY, {"job_id": "job-1", "job_type": "discover"})
        messages = queue.read_group(STREAM_DISCOVERY, "cg:discovery", "worker-a", count=10)
        acknowledged = queue.ack(STREAM_DISCOVERY, "cg:discovery", (message_id,))

        self.assertEqual(message_id, "1-1")
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].message_id, "1-1")
        self.assertEqual(messages[0].values["job_id"], "job-1")
        self.assertEqual(acknowledged, 1)

    def test_delayed_scheduler_returns_due_jobs_in_score_order(self) -> None:
        scheduler = DelayedJobScheduler(_FakeSortedSetBackend())

        scheduler.schedule("job-2", run_at_epoch_ms=2000)
        scheduler.schedule("job-1", run_at_epoch_ms=1000)
        scheduler.schedule("job-3", run_at_epoch_ms=3000)

        due = scheduler.pop_due(now_epoch_ms=2500, limit=10)

        self.assertEqual([item.job_id for item in due], ["job-1", "job-2"])
        self.assertEqual([item.run_at_epoch_ms for item in due], [1000, 2000])


if __name__ == "__main__":
    unittest.main()
