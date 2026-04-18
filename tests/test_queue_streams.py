from __future__ import annotations

import unittest

from schema_inspector.queue import (
    DELAYED_JOBS_KEY,
    STREAM_DISCOVERY,
    STREAM_DLQ,
    STREAM_HISTORICAL_DISCOVERY,
    STREAM_HISTORICAL_ENRICHMENT,
    STREAM_HISTORICAL_HYDRATE,
    STREAM_HISTORICAL_MAINTENANCE,
    STREAM_HISTORICAL_TOURNAMENT,
    STREAM_HYDRATE,
    STREAM_LIVE_DISCOVERY,
    STREAM_LIVE_HOT,
    STREAM_LIVE_WARM,
    STREAM_MAINTENANCE,
    STREAM_NORMALIZE,
)
from schema_inspector.queue.delayed import DelayedJobScheduler
from schema_inspector.queue.streams import ConsumerGroupInfo, PendingEntry, PendingSummary, RedisStreamQueue


class _FakeStreamBackend:
    def __init__(self) -> None:
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.group_offsets: dict[tuple[str, str], int] = {}
        self.pending: dict[tuple[str, str], set[str]] = {}
        self.pending_meta: dict[tuple[str, str], dict[str, dict[str, object]]] = {}
        self.counters: dict[str, int] = {}
        self.created_groups: list[tuple[str, str, str]] = []
        self.autoclaim_cursor: str = "0-0"
        self.claimed_by: dict[tuple[str, str], dict[str, str]] = {}

    def xadd(self, stream: str, fields: dict[str, str]) -> str:
        counter = self.counters.get(stream, 0) + 1
        self.counters[stream] = counter
        message_id = f"1-{counter}"
        self.streams.setdefault(stream, []).append((message_id, dict(fields)))
        return message_id

    def xgroup_create(self, stream: str, group: str, *, id: str = "0-0", mkstream: bool = False) -> bool:
        group_key = (stream, group)
        if group_key in self.group_offsets:
            raise RuntimeError("BUSYGROUP Consumer Group name already exists")
        if mkstream:
            self.streams.setdefault(stream, [])
        self.group_offsets[group_key] = 0
        self.created_groups.append((stream, group, id))
        return True

    def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int | None = None,
        block: int | None = None,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        del block
        results: list[tuple[str, list[tuple[str, dict[str, str]]]]] = []
        for stream in streams:
            offset = self.group_offsets.get((stream, group), 0)
            items = self.streams.get(stream, [])[offset : offset + (count or len(self.streams.get(stream, [])))]
            if items:
                self.group_offsets[(stream, group)] = offset + len(items)
                self.pending.setdefault((stream, group), set()).update(message_id for message_id, _ in items)
                pending_meta = self.pending_meta.setdefault((stream, group), {})
                claimed_by = self.claimed_by.setdefault((stream, group), {})
                for message_id, _ in items:
                    pending_meta[message_id] = {
                        "consumer": consumer,
                        "idle": 0,
                        "deliveries": 1,
                    }
                    claimed_by[message_id] = consumer
                results.append((stream, items))
        return results

    def xack(self, stream: str, group: str, *message_ids: str) -> int:
        pending = self.pending.setdefault((stream, group), set())
        pending_meta = self.pending_meta.setdefault((stream, group), {})
        claimed_by = self.claimed_by.setdefault((stream, group), {})
        acknowledged = 0
        for message_id in message_ids:
            if message_id in pending:
                pending.remove(message_id)
                pending_meta.pop(message_id, None)
                claimed_by.pop(message_id, None)
                acknowledged += 1
        return acknowledged

    def xpending(self, stream: str, group: str) -> dict[str, object]:
        pending_meta = self.pending_meta.setdefault((stream, group), {})
        consumers: dict[str, int] = {}
        for metadata in pending_meta.values():
            consumer = str(metadata["consumer"])
            consumers[consumer] = consumers.get(consumer, 0) + 1
        message_ids = sorted(pending_meta)
        return {
            "pending": len(pending_meta),
            "min": message_ids[0] if message_ids else None,
            "max": message_ids[-1] if message_ids else None,
            "consumers": consumers,
        }

    def xlen(self, stream: str) -> int:
        return len(self.streams.get(stream, ()))

    def xinfo_groups(self, stream: str) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for current_stream, group in sorted(self.group_offsets):
            if current_stream != stream:
                continue
            pending_meta = self.pending_meta.setdefault((stream, group), {})
            rows.append(
                {
                    "name": group,
                    "consumers": len({str(item["consumer"]) for item in pending_meta.values()}),
                    "pending": len(pending_meta),
                    "last-delivered-id": None if not self.streams.get(stream) else self.streams[stream][-1][0],
                    "entries-read": self.group_offsets[(stream, group)],
                    "lag": max(0, len(self.streams.get(stream, ())) - self.group_offsets[(stream, group)]),
                }
            )
        return rows

    def xpending_range(
        self,
        stream: str,
        group: str,
        min: str,
        max: str,
        count: int,
        consumername: str | None = None,
    ) -> list[dict[str, object]]:
        del min, max
        pending_meta = self.pending_meta.setdefault((stream, group), {})
        rows: list[dict[str, object]] = []
        for message_id in sorted(pending_meta):
            metadata = pending_meta[message_id]
            if consumername is not None and metadata["consumer"] != consumername:
                continue
            rows.append(
                {
                    "message_id": message_id,
                    "consumer": metadata["consumer"],
                    "time_since_delivered": metadata["idle"],
                    "times_delivered": metadata["deliveries"],
                }
            )
            if len(rows) >= count:
                break
        return rows

    def xautoclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        min_idle_time: int,
        start_id: str,
        *,
        count: int = 100,
    ) -> tuple[str, list[tuple[str, dict[str, str]]], list[str]]:
        del start_id
        pending_meta = self.pending_meta.setdefault((stream, group), {})
        claimed_by = self.claimed_by.setdefault((stream, group), {})
        claimed: list[tuple[str, dict[str, str]]] = []
        for message_id in sorted(pending_meta):
            metadata = pending_meta[message_id]
            if int(metadata["idle"]) < min_idle_time:
                continue
            metadata["consumer"] = consumer
            metadata["idle"] = 0
            metadata["deliveries"] = int(metadata["deliveries"]) + 1
            claimed_by[message_id] = consumer
            payload = dict(self._lookup_message(stream, message_id))
            claimed.append((message_id, payload))
            if len(claimed) >= count:
                break
        return (self.autoclaim_cursor, claimed, [])

    def _lookup_message(self, stream: str, message_id: str) -> dict[str, str]:
        for current_message_id, values in self.streams.get(stream, []):
            if current_message_id == message_id:
                return values
        raise KeyError(message_id)


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
        self.assertEqual(STREAM_HISTORICAL_DISCOVERY, "stream:etl:historical_discovery")
        self.assertEqual(STREAM_HISTORICAL_TOURNAMENT, "stream:etl:historical_tournament")
        self.assertEqual(STREAM_HISTORICAL_ENRICHMENT, "stream:etl:historical_enrichment")
        self.assertEqual(STREAM_HISTORICAL_HYDRATE, "stream:etl:historical_hydrate")
        self.assertEqual(STREAM_HISTORICAL_MAINTENANCE, "stream:etl:historical_maintenance")
        self.assertEqual(STREAM_LIVE_DISCOVERY, "stream:etl:live_discovery")
        self.assertEqual(STREAM_LIVE_HOT, "stream:etl:live_hot")
        self.assertEqual(STREAM_LIVE_WARM, "stream:etl:live_warm")
        self.assertEqual(STREAM_MAINTENANCE, "stream:etl:maintenance")
        self.assertEqual(STREAM_DLQ, "stream:etl:dlq")
        self.assertEqual(DELAYED_JOBS_KEY, "zset:etl:delayed")

    def test_stream_queue_tracks_distinct_groups_on_distinct_streams_independently(self) -> None:
        backend = _FakeStreamBackend()
        queue = RedisStreamQueue(backend)

        queue.ensure_group(STREAM_DISCOVERY, "cg:discovery")
        queue.ensure_group(STREAM_LIVE_DISCOVERY, "cg:live_discovery")
        queue.publish(STREAM_DISCOVERY, {"job_id": "scheduled-1"})
        queue.publish(STREAM_LIVE_DISCOVERY, {"job_id": "live-1"})

        scheduled_messages = queue.read_group(STREAM_DISCOVERY, "cg:discovery", "scheduled-consumer", count=10)
        live_messages = queue.read_group(STREAM_LIVE_DISCOVERY, "cg:live_discovery", "live-consumer", count=10)
        scheduled_group = queue.group_info(STREAM_DISCOVERY, "cg:discovery")
        live_group = queue.group_info(STREAM_LIVE_DISCOVERY, "cg:live_discovery")

        self.assertEqual([item.values["job_id"] for item in scheduled_messages], ["scheduled-1"])
        self.assertEqual([item.values["job_id"] for item in live_messages], ["live-1"])
        self.assertEqual(scheduled_group.entries_read if scheduled_group is not None else None, 1)
        self.assertEqual(live_group.entries_read if live_group is not None else None, 1)

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

    def test_stream_queue_can_create_group_idempotently(self) -> None:
        backend = _FakeStreamBackend()
        queue = RedisStreamQueue(backend)

        queue.ensure_group(STREAM_HYDRATE, "cg:hydrate")
        queue.ensure_group(STREAM_HYDRATE, "cg:hydrate")

        self.assertEqual(backend.created_groups, [(STREAM_HYDRATE, "cg:hydrate", "0-0")])

    def test_stream_queue_can_inspect_pending_summary_and_entries(self) -> None:
        backend = _FakeStreamBackend()
        queue = RedisStreamQueue(backend)

        queue.ensure_group(STREAM_HYDRATE, "cg:hydrate")
        queue.publish(STREAM_HYDRATE, {"job_id": "job-1"})
        queue.publish(STREAM_HYDRATE, {"job_id": "job-2"})
        queue.read_group(STREAM_HYDRATE, "cg:hydrate", "worker-a", count=10)
        backend.pending_meta[(STREAM_HYDRATE, "cg:hydrate")]["1-1"]["idle"] = 45_000
        backend.pending_meta[(STREAM_HYDRATE, "cg:hydrate")]["1-2"]["idle"] = 15_000

        summary = queue.pending_summary(STREAM_HYDRATE, "cg:hydrate")
        entries = queue.pending_entries(STREAM_HYDRATE, "cg:hydrate", count=10)

        self.assertEqual(
            summary,
            PendingSummary(
                total=2,
                smallest_id="1-1",
                largest_id="1-2",
                consumers={"worker-a": 2},
            ),
        )
        self.assertEqual(
            entries,
            (
                PendingEntry(message_id="1-1", consumer="worker-a", idle_ms=45_000, delivery_count=1),
                PendingEntry(message_id="1-2", consumer="worker-a", idle_ms=15_000, delivery_count=1),
            ),
        )

    def test_stream_queue_can_claim_stale_messages(self) -> None:
        backend = _FakeStreamBackend()
        queue = RedisStreamQueue(backend)

        queue.ensure_group(STREAM_HYDRATE, "cg:hydrate")
        queue.publish(STREAM_HYDRATE, {"job_id": "job-1"})
        queue.publish(STREAM_HYDRATE, {"job_id": "job-2"})
        queue.read_group(STREAM_HYDRATE, "cg:hydrate", "worker-a", count=10)
        backend.pending_meta[(STREAM_HYDRATE, "cg:hydrate")]["1-1"]["idle"] = 45_000
        backend.pending_meta[(STREAM_HYDRATE, "cg:hydrate")]["1-2"]["idle"] = 5_000

        claimed = queue.claim_stale(
            STREAM_HYDRATE,
            "cg:hydrate",
            "worker-b",
            min_idle_ms=30_000,
            count=10,
        )

        self.assertEqual(len(claimed), 1)
        self.assertEqual(claimed[0].message_id, "1-1")
        self.assertEqual(claimed[0].values["job_id"], "job-1")
        self.assertEqual(backend.claimed_by[(STREAM_HYDRATE, "cg:hydrate")]["1-1"], "worker-b")
        self.assertEqual(backend.pending_meta[(STREAM_HYDRATE, "cg:hydrate")]["1-1"]["deliveries"], 2)

    def test_stream_queue_can_report_length_and_group_info(self) -> None:
        backend = _FakeStreamBackend()
        queue = RedisStreamQueue(backend)

        queue.ensure_group(STREAM_HYDRATE, "cg:hydrate")
        queue.publish(STREAM_HYDRATE, {"job_id": "job-1"})
        queue.publish(STREAM_HYDRATE, {"job_id": "job-2"})
        queue.read_group(STREAM_HYDRATE, "cg:hydrate", "worker-a", count=1)

        self.assertEqual(queue.stream_length(STREAM_HYDRATE), 2)
        self.assertEqual(
            queue.group_info(STREAM_HYDRATE, "cg:hydrate"),
            ConsumerGroupInfo(
                consumers=1,
                pending=1,
                last_delivered_id="1-2",
                entries_read=1,
                lag=1,
            ),
        )

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
