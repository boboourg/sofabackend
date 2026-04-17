"""Bootstrap helpers for long-running planner and worker services."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from ..jobs.types import JOB_REPLAY_FAILED_JOB
from ..queue.delayed import DelayedJobScheduler
from ..queue.streams import (
    STREAM_DISCOVERY,
    STREAM_HYDRATE,
    STREAM_LIVE_HOT,
    STREAM_LIVE_WARM,
    STREAM_MAINTENANCE,
    RedisStreamQueue,
    StreamEntry,
)
from ..services.planner_daemon import PlannerDaemon, ScheduledPlanningTarget
from ..workers._stream_jobs import decode_stream_payload
from ..workers.hydrate_worker import HydrateWorker
from ..workers.live_worker_service import LiveWorkerService
from ..workers.maintenance_worker import MaintenanceWorker

logger = logging.getLogger(__name__)

DELAYED_ENVELOPE_HASH = "hash:etl:delayed_envelopes"
DEFAULT_SERVICE_SPORT_SLUGS = (
    "football",
    "basketball",
    "tennis",
    "volleyball",
    "baseball",
    "american-football",
    "handball",
    "table-tennis",
    "ice-hockey",
    "rugby",
    "cricket",
    "futsal",
    "esports",
)

DEFAULT_CONSUMER_GROUPS = (
    (STREAM_DISCOVERY, "cg:discovery"),
    (STREAM_HYDRATE, "cg:hydrate"),
    (STREAM_LIVE_HOT, "cg:live_hot"),
    (STREAM_LIVE_WARM, "cg:live_warm"),
    (STREAM_MAINTENANCE, "cg:maintenance"),
)


class DelayedEnvelopeStore:
    """Stores serialized job payloads so delayed jobs can be reconstructed later."""

    def __init__(self, backend: Any, *, hash_key: str = DELAYED_ENVELOPE_HASH) -> None:
        self.backend = backend
        self.hash_key = hash_key

    def save_entry(self, entry: StreamEntry) -> None:
        self.save_payload(entry.values, fallback_job_id=entry.message_id)

    def save_payload(self, payload: dict[str, object] | dict[str, str], *, fallback_job_id: str | None = None) -> None:
        job_id = str(payload.get("job_id") or fallback_job_id or "")
        if not job_id:
            raise RuntimeError("Delayed envelope store requires a job_id.")
        encoded = json.dumps({str(key): value for key, value in payload.items()}, ensure_ascii=True, sort_keys=True)
        self.backend.hset(self.hash_key, {job_id: encoded})

    def load(self, job_id: str):
        raw_payload = self.backend.hgetall(self.hash_key).get(str(job_id))
        if raw_payload in (None, ""):
            return None
        if isinstance(raw_payload, bytes):
            raw_payload = raw_payload.decode("utf-8", errors="ignore")
        values = json.loads(str(raw_payload))
        job = decode_stream_payload(values, fallback_job_id=str(job_id))
        delete = getattr(self.backend, "hdel", None)
        if callable(delete):
            delete(self.hash_key, str(job_id))
        return job


@dataclass
class ServiceApp:
    app: Any

    def __post_init__(self) -> None:
        if self.app.redis_backend is None:
            raise RuntimeError("Redis backend is required for continuous services.")
        self.stream_queue = self.app.stream_queue or RedisStreamQueue(self.app.redis_backend)
        self.live_state_store = self.app.live_state_store
        self.delayed_scheduler = DelayedJobScheduler(self.app.redis_backend)
        self.delayed_envelope_store = DelayedEnvelopeStore(self.app.redis_backend)

    def ensure_consumer_groups(self) -> None:
        for stream, group in DEFAULT_CONSUMER_GROUPS:
            self.stream_queue.ensure_group(stream, group)

    def build_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        scheduled_interval_seconds: float = 300.0,
        loop_interval_seconds: float = 5.0,
    ) -> PlannerDaemon:
        normalized_sports = tuple(sport_slugs or DEFAULT_SERVICE_SPORT_SLUGS)
        targets = tuple(
            ScheduledPlanningTarget(
                sport_slug=sport_slug,
                interval_ms=int(scheduled_interval_seconds * 1000),
                date_factory=self._date_factory,
            )
            for sport_slug in normalized_sports
        )
        return PlannerDaemon(
            queue=self.stream_queue,
            delayed_scheduler=self.delayed_scheduler,
            delayed_job_loader=self.delayed_envelope_store.load,
            live_state_store=self.live_state_store,
            scheduled_targets=targets,
            loop_interval_s=loop_interval_seconds,
        )

    def build_hydrate_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> HydrateWorker:
        self.ensure_consumer_groups()
        return HydrateWorker(
            orchestrator=self.app,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            queue=self.stream_queue,
            consumer=consumer_name,
            block_ms=block_ms,
        )

    def build_live_worker(self, *, lane: str, consumer_name: str, block_ms: int = 5_000) -> LiveWorkerService:
        self.ensure_consumer_groups()
        return LiveWorkerService(
            orchestrator=self.app,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            queue=self.stream_queue,
            lane=lane,
            consumer=consumer_name,
            block_ms=block_ms,
        )

    def build_maintenance_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> MaintenanceWorker:
        self.ensure_consumer_groups()
        return MaintenanceWorker(
            handler=self._handle_maintenance,
            delayed_scheduler=self.delayed_scheduler,
            delayed_payload_store=self.delayed_envelope_store,
            queue=self.stream_queue,
            consumer=consumer_name,
            block_ms=block_ms,
        )

    async def run_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...] | None = None,
        scheduled_interval_seconds: float = 300.0,
        loop_interval_seconds: float = 5.0,
    ) -> None:
        self.ensure_consumer_groups()
        daemon = self.build_planner_daemon(
            sport_slugs=sport_slugs,
            scheduled_interval_seconds=scheduled_interval_seconds,
            loop_interval_seconds=loop_interval_seconds,
        )
        await daemon.run_forever()

    async def run_hydrate_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> None:
        worker = self.build_hydrate_worker(consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()

    async def run_live_worker(self, *, lane: str, consumer_name: str, block_ms: int = 5_000) -> None:
        worker = self.build_live_worker(lane=lane, consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()

    async def run_maintenance_worker(self, *, consumer_name: str, block_ms: int = 5_000) -> None:
        worker = self.build_maintenance_worker(consumer_name=consumer_name, block_ms=block_ms)
        await worker.run_forever()

    async def _handle_maintenance(self, entry: StreamEntry) -> str:
        job = decode_stream_payload(entry.values, fallback_job_id=entry.message_id)
        if job.job_type == JOB_REPLAY_FAILED_JOB:
            snapshot_id = job.params.get("snapshot_id")
            if snapshot_id is None:
                return "ignored"
            await self.app.replay_snapshot(int(snapshot_id))
            return "replayed"
        logger.info("Maintenance worker ignored job_type=%s job_id=%s", job.job_type, job.job_id)
        return "ignored"

    @staticmethod
    def _date_factory(now_ms: int) -> str:
        from datetime import datetime, timezone

        return datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).date().isoformat()
