"""Typed job envelope used by the hybrid ETL planner and workers."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Mapping


@dataclass(frozen=True)
class JobEnvelope:
    job_id: str
    job_type: str
    sport_slug: str | None
    entity_type: str | None
    entity_id: int | None
    scope: str | None
    params: dict[str, object]
    priority: int
    scheduled_at: str
    attempt: int
    parent_job_id: str | None
    trace_id: str | None
    capability_hint: str | None
    idempotency_key: str

    @classmethod
    def create(
        cls,
        *,
        job_type: str,
        sport_slug: str | None,
        entity_type: str | None,
        entity_id: int | None,
        scope: str | None,
        params: Mapping[str, object] | None,
        priority: int,
        trace_id: str | None,
        scheduled_at: str | None = None,
        attempt: int = 1,
        parent_job_id: str | None = None,
        capability_hint: str | None = None,
    ) -> "JobEnvelope":
        resolved_params = dict(params or {})
        return cls(
            job_id=str(uuid.uuid4()),
            job_type=job_type,
            sport_slug=sport_slug,
            entity_type=entity_type,
            entity_id=entity_id,
            scope=scope,
            params=resolved_params,
            priority=priority,
            scheduled_at=scheduled_at or _utc_now(),
            attempt=attempt,
            parent_job_id=parent_job_id,
            trace_id=trace_id,
            capability_hint=capability_hint,
            idempotency_key=_build_idempotency_key(
                job_type=job_type,
                sport_slug=sport_slug,
                entity_type=entity_type,
                entity_id=entity_id,
                scope=scope,
                params=resolved_params,
            ),
        )

    def spawn_child(
        self,
        *,
        job_type: str,
        entity_type: str | None,
        entity_id: int | None,
        scope: str | None,
        params: Mapping[str, object] | None,
        priority: int,
        capability_hint: str | None = None,
    ) -> "JobEnvelope":
        return JobEnvelope.create(
            job_type=job_type,
            sport_slug=self.sport_slug,
            entity_type=entity_type,
            entity_id=entity_id,
            scope=scope,
            params=params,
            priority=priority,
            trace_id=self.trace_id,
            parent_job_id=self.job_id,
            capability_hint=capability_hint,
        )


def _build_idempotency_key(
    *,
    job_type: str,
    sport_slug: str | None,
    entity_type: str | None,
    entity_id: int | None,
    scope: str | None,
    params: Mapping[str, object],
) -> str:
    identity = {
        "job_type": job_type,
        "sport_slug": sport_slug,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "scope": scope,
        "params": params,
    }
    rendered = json.dumps(identity, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
