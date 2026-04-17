"""Helpers for decoding stream payloads into job-like objects."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from ..jobs.envelope import JobEnvelope
from ..queue.streams import StreamEntry


def decode_stream_job(entry: StreamEntry) -> JobEnvelope:
    return decode_stream_payload(entry.values, fallback_job_id=entry.message_id)


def decode_stream_payload(values: dict[str, object] | dict[str, str], *, fallback_job_id: str | None = None) -> JobEnvelope:
    entity_id = _as_int(values.get("entity_id"))
    if entity_id is None:
        entity_id = _as_int(values.get("event_id"))

    params = _decode_params(values.get("params_json"))
    status_type = _as_text(values.get("status_type"))
    if status_type is not None and "status_type" not in params:
        params["status_type"] = status_type
    next_poll_at = _as_int(values.get("next_poll_at"))
    if next_poll_at is not None and "next_poll_at" not in params:
        params["next_poll_at"] = next_poll_at

    return JobEnvelope(
        job_id=_as_text(values.get("job_id")) or str(fallback_job_id or ""),
        job_type=_as_text(values.get("job_type")) or "",
        sport_slug=_as_text(values.get("sport_slug")),
        entity_type=_as_text(values.get("entity_type")) or ("event" if entity_id is not None else None),
        entity_id=entity_id,
        scope=_as_text(values.get("scope")) or _as_text(values.get("lane")),
        params=params,
        priority=_as_int(values.get("priority")) or 0,
        scheduled_at=_as_text(values.get("scheduled_at")) or _utc_now(),
        attempt=_as_int(values.get("attempt")) or 1,
        parent_job_id=_as_text(values.get("parent_job_id")),
        trace_id=_as_text(values.get("trace_id")),
        capability_hint=_as_text(values.get("capability_hint")),
        idempotency_key=_as_text(values.get("idempotency_key")) or "",
    )


def _decode_params(raw: object) -> dict[str, object]:
    text = _as_text(raw)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if isinstance(parsed, dict):
        return dict(parsed)
    return {}


def _as_text(value: object) -> str | None:
    if value in (None, "", b""):
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def _as_int(value: object) -> int | None:
    text = _as_text(value)
    if text is None:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
