"""Live event polling state and lane indexes."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

LIVE_HOT_ZSET = "zset:live:hot"
LIVE_WARM_ZSET = "zset:live:warm"
LIVE_COLD_ZSET = "zset:live:cold"
LIVE_DISPATCH_CLAIM_PREFIX = "live:dispatch_claim"

# #42 (2026-05-30) atomic lane membership. KEYS[1..3] = hot/warm/cold zsets,
# KEYS[4] = target-lane zset, KEYS[5] = event hash (live:event:{id}).
# ARGV[1] = member, ARGV[2] = next_poll_at score. ZREM from all three lanes,
# then refuse to (re-)ZADD if the event hash is already finalized
# (HGET is_finalized in {"1","true"} — the decode_responses=True string forms
# of int(state.is_finalized) / _as_bool). Folding the finalized check into the
# same EVAL closes the planner/track_event read-then-ZADD TOCTOU that re-livens
# ended matches. Returns 1 when (re)added, 0 when refused.
_MOVE_LANE_SCRIPT = """
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
local finalized = redis.call("HGET", KEYS[5], "is_finalized")
if finalized == "1" or finalized == "true" then
    return 0
end
redis.call("ZADD", KEYS[4], ARGV[2], ARGV[1])
return 1
"""

# #42 atomic finalize lane-removal. KEYS[1..3] = hot/warm/cold zsets,
# ARGV[1] = member. Single EVAL replacing finalize_event's three discrete
# ZREMs so a finalize can never leave the member in only some lanes.
_REMOVE_FROM_LANES_SCRIPT = """
redis.call("ZREM", KEYS[1], ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("ZREM", KEYS[3], ARGV[1])
return 1
"""
# F-7 Phase 0: cumulative dispatch metrics for diagnosing the 90s lease cap.
# Survives planner restarts (no reset), HINCRBY-incremented per event from
# the planner publish path and the live worker claim-clear path. Exposed
# read-only via /ops/queues/summary. Never read in any hot path — only by
# the periodic ops poller.
LIVE_DISPATCH_METRICS_KEY = "live:dispatch_metrics"
_LIVE_DISPATCH_TIER_FIELDS: tuple[str, ...] = ("tier_1", "tier_2", "tier_3")


@dataclass(frozen=True)
class LiveEventState:
    event_id: int
    sport_slug: str
    status_type: str | None
    poll_profile: str | None
    last_seen_at: int | None
    last_ingested_at: int | None
    last_changed_at: int | None
    next_poll_at: int | None
    hot_until: int | None
    home_score: int | None
    away_score: int | None
    version_hint: str | None
    is_finalized: bool
    dispatch_tier: str | None = None


class LiveEventStateStore:
    """Stores live event metadata and indexes by polling lane."""

    def __init__(
        self,
        backend: Any,
        *,
        hot_zset_key: str = LIVE_HOT_ZSET,
        warm_zset_key: str = LIVE_WARM_ZSET,
        cold_zset_key: str = LIVE_COLD_ZSET,
        metrics_hash_key: str = LIVE_DISPATCH_METRICS_KEY,
    ) -> None:
        self.backend = backend
        self.hot_zset_key = hot_zset_key
        self.warm_zset_key = warm_zset_key
        self.cold_zset_key = cold_zset_key
        self._metrics_hash_key = metrics_hash_key
        self._metrics_started_at_recorded = False

    def upsert(self, state: LiveEventState, *, lane: str | None = None) -> None:
        _hset_mapping(
            self.backend,
            self._key(state.event_id),
            _compact_mapping(
                {
                "sport_slug": state.sport_slug,
                "status_type": state.status_type,
                "poll_profile": state.poll_profile,
                "last_seen_at": state.last_seen_at,
                "last_ingested_at": state.last_ingested_at,
                "last_changed_at": state.last_changed_at,
                "next_poll_at": state.next_poll_at,
                "hot_until": state.hot_until,
                "home_score": state.home_score,
                "away_score": state.away_score,
                "version_hint": state.version_hint,
                "is_finalized": int(state.is_finalized),
                "dispatch_tier": state.dispatch_tier,
                }
            ),
        )
        if lane and state.next_poll_at is not None:
            self.move_lane(state.event_id, lane=lane, next_poll_at=state.next_poll_at)

    def move_lane(self, event_id: int, *, lane: str, next_poll_at: int) -> None:
        """Atomically move ``event_id`` into ``lane`` at score ``next_poll_at``.

        #42: single Redis EVAL (ZREM hot/warm/cold + ZADD target), but the
        ZADD is REFUSED if the event hash is already finalized — preventing
        (a) a crash/interleave leaving the member in two lanes or zero lanes,
        and (b) re-livening a just-finalized event (the planner/track_event
        read-then-act TOCTOU). Falls back to discrete ops when the backend
        lacks EVAL (dev _MemoryRedisBackend + test fakes), preserving the
        previous best-effort behaviour. Score is passed as repr(float(...)) so
        a large epoch-ms value is never serialised in scientific notation that
        Lua ZADD would misparse.
        """
        member = str(event_id)
        target_key = self._lane_key(lane)
        eval_method = getattr(self.backend, "eval", None)
        if callable(eval_method):
            try:
                eval_method(
                    _MOVE_LANE_SCRIPT,
                    5,
                    self.hot_zset_key,
                    self.warm_zset_key,
                    self.cold_zset_key,
                    target_key,
                    self._key(event_id),
                    member,
                    repr(float(next_poll_at)),
                )
                return
            except Exception:  # pragma: no cover - defensive fallback for non-Redis backends
                pass
        self.backend.zrem(self.hot_zset_key, member)
        self.backend.zrem(self.warm_zset_key, member)
        self.backend.zrem(self.cold_zset_key, member)
        self.backend.zadd(target_key, {member: float(next_poll_at)})

    def remove_from_lanes(self, event_id: int) -> None:
        """Atomically remove ``event_id`` from all three polling lanes.

        #42: single Redis EVAL (ZREM hot/warm/cold) for the finalize path so a
        finalize can never strand the member in only some lanes. Falls back to
        three discrete ZREMs when the backend lacks EVAL.
        """
        member = str(event_id)
        eval_method = getattr(self.backend, "eval", None)
        if callable(eval_method):
            try:
                eval_method(
                    _REMOVE_FROM_LANES_SCRIPT,
                    3,
                    self.hot_zset_key,
                    self.warm_zset_key,
                    self.cold_zset_key,
                    member,
                )
                return
            except Exception:  # pragma: no cover - defensive fallback for non-Redis backends
                pass
        self.backend.zrem(self.hot_zset_key, member)
        self.backend.zrem(self.warm_zset_key, member)
        self.backend.zrem(self.cold_zset_key, member)

    def due_events(self, *, lane: str, now_ms: int) -> tuple[int, ...]:
        return tuple(int(value) for value in self.backend.zrangebyscore(self._lane_key(lane), float("-inf"), float(now_ms)))

    def fetch(self, event_id: int) -> LiveEventState | None:
        payload = self.backend.hgetall(self._key(event_id))
        if not payload:
            return None
        return LiveEventState(
            event_id=int(event_id),
            sport_slug=_as_text(payload.get("sport_slug")) or "unknown",
            status_type=_as_text(payload.get("status_type")),
            poll_profile=_as_text(payload.get("poll_profile")),
            last_seen_at=_as_int(payload.get("last_seen_at")),
            last_ingested_at=_as_int(payload.get("last_ingested_at")),
            last_changed_at=_as_int(payload.get("last_changed_at")),
            next_poll_at=_as_int(payload.get("next_poll_at")),
            hot_until=_as_int(payload.get("hot_until")),
            home_score=_as_int(payload.get("home_score")),
            away_score=_as_int(payload.get("away_score")),
            version_hint=_as_text(payload.get("version_hint")),
            is_finalized=_as_bool(payload.get("is_finalized")),
            dispatch_tier=_as_text(payload.get("dispatch_tier")),
        )

    def claim_dispatch(
        self,
        event_id: int,
        *,
        now_ms: int,
        lease_ms: int,
        tier: str | None = None,
    ) -> bool:
        self._record_metric("claim_attempts", tier)
        key = self._claim_key(event_id)
        ttl_ms = max(int(lease_ms), 1)
        setter = getattr(self.backend, "set", None)
        if callable(setter):
            try:
                ok = bool(setter(key, str(now_ms + ttl_ms), nx=True, px=ttl_ms))
            except TypeError:
                ok = bool(setter(key, str(now_ms + ttl_ms), nx=True))
            if ok:
                self._record_metric("claim_succeeded", tier)
            else:
                self._record_metric("claim_failed_blocked", tier)
            return ok

        claims = _fallback_claims(self.backend)
        existing_expiry = claims.get(key)
        if existing_expiry is not None:
            try:
                if int(existing_expiry) > int(now_ms):
                    self._record_metric("claim_failed_blocked", tier)
                    return False
            except (TypeError, ValueError):
                self._record_metric("claim_failed_blocked", tier)
                return False
        claims[key] = int(now_ms) + ttl_ms
        self._record_metric("claim_succeeded", tier)
        return True

    def clear_dispatch_claim(self, event_id: int, *, tier: str | None = None) -> None:
        self._record_metric("clear_called", tier)
        key = self._claim_key(event_id)
        delete = getattr(self.backend, "delete", None)
        if callable(delete):
            delete(key)
            return
        claims = _fallback_claims(self.backend)
        claims.pop(key, None)

    def record_published(self, tier: str | None) -> None:
        """Increment per-tier publish counter from the planner publish path."""
        self._record_metric("published", tier)

    def dispatch_metrics_snapshot(self) -> dict[str, int]:
        """Return the cumulative dispatch metrics hash as int values.

        Read-only path: only invoked by /ops/queues/summary collection.
        Returns an empty dict if the backend lacks HGETALL or the hash is
        empty (e.g., before any claim/publish/clear has happened on a
        freshly-deployed planner).
        """
        hgetall = getattr(self.backend, "hgetall", None)
        if not callable(hgetall):
            return {}
        try:
            raw = hgetall(self._metrics_hash_key) or {}
        except Exception:
            return {}
        result: dict[str, int] = {}
        for field, value in raw.items():
            field_str = (
                field.decode("utf-8", errors="ignore")
                if isinstance(field, bytes)
                else str(field)
            )
            text = _as_text(value)
            if text is None:
                continue
            try:
                result[field_str] = int(text)
            except (TypeError, ValueError):
                continue
        return result

    def tier_active_counts(self) -> dict[str, int]:
        """Tally currently-tracked events by dispatch_tier.

        WARNING: iterates all hot/warm/cold zset members and HGETs each
        event's dispatch_tier field. Caller is /ops/queues/summary only —
        do NOT invoke from the planner hot path. With ~500 active events
        this is ~500 HGETs against local Redis (~1-2 ms total).
        """
        counts: dict[str, int] = {tier: 0 for tier in _LIVE_DISPATCH_TIER_FIELDS}
        counts["unknown"] = 0
        for lane_key in (self.hot_zset_key, self.warm_zset_key, self.cold_zset_key):
            members = self.backend.zrangebyscore(lane_key, float("-inf"), float("inf"))
            for raw_member in members:
                try:
                    event_id = int(raw_member)
                except (TypeError, ValueError):
                    continue
                payload = self.backend.hgetall(self._key(event_id)) or {}
                tier = _as_text(payload.get("dispatch_tier"))
                normalized = (tier or "").strip().lower()
                if normalized in _LIVE_DISPATCH_TIER_FIELDS:
                    counts[normalized] += 1
                else:
                    counts["unknown"] += 1
        return counts

    def _record_metric(self, base: str, tier: str | None) -> None:
        """Increment HINCRBY counters for the given metric base + optional tier.

        Always increments `<base>:total`; if a recognised tier is provided
        also increments `<base>:<tier>`. Silently no-ops when the backend
        lacks HINCRBY (e.g., test fakes without metrics support) — metrics
        MUST NEVER fail the live path.
        """
        self._ensure_metrics_started_at()
        self._hincrby_safe(f"{base}:total", 1)
        if tier is None:
            return
        normalized = str(tier).strip().lower()
        if normalized in _LIVE_DISPATCH_TIER_FIELDS:
            self._hincrby_safe(f"{base}:{normalized}", 1)

    def _hincrby_safe(self, field: str, amount: int = 1) -> None:
        hincrby = getattr(self.backend, "hincrby", None)
        if not callable(hincrby):
            return
        try:
            hincrby(self._metrics_hash_key, field, amount)
        except Exception:
            return

    def _ensure_metrics_started_at(self) -> None:
        if self._metrics_started_at_recorded:
            return
        hsetnx = getattr(self.backend, "hsetnx", None)
        if callable(hsetnx):
            try:
                hsetnx(self._metrics_hash_key, "started_at_ms", str(int(time.time() * 1000)))
            except Exception:
                pass
        self._metrics_started_at_recorded = True

    def _lane_key(self, lane: str) -> str:
        normalized = lane.strip().lower()
        if normalized == "hot":
            return self.hot_zset_key
        if normalized == "warm":
            return self.warm_zset_key
        return self.cold_zset_key

    @staticmethod
    def _key(event_id: int) -> str:
        return f"live:event:{event_id}"

    @staticmethod
    def _claim_key(event_id: int) -> str:
        return f"{LIVE_DISPATCH_CLAIM_PREFIX}:{event_id}"


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
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _as_bool(value: object) -> bool:
    text = _as_text(value)
    if text is None:
        return False
    return text.strip().lower() in {"1", "true", "yes", "y"}


def _hset_mapping(backend: Any, name: str, mapping: dict[str, object]) -> None:
    try:
        backend.hset(name, mapping=mapping)
    except TypeError:
        backend.hset(name, mapping)


def _compact_mapping(mapping: dict[str, object | None]) -> dict[str, object]:
    return {str(key): value for key, value in mapping.items() if value is not None}


def _fallback_claims(backend: Any) -> dict[str, int]:
    claims = getattr(backend, "_live_dispatch_claims", None)
    if isinstance(claims, dict):
        return claims
    claims = {}
    setattr(backend, "_live_dispatch_claims", claims)
    return claims
