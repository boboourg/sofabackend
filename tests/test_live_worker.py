from __future__ import annotations

import unittest

from schema_inspector.queue.live_state import LIVE_WARM_ZSET, LiveEventStateStore
from schema_inspector.workers.live_worker import LiveWorker


class _FakeLiveBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.claims: dict[str, object] = {}

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(key, {}).update(mapping)
        return 1

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[str(member)] = float(score)
        return 1

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                removed += 1
                del bucket[member]
        return removed

    def set(
        self,
        key: str,
        value: object,
        *,
        nx: bool | None = None,
        px: int | None = None,
    ) -> bool:
        del px
        if nx and key in self.claims:
            return False
        self.claims[key] = value
        return True

    def delete(self, key: str) -> int:
        if key in self.claims:
            del self.claims[key]
            return 1
        return 0


class _FakeStreamQueue:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []

    def publish(self, stream: str, payload: dict[str, object]) -> None:
        self.published.append((stream, dict(payload)))


class LiveWorkerTests(unittest.TestCase):
    def test_track_event_updates_state_without_immediate_stream_publish(self) -> None:
        backend = _FakeLiveBackend()
        queue = _FakeStreamQueue()
        store = LiveEventStateStore(backend)
        worker = LiveWorker(now_ms_factory=lambda: 1_800_000_000_000)
        self.assertTrue(
            store.claim_dispatch(
                15400165,
                now_ms=1_800_000_000_000,
                lease_ms=90_000,
            )
        )

        result = worker.track_event(
            sport_slug="football",
            event_id=15400165,
            status_type="scheduled",
            minutes_to_start=20,
            trace_id="trace-1",
            live_state_store=store,
            stream_queue=queue,
        )

        self.assertEqual(result.decision.lane, "warm")
        self.assertIsNone(result.job)
        self.assertEqual(result.next_poll_at, 1_800_000_600_000)
        self.assertEqual(backend.hashes["live:event:15400165"]["poll_profile"], "warm")
        self.assertEqual(backend.zsets[LIVE_WARM_ZSET]["15400165"], float(1_800_000_600_000))
        self.assertEqual(queue.published, [])
        self.assertEqual(backend.claims, {})

    def test_track_event_assigns_tier_1_dispatch_for_top_live_football(self) -> None:
        store = LiveEventStateStore(_FakeLiveBackend())
        worker = LiveWorker(now_ms_factory=lambda: 1_800_000_000_000)

        result = worker.track_event(
            sport_slug="football",
            event_id=15235532,
            status_type="inprogress",
            minutes_to_start=0,
            trace_id="trace-2",
            detail_id=1,
            tournament_user_count=317795,
            live_state_store=store,
            stream_queue=_FakeStreamQueue(),
        )

        self.assertEqual(result.decision.lane, "hot")
        self.assertEqual(result.stream, "stream:etl:live_tier_1")
        self.assertEqual(result.next_poll_at, 1_800_000_005_000)
        self.assertEqual(store.fetch(15235532).dispatch_tier, "tier_1")

    def test_track_event_skips_upsert_when_existing_state_is_finalized(self) -> None:
        # F-8 Fix B: defense-in-depth complementing the SQL monotonic
        # guards. When upstream Sofascore returns a stale "inprogress"
        # payload after the match was legitimately finalized
        # (CDN flap or out-of-order delayed-insert snapshot), the
        # orchestrator's planner switch on parsed status_type will route
        # to JOB_TRACK_LIVE_EVENT → live_worker.track_event(). Without
        # this guard, track_event would upsert is_finalized=False back
        # to the live state store and re-add the event into hot/warm
        # zsets, burning tier_1 polling resources and producing UI
        # artifacts on an already-ended match.
        backend = _FakeLiveBackend()
        store = LiveEventStateStore(backend)
        worker = LiveWorker(now_ms_factory=lambda: 1_800_000_000_000)
        # Pre-populate the live state with a finalized event (as if
        # finalize_event had run earlier).
        worker.finalize_event(
            sport_slug="football",
            event_id=14083568,
            status_type="finished",
            live_state_store=store,
        )
        # Sanity: the event is recorded as finalized and not in any zset.
        finalized = store.fetch(14083568)
        self.assertIsNotNone(finalized)
        self.assertTrue(finalized.is_finalized)

        # A stale "inprogress" parse arrives and tries to re-track.
        result = worker.track_event(
            sport_slug="football",
            event_id=14083568,
            status_type="inprogress",
            minutes_to_start=0,
            trace_id="stale-inprogress",
            detail_id=1,
            tournament_user_count=317795,
            live_state_store=store,
            stream_queue=_FakeStreamQueue(),
        )

        # Track must short-circuit — no upsert, no zset re-add, no
        # claim/clear churn. The result conveys that no live state
        # transition was taken.
        self.assertIsNone(result.next_poll_at)
        self.assertIsNone(result.stream)
        self.assertIsNone(result.job)
        # State stays terminal — is_finalized=True, removed from zsets.
        still_finalized = store.fetch(14083568)
        self.assertTrue(still_finalized.is_finalized)
        self.assertEqual(still_finalized.poll_profile, "terminal")
        self.assertNotIn("14083568", backend.zsets.get("zset:live:hot", {}))
        self.assertNotIn("14083568", backend.zsets.get("zset:live:warm", {}))
        self.assertNotIn("14083568", backend.zsets.get("zset:live:cold", {}))

    def test_track_event_proceeds_when_existing_state_is_not_finalized(self) -> None:
        # Negative test for F-8 Fix B: the guard must NOT short-circuit
        # the normal mid-match status transition flow (e.g., halftime →
        # 2nd half). Existing state is_finalized=False → track_event
        # proceeds with the upsert and zset placement.
        backend = _FakeLiveBackend()
        store = LiveEventStateStore(backend)
        worker = LiveWorker(now_ms_factory=lambda: 1_800_000_000_000)
        # Pre-populate the live state with a NOT finalized event (as if
        # an earlier track_event for "1st half" had run).
        from schema_inspector.queue.live_state import LiveEventState

        store.upsert(
            LiveEventState(
                event_id=14083568,
                sport_slug="football",
                status_type="inprogress",
                poll_profile="hot",
                last_seen_at=1_799_999_000_000,
                last_ingested_at=1_799_999_000_000,
                last_changed_at=1_799_999_000_000,
                next_poll_at=1_800_000_000_000,
                hot_until=1_800_000_000_000,
                home_score=1,
                away_score=0,
                version_hint=None,
                is_finalized=False,
                dispatch_tier="tier_1",
            ),
            lane="hot",
        )

        result = worker.track_event(
            sport_slug="football",
            event_id=14083568,
            status_type="inprogress",
            minutes_to_start=0,
            trace_id="next-tick",
            detail_id=1,
            tournament_user_count=317795,
            live_state_store=store,
            stream_queue=_FakeStreamQueue(),
        )

        # Normal transition: upsert ran, next_poll_at is set, stream
        # routes to tier_1 hot.
        self.assertEqual(result.decision.lane, "hot")
        self.assertEqual(result.stream, "stream:etl:live_tier_1")
        self.assertEqual(result.next_poll_at, 1_800_000_005_000)
        # State stays not-finalized.
        updated = store.fetch(14083568)
        self.assertFalse(updated.is_finalized)

    def test_track_event_proceeds_when_no_prior_state_exists(self) -> None:
        # Negative test for F-8 Fix B: a brand-new live event has no
        # row in the live state store yet — the guard must fall through
        # so the first track_event call records the initial state.
        backend = _FakeLiveBackend()
        store = LiveEventStateStore(backend)
        worker = LiveWorker(now_ms_factory=lambda: 1_800_000_000_000)
        # No pre-existing state.
        self.assertIsNone(store.fetch(14083568))

        result = worker.track_event(
            sport_slug="football",
            event_id=14083568,
            status_type="inprogress",
            minutes_to_start=0,
            trace_id="first-track",
            detail_id=1,
            tournament_user_count=317795,
            live_state_store=store,
            stream_queue=_FakeStreamQueue(),
        )

        self.assertEqual(result.decision.lane, "hot")
        self.assertEqual(result.stream, "stream:etl:live_tier_1")
        new_state = store.fetch(14083568)
        self.assertIsNotNone(new_state)
        self.assertFalse(new_state.is_finalized)
        self.assertEqual(new_state.dispatch_tier, "tier_1")

    def test_track_event_clears_dispatch_claim_with_tier(self) -> None:
        # F-7 Phase 0: clear_dispatch_claim must receive the resolved
        # dispatch_tier so the per-tier clear counter reflects which
        # stream actually drained a job (asymmetry between claim/clear
        # rates per tier is the diagnostic signal).
        store = _ClearRecordingStore()
        worker = LiveWorker(now_ms_factory=lambda: 1_800_000_000_000)

        worker.track_event(
            sport_slug="football",
            event_id=15235532,
            status_type="inprogress",
            minutes_to_start=0,
            trace_id="trace-clear",
            detail_id=1,
            tournament_user_count=317795,
            live_state_store=store,
            stream_queue=_FakeStreamQueue(),
        )

        self.assertEqual(store.clear_calls, [(15235532, "tier_1")])


class LiveWorkerRescheduleAfterTransientFailureTests(unittest.TestCase):
    """2026-05-29 live audit: a retryable root-/event fetch failure raises
    before track_event, so next_poll_at (the zset score) never advances and
    the event freezes overdue in its lane — the planner re-dispatches it every
    tick and oldest_hot_score_age breaches the SLO. reschedule_after_transient_
    failure pushes the score forward to a backed-off retry instead."""

    _NOW = 1_800_000_000_000

    def _store_with_live_event(
        self,
        *,
        event_id: int,
        dispatch_tier: str,
        poll_profile: str = "hot",
        next_poll_at: int | None = None,
    ) -> tuple[LiveEventStateStore, _FakeLiveBackend]:
        from schema_inspector.queue.live_state import LiveEventState

        backend = _FakeLiveBackend()
        store = LiveEventStateStore(backend)
        # next_poll_at deliberately in the PAST (overdue) to mimic a frozen
        # event whose last refresh attempt failed.
        overdue = next_poll_at if next_poll_at is not None else self._NOW - 1_000_000
        store.upsert(
            LiveEventState(
                event_id=event_id,
                sport_slug="football",
                status_type="inprogress",
                poll_profile=poll_profile,
                last_seen_at=self._NOW - 1_000_000,
                last_ingested_at=self._NOW - 1_000_000,
                last_changed_at=self._NOW - 1_000_000,
                next_poll_at=overdue,
                hot_until=overdue,
                home_score=1,
                away_score=0,
                version_hint=None,
                is_finalized=False,
                dispatch_tier=dispatch_tier,
            ),
            lane=poll_profile,
        )
        return store, backend

    def test_reschedules_overdue_event_to_tier_cadence(self) -> None:
        store, backend = self._store_with_live_event(
            event_id=14722478, dispatch_tier="tier_3"
        )
        worker = LiveWorker(now_ms_factory=lambda: self._NOW)

        new_next_poll = worker.reschedule_after_transient_failure(
            sport_slug="football",
            event_id=14722478,
            live_state_store=store,
        )

        # tier_3 poll cadence is 90s; floored min (15s) does not apply.
        self.assertEqual(new_next_poll, self._NOW + 90_000)
        # The zset score moved from overdue (now - 1_000_000) to a future
        # value, so the freshness metric (now - min score) no longer breaches.
        self.assertEqual(
            backend.zsets["zset:live:hot"]["14722478"], float(self._NOW + 90_000)
        )

    def test_floors_fast_tier_to_min_backoff(self) -> None:
        # tier_1 normal cadence is 5s — too aggressive as a failure retry.
        # The min_backoff floor (15s) protects against a refresh storm.
        store, backend = self._store_with_live_event(
            event_id=15235532, dispatch_tier="tier_1"
        )
        worker = LiveWorker(now_ms_factory=lambda: self._NOW)

        new_next_poll = worker.reschedule_after_transient_failure(
            sport_slug="football",
            event_id=15235532,
            live_state_store=store,
        )

        self.assertEqual(new_next_poll, self._NOW + 15_000)
        self.assertEqual(
            backend.zsets["zset:live:hot"]["15235532"], float(self._NOW + 15_000)
        )

    def test_clears_dispatch_claim_so_planner_can_redispatch(self) -> None:
        store, backend = self._store_with_live_event(
            event_id=14722478, dispatch_tier="tier_3"
        )
        # Simulate the planner having claimed the dispatch lease.
        self.assertTrue(
            store.claim_dispatch(14722478, now_ms=self._NOW, lease_ms=90_000)
        )
        self.assertIn("live:dispatch_claim:14722478", backend.claims)
        worker = LiveWorker(now_ms_factory=lambda: self._NOW)

        worker.reschedule_after_transient_failure(
            sport_slug="football",
            event_id=14722478,
            live_state_store=store,
        )

        # Claim dropped — otherwise the ~90s lease would block the retry even
        # after next_poll_at fell due.
        self.assertNotIn("live:dispatch_claim:14722478", backend.claims)

    def test_does_not_touch_data_freshness_fields(self) -> None:
        # No data was fetched on a failed refresh, so the per-event hash must
        # keep telling the truth that the payload is stale. Only the schedule
        # (zset score) moves; last_changed_at / last_ingested_at stay put.
        store, backend = self._store_with_live_event(
            event_id=14722478, dispatch_tier="tier_3"
        )
        original_last_changed = backend.hashes["live:event:14722478"]["last_changed_at"]
        worker = LiveWorker(now_ms_factory=lambda: self._NOW)

        worker.reschedule_after_transient_failure(
            sport_slug="football",
            event_id=14722478,
            live_state_store=store,
        )

        self.assertEqual(
            backend.hashes["live:event:14722478"]["last_changed_at"],
            original_last_changed,
        )

    def test_no_op_for_finalized_event(self) -> None:
        # A finished match must never be re-livened back into a polling lane.
        backend = _FakeLiveBackend()
        store = LiveEventStateStore(backend)
        worker = LiveWorker(now_ms_factory=lambda: self._NOW)
        worker.finalize_event(
            sport_slug="football",
            event_id=14083568,
            status_type="finished",
            live_state_store=store,
        )

        result = worker.reschedule_after_transient_failure(
            sport_slug="football",
            event_id=14083568,
            live_state_store=store,
        )

        self.assertIsNone(result)
        self.assertNotIn("14083568", backend.zsets.get("zset:live:hot", {}))
        self.assertNotIn("14083568", backend.zsets.get("zset:live:warm", {}))
        self.assertNotIn("14083568", backend.zsets.get("zset:live:cold", {}))

    def test_no_op_when_no_prior_state(self) -> None:
        backend = _FakeLiveBackend()
        store = LiveEventStateStore(backend)
        worker = LiveWorker(now_ms_factory=lambda: self._NOW)

        result = worker.reschedule_after_transient_failure(
            sport_slug="football",
            event_id=99999999,
            live_state_store=store,
        )

        self.assertIsNone(result)
        self.assertEqual(backend.zsets.get("zset:live:hot", {}), {})

    def test_no_op_when_live_state_store_is_none(self) -> None:
        worker = LiveWorker(now_ms_factory=lambda: self._NOW)
        self.assertIsNone(
            worker.reschedule_after_transient_failure(
                sport_slug="football",
                event_id=14722478,
                live_state_store=None,
            )
        )


class _ClearRecordingStore:
    """Live state store fake that records every clear_dispatch_claim call
    so we can assert the tier kwarg threading."""

    def __init__(self) -> None:
        self.upserts: list[object] = []
        self.clear_calls: list[tuple[int, str | None]] = []
        self.backend = _FakeLiveBackend()
        self.hot_zset_key = "zset:live:hot"
        self.warm_zset_key = "zset:live:warm"
        self.cold_zset_key = "zset:live:cold"

    def upsert(self, state, *, lane: str | None = None) -> None:
        del lane
        self.upserts.append(state)

    def clear_dispatch_claim(self, event_id: int, *, tier: str | None = None) -> None:
        self.clear_calls.append((int(event_id), tier))


if __name__ == "__main__":
    unittest.main()
