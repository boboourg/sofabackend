from __future__ import annotations

import unittest


class FreshnessPolicyTests(unittest.TestCase):
    def test_claim_event_hydration_dedupes_by_event_and_mode(self) -> None:
        from schema_inspector.queue.dedupe import DedupeStore
        from schema_inspector.services.freshness_policy import FreshnessPolicy

        store = DedupeStore(_FakeRedisBackend())
        policy = FreshnessPolicy(
            store=store,
            core_ttl_ms=1_000,
            full_ttl_ms=2_000,
        )

        self.assertTrue(
            policy.claim_event_hydration(
                event_id=501,
                hydration_mode="core",
                force_rehydrate=False,
                now_ms=1_000,
            )
        )
        self.assertFalse(
            policy.claim_event_hydration(
                event_id=501,
                hydration_mode="core",
                force_rehydrate=False,
                now_ms=1_500,
            )
        )
        self.assertTrue(
            policy.claim_event_hydration(
                event_id=501,
                hydration_mode="full",
                force_rehydrate=False,
                now_ms=1_500,
            )
        )
        self.assertTrue(
            policy.claim_event_hydration(
                event_id=501,
                hydration_mode="core",
                force_rehydrate=False,
                now_ms=2_100,
            )
        )

    def test_force_rehydrate_bypasses_freshness_window(self) -> None:
        from schema_inspector.queue.dedupe import DedupeStore
        from schema_inspector.services.freshness_policy import FreshnessPolicy

        store = DedupeStore(_FakeRedisBackend())
        policy = FreshnessPolicy(store=store, core_ttl_ms=10_000, full_ttl_ms=10_000)

        self.assertTrue(
            policy.claim_event_hydration(
                event_id=777,
                hydration_mode="full",
                force_rehydrate=False,
                now_ms=5_000,
            )
        )
        self.assertTrue(
            policy.claim_event_hydration(
                event_id=777,
                hydration_mode="full",
                force_rehydrate=True,
                now_ms=5_100,
            )
        )


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.values: dict[str, dict[str, int | str | None]] = {}

    def set(self, key: str, value: str, *, nx: bool = False, px: int | None = None, now_ms: int | None = None) -> bool:
        observed_at = int(now_ms or 0)
        self._expire(key, now_ms=observed_at)
        if nx and key in self.values:
            return False
        self.values[key] = {
            "value": str(value),
            "expires_at": None if px is None else observed_at + int(px),
        }
        return True

    def exists(self, key: str, *, now_ms: int | None = None) -> bool:
        observed_at = int(now_ms or 0)
        self._expire(key, now_ms=observed_at)
        return key in self.values

    def _expire(self, key: str, *, now_ms: int) -> None:
        current = self.values.get(key)
        if current is None:
            return
        expires_at = current.get("expires_at")
        if expires_at is not None and int(expires_at) <= int(now_ms):
            del self.values[key]


if __name__ == "__main__":
    unittest.main()
