from __future__ import annotations

import unittest


class RetryPolicyTests(unittest.TestCase):
    def test_retry_policy_marks_lock_contention_errors_as_retryable(self) -> None:
        from schema_inspector.services.retry_policy import is_retryable_db_error

        lock_timeout = RuntimeError("canceling statement due to lock timeout")
        deadlock = RuntimeError("deadlock detected")
        lock_not_available = type("LockNotAvailableError", (Exception,), {})()

        self.assertTrue(is_retryable_db_error(lock_timeout))
        self.assertTrue(is_retryable_db_error(deadlock))
        self.assertTrue(is_retryable_db_error(lock_not_available))

    def test_retry_policy_marks_timeout_error_as_retryable(self) -> None:
        from schema_inspector.services.retry_policy import is_retryable_db_error

        self.assertTrue(is_retryable_db_error(TimeoutError()))

    def test_retry_policy_marks_admission_deferred_error_as_retryable(self) -> None:
        from schema_inspector.services.retry_policy import (
            AdmissionDeferredError,
            is_retryable_db_error,
            retry_audit_status,
            retry_delay_ms,
        )

        exc = AdmissionDeferredError("hydrate backlog", delay_ms=30_000)

        self.assertTrue(is_retryable_db_error(exc))
        self.assertEqual(retry_audit_status(exc), "deferred_backpressure")
        self.assertEqual(retry_delay_ms(attempt=1, exc=exc), 30_000)

    def test_retry_policy_ignores_non_lock_errors(self) -> None:
        from schema_inspector.services.retry_policy import is_retryable_db_error

        self.assertFalse(is_retryable_db_error(RuntimeError("duplicate key value violates unique constraint")))

    def test_retry_policy_uses_capped_exponential_backoff(self) -> None:
        """Deterministic backoff (jitter=False) preserves exact schedule
        — needed for tests that assert specific delays."""
        from schema_inspector.services.retry_policy import retry_delay_ms

        self.assertEqual(retry_delay_ms(attempt=1, jitter=False), 5_000)
        self.assertEqual(retry_delay_ms(attempt=2, jitter=False), 10_000)
        self.assertEqual(retry_delay_ms(attempt=3, jitter=False), 20_000)
        self.assertEqual(retry_delay_ms(attempt=5, jitter=False), 60_000)


# ---------------------------------------------------------------------------
# Stage 1.3 (2026-05-20 stability re-audit): jitter in retry backoff.
#
# Without jitter, 9 hydrate workers that hit the same deadlock / lock_timeout
# (typical on saturday-evening live load) all wake up at exactly t+5s, then
# at t+10s, then t+20s — synchronous retry causes thundering herd. The fix
# adds ±20% multiplicative jitter to the computed exponential delay.
# ---------------------------------------------------------------------------


class RetryDelayJitterTests(unittest.TestCase):
    def test_default_call_adds_jitter_within_20_percent(self) -> None:
        from schema_inspector.services.retry_policy import retry_delay_ms

        samples = [retry_delay_ms(attempt=1) for _ in range(100)]
        # Base = 5000ms, ±20% → expected range [4000, 6000]
        self.assertTrue(
            all(4000 <= s <= 6000 for s in samples),
            msg=f"All samples must lie in [4000, 6000] ms; got min={min(samples)}, max={max(samples)}",
        )
        # And the sample set must NOT be a single constant — confirms
        # jitter is actually applied, not silently dropped.
        self.assertGreater(
            len(set(samples)), 1,
            msg="Default call must produce a non-constant distribution",
        )

    def test_jitter_off_returns_exact_value(self) -> None:
        from schema_inspector.services.retry_policy import retry_delay_ms

        self.assertEqual(retry_delay_ms(attempt=2, jitter=False), 10_000)
        self.assertEqual(retry_delay_ms(attempt=3, jitter=False), 20_000)

    def test_jitter_clamped_to_cap(self) -> None:
        """At the cap (attempt=5+, base 5_000, cap 60_000) jitter should
        not push values above cap — clamp at cap_ms after jitter."""
        from schema_inspector.services.retry_policy import retry_delay_ms

        # cap_ms = 60_000; with +20% jitter we'd compute 72_000, but
        # the function must clamp.
        for _ in range(50):
            value = retry_delay_ms(attempt=10)
            self.assertLessEqual(value, 60_000)
            # And not absurdly below cap either (lower bound 48_000 from -20%)
            self.assertGreaterEqual(value, 48_000)

    def test_custom_delay_ms_bypasses_jitter(self) -> None:
        """When the exception carries an explicit ``delay_ms`` (e.g.
        AdmissionDeferred), jitter must NOT be applied — the caller is
        signalling a specific delay (typical for rate-limit hints)."""
        from schema_inspector.services.retry_policy import (
            AdmissionDeferredError,
            retry_delay_ms,
        )

        exc = AdmissionDeferredError("backpressure", delay_ms=30_000)
        for _ in range(20):
            self.assertEqual(retry_delay_ms(attempt=1, exc=exc), 30_000)


# ---------------------------------------------------------------------------
# Stage 1.5 (2026-05-20 stability re-audit, Constraint #2b): redis
# ConnectionError must be classified retryable. Without this, a transient
# Redis hiccup (OOM-restart, sub-second network glitch) kills all 9 hydrate
# workers via fatal exception → systemd restart → simultaneous reconnect
# burst on Redis → cascading failure.
# ---------------------------------------------------------------------------


class RedisErrorRetryClassificationTests(unittest.TestCase):
    def test_redis_connection_error_is_retryable(self) -> None:
        import redis
        from schema_inspector.services.retry_policy import is_retryable_worker_error

        exc = redis.exceptions.ConnectionError("Connection closed by server")
        self.assertTrue(
            is_retryable_worker_error(exc),
            msg=(
                "redis.exceptions.ConnectionError must be retryable so a "
                "Redis hiccup goes into delayed scheduler rather than "
                "killing the worker process."
            ),
        )

    def test_redis_timeout_error_is_retryable(self) -> None:
        import redis
        from schema_inspector.services.retry_policy import is_retryable_worker_error

        exc = redis.exceptions.TimeoutError("Read timed out after 15s")
        self.assertTrue(
            is_retryable_worker_error(exc),
            msg="redis.exceptions.TimeoutError must be retryable",
        )

    def test_generic_redis_error_is_retryable(self) -> None:
        """Any RedisError subclass (BusyLoadingError, ReadOnlyError, …)
        is a transient cluster condition — must retry, never crash."""
        import redis
        from schema_inspector.services.retry_policy import is_retryable_worker_error

        exc = redis.exceptions.BusyLoadingError("Redis is loading the dataset in memory")
        self.assertTrue(
            is_retryable_worker_error(exc),
            msg="redis.exceptions.RedisError subclasses must be retryable",
        )


class RedisFromUrlConnectionSafetyTests(unittest.TestCase):
    """Stage 1.5 (2026-05-20 stability re-audit, Constraint #2b): the
    redis.Redis.from_url call in cli.py must pass socket_timeout +
    retry_on_timeout. Default redis-py timeout is None → XREADGROUP
    can hang past the BLOCK ms even when the socket is dead, because
    BLOCK only governs Redis-side waits, not client-side socket
    behaviour."""

    def test_redis_from_url_uses_socket_timeout(self) -> None:
        import re
        from pathlib import Path

        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "cli.py"
        ).read_text(encoding="utf-8")
        # Find every redis.Redis.from_url call and confirm the kwargs
        # include socket_timeout. We use a broad regex that tolerates
        # multiline kwarg formatting.
        matches = list(
            re.finditer(
                r"redis\.Redis\.from_url\([^)]*?\)",
                text,
                flags=re.DOTALL,
            )
        )
        self.assertTrue(matches, msg="redis.Redis.from_url call not found in cli.py")
        for match in matches:
            block = match.group(0)
            self.assertIn(
                "socket_timeout",
                block,
                msg=(
                    "redis.Redis.from_url must include socket_timeout — "
                    "default is None which lets XREADGROUP hang on a dead "
                    "socket past the BLOCK budget. "
                    f"Offending block: {block!r}"
                ),
            )

    def test_redis_from_url_uses_retry_on_timeout(self) -> None:
        import re
        from pathlib import Path

        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "cli.py"
        ).read_text(encoding="utf-8")
        matches = list(
            re.finditer(
                r"redis\.Redis\.from_url\([^)]*?\)",
                text,
                flags=re.DOTALL,
            )
        )
        self.assertTrue(matches)
        for match in matches:
            block = match.group(0)
            self.assertIn(
                "retry_on_timeout",
                block,
                msg=(
                    "redis.Redis.from_url must include retry_on_timeout=True "
                    "so the client retries a single packet timeout before "
                    "raising — small network glitches no longer trigger a "
                    "9-worker restart cascade."
                ),
            )


if __name__ == "__main__":
    unittest.main()
