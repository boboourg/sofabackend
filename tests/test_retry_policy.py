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
        from schema_inspector.services.retry_policy import AdmissionDeferredError, is_retryable_db_error, retry_delay_ms

        exc = AdmissionDeferredError("hydrate backlog", delay_ms=30_000)

        self.assertTrue(is_retryable_db_error(exc))
        self.assertEqual(retry_delay_ms(attempt=1, exc=exc), 30_000)

    def test_retry_policy_ignores_non_lock_errors(self) -> None:
        from schema_inspector.services.retry_policy import is_retryable_db_error

        self.assertFalse(is_retryable_db_error(RuntimeError("duplicate key value violates unique constraint")))

    def test_retry_policy_uses_capped_exponential_backoff(self) -> None:
        from schema_inspector.services.retry_policy import retry_delay_ms

        self.assertEqual(retry_delay_ms(attempt=1), 5_000)
        self.assertEqual(retry_delay_ms(attempt=2), 10_000)
        self.assertEqual(retry_delay_ms(attempt=3), 20_000)
        self.assertEqual(retry_delay_ms(attempt=5), 60_000)


if __name__ == "__main__":
    unittest.main()
