from __future__ import annotations

import unittest

from schema_inspector.fetch_classifier import (
    CLASSIFICATION_ACCESS_DENIED,
    CLASSIFICATION_CHALLENGE_DETECTED,
    CLASSIFICATION_NOT_FOUND,
    CLASSIFICATION_SOFT_ERROR_JSON,
    CLASSIFICATION_SUCCESS_JSON,
    classify_fetch_result,
)
from schema_inspector.runtime import TransportAttempt, TransportResult


class FetchClassifierTests(unittest.TestCase):
    def test_classifies_success_json_payload(self) -> None:
        result = classify_fetch_result(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/1",
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"event":{"id":1,"slug":"match"}}',
                attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
                final_proxy_name="proxy_1",
                challenge_reason=None,
            )
        )

        self.assertEqual(result.classification, CLASSIFICATION_SUCCESS_JSON)
        self.assertTrue(result.is_valid_json)
        self.assertFalse(result.is_empty_payload)
        self.assertFalse(result.is_soft_error_payload)
        self.assertEqual(result.payload_root_keys, ("event",))
        self.assertFalse(result.retry_recommended)
        self.assertEqual(result.capability_signal, "supported")

    def test_classifies_soft_error_json_payload(self) -> None:
        result = classify_fetch_result(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/1/comments",
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"error":{"code":404,"message":"Not found"}}',
                attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
                final_proxy_name="proxy_1",
                challenge_reason=None,
            )
        )

        self.assertEqual(result.classification, CLASSIFICATION_SOFT_ERROR_JSON)
        self.assertTrue(result.is_valid_json)
        self.assertTrue(result.is_soft_error_payload)
        self.assertEqual(result.payload_root_keys, ("error",))
        self.assertEqual(result.capability_signal, "soft_error")

    def test_classifies_not_found_as_unsupported_signal(self) -> None:
        result = classify_fetch_result(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/1/weather",
                status_code=404,
                headers={"Content-Type": "application/json"},
                body_bytes=b'{"error":"missing"}',
                attempts=(TransportAttempt(1, "proxy_1", 404, None, None),),
                final_proxy_name="proxy_1",
                challenge_reason=None,
            )
        )

        self.assertEqual(result.classification, CLASSIFICATION_NOT_FOUND)
        self.assertFalse(result.retry_recommended)
        self.assertEqual(result.capability_signal, "unsupported")

    def test_classifies_guarded_responses(self) -> None:
        denied = classify_fetch_result(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/1",
                status_code=403,
                headers={"Content-Type": "text/html"},
                body_bytes=b"<html>access denied</html>",
                attempts=(TransportAttempt(1, "proxy_1", 403, None, "access_denied"),),
                final_proxy_name="proxy_1",
                challenge_reason="access_denied",
            )
        )
        challenged = classify_fetch_result(
            TransportResult(
                resolved_url="https://www.sofascore.com/api/v1/event/1",
                status_code=403,
                headers={"Content-Type": "text/html"},
                body_bytes=b"<html>captcha</html>",
                attempts=(TransportAttempt(1, "proxy_1", 403, None, "bot_challenge"),),
                final_proxy_name="proxy_1",
                challenge_reason="bot_challenge",
            )
        )

        self.assertEqual(denied.classification, CLASSIFICATION_ACCESS_DENIED)
        self.assertEqual(challenged.classification, CLASSIFICATION_CHALLENGE_DETECTED)
        self.assertTrue(denied.retry_recommended)
        self.assertTrue(challenged.retry_recommended)


if __name__ == "__main__":
    unittest.main()
