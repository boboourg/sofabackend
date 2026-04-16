"""Classify transport responses into planner-friendly fetch outcomes."""

from __future__ import annotations

import json
from typing import Any, Mapping

from .fetch_models import ClassifiedFetchResult
from .runtime import TransportResult

CLASSIFICATION_SUCCESS_JSON = "success_json"
CLASSIFICATION_SUCCESS_EMPTY_JSON = "success_empty_json"
CLASSIFICATION_SOFT_ERROR_JSON = "soft_error_json"
CLASSIFICATION_NOT_FOUND = "not_found"
CLASSIFICATION_ACCESS_DENIED = "access_denied"
CLASSIFICATION_RATE_LIMITED = "rate_limited"
CLASSIFICATION_CHALLENGE_DETECTED = "challenge_detected"
CLASSIFICATION_NETWORK_ERROR = "network_error"
CLASSIFICATION_DECODE_ERROR = "decode_error"
CLASSIFICATION_UNEXPECTED_CONTENT = "unexpected_content"


def classify_fetch_result(transport_result: TransportResult) -> ClassifiedFetchResult:
    content_type = _content_type(transport_result.headers)

    if transport_result.challenge_reason == "rate_limited" or transport_result.status_code == 429:
        return ClassifiedFetchResult(
            classification=CLASSIFICATION_RATE_LIMITED,
            payload=None,
            is_valid_json=False,
            is_empty_payload=False,
            is_soft_error_payload=False,
            payload_root_keys=(),
            retry_recommended=True,
            capability_signal="guarded",
            content_type=content_type,
        )

    if transport_result.challenge_reason == "bot_challenge":
        return ClassifiedFetchResult(
            classification=CLASSIFICATION_CHALLENGE_DETECTED,
            payload=None,
            is_valid_json=False,
            is_empty_payload=False,
            is_soft_error_payload=False,
            payload_root_keys=(),
            retry_recommended=True,
            capability_signal="guarded",
            content_type=content_type,
        )

    if transport_result.challenge_reason == "access_denied" or transport_result.status_code == 403:
        return ClassifiedFetchResult(
            classification=CLASSIFICATION_ACCESS_DENIED,
            payload=None,
            is_valid_json=False,
            is_empty_payload=False,
            is_soft_error_payload=False,
            payload_root_keys=(),
            retry_recommended=True,
            capability_signal="guarded",
            content_type=content_type,
        )

    payload, is_valid_json = _decode_json(transport_result.body_bytes)
    root_keys = _root_keys(payload)
    is_empty_payload = _is_empty_payload(payload)
    is_soft_error_payload = _is_soft_error_payload(payload)

    if transport_result.status_code == 404:
        return ClassifiedFetchResult(
            classification=CLASSIFICATION_NOT_FOUND,
            payload=payload,
            is_valid_json=is_valid_json,
            is_empty_payload=is_empty_payload,
            is_soft_error_payload=is_soft_error_payload,
            payload_root_keys=root_keys,
            retry_recommended=False,
            capability_signal="unsupported",
            content_type=content_type,
        )

    if not is_valid_json:
        return ClassifiedFetchResult(
            classification=CLASSIFICATION_DECODE_ERROR,
            payload=None,
            is_valid_json=False,
            is_empty_payload=False,
            is_soft_error_payload=False,
            payload_root_keys=(),
            retry_recommended=False,
            capability_signal="unexpected",
            content_type=content_type,
        )

    if is_soft_error_payload:
        return ClassifiedFetchResult(
            classification=CLASSIFICATION_SOFT_ERROR_JSON,
            payload=payload,
            is_valid_json=True,
            is_empty_payload=is_empty_payload,
            is_soft_error_payload=True,
            payload_root_keys=root_keys,
            retry_recommended=False,
            capability_signal="soft_error",
            content_type=content_type,
        )

    if is_empty_payload:
        return ClassifiedFetchResult(
            classification=CLASSIFICATION_SUCCESS_EMPTY_JSON,
            payload=payload,
            is_valid_json=True,
            is_empty_payload=True,
            is_soft_error_payload=False,
            payload_root_keys=root_keys,
            retry_recommended=False,
            capability_signal="supported",
            content_type=content_type,
        )

    return ClassifiedFetchResult(
        classification=CLASSIFICATION_SUCCESS_JSON,
        payload=payload,
        is_valid_json=True,
        is_empty_payload=False,
        is_soft_error_payload=False,
        payload_root_keys=root_keys,
        retry_recommended=False,
        capability_signal="supported",
        content_type=content_type,
    )


def _decode_json(body_bytes: bytes) -> tuple[object | None, bool]:
    if not body_bytes:
        return None, True
    try:
        return json.loads(body_bytes.decode("utf-8")), True
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, False


def _content_type(headers: Mapping[str, str]) -> str | None:
    for key, value in headers.items():
        if key.lower() == "content-type":
            return str(value)
    return None


def _root_keys(payload: object | None) -> tuple[str, ...]:
    if isinstance(payload, dict):
        return tuple(str(key) for key in payload.keys())
    return ()


def _is_empty_payload(payload: object | None) -> bool:
    if payload is None:
        return True
    if isinstance(payload, (list, tuple, set, dict, str)):
        return len(payload) == 0
    return False


def _is_soft_error_payload(payload: object | None) -> bool:
    if not isinstance(payload, dict):
        return False
    if "error" in payload or "errors" in payload:
        return True
    return False
