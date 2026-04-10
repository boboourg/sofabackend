"""Detect challenge-like upstream responses."""

from __future__ import annotations

from typing import Mapping


def detect_challenge(status_code: int, headers: Mapping[str, str], body: bytes, markers: tuple[str, ...]) -> str | None:
    """Return a challenge reason when the response looks guarded."""

    del headers
    lowered = body.decode("utf-8", errors="ignore").lower()
    if status_code == 429:
        return "rate_limited"
    if status_code in {401, 403}:
        for marker in markers:
            if marker in lowered:
                return "bot_challenge"
        return "access_denied"
    for marker in markers:
        if marker in lowered:
            return "bot_challenge"
    return None
