"""Replayable parser registry for raw snapshot normalization."""

from .base import (
    PARSE_STATUS_FAILED,
    PARSE_STATUS_PARSED,
    PARSE_STATUS_PARSED_EMPTY,
    PARSE_STATUS_PARTIALLY_PARSED,
    PARSE_STATUS_SOFT_ERROR,
    PARSE_STATUS_UNSUPPORTED,
    ParseResult,
    RawSnapshot,
)
from .registry import ParserRegistry

__all__ = [
    "PARSE_STATUS_FAILED",
    "PARSE_STATUS_PARSED",
    "PARSE_STATUS_PARSED_EMPTY",
    "PARSE_STATUS_PARTIALLY_PARSED",
    "PARSE_STATUS_SOFT_ERROR",
    "PARSE_STATUS_UNSUPPORTED",
    "ParseResult",
    "RawSnapshot",
    "ParserRegistry",
]
