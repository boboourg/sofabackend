"""Normalize worker backed by the new parser registry."""

from __future__ import annotations

from typing import Callable

from ..parsers.base import ParseResult, RawSnapshot


class NormalizeWorker:
    def __init__(self, parser_registry, *, result_sink: Callable[[ParseResult], object] | None = None) -> None:
        self.parser_registry = parser_registry
        self.result_sink = result_sink

    def handle(self, snapshot: RawSnapshot) -> ParseResult:
        result = self.parser_registry.parse(snapshot)
        if self.result_sink is not None:
            self.result_sink(result)
        return result
