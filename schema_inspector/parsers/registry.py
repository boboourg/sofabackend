"""Parser registry for replayable raw-snapshot normalization."""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import PARSE_STATUS_SOFT_ERROR, PARSE_STATUS_UNSUPPORTED, ParseResult, RawSnapshot, SnapshotParser
from .classifier import classify_snapshot, is_soft_error_payload
from .families.entity_profiles import EntityProfilesParser
from .families.event_incidents import EventIncidentsParser
from .families.event_lineups import EventLineupsParser
from .families.event_root import EventRootParser
from .families.event_statistics import EventStatisticsParser
from .special.tennis_point_by_point import TennisPointByPointParser
from .special.tennis_power import TennisPowerParser


@dataclass
class ParserRegistry:
    parsers: dict[str, SnapshotParser] = field(default_factory=dict)

    @classmethod
    def default(cls) -> "ParserRegistry":
        registry = cls()
        for parser in (
            EventRootParser(),
            EventStatisticsParser(),
            EventLineupsParser(),
            EventIncidentsParser(),
            EntityProfilesParser(),
            TennisPointByPointParser(),
            TennisPowerParser(),
        ):
            registry.register(parser)
        return registry

    def register(self, parser: SnapshotParser) -> None:
        self.parsers[parser.parser_family] = parser

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        if is_soft_error_payload(snapshot.payload):
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family="soft_error",
                parser_version="v1",
                status=PARSE_STATUS_SOFT_ERROR,
                warnings=("Sofascore-style error payload observed.",),
            )

        family = classify_snapshot(snapshot)
        parser = self.parsers.get(family)
        if parser is None:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=family,
                parser_version="v1",
                status=PARSE_STATUS_UNSUPPORTED,
                warnings=(f"No registered parser for family '{family}'.",),
            )
        return parser.parse(snapshot)
