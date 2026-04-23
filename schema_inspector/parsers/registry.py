"""Parser registry for replayable raw-snapshot normalization."""

from __future__ import annotations

from dataclasses import dataclass, field

from .base import PARSE_STATUS_SOFT_ERROR, PARSE_STATUS_UNSUPPORTED, ParseResult, RawSnapshot, SnapshotParser
from .classifier import classify_snapshot, is_soft_error_payload
from .families.entity_profiles import EntityProfilesParser
from .families.event_best_players import EventBestPlayersParser
from .families.event_comments import EventCommentsParser
from .families.event_graph import EventGraphParser
from .families.event_h2h import EventH2HParser
from .families.event_incidents import EventIncidentsParser
from .families.event_lineups import EventLineupsParser
from .families.event_managers import EventManagersParser
from .families.event_odds import EventOddsParser
from .families.event_player_statistics import EventPlayerStatisticsParser
from .families.event_pregame_form import EventPregameFormParser
from .families.event_root import EventRootParser
from .families.event_statistics import EventStatisticsParser
from .families.event_team_heatmap import EventTeamHeatmapParser
from .families.event_votes import EventVotesParser
from .families.event_winning_odds import EventWinningOddsParser
from .families.season_cuptrees import SeasonCupTreesParser
from .families.season_rounds import SeasonRoundsParser
from .special.baseball_innings import BaseballInningsParser
from .special.baseball_pitches import BaseballPitchesParser
from .special.event_player_rating_breakdown import EventPlayerRatingBreakdownParser
from .special.esports_games import EsportsGamesParser
from .special.shotmap import ShotmapParser
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
            EventBestPlayersParser(),
            EventCommentsParser(),
            EventGraphParser(),
            EventManagersParser(),
            EventH2HParser(),
            EventPregameFormParser(),
            EventVotesParser(),
            EventTeamHeatmapParser(),
            EventOddsParser(),
            EventWinningOddsParser(),
            EventPlayerStatisticsParser(),
            SeasonRoundsParser(),
            SeasonCupTreesParser(),
            EntityProfilesParser(),
            BaseballInningsParser(),
            BaseballPitchesParser(),
            EventPlayerRatingBreakdownParser(),
            ShotmapParser(),
            EsportsGamesParser(),
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
