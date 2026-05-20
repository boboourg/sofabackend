"""TDD tests for NULL-resilient upserts in event + event_round_info.

Bug discovered 2026-05-20 from /round/29/slug/final returning wrong
events:
* ``event.season_id`` was NULL for Man City vs Inter (UCL 22/23 Final
  event 11289024) — gets erased because ON CONFLICT writes
  ``season_id = EXCLUDED.season_id`` unconditionally. When a later
  compact payload (e.g. team-events feed) re-ingests the event
  without ``season.id`` in the wire format, the previously stored
  season_id is wiped.
* ``event_round_info.slug`` was NULL for the same event — same
  pattern: rich endpoint payload (events/last/0) carries ``slug``;
  compact endpoint payload (team-events scope) omits it; the
  ``ON CONFLICT DO UPDATE SET slug = EXCLUDED.slug`` wipes it.

Fix: every nullable column in ON CONFLICT clauses must use
``COALESCE(EXCLUDED.<col>, <table>.<col>)`` so that NULL from a new
ingest can never erase a previously stored non-null value. Non-null
values from new ingest still win (no regression for legitimate
updates).
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path


REPO_FILE = Path(__file__).resolve().parent.parent / "schema_inspector" / "event_list_repository.py"


def _read_repo_text() -> str:
    return REPO_FILE.read_text(encoding="utf-8")


class EventRoundInfoUpsertCoalesceTests(unittest.TestCase):
    """Pin the NULL-resilient pattern on `_upsert_event_round_infos`.

    These are static SQL inspection tests — pure unit, no DB needed.
    """

    def setUp(self) -> None:
        self.text = _read_repo_text()
        # Extract the body of _upsert_event_round_infos.
        match = re.search(
            r"async def _upsert_event_round_infos\(.*?\"\"\"(.*?)\"\"\"",
            self.text,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(match, "could not find _upsert_event_round_infos SQL")
        self.sql = match.group(1)

    def test_round_number_uses_coalesce_to_preserve_existing(self) -> None:
        self.assertRegex(
            self.sql,
            r"round_number\s*=\s*COALESCE\(\s*EXCLUDED\.round_number\s*,\s*event_round_info\.round_number\s*\)",
        )

    def test_slug_uses_coalesce_to_preserve_existing(self) -> None:
        self.assertRegex(
            self.sql,
            r"slug\s*=\s*COALESCE\(\s*EXCLUDED\.slug\s*,\s*event_round_info\.slug\s*\)",
        )

    def test_name_uses_coalesce_to_preserve_existing(self) -> None:
        self.assertRegex(
            self.sql,
            r"name\s*=\s*COALESCE\(\s*EXCLUDED\.name\s*,\s*event_round_info\.name\s*\)",
        )

    def test_cup_round_type_uses_coalesce_to_preserve_existing(self) -> None:
        self.assertRegex(
            self.sql,
            r"cup_round_type\s*=\s*COALESCE\(\s*EXCLUDED\.cup_round_type\s*,\s*event_round_info\.cup_round_type\s*\)",
        )


class EventUpsertCoalesceTests(unittest.TestCase):
    """Pin the NULL-resilient pattern on `_upsert_events` for the
    columns that are most damaging when erased: foreign keys to
    season / unique_tournament / tournament / teams. A compact
    endpoint (e.g. team-events) may carry the event id but omit
    season context — that must not erase known values.
    """

    def setUp(self) -> None:
        self.text = _read_repo_text()
        match = re.search(
            r"async def _upsert_events\(.*?\"\"\"(.*?)\"\"\"",
            self.text,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(match, "could not find _upsert_events SQL")
        self.sql = match.group(1)

    def test_season_id_uses_coalesce_to_preserve_existing(self) -> None:
        self.assertRegex(
            self.sql,
            r"season_id\s*=\s*COALESCE\(\s*EXCLUDED\.season_id\s*,\s*event\.season_id\s*\)",
        )

    def test_unique_tournament_id_uses_coalesce_to_preserve_existing(self) -> None:
        self.assertRegex(
            self.sql,
            r"unique_tournament_id\s*=\s*COALESCE\(\s*EXCLUDED\.unique_tournament_id\s*,\s*event\.unique_tournament_id\s*\)",
        )

    def test_tournament_id_uses_coalesce_to_preserve_existing(self) -> None:
        self.assertRegex(
            self.sql,
            r"tournament_id\s*=\s*COALESCE\(\s*EXCLUDED\.tournament_id\s*,\s*event\.tournament_id\s*\)",
        )

    def test_home_team_id_uses_coalesce_to_preserve_existing(self) -> None:
        self.assertRegex(
            self.sql,
            r"home_team_id\s*=\s*COALESCE\(\s*EXCLUDED\.home_team_id\s*,\s*event\.home_team_id\s*\)",
        )

    def test_away_team_id_uses_coalesce_to_preserve_existing(self) -> None:
        self.assertRegex(
            self.sql,
            r"away_team_id\s*=\s*COALESCE\(\s*EXCLUDED\.away_team_id\s*,\s*event\.away_team_id\s*\)",
        )


if __name__ == "__main__":
    unittest.main()
