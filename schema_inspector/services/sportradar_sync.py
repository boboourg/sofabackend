"""Sportradar coverage sync — load matrix xlsx → reconcile → upsert.

Reconciliation pipeline (priority order, first hit wins):

  1. Manual override     — config/sportradar_id_overrides.yaml
                           method='manual', confidence=1.0
  2. Blind ID match      — sr_id == ut.id AND sport matches AND name fuzz>=0.7
                           method='blind_id', confidence=1.0
  3. Name match          — fuzzy (name, country, sport) similarity
                           method='name_match', confidence=score (0.7..0.95)
  4. Unmatched           — none of the above
                           method='unmatched', confidence=0.0, ut_id=NULL

Sport scope: Phase 1 = football only. The xlsx file is per-sport; this sync
is invoked once per sport. Currently only football has been sourced.

The ``run_sportradar_sync`` function is the public entry point used by the
CLI subcommand ``sync-sportradar-coverage``.
"""

from __future__ import annotations

import datetime
import difflib
import logging
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import yaml

from ..storage.sportradar_repository import (
    SportradarCoverageRecord,
    SportradarCoverageRepository,
    SqlExecutor,
)

logger = logging.getLogger(__name__)


# Coverage attribute column names per the Sportradar matrix xlsx (football).
# These are the headers in the export.csv sheet starting from column index 3.
FOOTBALL_COVERAGE_ATTRS: tuple[str, ...] = (
    "Live", "Schedules", "Results", "Scoring Events",
    "Standings", "Live Standings", "Squads", "Competitor Profile",
    "Head2Head", "Push", "Lineups", "Leaders",
    "Extended Statistics", "Deeper Statistics", "Basic Statistics",
    "Deeper Play by Play", "Basic Play by Play",
)

# Threshold for accepting blind ID match (sr_id == ut.id) without confirmation.
# Verified empirically (12/12 popular leagues = 100% match), so threshold
# 0.7 is permissive — name signal is just a sanity check.
BLIND_ID_NAME_FUZZ_MIN = 0.7

# Threshold for accepting name match when blind ID failed.
NAME_MATCH_FUZZ_MIN = 0.85


@dataclass(frozen=True)
class XlsxRow:
    """One row from the Sportradar coverage matrix xlsx."""
    sr_competition_id: int
    sr_competition_name: str
    sr_category_name: str
    coverage_attrs: dict[str, int]
    tier: int | None = None


@dataclass
class SyncReport:
    sport_slug: str
    total_rows: int = 0
    method_blind_id: int = 0
    method_name_match: int = 0
    method_manual: int = 0
    method_unmatched: int = 0
    upserts_succeeded: int = 0
    upserts_failed: int = 0
    errors: list[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.errors is None:
            self.errors = []

    def as_dict(self) -> dict[str, Any]:
        return {
            "sport_slug": self.sport_slug,
            "total_rows": self.total_rows,
            "by_method": {
                "blind_id": self.method_blind_id,
                "name_match": self.method_name_match,
                "manual": self.method_manual,
                "unmatched": self.method_unmatched,
            },
            "upserts_succeeded": self.upserts_succeeded,
            "upserts_failed": self.upserts_failed,
            "errors_sample": self.errors[:5],
        }


# ---------------------------------------------------------------------------
# xlsx loader
# ---------------------------------------------------------------------------

def load_matrix_xlsx(path: Path) -> list[XlsxRow]:
    """Load Sportradar coverage matrix xlsx → list of XlsxRow.

    Expected schema: first row is header (Category, Name, Comp. ID, then
    coverage attribute columns 0/1/2 integers).
    """
    import openpyxl  # imported here to avoid hard dep on tests that mock load

    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active
    headers: list[str] = []
    rows: list[XlsxRow] = []
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            headers = [str(h or "") for h in row]
            continue
        if not row or len(row) < 3:
            continue
        sr_full = row[2]
        if not sr_full or "sr:competition:" not in str(sr_full):
            continue
        try:
            sr_id = int(str(sr_full).replace("sr:competition:", "").strip())
        except (TypeError, ValueError):
            continue
        category = str(row[0] or "")
        name = str(row[1] or "")
        attrs: dict[str, int] = {}
        for h, v in zip(headers, row):
            if h in ("Category", "Name", "Comp. ID") or v is None:
                continue
            try:
                attrs[h] = int(v)
            except (TypeError, ValueError):
                continue
        rows.append(XlsxRow(
            sr_competition_id=sr_id,
            sr_competition_name=name,
            sr_category_name=category,
            coverage_attrs=attrs,
        ))
    return rows


# ---------------------------------------------------------------------------
# Manual overrides loader
# ---------------------------------------------------------------------------

def load_manual_overrides(path: Path) -> dict[int, int]:
    """Load football overrides as {sportradar_id: sofascore_ut_id}."""
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    overrides = data.get("overrides", {}) or {}
    football = overrides.get("football", {}) or {}
    out: dict[int, int] = {}
    for sr_id, ut_id in football.items():
        try:
            out[int(sr_id)] = int(ut_id)
        except (TypeError, ValueError):
            continue
    return out


# ---------------------------------------------------------------------------
# Name normalization for fuzzy match
# ---------------------------------------------------------------------------

_NAME_SCRUB_PATTERNS = [
    re.compile(r"\b(fc|afc|cf|sc|club|football|federation|federa[cç]ão)\b", re.I),
    re.compile(r"\b(women|men|youth|u\d{1,2}|u-\d{1,2})\b", re.I),
    re.compile(r"[^\w\s]", re.UNICODE),
]


def _normalize_name(s: str) -> str:
    """Normalize for fuzzy matching: lowercase, strip diacritics, scrub fillers."""
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    text = ascii_only.lower().strip()
    for pat in _NAME_SCRUB_PATTERNS:
        text = pat.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def _fuzz_ratio(a: str, b: str) -> float:
    """SequenceMatcher ratio after name normalization. Range [0, 1]."""
    return difflib.SequenceMatcher(
        None, _normalize_name(a), _normalize_name(b)
    ).ratio()


# ---------------------------------------------------------------------------
# Reconciliation per row
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ReconcileResult:
    unique_tournament_id: int | None
    method: str
    confidence: float


async def reconcile_row(
    *,
    executor: SqlExecutor,
    xlsx_row: XlsxRow,
    sport_slug: str,
    manual_overrides: dict[int, int],
) -> ReconcileResult:
    """Determine ut_id for an xlsx row using priority: manual → blind_id → name_match → unmatched."""

    # 1. Manual override
    if xlsx_row.sr_competition_id in manual_overrides:
        return ReconcileResult(
            unique_tournament_id=manual_overrides[xlsx_row.sr_competition_id],
            method="manual",
            confidence=1.0,
        )

    # 2. Blind ID match — same id + same sport + name not totally unrelated
    blind_row = await executor.fetchrow(
        """
        SELECT ut.id AS ut_id, ut.name AS ut_name, s.slug AS sport_slug
        FROM unique_tournament ut
        JOIN category c ON c.id = ut.category_id
        JOIN sport s ON s.id = c.sport_id
        WHERE ut.id = $1
        """,
        xlsx_row.sr_competition_id,
    )
    if blind_row is not None and blind_row["sport_slug"] == sport_slug:
        name_score = _fuzz_ratio(blind_row["ut_name"], xlsx_row.sr_competition_name)
        if name_score >= BLIND_ID_NAME_FUZZ_MIN:
            return ReconcileResult(
                unique_tournament_id=int(blind_row["ut_id"]),
                method="blind_id",
                confidence=1.0,
            )

    # 3. Name match — search by sport + country + fuzzy name
    candidates = await executor.fetch(
        """
        SELECT ut.id AS ut_id, ut.name AS ut_name, c.name AS category_name
        FROM unique_tournament ut
        JOIN category c ON c.id = ut.category_id
        JOIN sport s ON s.id = c.sport_id
        WHERE s.slug = $1
          AND lower(c.name) = lower($2)
        """,
        sport_slug, xlsx_row.sr_category_name,
    )
    best: tuple[int, float] | None = None  # (ut_id, score)
    for cand in candidates:
        score = _fuzz_ratio(cand["ut_name"], xlsx_row.sr_competition_name)
        if best is None or score > best[1]:
            best = (int(cand["ut_id"]), score)
    if best is not None and best[1] >= NAME_MATCH_FUZZ_MIN:
        return ReconcileResult(
            unique_tournament_id=best[0],
            method="name_match",
            confidence=round(best[1], 2),
        )

    # 4. Unmatched
    return ReconcileResult(
        unique_tournament_id=None,
        method="unmatched",
        confidence=0.0,
    )


# ---------------------------------------------------------------------------
# Top-level sync
# ---------------------------------------------------------------------------

async def run_sportradar_sync(
    *,
    database: Any,
    xlsx_path: Path,
    manual_overrides_path: Path | None = None,
    sport_slug: str = "football",
    last_changed: str | None = None,
) -> SyncReport:
    """Load xlsx → reconcile → upsert all rows.

    Each row is reconciled+upserted in its own transaction so that one failure
    doesn't roll back the entire run.
    """
    rows = load_matrix_xlsx(xlsx_path)
    overrides = load_manual_overrides(manual_overrides_path) if manual_overrides_path else {}

    if last_changed is None:
        last_changed = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

    report = SyncReport(sport_slug=sport_slug, total_rows=len(rows))
    repo = SportradarCoverageRepository()

    for row in rows:
        async with database.connection() as conn:
            try:
                result = await reconcile_row(
                    executor=conn,
                    xlsx_row=row,
                    sport_slug=sport_slug,
                    manual_overrides=overrides,
                )
                if result.method == "blind_id":
                    report.method_blind_id += 1
                elif result.method == "name_match":
                    report.method_name_match += 1
                elif result.method == "manual":
                    report.method_manual += 1
                else:
                    report.method_unmatched += 1

                record = SportradarCoverageRecord(
                    sr_competition_id=row.sr_competition_id,
                    sr_competition_name=row.sr_competition_name,
                    sr_category_name=row.sr_category_name,
                    sport_slug=sport_slug,
                    unique_tournament_id=result.unique_tournament_id,
                    reconciliation_method=result.method,
                    reconciliation_confidence=result.confidence,
                    coverage_attrs=row.coverage_attrs,
                    tier=row.tier,
                    season_id=None,
                    season_start_date=None,
                    last_changed=last_changed,
                )
                await repo.upsert_coverage(conn, record)
                report.upserts_succeeded += 1
            except Exception as exc:
                report.upserts_failed += 1
                report.errors.append(f"sr={row.sr_competition_id}: {exc}")
                logger.warning("sportradar_sync upsert failed: sr=%s err=%s",
                               row.sr_competition_id, exc)

    return report


__all__ = [
    "FOOTBALL_COVERAGE_ATTRS",
    "ReconcileResult",
    "SyncReport",
    "XlsxRow",
    "load_manual_overrides",
    "load_matrix_xlsx",
    "reconcile_row",
    "run_sportradar_sync",
]
