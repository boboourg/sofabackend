"""REJECTED EXPERIMENT (2026-05-08): kept as historical reference.

This audit utility was used to validate the Sportradar OpenAPI mapping
against our endpoint_registry during the P3.2 design phase. The gate
based on this mapping was rolled back because historical simulation
showed FP=69.23 percent (target <0.5 percent) — Sofascore augments
coverage from multiple non-Sportradar sources, so the matrix is not
predictive of actual API availability.

See docs/archived/rejected-experiments/2026-05-08-p3-2-sportradar-coverage-gate.md
for the full design doc + rejection rationale.

Do NOT revive this utility without solving the data-source gap.
"""

"""Read-only audit utility: cross-reference Sportradar OpenAPI spec with our
endpoint_registry to determine which Sofascore endpoints are gate-able via
the Sportradar coverage matrix and which are Sofascore-only.

Inputs:
- openapi.yaml (Sportradar Soccer Extended v4 spec)
- footballmatrix.xlsx (Sportradar coverage matrix export)
- prod endpoint_registry (via SSH)

Outputs:
- Markdown report to .cache/sportradar_endpoint_audit.md
- Recommended ENDPOINT_TO_COVERAGE_REQUIREMENT dict as Python literal

This is READ-ONLY: no DB writes, no code changes, no prod modifications.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import openpyxl
import yaml


# Heuristic mapping: Sportradar API path → coverage attribute name
# Based on path semantics + Sportradar Soccer Extended v4 documentation.
# This is the AUTHORITATIVE source-of-truth: each SR endpoint maps to one
# (or sometimes multiple) coverage attribute(s) in the Sportradar matrix.
SR_ENDPOINT_TO_ATTR: dict[str, str | tuple[str, ...]] = {
    # Event-level endpoints
    "/sport_events/{urn_sport_event}/lineups": "Lineups",
    "/sport_events/{urn_sport_event}/timeline": "Basic Play by Play",
    "/sport_events/{urn_sport_event}/extended_timeline": "Deeper Play by Play",
    "/sport_events/{urn_sport_event}/summary": ("Basic Statistics", "Results", "Schedules"),
    "/sport_events/{urn_sport_event}/extended_summary": "Extended Statistics",
    "/sport_events/{urn_sport_event}/momentum": "(no SR matrix attr)",  # Sportradar feature, not in xlsx
    "/sport_events/{urn_sport_event}/insights": "(no SR matrix attr)",  # analytics, not gated
    "/sport_events/{urn_sport_event}/fun_facts": "(no SR matrix attr)",
    "/sport_events/{urn_sport_event}/league_timeline": "(no SR matrix attr)",
    # Season-level endpoints
    "/seasons/{urn_season}/standings": ("Standings", "Live Standings"),
    "/seasons/{urn_season}/form_standings": "Standings",
    "/seasons/{urn_season}/leaders": "Leaders",
    "/seasons/{urn_season}/lineups": "Lineups",  # season-aggregated
    "/seasons/{urn_season}/players": "Squads",
    "/seasons/{urn_season}/competitor_players": "Squads",
    "/seasons/{urn_season}/missing_players": "Squads",
    "/seasons/{urn_season}/competitors": "Schedules",
    "/seasons/{urn_season}/competitors/{urn_competitor}/extended_statistics": "Extended Statistics",
    "/seasons/{urn_season}/competitors/{urn_competitor}/statistics": "Basic Statistics",
    "/seasons/{urn_season}/probabilities": "(no SR matrix attr — premium add-on)",
    "/seasons/{urn_season}/over_under_statistics": "(no SR matrix attr)",
    "/seasons/{urn_season}/transfers": "(no SR matrix attr)",
    "/seasons/{urn_season}/schedules": "Schedules",
    "/seasons/{urn_season}/summaries": "Schedules",
    "/seasons/{urn_season}/info": "Schedules",
    "/seasons/{urn_season}/stages_groups_cup_rounds": "Standings",
    "/seasons/{urn_season}/simple_team_mappings": "(reconciliation)",
    "/seasons/{urn_season}/simple_tournament_mappings": "(reconciliation)",
    # Live endpoints
    "/schedules/live/schedules": ("Live", "Schedules"),
    "/schedules/live/summaries": ("Live", "Schedules"),
    "/schedules/live/timelines": "Live",
    "/schedules/live/timelines_delta": "Push",
    "/sport_events/created": "Schedules",
    "/sport_events/updated": "Schedules",
    "/sport_events/removed": "Schedules",
    "/sport_events/{urn_sport_event}/extended_summary": "Extended Statistics",
    # Schedule endpoints
    "/schedules/{urn_date}/schedules": "Schedules",
    "/schedules/{urn_date}/summaries": "Schedules",
    # Competitor / player endpoints
    "/competitors/{urn_competitor}/profile": "Competitor Profile",
    "/competitors/{urn_competitor}/schedules": "Schedules",
    "/competitors/{urn_competitor}/summaries": "Schedules",
    "/competitors/{urn_competitor}/versus/{urn_competitor2}/summaries": "Head2Head",
    "/competitors/mappings": "(reconciliation)",
    "/competitors/merge_mappings": "(reconciliation)",
    "/players/{urn_player}/profile": "Squads",
    "/players/{urn_player}/schedules": "Schedules",
    "/players/{urn_player}/summaries": "Schedules",
    "/players/mappings": "(reconciliation)",
    "/players/merge_mappings": "(reconciliation)",
    # Competition / structural
    "/competitions": "Schedules",
    "/competitions/{urn_competition}/info": "Schedules",
    "/competitions/{urn_competition}/seasons": "Schedules",
    "/seasons": "Schedules",
    "/seasons_disabled": "Schedules",
    "/fifa_rankings": "(no SR matrix attr)",
    # Stream endpoints
    "/stream/events/subscribe": "Push",
    "/stream/statistics/subscribe": "Push",
}

# Mapping: Sportradar endpoint pattern → Sofascore endpoint pattern.
# Built from empirical probes through Chrome (2026-05-08) + reverse-engineering
# of our parsers in schema_inspector/parsers/.
#
# Endpoints in EXPLICIT_FAIL_OPEN below take precedence: even if mapped here,
# they will be classified as fail-open per user policy decision.
SR_TO_SOFASCORE: list[tuple[str, str]] = [
    # Event-level
    ("/sport_events/{urn_sport_event}/lineups", "/api/v1/event/{event_id}/lineups"),
    ("/sport_events/{urn_sport_event}/timeline", "/api/v1/event/{event_id}/incidents"),
    ("/sport_events/{urn_sport_event}/extended_timeline", "/api/v1/event/{event_id}/incidents"),
    # /event/{id} (root) intentionally NOT gated — always-useful base endpoint
    ("/sport_events/{urn_sport_event}/summary", "/api/v1/event/{event_id}/statistics"),
    ("/sport_events/{urn_sport_event}/extended_summary", "/api/v1/event/{event_id}/statistics"),
    # Season-level standings
    ("/seasons/{urn_season}/standings", "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/total"),
    ("/seasons/{urn_season}/standings", "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/home"),
    ("/seasons/{urn_season}/standings", "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/away"),
    # Season-level leaders (Leaders attr): all top-N variants + regularSeason variants
    ("/seasons/{urn_season}/leaders", "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall"),
    ("/seasons/{urn_season}/leaders", "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/regularSeason"),
    ("/seasons/{urn_season}/leaders", "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/overall"),
    ("/seasons/{urn_season}/leaders", "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/regularSeason"),
    ("/seasons/{urn_season}/leaders", "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players-per-game/all/overall"),
    ("/seasons/{urn_season}/leaders", "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players-per-game/all/regularSeason"),
    ("/seasons/{urn_season}/leaders", "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-ratings/overall"),
    ("/seasons/{urn_season}/leaders", "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season"),
    # Team-level extended statistics (with explicit UT/season context)
    ("/seasons/{urn_season}/competitors/{urn_competitor}/extended_statistics", "/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics"),
    # Competitor / player profile
    ("/competitors/{urn_competitor}/profile", "/api/v1/team/{team_id}"),
    ("/competitors/{urn_competitor}/versus/{urn_competitor2}/summaries", "/api/v1/event/{event_id}/h2h"),
    ("/competitors/{urn_competitor}/schedules", "/api/v1/team/{team_id}/events/last/{page}"),
    ("/competitors/{urn_competitor}/schedules", "/api/v1/team/{team_id}/events/next/{page}"),
    # NOTE: /player/{id} is NOT gated — player profile is always-available, not Squads
    # NOTE: /team/{id}/players is NOT gated — no season context to look up SR coverage
    # Live
    ("/schedules/live/summaries", "/api/v1/sport/{sport_slug}/events/live"),
]


# Explicit fail-open overrides: endpoints that map to SR but should NEVER be
# hard-gated. Reasons documented per-pattern. These take precedence over
# SR_TO_SOFASCORE in classification.
EXPLICIT_FAIL_OPEN: dict[str, str] = {
    "/api/v1/event/{event_id}":
        "Root event endpoint — always-useful data, never gate",
    "/api/v1/player/{player_id}":
        "Player profile — not a Squads endpoint, profile is always-available data",
    "/api/v1/team/{team_id}/players":
        "Sofascore endpoint has no season context — can't reliably lookup SR Squads coverage",
    # Phase-1 policy: team-level endpoints lack reliable UT context.
    # A team plays in multiple tournaments; no canonical (ut_id, season_id) to look up SR.
    # Plus Competitor Profile / Schedules have ~100% SR coverage anyway → gate would be no-op.
    # See design doc 2026-05-08-p3-2-sportradar-coverage-gate.md §4.2 + §6.3 (GateContextResolver).
    "/api/v1/team/{team_id}":
        "Team profile — no canonical UT context for SR lookup; defer to Phase 2 with resolver",
    "/api/v1/team/{team_id}/events/last/{page}":
        "Team schedules — no canonical UT context; Schedules attr is 100% covered anyway",
    "/api/v1/team/{team_id}/events/next/{page}":
        "Team schedules — no canonical UT context; Schedules attr is 100% covered anyway",
}

# Endpoints that are gate-able but kept in dry-run observation only during Phase 3.
# Reason: insufficient historical data to validate predicted-block accuracy.
# At runtime, gate emits dry_run log but does NOT block, regardless of
# global SCHEMA_INSPECTOR_SPORTRADAR_GATE_DRY_RUN value.
DRYRUN_OBSERVATION_ONLY: dict[str, str] = {
    "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-ratings/overall":
        "Insufficient probe history — observe 30+ days before graduating to live block",
}

# Sofascore-only endpoints (NOT in Sportradar API spec → fail open in gate)
# These are Sofascore-derived, third-party-sourced, or computed locally.
SOFASCORE_ONLY_ENDPOINTS: dict[str, str] = {
    "/api/v1/event/{event_id}/best-players": "Sofascore-derived (per-event aggregation over Lineups + ratings)",
    "/api/v1/event/{event_id}/best-players/summary": "Sofascore-derived",
    "/api/v1/event/{event_id}/comments": "Sofascore live commentary feed (separate provider/source)",
    "/api/v1/event/{event_id}/votes": "Sofascore feature (user votes)",
    "/api/v1/event/{event_id}/highlights": "Sofascore content (video links)",
    "/api/v1/event/{event_id}/pregame-form": "Sofascore-derived (computed from history)",
    "/api/v1/event/{event_id}/managers": "Sofascore data (sourced separately)",
    "/api/v1/event/{event_id}/team-streaks": "Sofascore-derived (computed)",
    "/api/v1/event/{event_id}/featured-players": "Sofascore feature",
    "/api/v1/event/{event_id}/graph": "Maps to SR /momentum but no matrix attr → fail-open",
    # Odds providers (not Sportradar)
    "/api/v1/event/{event_id}/odds/{provider_id}/all": "Odds provider feed (not Sportradar)",
    "/api/v1/event/{event_id}/odds/{provider_id}/featured": "Odds provider feed",
    "/api/v1/event/{event_id}/winning-odds": "DEPRECATED (always 404)",
    "/api/v1/event/{event_id}/featured-odds": "DEPRECATED (always 404)",
    # Sport-specific Sofascore endpoints
    "/api/v1/event/{event_id}/at-bats": "Baseball-specific (probably MLB Stats API source, not SR)",
    "/api/v1/event/{event_id}/innings": "Cricket-specific",
    "/api/v1/event/{event_id}/point-by-point": "Tennis-specific",
    "/api/v1/event/{event_id}/tennis-power": "Tennis-specific",
    "/api/v1/event/{event_id}/esports-games": "Esports-specific",
    "/api/v1/event/{event_id}/shotmap": "Sofascore (computed shot positions)",
    "/api/v1/event/{event_id}/heatmap/{team_id}": "Sofascore (computed)",
    "/api/v1/event/{event_id}/player/{player_id}/heatmap": "Sofascore (computed)",
    "/api/v1/event/{event_id}/player/{player_id}/rating-breakdown": "Sofascore (computed ratings)",
    "/api/v1/event/{event_id}/average-positions": "Sofascore (computed)",
    "/api/v1/event/{event_id}/goalkeeper-shotmap/player/{player_id}": "Sofascore (computed)",
    "/api/v1/event/{event_id}/official-tweets": "Twitter feed (not SR)",
    "/api/v1/event/{event_id}/graph/sequence": "Sofascore variant",
}


@dataclass
class SRpath:
    path: str
    operation_id: str
    description: str
    parameters: list[dict[str, Any]] = field(default_factory=list)
    response_schema_ref: str | None = None
    coverage_attr: str | tuple[str, ...] = "?"


@dataclass
class SofascoreEndpoint:
    pattern: str
    target_table: str | None
    sr_path: str | None = None
    sr_attr: str | tuple[str, ...] | None = None
    category: str = "unknown"  # gate-able / sofascore-only / odds / deprecated
    notes: str = ""


def parse_openapi(yaml_path: Path) -> dict[str, SRpath]:
    """Parse Sportradar openapi.yaml and extract path metadata."""
    with yaml_path.open("r", encoding="utf-8") as f:
        spec = yaml.safe_load(f)

    paths_raw = spec.get("paths", {})
    out: dict[str, SRpath] = {}
    for path, ops in paths_raw.items():
        for method, op in ops.items():
            if not isinstance(op, dict):
                continue
            opid = op.get("operationId", "?")
            desc = op.get("description", op.get("summary", ""))
            params = op.get("parameters", [])
            # Try to extract response schema reference
            ref = None
            try:
                resp_200 = op["responses"]["200"]["content"]["application/json"]["schema"]
                ref = resp_200.get("$ref") or resp_200.get("type", "?")
            except (KeyError, TypeError):
                pass

            # Strip {locale} prefix for matching
            normalized_path = path
            if "/{locale}/" in normalized_path:
                normalized_path = normalized_path.replace("/{locale}", "")

            out[normalized_path] = SRpath(
                path=normalized_path,
                operation_id=opid,
                description=str(desc).strip()[:200],
                parameters=params,
                response_schema_ref=ref,
                coverage_attr=SR_ENDPOINT_TO_ATTR.get(normalized_path, "?"),
            )
    return out


def parse_xlsx_attrs(xlsx_path: Path) -> dict[str, dict[str, int]]:
    """Parse Sportradar football matrix → returns {sr_competition_id: {attr: value}}."""
    wb = openpyxl.load_workbook(xlsx_path, data_only=True)
    ws = wb.active
    headers = []
    out: dict[str, dict[str, int]] = {}
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            headers = list(row)
            continue
        sr_full = row[2]
        if sr_full and "sr:competition:" in str(sr_full):
            sr_id = str(sr_full).replace("sr:competition:", "")
            attrs = {}
            for h, v in zip(headers, row):
                if h not in ("Category", "Name", "Comp. ID") and v is not None:
                    try:
                        attrs[h] = int(v)
                    except (TypeError, ValueError):
                        pass
            out[sr_id] = attrs
    return out


def fetch_endpoint_registry_from_prod() -> list[tuple[str, str | None]]:
    """SSH to prod, fetch endpoint_registry rows, dedupe by path_template + target_table.

    Uses bash -c to avoid Windows cmd.exe escaping issues. The SQL must use
    NULL coalescing inline because we cannot reliably escape inner quotes.
    """
    inner = (
        "cd /opt/sofascore && "
        "PGPASSWORD=$(grep -oP 'postgresql://[^:]+:\\K[^@]+' .env) "
        "psql -U sofascore_user -d sofascore_schema_inspector -h localhost -p 5432 "
        "-A -t -F'|' "
        "-c \"SELECT DISTINCT path_template, COALESCE(target_table, '(none)') "
        "FROM endpoint_registry ORDER BY 1, 2;\""
    )
    cmd = ["ssh", "sofascore-prod", inner]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        rows = []
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue
            parts = line.split("|")
            if len(parts) < 2:
                continue
            pt = parts[0].strip()
            tt_raw = parts[1].strip()
            tt = None if tt_raw == "(none)" else tt_raw
            if pt:
                rows.append((pt, tt))
        return rows
    except Exception as e:
        print(f"WARN: SSH fetch failed: {e}", file=sys.stderr)
        return []


@dataclass
class ClassifyResult:
    """Single canonical classification per Sofascore endpoint pattern.

    Used as the source of truth for all report sections — no double-counting,
    no recategorization elsewhere.
    """
    category: str  # gate_able | dry_run_observation | fail_open_override | sofascore_only | odds | deprecated | unmapped
    required_attr: str | None
    hard_gate: bool
    reason: str
    confidence: float
    sr_path: str | None = None


# Preference order for picking required_attr when multiple SR endpoints map
# to one Sofascore endpoint. LEAST RESTRICTIVE first to minimize false negatives.
ATTR_PREF_ORDER: tuple[str, ...] = (
    "Lineups", "Leaders", "Squads", "Head2Head", "Competitor Profile",
    "Basic Play by Play", "Basic Statistics",
    "Deeper Play by Play", "Extended Statistics",
    "Standings", "Live Standings", "Schedules", "Live", "Push",
)


def _flatten_attrs(attr_value: str | tuple[str, ...]) -> list[str]:
    """Unpack tuple/string attr to list, filtering out (no SR matrix attr) markers."""
    raw = (attr_value,) if isinstance(attr_value, str) else attr_value
    return [a for a in raw if not a.startswith("(no") and not a.startswith("(reconciliation")]


def _pick_least_restrictive(attrs: list[str]) -> str:
    """Among candidate attrs, pick the most inclusive baseline."""
    for pref in ATTR_PREF_ORDER:
        if pref in attrs:
            return pref
    return attrs[0] if attrs else "?"


def classify_sofascore_endpoint(pattern: str) -> ClassifyResult:
    """Single canonical classifier. Order of precedence:
    1. Explicit fail-open override (user policy)
    2. Sofascore-only / odds / deprecated (no SR equivalent)
    3. Mapped via SR_TO_SOFASCORE (gate-able)
    4. Unmapped (default fail-open, needs review)
    """
    # 1. Explicit fail-open overrides
    if pattern in EXPLICIT_FAIL_OPEN:
        return ClassifyResult(
            category="fail_open_override",
            required_attr=None,
            hard_gate=False,
            reason=EXPLICIT_FAIL_OPEN[pattern],
            confidence=1.0,
        )
    # 2. Sofascore-only / odds / deprecated registry
    for pat, note in SOFASCORE_ONLY_ENDPOINTS.items():
        if pattern == pat:
            if "DEPRECATED" in note:
                cat = "deprecated"
            elif "Odds provider" in note:
                cat = "odds"
            else:
                cat = "sofascore_only"
            return ClassifyResult(
                category=cat,
                required_attr=None,
                hard_gate=False,
                reason=note,
                confidence=1.0,
            )
    # 3. SR mapping → gate-able OR dry_run_observation (graduated rollout)
    candidate_attrs: list[str] = []
    candidate_sr_paths: list[str] = []
    for sr_path, sof_path in SR_TO_SOFASCORE:
        if pattern == sof_path:
            attrs = _flatten_attrs(SR_ENDPOINT_TO_ATTR.get(sr_path, ""))
            candidate_attrs.extend(attrs)
            candidate_sr_paths.append(sr_path)
    if candidate_attrs:
        chosen = _pick_least_restrictive(candidate_attrs)
        sr_path_str = "; ".join(sorted(set(candidate_sr_paths)))
        # Dry-run observation policy: hard_gate stays True (gate evaluates),
        # but at runtime the env-driven `dryrun_only_endpoints` set forces
        # dry-run mode for these specific paths regardless of global setting.
        if pattern in DRYRUN_OBSERVATION_ONLY:
            return ClassifyResult(
                category="dry_run_observation",
                required_attr=chosen,
                hard_gate=False,  # effectively dry-run during Phase 3
                reason=f"{DRYRUN_OBSERVATION_ONLY[pattern]} (maps to SR via {chosen})",
                confidence=1.0,
                sr_path=sr_path_str,
            )
        return ClassifyResult(
            category="gate_able",
            required_attr=chosen,
            hard_gate=True,
            reason=f"Maps to SR {sr_path_str} via {chosen}",
            confidence=1.0,
            sr_path=sr_path_str,
        )
    # 4. Unmapped
    return ClassifyResult(
        category="unmapped",
        required_attr=None,
        hard_gate=False,
        reason="No SR mapping found — defaults to fail-open until manual review",
        confidence=0.0,
    )


def derive_min_value_for_attr(attr: str | tuple[str, ...]) -> int:
    """Default min_value for gate decision (>= 1 means partial+full, == 2 means full only)."""
    return 1


def render_report(
    sr_paths: dict[str, SRpath],
    coverage_stats: dict[str, dict[str, int]],
    registry: list[tuple[str, str | None]],
) -> str:
    """Render markdown report v2 — single classifier source of truth.

    All sections derived from one pass over deduped registry patterns.
    """
    # Single source of truth: classify each unique pattern from registry once.
    unique_patterns: dict[str, str | None] = {}
    for pt, tt in registry:
        if pt not in unique_patterns:
            unique_patterns[pt] = tt
    classifications: dict[str, ClassifyResult] = {
        pat: classify_sofascore_endpoint(pat) for pat in unique_patterns
    }
    by_cat: dict[str, list[str]] = defaultdict(list)
    for pat, cls in classifications.items():
        by_cat[cls.category].append(pat)

    lines = []
    lines.append("# Sportradar OpenAPI x Sofascore Endpoint Audit (v2)")
    lines.append("")
    lines.append(f"Generated by `tools/sportradar_openapi_audit.py` (read-only)")
    lines.append("")
    lines.append(f"- Sportradar API paths in spec: **{len(sr_paths)}**")
    lines.append(f"- Sportradar football competitions in coverage matrix: **{len(coverage_stats)}**")
    lines.append(f"- Unique Sofascore **path_template** values in prod endpoint_registry: **{len(unique_patterns)}**")
    lines.append("")
    lines.append("> **Terminology** (locked):")
    lines.append("> - `path_template` = canonical Sofascore route (e.g. `/api/v1/event/{event_id}/lineups`).")
    lines.append("> - `pattern` = `path_template` + optional `#phase=...` qualifier")
    lines.append("> (e.g. `#phase=inprogress`, `#phase=finished`, `#phase=notstarted`,")
    lines.append("> `#phase=canceled`, `#phase=postponed`).")
    lines.append("> - In prod registry: **244 distinct patterns**, of which **170 are unique path_templates**")
    lines.append("> (74 are `#phase` variants of the same route).")
    lines.append(">")
    lines.append("> **Gate decision is at `path_template` level**: all `#phase` variants of one route")
    lines.append("> share the same SR coverage check. Per-phase fetching policy (when to fetch)")
    lines.append("> is orthogonal — handled by the existing endpoint state matrix, not by this gate.")
    lines.append("")
    lines.append("## Section 0: FINAL TABLE — single source of truth")
    lines.append("")
    lines.append("Strict 1:1 with `endpoint_registry.path_template` (deduped). All counts in")
    lines.append("subsequent sections derive from this table.")
    lines.append("")
    lines.append("| pattern | required_attr | hard_gate | reason | confidence |")
    lines.append("|---|---|:---:|---|---:|")
    cat_priority = {"gate_able": 0, "dry_run_observation": 1, "fail_open_override": 2,
                    "sofascore_only": 3, "odds": 4, "deprecated": 5, "unmapped": 6}
    for pat in sorted(classifications, key=lambda p: (cat_priority.get(classifications[p].category, 99), p)):
        cls = classifications[pat]
        attr = cls.required_attr or "—"
        gate = "**YES**" if cls.hard_gate else "no"
        # Truncate reason for table
        rsn = cls.reason if len(cls.reason) < 80 else cls.reason[:77] + "..."
        # Escape pipes in pattern
        pat_safe = pat.replace("|", "\\|")
        lines.append(f"| `{pat_safe}` | {attr} | {gate} | {rsn} | {cls.confidence:.1f} |")
    lines.append("")
    # Single canonical count
    total = len(unique_patterns)
    n_gate = len(by_cat.get("gate_able", []))
    n_dry = len(by_cat.get("dry_run_observation", []))
    n_fopen = len(by_cat.get("fail_open_override", []))
    n_sof = len(by_cat.get("sofascore_only", []))
    n_odds = len(by_cat.get("odds", []))
    n_dep = len(by_cat.get("deprecated", []))
    n_unm = len(by_cat.get("unmapped", []))
    lines.append("**Counts (single source of truth):**")
    lines.append("")
    lines.append(f"- gate_able (hard_gate=YES, Phase 3 live): **{n_gate}**")
    lines.append(f"- dry_run_observation (mapped but Phase 3 dry-run only): **{n_dry}**")
    lines.append(f"- fail_open_override (policy fail-open, no UT context): **{n_fopen}**")
    lines.append(f"- sofascore_only (no SR equivalent): **{n_sof}**")
    lines.append(f"- odds providers: **{n_odds}**")
    lines.append(f"- deprecated (always 404): **{n_dep}**")
    lines.append(f"- unmapped (default fail-open, needs manual review): **{n_unm}**")
    lines.append(f"- **TOTAL** unique patterns in registry: **{total}**")
    lines.append("")

    # Section 1: Coverage attr → SR endpoints
    lines.append("## Section 1: Coverage attribute -> Sportradar endpoint(s)")
    lines.append("")
    lines.append("Maps each xlsx coverage attribute to the Sportradar API endpoint that produces it.")
    lines.append("")
    attr_to_sr = defaultdict(list)
    for path, sr in sr_paths.items():
        attrs = sr.coverage_attr if isinstance(sr.coverage_attr, tuple) else (sr.coverage_attr,)
        for a in attrs:
            attr_to_sr[a].append(path)
    lines.append("| Coverage attribute | Sportradar endpoint(s) |")
    lines.append("|---|---|")
    primary_attrs = [
        "Live", "Schedules", "Results", "Scoring Events", "Standings", "Live Standings",
        "Squads", "Competitor Profile", "Head2Head", "Push", "Lineups", "Leaders",
        "Extended Statistics", "Deeper Statistics", "Basic Statistics",
        "Deeper Play by Play", "Basic Play by Play",
    ]
    for a in primary_attrs:
        eps = attr_to_sr.get(a, [])
        if eps:
            ep_list = "; ".join(sorted(eps))
            lines.append(f"| **{a}** | {ep_list} |")
        else:
            lines.append(f"| **{a}** | (no direct endpoint - may be aggregate of multiple) |")
    lines.append("")

    # Section 2: SR endpoint → Sofascore endpoint mapping
    lines.append("## Section 2: Sportradar endpoint -> Sofascore endpoint")
    lines.append("")
    lines.append("Mapping built from empirical Sofascore probes (2026-05-08) + parser code.")
    lines.append("")
    lines.append("| Sportradar path | Sofascore path | Coverage attr | Min value |")
    lines.append("|---|---|---|---|")
    for sr_path, sof_path in SR_TO_SOFASCORE:
        attr = SR_ENDPOINT_TO_ATTR.get(sr_path, "?")
        attr_str = attr if isinstance(attr, str) else " / ".join(attr)
        min_val = derive_min_value_for_attr(attr)
        lines.append(f"| `{sr_path}` | `{sof_path}` | {attr_str} | >= {min_val} |")
    lines.append("")

    # Section 3: Detail per category (uses single classifications dict)
    lines.append("## Section 3: Per-category detail (from same classifications)")
    lines.append("")
    for cat_label, cat_key in [
        ("3a. gate_able (hard_gate=YES, Phase 3 live)", "gate_able"),
        ("3b. dry_run_observation (Phase 3 dry-run, Phase 4+ graduate)", "dry_run_observation"),
        ("3c. fail_open_override (policy fail-open)", "fail_open_override"),
        ("3d. sofascore_only", "sofascore_only"),
        ("3e. odds providers", "odds"),
        ("3f. deprecated (always 404)", "deprecated"),
        ("3g. unmapped (default fail-open)", "unmapped"),
    ]:
        pats = by_cat.get(cat_key, [])
        lines.append(f"### {cat_label} — {len(pats)}")
        lines.append("")
        if not pats:
            lines.append("_(none)_")
            lines.append("")
            continue
        if cat_key == "gate_able":
            lines.append("| pattern | required_attr | target_table | SR source |")
            lines.append("|---|---|---|---|")
            for pat in sorted(pats):
                cls = classifications[pat]
                tt = unique_patterns[pat] or "(none)"
                lines.append(f"| `{pat}` | **{cls.required_attr}** | {tt} | {cls.sr_path} |")
        else:
            lines.append("| pattern | target_table | reason |")
            lines.append("|---|---|---|")
            for pat in sorted(pats):
                cls = classifications[pat]
                tt = unique_patterns[pat] or "(none)"
                rsn = cls.reason[:90] + "..." if len(cls.reason) > 90 else cls.reason
                lines.append(f"| `{pat}` | {tt} | {rsn} |")
        lines.append("")

    # Section 4: Recommended ENDPOINT_TO_COVERAGE_REQUIREMENT — only from
    # gate_able classifications (strict registry-driven, no inferred entries)
    lines.append("## Section 4: Recommended ENDPOINT_TO_COVERAGE_REQUIREMENT")
    lines.append("")
    lines.append("Drop into `schema_inspector/services/sportradar_coverage_gate.py`.")
    lines.append("Built **strictly from gate_able classifications** (registry-driven, single source of truth).")
    lines.append("")
    lines.append("```python")
    lines.append("ENDPOINT_TO_COVERAGE_REQUIREMENT: dict[str, tuple[str, int]] = {")
    for pat in sorted(by_cat.get("gate_able", [])):
        cls = classifications[pat]
        min_val = derive_min_value_for_attr(cls.required_attr or "")
        lines.append(f'    "{pat}": ("{cls.required_attr}", {min_val}),')
    lines.append("}")
    lines.append("```")
    lines.append("")
    lines.append("Endpoints NOT in this dict → gate falls open (worker fetches as before).")
    lines.append("")

    # Section 5: Coverage distribution sanity
    lines.append("## Section 5: Coverage distribution (football, 1269 competitions)")
    lines.append("")
    lines.append("| Attribute | Full (2) | Partial (1) | None (0) |")
    lines.append("|---|---:|---:|---:|")
    for a in primary_attrs:
        c = defaultdict(int)
        for sr_id, attrs in coverage_stats.items():
            v = attrs.get(a)
            if v is not None:
                c[v] += 1
        full = c.get(2, 0)
        partial = c.get(1, 0)
        none_ = c.get(0, 0)
        lines.append(f"| {a} | {full} | {partial} | {none_} |")
    lines.append("")

    # Section 6: Field-level proof-of-concept (one endpoint sample)
    lines.append("## Section 6: Field-level audit (proof-of-concept for /lineups)")
    lines.append("")
    lines.append("This section samples ONE endpoint to demonstrate field-level diff capability.")
    lines.append("Full audit can be extended in v2 with per-endpoint parser introspection.")
    lines.append("")
    lineups_sr = sr_paths.get("/sport_events/{urn_sport_event}/lineups")
    if lineups_sr:
        lines.append(f"**Sportradar endpoint**: `/sport_events/{{urn_sport_event}}/lineups`")
        lines.append(f"- operationId: `{lineups_sr.operation_id}`")
        lines.append(f"- response schema ref: `{lineups_sr.response_schema_ref}`")
        lines.append(f"- description: _{lineups_sr.description[:120]}_")
        lines.append("")
        lines.append("**Sofascore equivalent**: `/api/v1/event/{event_id}/lineups`")
        lines.append("- target_table: `event_lineup`")
        lines.append("- empirical sample (live event): 47 KB JSON, ~20 players + meta")
        lines.append("")
        lines.append("**Cross-check needed**: parse `event_lineup` schema in `schema_inspector/parsers/families/`")
        lines.append("and diff against fields declared in Sportradar `lineup` schema (`SportEventLineupResponse`).")
        lines.append("Mismatches indicate either (a) Sofascore-derived fields, or (b) data we are losing in normalization.")
        lines.append("")

    # Section 7: Stats summary (single source — same as Section 0)
    lines.append("## Section 7: Summary stats (re-affirmed from Section 0)")
    lines.append("")
    lines.append(f"- Total unique Sofascore endpoints in registry: **{total}**")
    lines.append(f"- **gate_able** (hard_gate=YES, Phase 3 live): **{n_gate}** ({n_gate * 100 // max(total, 1)}%)")
    lines.append(f"- **dry_run_observation** (Phase 3 dry-run only): **{n_dry}**")
    lines.append(f"- **fail_open_override**: **{n_fopen}**")
    lines.append(f"- **sofascore_only**: **{n_sof}**")
    lines.append(f"- **odds providers**: **{n_odds}**")
    lines.append(f"- **deprecated**: **{n_dep}**")
    lines.append(f"- **unmapped**: **{n_unm}**")
    lines.append("")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--openapi", default=r"C:\Users\bobur\Downloads\openapi.yaml")
    parser.add_argument("--xlsx", default=r"C:\Users\bobur\Downloads\footballmatrix.xlsx")
    parser.add_argument(
        "--output",
        default=r"D:\sofascore\.cache\sportradar_endpoint_audit.md",
    )
    parser.add_argument("--skip-prod", action="store_true", help="Skip SSH to prod (use empty registry)")
    args = parser.parse_args()

    print(f"[1/4] Parsing openapi: {args.openapi}", file=sys.stderr)
    sr_paths = parse_openapi(Path(args.openapi))
    print(f"      Found {len(sr_paths)} Sportradar endpoints", file=sys.stderr)

    print(f"[2/4] Parsing xlsx: {args.xlsx}", file=sys.stderr)
    coverage_stats = parse_xlsx_attrs(Path(args.xlsx))
    print(f"      Found {len(coverage_stats)} football competitions", file=sys.stderr)

    if args.skip_prod:
        print("[3/4] Skipping prod registry fetch", file=sys.stderr)
        registry = []
    else:
        print("[3/4] Fetching endpoint_registry from prod via SSH...", file=sys.stderr)
        registry = fetch_endpoint_registry_from_prod()
        print(f"      Got {len(registry)} rows", file=sys.stderr)

    print(f"[4/4] Rendering report to {args.output}", file=sys.stderr)
    report = render_report(sr_paths, coverage_stats, registry)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(report, encoding="utf-8")
    print(f"      Wrote {len(report):,} chars", file=sys.stderr)

    # Also print stats summary to stdout
    print(report.split("## Section 7:")[1] if "## Section 7:" in report else "(no summary)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
