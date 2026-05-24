from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from schema_inspector.fetch import FetchJsonError, fetch_json_sync
from schema_inspector.runtime import load_runtime_config


PROJECT_ROOT = Path(__file__).resolve().parent
INSPECT_API = PROJECT_ROOT / "inspect_api.py"
NEXT_DATA_PATH = PROJECT_ROOT / "reports" / "__NEXT_DATA__.txt"
DEFAULT_DATE = "2026-04-15"
DEFAULT_OUTDIR = PROJECT_ROOT / "reports" / "next_data_feature_probe_2026_04_15"


@dataclass
class SportContext:
    sport: str
    event_id: int | None = None
    team_id: int | None = None
    player_id: int | None = None
    urls_200: list[str] = field(default_factory=list)
    failures: list[dict[str, str]] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe sport-specific feature families inferred from __NEXT_DATA__.")
    parser.add_argument("--date", default=DEFAULT_DATE)
    parser.add_argument("--outdir", default=str(DEFAULT_OUTDIR))
    parser.add_argument("--timeout", type=float, default=18.0)
    parser.add_argument("--max-attempts", type=int, default=2)
    parser.add_argument(
        "--sport",
        action="append",
        default=[],
        help="Optional target sport. Defaults to ice-hockey, baseball, cricket, mma.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    runtime = load_runtime_config(max_attempts=args.max_attempts)

    next_data = load_next_data()
    target_sports = args.sport or ["ice-hockey", "baseball", "cricket", "mma"]
    contexts = []
    for sport in target_sports:
        context = SportContext(sport=sport)
        discover_context(context=context, runtime=runtime, date=args.date, timeout=args.timeout)
        probe_sport_features(context=context, runtime=runtime, outdir=outdir, timeout=args.timeout)
        contexts.append(context)

    summary = {
        "date": args.date,
        "source_file": str(NEXT_DATA_PATH),
        "requestUrl": next_data.get("requestUrl"),
        "sports": [
            {
                "sport": context.sport,
                "event_id": context.event_id,
                "team_id": context.team_id,
                "player_id": context.player_id,
                "urls_200": context.urls_200,
                "failures": context.failures,
            }
            for context in contexts
        ],
    }
    (outdir / "probe_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    lines = [
        "# Next Data Feature Probe Summary",
        "",
        f"- Source: `{NEXT_DATA_PATH}`",
        f"- Date: `{args.date}`",
        "",
    ]
    for context in contexts:
        lines.extend(
            [
                f"## {context.sport}",
                "",
                f"- event_id: `{context.event_id}`",
                f"- team_id: `{context.team_id}`",
                f"- player_id: `{context.player_id}`",
                f"- 200 urls: `{len(context.urls_200)}`",
                f"- failures: `{len(context.failures)}`",
                "",
                "### 200 URLs",
                "",
            ]
        )
        for url in context.urls_200:
            lines.append(f"- `{url}`")
        if not context.urls_200:
            lines.append("- None")
        lines.extend(["", "### Failures", ""])
        for item in context.failures:
            lines.append(f"- `{item['url']}` -> `{item['error']}`")
        if not context.failures:
            lines.append("- None")
        lines.append("")
    (outdir / "probe_summary.md").write_text("\n".join(lines), encoding="utf-8")
    print(outdir)
    return 0


def load_next_data() -> dict[str, Any]:
    raw = NEXT_DATA_PATH.read_text(encoding="utf-8", errors="ignore")
    raw = re.sub(r'^<script id="__NEXT_DATA__" type="application/json">', "", raw)
    raw = re.sub(r"</script>$", "", raw)
    data = json.loads(raw)
    return {
        "requestUrl": data.get("props", {}).get("requestUrl"),
        "footerEntities": data.get("props", {}).get("footerEntities", {}),
    }


def discover_context(*, context: SportContext, runtime, date: str, timeout: float) -> None:
    event_url = f"https://www.sofascore.com/api/v1/sport/{context.sport}/scheduled-events/{date}"
    try:
        result = fetch_json_sync(event_url, timeout=timeout, runtime_config=runtime)
    except Exception as exc:
        context.failures.append({"url": event_url, "error": repr(exc)})
        return
    events = result.payload.get("events", []) if isinstance(result.payload, dict) else []
    if not events:
        context.failures.append({"url": event_url, "error": "No events in scheduled-events payload"})
        return
    first_event = events[0]
    if isinstance(first_event, dict):
        context.event_id = first_event.get("id")
        for side in ("homeTeam", "awayTeam"):
            team = first_event.get(side)
            if context.team_id is None and isinstance(team, dict) and isinstance(team.get("id"), int):
                context.team_id = team["id"]

    if context.event_id is None:
        return

    detail_url = f"https://www.sofascore.com/api/v1/event/{context.event_id}"
    try:
        detail = fetch_json_sync(detail_url, timeout=timeout, runtime_config=runtime).payload
        extract_ids(detail, context)
    except Exception as exc:
        context.failures.append({"url": detail_url, "error": repr(exc)})

    lineup_url = f"https://www.sofascore.com/api/v1/event/{context.event_id}/lineups"
    try:
        lineup = fetch_json_sync(lineup_url, timeout=timeout, runtime_config=runtime).payload
        extract_ids(lineup, context)
    except Exception as exc:
        context.failures.append({"url": lineup_url, "error": repr(exc)})


def extract_ids(node: Any, context: SportContext) -> None:
    if isinstance(node, dict):
        object_id = node.get("id")
        if context.team_id is None and isinstance(object_id, int) and {"teamColors", "slug", "name"} <= set(node.keys()):
            context.team_id = object_id
        if context.player_id is None and isinstance(object_id, int) and {"slug", "name"} <= set(node.keys()):
            # avoid grabbing event/team/tournament ids
            if not {"homeTeam", "awayTeam"} & set(node.keys()) and "teamColors" not in node:
                context.player_id = object_id
        for value in node.values():
            extract_ids(value, context)
    elif isinstance(node, list):
        for item in node:
            extract_ids(item, context)


def probe_sport_features(*, context: SportContext, runtime, outdir: Path, timeout: float) -> None:
    if context.event_id is None:
        return

    generic = [
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/meta",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/statistics",
        f"https://api.sofascore.com/api/v1/event/{context.event_id}/statistics",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/lineups",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/incidents",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/best-players",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/h2h",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/pregame-form",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/graph",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/graph/win-probability",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/votes",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/comments",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/official-tweets",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/highlights",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/sport-video-highlights/country/UA/extended",
        f"https://www.sofascore.com/api/v1/event/{context.event_id}/live-action-widget",
    ]
    sport_specific: list[str] = []
    if context.sport == "ice-hockey":
        sport_specific.extend(
            [
                f"https://www.sofascore.com/api/v1/event/{context.event_id}/play-by-play",
                f"https://www.sofascore.com/api/v1/event/{context.event_id}/event-map",
                f"https://www.sofascore.com/api/v1/event/{context.event_id}/penalties",
                f"https://api.sofascore.com/api/v1/event/{context.event_id}/shotmap",
            ]
        )
        if context.team_id is not None:
            sport_specific.append(f"https://www.sofascore.com/api/v1/event/{context.event_id}/shotmap/{context.team_id}")
        if context.player_id is not None:
            sport_specific.append(
                f"https://www.sofascore.com/api/v1/event/{context.event_id}/player/{context.player_id}/shotmap"
            )
    elif context.sport == "baseball":
        sport_specific.extend(
            [
                f"https://www.sofascore.com/api/v1/event/{context.event_id}/plays",
                f"https://www.sofascore.com/api/v1/event/{context.event_id}/all-plays",
                f"https://www.sofascore.com/api/v1/event/{context.event_id}/batting-order",
                f"https://www.sofascore.com/api/v1/event/{context.event_id}/starting-pitchers",
            ]
        )
        if context.team_id is not None:
            sport_specific.append(f"https://www.sofascore.com/api/v1/event/{context.event_id}/shotmap/{context.team_id}")
    elif context.sport == "cricket":
        sport_specific.extend(
            [
                f"https://www.sofascore.com/api/v1/event/{context.event_id}/runs-per-over",
                f"https://www.sofascore.com/api/v1/event/{context.event_id}/runs-per-over/graph",
                f"https://www.sofascore.com/api/v1/event/{context.event_id}/overs",
            ]
        )

    seen = set()
    for url in generic + sport_specific:
        if url in seen:
            continue
        seen.add(url)
        probe_url(url=url, context=context, runtime=runtime, outdir=outdir, timeout=timeout)


def probe_url(*, url: str, context: SportContext, runtime, outdir: Path, timeout: float) -> None:
    try:
        result = fetch_json_sync(url, timeout=timeout, runtime_config=runtime)
    except FetchJsonError as exc:
        context.failures.append({"url": url, "error": str(exc)})
        return
    except Exception as exc:
        context.failures.append({"url": url, "error": repr(exc)})
        return

    if result.status_code != 200:
        context.failures.append({"url": url, "error": f"Unexpected status {result.status_code}"})
        return

    command = [
        sys.executable,
        str(INSPECT_API),
        url,
        "--outdir",
        str(outdir),
        "--timeout",
        str(timeout),
    ]
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode == 0:
        context.urls_200.append(url)
    else:
        context.failures.append(
            {
                "url": url,
                "error": completed.stderr.strip() or completed.stdout.strip() or "inspect_api failed",
            }
        )


if __name__ == "__main__":
    raise SystemExit(main())
