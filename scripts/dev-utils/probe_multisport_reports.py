from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from schema_inspector.fetch import FetchJsonError, FetchResult, fetch_json_sync
from schema_inspector.report import build_markdown_report, report_filename
from schema_inspector.runtime import RuntimeConfig, load_runtime_config
from schema_inspector.schema import infer_schema


EVENT_COUNT_URL = "https://www.sofascore.com/api/v1/sport/10800/event-count"
DEFAULT_DATE = "2026-04-15"
DEFAULT_TZ_OFFSET = "10800"
DEFAULT_COUNTRY = "UA"
DEFAULT_LANG = "ru"
PROJECT_ROOT = Path(__file__).resolve().parent
INSPECT_API_PATH = PROJECT_ROOT / "inspect_api.py"


@dataclass
class ProbeRecord:
    url: str
    status_code: int | None
    empty_200: bool
    empty_reason: str | None
    report_path: str | None
    report_mode: str | None
    note: str | None = None


@dataclass
class SportProbeContext:
    sport: str
    category_ids: list[int] = field(default_factory=list)
    preferred_category_ids: list[int] = field(default_factory=list)
    unique_tournament_ids: list[int] = field(default_factory=list)
    tournament_ids: list[int] = field(default_factory=list)
    season_ids: list[int] = field(default_factory=list)
    preferred_season_ids: list[int] = field(default_factory=list)
    event_ids: list[int] = field(default_factory=list)
    team_ids: list[int] = field(default_factory=list)
    player_ids: list[int] = field(default_factory=list)
    manager_ids: list[int] = field(default_factory=list)
    period_ids: list[int] = field(default_factory=list)
    round_numbers: list[int] = field(default_factory=list)
    successes: list[ProbeRecord] = field(default_factory=list)
    failures: list[dict[str, str]] = field(default_factory=list)
    skipped_urls: list[str] = field(default_factory=list)
    probed_urls: set[str] = field(default_factory=set)

    def first(self, field_name: str) -> int | None:
        values = getattr(self, field_name)
        return values[0] if values else None

    def preferred_first(self, preferred_field: str, fallback_field: str) -> int | None:
        preferred_values = getattr(self, preferred_field)
        if preferred_values:
            return preferred_values[0]
        return self.first(fallback_field)

    def add_value(self, field_name: str, value: Any) -> None:
        if not isinstance(value, int):
            return
        if value <= 0:
            return
        values = getattr(self, field_name)
        if value not in values:
            values.append(value)

    def snapshot(self) -> dict[str, Any]:
        return {
            "sport": self.sport,
            "category_ids": list(self.category_ids),
            "preferred_category_ids": list(self.preferred_category_ids),
            "unique_tournament_ids": list(self.unique_tournament_ids),
            "tournament_ids": list(self.tournament_ids),
            "season_ids": list(self.season_ids),
            "preferred_season_ids": list(self.preferred_season_ids),
            "event_ids": list(self.event_ids),
            "team_ids": list(self.team_ids),
            "player_ids": list(self.player_ids),
            "manager_ids": list(self.manager_ids),
            "period_ids": list(self.period_ids),
            "round_numbers": list(self.round_numbers),
            "successes": [asdict(item) for item in self.successes],
            "failures": list(self.failures),
            "skipped_urls": list(self.skipped_urls),
            "probed_url_count": len(self.probed_urls),
            "success_count": len(self.successes),
            "failure_count": len(self.failures),
            "empty_200_count": sum(1 for item in self.successes if item.empty_200),
            "non_empty_200_count": sum(1 for item in self.successes if not item.empty_200),
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe Sofascore multisport endpoints and generate markdown reports for HTTP 200 responses.",
    )
    parser.add_argument(
        "--date",
        default=DEFAULT_DATE,
        help="Date to use for date-scoped endpoints (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--tz-offset",
        default=DEFAULT_TZ_OFFSET,
        help="Timezone offset in seconds used by daily categories endpoints.",
    )
    parser.add_argument(
        "--country-code",
        default=DEFAULT_COUNTRY,
        help="Country code for default-unique-tournaments endpoints.",
    )
    parser.add_argument(
        "--lang",
        default=DEFAULT_LANG,
        help="Language slug for localized content endpoints.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=25.0,
        help="Probe timeout in seconds.",
    )
    parser.add_argument(
        "--outdir",
        default=str(PROJECT_ROOT / "reports" / "multisport_probe_2026_04_15"),
        help="Directory where probe reports will be written.",
    )
    parser.add_argument(
        "--sport",
        action="append",
        default=[],
        help="Optional sport slug to probe. Can be passed multiple times.",
    )
    parser.add_argument(
        "--limit-sports",
        type=int,
        default=None,
        help="Optional cap on the number of sports to probe after discovery.",
    )
    parser.add_argument(
        "--summary-name",
        default="probe_summary",
        help="Base filename for JSON/Markdown summaries.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=2,
        help="Transport retry attempts for probe fetches.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.outdir)
    output_dir.mkdir(parents=True, exist_ok=True)
    runtime_config = load_runtime_config(max_attempts=args.max_attempts)

    sports = args.sport or discover_sports(runtime_config=runtime_config, timeout=args.timeout)
    if args.limit_sports is not None:
        sports = sports[: args.limit_sports]

    all_contexts: list[SportProbeContext] = []
    for sport in sports:
        context = SportProbeContext(sport=sport)
        probe_sport(
            context=context,
            output_dir=output_dir,
            runtime_config=runtime_config,
            timeout=args.timeout,
            date=args.date,
            tz_offset=args.tz_offset,
            country_code=args.country_code,
            lang=args.lang,
        )
        all_contexts.append(context)

    write_summaries(
        contexts=all_contexts,
        output_dir=output_dir,
        summary_name=args.summary_name,
        date=args.date,
        tz_offset=args.tz_offset,
        country_code=args.country_code,
        lang=args.lang,
        max_attempts=args.max_attempts,
    )
    print(output_dir)
    return 0


def discover_sports(*, runtime_config: RuntimeConfig, timeout: float) -> list[str]:
    result = fetch_json_sync(EVENT_COUNT_URL, timeout=timeout, runtime_config=runtime_config)
    payload = result.payload
    if isinstance(payload, dict):
        if "eventCount" in payload and isinstance(payload["eventCount"], dict):
            candidates = payload["eventCount"]
        else:
            candidates = payload
        sports = [
            key
            for key, value in candidates.items()
            if isinstance(key, str) and isinstance(value, dict) and {"live", "total"} & set(value.keys())
        ]
        return sorted(dict.fromkeys(sports))
    raise RuntimeError(f"Unexpected event-count payload shape: {type(payload)!r}")


def probe_sport(
    *,
    context: SportProbeContext,
    output_dir: Path,
    runtime_config: RuntimeConfig,
    timeout: float,
    date: str,
    tz_offset: str,
    country_code: str,
    lang: str,
) -> None:
    sport = context.sport
    initial_urls = [
        f"https://www.sofascore.com/api/v1/sport/{sport}/categories/all",
        f"https://www.sofascore.com/api/v1/sport/{sport}/{date}/{tz_offset}/categories",
        f"https://www.sofascore.com/api/v1/sport/{sport}/scheduled-tournaments/{date}/page/1",
        f"https://www.sofascore.com/api/v1/sport/{sport}/scheduled-events/{date}",
        f"https://www.sofascore.com/api/v1/sport/{sport}/events/live",
        f"https://www.sofascore.com/api/v1/sport/{sport}/trending-top-players",
        f"https://www.sofascore.com/api/v1/config/default-unique-tournaments/{country_code}/{sport}",
    ]
    if sport == "tennis":
        initial_urls.append("https://www.sofascore.com/api/v1/category/-101/unique-tournaments")

    for url in initial_urls:
        probe_url(
            context=context,
            url=url,
            output_dir=output_dir,
            runtime_config=runtime_config,
            timeout=timeout,
        )

    category_id = context.preferred_first("preferred_category_ids", "category_ids")
    if category_id is not None:
        probe_url(
            context=context,
            url=f"https://www.sofascore.com/api/v1/category/{category_id}/unique-tournaments",
            output_dir=output_dir,
            runtime_config=runtime_config,
            timeout=timeout,
        )

    unique_tournament_id = context.first("unique_tournament_ids")
    if unique_tournament_id is not None:
        for url in [
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/seasons",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/featured-events",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/scheduled-events/{date}",
        ]:
            probe_url(
                context=context,
                url=url,
                output_dir=output_dir,
                runtime_config=runtime_config,
                timeout=timeout,
            )

    season_id = context.preferred_first("preferred_season_ids", "season_ids")
    unique_tournament_id = context.first("unique_tournament_ids")
    tournament_id = context.first("tournament_ids")
    if unique_tournament_id is not None and season_id is not None:
        season_urls = [
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/info",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/total",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/home",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/away",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/info",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics?limit=20&offset=0&order=rating&accumulation=total&group=summary",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players-per-game/all/overall",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-ratings/overall",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/overall",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/venues",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/groups",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season-race",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-statistics/types",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-statistics/types",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/total",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/home",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/away",
            f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-of-the-week/periods",
        ]
        if tournament_id is not None:
            season_urls.append(
                f"https://www.sofascore.com/api/v1/tournament/{tournament_id}/season/{season_id}/standings/total"
            )
        for url in season_urls:
            probe_url(
                context=context,
                url=url,
                output_dir=output_dir,
                runtime_config=runtime_config,
                timeout=timeout,
            )

    period_id = context.first("period_ids")
    if unique_tournament_id is not None and season_id is not None and period_id is not None:
        probe_url(
            context=context,
            url=f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-of-the-week/{period_id}",
            output_dir=output_dir,
            runtime_config=runtime_config,
            timeout=timeout,
        )

    round_number = context.first("round_numbers")
    if unique_tournament_id is not None and season_id is not None and round_number is not None:
        probe_url(
            context=context,
            url=f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round_number}",
            output_dir=output_dir,
            runtime_config=runtime_config,
            timeout=timeout,
        )

    event_id = context.first("event_ids")
    team_id = context.first("team_ids")
    player_id = context.first("player_ids")
    if event_id is not None:
        event_urls = [
            f"https://www.sofascore.com/api/v1/event/{event_id}",
            f"https://www.sofascore.com/api/v1/event/{event_id}/statistics",
            f"https://api.sofascore.com/api/v1/event/{event_id}/statistics",
            f"https://www.sofascore.com/api/v1/event/{event_id}/lineups",
            f"https://www.sofascore.com/api/v1/event/{event_id}/managers",
            f"https://www.sofascore.com/api/v1/event/{event_id}/h2h",
            f"https://www.sofascore.com/api/v1/event/{event_id}/pregame-form",
            f"https://www.sofascore.com/api/v1/event/{event_id}/votes",
            f"https://www.sofascore.com/api/v1/event/{event_id}/comments",
            f"https://www.sofascore.com/api/v1/event/{event_id}/graph",
            f"https://www.sofascore.com/api/v1/event/{event_id}/odds/1/all",
            f"https://www.sofascore.com/api/v1/event/{event_id}/odds/1/featured",
            f"https://www.sofascore.com/api/v1/event/{event_id}/provider/1/winning-odds",
            f"https://www.sofascore.com/api/v1/event/{event_id}/team-streaks/betting-odds/1",
        ]
        if sport == "tennis":
            event_urls.extend(
                [
                    f"https://www.sofascore.com/api/v1/event/{event_id}/point-by-point",
                    f"https://www.sofascore.com/api/v1/event/{event_id}/tennis-power",
                ]
            )
        if team_id is not None:
            event_urls.append(f"https://www.sofascore.com/api/v1/event/{event_id}/heatmap/{team_id}")
        if player_id is not None:
            event_urls.extend(
                [
                    f"https://www.sofascore.com/api/v1/event/{event_id}/player/{player_id}/statistics",
                    f"https://www.sofascore.com/api/v1/event/{event_id}/player/{player_id}/heatmap",
                ]
            )
        for url in event_urls:
            probe_url(
                context=context,
                url=url,
                output_dir=output_dir,
                runtime_config=runtime_config,
                timeout=timeout,
            )

    team_id = context.first("team_ids")
    if team_id is not None:
        team_urls = [
            f"https://www.sofascore.com/api/v1/team/{team_id}",
            f"https://www.sofascore.com/api/v1/team/{team_id}/team-statistics/seasons",
            f"https://www.sofascore.com/api/v1/team/{team_id}/player-statistics/seasons",
            f"https://www.sofascore.com/api/v1/team/{team_id}/events/last/0",
            f"https://www.sofascore.com/api/v1/team/{team_id}/standings-seasons",
        ]
        if unique_tournament_id is not None and season_id is not None:
            team_urls.extend(
                [
                    f"https://www.sofascore.com/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall",
                    f"https://www.sofascore.com/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team/{team_id}/team-performance-graph-data",
                    f"https://www.sofascore.com/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall",
                ]
            )
        for url in team_urls:
            probe_url(
                context=context,
                url=url,
                output_dir=output_dir,
                runtime_config=runtime_config,
                timeout=timeout,
            )

    player_id = context.first("player_ids")
    if player_id is not None:
        player_urls = [
            f"https://www.sofascore.com/api/v1/player/{player_id}",
            f"https://www.sofascore.com/api/v1/player/{player_id}/statistics",
            f"https://www.sofascore.com/api/v1/player/{player_id}/statistics/seasons",
            f"https://www.sofascore.com/api/v1/player/{player_id}/transfer-history",
            f"https://www.sofascore.com/api/v1/player/{player_id}/national-team-statistics",
            f"https://www.sofascore.com/api/v1/player/{player_id}/events/last/0",
        ]
        if unique_tournament_id is not None and season_id is not None:
            player_urls.extend(
                [
                    f"https://www.sofascore.com/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall",
                    f"https://www.sofascore.com/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/heatmap/overall",
                ]
            )
        for url in player_urls:
            probe_url(
                context=context,
                url=url,
                output_dir=output_dir,
                runtime_config=runtime_config,
                timeout=timeout,
            )

    manager_id = context.first("manager_ids")
    if manager_id is not None:
        for url in [
            f"https://www.sofascore.com/api/v1/manager/{manager_id}",
            f"https://www.sofascore.com/api/v1/manager/{manager_id}/career-history",
            f"https://www.sofascore.com/api/v1/manager/{manager_id}/events/last/0",
            f"https://www.sofascore.com/api/v1/seo/content/manager/{manager_id}/{lang}",
        ]:
            probe_url(
                context=context,
                url=url,
                output_dir=output_dir,
                runtime_config=runtime_config,
                timeout=timeout,
            )


def probe_url(
    *,
    context: SportProbeContext,
    url: str,
    output_dir: Path,
    runtime_config: RuntimeConfig,
    timeout: float,
) -> None:
    if url in context.probed_urls:
        return
    context.probed_urls.add(url)

    try:
        result = fetch_json_sync(url, timeout=timeout, runtime_config=runtime_config)
    except FetchJsonError as exc:
        context.failures.append({"url": url, "error": str(exc)})
        return
    except Exception as exc:  # pragma: no cover - defensive
        context.failures.append({"url": url, "error": repr(exc)})
        return

    extract_ids(result.payload, context=context)
    empty_200, empty_reason = classify_empty_payload(result.payload)
    report_path, report_mode, note = write_report_for_200(
        url=url,
        output_dir=output_dir,
        timeout=timeout,
        fetch_result=result,
    )
    context.successes.append(
        ProbeRecord(
            url=url,
            status_code=result.status_code,
            empty_200=empty_200,
            empty_reason=empty_reason,
            report_path=report_path,
            report_mode=report_mode,
            note=note,
        )
    )


def write_report_for_200(
    *,
    url: str,
    output_dir: Path,
    timeout: float,
    fetch_result: FetchResult,
) -> tuple[str | None, str | None, str | None]:
    command = [
        sys.executable,
        str(INSPECT_API_PATH),
        url,
        "--outdir",
        str(output_dir),
        "--timeout",
        str(timeout),
    ]
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode == 0:
        report_path = completed.stdout.strip().splitlines()[-1] if completed.stdout.strip() else None
        return report_path, "inspect_api", None

    root = infer_schema(fetch_result.payload)
    markdown = build_markdown_report(fetch_result, root)
    fallback_path = output_dir / report_filename(url)
    fallback_path.write_text(markdown, encoding="utf-8")
    note = completed.stderr.strip() or completed.stdout.strip() or "inspect_api failed, used inline fallback"
    return str(fallback_path), "inline_fallback", note


def classify_empty_payload(payload: Any) -> tuple[bool, str | None]:
    if payload is None:
        return True, "payload is null"
    if isinstance(payload, list):
        return (len(payload) == 0, "root list is empty" if len(payload) == 0 else None)
    if isinstance(payload, dict):
        if not payload:
            return True, "root object has no keys"
        collection_keys = []
        non_empty_collections = []
        for key, value in payload.items():
            if isinstance(value, (list, dict)):
                collection_keys.append(key)
                if has_deep_content(value):
                    non_empty_collections.append(key)
        if collection_keys and not non_empty_collections:
            return True, f"collections empty: {', '.join(collection_keys)}"
        if not has_deep_content(payload):
            return True, "no nested content"
    return False, None


def has_deep_content(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, list):
        return any(has_deep_content(item) for item in value)
    if isinstance(value, dict):
        return any(has_deep_content(item) for item in value.values())
    return True


def extract_ids(payload: Any, *, context: SportProbeContext, path: str = "root") -> None:
    if isinstance(payload, dict):
        object_id = payload.get("id")
        last_segment = path.split(".")[-1].replace("[]", "")
        is_archive_seasons_list = ".seasons[]" in path
        if isinstance(object_id, int):
            if last_segment in {"category"}:
                context.add_value("category_ids", object_id)
                if ".categories[]" not in path:
                    context.add_value("preferred_category_ids", object_id)
            elif last_segment in {"uniqueTournament"}:
                context.add_value("unique_tournament_ids", object_id)
            elif last_segment in {"tournament"}:
                context.add_value("tournament_ids", object_id)
            elif last_segment in {"season"}:
                context.add_value("season_ids", object_id)
                if not is_archive_seasons_list:
                    context.add_value("preferred_season_ids", object_id)
            elif "manager" in last_segment.lower():
                context.add_value("manager_ids", object_id)
            elif last_segment in {"player", "homePlayer", "awayPlayer"}:
                context.add_value("player_ids", object_id)
            elif last_segment in {"team", "homeTeam", "awayTeam", "winnerTeam", "loserTeam"}:
                context.add_value("team_ids", object_id)
            elif last_segment in {"event"}:
                context.add_value("event_ids", object_id)
            elif last_segment in {"period"}:
                context.add_value("period_ids", object_id)

        if isinstance(payload.get("round"), int) and "round" in last_segment.lower():
            context.add_value("round_numbers", payload["round"])

        if isinstance(object_id, int):
            keys = set(payload.keys())
            if {"homeTeam", "awayTeam"} <= keys or (
                {"homeTeam", "awayTeam"} & keys and {"startTimestamp", "tournament"} & keys
            ):
                context.add_value("event_ids", object_id)
            if "teamColors" in keys and "name" in keys and "slug" in keys:
                context.add_value("team_ids", object_id)
            if "position" in keys and "name" in keys and "slug" in keys:
                context.add_value("player_ids", object_id)
            if "category" in keys and "name" in keys and "slug" in keys and "userCount" in keys:
                context.add_value("unique_tournament_ids", object_id)
            if "uniqueTournament" in keys and "season" in keys and "category" in keys:
                context.add_value("tournament_ids", object_id)
            if "year" in keys and "name" in keys:
                context.add_value("season_ids", object_id)
                if not is_archive_seasons_list:
                    context.add_value("preferred_season_ids", object_id)
            if "flag" in keys and "priority" in keys and "sport" in keys:
                context.add_value("category_ids", object_id)
                if ".categories[]" not in path:
                    context.add_value("preferred_category_ids", object_id)

        round_info = payload.get("roundInfo")
        if isinstance(round_info, dict) and isinstance(round_info.get("round"), int):
            context.add_value("round_numbers", round_info["round"])

        for key, value in payload.items():
            child_path = f"{path}.{key}"
            extract_ids(value, context=context, path=child_path)
        return

    if isinstance(payload, list):
        for item in payload:
            extract_ids(item, context=context, path=f"{path}[]")


def write_summaries(
    *,
    contexts: list[SportProbeContext],
    output_dir: Path,
    summary_name: str,
    date: str,
    tz_offset: str,
    country_code: str,
    lang: str,
    max_attempts: int,
) -> None:
    summary = {
        "date": date,
        "tz_offset": tz_offset,
        "country_code": country_code,
        "lang": lang,
        "max_attempts": max_attempts,
        "sports": [context.snapshot() for context in contexts],
        "totals": {
            "sports": len(contexts),
            "successes": sum(len(context.successes) for context in contexts),
            "failures": sum(len(context.failures) for context in contexts),
            "empty_200": sum(1 for context in contexts for item in context.successes if item.empty_200),
            "non_empty_200": sum(1 for context in contexts for item in context.successes if not item.empty_200),
        },
    }
    json_path = output_dir / f"{summary_name}.json"
    json_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    lines = [
        "# Multisport Probe Summary",
        "",
        f"- Date: `{date}`",
        f"- TZ Offset: `{tz_offset}`",
        f"- Country Code: `{country_code}`",
        f"- Language: `{lang}`",
        f"- Probe Max Attempts: `{max_attempts}`",
        f"- Sports Probed: `{summary['totals']['sports']}`",
        f"- 200 Responses: `{summary['totals']['successes']}`",
        f"- Empty 200 Responses: `{summary['totals']['empty_200']}`",
        f"- Non-empty 200 Responses: `{summary['totals']['non_empty_200']}`",
        f"- Failures: `{summary['totals']['failures']}`",
        "",
    ]

    for context in contexts:
        empty_200 = sum(1 for item in context.successes if item.empty_200)
        non_empty_200 = sum(1 for item in context.successes if not item.empty_200)
        lines.extend(
            [
                f"## {context.sport}",
                "",
                f"- 200 Responses: `{len(context.successes)}`",
                f"- Empty 200: `{empty_200}`",
                f"- Non-empty 200: `{non_empty_200}`",
                f"- Failures: `{len(context.failures)}`",
                f"- category_ids: `{context.category_ids}`",
                f"- preferred_category_ids: `{context.preferred_category_ids}`",
                f"- unique_tournament_ids: `{context.unique_tournament_ids}`",
                f"- tournament_ids: `{context.tournament_ids}`",
                f"- season_ids: `{context.season_ids}`",
                f"- preferred_season_ids: `{context.preferred_season_ids}`",
                f"- event_ids: `{context.event_ids}`",
                f"- team_ids: `{context.team_ids}`",
                f"- player_ids: `{context.player_ids}`",
                f"- manager_ids: `{context.manager_ids}`",
                f"- round_numbers: `{context.round_numbers}`",
                f"- period_ids: `{context.period_ids}`",
                "",
                "### 200 URLs",
                "",
            ]
        )
        for record in context.successes:
            empty_label = "empty-200" if record.empty_200 else "non-empty-200"
            report_label = record.report_path or "-"
            suffix = f"; reason: `{record.empty_reason}`" if record.empty_reason else ""
            mode = record.report_mode or "-"
            lines.append(
                f"- `{empty_label}` `{record.url}` -> `{report_label}` via `{mode}`{suffix}"
            )
        if not context.successes:
            lines.append("- None")
        lines.extend(["", "### Failures", ""])
        for failure in context.failures:
            lines.append(f"- `{failure['url']}` -> `{failure['error']}`")
        if not context.failures:
            lines.append("- None")
        lines.append("")

    markdown_path = output_dir / f"{summary_name}.md"
    markdown_path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
