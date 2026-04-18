from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from urllib.request import urlopen


DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_LOG_PATHS = (
    "/opt/sofascore/logs/hydrate-1.log",
    "/opt/sofascore/logs/hydrate-2.log",
    "/opt/sofascore/logs/hydrate-3.log",
    "/opt/sofascore/logs/historical-hydrate-1.log",
    "/opt/sofascore/logs/historical-hydrate-2.log",
)
REQUIRED_STREAMS = (
    "stream:etl:discovery",
    "stream:etl:live_discovery",
    "stream:etl:hydrate",
    "stream:etl:live_hot",
    "stream:etl:historical_hydrate",
)


class ReadinessReport:
    def __init__(self, *, ready: bool, reasons: list[str]) -> None:
        self.ready = bool(ready)
        self.reasons = list(reasons)


def fetch_json(base_url: str, path: str) -> dict[str, Any]:
    normalized = base_url.rstrip("/") + path
    with urlopen(normalized) as response:
        return json.load(response)


def scan_logs_for_timeout(log_paths: tuple[str, ...]) -> bool:
    for raw_path in log_paths:
        path = Path(raw_path)
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if "TimeoutError" in text:
            return True
    return False


def evaluate_readiness(
    *,
    queue_summary: dict[str, Any],
    job_runs_payload: dict[str, Any],
    timeout_detected: bool,
) -> ReadinessReport:
    streams = {
        str(item["stream"]): item
        for item in queue_summary.get("streams", ())
        if isinstance(item, dict) and item.get("stream")
    }
    reasons: list[str] = []

    missing = [name for name in REQUIRED_STREAMS if name not in streams]
    if missing:
        reasons.append(f"missing required streams: {', '.join(missing)}")

    discovery_lag = _stream_lag(streams, "stream:etl:discovery")
    if discovery_lag not in (None, 0):
        reasons.append(f"discovery lag is not drained: {discovery_lag}")

    live_discovery_lag = _stream_lag(streams, "stream:etl:live_discovery")
    if live_discovery_lag not in (None, 0):
        reasons.append(f"live_discovery lag is not drained: {live_discovery_lag}")

    if timeout_detected:
        reasons.append("timeout signature detected in hydrate logs")

    recent_failed = [
        item
        for item in job_runs_payload.get("jobRuns", ())
        if isinstance(item, dict) and str(item.get("status")) == "failed"
    ]
    if recent_failed:
        reasons.append(f"recent failed jobs detected: {len(recent_failed)}")

    return ReadinessReport(ready=not reasons, reasons=reasons)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check production readiness for the live runtime.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Base URL for the local ops API.")
    parser.add_argument(
        "--log-path",
        action="append",
        default=[],
        help="Optional repeatable log path override. Defaults to hydrate and historical-hydrate logs.",
    )
    parser.add_argument("--jobs-limit", type=int, default=30, help="Recent job runs to inspect.")
    args = parser.parse_args(argv)

    queue_summary = fetch_json(args.base_url, "/ops/queues/summary")
    job_runs_payload = fetch_json(args.base_url, f"/ops/jobs/runs?limit={max(1, int(args.jobs_limit))}")
    log_paths = tuple(str(item) for item in (args.log_path or ())) or DEFAULT_LOG_PATHS
    timeout_detected = scan_logs_for_timeout(log_paths)
    report = evaluate_readiness(
        queue_summary=queue_summary,
        job_runs_payload=job_runs_payload,
        timeout_detected=timeout_detected,
    )

    print(f"readiness base_url={args.base_url}")
    for stream_name in REQUIRED_STREAMS:
        item = next((row for row in queue_summary.get("streams", ()) if row.get("stream") == stream_name), None)
        if item is None:
            print(f"{stream_name} missing=1")
            continue
        print(
            f"{stream_name} lag={item.get('lag')} entries_read={item.get('entries_read')} length={item.get('length')}"
        )
    print(
        "jobs "
        f"failed={job_runs_payload.get('status_counts', {}).get('failed')} "
        f"retry_scheduled={job_runs_payload.get('status_counts', {}).get('retry_scheduled')} "
        f"succeeded={job_runs_payload.get('status_counts', {}).get('succeeded')}"
    )
    print(f"timeouts detected={int(timeout_detected)}")

    if report.ready:
        print("READY")
        return 0

    print("NOT_READY")
    for reason in report.reasons:
        print(f"- {reason}")
    return 1


def _stream_lag(streams: dict[str, dict[str, Any]], stream_name: str) -> int | None:
    item = streams.get(stream_name)
    if item is None:
        return None
    lag = item.get("lag")
    if lag is None:
        return None
    return int(lag)


if __name__ == "__main__":
    raise SystemExit(main())
