from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from schema_inspector.endpoints import (
    UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT,
    season_cuptrees_endpoint,
    season_rounds_endpoint,
)
from schema_inspector.runtime import load_runtime_config
from schema_inspector.sofascore_client import SofascoreClient, SofascoreHttpError


SAMPLES = (
    {"unique_tournament_id": 336, "season_id": 80287, "sport_slug": "football"},
    {"unique_tournament_id": 8, "season_id": 77559, "sport_slug": "football"},
)


async def _fetch_json(client: SofascoreClient, url: str) -> tuple[int, object | None]:
    try:
        response = await client.get_json(url, timeout=30.0)
    except SofascoreHttpError as exc:
        status_code = exc.transport_result.status_code if exc.transport_result is not None else None
        return (int(status_code) if status_code is not None else -1, None)
    return response.status_code, response.payload


async def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    output_dir = PROJECT_ROOT / "reports" / "upstream_samples"
    output_dir.mkdir(parents=True, exist_ok=True)

    runtime = load_runtime_config()
    client = SofascoreClient(runtime)

    summaries: list[dict[str, object]] = []
    for sample in SAMPLES:
        unique_tournament_id = int(sample["unique_tournament_id"])
        season_id = int(sample["season_id"])

        rounds_url = season_rounds_endpoint().build_url(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
        )
        rounds_status, rounds_payload = await _fetch_json(client, rounds_url)
        round_number = None
        if isinstance(rounds_payload, dict):
            current_round = rounds_payload.get("currentRound")
            if isinstance(current_round, dict):
                round_number = current_round.get("round")
            if round_number is None:
                rounds = rounds_payload.get("rounds")
                if isinstance(rounds, list) and rounds:
                    maybe_last = rounds[-1]
                    if isinstance(maybe_last, dict):
                        round_number = maybe_last.get("round")
            rounds_path = output_dir / f"ut{unique_tournament_id}_season{season_id}_rounds.json"
            rounds_path.write_text(json.dumps(rounds_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            rounds_path = None

        cuptrees_url = season_cuptrees_endpoint().build_url(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
        )
        cuptrees_status, cuptrees_payload = await _fetch_json(client, cuptrees_url)
        if isinstance(cuptrees_payload, dict):
            cuptrees_path = output_dir / f"ut{unique_tournament_id}_season{season_id}_cuptrees.json"
            cuptrees_path.write_text(json.dumps(cuptrees_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            cuptrees_path = None

        round_events_status = None
        round_events_path = None
        if round_number is not None:
            round_events_url = UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT.build_url(
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                round_number=round_number,
            )
            round_events_status, round_events_payload = await _fetch_json(client, round_events_url)
            if isinstance(round_events_payload, dict):
                round_events_path = output_dir / (
                    f"ut{unique_tournament_id}_season{season_id}_events_round_{int(round_number)}.json"
                )
                round_events_path.write_text(
                    json.dumps(round_events_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

        summaries.append(
            {
                "unique_tournament_id": unique_tournament_id,
                "season_id": season_id,
                "rounds_status": rounds_status,
                "round_number_probe": round_number,
                "rounds_file": str(rounds_path) if rounds_path is not None else None,
                "cuptrees_status": cuptrees_status,
                "cuptrees_file": str(cuptrees_path) if cuptrees_path is not None else None,
                "round_events_status": round_events_status,
                "round_events_file": str(round_events_path) if round_events_path is not None else None,
            }
        )

    print(json.dumps({"samples": summaries}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
