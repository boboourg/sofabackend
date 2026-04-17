"""CLI entrypoint for the hybrid ETL pilot orchestrator."""

from __future__ import annotations

import argparse
import asyncio
import json


async def _run() -> int:
    parser = argparse.ArgumentParser(description="Pilot hybrid ETL orchestrator")
    parser.add_argument("--sport", required=True)
    parser.add_argument("--event-id", required=True, type=int)
    args = parser.parse_args()
    print(json.dumps({"sport": args.sport, "event_id": args.event_id, "status": "pilot_cli_placeholder"}))
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
