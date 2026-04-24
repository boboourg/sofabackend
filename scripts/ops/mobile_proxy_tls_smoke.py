#!/usr/bin/env python3
"""TLS-aware smoke probe for the temporary historical mobile proxy."""

from __future__ import annotations

import argparse
import asyncio
import time

from schema_inspector.runtime import load_runtime_config
from schema_inspector.transport import InspectorTransport


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default="https://www.sofascore.com/api/v1/event/14149626",
        help="Smoke URL fetched through InspectorTransport.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=25.0,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--proxy-env-key",
        default="SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS",
        help="Environment key used to build the proxy pool.",
    )
    return parser.parse_args()


async def _run(url: str, timeout: float, proxy_env_key: str) -> int:
    config = load_runtime_config(proxy_env_key=proxy_env_key)
    transport = InspectorTransport(config)
    started = time.monotonic()
    try:
        result = await transport.fetch(url, timeout=timeout)
    except Exception as exc:
        elapsed = time.monotonic() - started
        print(f"smoke=error elapsed={elapsed:.2f}s error={exc}")
        return 1
    finally:
        await transport.close()

    elapsed = time.monotonic() - started
    print(
        "smoke=status "
        f"status={result.status_code} "
        f"challenge={result.challenge_reason} "
        f"proxy={result.final_proxy_name} "
        f"attempts={len(result.attempts)} "
        f"elapsed={elapsed:.2f}s"
    )
    for attempt in result.attempts:
        print(
            "attempt="
            f"{attempt.attempt_number} "
            f"proxy={attempt.proxy_name} "
            f"status={attempt.status_code} "
            f"challenge={attempt.challenge_reason} "
            f"error={attempt.error}"
        )

    if result.status_code == 200 and result.challenge_reason is None:
        return 0
    return 1


def main() -> int:
    args = _parse_args()
    return asyncio.run(_run(args.url, args.timeout, args.proxy_env_key))


if __name__ == "__main__":
    raise SystemExit(main())
