#!/usr/bin/env python3
"""Probe proxy pools and print which proxies are healthy or dead."""

from __future__ import annotations

import argparse
import asyncio

from schema_inspector.proxy_health import DEFAULT_HEALTH_URL, load_pool_runtime_config, probe_proxy_endpoint


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pool",
        choices=("live", "historical", "structure"),
        default="live",
        help="Which configured proxy pool to probe.",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_HEALTH_URL,
        help="HTTPS URL fetched through each proxy.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Per-proxy timeout in seconds.",
    )
    return parser.parse_args()


async def _run(pool: str, url: str, timeout: float) -> int:
    config = load_pool_runtime_config(pool)
    if not config.proxy_endpoints:
        print(f"pool={pool} total=0 healthy=0 dead=0 note=no_proxies_configured")
        return 1

    results = []
    for endpoint in config.proxy_endpoints:
        result = await probe_proxy_endpoint(
            endpoint=endpoint,
            base_config=config,
            url=url,
            timeout=timeout,
            pool=pool,
        )
        results.append(result)
        print(
            f"pool={result.pool} proxy={result.proxy_name} verdict={result.verdict} "
            f"status={result.status_code} challenge={result.challenge_reason or '-'} "
            f"attempts={result.attempts} elapsed={result.elapsed_seconds:.2f}s "
            f"error={result.error or '-'} url={result.proxy_url}"
        )

    healthy = [item.proxy_name for item in results if item.verdict == "healthy"]
    dead = [item.proxy_name for item in results if item.verdict != "healthy"]
    print(
        f"summary pool={pool} total={len(results)} healthy={len(healthy)} dead={len(dead)} "
        f"dead_list={','.join(dead) if dead else '-'}"
    )
    return 0 if not dead else 1


def main() -> int:
    args = _parse_args()
    return asyncio.run(_run(args.pool, args.url, args.timeout))


if __name__ == "__main__":
    raise SystemExit(main())
