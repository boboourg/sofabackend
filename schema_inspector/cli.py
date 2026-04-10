"""CLI entry point."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from .runtime import load_runtime_config
from .service import inspect_url_to_markdown


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch a JSON API URL and generate a Markdown schema report.",
    )
    parser.add_argument("url", help="HTTP(S) or file URL that returns JSON")
    parser.add_argument(
        "--outdir",
        default="reports",
        help="Directory where the Markdown report will be written",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="Request timeout in seconds",
    )
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        help="Optional request header in KEY=VALUE form. Can be passed multiple times.",
    )
    parser.add_argument(
        "--proxy",
        action="append",
        default=[],
        help="Optional proxy URL. Can be passed multiple times.",
    )
    parser.add_argument(
        "--user-agent",
        default=None,
        help="Override User-Agent for the transport layer.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=None,
        help="Override retry attempts for the transport layer.",
    )
    args = parser.parse_args()

    headers = parse_headers(args.header)
    runtime_config = load_runtime_config(
        proxy_urls=args.proxy,
        user_agent=args.user_agent,
        extra_headers=headers,
        max_attempts=args.max_attempts,
    )
    report_path = asyncio.run(
        inspect_url_to_markdown(
            args.url,
            output_dir=Path(args.outdir),
            headers=headers,
            timeout=args.timeout,
            runtime_config=runtime_config,
        )
    )
    print(report_path)
    return 0


def parse_headers(items: list[str]) -> dict[str, str]:
    headers = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Header must be KEY=VALUE: {item}")
        key, value = item.split("=", 1)
        headers[key.strip()] = value.strip()
    return headers


if __name__ == "__main__":
    raise SystemExit(main())
