"""Compatibility wrapper for the unified hybrid full-backfill runner."""

from __future__ import annotations

import sys

from .cli import main as hybrid_cli_main


def main() -> int:
    return hybrid_cli_main(["full-backfill", *sys.argv[1:]])


if __name__ == "__main__":
    raise SystemExit(main())
