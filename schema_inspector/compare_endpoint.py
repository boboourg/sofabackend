"""Diff a local API endpoint against the upstream Sofascore endpoint.

Designed for the iterative "is our /api/v1/... 1:1 with sofascore.com?"
workflow. Fetches both URLs, walks the payloads recursively and reports
missing / extra / common leaf keypaths plus a byte-size ratio.

Typical usage::

    # Both URLs explicit:
    python -m schema_inspector.compare_endpoint \\
      --upstream https://www.sofascore.com/api/v1/unique-tournament/8/season/77559/events/last/0 \\
      --local    http://127.0.0.1:8000/api/v1/unique-tournament/8/season/77559/events/last/0

    # Common case: same path on both sides, just give the path once:
    python -m schema_inspector.compare_endpoint \\
      --path /api/v1/unique-tournament/8/season/77559/events/last/0

The local URL goes through a plain HTTP GET (no proxy, no transport),
the upstream URL goes through the project's ``InspectorTransport`` so
it benefits from the configured proxy pool / auth / TLS profile.

Exit code:
  0 — no missing keypaths in local (it's a shape-superset of upstream).
  1 — local is missing at least one keypath upstream produced.
  2 — fetch error (transport / HTTP / JSON).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from typing import Any
from urllib.request import urlopen
from urllib.error import URLError

from .runtime import _load_project_env, load_runtime_config
from .transport import InspectorTransport


DEFAULT_LOCAL_BASE = "http://127.0.0.1:8000"
DEFAULT_UPSTREAM_BASE = "https://www.sofascore.com"


@dataclass(frozen=True)
class FetchedPayload:
    url: str
    status: int
    bytes_total: int
    payload: Any  # parsed JSON (None on error)
    error: str | None = None


@dataclass(frozen=True)
class DiffReport:
    upstream: FetchedPayload
    local: FetchedPayload
    upstream_paths: dict[str, str]
    local_paths: dict[str, str]
    missing_in_local: list[str]
    extra_in_local: list[str]
    common: list[str]


# ---------------------------------------------------------------------------
# Payload walking
# ---------------------------------------------------------------------------


def collect_shape_keypaths(obj: Any, prefix: str = "", out: dict[str, str] | None = None) -> dict[str, str]:
    """Walk ``obj`` and emit a flat ``{keypath: type_name}`` map.

    Lists are treated as homogeneous: only ``obj[0]`` is descended into and
    the index segment is rendered as ``[*]`` (e.g. ``events[*].homeScore``).
    Empty containers map to ``(empty dict)`` / ``(empty list)`` so the diff
    can still tell them apart from "key absent".
    """

    if out is None:
        out = {}
    if isinstance(obj, dict):
        if not obj:
            out[prefix or "<root>"] = "(empty dict)"
            return out
        for key, value in obj.items():
            child_prefix = f"{prefix}.{key}" if prefix else str(key)
            collect_shape_keypaths(value, child_prefix, out)
        return out
    if isinstance(obj, list):
        if not obj:
            out[prefix or "<root>"] = "(empty list)"
            return out
        collect_shape_keypaths(obj[0], f"{prefix}[*]", out)
        return out
    # Leaf
    out[prefix or "<root>"] = type(obj).__name__
    return out


def diff_keypaths(upstream_paths: dict[str, str], local_paths: dict[str, str]) -> tuple[list[str], list[str], list[str]]:
    """Return (missing_in_local, extra_in_local, common_keys), sorted."""

    upstream_set = set(upstream_paths)
    local_set = set(local_paths)
    missing = sorted(upstream_set - local_set)
    extra = sorted(local_set - upstream_set)
    common = sorted(upstream_set & local_set)
    return missing, extra, common


# ---------------------------------------------------------------------------
# Fetchers
# ---------------------------------------------------------------------------


async def fetch_upstream(url: str, *, timeout: float = 20.0) -> FetchedPayload:
    _load_project_env()
    runtime_config = load_runtime_config()
    transport = InspectorTransport(runtime_config)
    try:
        result = await transport.fetch(url, timeout=timeout)
    except Exception as exc:
        return FetchedPayload(url=url, status=0, bytes_total=0, payload=None, error=str(exc))
    finally:
        try:
            await transport.close()
        except Exception:
            pass

    body = result.body_bytes or b""
    try:
        payload = json.loads(body) if body else None
        return FetchedPayload(
            url=url,
            status=int(result.status_code or 0),
            bytes_total=len(body),
            payload=payload,
        )
    except json.JSONDecodeError as exc:
        return FetchedPayload(
            url=url,
            status=int(result.status_code or 0),
            bytes_total=len(body),
            payload=None,
            error=f"upstream JSON decode error: {exc}",
        )


def fetch_local(url: str, *, timeout: float = 20.0) -> FetchedPayload:
    """Plain HTTP GET against the local API. No proxy."""

    try:
        with urlopen(url, timeout=timeout) as response:  # noqa: S310 -- local-only loopback URL
            body = response.read()
            status = int(response.status)
    except URLError as exc:
        # urlopen raises URLError both for transport-level failures and 4xx/5xx;
        # for the 4xx/5xx case the response body is on exc.fp.
        if hasattr(exc, "code") and exc.code is not None:
            try:
                body = exc.read()
            except Exception:
                body = b""
            status = int(exc.code)
        else:
            return FetchedPayload(url=url, status=0, bytes_total=0, payload=None, error=str(exc))
    except Exception as exc:
        return FetchedPayload(url=url, status=0, bytes_total=0, payload=None, error=str(exc))

    if not body:
        return FetchedPayload(url=url, status=status, bytes_total=0, payload=None)
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        return FetchedPayload(
            url=url,
            status=status,
            bytes_total=len(body),
            payload=None,
            error=f"local JSON decode error: {exc}",
        )
    return FetchedPayload(url=url, status=status, bytes_total=len(body), payload=payload)


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------


def build_report(upstream: FetchedPayload, local: FetchedPayload) -> DiffReport:
    upstream_paths = collect_shape_keypaths(upstream.payload) if upstream.payload is not None else {}
    local_paths = collect_shape_keypaths(local.payload) if local.payload is not None else {}
    missing, extra, common = diff_keypaths(upstream_paths, local_paths)
    return DiffReport(
        upstream=upstream,
        local=local,
        upstream_paths=upstream_paths,
        local_paths=local_paths,
        missing_in_local=missing,
        extra_in_local=extra,
        common=common,
    )


def format_report(report: DiffReport, *, max_paths: int = 80) -> str:
    out: list[str] = []
    up = report.upstream
    lo = report.local
    out.append("=== UPSTREAM ===")
    out.append(f"  url:    {up.url}")
    out.append(f"  status: {up.status}   bytes: {up.bytes_total}")
    if up.error:
        out.append(f"  error:  {up.error}")
    out.append("")
    out.append("=== LOCAL ===")
    out.append(f"  url:    {lo.url}")
    out.append(f"  status: {lo.status}   bytes: {lo.bytes_total}")
    if lo.error:
        out.append(f"  error:  {lo.error}")
    out.append("")

    if up.bytes_total > 0:
        ratio = (lo.bytes_total / up.bytes_total) * 100
        out.append(f"  size ratio (local / upstream): {ratio:.1f}%")
        out.append("")

    out.append(
        f"=== KEYPATHS  upstream={len(report.upstream_paths)}  "
        f"local={len(report.local_paths)}  common={len(report.common)} ==="
    )
    out.append("")
    out.append(f"--- MISSING in local ({len(report.missing_in_local)}) ---")
    if report.missing_in_local:
        for path in report.missing_in_local[:max_paths]:
            out.append(f"  {path:60s} :: {report.upstream_paths.get(path, '?')}")
        if len(report.missing_in_local) > max_paths:
            out.append(f"  ... +{len(report.missing_in_local) - max_paths} more")
    else:
        out.append("  (none)")
    out.append("")
    out.append(f"--- EXTRA in local ({len(report.extra_in_local)}) ---")
    if report.extra_in_local:
        for path in report.extra_in_local[:max_paths]:
            out.append(f"  {path:60s} :: {report.local_paths.get(path, '?')}")
        if len(report.extra_in_local) > max_paths:
            out.append(f"  ... +{len(report.extra_in_local) - max_paths} more")
    else:
        out.append("  (none)")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description=(
            "Compare a local API endpoint against the upstream Sofascore endpoint. "
            "Outputs missing/extra leaf keypaths and byte-size ratio."
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--path", help="Path shared by both URLs, e.g. /api/v1/event/12345.")
    group.add_argument(
        "--upstream",
        help="Full upstream URL. Requires --local too.",
    )
    parser.add_argument("--local", help="Full local URL (only when --upstream is set).")
    parser.add_argument(
        "--local-base",
        default=DEFAULT_LOCAL_BASE,
        help=f"Base for the local URL when --path is used (default {DEFAULT_LOCAL_BASE}).",
    )
    parser.add_argument(
        "--upstream-base",
        default=DEFAULT_UPSTREAM_BASE,
        help=f"Base for the upstream URL when --path is used (default {DEFAULT_UPSTREAM_BASE}).",
    )
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--max-paths", type=int, default=80, help="Cap on missing/extra paths printed.")
    args = parser.parse_args()

    if args.path:
        upstream_url = f"{args.upstream_base.rstrip('/')}{args.path}"
        local_url = f"{args.local_base.rstrip('/')}{args.path}"
    else:
        if not args.local:
            parser.error("--local is required when --upstream is set")
        upstream_url = args.upstream
        local_url = args.local

    upstream = asyncio.run(fetch_upstream(upstream_url, timeout=args.timeout))
    local = fetch_local(local_url, timeout=args.timeout)
    if upstream.error and upstream.payload is None and upstream.status == 0:
        print(format_report(build_report(upstream, local), max_paths=args.max_paths))
        return 2
    if local.error and local.payload is None and local.status == 0:
        print(format_report(build_report(upstream, local), max_paths=args.max_paths))
        return 2

    report = build_report(upstream, local)
    print(format_report(report, max_paths=args.max_paths))
    return 0 if not report.missing_in_local else 1


if __name__ == "__main__":
    raise SystemExit(main())
