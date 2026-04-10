"""Markdown rendering for schema summaries."""

from __future__ import annotations

import hashlib
from urllib.parse import urlparse

from .fetch import FetchResult
from .schema import NodeSummary


def build_markdown_report(fetch_result: FetchResult, root: NodeSummary) -> str:
    """Render a Markdown report for one fetched JSON payload."""

    object_nodes = root.collect_object_nodes()
    array_nodes = root.collect_array_nodes()
    lines = [
        f"# API Schema Report: `{report_basename(fetch_result.source_url)}`",
        "",
        "## Source",
        "",
        f"- URL: `{fetch_result.source_url}`",
        f"- Resolved URL: `{fetch_result.resolved_url}`",
        f"- Fetched At: `{fetch_result.fetched_at}`",
        f"- HTTP Status: `{fetch_result.status_code}`",
        f"- Content Length: `{len(fetch_result.body_bytes)}` bytes",
        f"- Top-Level Type: `{root.rendered_types()}`",
        f"- Attempts: `{len(fetch_result.attempts)}`",
        f"- Final Proxy: `{fetch_result.final_proxy_name or 'direct'}`",
        f"- Challenge Detection: `{fetch_result.challenge_reason or 'none'}`",
        "",
        "## Summary",
        "",
        f"- Object Nodes: `{len(object_nodes)}`",
        f"- Array Nodes: `{len(array_nodes)}`",
        "",
        "## Network Attempts",
        "",
        "| Attempt | Proxy | Status | Error | Challenge |",
        "| --- | --- | --- | --- | --- |",
    ]

    for attempt in fetch_result.attempts:
        lines.append(
            f"| {attempt.attempt_number} | `{attempt.proxy_name or 'direct'}` | "
            f"`{attempt.status_code if attempt.status_code is not None else '-'}` | "
            f"`{attempt.error or '-'}` | `{attempt.challenge_reason or '-'}` |"
        )

    lines.extend(
        [
            "",
        "## Suggested Entities",
        "",
        ]
    )

    for node in object_nodes:
        candidate_keys = ", ".join(node.candidate_keys()) or "-"
        lines.append(
            f"- `{node.path}` -> table `{node.suggested_table_name()}`; "
            f"instances `{node.object_instance_count}`; candidate keys `{candidate_keys}`"
        )

    if not object_nodes:
        lines.append("- No object entities detected.")

    if array_nodes:
        lines.extend(
            [
                "",
                "## Arrays",
                "",
            ]
        )
        for node in array_nodes:
            item_types = node.item_summary.rendered_types() if node.item_summary is not None else "-"
            lines.append(
                f"- `{node.path}` -> `{node.rendered_types()}`; "
                f"items `{node.min_items or 0}..{node.max_items}`; item schema `{item_types}`"
            )

    for node in object_nodes:
        lines.extend(
            [
                "",
                f"## Entity: `{node.suggested_table_name()}`",
                "",
                f"- Path: `{node.path}`",
                f"- Suggested PostgreSQL Table: `{node.suggested_table_name()}`",
                f"- Observed Instances: `{node.object_instance_count}`",
                f"- Candidate Primary Keys: `{', '.join(node.candidate_keys()) or '-'}`",
                "",
                "| Field | Types | Required | Nullable | Example | Notes |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in node.field_rows():
            lines.append(
                f"| `{row['field']}` | `{row['types']}` | {row['required']} | "
                f"{row['nullable']} | `{row['example']}` | {row['notes']} |"
            )
        if not node.field_rows():
            lines.append("| - | - | - | - | - | no fields |")

    return "\n".join(lines) + "\n"


def report_filename(url: str) -> str:
    basename = report_basename(url)
    if len(basename) > 180:
        digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
        basename = f"{basename[:140].rstrip('_')}_{digest}"
    return f"{basename}.md"


def report_basename(url: str) -> str:
    parsed = urlparse(url)
    parts = []
    if parsed.netloc:
        parts.append(parsed.netloc.replace(".", "_"))
    if parsed.path:
        parts.extend(segment for segment in parsed.path.split("/") if segment)
    if parsed.query:
        parts.append(parsed.query.replace("&", "_").replace("=", "_"))
    normalized = [_safe_part(part) for part in parts if _safe_part(part)]
    return "_".join(normalized) or "response"


def _safe_part(value: str) -> str:
    chars = []
    previous_sep = False
    for char in value.lower():
        if char.isalnum():
            chars.append(char)
            previous_sep = False
        elif not previous_sep:
            chars.append("_")
            previous_sep = True
    return "".join(chars).strip("_")
