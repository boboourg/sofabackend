"""High-level inspection service."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Mapping

from .fetch import fetch_json
from .report import build_markdown_report, report_filename
from .runtime import RuntimeConfig
from .schema import infer_schema


async def inspect_url_to_markdown(
    url: str,
    *,
    output_dir: str | Path = "reports",
    headers: Mapping[str, str] | None = None,
    timeout: float = 20.0,
    runtime_config: RuntimeConfig | None = None,
) -> Path:
    """Fetch a URL, infer JSON schema, and write a Markdown report."""

    fetch_result = await fetch_json(url, headers=headers, timeout=timeout, runtime_config=runtime_config)
    root = infer_schema(fetch_result.payload)
    markdown = build_markdown_report(fetch_result, root)

    output_path = Path(output_dir)
    await asyncio.to_thread(output_path.mkdir, parents=True, exist_ok=True)
    report_path = output_path / report_filename(url)
    await asyncio.to_thread(report_path.write_text, markdown, encoding="utf-8")
    return report_path
