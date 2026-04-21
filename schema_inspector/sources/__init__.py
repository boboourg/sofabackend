from __future__ import annotations

from ..runtime import RuntimeConfig
from ..transport import InspectorTransport
from .base import SourceAdapter, SourceFetchRequest, SourceFetchResponse
from .secondary_stub_adapter import SecondaryStubSourceAdapter
from .sofascore_adapter import SofascoreSourceAdapter


def build_source_adapter(
    source_slug: str | None = None,
    *,
    runtime_config: RuntimeConfig,
    transport: InspectorTransport | None = None,
) -> SourceAdapter:
    normalized_source_slug = str(source_slug or "sofascore").strip().lower()
    if normalized_source_slug == "sofascore":
        return SofascoreSourceAdapter(runtime_config=runtime_config, transport=transport)
    if normalized_source_slug == "secondary_source":
        return SecondaryStubSourceAdapter()
    raise ValueError(f"Unknown source adapter: {normalized_source_slug}")


__all__ = [
    "SecondaryStubSourceAdapter",
    "SofascoreSourceAdapter",
    "SourceAdapter",
    "SourceFetchRequest",
    "SourceFetchResponse",
    "build_source_adapter",
]
