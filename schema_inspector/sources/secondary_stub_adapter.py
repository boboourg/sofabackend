from __future__ import annotations

from .base import SourceAdapter, SourceFetchRequest, SourceFetchResponse


class SecondaryStubSourceAdapter(SourceAdapter):
    source_slug = "secondary_source"
    is_enabled = False

    async def get_json(self, request: SourceFetchRequest) -> SourceFetchResponse:
        del request
        raise RuntimeError("secondary_source adapter is disabled")
