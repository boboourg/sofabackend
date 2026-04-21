from __future__ import annotations

from .base import (
    DisabledSourceAdapterError,
    SourceAdapter,
    SourceFetchRequest,
    SourceFetchResponse,
    UnsupportedSourceAdapterError,
)


class SecondaryStubSourceAdapter(SourceAdapter):
    source_slug = "secondary_source"
    is_enabled = False

    async def get_json(self, request: SourceFetchRequest) -> SourceFetchResponse:
        del request
        raise DisabledSourceAdapterError("secondary_source adapter is disabled")

    def build_event_list_job(self, database):
        del database
        raise UnsupportedSourceAdapterError("event-list discovery is not wired for source secondary_source")
