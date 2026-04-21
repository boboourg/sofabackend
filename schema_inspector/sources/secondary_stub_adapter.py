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

    def build_competition_job(self, database):
        del database
        raise UnsupportedSourceAdapterError("competition ingestion is not wired for source secondary_source")

    def build_event_detail_job(self, database):
        del database
        raise UnsupportedSourceAdapterError("event-detail enrichment is not wired for source secondary_source")

    def build_entities_job(self, database):
        del database
        raise UnsupportedSourceAdapterError("entities enrichment is not wired for source secondary_source")
