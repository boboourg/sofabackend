from __future__ import annotations

from ..runtime import RuntimeConfig
from ..sofascore_client import SofascoreClient
from ..transport import InspectorTransport
from .base import SourceAdapter, SourceFetchRequest, SourceFetchResponse


class SofascoreSourceAdapter(SourceAdapter):
    source_slug = "sofascore"

    def __init__(
        self,
        *,
        runtime_config: RuntimeConfig,
        transport: InspectorTransport | None = None,
        client: SofascoreClient | None = None,
    ) -> None:
        self.runtime_config = runtime_config
        self.client = client or SofascoreClient(runtime_config, transport=transport)

    async def get_json(self, request: SourceFetchRequest) -> SourceFetchResponse:
        response = await self.client.get_json(
            request.url,
            headers=request.headers,
            timeout=float(request.timeout),
        )
        return SourceFetchResponse(
            source_slug=self.source_slug,
            source_url=response.source_url,
            resolved_url=response.resolved_url,
            fetched_at=response.fetched_at,
            status_code=int(response.status_code),
            headers=response.headers,
            body_bytes=response.body_bytes,
            payload=response.payload,
        )
