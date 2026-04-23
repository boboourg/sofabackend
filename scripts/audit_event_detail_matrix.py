from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

import asyncpg

from schema_inspector.db import load_database_config
from schema_inspector.detail_resource_policy import build_event_detail_request_specs
from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.classifier import classify_snapshot
from schema_inspector.parsers.registry import ParserRegistry


TARGET_SPORTS = ("tennis", "basketball", "esports")
TARGET_STATUSES = ("notstarted", "finished", "canceled", "postponed")


@dataclass(frozen=True)
class MatrixCandidate:
    sport_slug: str
    status_type: str
    event_id: int


EVENT_MATRIX_QUERY = """
WITH ranked_events AS (
    SELECT
        s.slug AS sport_slug,
        es.type AS status_type,
        e.id AS event_id,
        ROW_NUMBER() OVER (
            PARTITION BY s.slug, es.type
            ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
        ) AS row_num
    FROM event e
    JOIN event_status es ON es.code = e.status_code
    LEFT JOIN tournament t ON t.id = e.tournament_id
    LEFT JOIN unique_tournament ut ON ut.id = e.unique_tournament_id
    LEFT JOIN category c ON c.id = COALESCE(t.category_id, ut.category_id)
    LEFT JOIN sport s ON s.id = c.sport_id
    WHERE s.slug = ANY($1::text[])
      AND es.type = ANY($2::text[])
)
SELECT sport_slug, status_type, event_id
FROM ranked_events
WHERE row_num = 1
ORDER BY sport_slug, status_type
"""


ROOT_SNAPSHOT_QUERY = """
SELECT
    id,
    endpoint_pattern,
    source_url,
    envelope_key,
    context_entity_type,
    context_entity_id,
    payload,
    fetched_at
FROM api_payload_snapshot
WHERE endpoint_pattern = '/api/v1/event/{event_id}'
  AND context_entity_type = 'event'
  AND context_entity_id = $1
ORDER BY fetched_at DESC NULLS LAST, id DESC
LIMIT 1
"""


async def main() -> None:
    database_config = load_database_config()
    conn = await asyncpg.connect(database_config.dsn)
    try:
        registry = ParserRegistry.default()
        candidates = await _load_candidates(conn)
        if not candidates:
            print("No matching events found.")
            return

        for candidate in candidates:
            snapshot = await _load_root_snapshot(conn, candidate)
            if snapshot is None:
                print(
                    json.dumps(
                        {
                            "sport_slug": candidate.sport_slug,
                            "status_type": candidate.status_type,
                            "event_id": candidate.event_id,
                            "error": "missing_root_snapshot",
                        },
                        ensure_ascii=False,
                    )
                )
                continue

            parsed = registry.parse(snapshot)
            event_row = parsed.entity_upserts.get("event", ({},))[0]
            team_ids = tuple(
                team_id
                for team_id in (event_row.get("home_team_id"), event_row.get("away_team_id"))
                if isinstance(team_id, int)
            )
            specs = build_event_detail_request_specs(
                sport_slug=candidate.sport_slug,
                status_type=candidate.status_type,
                team_ids=team_ids,
                provider_ids=(1,),
                has_event_player_heat_map=event_row.get("has_event_player_heat_map"),
                has_xg=event_row.get("has_xg"),
                core_only=False,
            )
            routed = []
            for spec in specs:
                routed.append(
                    {
                        "endpoint_pattern": spec.endpoint.pattern,
                        "parser_family": classify_snapshot(
                            RawSnapshot(
                                snapshot_id=None,
                                endpoint_pattern=spec.endpoint.pattern,
                                sport_slug=candidate.sport_slug,
                                source_url=spec.endpoint.build_url(**spec.resolved_path_params(event_id=candidate.event_id)),
                                resolved_url=None,
                                envelope_key=spec.endpoint.envelope_key,
                                http_status=None,
                                payload={},
                                fetched_at=None,
                                context_entity_type="event",
                                context_entity_id=candidate.event_id,
                                context_event_id=candidate.event_id,
                            )
                        ),
                    }
                )

            print(
                json.dumps(
                    {
                        "sport_slug": candidate.sport_slug,
                        "status_type": candidate.status_type,
                        "event_id": candidate.event_id,
                        "planned_routes": routed,
                    },
                    ensure_ascii=False,
                )
            )
    finally:
        await conn.close()


async def _load_candidates(conn: asyncpg.Connection) -> list[MatrixCandidate]:
    rows = await conn.fetch(EVENT_MATRIX_QUERY, list(TARGET_SPORTS), list(TARGET_STATUSES))
    return [
        MatrixCandidate(
            sport_slug=str(row["sport_slug"]),
            status_type=str(row["status_type"]),
            event_id=int(row["event_id"]),
        )
        for row in rows
    ]


async def _load_root_snapshot(
    conn: asyncpg.Connection,
    candidate: MatrixCandidate,
) -> RawSnapshot | None:
    row = await conn.fetchrow(ROOT_SNAPSHOT_QUERY, candidate.event_id)
    if row is None:
        return None
    fetched_at = row["fetched_at"]
    return RawSnapshot(
        snapshot_id=int(row["id"]),
        endpoint_pattern=str(row["endpoint_pattern"]),
        sport_slug=candidate.sport_slug,
        source_url=str(row["source_url"]),
        resolved_url=str(row["source_url"]),
        envelope_key=str(row["envelope_key"]),
        http_status=200,
        payload=row["payload"],
        fetched_at=fetched_at.isoformat() if fetched_at is not None else None,
        context_entity_type=str(row["context_entity_type"]) if row["context_entity_type"] is not None else None,
        context_entity_id=int(row["context_entity_id"]) if row["context_entity_id"] is not None else None,
        context_event_id=candidate.event_id,
    )


if __name__ == "__main__":
    asyncio.run(main())
