CREATE TABLE IF NOT EXISTS provider_source (
    source_slug text PRIMARY KEY,
    display_name text NOT NULL,
    transport_kind text NOT NULL,
    trust_rank integer NOT NULL DEFAULT 100,
    is_active boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS canonical_entity (
    canonical_id bigserial PRIMARY KEY,
    entity_type text NOT NULL,
    canonical_slug text,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS entity_source_key (
    canonical_id bigint NOT NULL REFERENCES canonical_entity(canonical_id) ON DELETE CASCADE,
    source_slug text NOT NULL REFERENCES provider_source(source_slug) ON DELETE RESTRICT,
    entity_type text NOT NULL,
    external_id text NOT NULL,
    mapping_status text NOT NULL DEFAULT 'direct',
    confidence numeric(4,3) NOT NULL DEFAULT 1.000,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_slug, entity_type, external_id)
);

ALTER TABLE api_payload_snapshot
    ADD COLUMN IF NOT EXISTS source_slug text NOT NULL DEFAULT 'sofascore',
    ADD COLUMN IF NOT EXISTS schema_fingerprint text,
    ADD COLUMN IF NOT EXISTS scope_hash text;
