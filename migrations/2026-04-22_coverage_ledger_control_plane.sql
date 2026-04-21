BEGIN;

CREATE TABLE IF NOT EXISTS coverage_ledger (
    source_slug text NOT NULL REFERENCES provider_source(source_slug) ON DELETE RESTRICT,
    sport_slug text NOT NULL,
    surface_name text NOT NULL,
    scope_type text NOT NULL,
    scope_id bigint NOT NULL,
    freshness_status text NOT NULL,
    completeness_ratio numeric(5,4) NOT NULL DEFAULT 0,
    last_success_at timestamptz,
    last_checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_slug, sport_slug, surface_name, scope_type, scope_id)
);

CREATE TABLE IF NOT EXISTS endpoint_contract_registry (
    pattern text NOT NULL,
    source_slug text NOT NULL REFERENCES provider_source(source_slug) ON DELETE RESTRICT,
    path_template text NOT NULL,
    query_template text,
    envelope_key text NOT NULL,
    target_table text,
    notes text,
    contract_version text NOT NULL DEFAULT 'v1',
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pattern, source_slug)
);

ALTER TABLE endpoint_registry
    ADD COLUMN IF NOT EXISTS source_slug text NOT NULL DEFAULT 'sofascore',
    ADD COLUMN IF NOT EXISTS contract_version text NOT NULL DEFAULT 'v1';

INSERT INTO provider_source (
    source_slug,
    display_name,
    transport_kind,
    trust_rank,
    is_active
)
VALUES ('sofascore', 'Sofascore', 'http', 100, true)
ON CONFLICT (source_slug) DO NOTHING;

INSERT INTO endpoint_contract_registry (
    pattern,
    source_slug,
    path_template,
    query_template,
    envelope_key,
    target_table,
    notes,
    contract_version
)
SELECT
    pattern,
    source_slug,
    path_template,
    query_template,
    envelope_key,
    target_table,
    notes,
    contract_version
FROM endpoint_registry
ON CONFLICT (pattern, source_slug) DO NOTHING;

COMMIT;
