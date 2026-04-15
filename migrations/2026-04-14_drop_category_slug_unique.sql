BEGIN;

ALTER TABLE category
    DROP CONSTRAINT IF EXISTS category_slug_key;

CREATE INDEX IF NOT EXISTS idx_category_slug ON category(slug);

COMMIT;
