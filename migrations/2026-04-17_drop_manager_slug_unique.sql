BEGIN;

ALTER TABLE manager DROP CONSTRAINT IF EXISTS manager_slug_key;
DROP INDEX IF EXISTS manager_slug_key;

COMMIT;
