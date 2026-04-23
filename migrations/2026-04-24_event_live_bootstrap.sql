ALTER TABLE event
    ADD COLUMN IF NOT EXISTS live_bootstrap_done_at TIMESTAMPTZ;
