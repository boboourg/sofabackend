-- Add credential-free proxy endpoint tracing to api_request_log.
--
-- proxy_id stores only the generated pool-local name (proxy_1, proxy_2, ...).
-- proxy_address stores host:port without credentials, so production logs can
-- answer which proxy endpoint returned 403/404 without exposing passwords.

ALTER TABLE api_request_log
    ADD COLUMN IF NOT EXISTS proxy_address text;

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_api_request_log_proxy_address
ON api_request_log (proxy_address)
WHERE proxy_address IS NOT NULL;
