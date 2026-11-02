CREATE SCHEMA cache_store;
REVOKE ALL ON SCHEMA cache_store FROM PUBLIC;
CREATE TABLE cache_store.entries (
    key bytea PRIMARY KEY,
    value bytea NOT NULL,
    expires_at_ms bigint
);
CREATE TABLE cache_store.checkpoint (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    sequence bigint NOT NULL CHECK (sequence >= 0)
);
INSERT INTO cache_store.checkpoint VALUES (true, 0);
REVOKE ALL ON ALL TABLES IN SCHEMA cache_store FROM PUBLIC;
