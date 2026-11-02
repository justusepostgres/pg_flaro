CREATE SCHEMA cache_store;
REVOKE ALL ON SCHEMA cache_store FROM PUBLIC;
CREATE TABLE cache_store.entries (
    key bytea NOT NULL,
    value bytea NOT NULL,
    expires_at_ms bigint
);
-- Hash scans recheck the full bytea key and support keys larger than a B-tree
-- index tuple. The private writer serializes MERGE with a table write lock.
CREATE INDEX entries_key_hash_idx ON cache_store.entries USING hash (key);
CREATE TABLE cache_store.checkpoint (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    sequence bigint NOT NULL CHECK (sequence >= 0)
);
INSERT INTO cache_store.checkpoint VALUES (true, 0);
REVOKE ALL ON ALL TABLES IN SCHEMA cache_store FROM PUBLIC;
