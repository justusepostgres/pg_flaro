-- Experimental schema only; not included in CREATE EXTENSION or the RESP worker.
CREATE SCHEMA cache_journal;
REVOKE ALL ON SCHEMA cache_journal FROM PUBLIC;
CREATE TABLE cache_journal.state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    materialized bigint NOT NULL DEFAULT 0 CHECK(materialized >= 0),
    committed bigint NOT NULL DEFAULT 0 CHECK(committed >= materialized),
    active bigint NOT NULL DEFAULT 0 CHECK(active IN (0,1)),
    previous0 bigint NOT NULL DEFAULT 0, sequence0 bigint NOT NULL DEFAULT 0,
    bytes0 bigint NOT NULL DEFAULT 0,
    previous1 bigint NOT NULL DEFAULT 0, sequence1 bigint NOT NULL DEFAULT 0,
    bytes1 bigint NOT NULL DEFAULT 0,
    segment_limit bigint NOT NULL DEFAULT 268435456 CHECK(segment_limit BETWEEN 1 AND 268435456),
    CHECK(previous0 >= 0 AND sequence0 >= previous0 AND bytes0 BETWEEN 0 AND segment_limit),
    CHECK(previous1 >= 0 AND sequence1 >= previous1 AND bytes1 BETWEEN 0 AND segment_limit)
);
INSERT INTO cache_journal.state(singleton) VALUES(true);
CREATE TABLE cache_journal.segment0 (
    sequence bigint NOT NULL, previous bigint NOT NULL,
    part integer NOT NULL, parts integer NOT NULL, payload bytea NOT NULL,
    PRIMARY KEY(sequence,part),
    CHECK(parts BETWEEN 1 AND 2731 AND part >= 0 AND part < parts),
    CHECK(octet_length(payload) BETWEEN 1 AND 6144),
    CHECK(previous >= 0 AND sequence > previous)
);
CREATE TABLE cache_journal.segment1 (LIKE cache_journal.segment0 INCLUDING ALL);
ALTER TABLE cache_journal.segment0 ALTER COLUMN payload SET STORAGE PLAIN;
ALTER TABLE cache_journal.segment1 ALTER COLUMN payload SET STORAGE PLAIN;
CREATE TABLE cache_journal.entries(key bytea NOT NULL, value bytea NOT NULL, expires_at_ms bigint);
CREATE INDEX entries_key_hash_idx ON cache_journal.entries USING hash (key);
REVOKE ALL ON ALL TABLES IN SCHEMA cache_journal FROM PUBLIC;
