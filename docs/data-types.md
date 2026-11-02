# Hashes, lists, sets and sorted sets

Flaro supports strings and four collection types through RESP2. There are 54
collection commands, plus the generic `TYPE` command. Keys, hash fields, values,
and members are binary-safe. The table below is the supported command subset;
unlisted Redis commands and options are not implemented.

| Type | Supported commands |
|---|---|
| Hash | `HSET`, `HMSET`, `HSETNX`, `HGET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HINCRBY`, `HSTRLEN` |
| List | `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LTRIM`, `LREM` |
| Set | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SUNION`, `SINTER`, `SDIFF`, `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE` |
| Sorted set | `ZADD`, `ZINCRBY`, `ZREM`, `ZSCORE`, `ZMSCORE`, `ZCARD`, `ZCOUNT`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZPOPMIN`, `ZPOPMAX`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE` |

`HSET` accepts multiple field/value pairs and returns the count of newly added
fields. `HINCRBY` uses checked signed 64-bit arithmetic. List push commands accept
multiple values; `LPOP`/`RPOP` accept an optional count. List indexes/ranges support
negative offsets. Removing the final field, item, or member deletes the key.

`ZADD` supports `NX`, `XX`, `GT`, `LT`, `CH`, and single-pair `INCR`, including their
mutual-exclusion rules. Scores use binary64 arithmetic. NaNs and overflow are
rejected; infinities and hexadecimal double syntax are supported. Equal scores
sort by binary member order. `ZRANGE` supports rank ranges, `BYSCORE`, `REV`,
`WITHSCORES`, and `LIMIT` with score ranges. Score bounds support `(` for exclusion
and `-inf`/`+inf`. `ZRANK`/`ZREVRANK` support the basic form without `WITHSCORE`.
`ZPOPMIN`/`ZPOPMAX` accept an optional nonnegative count.

Missing-key results follow the supported Redis command semantics. Commands against
the wrong type return `WRONGTYPE`; Redis's special `MGET` behavior returns null for
non-string values. `SET` and `MSET` can replace a collection with a string. Generic
`DEL`, `EXISTS`, `TYPE`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, and `PERSIST` work across
types. Collection mutations preserve an existing key TTL. Set algebra store
commands replace the destination and clear its TTL, including when it is also an
input. Invalid arguments, capacity failures, and persistence admission failures do
not partially mutate a collection.

## Persistence and upgrade

Each entries table has a `data_type smallint NOT NULL DEFAULT 0` column:

| Tag | Type |
|---|---|
| 0 | String: unchanged raw binary bytes |
| 1 | Hash |
| 2 | List |
| 3 | Set |
| 4 | Sorted set |

Collection payloads use a versioned, length-prefixed binary encoding with explicit
counts, duplicate validation, and score validation. No string prefix is interpreted
as a type tag. A string beginning with `COL1`, including embedded zero bytes, stays
a string. The ordinary WAL-logged tables, journal records, journal compaction,
recovery, and cluster imports all retain the type tag and absolute expiry.

The worker adds the type column to an existing string-only entries table before
restoring data or opening the listener. Existing rows receive tag 0 without changing
their bytes. The compactor waits for the upgraded schema. New installations include
the column in their extension SQL. Existing string journal records remain readable.

Upgrade the binaries on every shard and standby before using collections or
migrating them. Do not downgrade to a string-only binary after storing typed data:
older binaries do not understand the new journal flags or typed payloads. This is
an experimental startup migration, not a general rolling-upgrade framework.

Ordinary writes retain the chosen write-behind contract: acknowledgment precedes
commit. Committed snapshots recover through PostgreSQL WAL; recent uncommitted
writes can be lost. Snapshots handed to persistence are immutable, so a later in-memory mutation
cannot change the value being persisted by an earlier batch. Single-item `HSET`,
`LSET`, `SADD`, `SREM`, and `ZINCRBY` reserve their full updated size before
changing live data. Their dirty keys are captured at handoff; rejected admission
leaves live data unchanged. Cluster durability barriers capture dirty keys before
waiting for commit.

Collection-only handoffs use `flush_ms` (at least 5 ms), or happen earlier under
memory pressure. Ordinary string workloads retain the 5-ms handoff window. The SQL
worker still flushes on its interval or capacity threshold; these intervals are
batching targets, not a maximum crash-loss window or a durability guarantee.
The old journal byte accounting remains stable across this memory-layout change.

## Cluster behavior

`COMMAND` advertises collection key positions, so cluster-aware clients route them
normally. Multi-key set algebra requires a single hash slot, including the
`*STORE` destination. `CROSSSLOT`, `MOVED`, `ASK`/`ASKING`, and migration fencing
apply to collections. `MIGRATE` transfers the complete encoded collection, type,
and absolute expiry using the same durable commit/replay protocol as strings.

## Bounds and current limitations

- The key plus encoded collection plus 128 bytes must fit within 1 MiB. The
  estimated memory cost of a collection entry must not exceed 8 MiB. The global
  cache memory budget still applies. Oversized mutations return an error atomically.
- Accounting includes encoded bytes, decoded payload sizes, and estimated per-item
  structure overhead, plus an 8-KiB allowance per collection for tree roots and
  vector chunks; it is conservative accounting, not an exact RSS limit.
- Requests remain limited to 1,024 arguments. Large array replies are rejected
  before exceeding the 2-MiB output budget.
- Hashes, sets and sorted-set member lookup use flat hash tables. Lists and sorted
  score indexes use structurally shared vectors and ordered trees. The single-item
  fast paths above update private data in place between handoffs. The first write
  sharing a handed-off hash table clones that table; other collection commands
  still prepare an atomic copy before admission. This can cause size-dependent
  latency spikes and means these commands do not all have Redis's complexity.
  Full serialization is deferred until persistence or migration needs it. The
  persisted representation is still a whole collection, so flushing large,
  frequently changed collections can amplify WAL and CPU work. Rank/range queries
  currently traverse the ordered score index.
- Redis performance depends on collection size, command, pipeline depth, and flush
  settings. See the [Linux ARM64 collection results](collection-performance-arm64.md)
  for throughput, tail latency, exact workloads, and limits.
- Unsupported examples include blocking list operations, `LMOVE`/`RPOPLPUSH`,
  `LINSERT`/`LPOS`, `SMOVE`, random-member/pop commands, cursor scans, hash-field TTLs,
  `HINCRBYFLOAT`, sorted-set algebra, `BYLEX`, `ZRANGESTORE`, and blocking/multi-key
  sorted-set pops. Transactions, Lua and RESP3 remain unsupported.

## Validation

Validated on macOS ARM64 / PostgreSQL 17.11: 49 Rust tests and 1,346 deterministic
and randomized Redis comparisons in each persistence mode, using Redis 8.2.11
and redis-py 6.4.0. They cover binary values, wrong types, duplicate members,
negative indexes, score ordering, conditional updates, empty-key deletion,
TTL preservation, old-schema upgrade, and crash recovery in direct and journal
modes. Journal tests include both unmaterialized records and compacted rows.
Three-shard tests migrate every collection kind and verify it after restart.

```sh
PGRX_PG_CONFIG_PATH=/path/to/pg_config cargo build -p pg-cache --locked --features journal-prototype
PGRX_PG_CONFIG_PATH=/path/to/pg_config python scripts/collections_integration.py --redis-server /path/to/redis-server
PGRX_PG_CONFIG_PATH=/path/to/pg_config python scripts/collections_integration.py --journal --redis-server /path/to/redis-server
PGRX_PG_CONFIG_PATH=/path/to/pg_config python scripts/cluster_integration.py
PGRX_PG_CONFIG_PATH=/path/to/pg_config python scripts/cluster_integration.py --journal
cargo test --locked
```

Redis command references: [HSET](https://redis.io/docs/latest/commands/hset/),
[LPOP](https://redis.io/docs/latest/commands/lpop/),
[ZADD](https://redis.io/docs/latest/commands/zadd/), and
[ZRANGE](https://redis.io/docs/latest/commands/zrange/).
