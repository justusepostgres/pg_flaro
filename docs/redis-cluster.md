# Redis Cluster client mode

Flaro supports Redis Cluster client routing over RESP2, with one independent
PostgreSQL cluster per shard. Each shard holds its own in-memory dataset and
WAL-logged persistence. CNPG can replicate and promote PostgreSQL instances within
that shard. Advertise a stable primary Service endpoint for each shard.

This is an experimental Redis Cluster **protocol subset**. It does not implement
the Redis cluster bus, gossip, Redis replication, Sentinel, automatic shard
membership, or Redis-native elections. It cannot join a cluster of Redis servers.
Membership is configured at bootstrap; slots can move online between those members.
The previous single-shard Redis performance results do not measure cluster mode.

## Bootstrap

Set `pg_cache.cluster_config` to a single-line JSON object in each shard's
`postgresql.conf`, then restart PostgreSQL. Use the same ordered `nodes` array on
all shards, and a different `local_id` matching the local node. For example:

```json
{
  "local_id": "1111111111111111111111111111111111111111",
  "nodes": [
    {"id": "1111111111111111111111111111111111111111", "host": "flaro-a-redis", "port": 6380, "slots": [[0, 5460]]},
    {"id": "2222222222222222222222222222222222222222", "host": "flaro-b-redis", "port": 6380, "slots": [[5461, 10922]]},
    {"id": "3333333333333333333333333333333333333333", "host": "flaro-c-redis", "port": 6380, "slots": [[10923, 16383]]}
  ]
}
```

Use unique, stable 40-character lowercase hexadecimal IDs and reachable DNS/IP
endpoints. All 16,384 slots must be assigned exactly once. There can be 1–64
members. Standalone mode remains the default when this setting is absent and the
database has no cluster metadata. Existing data must belong to the configured
local shard; bootstrap refuses a mismatched dataset.

The worker creates `cache_store.cluster_state` when cluster mode is first enabled.
It is a private, WAL-logged table. Subsequent startups recover its current owners,
import/export states, and pending transfer intent. The initial ranges in the GUC
are only the bootstrap map: **do not edit them after moving slots**. Changing IDs,
endpoints, ordering, or other bootstrap fields fails closed; membership updates
are not implemented yet. Removing the GUC from a clustered database also fails
closed. Standbys wait for promotion before opening the RESP listener.

[`deploy/cnpg/sharded.yaml`](../deploy/cnpg/sharded.yaml) shows three independent
CNPG clusters and their stable primary Services. Build a fresh image containing
this code; previous benchmark images and packages do not contain cluster support.

## Client and command compatibility

Use RESP2 and database 0. The string commands and [collection command subset](data-types.md) are available.
Redis Cluster CRC16/XMODEM, nonempty `{hash tags}`, binary keys, `MOVED`, `ASK`,
one-command/per-connection `ASKING`, and `CROSSSLOT` checks are implemented.
Multi-key operations must use one slot, including `MGET`, `MSET`, `DEL`, `EXISTS`, and multi-key set algebra (including
store destinations). During migration, a request with both present and missing source keys
receives `TRYAGAIN`. Pipelining is supported; transactions and Lua remain unsupported.

Discovery and administration:

- `COMMAND`, `COMMAND COUNT`, `COMMAND INFO`, `COMMAND GETKEYS` describe the supported
  data commands for client-side key extraction.
- `CLUSTER SLOTS`, `SHARDS`, `NODES`, `MYID`, `INFO`, `KEYSLOT`.
- `CLUSTER COUNTKEYSINSLOT slot`, `GETKEYSINSLOT slot count` (maximum count 1024;
  reduce it if the bounded reply would exceed 1 MiB).
- `CLUSTER SETSLOT slot IMPORTING source-id`, `MIGRATING target-id`, `NODE owner-id`.
  Finalizing a source slot with live keys is rejected. `STABLE` is a no-op only
  when there is no active migration; cancellation is deliberately unsupported.
- `MIGRATE host port key 0 timeout [REPLACE]`: one key at a time, timeout 1–30,000 ms,
  and only to the configured migration target. `COPY`, `AUTH`, `AUTH2`, and `KEYS`
  variants are not supported. The internal transfer format is Flaro-specific,
  not Redis RDB/DUMP/RESTORE; migration to/from Redis servers is unsupported.
- `PGCACHE.MIGRATION`: null when no transfer is pending; otherwise
  `[slot, binary-key, target-node-id]` for recovery tooling.

`CLUSTER INFO`/`SHARDS` report the configured topology, not peer reachability or
quorum health. There are no Redis replicas advertised: PostgreSQL replicas are
behind each shard's stable primary endpoint. Cluster-wide `SCAN`, multi-shard
transactions, read-from-replica mode, `MEET`, `FORGET`, `ADDSLOTS`, `FAILOVER`, and
`redis-cli --cluster create/reshard` are not supported. Use the supplied move tool.

The integration test uses **redis-py 6.4.0 `RedisCluster`**, including discovery,
redirects, pipelines, and resharding. That client version cannot follow `ASK` to a
node missing from its cached slot map. Give every shard an initial nonempty range
and connect clients after bootstrap. Empty nodes can be represented by `SHARDS`
and `NODES`, but are not discoverable through `SLOTS`. Other client libraries have
not been certified.

```python
from redis.cluster import RedisCluster
cache = RedisCluster(host="flaro-a-redis", port=6380, protocol=2)
cache.set("{customer:42}name", "Ada")
cache.mget("{customer:42}name", "{customer:42}email")
```

The listener still has no AUTH, TLS, or ACLs. Its reachable clients can administer
slots and call the internal `PGCACHE.IMPORT` and `PGCACHE.FINISH` endpoints. Keep it private to trusted
applications and operators; PostgreSQL connection authentication does not cover RESP.

## Move a slot online

Install redis-py 6.4.0 in an operator environment and save the bootstrap JSON as
`cluster.json`. Run one controller at a time:

```sh
python scripts/reshard.py --config cluster.json \
  --source 1111111111111111111111111111111111111111 \
  --target 3333333333333333333333333333333333333333 --slot 5061
```

The tool marks the destination importing, marks the source migrating, drains keys,
then publishes the destination as owner on the destination, source, and remaining
shards, in that order. New keys are directed to the destination via `ASK` while
existing source keys remain usable between transfers. There is no automatic
background rebalancer or distributed controller election. Do not run concurrent
controllers or manually modify the private metadata table.

Each `MIGRATE` temporarily blocks the source event loop, as a synchronous transfer;
other shards continue serving. Key enumeration currently scans the in-memory map,
so large resharding jobs can be slow. Destination imports and administrative metadata
commits also briefly block that shard. The ordinary request bound is 1 MiB; transfer
frames reserve an additional 512 bytes for IDs, endpoint and expiry metadata, so
an accepted key/value remains transferable. Expiry is copied as an absolute Unix
millisecond deadline. Hosts need synchronized clocks.

## Crash and timeout behavior

Ordinary client writes still acknowledge before commit, with the chosen
write-behind crash-loss window. Migration adds stronger ordering:

1. Flush prior source writes and commit a durable transfer intent.
2. Import the value and absolute expiry; commit a per-source replay watermark
   before acknowledging. Delayed duplicate imports cannot overwrite newer writes.
3. Delete the source key; wait for source WAL commit.
4. Clear the transfer intent durably, then resume serving the slot.

If the destination response is uncertain, the source retains the key and the
slot returns `TRYAGAIN` until the move is resumed. This fence survives restart and
prevents a potentially duplicated value from being overwritten or deleted on the
source while the destination has a conflicting copy. Other slots remain available
once the synchronous transfer returns. A local persistence timeout terminates the
serving worker rather than serving an uncommitted deletion or metadata transition.

**Rerun the same move command after an interruption.** It discovers a pending
transfer, retries with `REPLACE`, and finishes partially published topology. If a
source deletion committed just before a crash, the retry sees the key absent and
clears the durable intent without overwriting the destination. If an expiring key
disappears, the source first advances the destination replay watermark before
clearing its intent. A delayed expired import cannot erase a newly created value.

These guarantees depend on retaining the acknowledged WAL on both shards.
Asynchronous PostgreSQL promotion can discard locally committed destination data
after the source deletion. For migration across failover, configure synchronous
replication and promote only a replica containing the acknowledged WAL. Independent
PITR/rollback of one shard, split-brain, and loss of both durable copies are not
coordinated by this protocol. The supplied CNPG example requires a synchronous
standby, sacrificing write availability when that standby is unavailable.

## Verification

```sh
PGRX_PG_CONFIG_PATH=/path/to/pg_config cargo build -p pg-cache --locked --features journal-prototype
PGRX_PG_CONFIG_PATH=/path/to/pg_config python scripts/cluster_integration.py
PGRX_PG_CONFIG_PATH=/path/to/pg_config python scripts/cluster_integration.py --journal
cargo test --locked
```

Validated locally on macOS ARM64 with PostgreSQL 17.11 and redis-py 6.4.0 in both
direct-table and journal modes; all 43 Rust tests and the standalone regression
suite also pass. Linux ARM64 clustered throughput and CNPG promotion during
resharding have not been measured.

The test creates and stops three isolated local PostgreSQL instances, retaining
their data and logs under the ignored `.local/` directory for inspection. It checks
real RedisCluster routing, hash tags, binary data, cross-slot rejection, ASKING
scope, large-value and TTL transfer, unavailable destinations, commits after lost
replies, delayed duplicate imports and expiry during recovery, crash recovery during migration, interrupted topology publication, and
resuming/idempotently completing the operator workflow. This is a correctness test,
not a clustered throughput benchmark or a CNPG promotion test.

Protocol references: [Redis Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/),
[SETSLOT](https://redis.io/docs/latest/commands/cluster-setslot/),
[MIGRATE](https://redis.io/docs/latest/commands/migrate/).
The deployment example follows [CNPG synchronous replication](https://cloudnative-pg.io/docs/1.27/replication/).
