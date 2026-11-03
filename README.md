<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/flaro-logo-dark.svg">
  <source media="(prefers-color-scheme: light)" srcset="docs/assets/flaro-logo-light.svg">
  <img alt="Flaro logo: a blue elephant with an orange flame and the pg_flaro wordmark" src="docs/assets/flaro-logo-light.svg" width="520">
</picture>

# Flaro (`pg_flaro`)

Experimental PostgreSQL 17 extension written with Rust/pgrx. The current SQL
extension, library, and configuration prefix remain named `pg_cache`. It serves a documented
Redis RESP2 command subset over TCP, keeps the live dataset in memory, and persists
writes asynchronously into **ordinary WAL-logged PostgreSQL tables**.

This is a working first version, not a full Redis replacement. Beating Redis is a
performance target, not a general performance guarantee. The deployment target is
Linux ARM64. See [the CNPG deployment notes](deploy/cnpg/README.md) for current
deployment limitations.

Hashes, lists, sets, and sorted sets are supported through a documented
[collection command subset](docs/data-types.md), including persistence and migration.
The [Linux ARM64 collection benchmark](docs/collection-performance-arm64.md)
measured higher median throughput in 31 of 32 cases (up to 2.33× Redis), with the
remaining case 0.33% slower. Larger-collection write p99 was still 1.04–1.98×
Redis's latency; these results do not establish universal Redis parity.

Redis Cluster client mode and online slot moves are available as an opt-in
experimental feature. See [cluster setup, compatibility, migration recovery, and
limits](docs/redis-cluster.md). Each shard uses its own PostgreSQL/CNPG cluster.

## Architecture and durability

```text
Redis client → TCP / RESP2 → Rust event loop + in-memory map
                                         │ bounded FIFO channel
                                         ▼
                                PostgreSQL worker main thread
                                         │ batched PostgreSQL transaction
                                         ▼
                          cache_store.entries + checkpoint + WAL
```

One pgrx background worker contains the network thread and persistence main thread.
Only the main thread calls PostgreSQL APIs. The network thread inherits a signal
mask that keeps asynchronous PostgreSQL signal handlers on the main thread.
The cache is process-local, with one owner; it is not copied per SQL connection.

- A write succeeds after persistence-queue capacity is reserved and the memory
  mutation is applied. Success **does not mean the write is durable**.
- The event loop groups mutations with a 5-ms handoff target, sending sooner at
  128 KiB of staged mutations. This staging time can add to persistence latency;
  it does not delay successful command replies. Batches
  coalesce repeated keys as handoffs arrive, preserving the final value or deletion
  and releasing superseded payload reservations before the transaction starts. Values,
  expirations, deletes, and the committed sequence update in one transaction with
  `synchronous_commit=on`. Absolute Unix-millisecond expiration times survive restart.
- `PGCACHE.STATS` returns JSON with `accepted_sequence`, `committed_sequence`, and
  `pending_bytes`. The committed sequence advances only after commit returns.
  Sequence numbers refer to mutating commands within this instance's history;
  uncommitted sequence numbers can be reused after recovery.
- Diagnostic counters include `flushes`, `persisted_rows`, `native_rows`, `prepare_us`, `sql_us`,
  `persist_total_us`, `max_flush_us`, `max_sql_us`, and `max_non_sql_us`. Times are
  cumulative microseconds except the independent maxima. The legacy `sql_us` and
  `max_sql_us` names now measure storage execution, including native heap updates
  and SQL parameter conversion/execution. `native_rows` counts committed native
  updates; other rows use SQL. Total time
  also includes array preparation and transaction commit. Collection/coalescing
  occurs before the flush timer and is excluded from these timings. These diagnostic counters
  reset on worker restart and are approximate concurrent snapshots, not latency
  percentiles. PostgreSQL table statistics are published once per second.
  `max_non_sql_us` measures total flush time minus SQL time, including preparation
  and transaction setup/commit. The maxima can come from different flushes and
  must not be added or subtracted to infer a particular transaction's timing.
- The target flush interval is not a maximum crash-loss window. Database stalls
  can increase it. A full queue pauses that connection's command processing until
  persistence releases capacity. Its input remains bounded and reads on other
  connections continue. Commands on a blocked connection retain their original
  order. A prolonged database stall therefore increases write latency; client
  timeouts do not establish whether a queued command was eventually applied.
- An immediate shutdown or crash can lose every uncommitted write. Already
  committed data recovers from PostgreSQL WAL. Clean shutdown stops the listener,
  joins the network thread, then drains accepted writes.
- A persistence error terminates the worker; PostgreSQL restarts it after five
  seconds and it reloads committed data. There is no in-process retry mechanism.
- Startup restores the full live dataset before opening the TCP listener. Recovery
  fails explicitly if the live dataset exceeds the configured memory limit.

The persistence tables are **not UNLOGGED**. Unlogged tables are unsuitable for this
durability contract because PostgreSQL truncates them after a crash. SQL reads show
the last flushed state and can include expired rows awaiting cleanup. Direct SQL
updates to these tables are unsupported and do not invalidate the memory cache.

The entries tables use PostgreSQL hash indexes over the full binary key. PostgreSQL
rechecks equality after a hash match, so colliding hashes do not identify the same
key. These indexes avoid the B-tree index-tuple limit for larger keys. Hash indexes
do not enforce uniqueness: the private writer serializes its exact-key `MERGE`
operations, and journal compactors share a transaction-scoped advisory lock.
Direct SQL writes can violate these invariants and remain unsupported. This schema
revision requires a fresh installation; no in-place migration is provided yet.

### Journal mode used by the CNPG example

The default build writes directly to `cache_store.entries`. The CNPG example
uses the optional `journal-prototype` build feature and `pg_cache.journal=on`.
Install both SQL extensions with `make install FEATURES=journal-prototype`, then
create `pg_cache` and `pg_cache_journal` in the configured database.

In journal mode the persistence worker commits batches into two bounded,
WAL-logged tables, `cache_journal.segment0` and `segment1`. A separate PostgreSQL
background worker coalesces a sealed segment's changes and applies them to
`cache_journal.entries`. It retires that segment and advances the materialized
sequence in the same transaction. Recovery combines materialized entries with
committed journal changes, including deletes and absolute expiration times.
The direct-store tuple-location optimization described below does not run in
this mode; journal materialization currently uses batched SQL `MERGE` statements.
Persistence batches temporarily disable hash/merge joins and JIT so freshly
populated tables use indexed key lookups rather than repeated full-table joins.
The extension restores the caller's planner settings after the operation.

`PGCACHE.STATS.committed_sequence` measures journal durability, not completion of
materialization. Inspect `cache_journal.state` for the `materialized` and
`committed` sequences and the two segment byte counts. SQL queries of
`cache_journal.entries` alone can lag committed Redis writes. The two modes use
different storage tables; toggling the setting does not migrate their data.

Sustained write throughput remains a limitation: a three-minute, million-key
mixed SET/GET test on Linux ARM64 exhausted journal capacity and produced
`MISCONF` replies under the former 100-ms admission deadline, even with a
128-MiB pending queue and 1-GiB shared buffers. The current backpressure candidate
waits for capacity; its sustained throughput and latency remain under validation.
Short successful runs do not establish sustained Redis parity. Increasing queue
capacity can absorb bursts but does not increase compaction throughput.

## Build and run

Requires Rust 1.92+, a C compiler/libclang, and PostgreSQL **17** server development
headers and binaries. pgrx is pinned to 0.16.1, which was available locally and
validated for this version. `cargo-pgrx` is not required: the extension has explicit
SQL and no generated SQL functions.

```sh
cargo test --locked
make build PG_CONFIG=/path/to/postgresql17/bin/pg_config
make install PG_CONFIG=/path/to/postgresql17/bin/pg_config
```

`make install` copies the library, control file, and SQL into that PostgreSQL
installation. Use an appropriate account, or `DESTDIR=/staging/path` to stage a
package. It does not change configuration or restart any server.

If using the native ARM64 archive instead, its `manifest.json` records file
checksums and the tested platform: Debian 12, PostgreSQL 17.11. From the extracted
directory, use the matching PostgreSQL 17 installation's `pg_config`:

```sh
PG_CONFIG=/path/to/postgresql17/bin/pg_config
install -m 755 pg_cache.so "$("$PG_CONFIG" --pkglibdir)/pg_cache.so"
install -m 644 pg_cache.control pg_cache_journal.control \
  pg_cache--0.1.0.sql pg_cache_journal--0.1.0.sql \
  "$("$PG_CONFIG" --sharedir)/extension/"
```

The native archive includes journal support. Enable `pg_cache.journal=on` and
create both extensions to use the benchmarked journal mode; see the journal-mode
and CNPG sections. Fresh installation is required for this schema revision.

Add to `postgresql.conf` (append to any existing preload list):

```conf
shared_preload_libraries = 'pg_cache'
pg_cache.database = 'postgres'
pg_cache.port = 6380
pg_cache.memory_mb = 64
pg_cache.flush_ms = 100
```

Restart PostgreSQL and, as a superuser in `pg_cache.database`, run:

```sql
CREATE EXTENSION pg_cache;
```

The worker waits for the schema before listening. All settings require restart.
Exactly one persistence database and one listener are supported per PostgreSQL cluster.
Redis sharding uses multiple such clusters.

```sh
redis-cli -h 127.0.0.1 -p 6380 SET greeting hello PX 60000
redis-cli -h 127.0.0.1 -p 6380 GET greeting
redis-cli -h 127.0.0.1 -p 6380 PGCACHE.STATS
```

Use RESP2 clients, database 0, no authentication, and disable optional unsupported
connection handshakes if your library treats their errors as fatal. `HELLO`/RESP3,
`AUTH`, and `CLIENT` are not implemented. The listener defaults to
`127.0.0.1`; set `pg_cache.listen_address` to an explicit IP (restart required)
for private remote access. PostgreSQL's `pg_hba.conf` does not authenticate RESP
connections. Any process that can reach the port can read/write the cache.
TLS, Redis ACLs and multi-tenancy are not implemented. See
[CNPG deployment](deploy/cnpg/README.md) for the private Service example and test
status. With redis-py use `protocol=2` and `pipeline(transaction=False)`;
`incr()`, `incrby()`, `setex()` and `psetex()` use supported commands.

For a disposable local PostgreSQL test cluster, without installing any system files:

```sh
make integration PG_CONFIG=/path/to/postgresql17/bin/pg_config
```

The test applies the same extension SQL directly, starts PostgreSQL on randomly
selected loopback ports, tests recovery, and shuts it down. Artifacts and logs stay
under `.local/`. This verifies the worker and SQL, but not the system-wide
`CREATE EXTENSION` installation path.

An independent **nonpersistent** development harness is also available:

```sh
cargo run -p pg-cache-core --bin memory-server -- 6380
```

## Command compatibility

Keys, string values, hash fields, and collection members are binary-safe. Commands are case-insensitive. TCP
fragmentation, multiple connections, and pipelining are supported. Only RESP arrays
of bulk strings are accepted; inline commands are not supported.

| Commands | Supported behavior |
|---|---|
| `PING`, `ECHO`, `QUIT` | Basic connection commands |
| `GET`, `MGET` | Missing/expired keys return null bulk strings |
| `SET` | `NX` or `XX`; `EX seconds` or `PX milliseconds`; overwrites clear old TTL |
| `SETEX`, `PSETEX` | Atomic value replacement with a positive seconds/milliseconds TTL |
| `MSET` | Atomic within the cache, including on memory/backlog rejection |
| `DEL`, `EXISTS` | Multiple keys; Redis duplicate-key counting behavior |
| `INCR`, `INCRBY` | Signed 64-bit integer semantics, checked overflow, retains TTL |
| `EXPIRE`, `PEXPIRE` | Basic form only; nonpositive TTL deletes immediately |
| `TTL`, `PTTL`, `PERSIST` | Redis missing/no-expiration sentinel values |
| `TYPE` | `none`, `string`, `hash`, `list`, `set`, or `zset` |
| Hash/list/set/sorted-set commands | [54 supported collection commands, persistence and limits](docs/data-types.md) |
| `SELECT 0` | Only database 0 |
| `PGCACHE.STATS` | Extension-specific persistence counters |
| `COMMAND` | Supported command metadata and key discovery |
| `CLUSTER`, `ASKING`, `MIGRATE`, `PGCACHE.MIGRATION` | Opt-in [cluster subset and online resharding](docs/redis-cluster.md) |

Other commands/options return errors. Streams, Pub/Sub, Lua, transactions, WATCH, Redis replication and cluster-bus protocols,
Sentinel, eviction, `SET GET/KEEPTTL/EXAT/PXAT`, and conditional EXPIRE options remain unsupported.
Malformed/unsupported syntax errors are not all byte-for-byte Redis matches.

## Limits and remaining work

- String accounting includes key/value lengths plus 128 bytes per entry. Collections
  also account for decoded payloads and estimated item overhead. This is not
  an exact RSS limit; hash-table capacity, allocations, socket buffers, pending
  writes, and PostgreSQL memory add overhead.
- Maximum command frame: 1 MiB; maximum arguments: 1,024; maximum clients: 128.
  Slow clients are disconnected when queued responses exceed 2 MiB. Large `MGET`
  responses are rejected before copying values if they would exceed the limit.
- Pending mutations: direct mode permits 16 MiB by payload accounting. Journal
  mode defaults to 16 MiB and allows 16–128 MiB through
  `pg_cache.journal_pending_mb`. Accounting includes staged, queued, and currently
  committing mutations. Staged writes are handed off even
  when clients disconnect or send no more commands; clean shutdown hands off all
  remaining staged writes. The writer receives grouped event-loop
  handoffs, draining at most 1,024 additional handoffs before rechecking flush and
  shutdown conditions. It flushes once the collected distinct keys reach 8,192,
  their accounted size reaches 2 MiB,
  or the target interval elapses. A handoff
  can itself contain many commands; the byte budget bounds its size. Shutdown
  drains the remaining queue in one transaction.
  No eviction: memory-full writes receive `OOM`.
- Expiration is checked on reads; a full memory expiration sweep runs each second.
  Direct mode deletes up to 1,000 expired persisted entries every minute. Journal
  mode filters expired values during recovery but has no periodic table expiry
  sweep; expired table rows can remain until overwritten or deleted. High TTL churn
  needs a better expiration index/scheduler and more effective disk cleanup.
- In direct mode, existing keys use validated tuple-location hints and PostgreSQL's heap/index
  update helpers. New keys and stale hints use typed-array SQL `MERGE`; deletes
  use SQL. Values are never interpolated into SQL text. Hints are installed only
  after commit and have a separate 8-MiB accounting cap (key length plus 128 bytes
  per hint). Reaching the cap clears the hints; this can reduce performance on
  larger keyspaces. Frequent updates still generate WAL, dead tuples, and
  autovacuum work.
- The native path verifies the exact plain-heap column layout and one plain hash
  index on the key. Extra indexes, triggers, check constraints, RLS, or other
  incompatible metadata cause SQL fallback. It holds a table lock that serializes
  writers and maintenance during the batch; ordinary SQL readers remain allowed.
  Stale hints after row changes or physical rewrites are validated against the key
  and current relation bounds before use. Schema changes remain unsupported as
  an operational workflow; fallback checks protect against bypassing constraints.
- The worker runs with its PostgreSQL role's privileges (currently the default
  bootstrap superuser). Role isolation and a production deployment review remain.
- Disable preload and restart before dropping the extension or modifying its
  storage schema. Lifecycle DDL coordination and upgrades are not implemented.

## Linux ARM64 performance validation

The current hash-index/journal build was compared with Redis 8.2.11 using Redis
AOF `appendfsync everysec` and PostgreSQL 17.11 WAL-logged write-behind storage.
Measurements used one dedicated OCI A1 host with 4 CPU cores, 24 GiB RAM and a
500 GiB Balanced block volume. Clients and servers shared that host and used
loopback connections. PostgreSQL used 1 GiB shared buffers, 4 GiB `max_wal_size`,
a 512 MiB cache, a 128 MiB pending-write limit and two 256 MiB journal segments.
CNPG deployment tests were separate from these timing measurements.

Unmodified memtier 2.5.2 ran a 50/50 SET/GET workload with one million keys,
32-byte repeated payloads, pipeline 10, and 100 connections across four client
threads. Each server ran for 180 seconds per round, with alternating server order
over three rounds. Medians were:

| Metric | Flaro | Redis |
|---|---:|---:|
| Operations/s | 586,618 | 390,423 |
| p99 latency | 5.471 ms | 7.551 ms |
| p99.9 latency | 13.375 ms | 12.799 ms |
| Trailing durability barrier | 124 ms | 262 ms |
| Whole test cgroup CPU usage, including client | 3.04 cores | 1.87 cores |

Flaro delivered 1.50× throughput in this workload, with higher CPU usage and a
slightly higher p99.9. All rounds passed zero-error/miss checks, exact accepted
write counts, durability barriers and full PostgreSQL materialization. All one
million workload keys were verified after PostgreSQL restart.

The **unmodified stock Redis 8.2.11 benchmark** also ran three repetitions with
10,000 keys, 50 connections, one client thread and two million requests per
command. Flaro was faster in five of eight cases, including every pipelined case,
but 3–6% slower on small unpipelined operations and 4 KiB unpipelined reads:

| Value bytes | Pipeline | Command | Flaro median ops/s | Redis median ops/s | Ratio |
|---:|---:|---|---:|---:|---:|
| 64 | 1 | GET | 95,324 | 98,707 | 0.97× |
| 64 | 1 | SET | 96,297 | 102,323 | 0.94× |
| 64 | 16 | GET | 1,305,483 | 990,589 | 1.32× |
| 64 | 16 | SET | 839,983 | 518,135 | 1.62× |
| 4096 | 1 | GET | 85,970 | 90,510 | 0.95× |
| 4096 | 1 | SET | 74,358 | 43,609 | 1.71× |
| 4096 | 16 | GET | 415,110 | 349,956 | 1.19× |
| 4096 | 16 | SET | 203,169 | 56,400 | 3.60× |

All stock-client cases passed error, accepted-write count, durability and full
materialization checks. The stock and modified-client results below use different
payload behavior and client thread counts and should be assessed separately.

A separate **modified** Redis benchmark client generated fresh binary bytes for
every SET, used a monotonic clock and checked latency sample counts. This is not
the stock Redis benchmark. It used 10,000 keys, 50 connections, two client threads,
two million requests per command and three repetitions per case:

| Value bytes | Pipeline | Command | Flaro median ops/s | Redis median ops/s | Ratio |
|---:|---:|---|---:|---:|---:|
| 64 | 1 | GET | 156,556 | 137,504 | 1.14× |
| 64 | 1 | SET | 139,733 | 114,976 | 1.22× |
| 64 | 16 | GET | 1,392,758 | 995,520 | 1.40× |
| 64 | 16 | SET | 841,751 | 523,697 | 1.61× |
| 4096 | 1 | GET | 109,721 | 100,346 | 1.09× |
| 4096 | 1 | SET | 86,666 | 45,725 | 1.90× |
| 4096 | 16 | GET | 416,146 | 352,858 | 1.18× |
| 4096 | 16 | SET | 219,563 | 56,820 | 3.86× |

Every case passed error, accepted-write count, durability and full materialization
checks. Higher throughput did not imply lower latency at every percentile:
64-byte pipeline-1 SET p99 was 0.791 ms versus Redis's 0.735 ms.
4 KiB GET p99.9 was 2.711/6.831 ms for Flaro versus 2.055/6.015 ms for Redis at
pipeline 1/16. Pipelined latency samples describe batches rather than independent
command timings. Both systems acknowledged writes before durability; trailing
barriers were measured separately from foreground latency. These results cover
the stated workloads and do not establish performance for other commands,
concurrency levels, hardware or network paths.

References: [pgrx background workers](https://docs.rs/pgrx/0.16.1/pgrx/bgworkers/index.html),
[PostgreSQL background workers](https://www.postgresql.org/docs/17/bgworker.html),
[PostgreSQL hash indexes](https://www.postgresql.org/docs/17/hash-index.html),
[PostgreSQL MERGE](https://www.postgresql.org/docs/17/sql-merge.html),
[Redis RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/),
[Redis benchmarking](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/).

### Socket diagnostics

The optional `io-diagnostics` Cargo feature adds socket-call counters and duration
buckets, available through `PGCACHE.IOSTATS` and an orderly shutdown log. It is
compiled out of normal builds. The comparison harness detects this feature and
rejects instrumented timings as performance evidence. Benchmark artifacts and
local experiment tooling are excluded from this repository.
