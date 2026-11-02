# pg_cache

Experimental PostgreSQL 17 extension written with Rust/pgrx. It serves a documented
Redis RESP2 command subset over TCP, keeps the live dataset in memory, and persists
writes asynchronously into **ordinary WAL-logged PostgreSQL tables**.

This is a working first version, not a full Redis replacement. Beating Redis is a
performance target, not a general performance guarantee. The deployment target is
Linux ARM64. See [the CNPG deployment notes](deploy/cnpg/README.md) for current
deployment limitations.

## Architecture and durability

```text
Redis client → loopback TCP / RESP2 → Rust event loop + in-memory map
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
  can increase it. A full queue pauses that connection's command processing for
  up to 100 ms; if still full, it returns `MISCONF` without applying the command.
  Reads on other connections continue. Commands on a blocked connection retain
  their original order.
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
Exactly one persistence database and one listener are supported per cluster.

```sh
redis-cli -h 127.0.0.1 -p 6380 SET greeting hello PX 60000
redis-cli -h 127.0.0.1 -p 6380 GET greeting
redis-cli -h 127.0.0.1 -p 6380 PGCACHE.STATS
```

Use RESP2 clients, database 0, no authentication, and disable optional unsupported
connection handshakes if your library treats their errors as fatal. `HELLO`/RESP3,
`AUTH`, and `CLIENT` are not implemented. The listener is deliberately restricted
to `127.0.0.1`; PostgreSQL's `pg_hba.conf` does not authenticate RESP connections.
Any local process that can connect can read/write the cache. TLS, Redis ACLs,
multi-tenancy, and remote deployment require additional work.

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

Keys and values are binary-safe strings. Commands are case-insensitive. TCP
fragmentation, multiple connections, and pipelining are supported. Only RESP arrays
of bulk strings are accepted; inline commands are not supported.

| Commands | Supported behavior |
|---|---|
| `PING`, `ECHO`, `QUIT` | Basic connection commands |
| `GET`, `MGET` | Missing/expired keys return null bulk strings |
| `SET` | `NX` or `XX`; `EX seconds` or `PX milliseconds`; overwrites clear old TTL |
| `MSET` | Atomic within the cache, including on memory/backlog rejection |
| `DEL`, `EXISTS` | Multiple keys; Redis duplicate-key counting behavior |
| `INCR` | Signed 64-bit integer semantics, checked overflow, retains TTL |
| `EXPIRE`, `PEXPIRE` | Basic form only; nonpositive TTL deletes immediately |
| `TTL`, `PTTL`, `PERSIST` | Redis missing/no-expiration sentinel values |
| `SELECT 0` | Only database 0 |
| `PGCACHE.STATS` | Extension-specific persistence counters |

Other commands/options return errors. In particular: no hashes, lists, sets, sorted
sets, streams, Pub/Sub, Lua, transactions, WATCH, replication protocol, Cluster,
Sentinel, eviction, `SET GET/KEEPTTL/EXAT/PXAT`, or conditional EXPIRE options.
Malformed/unsupported syntax errors are not all byte-for-byte Redis matches.

## Limits and remaining work

- Cache accounting includes key/value lengths plus 128 bytes per entry. It is not
  an exact RSS limit; hash-table capacity, allocations, socket buffers, pending
  writes, and PostgreSQL memory add overhead.
- Maximum command frame: 1 MiB; maximum arguments: 1,024; maximum clients: 128.
  Slow clients are disconnected when queued responses exceed 2 MiB. Large `MGET`
  responses are rejected before copying values if they would exceed the limit.
- Pending mutations: at most 16 MiB by payload accounting, including staged,
  queued, and currently committing mutations. Staged writes are handed off even
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
  Up to 1,000 expired persisted entries are deleted every minute. High TTL churn
  needs a better expiration index/scheduler and more effective disk cleanup.
- Existing keys use validated tuple-location hints and PostgreSQL's heap/index
  update helpers. New keys and stale hints use typed-array SQL upsert; deletes
  use SQL. Values are never interpolated into SQL text. Hints are installed only
  after commit and have a separate 8-MiB accounting cap (key length plus 128 bytes
  per hint). Reaching the cap clears the hints; this can reduce performance on
  larger keyspaces. Frequent updates still generate WAL, dead tuples, and
  autovacuum work.
- The native path verifies the exact plain-heap column layout and one immediate
  primary index. Extra indexes, triggers, check constraints, RLS, or other
  incompatible metadata cause SQL fallback. It holds a table lock that serializes
  writers and maintenance during the batch; ordinary SQL readers remain allowed.
  Stale hints after row changes or physical rewrites are validated against the key
  and current relation bounds before use. Schema changes remain unsupported as
  an operational workflow; fallback checks protect against bypassing constraints.
- The worker runs with its PostgreSQL role's privileges (currently the default
  bootstrap superuser). Role isolation and a production deployment review remain.
- Disable preload and restart before dropping the extension or modifying its
  storage schema. Lifecycle DDL coordination and upgrades are not implemented.

References: [pgrx background workers](https://docs.rs/pgrx/0.16.1/pgrx/bgworkers/index.html),
[PostgreSQL background workers](https://www.postgresql.org/docs/17/bgworker.html),
[Redis RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/),
[Redis benchmarking](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/).

### Socket diagnostics

The optional `io-diagnostics` Cargo feature adds socket-call counters and duration
buckets, available through `PGCACHE.IOSTATS` and an orderly shutdown log. It is
compiled out of normal builds. The comparison harness detects this feature and
rejects instrumented timings as performance evidence. Benchmark artifacts and
local experiment tooling are excluded from this repository.
