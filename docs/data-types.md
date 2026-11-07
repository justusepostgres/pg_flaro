# Hashes, lists, sets and sorted sets

Flaro supports strings and four collection types through RESP2 and RESP3,
plus generic `TYPE`, `SORT`, and `SORT_RO` commands. Keys, hash fields, values,
and members are binary-safe. The table below is the supported command subset;
unlisted Redis commands and options are not implemented.

| Type | Supported commands |
|---|---|
| Hash | `HGETEX`, `HSETEX`, `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`, `HPERSIST`, `HRANDFIELD`, `HSCAN`, `HSET`, `HMSET`, `HSETNX`, `HGET`, `HGETDEL`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HINCRBY`, `HINCRBYFLOAT`, `HSTRLEN` |
| List | `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`, `RPOP`, `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPUSH`, `BLMPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LPOS`, `LINSERT`, `LMOVE`, `RPOPLPUSH`, `LMPOP`, `LSET`, `LTRIM`, `LREM` |
| Set | `SRANDMEMBER`, `SPOP`, `SSCAN`, `SINTERCARD`, `SMOVE`, `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SUNION`, `SINTER`, `SDIFF`, `SUNIONSTORE`, `SINTERSTORE`, `SDIFFSTORE` |
| Sorted set | `ZRANDMEMBER`, `ZSCAN`, `ZUNION`, `ZUNIONSTORE`, `ZINTER`, `ZINTERSTORE`, `ZINTERCARD`, `ZDIFF`, `ZDIFFSTORE`, `ZADD`, `ZINCRBY`, `ZREM`, `ZSCORE`, `ZMSCORE`, `ZCARD`, `ZCOUNT`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZRANGESTORE`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZLEXCOUNT`, `ZREMRANGEBYLEX`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZPOPMIN`, `ZPOPMAX`, `ZMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE` |

`HSET` accepts multiple field/value pairs and returns the count of newly added
fields. `HINCRBY` uses checked signed 64-bit arithmetic. List push commands accept
multiple values; `LPOP`/`RPOP` accept an optional count. List indexes/ranges support
negative offsets. Removing the final field, item, or member deletes the key.

`BLPOP` and `BRPOP` wait on multiple keys with fractional-second timeouts;
zero waits indefinitely. Other clients remain able to issue commands. Waiters
on a ready key run in arrival order before later pipelined producer commands.
Inside `EXEC`, a missing list returns null immediately. On wakeup, the original
key list is re-evaluated in argument order, matching the Redis 8.2.11 reference.
Timeout, disconnected-client, wrong-type, transaction and recovery cases have
live differential coverage. The initial scheduler polls waiting connections at
1 ms intervals and scans waiter interests when readiness changes; high waiter
counts and Linux ARM64 blocking throughput have not been benchmarked.

`BLMOVE` and `BRPOPLPUSH` wait only for their source, then atomically update
source and destination while preserving each key's TTL. A wrong-type destination
fails once a source item is available without consuming it. Same-key moves rotate
the list. `BLMPOP` and `BZMPOP` support multiple keys and an optional positive
`COUNT`; timeout is the first argument. `BZPOPMIN` and `BZPOPMAX` return a key,
member and score, with RESP3 scores encoded as doubles. All blocking variants
return immediately inside `EXEC` and use the same persistence admission and
wake scheduling as `BLPOP`/`BRPOP`. Streams and Pub/Sub are out of scope.

`CLIENT UNBLOCK id [TIMEOUT|ERROR]` cancels these blocking operations on the
same serving node. It returns 1 for a cancelled waiter and 0 for an absent or
nonblocking client. TIMEOUT returns the command's null reply; ERROR returns
`UNBLOCKED client unblocked via CLIENT UNBLOCK`. The connection stays usable and
subsequent pipelined commands resume. Within `EXEC`, cancellation is published
only after successful admission; `DISCARD` does not cancel the target.

`SRANDMEMBER` returns one member without a count, distinct members for a
nonnegative count, or samples with replacement for a negative count. `SPOP`
accepts an optional nonnegative count, removes only the returned members,
preserves TTL and deletes an emptied set. Persistence rejection leaves the
original set unchanged. Selection currently builds an array of member references,
so single-member selection is O(N); Redis complexity parity remains pending.

`HRANDFIELD` and `ZRANDMEMBER` use the same positive/negative count rules as
`SRANDMEMBER`. `WITHVALUES` and `WITHSCORES` require a count and return flat
field/value or member/score pairs over RESP2. They preserve the value and TTL.
Selection has the same O(N) member-reference allocation limitation as sets.

`INCRBYFLOAT` and `HINCRBYFLOAT` use a small C helper for native `long double`
arithmetic and Redis-style fixed decimal formatting. Parsing uses an explicit C
numeric locale; formatting temporarily selects it only on the calling thread.
The build requires a C compiler (already installed in the CNPG build image).
Precision follows the target platform's `long double`, as in Redis; it can differ
between macOS ARM64 and Linux ARM64. Values are persisted as the computed decimal
bytes, so replay does not repeat floating arithmetic. Hash-field TTL support is
still pending independently of these increments.

`HGETDEL key FIELDS numfields field ...` atomically returns and deletes fields.
Duplicate fields return a value only on the first occurrence; later occurrences
return null. Remaining fields retain the hash's key TTL, an empty hash is deleted,
and a request containing only missing fields does not enqueue persistence work.
Hash-field expiration remains unsupported.

`ZADD` supports `NX`, `XX`, `GT`, `LT`, `CH`, and single-pair `INCR`, including their
mutual-exclusion rules. Scores use binary64 arithmetic. NaNs and overflow are
rejected; infinities and hexadecimal double syntax are supported. Equal scores
sort by binary member order. `ZRANGE` supports rank ranges, `BYSCORE`, `BYLEX`, `REV`,
`WITHSCORES`, and `LIMIT` with score or lexicographic ranges. `ZRANGESTORE` stores the selected
members with scores, clears the destination TTL, and supports source=destination.
Lexicographic ranges require equal scores for defined Redis ordering. Score bounds support `(` for exclusion
and `-inf`/`+inf`. `ZRANK`/`ZREVRANK` support the basic form and optional `WITHSCORE`.
`ZPOPMIN`/`ZPOPMAX` accept an optional nonnegative count.

Missing-key results follow the supported Redis command semantics. Commands against
the wrong type return `WRONGTYPE`; Redis's special `MGET` behavior returns null for
non-string values. `SET` and `MSET` can replace a collection with a string. Generic
`DEL`, `UNLINK`, `EXISTS`, `TYPE`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, and `PERSIST` work across
types. Collection mutations preserve an existing key TTL. Set algebra store
commands replace the destination and clear its TTL, including when it is also an
input. Invalid arguments, capacity failures, and persistence admission failures do
not partially mutate a collection.

`HSCAN`, `SSCAN` and `ZSCAN` accept `MATCH` and a positive `COUNT` hint;
`HSCAN` also accepts `NOVALUES`. `SCAN` enumerates keys and supports `TYPE`.
Cursors are opaque; iterate until cursor zero, including after an empty page.
Scan ordering and page boundaries are not Redis cursor encodings. Stateless hash
ordering preserves continuously present entries across insertions and deletions.
Selection retains at most 1,025 hash groups, but each page currently examines the
whole source. A persistent scan index is still needed for Redis complexity parity.

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
- Requests accept Redis's signed-32-bit array-count range, with actual arguments
  bounded by the 1 MiB command frame. Large array replies are rejected
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
- Hash-field TTLs and Lua remain unsupported. WATCH/UNWATCH support optimistic transactions. MULTI/EXEC/DISCARD support
  transactions over implemented commands. RESP3 command replies and HELLO
  negotiation are supported; push notifications and client tracking are not.

## Hash-field expiration

HEXPIRE/HPEXPIRE and HEXPIREAT/HPEXPIREAT set relative or absolute field deadlines,
with NX/XX/GT/LT conditions and per-field replies. HTTL/HPTTL and
HEXPIRETIME/HPEXPIRETIME inspect deadlines; HPERSIST clears them. All use the
FIELDS count field-list syntax, including duplicate-field behavior. Existing key
TTLs survive field changes. HSET clears a field deadline, numeric increments
preserve it, and deleting a field removes its metadata.

Expired fields are removed before command execution and by periodic maintenance;
removing the final field removes the hash. Expiration invalidates WATCH for live
watched hashes, and transaction rollback restores values, versions and expiry
scheduling. Like whole-key expiration, cleanup needs no new write admission:
persisted deadlines allow recovery to repeat cleanup. Physical PostgreSQL rows
can retain expired fields until a later write replaces them. An ordered deadline
index holds one entry per expiring hash; cleanup visits only
due hashes, without scanning unrelated cache keys. Persistent tree snapshots keep
transaction setup constant-time and restore the schedule on rollback. Each hash
also indexes field deadlines, so finding and removing due deadlines
avoids a scan over unrelated field expirations. Modified hashes still require
copying and serialization; Linux ARM64 benchmarks remain pending.

Ordinary hashes retain COL1 encoding. A hash with deadlines uses COL2 under the
same hash data-type tag: each field/value pair gains a length-prefixed expiration
field, empty or an eight-byte little-endian signed timestamp. Only hashes accept
COL2; clearing the last deadline returns to COL1. Encoded-size and memory accounting
include metadata. Timestamped hashes use generic writes until incremental accounting
is implemented. Older binaries cannot read COL2 records. Direct/journal recovery
and cluster migration tests preserve deadlines and discard already-expired fields.
HGETEX returns fields and optionally applies EX/PX/EXAT/PXAT or PERSIST. Past
expirations remove each returned field immediately, so later duplicates return nil.
HSETEX supports FNX/FXX across the original requested fields and expiration options
or KEEPTTL; default writes clear field deadlines. Conditional failures make no
writes. Successful set-then-expire operations invalidate WATCH even when no key
remains. Read-only and no-op HGETEX paths borrow the hash without copying it.
Broader interaction/clock-edge audits and per-hash cleanup optimization remain
pending.

## Geospatial sorted sets

GEOADD, GEOPOS, GEOHASH and GEODIST use ordinary sorted sets with Redis's
52-bit interleaved coordinate scores. GEOADD supports NX, XX and CH; all
coordinates are validated before changes become visible, and updates retain
key TTLs. Admission failures leave the value and WATCH versions unchanged.
GEOPOS returns decoded cell centers, GEOHASH returns the standard 11-character
representation, and GEODIST supports meters, kilometers, feet and miles.
RESP3 positions use doubles; distances remain fixed-point bulk strings.

Coordinate and distance comparisons are pinned to the macOS ARM64 Redis 8.2.11
reference. Decoding uses fused arithmetic to match that reference's cell centers;
Linux ARM64 and other compiler/architecture combinations still require validation.
GEOSEARCH and GEOSEARCHSTORE support FROMMEMBER/FROMLONLAT, BYRADIUS/BYBOX,
ASC/DESC, COUNT/ANY and result details (WITHDIST/WITHHASH/WITHCOORD). Stored results
replace the destination atomically, clear its TTL, and support STOREDIST and
source/destination aliases. Empty results delete an existing destination.
Search derives up to nine neighboring geohash cells, prunes cells outside the
shape bounds, and seeks into the sorted-set index for each score range. It uses
the reference's center/cardinal/diagonal order, including duplicate-cell handling,
so unsorted results and COUNT ANY match the exercised reference cases. A local
query test visits three candidates while skipping 10,000 unrelated indexed entries.
Broader boundary/floating-point audits and Linux ARM64 performance comparisons
remain pending; this does not establish Redis-level throughput or latency.
GEORADIUS, GEORADIUSBYMEMBER and their _RO variants share the same selection
engine. Writable variants support STORE/STOREDIST with the last destination
winning when options repeat. Read-only variants reject storage options. Cluster
key discovery includes only the source and the final destination. Member-based
radius queries skip radius/unit validation when the source key is absent, matching
the reference's validation order. Sorted-set persistence and
migration apply to geospatial values without a separate database schema.

## HyperLogLog strings

`PFADD key [element ...]`, `PFCOUNT key [key ...]` and
`PFMERGE destination [source ...]` operate on ordinary binary-safe Redis strings.
They use the Redis HYLL header, 16,384 six-bit registers, MurmurHash64A and the
Ertl estimator. Both sparse and dense sketches can be exchanged with Redis
through GET/SET. Sparse layouts are canonically rebuilt and need not be byte-for-
byte identical to Redis's incremental layout; register semantics are preserved.
Dense sketches occupy 12,304 bytes including the header. Sparse output promotes
at the default 3,000-byte ceiling or when a rank exceeds the sparse format's range.
Existing dense inputs remain dense.

PFADD creates an empty sketch when called without elements on a missing key;
repeated elements that change no registers return zero without persistence work.
PFMERGE includes the destination's existing sketch in its union and preserves
its TTL. A missing destination is created without a TTL. Multi-key PFCOUNT
computes a union without modifying inputs. Single-key PFCOUNT refreshes the
cached count when needed; this changes the string and invalidates WATCH, as in
Redis. These cache writes pass through the same persistence admission as data
writes and retain the key TTL. Cache hits do not mutate the key.

PFDEBUG supports GETREG, DECODE, ENCODING and TODENSE. GETREG and TODENSE
convert sparse sketches to dense format through persistence admission while
preserving the count cache, TTL and WATCH versions. DECODE describes the stored
sparse representation, so its output can differ for independently built sketches
with equivalent registers. SIMD ON/OFF reports disabled: this implementation has
no optional AVX2 path. Invalid SIMD arguments return one error; Redis 8.2 also
emits an extra status reply for this invalid input, which is not reproduced.

PFSELFTEST runs diagnostics without accessing database keys: 1,000 randomized
packed-register round trips and cardinality checkpoints up to ten million
unique elements, comparing dense and sparse encodings and estimator error bounds.
It returns OK only after these checks pass. The diagnostic uses a reproducible
input seed and blocks the command-processing loop while it runs, like Redis.

Dense PFADD edits packed registers and copies the string only after the first
actual register change. Sparse updates currently unpack/rebuild the sketch;
changed values still use whole-string persistence snapshots. Broader forged-
payload audits and Linux ARM64 performance comparisons remain
pending. This does not establish Redis-level HyperLogLog performance.

## Validation

Validated on macOS ARM64 / PostgreSQL 17.11: 116 Rust tests and 19,522 differential/contract checks for each combination of
RESP2/RESP3 and direct/journal persistence, using Redis 8.2.11
and redis-py 6.4.0. They cover binary values, wrong types, duplicate members,
negative indexes, score ordering, random collection selection, conditional updates, empty-key deletion,
TTL preservation, old-schema upgrade, and crash recovery in direct and journal
modes. Journal tests include both unmaterialized records and compacted rows.
Three-shard tests migrate every collection kind and verify it after restart.
The current compatibility implementation passes 140 Rust tests and 21,751 checks in
RESP2 direct and RESP3 journal modes, including HyperLogLog self-tests, command
discovery and geospatial crash recovery. The three-shard RESP3 journal suite also
passes geospatial routing, online migration and recovery.

```sh
PGRX_PG_CONFIG_PATH=/path/to/pg_config cargo build -p pg-cache --locked --features journal-prototype
PGRX_PG_CONFIG_PATH=/path/to/pg_config python scripts/collections_integration.py --redis-server /path/to/redis-server
PGRX_PG_CONFIG_PATH=/path/to/pg_config python scripts/collections_integration.py --journal --redis-server /path/to/redis-server
PGRX_PG_CONFIG_PATH=/path/to/pg_config python scripts/collections_integration.py --protocol 3
PGRX_PG_CONFIG_PATH=/path/to/pg_config python scripts/collections_integration.py --protocol 3 --journal
PGRX_PG_CONFIG_PATH=/path/to/pg_config python scripts/cluster_integration.py
PGRX_PG_CONFIG_PATH=/path/to/pg_config python scripts/cluster_integration.py --journal
cargo test --locked
```

Redis command references: [HSET](https://redis.io/docs/latest/commands/hset/),
[LPOP](https://redis.io/docs/latest/commands/lpop/),
[ZADD](https://redis.io/docs/latest/commands/zadd/), and
[ZRANGE](https://redis.io/docs/latest/commands/zrange/).


`SORT` and `SORT_RO` accept lists, sets and sorted sets. They support numeric
sorting, `ALPHA`, `ASC`/`DESC`, `LIMIT`, `BY`, repeated `GET`, `GET #`, and hash-field
patterns. `SORT STORE` writes a list atomically through persistence admission,
clears destination expiration, turns missing GET results into empty strings, and
deletes the destination when the result is empty. Source/destination overlap is
supported. Cluster STORE destinations must share the source slot; external
patterns must identify that same slot before a wildcard, as in `{tag}weight:*`.

Sorting currently processes the full collection even for a small LIMIT. ALPHA
uses C-locale byte collation (and Redis's NUL-terminated comparison behavior
without STORE); configurable locale collation is not implemented. Existing
response/value size limits apply. Equal ALPHA weights and unsorted set traversal
do not promise a stable order.


Transactions: MULTI queues commands without changing cache data; EXEC executes
against speculative state, preserving individual runtime errors, then admits
one final persistence batch. Invalid command names/arities poison the queue and
cause EXECABORT; DISCARD and RESET drop it. Backlog pressure defers EXEC while
preserving the queue. RESP3 encoding uses each queued command's reply shape.
Transactions are limited to 4,096 commands and 1 MiB of accounted queue storage;
existing 2 MiB response and persistence batch limits also apply. Oversized batches
are rejected without applying writes. Checkpoint setup is O(1); rollback records
and persistence coalescing depend on changed keys, while individual command costs
still apply. Transaction performance parity is not established.
Unsupported administrative commands and detailed migration-time
transaction compatibility remain pending.


WATCH/UNWATCH use key-local mutation versions, so writing the same value or
changing a key and restoring its old value still invalidates the guard. Collection
no-ops do not invalidate it. Expiration after WATCH aborts EXEC; a key already
expired at WATCH is observed as absent. Guards clear on UNWATCH, DISCARD, RESET,
completed/aborted EXEC and disconnect. Queued UNWATCH executes at EXEC, after
conflict detection. Rejected persistence admission does not publish speculative
watch versions. Watched-key metadata is limited to 1 MiB per connection and is
reclaimed during engine maintenance after guards are dropped. Redis-py's standard
optimistic pipeline pattern is covered in both protocols and persistence modes.


## UNLINK and RANDOMKEY

UNLINK deletes each distinct live key once, across every supported type. It admits
persistence before removing keys or invalidating WATCH, then hands retained values
to a dedicated non-PostgreSQL worker for reclamation. Transactions retain those
values until commit; rollback restores keys, deadlines and WATCH versions without
publishing deletions. The reclamation queue can temporarily retain memory after
logical deletion, and persistence snapshots or copied keys may keep values alive
longer. The worker falls back to ordinary destruction if its channel has failed.

RANDOMKEY returns a uniformly selected live key on the addressed shard, or null
when none exists. It filters expired keys and handles binary names. Reservoir
sampling uses constant auxiliary storage but scans the local key table; Redis
complexity parity requires a random-access key index.


## Redis DUMP export

DUMP exports the stored value as a Redis RDB v12 object with a little-endian version
footer and CRC64 checksum. It returns null for missing or expired keys, does not
include whole-key TTL, and preserves absolute hash-field deadlines. Payloads can
be imported using native Redis RESTORE. PostgreSQL persistence continues using the
existing COL1/COL2 encoding; DUMP does not alter those records.

The exporter writes raw strings and plain collection encodings accepted by Redis,
including the legacy list encoding, binary sorted-set scores, and hash metadata
with relative field deadlines. It does not reproduce Redis's optional integer,
LZF or listpack encoding choices, so payload bytes and sizes can differ while
restoring the same value. Payloads are bounded by the existing response limit; full Redis size limits
and interchange across older/newer Redis versions remain unverified.


## Redis RESTORE import

RESTORE validates the version/checksum and decoded value before persistence
admission or replacing the destination. It supports REPLACE, relative TTL and
ABSTTL, with native error ordering for options, BUSYKEY, TTL, checksum and malformed
data. Past key deadlines delete an existing destination only after admission;
transaction rejection restores data, deadlines and WATCH state. Imported hash
field deadlines remain absolute and survive PostgreSQL persistence and migration.

The decoder accepts raw/integer/LZF strings, plain collections, integer sets,
listpacks, packed/plain quicklist nodes, and historical ziplists/zipmaps/quicklists.
Hash field-expiration metadata supports both pre-GA and current layouts. Decoding
bounds input reads, declared counts and decompression allocations. It rejects
invalid checksums, malformed packed structures, duplicate members and NaN scores.
Deliberately malformed payload comparisons enable Redis's strict
`sanitize-dump-payload yes` mode: default Redis can accept some invalid duplicate
hash payloads that Flaro rejects. This strictness is a remaining behavior difference.

IDLETIME and FREQ syntax/ranges are validated, but their access/eviction metadata
is not retained yet. Streams are intentionally excluded; module payloads are not
supported. Legacy payloads are tested through Redis 8.2.11 RESTORE, not by running
every historical Redis release. These limits, resource limits and broader malformed
payload/version audits remain compatibility work.
