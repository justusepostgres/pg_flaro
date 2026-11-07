# Redis compatibility work tracker

The objective is Redis compatibility beyond the currently supported subset,
with Streams and Pub/Sub explicitly excluded from the target at the user's request.
These exclusions are intentional and are not pending implementation work.
Redis 8.2.11 is the initial executable reference already used by this repository.
Newer Redis releases require a separate version delta audit; passing the existing
suite does not prove full compatibility with either version.

## Verification gates

For each command: compare successful replies and errors against the reference,
including binary input, missing/expired keys, wrong types, boundary numbers,
options, arity, and duplicate keys. For mutations also verify atomic rejection,
TTL effects, persistence and recovery. Verify COMMAND metadata and cluster key
routing. Protocol changes need real-client tests, not only engine tests.

## Work remaining

- Finish strings, generic key commands, expiration options and scan complexity parity.
- Finish hashes, lists, sets and sorted sets, including blocking operations,
  random selections and field expiration; audit remaining command options.
- Complete transaction edge cases and performance parity; implement scripting and Functions.
- Complete HyperLogLog performance and malformed-payload audits;
  finish geospatial boundary/rounding audits and performance comparisons;
  audit the reference command catalog for additional types and commands.
- Complete remaining handshakes, CLIENT behavior and database selection; RESP3
  replies and HELLO negotiation are implemented for supported commands.
- Implement authentication, ACLs and TLS.
- Implement eviction, memory/size/client limit compatibility and administration.
- Implement and verify Redis replication/Sentinel/cluster interoperability,
  native cluster administration, replica reads and Redis data interchange.
- Audit exact errors, command introspection, complexity, durability semantics and
  client libraries. The current PostgreSQL write-behind architecture is not
  evidence of native Redis replication or persistence compatibility.
- Compare later Redis versions to the pinned reference before making an
  unqualified compatibility claim.

## Implemented increments

- APPEND, STRLEN, DECR and DECRBY, including binary strings, TTL preservation,
  integer boundaries and rejected persistence admission.
- GETDEL, GETSET, SETNX, MSETNX, GETRANGE/SUBSTR and SETRANGE. Range tests cover
  extreme signed indexes, empty writes, zero-filled gaps and binary data.
  MSETNX checks original key existence and applies duplicate-key writes atomically.
- Cluster command metadata and real RedisCluster routing tests for these commands,
  including cross-slot rejection of MSETNX.

- EXPIREAT, PEXPIREAT, EXPIRETIME and PEXPIRETIME; EXPIRE/PEXPIRE and
  absolute variants accept NX/XX/GT/LT, including valid combinations and repeated
  options. The option matrix is compared across all five supported data types.

- SET GET/KEEPTTL/EXAT/PXAT and GETEX with PERSIST or expiration options.
  Repeated options and validation ordering follow the reference. Tests preserve
  raw SET responses rather than redis-py's boolean coercion, so old-value replies
  and condition-failure replies are actually compared.

- RENAME, RENAMENX and COPY preserve value types and absolute expiration.
  COPY supports REPLACE and DB 0; other databases remain an open compatibility
  requirement. Tests verify source/destination isolation after collection writes.

- GETBIT, SETBIT and BITCOUNT with BYTE/BIT range options. Writes preserve
  TTLs, unchanged bits avoid persistence writes, and sparse growth is bounded
  before allocation. BITPOS supports BYTE/BIT ranges, explicit-end behavior and
  implicit zero padding.
- BITOP supports AND/OR/XOR/NOT and Redis 8.2 DIFF/DIFF1/ANDOR/ONE.
  Inputs are read before destination replacement, TTL is cleared, and empty
  results delete the destination. Randomized comparisons include overlapping
  source/destination keys and unequal lengths.
- BITFIELD and BITFIELD_RO support signed/unsigned fields, scaled offsets,
  sequential GET/SET/INCRBY and WRAP/SAT/FAIL. Parsing and persistence rejection
  are atomic; reads after writes see the new bits. Tests cover Redis 8.2's signed
  saturation behavior at extreme i64 inputs and zero-extension on failed writes.

- LPOS supports RANK/COUNT/MAXLEN, including negative ranks and duplicate
  options. LINSERT supports BEFORE/AFTER the first matching pivot, with TTL
  preservation and atomic admission. Both use collection command discovery.

- LMOVE and RPOPLPUSH atomically update both keys, preserve independent TTLs,
  delete emptied sources and support same-key rotation. Cluster metadata routes
  both source and destination; cross-slot tests cover both commands.

- SMOVE supports atomic source/destination updates, same-key no-ops, independent
  TTLs and empty-source deletion. Existing destination members avoid redundant
  destination writes. Differential tests cover missing and wrong-type inputs.

- ZRANK/ZREVRANK support WITHSCORE, including null-array replies for absent
  members. The new RESP2 null-array representation also corrects missing-key
  LPOP/RPOP with a count; byte-level unit tests distinguish it from null bulk.

- SINTERCARD supports dynamic key counts and LIMIT, including duplicate keys
  and repeated LIMIT options. COMMAND advertises movablekeys and GETKEYS excludes
  the count and options. Counting stops at LIMIT without building a result set.

- LMPOP supports first-nonempty-list selection, LEFT/RIGHT, COUNT, null-array
  replies and dynamic key discovery. Pops reserve persistence capacity before
  changing the selected list; other listed keys are untouched.

- ZMPOP supports MIN/MAX, COUNT and dynamic key discovery, sharing parsing
  and atomic persistence with LMPOP. Nested member/score replies preserve tie
  ordering; the first nonempty sorted set is selected.

- ZRANGESTORE stores rank/score/lex ranges, clears destination TTL and handles
  source=destination atomically. ZRANGE also supports BYLEX/REV/LIMIT. Lex ranges
  are tested on equal-score members, as required for defined Redis ordering.

- ZRANGEBYLEX/ZREVRANGEBYLEX, ZLEXCOUNT and ZREMRANGEBYLEX cover lexical
  selection, counting and atomic removal. They share binary bound handling with
  ZRANGE BYLEX and are tested with equal-score sets.

- ZDIFF/ZDIFFSTORE support set and sorted-set inputs, WITHSCORES on reads,
  destination/source overlap and atomic replacement with TTL clearing. Dynamic
  key discovery includes the store destination as well as counted source keys.

- ZUNION/ZUNIONSTORE and ZINTER/ZINTERSTORE support WEIGHTS, SUM/MIN/MAX,
  mixed set/sorted-set inputs, repeated keys and atomic destination replacement.
  ZINTERCARD supports LIMIT. Tests cover infinite scores, zero weights, TTL
  clearing, destination/source overlap and persistence admission rejection.

- LCS supports binary strings, LEN, IDX, MINMATCHLEN and WITHMATCHLEN.
  Differential tests cover tie-breaking, reverse match order, repeated options,
  missing keys and type errors. Temporary table allocation is bounded to 1 MiB,
  smaller than Redis's default limit; matching larger inputs remains a limit gap.

- SCAN/HSCAN/SSCAN/ZSCAN support opaque stateless cursors, MATCH/COUNT,
  SCAN TYPE and HSCAN NOVALUES. Full-iteration differential tests compare
  unordered results rather than requiring identical Redis page boundaries.
  Tests cover binary glob syntax, cursor parsing, missing/type-error precedence,
  real cluster client iterators and continuously present keys during mutations.
  Each page still examines the full source; a persistent scan index remains
  required for Redis's scan complexity. Scan selection memory is bounded by
  1,025 hash groups; response limits remain as documented for other commands.

- KEYS shares binary glob matching with SCAN, filters expired entries and bounds
  response allocation. DBSIZE counts resident entries, including expirations not
  yet reaped. TOUCH counts existing arguments including duplicates and leaves
  values/TTLs unchanged. TOUCH access-time/frequency updates remain pending with
  eviction metadata; its current support is limited to lookup/count behavior.

- SRANDMEMBER and SPOP support single-member and counted replies, distinct
  positive-count selection, negative-count SRANDMEMBER sampling with replacement,
  TTL preservation and atomic pop admission. Tests check random-result invariants
  independently against both servers and recover the actual post-pop snapshot.
  Selection currently builds an array of member references, so O(1) random-member
  access remains a complexity gap.

- HRANDFIELD/ZRANDMEMBER share the random sampler and implement positive
  distinct counts, negative sampling with replacement, WITHVALUES/WITHSCORES,
  paired-count overflow checks and RESP2 flat pairs. Tests validate membership,
  uniqueness, pair values/scores, TTL preservation, output bounds and cluster
  routing. Their single-member selection also remains O(N), pending indexed
  random access. Hash-field expiration remains a separate compatibility gap.

- INCRBYFLOAT/HINCRBYFLOAT use native long-double arithmetic through a small
  C shim, preserve key TTLs and store the computed decimal result atomically.
  Raw reply/GET/HGET comparisons cover decimal repetition, hex floats, negative
  zero, invalid syntax, infinities, range failures and input length bounds.
  Parsing uses a C locale and formatting restores the calling thread's locale.
  Linux ARM64 validation of this new shim remains pending; this increment was
  verified on macOS ARM64. Hash-field expiry remains unsupported.

- HGETDEL supports FIELDS/count validation, ordered replies, duplicate-field
  nulls, TTL preservation and atomic deletion. Tests cover no-op admission,
  last-field deletion and partial hashes after restart. The journal test exposed
  incorrect incremental hash size accounting for its FIELDS/count prefix; this
  is corrected, and unknown hash commands now fall back to full recalculation.
  A regression assertion compares incremental sizes with the canonical encoding
  and full memory calculation. Hash-field expiration remains pending.

- Inline request framing supports LF/CRLF, single/double quotes, quoted escapes,
  binary hexadecimal escapes, blank lines and mixed inline/RESP pipelines.
  RESP arrays retain borrowed argument storage. Live differential socket tests
  verify fragmentation, unbalanced-quote errors and connection closure; unit
  tests cover every prefix of a binary quoted request. Inline lines are bounded
  to 64 KiB, with the existing argument count ceiling. Remaining
  protocol edge cases remain open.

- RESP array headers accept zero and negative counts as empty commands and
  leave following pipelined commands intact. Numeric headers require canonical
  signed decimal syntax; leading zeros, plus signs, negative zero and overflow
  are rejected. Live tests compare exact numeric-header error replies and
  connection closure with Redis. Existing argument/frame limits remain, as does
  stricter CRLF validation for malformed requests and early rejection of overly
  long unfinished numeric headers.

- CLIENT ID/SETNAME/GETNAME/SETINFO maintain per-connection IDs, names and
  LIB-NAME/LIB-VER metadata. Exact differential replies cover empty names,
  invalid attributes, embedded NUL behavior and wrong arity. Tests verify
  connection isolation, stable IDs, pipelined updates and redis-py client_name
  initialization. CLIENT INFO/LIST, tracking, reply control, management commands
  and complete subcommand introspection remain pending.

- RESET clears the client name and ASKING permission, retaining connection ID
  and library metadata. It leaves cached data untouched. Tests cover pipelining,
  identity preservation and revoked ASKING permission during migration. Resetting
  authentication, transactions and other unimplemented connection modes
  remains tied to implementing those features. TIME returns decimal bulk-string
  seconds/microseconds, checked against wall-clock bounds and Redis reply shape.

Latest validation: 105 Rust tests and Clippy passed. Real PostgreSQL tests passed
17,870 Redis differential/contract checks in each of the four RESP2/RESP3 and
direct/journal combinations, including
crash/recovery. Three-shard RedisCluster integration passed in journal mode
through CLIENT connection metadata, including inline MOVED replies. Direct-mode cluster tests passed through RESET/TIME, including
explicit all-primary aggregation and node-local counts. Redis-py defaults KEYS
and DBSIZE to its default node unless all primaries are requested.
Dynamic-key discovery and cross-slot rejection are
covered with the real RedisCluster client. The list-pop null-array correction
is included in the rebuilt PostgreSQL binary and covered by byte-level unit tests.
The standalone socket lifecycle suite also passed after inline framing was added,
covering pipelining, binary values, backlog bounds, crash/TTL recovery and graceful
drain.

The reference rejects DECRBY with -9223372036854775808 regardless of the old
value; this boundary behavior is explicitly covered. Differential error checks
currently compare error classes, not all exact messages. Flaro's existing string
size and output limits remain compatibility gaps.

This tracker is a work queue, not a compatibility certification. The command
catalog, protocol, security and distributed-system gates above remain open.


RESP3 increment: HELLO negotiates RESP2/RESP3 with SETNAME and default-nopass
AUTH options. Failed negotiation preserves protocol and name; RESET restores
RESP2. Supported replies use maps, sets, doubles, nested score pairs and RESP3
nulls. Live redis-py tests cover switching on the same connection. HELLO reports
Flaro's product/version and an empty module list; configurable ACLs and password
enforcement are still absent. Push notifications and client tracking remain
unsupported. Streams and Pub/Sub remain explicitly out of scope.

COMMAND descriptions include the ACL-category field required by redis-py's
RESP3 cluster discovery. The later discovery increment populates descriptive
categories; ACL enforcement and full modern command key specifications remain
pending. The socket lifecycle suite passed after RESP3 changes.

RESP3 three-shard integration passed in both direct and journal modes with
redis-py 6.4.0: discovery, MOVED/ASK/ASKING, CROSSSLOT, pipelines, TTL migration,
interrupted transfers, durable topology and crash recovery.


SORT increment: SORT and SORT_RO support list/set/zset numeric and C-locale
alpha ordering, ASC/DESC, LIMIT, constant BY (native-order traversal), external
string/hash-field BY/GET patterns and repeated GET. STORE writes an admitted
list replacement, supports source overlap, clears destination TTL, replaces null
GETs with empty strings and deletes an empty destination. Tests cover failed
admission, exact syntax/numeric errors, NUL-terminated numeric parsing, repeated
options, and recovery from journal overlays and compacted rows. 625 new checks
run in all four protocol/persistence combinations. Three-shard RESP3 direct-mode
integration passed with SORT STORE routing and external-pattern slot checks.
Journal cluster validation remains at the previous RESP3 increment.

SORT limitations still requiring work: full-sort complexity for small LIMIT,
configurable non-C locale collation, and existing size ceilings. Unsorted set
order and equal ALPHA weights are unspecified; tests compare unordered GET pairs
only when all external ALPHA weights are equal. Dynamic external-key behavior
during slot migration still needs dedicated differential coverage.


Transaction execution groundwork (not yet exposed as Redis commands):
`Engine::transact` runs commands against a speculative key directory, returns
per-command runtime errors without discarding other successful commands, and
submits one final mutation per changed key in one admission call. Admission
failure leaves live entries, expiry tracking and memory accounting unchanged.
Read-only batches do not need persistence admission. Six new tests cover
read-your-writes, repeated writes/deletes, journal encoding/replay, typed-value
sharing, expiry rollback, retry after backlog rejection, and coherent FIFO and
coalescing handoff snapshots. 95 Rust tests, Clippy and the PostgreSQL extension
build passed. These unit tests do not establish live Redis transaction support
or PostgreSQL transaction crash recovery.

Next transaction work: connection queues and dirty-transaction errors; MULTI,
EXEC, DISCARD, WATCH/UNWATCH; RESP3 per-command reply encoding; cluster routing
and slot migration checks; reset/disconnect cleanup; bounded queue/output
accounting; real redis-py transaction tests and persistence/crash recovery.
The initial engine primitive cloned the entire key directory (immutable values
shared storage), with O(database keys) setup. The touched-key rollback increment
below replaces that implementation. The later wire integration is recorded below.


Transaction wire increment: MULTI/EXEC/DISCARD now use the speculative engine.
Connection-local metadata is staged alongside cache changes; per-command RESP3
encoding preserves map/set/double/null reply shapes. Queue-time unknown command,
arity and supported subcommand errors poison the transaction. Invalid EXEC arity
immediately discards it with EXECABORT. RESET drops the queue. Same-slot cluster
transactions validate source/destination slots while queuing and recheck routing
at EXEC. Oversized queues/responses/persistence batches are rejected; temporary
backlog pressure preserves the queue for retry without publishing metadata or
cache writes. Three new unit tests cover queue limits, output rollback and retry.
Live redis-py default transaction pipelines, isolation before EXEC, runtime errors,
DISCARD/RESET, failed queue validation, and persistence/recovery pass differential
checks against Redis 8.2.11. Direct-mode three-shard RESP3 tests passed including
same-slot transactions and cross-slot rejection.

Remaining transaction gates: WATCH/UNWATCH and mutation tracking; full admin and
subcommand coverage; native-cluster differential tests during slot migration,
ASKING and ownership changes; per-transaction timestamp/backlog-retry semantics;
transaction performance parity; queue/response/configuration limit parity. These
remain open requirements; initial MULTI support is not full transaction parity.


WATCH increment: WATCH/UNWATCH now track admitted mutations with per-key versions
and weak lifetime references. Failed EXEC admission restores original versions,
so a failed write cannot invalidate another connection's watch. Expiration guards distinguish keys expiring after WATCH from already
expired keys. Missing-key deletion and unchanged ZINCRBY scores do not spuriously
invalidate guards. Guard cleanup is wired to EXEC, DISCARD, RESET, UNWATCH and
connection lifetime. Engine maintenance prunes unused registrations.

New differential checks cover same-value/ABA writes, no-op set/zset changes,
list/hash writes, failed conditionals and runtime errors, own-connection writes,
other transactions, expiration, repeated WATCH, UNWATCH, DISCARD and RESET.
Redis-py optimistic read/modify/write and recovery of the resulting value are
included. Three additional unit tests cover expiration/reaping, failed admission
and speculative version publication. 101 Rust tests and Clippy passed.

Remaining WATCH gates: wider mutation/no-op option audits, cluster topology and
ASKING changes during optimistic transactions, memory accounting/limit parity,
and performance of watch maintenance. General transaction migration semantics
and transaction performance parity remain open.


Transaction scaling increment: replaced the full key-directory/watch-directory
clone with a first-write undo record per changed key. Repeated writes retain one
original snapshot; untouched keys are not copied. One coalesced persistence batch
is still admitted before publishing the command result. Rejection or unwinding
restores entries, watch versions, memory accounting and the expiration deadline.
The single serving thread and Rust's exclusive engine borrow prevent another
client from observing intermediate execution. Expiration maintenance is deferred
while a checkpoint is active; command lookups retain their transaction timestamp.

Checkpoint setup is O(1), and rollback/coalescing storage is O(changed keys),
excluding the normal value and command costs. Transactions still use immutable
collection replacements rather than the nontransactional in-place shortcut.
New tests detect accidental copying of untouched values, repeated-key checkpoint
growth, unwind rollback, maintenance deferral and nested checkpoint rejection.
104 unit tests pass; one opt-in local scaling probe is excluded from normal tests.
This improvement does not establish network throughput, persistence throughput,
Linux ARM64 or Redis transaction performance parity.


Local scaling evidence (macOS ARM64, Rust 1.92.0, release profile, 2026-11-04):
three runs of the opt-in `transaction_scaling_report`, 200 iterations per size.
Median nanoseconds per operation:

| Database keys | SET+GET transaction, accepting in-memory callback | Directory clone/drop alone (old-path lower bound) |
|---|---:|---:|
| 100 | 470 | 742 |
| 10,000 | 355 | 85,251 |
| 100,000 | 385 | 1,056,895 |

These measure core execution only, with no network, reply encoding, actual sink,
PostgreSQL persistence or Redis reference process. The clone comparator excludes
old command execution and is not an end-to-end before/after benchmark. Reproduce
with `CARGO_INCREMENTAL=0 cargo test -p pg-cache-core --release --lib --locked
transaction_scaling_report -- --ignored --nocapture` (as one shell command).
Raw local output is in `.local/transaction-scaling-repeat.log`.
All four live protocol/persistence combinations passed 17,870 checks after the
rollback change. Direct-mode three-shard RESP3 integration also passed, including
transactions, watch conflicts, redirection, migration and recovery.


Native-cluster transaction increment: a three-node Redis 8.2.11 oracle exposed
premature CROSSSLOT rejection during queuing. Transactions now queue same-node
commands independently and route the aggregate key set at EXEC, before dirty-queue
and WATCH checks. ASKING survives MULTI and queued commands, then clears when the
transaction finishes. Importing nodes reject distinct multi-key requests with
missing keys using TRYAGAIN, while repeated references to one missing key remain
allowed. EXEC checks aggregate key presence when migration begins after queuing.
Queued ASKING returns the native reply and does not grant permission past EXEC.

105 unit tests and Clippy passed (one manual scaling probe remains opt-in).
42 exact native-cluster comparisons passed in RESP2/direct and RESP3/journal
runs; addresses are normalized by node only. Both runs also passed the full
three-shard migration/recovery suite, and the standalone RESP3 journal suite
passed 17,870 checks. Wider topology-change, WATCH/migration, malformed key-spec,
admin-command and Redis-version audits remain pending.


Blocking-list increment: BLPOP and BRPOP now support multiple keys, binary
values, fractional-second deadlines, indefinite waits, FIFO waiters per ready
key, and immediate nonblocking execution inside MULTI/EXEC. Producer pipelines
yield to existing waiters after writes. Persistence admission precedes each pop;
disconnected waiters are removed without consuming the next item. COMMAND
metadata reports blocking flags and cluster routing includes every input key.
The Redis 8.2.11 oracle showed that wakeup re-executes the original key order,
including wrong-type checks; readiness order determines which waiter runs first.

108 Rust tests and Clippy pass. The standalone suite passes 17,903 checks in
RESP2/RESP3 and direct/journal modes, including the new blocking cases and crash
recovery of the remaining list. The RESP3 journal three-shard suite passes with
blocking-command MOVED/CROSSSLOT checks. The scheduler still polls blocked sockets
at 1 ms intervals; waiter scaling and performance parity are unverified.
The subsequent increment below adds blocking moves and sorted-set pops. Streams
and Pub/Sub remain intentionally excluded.


Blocking-family increment: added BLMOVE, BRPOPLPUSH, BLMPOP, BZPOPMIN,
BZPOPMAX and BZMPOP. Blocking moves wait only on their source while routing
both source and destination. Existing atomic collection operations preserve
TTL and leave both keys unchanged on persistence rejection. Multi-pop shares
option parsing with its nonblocking counterpart, including validation order
before timeout parsing. Readiness tracking now covers lists and sorted sets;
moves can wake consumers waiting on their destination. RESP3 sorted-set scores
use doubles, including nested BZMPOP results.

109 Rust tests and Clippy pass. Each of four standalone RESP2/RESP3 and
direct/journal runs passes 18,002 checks against Redis 8.2.11, including timeout,
wrong-type, rotation, count, transaction, chained wakeup, and list/sorted-set
crash-recovery cases. Three-shard RESP3 journal integration checks MOVED,
CROSSSLOT and variable key discovery for these commands. The 1 ms polling
scheduler, broader concurrency/topology corner cases, and Linux ARM64 performance
remain to be audited; command-family coverage is not full Redis compatibility.


Blocked-client control increment: CLIENT UNBLOCK supports default/TIMEOUT and
ERROR modes for all implemented blocking list/sorted-set commands. Only local
blocked connections are eligible; absent, already cancelled and nonblocking
clients return zero. Timeout preserves the command's null shape and ERROR uses
the UNBLOCKED error class. Pipelined commands resume on the same connection.
Cancellation effects are staged with transaction connection state and applied
by the socket loop after successful EXEC admission, so failed admission retries
and DISCARD do not affect another client.

110 Rust tests and Clippy pass, including rejected-admission cancellation
isolation. Each standalone RESP2/RESP3 × direct/journal run passes 18,120 checks
against Redis 8.2.11. Coverage includes all eight blocking commands, repeated
cancellation, malformed arguments, idle IDs, DISCARD, EXEC and pipelining.
The RESP3 journal three-shard suite passes with local-node cancellation coverage.
CLIENT KILL/PAUSE/UNPAUSE/LIST, tracking, broader administrative behavior and
blocking scheduler performance remain pending independently of this increment.


Command discovery increment: COMMAND LIST now returns supported top-level and
subcommand names and supports FILTERBY PATTERN/ACLCAT/MODULE. Pattern matching
is case-insensitive, including Redis bracket-range and escape semantics, while
SCAN keeps its case-sensitive behavior. INFO without names returns the full
catalog. INFO accepts supported subcommand names; flags and categories are RESP3
sets. GETKEYS distinguishes unknown commands, keyless commands, wrong arity and
invalid key specifications using the reference error messages.

Descriptive categories are generated from the pinned Redis 8.2.11 command JSON
plus its implicit category rules. Regenerate with
`python3 scripts/generate_command_categories.py .local/redis-8.2.11/src/commands`
and `cargo fmt --all`. Categories describe reference semantics, not proof of
complexity parity or ACL enforcement. COMMAND DOCS, GETKEYSANDFLAGS, full modern
key-spec/tip fields, nested subcommand metadata and wider error audits remain
pending.

111 Rust tests and Clippy pass. Each standalone protocol/persistence combination
passes 18,348 checks against Redis 8.2.11, including category comparisons for
every advertised reference command, filters, exact errors, subcommand lookup
and transactional LIST. RESP3 journal cluster integration verifies redis-py's
metadata parsing alongside migration and recovery.


Key-access discovery increment: COMMAND GETKEYSANDFLAGS returns per-key access
modes and effects with RESP2 arrays or RESP3 sets. Source and destination roles
remain distinct when the names are identical. Static flags are generated from
the pinned reference specifications; SET and BITFIELD derive dynamic flags from
their options, including conservative read/write reporting for malformed BITFIELD
operations. Discovery does not execute the command or require its keys to exist.
The subcommand participates in LIST, INFO, category filtering and transactions.

112 Rust tests and Clippy pass. Each standalone RESP2/RESP3 × direct/journal run
passes 18,516 checks against Redis 8.2.11. The new tests generate a key-discovery
request for every advertised key-bearing command, then check dynamic options,
duplicate keys, source/destination flags, exact errors and transactional replies.
RESP3 journal cluster integration also passes. Full modern INFO key-spec/tip
fields, nested subcommand metadata, COMMAND DOCS and broader malformed-input
coverage remain pending.


Modern command INFO increment: descriptions now have ten fields, including
reference command flags, legacy key positions, tips, full key specifications
and supported nested subcommands. RESP2 flattens maps/sets into arrays; RESP3
preserves nested maps and sets. Generated JSON is embedded in the binary and
included in the integration source manifest. Header arities are checked against
the executable command registry, not merely copied into descriptions. This
caught QUIT's negative arity: extra arguments are now ignored and QUIT executes
immediately during MULTI, discarding queued writes when the socket closes.

114 Rust tests and Clippy pass. Each standalone RESP2/RESP3 × direct/journal run
passes 19,045 checks against Redis 8.2.11. Discovery compares headers, categories,
tips and key specifications for every advertised reference command, verifies
supported nested descriptions, and tests QUIT with/without extra arguments both
inside and outside transactions. RESP3 journal cluster integration also passes.
Reference flags/categories describe the intended command semantics; they do not
prove complexity, performance, ACL, replication or scripting compatibility.
COMMAND DOCS and wider malformed-option/version audits remain pending alongside
the broader compatibility roadmap.


Command documentation increment: added COMMAND DOCS and COMMAND HELP. DOCS
supports the full local catalog, case-insensitive named lookups, duplicate names,
unknown-name omission and supported nested subcommands. Reference summaries,
histories, deprecation fields, argument trees and argument flags are generated
from Redis 8.2.11. Public group names, uppercase tokens and omitted empty optional
fields match the executable reference. RESP2 uses flattened maps and RESP3 uses
nested maps/sets. Metadata flags now use Redis's simple-string wire encoding.

114 Rust tests and Clippy pass. Each standalone RESP2/RESP3 × direct/journal run
passes 19,239 checks, including documentary comparisons for every advertised
reference command, whole-catalog output, repeated/unknown names, HELP, exact
errors, transaction replies and byte-level key-flag encoding. The RESP3 journal
cluster suite also passes. The generated documentation describes the reference
contract; matching it does not establish option coverage, computational
complexity, performance, ACL, scripting, or replication parity. Those remain
part of the broader compatibility work, with Streams and Pub/Sub still excluded.


HyperLogLog increment: added PFADD, PFCOUNT and PFMERGE over Redis-compatible
HYLL strings, with sparse/dense decoding, six-bit dense packing, MurmurHash64A
and Ertl cardinality estimation. Cached single-key counts preserve TTL and
participate in WATCH/persistence admission; multi-key counts do not modify
inputs. Merges include the existing destination. Dense PFADD updates packed
registers directly and avoids copying on no-op additions. Sparse writes currently
rebuild a canonical valid representation, which can differ byte-for-byte from
Redis's incremental sparse layout.

116 Rust tests and Clippy pass. All four standalone RESP2/RESP3 × direct/journal
runs pass 19,456 checks against Redis 8.2.11, including larger randomized sketches,
binary inputs, uncached sketch interchange in both directions, cache bytes,
malformed envelopes/sparse payloads, TTLs, WATCH, transactions and crash recovery.
The RESP3 journal three-shard suite passes with HyperLogLog MOVED/CROSSSLOT,
union, online migration and restart checks. Wider malformed-
payload behavior and HyperLogLog performance parity remain pending; geospatial
commands are still unimplemented.

PFDEBUG increment: GETREG, DECODE, ENCODING and TODENSE now expose HLL registers
and encodings. Sparse-to-dense conversion preserves cached counts, TTL and WATCH
versions, with admission and transaction rollback checks. Imported sketches are
compared byte-for-byte after conversion; malformed headers and sparse opcodes,
missing keys, wrong types and arity are checked against Redis 8.2.11. SIMD ON/OFF
reports disabled; the invalid-argument extra reply emitted by Redis is deliberately
not reproduced. Sparse DECODE output reflects the actual stored encoding, whose
canonical layout can differ from Redis's incremental layout.
Validation: all four RESP2/RESP3 × direct/journal runs pass 19,522
Redis differential/contract checks. RESP2 journal and RESP3 direct additionally
snapshot the converted sketch and verify its encoding after crash recovery.
The RESP3 journal three-shard suite passes PFDEBUG MOVED routing, conversion,
online migration and recovery. Core tests, extension build and Clippy pass.

PFSELFTEST increment: implemented the actual diagnostic rather than an unconditional
success response. It checks 1,000 packed-register round trips, independently
accumulated dense versus sparse register values, small-cardinality sparse encoding,
and estimation error at powers of ten through ten million distinct inputs. The
fixed input seed makes failures reproducible; no engine/database state is passed
into the diagnostic. Command metadata and arity are compared against Redis 8.2.11.
Validation: 116 Rust tests pass (one opt-in scaling test ignored), extension
build and Clippy pass, and RESP2 direct plus RESP3 journal each pass 19,529
Redis differential/contract checks. Geospatial commands are the next missing
command family; full compatibility and performance parity remain unproven.

Geospatial increment: GEOADD, GEOPOS, GEOHASH and GEODIST operate on native
sorted-set values using Redis's coordinate encoding. NX/XX/CH, binary members,
validation-before-mutation, TTL preservation, four distance units, coordinate
extrema, hexadecimal input, exact lookup results and transaction execution are
compared against Redis 8.2.11. A persistence-admission unit test verifies unchanged
values and WATCH versions on rejection. RESP3 GEOPOS encodes coordinates as
doubles, while GEODIST keeps bulk distance strings. GEOSEARCH/GEOSEARCHSTORE and
GEORADIUS variants remain unimplemented; this increment does not establish full
geospatial compatibility or Linux ARM64 performance/rounding parity.
Validation: 117 Rust tests pass (one opt-in scaling probe ignored), extension
build and Clippy pass. RESP2 direct and RESP3 journal each pass 20,038 differential/
contract checks. The RESP3 journal three-shard suite passes geospatial MOVED
routing, online slot migration and recovery, retaining the expected geohash and
sorted-set type. Both standalone modes verify geospatial scores, positions and
absolute expiration after crash recovery. Other protocol/backend combinations
and Linux ARM64 have not been rerun for this increment.

Geospatial search increment: GEOSEARCH and GEOSEARCHSTORE implement member/coordinate
origins, radius/box filters, ordering, COUNT/ANY, result details and stored distances.
Stored results replace any destination type, remove its expiration, and allow the
source and destination to be the same key. Empty searches delete the destination.
Admission and transaction rollback tests preserve the original destination and
WATCH versions on failure. Search still scans sorted-set scores; Redis's spatial
neighbor traversal, complexity parity, boundary audits and GEORADIUS variants
remain pending. Unsorted result ordering and ANY subsets need not be identical.
Validation: 118 Rust tests pass (one opt-in scaling probe ignored), extension
build and Clippy pass. RESP2 direct and RESP3 journal each pass 20,210 Redis
checks, including exact stored scores, shape/option errors, aliases and recovery.
The RESP3 journal three-shard suite passes search-store CROSSSLOT checks and
stored-result migration/recovery. Differential tests also corrected shortest-score
notation and ARM64 fused distance arithmetic; other architectures remain unaudited.

Radius-command increment: GEORADIUS, GEORADIUS_RO, GEORADIUSBYMEMBER and
GEORADIUSBYMEMBER_RO share the geospatial search implementation. Writable variants
support STORE/STOREDIST, replacing any destination type and using the last store
option when repeated. Read-only variants reject storage. Member-origin validation
matches Redis's missing-source behavior. COMMAND metadata and GETKEYS/GETKEYSANDFLAGS
include correct source/read and final-destination/write roles, even for duplicate
options and malformed syntax. Rejected radius-store persistence leaves the original
destination unchanged. Indexed spatial traversal, performance parity and broader
boundary/rounding audits remain pending.
Validation: full core suite passes 118 tests (one scaling probe ignored), followed
by passing expanded geospatial admission tests. Extension build and Clippy pass.
RESP2 direct and RESP3 journal each pass 20,428 Redis differential/contract checks.
The RESP3 journal three-shard suite passes radius last-destination routing,
CROSSSLOT/MOVED responses and stored-result migration/recovery. Standalone recovery
checks retain full STOREDIST scores.

Indexed geospatial increment: replaced whole-set scans with Morton-prefix score
ranges over the persistent ordered index. Search chooses cell precision from the
query size and latitude, expands at cell boundaries, prunes unnecessary neighbors,
and visits center/cardinal/diagonal cells in reference order. Duplicate cells and
COUNT ANY stop behavior match the exercised Redis 8.2.11 cases. Differential
coverage includes poles, antimeridian crossings, zero/tiny/world-sized radii,
rectangular searches, coordinate extrema and exact unsorted result order. A focused
candidate test seeks past 10,000 unrelated entries and visits only three local
entries. This is evidence of bounded candidate traversal, not Redis performance
parity; sorting, broader boundary audits and Linux ARM64 benchmarks remain open.
Validation: 120 Rust tests pass (one opt-in scaling probe ignored), extension
build and Clippy pass. RESP2 direct and RESP3 journal each pass 20,707 differential/
contract checks. The RESP3 journal three-shard suite passes searches after online
migration and crash recovery. Geospatial command coverage is implemented, while
full Redis compatibility remains incomplete for the other tracked families.

Hash-field expiration groundwork: replaced the plain field map with a hash value
that owns optional absolute field deadlines. Added backward-compatible COL1 reading
and plain-hash writing, plus a hash-only COL2 encoding for timestamped fields.
Overwrites/deletions clear metadata and numeric increments preserve it. Size/memory
accounting includes metadata; expiring hashes bypass the old HSET fast path and
incremental-size shortcut. Strict decoding, snapshot isolation, rejected persistence
and transaction rollback tests cover the new representation.

This is storage infrastructure, not completed hash-field expiration support.
HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT, TTL queries, HPERSIST, HGETEX/HSETEX, read-time
filtering, active cleanup, WATCH expiry semantics and timestamped-hash migration
verification still need implementation or validation. No new Redis commands are
advertised in this increment.

Validation: 123 Rust tests pass (one scaling probe ignored), extension build and
Clippy pass. Existing RESP2 direct and RESP3 journal suites each retain 20,707
Redis differential/contract checks. Additional internal PostgreSQL fixtures load
COL2 values, increment fields, inspect persisted deadlines, crash/restart, overwrite
fields and verify conversion back to COL1 in both persistence modes.

Hash-field expiration increment: implemented HEXPIRE/HPEXPIRE/HEXPIREAT/HPEXPIREAT,
HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME and HPERSIST. Setters admit timestamp changes
atomically, preserve key TTLs, implement NX/XX/GT/LT, and process duplicate fields
in order. Expired fields are removed at command boundaries, during maintenance
and on restore; removing the final field removes its hash. WATCH captures the
next field deadline and cleanup updates versions. Undo restores the scheduling
bound as well as snapshots and watch versions. Cleanup itself needs no persistence
admission because the deadline already exists durably.

The initial expiry scheduler scans the cache when a conservative earliest hash
field deadline becomes due. HGETEX/HSETEX, scalable expiration indexing, broader
command/transaction/clock-edge audits and Linux ARM64 performance remain pending.
Validation: 125 Rust tests pass (one opt-in scaling probe ignored), extension
build and Clippy pass. RESP2 direct and RESP3 journal each pass 21,008 Redis
checks. Tests cover conditions, duplicate fields, exact errors, expiry-aware reads,
last-field key removal, WATCH invalidation and field deadlines across recovery.
Internal cold-load fixtures discard already-expired fields. The three-shard RESP3
journal suite preserves future field deadlines across online migration and restart.

HGETEX/HSETEX increment: implemented field reads with expiry/PERSIST, multi-field
writes with FNX/FXX and EX/PX/EXAT/PXAT/KEEPTTL, and default deadline clearing.
Validation order differs intentionally: HSETEX parses options before type lookup,
while HGETEX checks type first. Duplicate fields are processed sequentially after
conditions inspect the pre-write state. Read-only and no-op HGETEX paths borrow
rather than clone the hash. A transient set with an immediately-expired deadline
invalidates an absent-key WATCH only after admission; transaction rollback restores
its watch version. Tests cover rejected read-with-expiry mutations and conditional
no-ops without persistence submission.

Validation: 127 Rust tests pass (one opt-in scaling probe ignored), extension
build and Clippy pass. RESP2 direct and RESP3 journal each pass 21,142 Redis checks,
including option/error matrices, duplicates, conditions and WATCH behavior. Stored
and persisted field deadlines survive restart. The RESP3 journal three-shard suite
passes HGETEX redirects and HSETEX KEEPTTL through migration/recovery. Scalable
expiry scheduling and broader field-expiry interaction/performance audits remain.


### Ordered hash-expiration scheduling

Replaced the cache-wide due-expiration scan with a persistent ordered index holding
one earliest deadline per expiring hash. Writes, deletes, copies, renames, recovery,
and whole-key reaping keep the index exact; repeated deadline changes leave no
stale entries. Transaction setup snapshots the index in constant time, and rejected
or unwinding execution restores it alongside values and WATCH versions. Tests
cover replacement/deletion and cleanup with 2,000 unrelated keys, plus rollback
after expiration cleanup.

Validation: 129 Rust tests pass (one opt-in scaling probe ignored), extension build
and Clippy pass. RESP2 direct and RESP3 journal each pass 21,142 Redis checks,
including persistence and crash recovery. The RESP3 journal three-shard suite
also passes routing, online migration, interrupted transfers and recovery.
Per-hash field scans, copying and serialization remain; Linux ARM64 performance
parity has not been established.


### Per-field deadline index

Hashes now maintain an ordered set of field deadlines alongside the field lookup
map. Earliest-deadline and live-field checks use the tree endpoints; expiration
removes only due deadlines. Replacements, persistence, numeric increments and
field deletion keep both indexes consistent. Reassigning a deadline removes the
old index entry. Recovery rebuilds the index from existing COL2 metadata, without
changing the persisted format. Memory estimates include the extra tree storage.

An independent model checks 5,000 mixed field mutations, expiration, and snapshot
restoration. Validation: 130 Rust tests pass (one opt-in scaling probe ignored),
Clippy and the extension build pass, and RESP2 direct plus RESP3 journal each pass
21,142 Redis differential/contract checks including crash recovery. The complete
cleanup operation still copies and serializes modified hashes, so this does not
establish Redis complexity or Linux ARM64 performance parity.


### Generic key catalog audit and UNLINK/RANDOMKEY

Compared the advertised command tables with the pinned Redis 8.2.11 command JSON.
All top-level command names in the string, hash, list, set, and sorted-set groups
are now advertised; this is a name-coverage observation, not proof of complete
option, error, complexity or behavioral compatibility. Generic key gaps after this
increment include DUMP, RESTORE, MIGRATE, MOVE, OBJECT and its subcommands, WAIT,
and WAITAOF. Scripting/Functions, authentication/ACLs, additional CLIENT controls,
database selection and native replication remain separate substantive gaps.

Implemented UNLINK with deduplicated live-key deletion, atomic persistence
admission, WATCH invalidation and deferred value reclamation. A shared worker
releases values after deletion; transactions hold values until successful commit
and release rollback references before handing them to the worker. Rejected
transactions restore data, field-expiry schedules and WATCH versions. RANDOMKEY
uses uniform reservoir sampling over live local keys and returns null for an
empty database; it still scans the key table rather than matching Redis complexity.
Generated command metadata includes both commands and their native key flags.


Validation: 133 Rust tests pass (one opt-in scaling probe ignored), extension build
and Clippy pass. RESP2 direct and RESP3 journal each pass 21,445 differential/
contract checks, including native command discovery, command-specific arity errors,
all-type deletion, duplicates, WATCH/EXEC, RANDOMKEY membership and restart
verification of persisted deletions. The RESP3 journal three-shard suite passes
UNLINK MOVED/CROSSSLOT routing alongside migration and crash recovery.


### Native DUMP export

Implemented DUMP for strings, hashes, lists, sets and sorted sets. Payloads use
native RDB object encodings, an RDB v12 footer and Redis CRC64. Plain collection
encodings avoid depending on Redis's in-memory packing choices. Hash metadata
preserves absolute field deadlines using the minimum-deadline/relative-TTL format,
including offsets requiring 64-bit lengths. Missing/expired keys return null;
whole-key TTL is excluded. Command discovery, arity errors, WATCH/EXEC and cluster
routing are covered. PostgreSQL COL1/COL2 persistence remains unchanged.

Validation: 135 Rust tests, extension build and Clippy pass. RESP2 direct and
RESP3 journal each pass 21,506 differential/contract checks. Native Redis RESTORE
accepts exported binary and integer-looking strings, length-boundary payloads,
lists with duplicates, sets, hashes, infinite sorted-set scores and large hashes
with field deadlines; resulting values/deadlines match. Native Redis rejects a
checksum-corrupted payload. The RESP3 journal cluster suite passes DUMP redirects
and payload framing alongside migration/recovery. Large hash fixtures require
batched writes because the existing 1,024-argument limit remains a compatibility
gap. RESTORE import, compact/compressed RDB decoding, older/newer-version audits
and full Redis size limits remain pending.


### Native RESTORE import

Added checksum/version validation, bounded native RDB decoding and atomic RESTORE
admission/replacement. Supports raw/integer/LZF strings; plain lists, sets, hashes
and sorted sets; integer sets; listpacks; current packed/plain quicklist nodes;
legacy ziplists, zipmaps and quicklists; current and pre-GA hash-field-expiration
metadata. Relative TTL, ABSTTL, REPLACE, duplicate options, BUSYKEY/error ordering,
WATCH and EXEC are exercised against native Redis. Imported field deadlines remain
absolute through PostgreSQL restart and cluster migration. Rejected writes restore
values, key deadlines, field schedules and WATCH versions.

Validation: 138 Rust tests pass (one opt-in scaling probe ignored); extension build
and Clippy pass. RESP2 direct and RESP3 journal each pass 21,729 differential/
contract checks, including native DUMP imports for all current encodings and
historical payload fixtures. Tests cover truncation, 4,096 checksum-valid random
bodies, expansion limits, checksum errors, invalid packed structures, admission
rejection and recovery. The RESP3 journal three-shard suite passes RESTORE redirects
and migration/recovery of a restored hash with field deadlines.

Remaining limitations: IDLETIME/FREQ options are validated but metadata is not
retained; strict malformed-payload rejection differs from default Redis. The
malformed native comparisons enable `sanitize-dump-payload yes` specifically,
while normal comparisons retain the default. Streams remain excluded, module
payloads unsupported, and full Redis resource limits plus exhaustive historical/
newer-version interoperability audits remain pending.


### Wide command argument vectors

Removed the 1,024-argument ceiling from RESP and inline commands. RESP array
headers now accept Redis's signed-32-bit count range, while the existing frame
byte limit bounds completed requests. Initial argument storage is capped by both
received bytes and 1,024 entries, then grows only as complete arguments are parsed;
a header declaring billions of arguments does not reserve that many slots. The
large-hash DUMP fixture now uses a single HSET rather than a batching workaround.

Validation: 140 Rust tests pass (one opt-in scaling probe ignored), extension build
and Clippy pass. RESP2 direct and RESP3 journal each pass 21,751 checks, covering
2,048-element collection writes/reads, wide EXEC, 100,001-argument EXISTS, fragmented
frames, inline input, pipeline boundaries and PostgreSQL restart recovery. Unit
checks exercise INT_MAX headers, count overflow and frame-limit enforcement.
The 1 MiB request limit, response limits and fragmented-input parsing performance
remain compatibility/performance work; accepting the native count range does not
mean a complete INT_MAX-argument request fits the current frame limit.
