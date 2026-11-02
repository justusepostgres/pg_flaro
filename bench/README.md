# Collection benchmarks

Run the same nonblocking Rust client against isolated PostgreSQL/Flaro and Redis
processes. The runner seeds a fixed-size collection per client and alternates
server order across repetitions. It checks every response, exact acknowledged
mutation counts, final values, and (with `--restart-check`) committed recovery.
Run as an ordinary user with PostgreSQL 17, Rust, Python and Redis installed.

```sh
mkdir -p .local
python3 -m venv .local/bench-venv
.local/bench-venv/bin/pip install redis==6.4.0
export PGRX_PG_CONFIG_PATH=/path/to/postgresql17/bin/pg_config
cargo build -p pg-cache --release --features journal-prototype --locked
cargo build -p pg-cache-core --release --bin collection-bench --locked
.local/bench-venv/bin/python bench/collection_benchmark.py \
  --library "$PWD/target/release/libpg_cache.so" \
  --redis-server /path/to/redis-server \
  --output .local/collections.json \
  --revision "$(git rev-parse HEAD)" \
  --seconds 3 --repeats 3 --restart-check
python3 bench/summarize_collections.py .local/collections.json
```

Use `.dylib` on macOS. Temporary database files and logs remain under `.local/`
for inspection; the runner stops its server processes on completion or failure.

Workloads: `HSET`, `HGET`, `LSET`, `LINDEX`, `SREM`/`SADD` pairs,
`SISMEMBER`, `ZINCRBY`, and `ZSCORE`. Each connection owns a distinct key.
Writes change values or membership: these are not repeated no-op `SADD` calls
or reads of empty/missing collections. Set churn counts each command separately;
its pipeline contains twice as many commands as the other workloads.

Defaults: 50 connections, two client threads, 128/1,024 elements per key,
pipeline depths 1/16, and 64-byte hash/list values. Fields and members are `fN`,
not 64-byte strings. Each trial is time-bounded and drains outstanding replies.
Latency is **pipeline-batch completion latency**, not individual-command latency.
Throughput includes the final response drain; PostgreSQL commit-drain time is
reported separately. The runner does not silently ignore command errors.

Flaro uses WAL-logged journal write-behind with `flush_ms=100`, 128-MiB pending
budget, and 512-MiB cache. Redis has AOF and RDB disabled, 512-MiB maxmemory and
`noeviction`. This compares asynchronous acknowledgments with Redis's volatile
write path; it is not an equal-durability comparison. Clients and both servers
share the machine. Cgroup CPU includes the client, persistence/compactor processes,
and the idle peer. Do not interpret it as server-only CPU or extrapolate these
fixed working sets to all commands, larger datasets, cluster failover, or WANs.

Raw results include source revision, library/driver hashes, versions, every
repetition, percentiles, persistence counters, drain times and CPU counters.
See [the ARM64 report](../docs/collection-performance-arm64.md) for measured results.
