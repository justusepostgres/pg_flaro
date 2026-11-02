# Collection performance on Linux ARM64

## Outcome

On the tested fixed-size working sets, Flaro reached Redis-level throughput:
**31 of 32 cases had higher median throughput; the remaining case was 0.33%
slower** (1,024-field HSET, pipeline 1). Throughput ratios ranged from 0.997×
to 2.326× Redis. This is a workload-specific result, not a universal Redis speed claim.

**Tail-latency parity is not achieved.** Flaro's median trial p99 was lower in
24 cases, but higher in all eight 1,024-element write cases, by 1.04–1.98×.
The largest p99 ratio was HSET at pipeline 16. Snapshot copying and whole-value
persistence remain size-dependent costs. Latencies below measure completion of
a pipeline batch, not an individual command within it.

## Reproduction and environment

- Source: [`bec5d1f`](https://github.com/justusepostgres/pg_flaro/tree/bec5d1f708a5424a0126428380b68bb9be8d72c4), release build with `journal-prototype`.
- Dedicated OCI A1 Linux ARM64 worker: four Neoverse-N1 cores, 24 GiB RAM;
  benchmark pod limited to four CPUs and 20 GiB. No CPU throttling was recorded.
- PostgreSQL 17.11; Redis 8.2.11 with jemalloc 5.3.0; Rust 1.92.0.
- 500-GiB OCI block volume, 10 VPUs/GB. Loopback networking, default CPU scheduling.
- 50 connections, two load-generator threads, one key per connection. Collections
  contain 128 or 1,024 elements. Hash/list values are 64 bytes; field/member names
  are `fN`. Pipelines are 1 or 16. Set churn sends a remove/add pair per iteration,
  counts both commands, and therefore has twice the commands per pipeline.
- Three three-second trials per server/case, alternating server order. Reported
  throughput and p99 are medians of the three trial metrics, not pooled percentiles.
  All 192 trials and 16 committed-state crash/recovery checks passed.
- Flaro: WAL-logged journal write-behind, `flush_ms=100`, 128-MiB pending budget,
  512-MiB cache, 512-MiB PostgreSQL shared buffers, one-hour checkpoint target.
- Redis: AOF/RDB disabled, 512-MiB maxmemory, `noeviction`. Neither server waits
  for a durable commit before the timed acknowledgment. Their crash guarantees
  differ: Flaro recovers committed snapshots, whereas this Redis configuration
  has no persistence. This is not an equal-durability benchmark.

The client and both servers share the same pod. Cgroup CPU counters include the
load generator and idle peer, including PostgreSQL maintenance processes; they
are not server-only CPU measurements. Fixed working sets, loopback networking,
and a small command subset do not establish performance for arbitrary commands,
high key counts, larger collections, eviction, cluster routing, resharding,
replication, or failover. No production workload or soak-test claim is made.

[Run instructions](../bench/README.md) include the standalone runner and summary
script. [Raw release results](../bench/results/oci-arm64-2026-11-02/release.json)
include every trial, binary hashes, exact accepted/committed counters, CPU usage,
and drain times. [Environment details](../bench/results/oci-arm64-2026-11-02/environment.json)
record the source archive hash. The exact same Rust driver binary was used for
before/after measurements. Each reply and final value was checked; write trials
also verified that accepted sequence growth matched the number of mutations.

Post-trial PostgreSQL commit drain took a median of 101.2 ms and a maximum of 253.2 ms across write trials. These waits are recorded separately from acknowledgment latency.

## All cases

Ratios above 1 favor Flaro throughput. Lower p99 is better.

| Elements | Pipeline | Operation | Flaro ops/s | Redis ops/s | Ratio | Flaro p99 ms | Redis p99 ms |
|---:|---:|---|---:|---:|---:|---:|---:|
| 128 | 1 | hset | 159,105 | 130,301 | 1.22× | 0.669 | 0.749 |
| 128 | 1 | hget | 167,669 | 133,516 | 1.26× | 0.587 | 0.726 |
| 128 | 1 | lset | 162,994 | 137,362 | 1.19× | 0.616 | 0.702 |
| 128 | 1 | lindex | 168,933 | 140,939 | 1.20× | 0.580 | 0.690 |
| 128 | 1 | set-churn | 314,470 | 241,159 | 1.30× | 0.620 | 0.786 |
| 128 | 1 | sismember | 174,105 | 147,259 | 1.18× | 0.556 | 0.650 |
| 128 | 1 | zincrby | 142,329 | 109,549 | 1.30× | 0.707 | 0.876 |
| 128 | 1 | zscore | 169,561 | 139,491 | 1.22× | 0.581 | 0.693 |
| 128 | 16 | hset | 1,199,880 | 564,924 | 2.12× | 1.167 | 1.946 |
| 128 | 16 | hget | 1,361,954 | 613,650 | 2.22× | 0.865 | 1.851 |
| 128 | 16 | lset | 1,241,132 | 654,037 | 1.90× | 1.137 | 1.742 |
| 128 | 16 | lindex | 1,368,554 | 707,179 | 1.94× | 0.859 | 1.640 |
| 128 | 16 | set-churn | 1,832,333 | 787,791 | 2.33× | 1.309 | 2.731 |
| 128 | 16 | sismember | 1,513,555 | 793,732 | 1.91× | 0.800 | 1.473 |
| 128 | 16 | zincrby | 690,251 | 297,220 | 2.32× | 1.657 | 3.893 |
| 128 | 16 | zscore | 1,323,925 | 645,896 | 2.05× | 0.878 | 1.780 |
| 1,024 | 1 | hset | 142,221 | 142,692 | 1.00× | 1.165 | 0.676 |
| 1,024 | 1 | hget | 164,861 | 146,959 | 1.12× | 0.597 | 0.652 |
| 1,024 | 1 | lset | 151,181 | 134,707 | 1.12× | 0.838 | 0.718 |
| 1,024 | 1 | lindex | 163,774 | 139,537 | 1.17× | 0.611 | 0.692 |
| 1,024 | 1 | set-churn | 298,662 | 262,114 | 1.14× | 0.803 | 0.773 |
| 1,024 | 1 | sismember | 171,073 | 150,701 | 1.14× | 0.570 | 0.638 |
| 1,024 | 1 | zincrby | 126,476 | 115,644 | 1.09× | 1.014 | 0.843 |
| 1,024 | 1 | zscore | 165,671 | 144,739 | 1.14× | 0.593 | 0.671 |
| 1,024 | 16 | hset | 870,495 | 771,246 | 1.13× | 3.052 | 1.544 |
| 1,024 | 16 | hget | 1,239,473 | 874,608 | 1.42× | 0.976 | 1.350 |
| 1,024 | 16 | lset | 974,825 | 645,469 | 1.51× | 3.334 | 1.854 |
| 1,024 | 16 | lindex | 1,293,638 | 712,339 | 1.82× | 0.928 | 1.606 |
| 1,024 | 16 | set-churn | 1,521,783 | 1,027,503 | 1.48× | 2.252 | 2.142 |
| 1,024 | 16 | sismember | 1,358,527 | 1,027,979 | 1.32× | 0.888 | 1.183 |
| 1,024 | 16 | zincrby | 574,444 | 525,773 | 1.09× | 2.387 | 2.164 |
| 1,024 | 16 | zscore | 1,188,273 | 858,205 | 1.38× | 0.992 | 1.371 |

## Change from the initial collection implementation

The original implementation copied and encoded entire collections for each
mutation. The optimized implementation reserves admission before changing live
data, updates private single-item collections in place, and captures immutable
snapshots at handoff. Lists and ordered score indexes share tree structure;
hashes and member lookup use flat hash tables. Serialization happens when
persistence or migration needs it. Collection handoffs and capacity thresholds
allow coalescing within the configured write-behind interval. Strings retain
their existing fast path.

The table uses 1,024-element collections at pipeline 16. The initial baseline
(`6121d96`) used one one-second trial, so these before/after ratios are directional
comparisons rather than estimates with statistical confidence intervals. The
Redis column comes from the repeated final run.

| Operation | Initial Flaro ops/s | Optimized Flaro ops/s | Improvement | Redis ops/s |
|---|---:|---:|---:|---:|
| hset | 9,768 | 870,495 | 89.1× | 771,246 |
| lset | 28,006 | 974,825 | 34.8× | 645,469 |
| set-churn | 17,924 | 1,521,783 | 84.9× | 1,027,503 |
| zincrby | 8,122 | 574,444 | 70.7× | 525,773 |

[Initial raw results](../bench/results/oci-arm64-2026-11-02/baseline.json) are retained.
The durable typed-value format and its journal accounting remain compatible with
the initial implementation. Ordinary acknowledgments are still write-behind;
uncommitted writes can be lost. Collection capture and SQL flush intervals are
batching targets, not a hard maximum crash-loss window.

## Correctness checks

49 Rust tests and strict core Clippy checks passed. Redis differential tests
passed 1,346 comparisons in each of direct and journal persistence modes. The
standalone protocol/backpressure/recovery suite and three-shard cluster tests
passed, including typed migration, ASK redirects, interrupted transfers,
durability barriers and restart recovery. Additional tests cover deferred
admission rejection, immutable committing snapshots, expiration and replacement
of dirty keys, configured handoff timing, and old typed-journal accounting.
