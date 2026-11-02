# CNPG prototype deployment

The acceptance target is Linux ARM64: beat Redis on the documented workload,
install through CloudNativePG, persist in ordinary WAL-logged PostgreSQL tables,
and accept existing Redis clients using the documented RESP command subset.
Write-behind acknowledgments remain intentionally non-durable until the
PostgreSQL commit. Functional CNPG deployment and planned promotion were verified
on OCI Linux ARM64. Measured throughput wins and latency tradeoffs are documented
in the [performance results](../../README.md#linux-arm64-performance-validation).
Production readiness remains open.

## Packaging

Build with `make install FEATURES=journal-prototype` to install both extensions.
The listener defaults to loopback; set `pg_cache.listen_address` explicitly for
private pod access.

## Deployment artifacts and current validation

`Dockerfile` builds both extensions from source and copies them into a pinned
PostgreSQL 17 CNPG base image. The root `.dockerignore` excludes benchmark
clusters, build output and local credentials from the build context. Build from
the repository root and push to your private registry. `cluster.yaml` shows two instances, extension bootstrap,
write-behind configuration, a managed primary Service on 6380, and an ingress
NetworkPolicy. Replace the image reference and configure imagePullSecrets for
your registry. NetworkPolicy requires an enforcing CNI.
Keep a PostgreSQL-version-prefixed image tag, such as `17.11-0.1.0`, when adding
the immutable digest (`:17.11-0.1.0@sha256:...`). CNPG uses that tag to identify the
PostgreSQL version and rejected a versionless tag during validation.

The full Dockerfile has now built successfully on native ARM64. Its resulting
image passed a disposable-container test as UID 26: both extensions bootstrapped,
all six storage tables were WAL-logged, the RESP listener served binary values and
the documented counter/expiry helpers, and committed values survived an immediate
PostgreSQL restart after full materialization. The test published RESP only on
localhost. This validates the source-build packaging; it is separate from the
operator deployment test below.

The current hash-index storage and scoped planner revision passed the source-build
and runtime test, including near-frame-limit binary keys and real hash collisions
through overwrite, delete and immediate restart. This schema requires fresh
extension tables rather than an in-place upgrade.

The same source-built image, pinned by digest, also passed CNPG 1.30.1 deployment
on OCI Linux ARM64. A separate pod's redis-py 6.4.0 client passed binary values,
nontransactional pipelines, conditional writes, counters, expiry, persistence and
reconnect through the managed Service. Catalog verification found six WAL-logged
tables; journal committed/materialized sequences matched on both primary and
standby before promotion.

Planned promotion from instance 1 to instance 2 passed: the same Service routed to
the new primary and committed binary values, deletions and expired state survived.
The demoted instance refused RESP connections while the new primary served them.
The redis-py observer completed 411 checks with 12 transient connection errors
during a 90-second window. New writes on the promoted primary passed, and an
immediate primary restart preserved both the original and newly written values.

Both test instances shared one dedicated node: this validates database promotion,
not node-loss HA. Use separate nodes/failure domains and your storage/backup policy
for production. The test waited for replica replay before promotion; asynchronous
failover can lose locally committed writes that have not reached the replica.
PostgreSQL readiness can precede the RESP worker listener: configure client
reconnect/backoff and do not assume CNPG Ready means the Redis port is ready.

**Observed isolation gap:** an earlier OCI test CNI did not enforce the example
NetworkPolicy. An unlabeled pod could connect. The Service remained ClusterIP,
but pod-level isolation was not established. Use an enforcing CNI/network boundary
before deploying this unauthenticated prototype where other workloads are untrusted.

The tested image was temporary; rebuild/publish your own image using the supplied
Dockerfile and reference its immutable digest. CNPG functional tests are not a
performance comparison through Kubernetes; that measurement is still outstanding.

## Redis client configuration

Use database 0, RESP2 and no AUTH/TLS. For redis-py:

```python
import redis
cache = redis.Redis(host="pg-cache-redis", port=6380, protocol=2)
cache.set(b"key", b"value", px=60000)
assert cache.get(b"key") == b"value"
pipe = cache.pipeline(transaction=False)  # MULTI/EXEC unsupported
pipe.set("a", "1").get("a")
assert pipe.execute() == [True, b"1"]
```

The command subset is in the root README. `HELLO`, `AUTH`, `CLIENT`, and
MULTI/EXEC are unsupported. redis-py ignores optional CLIENT SETINFO errors.
Its `incr()`, `incrby()`, `setex()` and `psetex()` helpers use supported commands.
The private listener has no Redis authentication; restrict network reachability.

## Verification checklist for a deployment

1. Package a Linux ARM64 PostgreSQL 17 image containing the native library,
   control file and complete versioned extension SQL. Pin and record image identity.
   Keep cache entries, checkpoint and journal segments in PostgreSQL tables.
2. Configure `shared_preload_libraries`, the target database and capacity settings
   in a CNPG Cluster. Bootstrap with `CREATE EXTENSION` in that database, without
   manual access to an instance-local library path.
3. Add a configurable Redis listener and an internal Service on port 6380 using
   CNPG's `managed.services.additional` with `selectorType: rw`. Restrict test
   access to its namespace/workload. Do not expose an unauthenticated public port.
4. Check startup readiness, primary-only serving and restart recovery. Test a
   planned promotion and reconnect through the same Service; verify committed
   values, expiry and deletion state. Uncommitted write-behind loss remains part
   of the documented contract, including any replication lag on promotion.
5. From another pod, exercise a real Redis client with binary keys/values,
   pipelines, conditional SET, TTL and reconnection. Explicitly document supported
   protocol versions/commands and unsupported Redis features.
6. Verify PostgreSQL catalog persistence flags, WAL-backed recovery and actual
   table contents after drain. Benchmark the deployed path against Redis under
   comparable hardware/storage/client conditions; report throughput and tails,
   client CPU, throttling, backlog and durable completion separately.

The existing pgrx 0.16.1 `enable_spi_access()` builder selects
`RecoveryFinished`, which is promising for promotion-only worker startup; this
was verified on the actual CNPG planned-promotion test.

## Primary documentation

- [CNPG shared preload configuration](https://cloudnative-pg.io/docs/current/postgresql_conf/)
- [CNPG primary-service templates](https://cloudnative-pg.io/docs/1.28/service_management/)
- [CNPG API/bootstrap fields](https://github.com/cloudnative-pg/cloudnative-pg/blob/main/docs/src/cloudnative-pg.v1.md)
- [Official ARM64 PostgreSQL operand images](https://github.com/cloudnative-pg/postgres-containers)
