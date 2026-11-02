# CNPG deployment work (not yet deployable)

The acceptance target is Linux ARM64: beat Redis on the documented workload,
install through CloudNativePG, persist in ordinary WAL-logged PostgreSQL tables,
and accept existing Redis clients using the documented RESP command subset.
Write-behind acknowledgments remain intentionally non-durable until the
PostgreSQL commit. No CNPG deployment or failover claim is established yet.

## Packaging

Build with `make install FEATURES=journal-prototype` to install both extensions.
The listener defaults to loopback; set `pg_cache.listen_address` explicitly for
private pod access.

## Current gaps

- No CNPG-compatible extension image or actual CNPG bootstrap has been tested.
- Compatibility evidence currently covers raw RESP/client wire captures and Redis
  differential commands, not a separate application's Redis-library connection
  through a CNPG-managed primary Service.

## Implementation and verification

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
must still be verified on the actual CNPG deployment.

## Primary documentation

- [CNPG shared preload configuration](https://cloudnative-pg.io/docs/current/postgresql_conf/)
- [CNPG primary-service templates](https://cloudnative-pg.io/docs/1.28/service_management/)
- [CNPG API/bootstrap fields](https://github.com/cloudnative-pg/cloudnative-pg/blob/main/docs/src/cloudnative-pg.v1.md)
- [Official ARM64 PostgreSQL operand images](https://github.com/cloudnative-pg/postgres-containers)
