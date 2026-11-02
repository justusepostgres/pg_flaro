#!/usr/bin/env python3
"""Disposable three-shard PostgreSQL test with redis-py RedisCluster.

Run after building pg-cache (journal-prototype feature for --journal).
Never connects to existing PostgreSQL instances. Requires redis==6.4.0.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import redis
from redis.cluster import RedisCluster
from integration import Client, RedisError, free_port, frame

ROOT = Path(__file__).resolve().parents[1]


def eventually(fn, timeout=20):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except (ConnectionError, OSError, redis.RedisError) as error:
            last = error
        time.sleep(.05)
    raise AssertionError(f"condition timed out: {last}")


def expect_error(result, prefix):
    assert isinstance(result, RedisError) and str(result).startswith(prefix), result


class Shard:
    def __init__(self, work, index, nodes, bindir, journal, legacy=False, cluster_mode=True):
        self.root = work / str(index)
        self.root.mkdir()
        self.data = self.root / "data"
        self.log = self.root / "postgres.log"
        self.bindir = bindir
        self.pgport = free_port()
        self.port = nodes[index]["port"]
        self.id = nodes[index]["id"]
        self.running = False
        self.tool("initdb", "-D", self.data, "-A", "trust", "--no-locale", "-E", "UTF8")
        config = json.dumps({"local_id": self.id, "nodes": nodes})
        cluster_config_line = f"pg_cache.cluster_config='{config}'\n" if cluster_mode else ""
        with (self.data / "postgresql.conf").open("a") as file:
            file.write(f"\nlisten_addresses='127.0.0.1'\nport={self.pgport}\nunix_socket_directories=''\nshared_preload_libraries='pg_cache'\ndynamic_library_path='{work / 'lib'}:$libdir'\npg_cache.port={self.port}\npg_cache.flush_ms=10\n{cluster_config_line}")
            if journal:
                file.write("pg_cache.journal=on\n")
        try:
            self.start(wait=False)
            schema=(ROOT / "crates/pg-cache/sql/pg_cache--0.1.0.sql").read_text()
            if journal:
                schema += (ROOT / "crates/pg-cache/sql/journal-prototype.sql").read_text()
            if legacy:
                schema=schema.replace(",\n    data_type smallint NOT NULL DEFAULT 0", "").replace(", data_type smallint NOT NULL DEFAULT 0", "")
                table="cache_journal.entries" if journal else "cache_store.entries"
                schema += f"INSERT INTO {table}(key,value) VALUES (decode('6c6567616379','hex'),decode('434f4c3100ff','hex'));"
            self.sql("BEGIN;" + schema + "COMMIT;")
            self.ready()
        except Exception:
            if self.log.exists(): print(self.log.read_text()[-4000:])
            self.stop()
            raise

    def tool(self, name, *args):
        return subprocess.run([str(self.bindir / name), *map(str, args)], check=True, capture_output=True, text=True).stdout.strip()

    def sql(self, statement):
        return self.tool("psql", "-h", "127.0.0.1", "-p", self.pgport, "-d", "postgres", "-XAt", "-v", "ON_ERROR_STOP=1", "-c", statement)

    def client(self):
        return Client(self.port)

    def ready(self):
        def ping():
            c = self.client()
            try:
                return c.cmd("PING") == b"PONG"
            finally:
                c.close()
        eventually(ping)

    def start(self, wait=True):
        self.tool("pg_ctl", "-D", self.data, "-l", self.log, "-w", "start")
        self.running = True
        if wait:
            self.ready()

    def stop(self, mode="fast"):
        if self.running:
            self.tool("pg_ctl", "-D", self.data, "-m", mode, "-w", "stop")
            self.running = False

    def crash_restart(self):
        self.stop("immediate")
        self.start()

    def lock(self, table):
        assert table in ("cache_store.entries", "cache_journal.state", "cache_store.cluster_state")
        process = subprocess.Popen([str(self.bindir / "psql"), "-h", "127.0.0.1", "-p", str(self.pgport), "-d", "postgres", "-XAt", "-v", "ON_ERROR_STOP=1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        process.stdin.write(f"BEGIN; LOCK {table} IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n")
        process.stdin.flush()
        while process.stdout.readline().strip() != "locked":
            assert process.poll() is None
        return process

    def committed(self):
        c = self.client()
        try:
            s = json.loads(c.cmd("PGCACHE.STATS"))
            return s["accepted_sequence"] == s["committed_sequence"]
        finally:
            c.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--journal", action="store_true")
    options = parser.parse_args()
    bindir = Path(subprocess.check_output([os.environ.get("PGRX_PG_CONFIG_PATH", "pg_config"), "--bindir"], text=True).strip())
    artifact = ROOT / "target" / os.environ.get("PG_CACHE_PROFILE", "debug") / ("libpg_cache.dylib" if os.uname().sysname == "Darwin" else "libpg_cache.so")
    work = Path(tempfile.mkdtemp(prefix="cluster-integration-", dir=ROOT / ".local"))
    (work / "lib").mkdir()
    shutil.copy2(artifact, work / "lib" / ("pg_cache" + artifact.suffix))
    ports = set()
    while len(ports) < 3:
        ports.add(free_port())
    nodes = [{"id": str(i + 1) * 40, "host": "127.0.0.1", "port": port, "slots": slots} for i, (port, slots) in enumerate(zip(sorted(ports), [[[0, 5460]], [[5461, 10922]], [[10923, 16383]]]))]
    shards, clients = [], []
    cluster = None
    try:
        for i in range(3):
            shards.append(Shard(work, i, nodes, bindir, options.journal))
        clients = [s.client() for s in shards]
        a, b, c = clients
        # Real client discovers the topology and command keys, then routes pipelines.
        cluster = RedisCluster(host="127.0.0.1", port=shards[0].port, socket_timeout=10)
        assert cluster.set("foo", b"binary\x00\xff")
        assert cluster.set("bar", b"left")
        assert cluster.get("foo") == b"binary\x00\xff"
        expect_error(a.cmd("GET", "foo"), "MOVED 12182 ")
        expect_error(a.cmd("MSET", "foo", "x", "bar", "y"), "CROSSSLOT")
        assert cluster.get("foo") == b"binary\x00\xff" and cluster.get("bar") == b"left"
        pipe = cluster.pipeline()
        for i in range(100):
            pipe.set(f"pipe-{i}", str(i))
        assert all(pipe.execute())
        assert cluster.mset({"{bar}a": "one", "{bar}b": "two"})
        assert cluster.mget("{bar}a", "{bar}b") == [b"one", b"two"]
        assert a.cmd("CLUSTER", "KEYSLOT", b"\xff{bar}\x00") == 5061
        assert a.cmd("CLUSTER", "COUNTKEYSINSLOT", 5061) == 3

        # Move one populated slot from shard A to shard C.
        slot, source, target = 5061, shards[0], shards[2]
        # All collection kinds participate in slot routing, durable migration,
        # replay deduplication and crash recovery alongside strings.
        assert cluster.hset("{bar}hash", mapping={b"f\x00": b"v\xff"}) == 1
        assert cluster.rpush("{bar}list", "a", "b", "c") == 3
        assert cluster.sadd("{bar}set", "a", "b") == 2
        assert cluster.zadd("{bar}zset", {"a": 2, "b": 1}) == 2
        expect_error(a.cmd("SUNION", "{bar}set", "{foo}set"), "CROSSSLOT")
        assert a.cmd("SET", "{bar}ttl", "expiring", "PX", 60000) == b"OK"
        assert a.cmd("SET", "{bar}late-expiry", "old-expired-copy") == b"OK"
        assert a.cmd("SET", b"{bar}large\x00", b"x" * (1024 * 1024 - 60)) == b"OK"
        assert c.cmd("CLUSTER", "SETSLOT", slot, "IMPORTING", source.id) == b"OK"
        assert a.cmd("CLUSTER", "SETSLOT", slot, "MIGRATING", target.id) == b"OK"
        expect_error(a.cmd("MGET", "{bar}a", "{bar}missing"), "TRYAGAIN")
        expect_error(a.cmd("GET", "{bar}missing"), "ASK 5061 ")
        expect_error(c.cmd("GET", "{bar}missing"), "MOVED 5061 ")
        # ASKING is per-connection, exactly once, including pipelines and PING.
        c.socket.sendall(frame("ASKING") + frame("GET", "{bar}missing") + frame("GET", "{bar}missing"))
        assert c.read() == b"OK" and c.read() is None
        expect_error(c.read(), "MOVED")
        assert c.cmd("ASKING") == b"OK" and c.cmd("PING") == b"PONG"
        expect_error(c.cmd("GET", "{bar}missing"), "MOVED")
        assert cluster.set("{bar}new", "created-at-target")
        assert cluster.get("{bar}new") == b"created-at-target"
        expect_error(a.cmd("CLUSTER", "SETSLOT", slot, "NODE", target.id), "ERR")
        expect_error(a.cmd("CLUSTER", "SETSLOT", slot, "STABLE"), "ERR")
        expect_error(a.cmd("MIGRATE", "127.0.0.1", shards[1].port, "bar", 0, 100), "ERR")
        before = a.cmd("PTTL", "{bar}ttl")
        assert a.cmd("MIGRATE", "127.0.0.1", target.port, "{bar}ttl", 0, 5000) == b"OK"
        assert 0 < cluster.pttl("{bar}ttl") <= before
        assert cluster.get("{bar}ttl") == b"expiring"

        # Destination unavailable: retain source, durably fence the uncertain slot.
        c.close()
        target.stop("immediate")
        expect_error(a.cmd("MIGRATE", "127.0.0.1", target.port, "bar", 0, 100), "IOERR")
        expect_error(a.cmd("GET", "bar"), "TRYAGAIN")
        assert a.cmd("PGCACHE.MIGRATION") == [slot, b"bar", target.id.encode()]
        first_token = json.loads(source.sql("SELECT state FROM cache_store.cluster_state"))["pending"]["token"]
        a.close()
        source.crash_restart()
        a = source.client(); clients[0] = a
        expect_error(a.cmd("SET", "bar", "must-not-overwrite"), "TRYAGAIN")
        target.start()
        c = target.client(); clients[2] = c
        assert a.cmd("MIGRATE", "127.0.0.1", target.port, "bar", 0, 5000, "REPLACE") == b"OK"
        assert a.cmd("PGCACHE.MIGRATION") is None
        assert cluster.get("bar") == b"left"
        assert cluster.set("bar", "newer-than-import")
        assert c.cmd("PGCACHE.IMPORT", "bar", "stale-copy", -1, source.id, 1, first_token) == b"OK"
        assert cluster.get("bar") == b"newer-than-import"
        assert cluster.set("bar", "left")

        # An uncertain transfer whose source expires still needs a destination
        # replay fence before permitting new writes to that key.
        c.close(); target.stop("immediate")
        assert a.cmd("PEXPIRE", "{bar}late-expiry", 500) == 1
        expect_error(a.cmd("MIGRATE", "127.0.0.1", target.port, "{bar}late-expiry", 0, 100), "IOERR")
        expired_token = json.loads(source.sql("SELECT state FROM cache_store.cluster_state"))["pending"]["token"]
        time.sleep(.6)
        target.start(); c = target.client(); clients[2] = c
        assert a.cmd("MIGRATE", "127.0.0.1", target.port, "{bar}late-expiry", 0, 5000, "REPLACE") == b"NOKEY"
        assert cluster.set("{bar}late-expiry", "new-live-value")
        assert c.cmd("PGCACHE.IMPORT", "{bar}late-expiry", "old-expired-copy", 0, source.id, 1, expired_token) == b"OK"
        assert cluster.get("{bar}late-expiry") == b"new-live-value"

        # Hold the destination writer so the source times out after sending its
        # import. Release it: destination commits, but the reply is now lost.
        table = "cache_journal.state" if options.journal else "cache_store.entries"
        lock = target.lock(table)
        try:
            expect_error(a.cmd("MIGRATE", "127.0.0.1", target.port, "{bar}a", 0, 200), "IOERR")
        finally:
            lock.communicate("ROLLBACK;\n\\q\n", timeout=5)
        eventually(target.committed)
        assert c.cmd("ASKING") == b"OK" and c.cmd("GET", "{bar}a") == b"one"
        a.close(); c.close()
        source.crash_restart(); target.crash_restart()
        a = source.client(); c = target.client(); clients[0] = a; clients[2] = c
        expect_error(a.cmd("DEL", "{bar}a"), "TRYAGAIN")
        assert a.cmd("MIGRATE", "127.0.0.1", target.port, "{bar}a", 0, 5000, "REPLACE") == b"OK"
        assert cluster.get("{bar}a") == b"one"
        # Crash after source deletion commits but before the pending intent is
        # cleared. On recovery, NOKEY must clear the fence without reimporting.
        eventually(source.committed)
        checkpoint = "SELECT committed FROM cache_journal.state" if options.journal else "SELECT sequence FROM cache_store.checkpoint"
        before_delete = int(source.sql(checkpoint))
        destination_lock = target.lock(table)
        metadata_lock = None
        def migrating_client():
            transfer_client = source.client()
            transfer_client.socket.settimeout(20)
            try:
                return transfer_client.cmd("MIGRATE", "127.0.0.1", target.port, "{bar}b", 0, 10000, "REPLACE")
            except (EOFError, OSError):
                return "disconnected"
            finally:
                transfer_client.close()
        with ThreadPoolExecutor(max_workers=1) as pool:
            transfer = pool.submit(migrating_client)
            try:
                eventually(lambda: json.loads(source.sql("SELECT state FROM cache_store.cluster_state"))["pending"] is not None)
                metadata_lock = source.lock("cache_store.cluster_state")
                destination_lock.communicate("ROLLBACK;\n\\q\n", timeout=5)
                destination_lock = None
                eventually(lambda: int(source.sql(checkpoint)) > before_delete)
                source.stop("immediate")
                assert transfer.result(timeout=5) == "disconnected"
            finally:
                if destination_lock:
                    destination_lock.communicate("ROLLBACK;\n\\q\n", timeout=5)
                if metadata_lock:
                    metadata_lock.communicate(timeout=5)
        a.close(); source.start(); a = source.client(); clients[0] = a
        expect_error(a.cmd("GET", "{bar}b"), "TRYAGAIN")
        assert a.cmd("MIGRATE", "127.0.0.1", target.port, "{bar}b", 0, 5000, "REPLACE") == b"NOKEY"
        assert cluster.get("{bar}b") == b"two"
        while keys := a.cmd("CLUSTER", "GETKEYSINSLOT", slot, 1):
            for key in keys:
                assert a.cmd("MIGRATE", "127.0.0.1", target.port, key, 0, 5000, "REPLACE") == b"OK"
        assert cluster.get(b"{bar}large\x00") == b"x" * (1024 * 1024 - 60)
        assert a.cmd("CLUSTER", "COUNTKEYSINSLOT", slot) == 0
        assert c.cmd("CLUSTER", "SETSLOT", slot, "NODE", target.id) == b"OK"
        # Crash between topology updates; stale clients still follow ASK safely.
        c.close(); target.crash_restart(); c = target.client(); clients[2] = c
        assert cluster.get("bar") == b"left"
        for peer in (a, b, c):
            assert peer.cmd("CLUSTER", "SETSLOT", slot, "NODE", target.id) == b"OK"
        assert cluster.get("{bar}b") == b"two"
        assert cluster.set("bar", "after-move")
        for shard in shards:
            eventually(shard.committed)
        for client in clients:
            client.close()
        clients.clear()
        cluster.close(); cluster = None
        for shard in shards:
            shard.crash_restart()
        cluster = RedisCluster(host="127.0.0.1", port=source.port, socket_timeout=10)
        assert cluster.get("bar") == b"after-move"
        replay_client = target.client()
        try:
            assert replay_client.cmd("PGCACHE.IMPORT", "bar", "stale-after-restart", -1, source.id, 1, first_token) == b"OK"
        finally:
            replay_client.close()
        assert cluster.get("bar") == b"after-move"
        assert cluster.get("{bar}new") == b"created-at-target"
        assert cluster.get("{bar}a") == b"one"
        assert cluster.hgetall("{bar}hash") == {b"f\x00": b"v\xff"}
        assert cluster.lrange("{bar}list", 0, -1) == [b"a", b"b", b"c"]
        assert cluster.smembers("{bar}set") == {b"a", b"b"}
        assert cluster.zrange("{bar}zset", 0, -1, withscores=True) == [(b"b", 1.0), (b"a", 2.0)]
        assert cluster.get("foo") == b"binary\x00\xff"
        assert cluster.get_node_from_key("bar").port == target.port
        # Exercise the operator itself, including idempotent completion, moving
        # the slot back while this client's cached topology still points at C.
        from reshard import move_slot
        assert move_slot(nodes, target.id, source.id, slot) >= 4
        assert move_slot(nodes, target.id, source.id, slot) == 0
        assert cluster.get("bar") == b"after-move"
        assert cluster.get("{bar}a") == b"one"
        print(f"PASS: redis-py {redis.__version__}, three PostgreSQL shards, discovery, MOVED/ASK/ASKING, CROSSSLOT, pipeline, TTL migration, interrupted transfers, durable topology and crash recovery ({'journal' if options.journal else 'direct'})")
    except Exception:
        for shard in shards:
            print(shard.log.read_text()[-4000:])
        raise
    finally:
        if cluster:
            cluster.close()
        for client in clients:
            client.close()
        for shard in shards:
            shard.stop()
        print(f"Isolated test data and logs: {work}")


if __name__ == "__main__":
    main()
