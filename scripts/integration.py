#!/usr/bin/env python3
"""Real PostgreSQL lifecycle checks, using a disposable workspace-owned cluster.

No external Python packages; never connects to an existing database.
The extension SQL is applied directly to avoid installing into the system share dir.
"""
import json
import hashlib
import os
import random
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def frame(*args):
    args = [a if isinstance(a, bytes) else str(a).encode() for a in args]
    return b"*%d\r\n" % len(args) + b"".join(b"$%d\r\n" % len(a) + a + b"\r\n" for a in args)


class RedisError(Exception):
    pass


class Client:
    def __init__(self, port, host="127.0.0.1"):
        self.socket = socket.create_connection((host, port), timeout=5)
        self.file = self.socket.makefile("rb")

    def read(self):
        line = self.file.readline()
        if not line:
            raise EOFError("server closed connection")
        kind, value = line[:1], line[1:-2]
        if kind == b"+": return value
        if kind == b"-": return RedisError(value.decode())
        if kind == b":": return int(value)
        if kind == b"*": return [self.read() for _ in range(int(value))]
        if kind == b"$":
            n = int(value)
            if n == -1: return None
            data = self.file.read(n)
            assert len(data) == n and self.file.read(2) == b"\r\n"
            return data
        raise AssertionError(line)

    def cmd(self, *args):
        self.socket.sendall(frame(*args))
        return self.read()

    def close(self):
        self.file.close()
        self.socket.close()


def check_input_framing(client):
    # Grow the receive buffer, then alternate short frames and partial tails.
    # Old initialized bytes must never be parsed as additional commands.
    value = bytes(range(256)) * 96
    assert client.cmd("ECHO", value) == value
    for split in (1, 7, 17, 8191):
        tail = frame("ECHO", value[:9000])
        client.socket.sendall(frame("PING") + tail[:split])
        assert client.read() == b"PONG"
        client.socket.sendall(tail[split:] + frame("ECHO", b"short"))
        assert client.read() == value[:9000]
        assert client.read() == b"short"
    for byte in frame("ECHO", b"fragment\x00"):
        client.socket.sendall(bytes([byte]))
    assert client.read() == b"fragment\x00"
    client.socket.sendall(frame("PING") * 6000)
    assert all(client.read() == b"PONG" for _ in range(6000))
    # Exceed the per-poll read budget, then half-close before reading replies.
    # Explicit continuations must drain remaining requests without another edge,
    # and EOF must not discard responses already produced for this connection.
    half_closed = Client(client.socket.getpeername()[1])
    try:
        half_closed.socket.sendall(frame("PING") * 12000)
        half_closed.socket.shutdown(socket.SHUT_WR)
        assert all(half_closed.read() == b"PONG" for _ in range(12000))
        assert half_closed.file.read(1) == b""
    finally:
        half_closed.close()
    # Exercise repeated slot reuse with overlapping requests and clean closes.
    # Unique replies expose accidental delivery to another connection generation.
    port = client.socket.getpeername()[1]
    for generation in range(16):
        peers = []
        try:
            for index in range(32):
                peer = Client(port)
                marker = f'churn-{generation}-{index}'.encode() + b'\x00\xff'
                peers.append((peer, marker))
                peer.socket.sendall(frame('ECHO', marker) + frame('QUIT'))
            for peer, marker in peers:
                assert peer.read() == marker and peer.read() == b'OK'
                assert peer.file.read(1) == b''
        finally:
            for peer, _ in peers:
                peer.close()


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def eventually(fn, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result: return result
        except (OSError, EOFError):
            pass
        time.sleep(.02)
    raise AssertionError("condition did not become true")


def main():
    config = os.environ.get("PGRX_PG_CONFIG_PATH", "pg_config")
    bindir = Path(subprocess.check_output([config, "--bindir"], text=True).strip())
    artifact = ROOT / "target" / os.environ.get("PG_CACHE_PROFILE", "debug") / ("libpg_cache.dylib" if os.uname().sysname == "Darwin" else "libpg_cache.so")
    assert artifact.exists(), "build pg-cache first"
    base = ROOT / ".local"
    base.mkdir(exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="integration-", dir=base))
    sources = sorted((ROOT / "crates").rglob("*.rs")) + sorted((ROOT / "crates").rglob("*.sql")) + sorted((ROOT / "crates").rglob("Cargo.toml")) + sorted((ROOT / "scripts").glob("*.py")) + [ROOT / "Cargo.toml", ROOT / "Cargo.lock"]
    disable_nestloop = os.environ.get("PG_CACHE_DISABLE_NESTLOOP", "0")
    assert disable_nestloop in ("0", "1")
    value_storage = os.environ.get("PG_CACHE_VALUE_STORAGE", "EXTENDED").upper()
    assert value_storage in ("EXTENDED", "EXTERNAL", "MAIN")
    fast_autovacuum = os.environ.get("PG_CACHE_FAST_AUTOVACUUM", "0")
    assert fast_autovacuum in ("0", "1")
    metadata = {
        "library_sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
        "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
        "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "postgres": subprocess.check_output([str(bindir / "postgres"), "--version"], text=True).strip(),
        "machine": os.uname().machine,
        "profile": os.environ.get("PG_CACHE_PROFILE", "debug"),
        "flush_ms": 25,
        "memory_mb": 64,
        "fillfactor_override": os.environ.get("PG_CACHE_FILLFACTOR"),
        "disable_nestloop": disable_nestloop == "1",
        "value_storage": value_storage,
        "fast_autovacuum": fast_autovacuum == "1",
    }
    (work / "build.json").write_text(json.dumps(metadata, indent=2))
    data, lib, log = work / "data", work / "lib", work / "postgres.log"
    lib.mkdir()
    shutil.copy2(artifact, lib / ("pg_cache" + artifact.suffix))
    pg_port, redis_port = free_port(), free_port()
    while redis_port == pg_port: redis_port = free_port()

    def tool(name, *args, **kwargs):
        return subprocess.run([str(bindir / name), *map(str, args)], check=True, capture_output=True, text=True, **kwargs)

    def sql(statement):
        return tool("psql", "-h", "127.0.0.1", "-p", pg_port, "-d", "postgres", "-XAt", "-v", "ON_ERROR_STOP=1", "-c", statement).stdout.strip()

    def connect():
        c = Client(redis_port)
        if c.cmd("PING") != b"PONG": c.close(); return None
        return c

    def drained(c):
        stats = json.loads(c.cmd("PGCACHE.STATS"))
        return stats["accepted_sequence"] == stats["committed_sequence"] and stats["pending_bytes"] == 0

    def lock_table(table):
        lock = subprocess.Popen([str(bindir / "psql"), "-h", "127.0.0.1", "-p", str(pg_port), "-d", "postgres", "-XAt", "-v", "ON_ERROR_STOP=1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        assert table in ("cache_store.entries", "cache_store.journal")
        lock.stdin.write(f"BEGIN; LOCK {table} IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n")
        lock.stdin.flush()
        while lock.stdout.readline().strip() != "locked":
            if lock.poll() is not None: raise AssertionError("lock session exited")
        return lock

    running = False
    try:
        tool("initdb", "-D", data, "-A", "trust", "--no-locale", "-E", "UTF8")
        with (data / "postgresql.conf").open("a") as f:
            f.write(f"\nlisten_addresses='127.0.0.1'\nport={pg_port}\nunix_socket_directories=''\nshared_preload_libraries='pg_cache'\ndynamic_library_path='{lib}:$libdir'\npg_cache.port={redis_port}\npg_cache.flush_ms=25\n")
            if disable_nestloop == "1":
                f.write("enable_nestloop=off\n")
            if fast_autovacuum == "1":
                f.write("autovacuum_naptime='1s'\nautovacuum_vacuum_cost_delay=0\nlog_autovacuum_min_duration=0\n")
        tool("pg_ctl", "-D", data, "-l", log, "-w", "start")
        running = True
        sql((ROOT / "crates/pg-cache/sql/pg_cache--0.1.0.sql").read_text())
        sql(f"ALTER TABLE cache_store.entries ALTER COLUMN value SET STORAGE {value_storage}")
        if fast_autovacuum == "1":
            sql("ALTER TABLE cache_store.entries SET (autovacuum_vacuum_scale_factor=0.02, toast.autovacuum_vacuum_scale_factor=0.02)")
        if os.environ.get("PG_CACHE_FILLFACTOR"):
            fillfactor = int(os.environ["PG_CACHE_FILLFACTOR"])
            assert 10 <= fillfactor <= 100
            sql(f"ALTER TABLE cache_store.entries SET (fillfactor={fillfactor})")
        c = eventually(connect)
        assert c.cmd("SET", b"binary\x00key", b"value\r\n\xff\x00") == b"OK"
        assert c.cmd("GET", b"binary\x00key") == b"value\r\n\xff\x00"
        assert c.cmd("MSET", "a", "1", "b", "2") == b"OK"
        assert c.cmd("MGET", "a", "b", "absent") == [b"1", b"2", None]
        assert c.cmd("SET", "a", "wrong", "NX") is None
        assert c.cmd("INCR", "a") == 2
        assert c.cmd("DEL", "b", "b") == 1
        assert c.cmd("SET", "gone", "x", "PX", 300) == b"OK"
        check_input_framing(c)
        eventually(lambda: drained(c))
        assert sql("SELECT encode(value,'hex') FROM cache_store.entries WHERE key=decode('62696e617279006b6579','hex')") == b"value\r\n\xff\x00".hex()
        assert sql("SELECT count(*) FROM cache_store.entries WHERE key=convert_to('b','UTF8')") == "0"

        # Tuple-location optimizations must treat locations as hints. Exercise
        # stale hints after an external row update, deletion, and physical rewrite.
        for key in ("hint:z", "hint:y", "hint:x"):
            assert c.cmd("SET", key, "before") == b"OK"
        eventually(lambda: drained(c))
        sql("UPDATE cache_store.entries SET value=convert_to('external','UTF8') WHERE key=convert_to('hint:z','UTF8')")
        sql("DELETE FROM cache_store.entries WHERE key=convert_to('hint:y','UTF8')")
        assert c.cmd("MSET", "hint:z", "after-z", "hint:y", "after-y") == b"OK"
        eventually(lambda: drained(c))
        sql("CLUSTER cache_store.entries USING entries_pkey")
        assert c.cmd("MSET", "hint:z", "final-z", "hint:y", "final-y", "hint:x", "final-x") == b"OK"
        eventually(lambda: drained(c))
        assert sql("SELECT string_agg(convert_from(value,'UTF8'), ',' ORDER BY key) FROM cache_store.entries WHERE key IN (convert_to('hint:x','UTF8'),convert_to('hint:y','UTF8'),convert_to('hint:z','UTF8'))") == "final-x,final-y,final-z"
        assert sql("SELECT encode(value,'hex') FROM cache_store.entries WHERE key=decode('62696e617279006b6579','hex')") == b"value\r\n\xff\x00".hex()

        # Unsupported fast-path indexes/triggers must use the SQL executor.
        sql("CREATE INDEX hint_value_idx ON cache_store.entries ((octet_length(value))) WHERE expires_at_ms IS NULL")
        assert c.cmd("SET", "hint:x", b"x" * 117) == b"OK"
        eventually(lambda: drained(c))
        assert sql("SET enable_seqscan=off; SELECT count(*) FROM cache_store.entries WHERE expires_at_ms IS NULL AND octet_length(value)=117").splitlines()[-1] == "1"
        sql("DROP INDEX cache_store.hint_value_idx")
        sql("CREATE TABLE cache_store.hint_audit(n int); CREATE FUNCTION cache_store.hint_audit_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO cache_store.hint_audit VALUES (1); RETURN NEW; END $$; CREATE TRIGGER hint_audit_trigger AFTER UPDATE ON cache_store.entries FOR EACH ROW EXECUTE FUNCTION cache_store.hint_audit_fn()")
        assert c.cmd("SET", "hint:x", "triggered") == b"OK"
        eventually(lambda: drained(c))
        assert sql("SELECT count(*) FROM cache_store.hint_audit") == "1"
        sql("DROP TRIGGER hint_audit_trigger ON cache_store.entries; DROP FUNCTION cache_store.hint_audit_fn(); DROP TABLE cache_store.hint_audit")

        # Shrink the file beneath cached block numbers, then recreate every key.
        shrink_keys = [f"shrink:{n:03}" for n in range(100)]
        assert c.cmd("MSET", *(part for key in shrink_keys for part in (key, b"x" * 1024))) == b"OK"
        eventually(lambda: drained(c))
        sql("DELETE FROM cache_store.entries WHERE key >= convert_to('shrink:010','UTF8') AND key <= convert_to('shrink:099','UTF8')")
        sql("VACUUM FULL cache_store.entries")
        assert c.cmd("MSET", *(part for key in shrink_keys for part in (key, "new"))) == b"OK"
        eventually(lambda: drained(c))
        assert sql("SET enable_seqscan=off; SELECT count(*) FROM cache_store.entries WHERE key >= convert_to('shrink:000','UTF8') AND key <= convert_to('shrink:099','UTF8') AND value=convert_to('new','UTF8')").splitlines()[-1] == "100"
        assert c.cmd("SET", "native-probe", "first") == b"OK"
        eventually(lambda: drained(c))
        before_native = json.loads(c.cmd("PGCACHE.STATS")).get("native_rows")
        assert c.cmd("SET", "native-probe", "second") == b"OK"
        eventually(lambda: drained(c))
        if before_native is not None:
            assert json.loads(c.cmd("PGCACHE.STATS"))["native_rows"] > before_native
        move_keys = [f"move:{n:03}" for n in range(100)]
        assert c.cmd("MSET", *(part for key in move_keys for part in (key, "s"))) == b"OK"
        eventually(lambda: drained(c))
        assert c.cmd("MSET", *(part for key in move_keys for part in (key, b"v" * 1800))) == b"OK"
        eventually(lambda: drained(c))
        sql("VACUUM cache_store.entries")
        assert sql("SET enable_seqscan=off; SELECT count(*) FROM cache_store.entries WHERE key >= convert_to('move:000','UTF8') AND key <= convert_to('move:099','UTF8') AND octet_length(value)=1800").splitlines()[-1] == "100"
        toast_rng = random.Random(42)
        toast_before, toast_after = toast_rng.randbytes(12000), toast_rng.randbytes(16000)
        assert c.cmd("SET", "toast-key", toast_before) == b"OK"
        eventually(lambda: drained(c))
        assert c.cmd("SET", "toast-key", toast_after, "PX", 600000) == b"OK"
        eventually(lambda: drained(c))
        assert sql("SELECT encode(value,'hex') FROM cache_store.entries WHERE key=convert_to('toast-key','UTF8')") == toast_after.hex()
        assert sql("SELECT expires_at_ms IS NOT NULL FROM cache_store.entries WHERE key=convert_to('toast-key','UTF8')") == "t"
        assert c.cmd("PERSIST", "toast-key") == 1
        eventually(lambda: drained(c))
        assert sql("SELECT expires_at_ms IS NULL FROM cache_store.entries WHERE key=convert_to('toast-key','UTF8')") == "t"

        # An acknowledged write must leave local staging even if its client
        # closes and no further RESP traffic arrives to wake the event loop.
        short_client = Client(redis_port)
        assert short_client.cmd("SET", "disconnect-staged", "kept") == b"OK"
        short_client.close()
        eventually(lambda: sql("SELECT convert_from(value,'UTF8') FROM cache_store.entries WHERE key=convert_to('disconnect-staged','UTF8')") == "kept")

        # Block the table writer, fill the bounded queue, and verify failed writes
        # never appear in memory. Other clients can still read while SPI is blocked.
        lock = lock_table("cache_store.entries")
        try:
            assert c.cmd("SET", "backlog-barrier", "blocked") == b"OK"
            eventually(lambda: sql("SELECT count(*) FROM pg_stat_activity WHERE backend_type='pg_cache' AND wait_event_type='Lock'") == "1")
            c.socket.sendall(b"".join(frame("SET", "backpressure", str(n).encode() + b"x" * 4096) for n in range(6000)))
            reader = Client(redis_port)
            assert reader.cmd("GET", "a") == b"2"
            reader.close()
            replies = [c.read() for _ in range(6000)]
            accepted = [i for i, r in enumerate(replies) if r == b"OK"]
            assert accepted and any(isinstance(r, RedisError) for r in replies)
            assert c.cmd("GET", "backpressure") == (str(accepted[-1]).encode() + b"x" * 4096)
            assert c.cmd("GET", "a") == b"2"
        finally:
            lock.stdin.write("ROLLBACK;\n\\q\n"); lock.stdin.flush()
            lock.communicate(timeout=5)
        eventually(lambda: drained(c))
        c.close()

        # Immediate shutdown skips the cache's graceful flush. All writes below
        # were already observed committed; PostgreSQL WAL recovery must retain them.
        tool("pg_ctl", "-D", data, "-m", "immediate", "-w", "stop")
        running = False
        tool("pg_ctl", "-D", data, "-l", log, "-w", "start")
        running = True
        c = eventually(connect)
        assert c.cmd("GET", b"binary\x00key") == b"value\r\n\xff\x00"
        assert c.cmd("GET", "a") == b"2"
        assert c.cmd("GET", "b") is None
        assert c.cmd("GET", "gone") is None
        assert c.cmd("GET", "backpressure") == (str(accepted[-1]).encode() + b"x" * 4096)
        assert c.cmd("GET", "toast-key") == toast_after
        # Graceful shutdown must drain acknowledged pending writes too.
        assert c.cmd("SET", "graceful", "retained") == b"OK"
        c.close()
        tool("pg_ctl", "-D", data, "-m", "fast", "-w", "stop")
        running = False
        tool("pg_ctl", "-D", data, "-l", log, "-w", "start")
        running = True
        c = eventually(connect)
        assert c.cmd("GET", "graceful") == b"retained"
        c.close()
        print("PASS: RESP, binary values, pipelining, SQL persistence, bounded backlog, crash recovery, TTL recovery, graceful drain")
        if os.environ.get("REDIS_BIN_DIR") and (ROOT / "scripts" / "compare.py").is_file():
            from compare import compare
            compare(redis_port, work, os.environ["REDIS_BIN_DIR"], sql)
    except Exception:
        if log.exists(): print(log.read_text()[-12000:])
        raise
    finally:
        if running: tool("pg_ctl", "-D", data, "-m", "fast", "-w", "stop")
        print(f"Isolated test cluster and logs: {work}")


if __name__ == "__main__":
    main()
