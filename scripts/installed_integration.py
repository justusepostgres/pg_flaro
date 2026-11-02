#!/usr/bin/env python3
"""Exercise installed extension packages, as required by CNPG bootstrap.

Run as a non-root PostgreSQL OS user after make install FEATURES=journal-prototype.
Uses its own cluster and ports; never connects to an existing database.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile

from integration import ROOT, Client, eventually, free_port


def main():
    config = os.environ.get("PGRX_PG_CONFIG_PATH", "pg_config")
    bindir = Path(subprocess.check_output([config, "--bindir"], text=True).strip())
    (ROOT / ".local").mkdir(exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="installed-integration-", dir=ROOT / ".local"))
    data, log = work / "data", work / "postgres.log"
    pg_port, resp_port = free_port(), free_port()
    while resp_port == pg_port:
        resp_port = free_port()

    def pg(name, *args):
        return subprocess.check_output([str(bindir / name), *map(str, args)], text=True, stderr=subprocess.STDOUT)

    def sql(query):
        return pg("psql", "-h", "127.0.0.1", "-p", pg_port, "-d", "postgres", "-XAt", "-v", "ON_ERROR_STOP=1", "-c", query).strip()

    running = False
    client = None
    try:
        pg("initdb", "-D", data, "-A", "trust", "--no-locale", "-E", "UTF8")
        with (data / "postgresql.conf").open("a") as f:
            f.write(f"\nlisten_addresses='127.0.0.1'\nport={pg_port}\nunix_socket_directories=''\nshared_preload_libraries='pg_cache'\npg_cache.journal=on\npg_cache.journal_pending_mb=128\npg_cache.listen_address='0.0.0.0'\npg_cache.port={resp_port}\npg_cache.flush_ms=25\n")
        pg("pg_ctl", "-D", data, "-l", log, "-w", "start")
        running = True
        sql("CREATE EXTENSION pg_cache; CREATE EXTENSION pg_cache_journal")
        assert sql("SELECT count(*) FROM pg_extension WHERE extname IN ('pg_cache','pg_cache_journal')") == "2"
        assert sql("SELECT count(*)=6 AND bool_and(relpersistence='p') FROM pg_class WHERE relnamespace IN ('cache_store'::regnamespace,'cache_journal'::regnamespace) AND relkind='r'") == "t"
        assert sql("SHOW pg_cache.listen_address") == "0.0.0.0"

        def connect():
            c = Client(resp_port, host=os.environ.get("PG_CACHE_TEST_HOST", "127.0.0.1"))
            try:
                assert c.cmd("PING") == b"PONG"
                return c
            except Exception:
                c.close()
                raise

        client = eventually(connect)
        expected = {b"binary\x00\xff": b"value\xff\x00", b"plain": b"persisted"}
        for key, value in expected.items():
            assert client.cmd("SET", key, value) == b"OK"
        assert client.cmd("SET", b"deleted", b"x") == b"OK"
        assert client.cmd("DEL", b"deleted") == 1

        def drained():
            s = json.loads(client.cmd("PGCACHE.STATS"))
            return s["accepted_sequence"] == s["committed_sequence"] and s["pending_bytes"] == 0 and sql("SELECT materialized=committed FROM cache_journal.state") == "t"

        eventually(drained)
        assert sql("SELECT count(*) FROM cache_journal.entries") == "2"
        client.close()
        client = None
        pg("pg_ctl", "-D", data, "-m", "immediate", "-w", "stop")
        running = False
        pg("pg_ctl", "-D", data, "-l", log, "-w", "start")
        running = True
        client = eventually(connect)
        for key, value in expected.items():
            assert client.cmd("GET", key) == value
        assert client.cmd("GET", b"deleted") is None
        (work / "result.json").write_text(json.dumps({"passed": True, "checks": ["CREATE EXTENSION bootstrap", "six WAL-logged tables", "configured wildcard listener", "materialized table contents", "committed binary/delete recovery"]}, indent=2))
        print("PASS: installed extension bootstrap and recovery")
    finally:
        if client is not None:
            client.close()
        if running:
            pg("pg_ctl", "-D", data, "-m", "immediate", "-w", "stop")
        print(f"Installed-extension artifacts: {work}")


if __name__ == "__main__":
    main()
