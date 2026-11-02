#!/usr/bin/env python3
"""Bounded collection benchmark. Same driver, keys, pipelines and machine for both servers.
Requires redis-py; starts disposable PostgreSQL and Redis processes only.
"""

import argparse, hashlib, json, os, platform, shutil, subprocess, sys, tempfile, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import redis
from cluster_integration import Shard, eventually
from integration import free_port

SEED = b"seed" + b"x" * 60
OPS = ["hset", "hget", "lset", "lindex", "set-churn", "sismember", "zincrby", "zscore"]
WRITES = {"hset", "lset", "set-churn", "zincrby"}


def stats(client):
    return json.loads(client.execute_command("PGCACHE.STATS"))


def drain(client):
    start = time.monotonic()
    eventually(
        lambda: (
            lambda s: s["accepted_sequence"] == s["committed_sequence"]
            and s["pending_bytes"] == 0
        )(stats(client)),
        timeout=120,
    )
    return time.monotonic() - start


def seed(client, op, clients, card):
    p = client.pipeline(transaction=False)
    for i in range(clients):
        key = f"bench:{i}"
        p.delete(key)
        for start in range(0, card, 200):
            members = [f"f{j}" for j in range(start, min(start + 200, card))]
            if op.startswith("h"):
                p.hset(key, mapping={m: SEED for m in members})
            elif op.startswith("l"):
                p.rpush(key, *[SEED for _ in members])
            elif op in ("set-churn", "sismember"):
                p.sadd(key, *members)
            else:
                p.zadd(key, {m: 0 for m in members})
    p.execute()


def verify(client, op, card, counts):
    for i, n in counts:
        key = f"bench:{i}"
        members = [f"f{j}" for j in range(card)]
        if op in ("hset", "lset"):
            expected = []
            for j in range(card):
                last = ((n - 1 - j) // card) * card + j
                expected.append(
                    f"{last:016x}".encode() + b"x" * 48 if last >= 0 else SEED
                )
            actual = (
                [
                    v
                    for start in range(0, card, 200)
                    for v in client.hmget(key, members[start : start + 200])
                ]
                if op == "hset"
                else client.lrange(key, 0, -1)
            )
            assert actual == expected, (op, i, n, "value mismatch")
        elif op == "set-churn":
            assert client.smembers(key) == {m.encode() for m in members}, (op, i)
        elif op == "zincrby":
            assert [
                v
                for start in range(0, card, 200)
                for v in client.zmscore(key, members[start : start + 200])
            ] == [
                float((n - 1 - j) // card + 1) if n > j else 0.0 for j in range(card)
            ], (op, i, n)


def cpu():
    p = Path("/sys/fs/cgroup/cpu.stat")
    return (
        {k: int(v) for k, v in (line.split() for line in p.read_text().splitlines())}
        if p.exists()
        else {}
    )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--library", type=Path, required=True)
    p.add_argument("--redis-server", type=Path, required=True)
    p.add_argument(
        "--driver", type=Path, default=ROOT / "target/release/collection-bench"
    )
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--operations", default=",".join(OPS))
    p.add_argument("--cardinalities", default="128,1024")
    p.add_argument("--pipelines", default="1,16")
    p.add_argument("--clients", type=int, default=50)
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--seconds", type=float, default=3)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--restart-check", action="store_true")
    a = p.parse_args()
    bindir = Path(
        subprocess.check_output(
            [os.environ.get("PGRX_PG_CONFIG_PATH", "pg_config"), "--bindir"], text=True
        ).strip()
    )
    work = Path(tempfile.mkdtemp(prefix="collection-bench-", dir=ROOT / ".local"))
    (work / "lib").mkdir()
    shutil.copy2(a.library, work / "lib" / ("pg_cache" + a.library.suffix))
    shard = None
    server = None
    clients = {}
    results = []
    a.output.parent.mkdir(parents=True, exist_ok=True)
    meta = {
        "machine": platform.machine(),
        "postgres": subprocess.check_output(
            [str(bindir / "postgres"), "--version"], text=True
        ).strip(),
        "redis": subprocess.check_output(
            [str(a.redis_server), "--version"], text=True
        ).strip(),
        "library_sha256": hashlib.sha256(a.library.read_bytes()).hexdigest(),
        "driver_sha256": hashlib.sha256(a.driver.read_bytes()).hexdigest(),
        "source_revision": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
        if (ROOT / ".git").exists()
        else "archive",
        "settings": {
            "flaro": "journal write-behind, WAL logged, flush_ms=100, pending=128MiB, memory=512MiB",
            "redis": "AOF off, RDB off, maxmemory=512MiB, noeviction",
            "clients_and_servers": "same machine/cgroup; CPU counters include driver and idle peer",
            "payload_bytes": 64,
        },
        "arguments": {
            k: str(v) if isinstance(v, Path) else v for k, v in vars(a).items()
        },
        "workdir": str(work),
    }

    def save():
        a.output.write_text(
            json.dumps({"metadata": meta, "results": results}, indent=2)
        )

    try:
        node = {
            "id": "1" * 40,
            "host": "127.0.0.1",
            "port": free_port(),
            "slots": [[0, 16383]],
        }
        shard = Shard(work, 0, [node], bindir, True, cluster_mode=False)
        shard.stop()
        with (shard.data / "postgresql.conf").open("a") as f:
            f.write(
                "\npg_cache.memory_mb=512\npg_cache.journal_pending_mb=128\npg_cache.flush_ms=100\nshared_buffers='512MB'\nmax_wal_size='4GB'\ncheckpoint_timeout='1h'\n"
            )
        shard.start()
        rp = free_port()
        with (work / "redis.log").open("w") as log:
            server = subprocess.Popen(
                [
                    str(a.redis_server),
                    "--port",
                    str(rp),
                    "--bind",
                    "127.0.0.1",
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                    "--maxmemory",
                    "512mb",
                    "--maxmemory-policy",
                    "noeviction",
                    "--dir",
                    str(work),
                ],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        clients = {
            "flaro": redis.Redis(port=shard.port, socket_timeout=120),
            "redis": redis.Redis(port=rp, socket_timeout=120),
        }
        eventually(clients["redis"].ping)
        for card in map(int, a.cardinalities.split(",")):
            for pipeline in map(int, a.pipelines.split(",")):
                for op in a.operations.split(","):
                    for repeat in range(a.repeats):
                        for name in (
                            ["flaro", "redis"]
                            if repeat % 2 == 0
                            else ["redis", "flaro"]
                        ):
                            c = clients[name]
                            seed(c, op, a.clients, card)
                            if name == "flaro":
                                drain(c)
                            before = stats(c) if name == "flaro" else None
                            cb = cpu()
                            run = subprocess.run(
                                [
                                    str(a.driver),
                                    f"127.0.0.1:{shard.port if name == 'flaro' else rp}",
                                    op,
                                    str(card),
                                    str(a.clients),
                                    str(pipeline),
                                    str(a.seconds),
                                    str(a.threads),
                                ],
                                text=True,
                                capture_output=True,
                                timeout=a.seconds + 120,
                            )
                            assert run.returncode == 0, (name, op, run.stderr)
                            result = json.loads(run.stdout)
                            ca = cpu()
                            result.update(
                                server=name,
                                repeat=repeat,
                                cpu_delta={k: ca[k] - cb.get(k, 0) for k in ca},
                            )
                            if name == "flaro":
                                result["drain_seconds"] = drain(c)
                                after = stats(c)
                                result["persistence_before"] = before
                                result["persistence_after"] = after
                                assert after["accepted_sequence"] - before[
                                    "accepted_sequence"
                                ] == (result["commands"] if op in WRITES else 0), (
                                    op,
                                    before,
                                    after,
                                    result,
                                )
                            if op in WRITES:
                                verify(c, op, card, result["per_client_iterations"])
                            if (
                                name == "flaro"
                                and a.restart_check
                                and repeat == a.repeats - 1
                                and op in WRITES
                            ):
                                c.close()
                                shard.crash_restart()
                                c = redis.Redis(port=shard.port, socket_timeout=120)
                                clients[name] = c
                                verify(c, op, card, result["per_client_iterations"])
                                result["crash_verified"] = True
                            results.append(result)
                            save()
                            print(
                                f"{name} {op} n={card} P={pipeline} rep={repeat}: {result['ops_per_second']:.0f} ops/s p99={result['p99_ms']:.3f}ms",
                                flush=True,
                            )
        print(
            "PASS: all responses, mutation counts, final states and requested restart checks",
            flush=True,
        )
    finally:
        save()
        for c in clients.values():
            c.close()
        if server:
            server.terminate()
            server.wait(timeout=20)
        if shard:
            shard.stop()


if __name__ == "__main__":
    main()
