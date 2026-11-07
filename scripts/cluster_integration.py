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
from native_cluster_transactions import check_native_cluster_transactions

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
    parser.add_argument("--protocol", type=int, choices=[2, 3], default=2)
    parser.add_argument("--native-redis", type=Path, help="Compare transactions with a disposable native Redis cluster")
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
        assert a.cmd('CLIENT','SETNAME','cluster-probe') == b'OK'
        assert a.cmd('CLIENT','GETNAME') == b'cluster-probe'
        assert b.cmd('CLIENT','GETNAME') is None
        assert isinstance(a.cmd('CLIENT','ID'),int)
        assert a.cmd('CLIENT','SETINFO','LIB-NAME','test-client') == b'OK'
        # CLIENT UNBLOCK is local to a serving node and resumes the target socket.
        blocked_peer = Client(shards[0].port)
        try:
            if options.protocol == 3: blocked_peer.cmd('HELLO', 3)
            blocked_id = blocked_peer.cmd('CLIENT', 'ID')
            blocked_peer.socket.sendall(frame('BLPOP', '{bar}cancel', 0))
            time.sleep(.05)
            assert a.cmd('CLIENT', 'UNBLOCK', blocked_id, 'ERROR') == 1
            expect_error(blocked_peer.read(), 'UNBLOCKED client unblocked via CLIENT UNBLOCK')
            assert blocked_peer.cmd('PING') == b'PONG'
            assert a.cmd('CLIENT', 'UNBLOCK', blocked_id) == 0
        finally:
            blocked_peer.close()
        # Real client discovers the topology and command keys, then routes pipelines.
        cluster = RedisCluster(host="127.0.0.1", port=shards[0].port, socket_timeout=10, protocol=options.protocol)
        assert cluster.set("foo", b"binary\x00\xff")
        assert cluster.set("bar", b"left")
        assert cluster.get("foo") == b"binary\x00\xff"
        expect_error(a.cmd("GET", "foo"), "MOVED 12182 ")
        a.socket.sendall(b'*0\r\n*-1\r\nPING\n\r\nGET foo\r\n')
        assert a.read() == b'PONG'
        expect_error(a.read(), 'MOVED 12182 ')
        expect_error(a.cmd("MSET", "foo", "x", "bar", "y"), "CROSSSLOT")
        for command in ('BLPOP', 'BRPOP', 'BZPOPMIN', 'BZPOPMAX'):
            expect_error(a.cmd(command, '{foo}blocking', '{bar}blocking', .01), 'CROSSSLOT')
            expect_error(a.cmd(command, '{foo}blocking', .01), 'MOVED')
        for command, tail in [('BLMOVE', ('LEFT', 'RIGHT', .01)), ('BRPOPLPUSH', (.01,))]:
            expect_error(a.cmd(command, '{foo}source', '{bar}target', *tail), 'CROSSSLOT')
            expect_error(a.cmd(command, '{foo}source', '{foo}target', *tail), 'MOVED')
        for command, side in [('BLMPOP','LEFT'), ('BZMPOP','MIN')]:
            expect_error(a.cmd(command, .01, 2, '{foo}a', '{bar}b', side), 'CROSSSLOT')
            expect_error(a.cmd(command, .01, 2, '{foo}a', '{foo}b', side), 'MOVED')
            assert a.cmd('COMMAND','GETKEYS',command,.01,2,'{foo}a','{foo}b',side)==[b'{foo}a',b'{foo}b']


        assert cluster.get("foo") == b"binary\x00\xff" and cluster.get("bar") == b"left"
        assert c.cmd('WATCH', '{foo}watched') == b'OK'
        assert cluster.set('{foo}watched', 'changed')
        assert c.cmd('MULTI') == b'OK'
        assert c.cmd('SET', '{foo}cas-result', 'never') == b'QUEUED'
        assert c.cmd('EXEC') is None
        assert cluster.get('{foo}cas-result') is None
        expect_error(a.cmd('WATCH', '{foo}watched'), 'MOVED 12182 ')
        assert c.cmd('MULTI') == b'OK'
        assert c.cmd('SET', '{foo}tx', '1') == b'QUEUED'
        assert c.cmd('INCR', '{foo}tx') == b'QUEUED'
        assert c.cmd('GET', '{foo}tx') == b'QUEUED'
        assert cluster.get('{foo}tx') is None
        assert c.cmd('EXEC') == [b'OK', 2, b'2']
        assert cluster.get('{foo}tx') == b'2'
        assert c.cmd('MULTI') == b'OK'
        assert c.cmd('SET', '{foo}tx-abort', 'bad') == b'QUEUED'
        # A redirection while collecting commands must poison the transaction.
        expect_error(c.cmd('SET', '{bar}tx-abort', 'bad'), 'MOVED')
        expect_error(c.cmd('EXEC'), 'EXECABORT')
        assert cluster.get('{foo}tx-abort') is None
        size_before = cluster.dbsize(target_nodes=RedisCluster.PRIMARIES)
        local_size_before = a.cmd("DBSIZE")
        assert cluster.set('{foo}enumerate', b'value')
        assert cluster.set('{bar}enumerate', b'value')
        assert cluster.dbsize(target_nodes=RedisCluster.PRIMARIES) == size_before+2
        assert a.cmd("DBSIZE") == local_size_before+1
        assert set(cluster.keys('*enumerate', target_nodes=RedisCluster.PRIMARIES)) == {b'{foo}enumerate',b'{bar}enumerate'}
        assert cluster.execute_command('TOUCH','{foo}enumerate','{foo}enumerate','{foo}absent') == 2
        expect_error(a.cmd('TOUCH','{foo}enumerate','{bar}enumerate'), 'CROSSSLOT')
        assert cluster.delete('{foo}enumerate') == 1
        assert cluster.delete('{bar}enumerate') == 1
        assert cluster.execute_command('INCRBYFLOAT','{foo}float',1.25) == 1.25
        assert cluster.execute_command('HINCRBYFLOAT','{foo}float-hash','f',1.25) == 1.25
        assert cluster.get('{foo}float') == b'1.25'
        assert cluster.hget('{foo}float-hash','f') == b'1.25'
        assert cluster.hset('{foo}getdel-hash', mapping={'a':'one','b':'two'}) == 2
        assert cluster.execute_command('HGETDEL','{foo}getdel-hash','FIELDS',3,'a','a','missing') == [b'one',None,None]
        assert cluster.hgetall('{foo}getdel-hash') == {b'b':b'two'}
        assert cluster.hset('{foo}random-hash', mapping={'f':'v'}) == 1
        assert cluster.execute_command('HRANDFIELD','{foo}random-hash') == b'f'
        assert cluster.execute_command('HRANDFIELD','{foo}random-hash',-2,'WITHVALUES') == ([[b'f',b'v'],[b'f',b'v']] if options.protocol==3 else [b'f',b'v',b'f',b'v'])
        assert cluster.zadd('{foo}random-zset', {'m':1.5}) == 1
        assert cluster.execute_command('ZRANDMEMBER','{foo}random-zset') == b'm'
        assert cluster.execute_command('ZRANDMEMBER','{foo}random-zset',-2,'WITHSCORES') == ([[b'm',1.5],[b'm',1.5]] if options.protocol==3 else [b'm',b'1.5',b'm',b'1.5'])
        assert cluster.sadd('{foo}random-set','a','b','c') == 3
        assert set(cluster.execute_command('SRANDMEMBER','{foo}random-set',99)) == {b'a',b'b',b'c'}
        chosen = cluster.execute_command('SPOP','{foo}random-set',2)
        assert len(chosen)==2 and len(set(chosen))==2
        assert cluster.smembers('{foo}random-set') == {b'a',b'b',b'c'}-set(chosen)
        assert cluster.execute_command('SPOP','{foo}random-set') in {b'a',b'b',b'c'}-set(chosen)
        assert cluster.type('{foo}random-set') == b'none'
        assert cluster.hset('{foo}scan-h', mapping={'a':'1','b':'2'}) == 2
        assert dict(cluster.hscan_iter('{foo}scan-h', count=1)) == {b'a':b'1',b'b':b'2'}
        assert cluster.sadd('{foo}scan-s', 'a', 'b') == 2
        assert set(cluster.sscan_iter('{foo}scan-s', count=1)) == {b'a',b'b'}
        assert cluster.zadd('{foo}scan-z', {'a':1,'b':2}) == 2
        assert dict(cluster.zscan_iter('{foo}scan-z', count=1)) == {b'a':1.0,b'b':2.0}
        assert {b'{foo}scan-h',b'{foo}scan-s',b'{foo}scan-z'} <= set(cluster.scan_iter(match='{foo}scan-*', count=1))
        assert cluster.rpush('{foo}sort', '3', '1', '2') == 3
        assert cluster.execute_command('SORT_RO', '{foo}sort') == [b'1', b'2', b'3']
        assert cluster.execute_command('SORT', '{foo}sort', 'DESC', 'STORE', '{foo}sort-out') == 3
        assert cluster.lrange('{foo}sort-out', 0, -1) == [b'3', b'2', b'1']
        assert cluster.set('{foo}weight:1', '9')
        assert cluster.execute_command('SORT', '{foo}sort', 'BY', '{foo}weight:*') == [b'2', b'3', b'1']
        expect_error(a.cmd('SORT', '{foo}sort', 'STORE', '{bar}sort-out'), 'CROSSSLOT')
        expect_error(a.cmd('SORT', '{foo}sort', 'BY', 'weight:*'), 'ERR BY option')
        expect_error(a.cmd('SORT_RO', '{foo}sort', 'GET', '{bar}weight:*'), 'ERR GET option')
        # New string commands use COMMAND metadata for cluster-aware routing.
        assert cluster.set('{foo}lcs-a', b'ab')
        assert cluster.set('{foo}lcs-b', b'ba')
        assert cluster.execute_command('LCS', '{foo}lcs-a', '{foo}lcs-b') == b'b'
        assert cluster.execute_command('LCS', '{foo}lcs-a', '{foo}lcs-b', 'LEN') == 1
        expect_error(a.cmd('LCS', '{foo}lcs-a', '{bar}lcs-b'), 'CROSSSLOT')
        assert cluster.execute_command('SETNX', '{foo}string-ext', '12') == 1
        assert cluster.execute_command('APPEND', '{foo}string-ext', '3') == 3
        assert cluster.execute_command('STRLEN', '{foo}string-ext') == 3
        assert cluster.execute_command('DECR', '{foo}string-ext') == 122
        assert cluster.execute_command('DECRBY', '{foo}string-ext', 2) == 120
        assert cluster.execute_command('GETSET', '{foo}string-ext', 'abc') == b'120'
        assert cluster.execute_command('SETRANGE', '{foo}string-ext', 4, 'z') == 5
        assert cluster.execute_command('GETRANGE', '{foo}string-ext', 2, -1) == b'c\0z'
        assert cluster.execute_command('SUBSTR', '{foo}string-ext', 0, 1) == b'ab'
        assert cluster.execute_command('MSETNX', '{foo}nx-a', 'a', '{foo}nx-b', 'b') == 1
        assert cluster.execute_command('MSETNX', '{foo}nx-c', 'c', '{foo}nx-a', 'x') == 0
        assert cluster.get('{foo}nx-c') is None
        expect_error(a.cmd('MSETNX', '{bar}nx', 'x', '{foo}nx', 'y'), 'CROSSSLOT')
        assert cluster.execute_command('GETDEL', '{foo}string-ext') == b'abc\0z'
        assert cluster.get('{foo}string-ext') is None
        assert cluster.set('{foo}expiry-ext', 'v')
        assert cluster.execute_command('PEXPIREAT', '{foo}expiry-ext', 4102444800501, 'NX') == 1
        assert cluster.execute_command('PEXPIRETIME', '{foo}expiry-ext') == 4102444800501
        assert cluster.execute_command('EXPIRETIME', '{foo}expiry-ext') == 4102444801
        assert cluster.execute_command('EXPIREAT', '{foo}expiry-ext', 4102444800, 'XX', 'GT') == 0
        assert cluster.execute_command('EXPIRE', '{foo}expiry-ext', 0, 'XX', 'LT') == 1
        assert cluster.execute_command('SET', '{foo}getex-ext', 'v', 'PXAT', 4102444800501)
        assert cluster.execute_command('SET', '{foo}getex-ext', 'new', 'GET', 'KEEPTTL', get=True) == b'v'
        assert cluster.execute_command('GETEX', '{foo}getex-ext', 'PERSIST') == b'new'
        assert cluster.execute_command('PEXPIRETIME', '{foo}getex-ext') == -1
        assert cluster.execute_command('GETEX', '{foo}getex-ext', 'PXAT', 1) == b'new'
        assert cluster.get('{foo}getex-ext') is None
        assert cluster.execute_command('SETBIT', '{foo}bitmap', 0, 1) == 0
        assert cluster.execute_command('SETBIT', '{foo}bitmap', 8, 1) == 0
        assert cluster.execute_command('GETBIT', '{foo}bitmap', 0) == 1
        assert cluster.execute_command('BITPOS', '{foo}bitmap', 1, 1, 8, 'BIT') == 8
        assert cluster.execute_command('BITOP', 'NOT', '{foo}bitmap-not', '{foo}bitmap') == 2
        assert cluster.get('{foo}bitmap-not') == b'\x7f\x7f'
        assert cluster.execute_command('BITFIELD', '{foo}field', 'SET', 'i8', 0, 127, 'INCRBY', 'i8', 0, 1) == [0, -128]
        assert cluster.execute_command('BITFIELD_RO', '{foo}field', 'GET', 'i8', 0) == [-128]
        expect_error(a.cmd('BITOP', 'OR', '{foo}bitmap-not', '{foo}bitmap', '{bar}bitmap'), 'CROSSSLOT')
        expect_error(a.cmd('BITOP', 'NOT', '{bar}bitmap-not', '{foo}bitmap'), 'CROSSSLOT')
        assert cluster.execute_command('BITCOUNT', '{foo}bitmap', 1, 8, 'BIT') == 1
        assert cluster.rpush('{foo}list-position', 'a', 'b', 'a') == 3
        assert cluster.execute_command('LINSERT', '{foo}list-position', 'AFTER', 'a', 'c') == 4
        assert cluster.execute_command('LPOS', '{foo}list-position', 'a', 'COUNT', 0) == [0, 3]
        assert cluster.execute_command('LMOVE', '{foo}list-position', '{foo}list-moved', 'RIGHT', 'LEFT') == b'a'
        assert cluster.execute_command('RPOPLPUSH', '{foo}list-moved', '{foo}list-position') == b'a'
        expect_error(a.cmd('LMOVE', '{foo}list-position', '{bar}list-moved', 'LEFT', 'RIGHT'), 'CROSSSLOT')
        expect_error(a.cmd('RPOPLPUSH', '{foo}list-position', '{bar}list-moved'), 'CROSSSLOT')
        assert cluster.sadd('{foo}set-source', 'member') == 1
        assert cluster.execute_command('SMOVE', '{foo}set-source', '{foo}set-target', 'member') == 1
        assert cluster.sismember('{foo}set-target', 'member')
        expect_error(a.cmd('SMOVE', '{foo}set-target', '{bar}set-target', 'member'), 'CROSSSLOT')
        assert cluster.zadd('{foo}rank-score', {'a': 1.5, 'b': 1.5}) == 2
        assert cluster.execute_command('ZRANK', '{foo}rank-score', 'b', 'WITHSCORE') == [1, 1.5 if options.protocol==3 else b'1.5']
        assert cluster.execute_command('ZREVRANK', '{foo}rank-score', 'b', 'WITHSCORE') == [0, 1.5 if options.protocol==3 else b'1.5']
        assert cluster.sadd('{foo}card-a', 'a', 'b', 'c') == 3
        assert cluster.sadd('{foo}card-b', 'b', 'c') == 2
        assert cluster.execute_command('SINTERCARD', 2, '{foo}card-a', '{foo}card-b', 'LIMIT', 1) == 1
        assert a.cmd('COMMAND', 'GETKEYS', 'SINTERCARD', 2, '{foo}card-a', '{foo}card-b', 'LIMIT', 1) == [b'{foo}card-a', b'{foo}card-b']
        expect_error(a.cmd('SINTERCARD', 2, '{foo}card-a', '{bar}card-b'), 'CROSSSLOT')
        assert cluster.rpush('{foo}mpop', 'a', 'b', 'c') == 3
        assert cluster.execute_command('LMPOP', 2, '{foo}mpop-missing', '{foo}mpop', 'RIGHT', 'COUNT', 2) == [b'{foo}mpop', [b'c', b'b']]
        assert a.cmd('COMMAND', 'GETKEYS', 'LMPOP', 2, '{foo}mpop-missing', '{foo}mpop', 'RIGHT', 'COUNT', 2) == [b'{foo}mpop-missing', b'{foo}mpop']
        expect_error(a.cmd('LMPOP', 2, '{foo}mpop', '{bar}mpop', 'LEFT'), 'CROSSSLOT')
        assert cluster.zadd('{foo}zmpop', {'a': 1, 'b': 2}) == 2
        popped = cluster.execute_command('ZMPOP', 2, '{foo}zmpop-missing', '{foo}zmpop', 'MAX', 'COUNT', 1)
        assert popped[0] == b'{foo}zmpop' and popped[1][0][0] == b'b' and float(popped[1][0][1]) == 2
        assert a.cmd('COMMAND', 'GETKEYS', 'ZMPOP', 2, '{foo}zmpop-missing', '{foo}zmpop', 'MAX') == [b'{foo}zmpop-missing', b'{foo}zmpop']
        expect_error(a.cmd('ZMPOP', 2, '{foo}zmpop', '{bar}zmpop', 'MIN'), 'CROSSSLOT')
        assert cluster.zadd('{foo}range-source', {'a': 0, 'b': 0, 'c': 0}) == 3
        assert cluster.execute_command('ZRANGESTORE', '{foo}range-dest', '{foo}range-source', '[a', '(c', 'BYLEX') == 2
        assert cluster.zrange('{foo}range-dest', 0, -1) == [b'a', b'b']
        expect_error(a.cmd('ZRANGESTORE', '{bar}range-dest', '{foo}range-source', 0, -1), 'CROSSSLOT')
        assert cluster.execute_command('ZLEXCOUNT', '{foo}range-source', '[a', '(c') == 2
        assert cluster.execute_command('ZRANGEBYLEX', '{foo}range-source', '[a', '(c') == [b'a', b'b']
        assert cluster.execute_command('ZREVRANGEBYLEX', '{foo}range-source', '[c', '[b') == [b'c', b'b']
        assert cluster.execute_command('ZREMRANGEBYLEX', '{foo}range-source', '[a', '[a') == 1
        assert cluster.zadd('{foo}diff-a', {'a': 1, 'b': 2}) == 2
        assert cluster.sadd('{foo}diff-b', 'b') == 1
        assert cluster.execute_command('ZDIFF', 2, '{foo}diff-a', '{foo}diff-b') == [b'a']
        assert cluster.execute_command('ZDIFFSTORE', '{foo}diff-target', 2, '{foo}diff-a', '{foo}diff-b') == 1
        assert a.cmd('COMMAND', 'GETKEYS', 'ZDIFFSTORE', '{foo}diff-target', 2, '{foo}diff-a', '{foo}diff-b') == [b'{foo}diff-target', b'{foo}diff-a', b'{foo}diff-b']
        expect_error(a.cmd('ZDIFFSTORE', '{bar}diff-target', 1, '{foo}diff-a'), 'CROSSSLOT')
        expect_error(a.cmd('ZDIFF', 2, '{foo}diff-a', '{bar}diff-b'), 'CROSSSLOT')
        assert cluster.execute_command('ZUNION', 2, '{foo}diff-a', '{foo}diff-b') == [b'a', b'b']
        assert cluster.execute_command('ZINTER', 2, '{foo}diff-a', '{foo}diff-b') == [b'b']
        assert cluster.execute_command('ZINTERCARD', 2, '{foo}diff-a', '{foo}diff-b', 'LIMIT', 1) == 1
        assert cluster.execute_command('ZUNIONSTORE', '{foo}union-target', 2, '{foo}diff-a', '{foo}diff-b') == 2
        assert cluster.execute_command('ZINTERSTORE', '{foo}inter-target', 2, '{foo}diff-a', '{foo}diff-b') == 1
        for command in ['ZUNION', 'ZINTER', 'ZINTERCARD']:
            expect_error(a.cmd(command, 2, '{foo}diff-a', '{bar}diff-b'), 'CROSSSLOT')
        for command in ['ZUNIONSTORE', 'ZINTERSTORE']:
            expect_error(a.cmd(command, '{bar}store-target', 1, '{foo}diff-a'), 'CROSSSLOT')
        assert cluster.hset('{foo}rename-src', 'f', 'original') == 1
        assert cluster.execute_command('COPY', '{foo}rename-src', '{foo}rename-copy') == 1
        assert cluster.execute_command('RENAME', '{foo}rename-src', '{foo}rename-dst')
        assert cluster.execute_command('RENAMENX', '{foo}rename-dst', '{foo}rename-copy') == 0
        assert cluster.hset('{foo}rename-dst', 'f', 'changed') == 0
        assert cluster.hget('{foo}rename-copy', 'f') == b'original'
        for command in ['RENAME', 'RENAMENX', 'COPY']:
            expect_error(a.cmd(command, '{foo}rename-dst', '{bar}rename-dst'), 'CROSSSLOT')
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
        assert cluster.execute_command("HPEXPIREAT","{bar}hash",4102444700123,"FIELDS",1,b"f\x00") == [1]
        expect_error(a.cmd("HPEXPIRETIME","{foo}hash","FIELDS",1,"field"), "MOVED")
        assert cluster.execute_command("HSETEX","{bar}hash","FXX","KEEPTTL","FIELDS",1,b"f\x00",b"v\xff") == 1
        assert cluster.execute_command("HGETEX","{bar}hash","FIELDS",1,b"f\x00") == [b"v\xff"]
        expect_error(a.cmd("HGETEX","{foo}hash","FIELDS",1,"field"), "MOVED")
        assert cluster.set("{bar}unlink", "v")
        expect_error(a.cmd("UNLINK", "{foo}unlink"), "MOVED")
        expect_error(a.cmd("UNLINK", "{bar}unlink", "{foo}unlink"), "CROSSSLOT")
        assert cluster.unlink("{bar}unlink", "{bar}unlink") == 1
        assert cluster.exists("{bar}unlink") == 0
        expect_error(a.cmd("DUMP", "{foo}dump"), "MOVED")
        dumped_hash = cluster.dump("{bar}hash")
        assert dumped_hash[-10:-8] == b"\x0c\x00"
        expect_error(a.cmd("RESTORE", "{foo}restore", 0, dumped_hash), "MOVED")
        assert cluster.restore("{bar}hash", 0, dumped_hash, replace=True) == b"OK"
        assert a.cmd("EXISTS", *(["{bar}hash"] * 2048)) == 2048
        expect_error(a.cmd("EXISTS", *(["{bar}hash"] * 2048 + ["{foo}hash"])), "CROSSSLOT")
        assert cluster.rpush("{bar}list", "a", "b", "c") == 3
        assert cluster.sadd("{bar}set", "a", "b") == 2
        assert cluster.zadd("{bar}zset", {"a": 2, "b": 1}) == 2
        assert cluster.execute_command('GEOADD','{bar}geo',13.361389,38.115556,'Palermo') == 1
        geo_hash = cluster.execute_command('GEOHASH','{bar}geo','Palermo')
        assert geo_hash == [b'sqc8b49rny0'], geo_hash
        expect_error(a.cmd('GEOPOS','{foo}geo','Palermo'), 'MOVED')
        assert cluster.execute_command('GEOSEARCHSTORE','{bar}geo-copy','{bar}geo','FROMMEMBER','Palermo','BYRADIUS',10,'km') == 1
        expect_error(a.cmd('GEORADIUSBYMEMBER','{bar}geo','Palermo',10,'km','STORE','{foo}geo-radius'), 'CROSSSLOT')
        assert cluster.execute_command('GEORADIUSBYMEMBER','{bar}geo','Palermo',10,'km','STORE','{foo}ignored','STORE','{bar}geo-radius') == 1
        expect_error(a.cmd('GEORADIUSBYMEMBER_RO','{foo}geo','Palermo',10,'km'), 'MOVED')
        expect_error(a.cmd('GEOSEARCHSTORE','{foo}geo','{bar}geo','FROMMEMBER','Palermo','BYRADIUS',10,'km'), 'CROSSSLOT')
        assert cluster.pfadd('{bar}hll','a','b','c') == 1
        assert cluster.pfcount('{bar}hll') == 3
        assert cluster.pfmerge('{bar}hll-union','{bar}hll')
        assert cluster.pfcount('{bar}hll','{bar}hll-union') == 3
        expect_error(a.cmd('PFCOUNT','{bar}hll','{foo}hll'), 'CROSSSLOT')
        expect_error(a.cmd('PFMERGE','{bar}hll','{foo}hll'), 'CROSSSLOT')
        expect_error(a.cmd('PFADD','{foo}hll','v'), 'MOVED')
        expect_error(a.cmd('PFDEBUG','ENCODING','{foo}hll'), 'MOVED')
        assert cluster.execute_command('PFDEBUG','TODENSE','{bar}hll') == 1
        assert cluster.execute_command('PFDEBUG','ENCODING','{bar}hll') == b'dense'

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
        client_id=c.cmd('CLIENT','ID')
        c.socket.sendall(frame('ASKING')+frame('RESET')+frame('GET','{bar}missing'))
        assert c.read()==b'OK' and c.read()==b'RESET'
        expect_error(c.read(),'MOVED 5061 ')
        assert c.cmd('CLIENT','ID')==client_id
        seconds,micros=map(int,c.cmd('TIME'))
        assert seconds>0 and 0<=micros<1000000
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
        cluster = RedisCluster(host="127.0.0.1", port=source.port, socket_timeout=10, protocol=options.protocol)
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
        assert cluster.execute_command("HPEXPIRETIME","{bar}hash","FIELDS",1,b"f\x00") == [4102444700123]
        assert cluster.lrange("{bar}list", 0, -1) == [b"a", b"b", b"c"]
        assert cluster.smembers("{bar}set") == {b"a", b"b"}
        assert cluster.pfcount('{bar}hll','{bar}hll-union') == 3
        assert cluster.type('{bar}hll') == b'string'
        geo_hash = cluster.execute_command('GEOHASH','{bar}geo','Palermo')
        assert geo_hash == [b'sqc8b49rny0'], geo_hash
        assert cluster.type('{bar}geo') == b'zset'
        assert cluster.execute_command('GEORADIUSBYMEMBER_RO','{bar}geo-radius','Palermo',10,'km') == [b'Palermo']
        assert cluster.execute_command('GEOSEARCH','{bar}geo-copy','FROMMEMBER','Palermo','BYBOX',10,10,'km') == [b'Palermo']
        assert cluster.execute_command('PFDEBUG','ENCODING','{bar}hll') == b'dense'
        assert [tuple(pair) for pair in cluster.zrange("{bar}zset", 0, -1, withscores=True)] == [(b"b", 1.0), (b"a", 2.0)]
        assert cluster.get("foo") == b"binary\x00\xff"
        assert cluster.get_node_from_key("bar").port == target.port
        # Exercise the operator itself, including idempotent completion, moving
        # the slot back while this client's cached topology still points at C.
        from reshard import move_slot
        assert move_slot(nodes, target.id, source.id, slot) >= 4
        assert move_slot(nodes, target.id, source.id, slot) == 0
        assert cluster.get("bar") == b"after-move"
        assert cluster.get("{bar}a") == b"one"
        if options.native_redis:
            native_checks=check_native_cluster_transactions(shards,work,options.native_redis.resolve(),options.protocol)
            print(f"PASS: {native_checks} native Redis cluster RESP{options.protocol} transaction comparisons")
        print(f"PASS: RESP{options.protocol}, redis-py {redis.__version__}, three PostgreSQL shards, discovery, MOVED/ASK/ASKING, CROSSSLOT, pipeline, TTL migration, interrupted transfers, durable topology and crash recovery ({'journal' if options.journal else 'direct'})")
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
