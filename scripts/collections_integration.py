#!/usr/bin/env python3
"""Redis differential tests plus PostgreSQL typed-value upgrade and recovery.

Requires redis-py 6.4.0, PostgreSQL 17 and an existing redis-server executable.
Starts only disposable loopback services; never uses existing Redis/PG databases.
"""
import argparse
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import tempfile
import time
import redis
from cluster_integration import Shard, eventually, ROOT
from integration import free_port


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--journal', action='store_true')
    parser.add_argument('--redis-server', type=Path, default=ROOT/'.local/redis-8.2.11/src/redis-server')
    args=parser.parse_args()
    assert args.redis_server.is_file(), 'provide --redis-server /path/to/redis-server'
    bindir=Path(subprocess.check_output([os.environ.get('PGRX_PG_CONFIG_PATH','pg_config'),'--bindir'],text=True).strip())
    artifact=ROOT/'target'/os.environ.get('PG_CACHE_PROFILE','debug')/('libpg_cache.dylib' if os.uname().sysname=='Darwin' else 'libpg_cache.so')
    work=Path(tempfile.mkdtemp(prefix='collections-integration-',dir=ROOT/'.local'))
    (work/'lib').mkdir();shutil.copy2(artifact,work/'lib'/('pg_cache'+artifact.suffix))
    shard=None; process=None; actual=None; reference=None; compaction_lock=None
    checks=0
    try:
        node={'id':'1'*40,'host':'127.0.0.1','port':free_port(),'slots':[[0,16383]]}
        shard=Shard(work,0,[node],bindir,args.journal,legacy=True,cluster_mode=False)
        actual=redis.Redis(port=shard.port,socket_timeout=10)
        assert actual.get('legacy')==b'COL1\0\xff' and actual.type('legacy')==b'string'
        refport=free_port()
        with (work/'redis.log').open('w') as log:
            process=subprocess.Popen([str(args.redis_server.resolve()),'--bind','127.0.0.1','--port',str(refport),'--save','','--appendonly','no','--dir',str(work)],stdout=log,stderr=subprocess.STDOUT)
        reference=redis.Redis(port=refport,socket_timeout=10)
        eventually(reference.ping)
        if args.journal:
            compaction_lock=subprocess.Popen([str(bindir/'psql'),'-h','127.0.0.1','-p',str(shard.pgport),'-d','postgres','-XAt','-v','ON_ERROR_STOP=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            compaction_lock.stdin.write("SELECT pg_advisory_lock(62400001); SELECT 'locked';\n");compaction_lock.stdin.flush()
            while compaction_lock.stdout.readline().strip()!='locked': assert compaction_lock.poll() is None
        unordered={'HKEYS','HVALS'}
        def result(client,command):
            try:
                value=client.execute_command(*command)
                if command[0] in unordered: value=sorted(value)
                return ('ok',value)
            except redis.ResponseError as error:
                # Syntax wording varies; preserve the important WRONGTYPE class.
                return ('error','WRONGTYPE' if str(error).startswith('WRONGTYPE') else 'ERR')
        def compare(*command):
            nonlocal checks
            a,b=result(actual,command),result(reference,command)
            assert a==b,(command,a,b)
            checks+=1
            return a
        cases=[
            ('HGET','h','missing'),('HSET','h',b'f\0',b'v\xff','a','1','a','2'),('HGETALL','h'),('HMGET','h','a','absent',b'f\0'),('HSETNX','h','a','3'),('HSETNX','h','new','4'),('HINCRBY','h','a',-3),('HSTRLEN','h',b'f\0'),('HKEYS','h'),('HVALS','h'),('HEXISTS','h','a'),('HLEN','h'),('HDEL','h','new','new'),('HMSET','h','legacy','value'),
            ('LPUSH','l','a','b','c'),('LRANGE','l',0,-1),('LINDEX','l',-1),('LSET','l',1,'changed'),('LREM','l',-2,'a'),('RPUSHX','l','a','a'),('LREM','l',0,'a'),('LRANGE','l',-20,20),('LPOP','l',0),('RPOP','l',2),('LPOP','l'),('LPOP','l',0),('LPUSHX','l','missing'),('RPUSH','l','a','b','c'),('LTRIM','l',1,-1),('LLEN','l'),('LSET','l',99,'x'),
            ('SADD','s','a','b','a'),('SISMEMBER','s','a'),('SMISMEMBER','s','a','missing'),('SCARD','s'),('SMEMBERS','s'),('SADD','s2','b','c'),('SUNION','s','s2'),('SINTER','s','s2'),('SDIFF','s','s2'),('SINTERSTORE','s','s','s2'),('SUNIONSTORE','dst','s','s2'),('SDIFFSTORE','dst','dst','s'),('SMEMBERS','dst'),('SREM','s','b','b'),('TYPE','s'),
            ('ZADD','z',2,'b',1,'x',2,'a'),('ZRANGE','z',0,-1,'WITHSCORES'),('ZREVRANGE','z',0,-1),('ZRANK','z','a'),('ZREVRANK','z','a'),('ZSCORE','z','a'),('ZMSCORE','z','a','absent'),('ZCARD','z'),('ZADD','z','NX','CH',3,'a'),('ZADD','z','XX','GT','CH',4,'a'),('ZADD','z','INCR',1,'a'),('ZINCRBY','z',-2,'a'),('ZCOUNT','z','(1','+inf'),('ZRANGE','z',1,4,'BYSCORE','LIMIT',0,2,'WITHSCORES'),('ZREVRANGEBYSCORE','z','+inf','-inf','LIMIT',0,2),('ZRANGE','z','+inf',1,'BYSCORE','REV','WITHSCORES'),('ZADD','z',1,'dup',2,'dup'),('ZADD','z','CH',3,'dup',4,'dup'),('ZADD','z',5,'new','nan','bad'),('ZSCORE','z','new'),('ZPOPMIN','z',2),('ZPOPMAX','z',0),('ZREM','z','missing'),('ZREMRANGEBYRANK','z',0,0),('ZREMRANGEBYSCORE','z','-inf','+inf'),('TYPE','z'),
        ]
        for command in cases: compare(*command)
        for command in [('HSET','h','f','v'),('RPUSH','l','x'),('SADD','s','x'),('ZADD','z',1,'x')]: compare(*command)
        for key in ('h','l','s','z'):
            compare('GET',key);compare('MGET',key,'absent');compare('INCR',key)
            for command in [('HGET',key,'f'),('LLEN',key),('SCARD',key),('ZCARD',key)]: compare(*command)
        for numeric in ['1e999','1e-999','NaN','nan(payload)','Infinity','+Infinity','-Infinity','0x1.8p+1','0x10','-0','+0','01','0.1','1e-320',' 1','1 ','']:
            compare('ZADD','numeric',numeric,'member')
            compare('ZSCORE','numeric','member')
        # Seeded stateful differential operations expose index/order drift.
        rng=random.Random(81427)
        for i in range(1200):
            member=rng.choice(['a','b','c','d',b'\0\xff']);number=rng.randint(-5,5)
            command=rng.choice([
                ('HSET','rh',member,str(number)),('HDEL','rh',member),('HINCRBY','rh',member,number),('HGETALL','rh'),
                ('LPUSH','rl',member),('RPUSH','rl',member),('LPOP','rl'),('RPOP','rl',rng.randint(0,3)),('LREM','rl',number,member),('LRANGE','rl',number,rng.randint(-5,5)),
                ('SADD','rs',member),('SREM','rs',member),('SMEMBERS','rs'),
                ('ZADD','rz',number,member),('ZADD','rz','XX','GT','CH',number,member),('ZINCRBY','rz',number,member),('ZREM','rz',member),('ZRANGE','rz',0,-1,'WITHSCORES'),('ZPOPMIN','rz',rng.randint(0,2)),('ZREVRANK','rz',member),
            ])
            compare(*command)
        # Known snapshots survive both a journal overlay and later compaction.
        for command in [('HSET','keep-h',b'\xff',b'\0'),('RPUSH','keep-l','a','b'),('SADD','keep-s','a','b'),('ZADD','keep-z','-inf','a','0.5','b','+inf','c')]: compare(*command)
        for key in ('keep-h','keep-l','keep-s','keep-z'):
            compare('PEXPIRE',key,120000)
            assert 0<actual.pttl(key)<=120000
        checks_to_repeat=[('HGETALL','keep-h'),('LRANGE','keep-l',0,-1),('SMEMBERS','keep-s'),('ZRANGE','keep-z',0,-1,'WITHSCORES'),('HGETALL','rh'),('LRANGE','rl',0,-1),('SMEMBERS','rs'),('ZRANGE','rz',0,-1,'WITHSCORES')]
        expected=[result(actual,cmd) for cmd in checks_to_repeat]
        eventually(shard.committed)
        if args.journal:
            assert shard.sql('SELECT materialized<committed FROM cache_journal.state')=='t'
            assert shard.sql("SELECT count(*) FROM cache_journal.entries WHERE key=convert_to('keep-h','UTF8')")=='0'
        actual.close();shard.crash_restart();actual=redis.Redis(port=shard.port,socket_timeout=10)
        for cmd,want in zip(checks_to_repeat,expected): assert result(actual,cmd)==want,(cmd,want,result(actual,cmd))
        if compaction_lock:
            compaction_lock.communicate("\\q\n",timeout=5);compaction_lock=None
        if args.journal:
            eventually(lambda: shard.sql('SELECT materialized=committed FROM cache_journal.state')=='t')
            actual.close();shard.crash_restart();actual=redis.Redis(port=shard.port,socket_timeout=10)
            for cmd,want in zip(checks_to_repeat,expected): assert result(actual,cmd)==want
        for key in ('keep-h','keep-l','keep-s','keep-z'): assert actual.pttl(key)>0
        assert actual.get('legacy')==b'COL1\0\xff'
        # Last-element deletion must persist, including across changing key types.
        for key,command in [('keep-h',('HDEL','keep-h',b'\xff')),('keep-l',('LTRIM','keep-l',1,0)),('keep-s',('SREM','keep-s','a','b')),('keep-z',('ZREMRANGEBYSCORE','keep-z','-inf','+inf'))]:
            actual.execute_command(*command);assert actual.type(key)==b'none'
            actual.set(key,b'COL1\x00string')
        eventually(shard.committed);actual.close();shard.crash_restart();actual=redis.Redis(port=shard.port,socket_timeout=10)
        for key in ('keep-h','keep-l','keep-s','keep-z'): assert actual.get(key)==b'COL1\x00string'
        print(f'PASS: {checks} Redis differential checks, four typed values, legacy upgrade, TTLs, PostgreSQL persistence and crash recovery ({"journal" if args.journal else "direct"})')
    except Exception:
        if shard: print(shard.log.read_text()[-4000:])
        raise
    finally:
        if compaction_lock: compaction_lock.communicate("\\q\n",timeout=5)
        if actual: actual.close()
        if reference: reference.close()
        if process:
            process.terminate();process.wait(timeout=10)
        if shard: shard.stop()
        print(f'Isolated test data and logs: {work}')


if __name__=='__main__': main()
