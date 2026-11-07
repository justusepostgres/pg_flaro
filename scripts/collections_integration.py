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
from watch_compatibility import check_watch
from hyperloglog_compatibility import check_hyperloglog
from geo_compatibility import check_geo
from hash_storage_compatibility import check_hash_storage
from hash_expiry_compatibility import check_hash_expiry
from generic_compatibility import check_generic
from dump_compatibility import check_dump
from restore_compatibility import check_restore, check_legacy_restore
from discovery_compatibility import check_discovery
from blocking_compatibility import check_blocking, check_blocking_family, check_unblock
from transaction_compatibility import check_transactions
from sort_compatibility import check_sort
from protocol_compatibility import check_wide_commands, check_inline, check_resp_headers, check_client_metadata, check_hello


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--journal', action='store_true')
    parser.add_argument('--protocol', type=int, choices=[2,3], default=2)
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
        actual=redis.Redis(port=shard.port,socket_timeout=10,protocol=args.protocol)
        assert actual.get('legacy')==b'COL1\0\xff' and actual.type('legacy')==b'string'
        refport=free_port()
        with (work/'redis.log').open('w') as log:
            process=subprocess.Popen([str(args.redis_server.resolve()),'--bind','127.0.0.1','--port',str(refport),'--save','','--appendonly','no','--dir',str(work)],stdout=log,stderr=subprocess.STDOUT)
        reference=redis.Redis(port=refport,socket_timeout=10,protocol=args.protocol)
        eventually(reference.ping)
        checks += check_inline(shard.port, refport)
        checks += check_resp_headers(shard.port, refport)
        checks += check_client_metadata(shard.port, refport)
        checks += check_hello(shard.port, refport)
        named=redis.Redis(port=shard.port,client_name='redis-py-startup',max_connections=1,protocol=args.protocol)
        try:
            assert named.client_getname()==(b'redis-py-startup' if args.protocol==3 else 'redis-py-startup')
            assert named.client_id()>0
        finally:
            named.close()
        if args.journal:
            compaction_lock=subprocess.Popen([str(bindir/'psql'),'-h','127.0.0.1','-p',str(shard.pgport),'-d','postgres','-XAt','-v','ON_ERROR_STOP=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            compaction_lock.stdin.write("SELECT pg_advisory_lock(62400001); SELECT 'locked';\n");compaction_lock.stdin.flush()
            while compaction_lock.stdout.readline().strip()!='locked': assert compaction_lock.poll() is None
        checks += check_sort(actual, reference)
        checks += check_transactions(actual, reference, args.protocol)
        checks += check_watch(actual, reference)
        checks += check_blocking(actual, reference, args.protocol)
        checks += check_blocking_family(actual, reference, args.protocol)
        checks += check_unblock(actual, reference, args.protocol)
        checks += check_discovery(actual, reference, args.protocol)
        checks += check_hyperloglog(actual, reference)
        checks += check_geo(actual, reference)
        checks += check_hash_expiry(actual, reference)
        checks += check_generic(actual, reference)
        checks += check_dump(actual, reference)
        checks += check_restore(actual, reference)
        checks += check_legacy_restore(actual, reference)
        checks += check_wide_commands(actual, reference)
        unordered={'HKEYS','HVALS','KEYS'}
        def result(client,command):
            try:
                value=client.execute_command(*command, **({"get": True} if command[0] == "SET" else {}))
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
        # The extension has one legacy fixture absent from the reference server.
        baseline_size = actual.dbsize()-reference.dbsize()
        for command in [('DBSIZE','extra'),('KEYS',),('KEYS','*','extra'),('TOUCH',)]:
            compare(*command)
        for command in [('SET','enumerate:a','value'),('HSET','enumerate:h','f','v'),
                        ('SET',b'enumerate:a\x00b','binary'),('SET',b'enumerate:\xff','v')]:
            compare(*command)
        assert actual.dbsize()-reference.dbsize()==baseline_size
        checks+=1
        compare('TOUCH','enumerate:a','enumerate:a','enumerate:h','enumerate:missing')
        compare('PEXPIREAT','enumerate:a',4102444800000)
        compare('TOUCH','enumerate:a')
        compare('PEXPIRETIME','enumerate:a')
        for pattern in [b'enumerate:*',b'enumerate:a?b',b'enumerate:[^a]*',b'enumerate:*\xff',b'no-keys']:
            compare('KEYS',pattern)
        compare('DEL','enumerate:a','enumerate:h')
        compare('TOUCH','enumerate:a','enumerate:h')
        compare('KEYS','enumerate:*')
        assert actual.dbsize()-reference.dbsize()==baseline_size
        checks+=1
        # Random outputs cannot be compared byte-for-byte; verify the same
        # contract independently on both servers, resetting before every trial.
        random_members={b'a',b'b',b'c',b'\x00\xff',b''}
        for name in ['SRANDMEMBER','SPOP']:
            for count in [None,0,1,2,5,8,9223372036854775807]+([-1,-8] if name=='SRANDMEMBER' else []):
                for client in (actual,reference):
                    client.delete('random-set')
                    client.sadd('random-set',*random_members)
                    client.pexpireat('random-set',4102444800000)
                    command=[name,'random-set']+([] if count is None else [count])
                    reply=client.execute_command(*command)
                    if count is None:
                        assert reply in random_members,(command,reply)
                        chosen=[reply]
                    else:
                        chosen=reply
                        assert len(chosen)==(abs(count) if count<0 else min(count,len(random_members))),(command,reply)
                        assert set(chosen)<=random_members
                        if count>=0: assert len(set(chosen))==len(chosen)
                    remaining=client.smembers('random-set')
                    expected=random_members-set(chosen) if name=='SPOP' else random_members
                    assert remaining==expected,(command,remaining,expected)
                    assert client.pexpiretime('random-set')==(4102444800000 if remaining else -2)
                checks+=1
            for bad in ['01','wat','-9223372036854775808','9223372036854775808']+(['-1'] if name=='SPOP' else []):
                compare(name,'random-set',bad)
                compare(name,'random-missing',bad)
                compare(name,'enumerate:a\x00b',bad)
            compare(name,'random-missing')
            compare(name,'random-missing',0)
            compare(name,'enumerate:a\x00b',0)
            compare(name,'random-set',1,'extra')
            compare(name)
        random_hash={b'a':b'value-a',b'b':b'\x00\xff',b'':b'',b'\xff':b'binary'}
        random_zset={b'a':1.5,b'b':-2.0,b'':0.0,b'\xff':float('inf')}
        for field,value in random_hash.items(): compare('HSET','random-hash',field,value)
        for member,score in random_zset.items(): compare('ZADD','random-zset',score,member)
        for name,key,option,values in [('HRANDFIELD','random-hash','WITHVALUES',random_hash),
                                       ('ZRANDMEMBER','random-zset','WITHSCORES',random_zset)]:
            compare('PEXPIREAT',key,4102444800000)
            for count in [None,0,1,2,4,9,-1,-9]:
                for paired in ([False] if count is None else [False,True]):
                    command=[name,key]+([] if count is None else [count])+([option] if paired else [])
                    for client in (actual,reference):
                        reply=client.execute_command(*command)
                        if count is None:
                            assert reply in values,(command,reply)
                        else:
                            members=([pair[0] for pair in reply] if args.protocol==3 else reply[::2]) if paired else reply
                            assert len(members)==(abs(count) if count<0 else min(count,len(values)))
                            assert set(members)<=set(values)
                            if count>=0: assert len(set(members))==len(members)
                            if paired:
                                assert len(reply)==(len(members) if args.protocol==3 else 2*len(members))
                                pairs=reply if args.protocol==3 else zip(members,reply[1::2])
                                for member,value in pairs:
                                    assert (float(value) if name=='ZRANDMEMBER' else value)==values[member]
                    checks+=1
            compare('PEXPIRETIME',key)
            compare('HGETALL' if name=='HRANDFIELD' else 'ZRANGE',key,*(() if name=='HRANDFIELD' else (0,-1,'WITHSCORES')))
            for bad in ['01','wat','-9223372036854775808','9223372036854775808']:
                for target in [key,'random-missing','enumerate:a\x00b']:
                    compare(name,target,bad)
            for target in [key,'random-missing','enumerate:a\x00b']:
                for flags in [(0,option),(1,'bad'),(1,option,'extra'),('4611686018427387904',option),('-4611686018427387904',option)]:
                    compare(name,target,*flags)
            compare(name,'random-missing')
            compare(name,'random-missing',1,option)
            compare(name)
        # Leave deterministic values for recovery assertions.
        compare('DEL','random-set')
        compare('SADD','random-set',b'only\x00member')
        compare('SRANDMEMBER','random-set')
        compare('SRANDMEMBER','random-set',-4)
        compare('SPOP','random-set')
        compare('TYPE','random-set')
        compare('SADD','random-recovery','a','b','c')
        actual.spop('random-recovery')
        reference.spop('random-recovery')
        # Keep raw decimal bulk replies: redis-py's float conversion would hide
        # formatting and precision differences in these commands.
        for client in (actual,reference):
            for name in ['INCRBYFLOAT','HINCRBYFLOAT']:
                client.set_response_callback(name,lambda response: response)
        float_values=['0','-0','1.25','0.1','9007199254740993','1e30','1e4000','1e-30',
                      '0x1.8p+1','inf','-inf','nan','bad',b'1\x00', '0'*5120]
        increments=['0','-0','0.1','-0.25','1e30','1e4000','1e-4000','0x1p-2','inf','nan',' 1','1 ']
        for initial in float_values:
            for increment in increments:
                compare('SET','float-string',initial)
                compare('HSET','float-hash','f',initial)
                compare('PEXPIREAT','float-string',4102444800000)
                compare('PEXPIREAT','float-hash',4102444800000)
                compare('INCRBYFLOAT','float-string',increment)
                compare('HINCRBYFLOAT','float-hash','f',increment)
                compare('GET','float-string')
                compare('HGET','float-hash','f')
                compare('PEXPIRETIME','float-string')
                compare('PEXPIRETIME','float-hash')
        for increment in ['nan','inf','bad','1']:
            compare('INCRBYFLOAT','float-hash',increment)
            compare('HINCRBYFLOAT','float-string','f',increment)
            compare('DEL','float-missing')
            compare('INCRBYFLOAT','float-missing',increment)
            compare('TYPE','float-missing')
            compare('DEL','float-missing')
            compare('HINCRBYFLOAT','float-missing','f',increment)
            compare('TYPE','float-missing')
        compare('SET','float-string','0')
        compare('HSET','float-hash','f','0')
        for _ in range(100):
            compare('INCRBYFLOAT','float-string','0.1')
            compare('HINCRBYFLOAT','float-hash','f','0.1')
        compare('GET','float-string')
        compare('HGET','float-hash','f')
        for fields in [('a',),('a','a'),('missing','a','missing','b'),(b'f\x00',b'f\x00','a','b')]:
            compare('DEL','getdel-hash')
            compare('HSET','getdel-hash','a','one','b','two',b'f\x00',b'v\xff')
            compare('PEXPIREAT','getdel-hash',4102444800000)
            compare('HGETDEL','getdel-hash','FIELDS',len(fields),*fields)
            compare('HGETALL','getdel-hash')
            compare('PEXPIRETIME','getdel-hash')
        for target in ['getdel-hash','getdel-missing','float-string']:
            for fields in [('FIELDS',0,'a'),('FIELDS',-1,'a'),('FIELDS','01','a'),
                           ('FIELDS','bad','a'),('FIELDS',2,'a'),('FIELDS',1,'a','b'),
                           ('bad',1,'a'),('FIELDS','9223372036854775808','a')]:
                compare('HGETDEL',target,*fields)
        compare('HGETDEL','getdel-missing','FIELDS',2,'a','b')
        compare('HGETDEL','getdel-hash','FIELDS',1)
        compare('DEL','getdel-hash')
        compare('HSET','getdel-hash','a','one','b','two')
        compare('PEXPIREAT','getdel-hash',4102444800000)
        compare('HGETDEL','getdel-hash','fields',2,'a','a')
        # Cursors/page boundaries are opaque: compare complete iteration results.
        def scan_all(client, name, key, options):
            cursor=0
            found=set()
            for _ in range(10000):
                command=[name] + ([key] if key is not None else []) + [cursor] + list(options)
                cursor,items=client.execute_command(*command,no_values='NOVALUES' in options)
                if isinstance(items,dict): items=items.items()
                found.update(items)
                if cursor==0: return found
            raise AssertionError(('scan did not terminate',name,key,options))
        def compare_scan(name,key,*options):
            nonlocal checks
            a,b=scan_all(actual,name,key,options),scan_all(reference,name,key,options)
            assert a==b,(name,key,options,a,b)
            checks+=1
        scan_members=[b'',b'a',b'ab',b'abc',b'axc',b'foo',b'a*b',b'a?b',b'[',b']',b'\\',
                      b'a\x00b',b'\x00',b'\xff',b'\x80',b'-',b'^',b'123']
        scan_members += [f'member:{n:03}'.encode() for n in range(80)]
        for n,member in enumerate(scan_members):
            compare('HSET','scan:hash',member,member+b'-value')
            compare('SADD','scan:set',member)
            compare('ZADD','scan:zset',n,member)
            compare('SET',b'scan:key:'+member,member)
        compare('SET','scan:expired','v','PXAT',1)
        patterns=[b'*',b'**',b'',b'?',b'a*',b'*b',b'*?*',b'a?c',b'[a-c]*',b'[^a-c]*',
                  b'[c-a]*',b'[',b'[]',b'[^]',b'[abc',b'a\\*b',b'a\\?b',b'\\',
                  b'[\\]]',b'[a-]',b'[-a]',b'[\x00-\xff]',b'*\x00*',b'*\xff*']
        for pattern in patterns:
            for name,key in [('HSCAN','scan:hash'),('SSCAN','scan:set'),('ZSCAN','scan:zset')]:
                compare_scan(name,key,'MATCH',pattern,'COUNT',7)
            compare_scan('HSCAN','scan:hash','MATCH',pattern,'COUNT',3,'NOVALUES')
        for kind in ['string','hash','set','zset','list','unknown','HASH']:
            compare_scan('SCAN',None,'MATCH','scan:*','COUNT',13,'TYPE',kind)
        compare_scan('SCAN',None,'MATCH','scan:*','COUNT',1,'COUNT',31)
        compare_scan('HSCAN','scan:hash','MATCH','no-match','MATCH','*','COUNT',9223372036854775807)
        for name,key in [('HSCAN','scan:hash'),('SSCAN','scan:set'),('ZSCAN','scan:zset')]:
            for options in [('COUNT',0),('COUNT',-1),('COUNT','01'),('TYPE','hash'),('MATCH',),('BOGUS',)]:
                compare(name,key,0,*options)
            for invalid in ['-1','18446744073709551616','wat','1 ','']:
                compare(name,key,invalid)
            for raw in ['00','+0',' 0','-0',b'0\x00junk']:
                # Different valid cursors are fine; a full iteration must still work.
                for client in (actual,reference):
                    client.execute_command(name,key,raw)
                checks+=1
            compare(name,'scan:missing',0,'BOGUS')
            compare(name,'scan:key:a',0,'BOGUS')
        compare('SCAN',0,'NOVALUES')
        compare('SSCAN','scan:set',0,'NOVALUES')
        compare('ZSCAN','scan:zset',0,'NOVALUES')
        # LCS replies are deterministic, including ambiguous subsequence ties.
        lcs_rng=random.Random(8127)
        lcs_pairs=[(b'',b''),(b'abc',b''),(b'ohmytext',b'mynewtext'),
                   (b'ab',b'ba'),(b'a\0\xffb',b'\xffa\0b'),(b'aaaa',b'aa')]
        lcs_pairs += [(bytes(lcs_rng.choice(b'abc\0\xff') for _ in range(lcs_rng.randrange(24))),
                       bytes(lcs_rng.choice(b'abc\0\xff') for _ in range(lcs_rng.randrange(24))))
                      for _ in range(100)]
        for left,right in lcs_pairs:
            compare('SET','lcs-a',left)
            compare('SET','lcs-b',right)
            compare('PEXPIREAT','lcs-a',4102444800000)
            for options in [(),('LEN',),('IDX',),('IDX','WITHMATCHLEN'),
                            ('IDX','MINMATCHLEN',2,'WITHMATCHLEN'),
                            ('IDX','MINMATCHLEN',-1),('IDX','MINMATCHLEN',100),
                            ('MINMATCHLEN',2),('WITHMATCHLEN',),
                            ('IDX','IDX','MINMATCHLEN',4,'MINMATCHLEN',1)]:
                compare('LCS','lcs-a','lcs-b',*options)
            compare('PEXPIRETIME','lcs-a')
        for options in [('LEN','IDX'),('MINMATCHLEN',),('MINMATCHLEN','01'),
                        ('MINMATCHLEN','9223372036854775808'),('BOGUS',)]:
            compare('LCS','lcs-a','lcs-b',*options)
        compare('LCS','lcs-a')
        compare('LCS','lcs-a','lcs-a','IDX','WITHMATCHLEN')
        compare('LCS','lcs-missing','lcs-b','IDX')
        compare('HSET','lcs-wrong','f','v')
        compare('LCS','lcs-wrong','lcs-b','LEN','IDX')
        compare('LCS','lcs-a','lcs-wrong')
        cases=[
            ('GETDEL','new-string'),('GETSET','new-string',b'a\0\xff'),('GETSET','new-string','abcdef'),
            ('GETRANGE','new-string',0,-1),('SUBSTR','new-string',-2,-1),
            ('SETRANGE','new-string',9,'z'),('GET','new-string'),
            ('SETRANGE','new-string',2,'XY'),('GETDEL','new-string'),('GET','new-string'),
            ('SETRANGE','new-string',9223372036854775807,''),('EXISTS','new-string'),
            ('SETRANGE','new-string',9223372036854775807,'x'),('SETRANGE','new-string',-1,''),
            ('SETNX','new-string','first'),('SETNX','new-string','second'),('GET','new-string'),
            ('MSETNX','nx-a','first','nx-a','last','nx-b','b'),('MGET','nx-a','nx-b'),
            ('MSETNX','nx-c','c','nx-a','replacement'),('MGET','nx-a','nx-c'),
            ('HSET','string-wrong','f','v'),('GETSET','string-wrong','x'),('GETDEL','string-wrong'),
            ('SETNX','string-wrong','x'),('SETRANGE','string-wrong',0,''),
            ('GETRANGE','string-wrong',0,1),('GETRANGE','string-wrong','bad',1),

            ('STRLEN','string-ext'),('APPEND','string-ext',b''),('EXISTS','string-ext'),
            ('APPEND','string-ext',b'a\0\xff'),('STRLEN','string-ext'),('GET','string-ext'),
            ('SET','number-ext','-1'),('DECRBY','number-ext','-9223372036854775808'),
            ('DECRBY','number-ext',-1),('GET','number-ext'),('DECR','number-ext'),
            ('SET','number-ext','-9223372036854775808'),('DECR','number-ext'),
            ('DECRBY','number-ext','-9223372036854775808'),('GET','number-ext'),
            ('DECR','missing-number'),('DECRBY','missing-number','01'),
            ('HSET','wrong-ext','f','v'),('STRLEN','wrong-ext'),('APPEND','wrong-ext','x'),
            ('DECR','wrong-ext'),('DECRBY','wrong-ext',1),
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
        # Preserve SET replies rather than redis-py's default boolean coercion.
        for initial in ['missing','string','hash']:
            for flags in [[],['GET'],['GET','GET'],['NX'],['XX'],['NX','GET'],['XX','GET'],
                          ['NX','NX'],['NX','XX'],['KEEPTTL'],['KEEPTTL','GET'],
                          ['EXAT',4102444800],['PXAT',4102444800501,'GET'],
                          ['PXAT',1,'GET'],['EXAT',0],['EX','bad'],
                          ['EX','bad','EX',100],['PX',100,'PX',200],
                          ['EX',100,'PX',100],['KEEPTTL','PX',100],
                          ['PX',100,'KEEPTTL'],['EXAT',4102444800,'NX','GET']]:
                compare('DEL','set-options')
                if initial=='string':
                    compare('SET','set-options',b'old\0\xff')
                    compare('PEXPIREAT','set-options',4102444900000)
                elif initial=='hash': compare('HSET','set-options','f','v')
                compare('SET','set-options','new',*flags)
                compare('GET','set-options')
                if 'EX' not in flags and 'PX' not in flags: compare('PEXPIRETIME','set-options')
        for initial in ['missing','string','hash']:
            for flags in [[],['PERSIST'],['PERSIST','PERSIST'],['EXAT',4102444800],
                          ['PXAT',4102444800501],['EXAT',1],['PXAT',1],
                          ['EX',0],['PX','bad'],['EX',9223372036854775807],
                          ['EX','bad','EX',100],['NX'],['GET'],['KEEPTTL'],
                          ['PERSIST','EX',100],['PX',100,'PERSIST'],['EX',100,'PX',100]]:
                compare('DEL','getex-options')
                if initial=='string':
                    compare('SET','getex-options',b'old\0\xff')
                    compare('PEXPIREAT','getex-options',4102444900000)
                elif initial=='hash': compare('HSET','getex-options','f','v')
                compare('GETEX','getex-options',*flags)
                compare('GET','getex-options')
                if 'EX' not in flags and 'PX' not in flags: compare('PEXPIRETIME','getex-options')
        compare('SET','keep-getex','v','PXAT',4102444800123)
        compare('GETEX','keep-getex','PXAT',4102444800456)
        for kind in ['string','hash','list','set','zset']:
            source='rename-'+kind;target=source+'-target';duplicate=source+'-copy'
            create={'string':('SET',source,b'v\0\xff'),'hash':('HSET',source,'f','v'),
                    'list':('RPUSH',source,'v'),'set':('SADD',source,'v'),
                    'zset':('ZADD',source,1,'v')}[kind]
            compare('RENAME',source,target);compare('RENAMENX',source,source)
            compare(*create);compare('PEXPIREAT',source,4102444800000)
            compare('RENAME',source,source);compare('RENAMENX',source,source)
            compare('COPY',source,source);compare('COPY',source,duplicate,'DB',0)
            compare('COPY',source,duplicate);compare('COPY',source,duplicate,'REPLACE','REPLACE')
            compare('SET',target,'old');compare('RENAMENX',source,target)
            compare('RENAME',source,target);compare('TYPE',source);compare('TYPE',target)
            compare('PEXPIRETIME',target);compare('PEXPIRETIME',duplicate)
            compare('RENAMENX',target,source);compare('TYPE',source);compare('TYPE',target)
            # Mutating one copy must not affect the other, including fast paths.
            mutation={'string':('APPEND',source,'changed'),'hash':('HSET',source,'f','changed'),
                      'list':('LSET',source,0,'changed'),'set':('SADD',source,'changed'),
                      'zset':('ZINCRBY',source,1,'v')}[kind]
            compare(*mutation)
            reads={'string':('GET',),'hash':('HGETALL',),'list':('LRANGE',),'set':('SMEMBERS',),'zset':('ZRANGE',)}
            for key in [source,duplicate]:
                tail=(0,-1) if kind in ['list','zset'] else ()
                compare(reads[kind][0],key,*tail)
        compare('COPY','absent-copy','absent-copy')
        compare('COPY','absent-copy','target-copy','DB','bad')
        compare('SET','bitmap',b'\x80\xff\x00\x01\x81')
        compare('PEXPIREAT','bitmap',4102444800000)
        for offset in [0,1,7,8,15,16,31,32,39,40,100,4095]:
            compare('GETBIT','bitmap',offset)
            compare('SETBIT','bitmap',offset,1)
            compare('SETBIT','bitmap',offset,1)
            compare('SETBIT','bitmap',offset,0)
            compare('GET','bitmap');compare('PEXPIRETIME','bitmap')
        for key in ['bitmap','missing-bitmap']:
            compare('BITCOUNT',key)
            for unit in ['BIT','BYTE']:
                for start in [-9223372036854775808,-100,-9,-1,0,1,7,8,15,100,9223372036854775807]:
                    for end in [-9223372036854775808,-100,-9,-1,0,1,7,8,15,100,9223372036854775807]:
                        compare('BITCOUNT',key,start,end,unit)
        compare('HSET','bitmap-wrong','f','v')
        for command in [('GETBIT','bitmap-wrong',0),('SETBIT','bitmap-wrong',0,1),
                        ('BITCOUNT','bitmap-wrong'),('GETBIT','bitmap',-1),
                        ('GETBIT','bitmap',4294967296),('GETBIT','bitmap',4294967295),
                        ('SETBIT','bitmap',0,2),('SETBIT','bitmap','01',1),
                        ('BITCOUNT','bitmap',0),('BITCOUNT','bitmap',0,1,'bad'),
                        ('SETBIT','bitmap-empty',8,0),('GET','bitmap-empty')]:
            compare(*command)
        for value in [None,b'',b'\xff',b'\0',b'\xff\x80\0\xff',b'\x01\x7f']:
            compare('DEL','bitpos')
            if value is not None: compare('SET','bitpos',value)
            for bit in [0,1]:
                compare('BITPOS','bitpos',bit)
                for start in [-9223372036854775808,-100,-2,-1,0,1,7,8,100,9223372036854775807]:
                    compare('BITPOS','bitpos',bit,start)
                    for end in [-100,-2,-1,0,1,7,8,100,9223372036854775807]:
                        for unit in ['BYTE','BIT']:
                            compare('BITPOS','bitpos',bit,start,end,unit)
        for command in [('BITPOS','bitmap-wrong',0),('BITPOS','bitmap',2),
                        ('BITPOS','bitmap','bad'),('BITPOS','bitmap',0,'bad'),
                        ('BITPOS','bitmap',0,0,'bad'),('BITPOS','bitmap',0,0,1,'bad')]:
            compare(*command)
        bitop_rng=random.Random(30117)
        for trial in range(40):
            for key in ['bitop-a','bitop-b','bitop-c']:
                value=bytes(bitop_rng.randrange(256) for _ in range(bitop_rng.choice([0,1,2,7,32,65])))
                compare('SET',key,value)
            for operation in ['AND','OR','XOR','NOT','DIFF','DIFF1','ANDOR','ONE']:
                sources=['bitop-a'] if operation=='NOT' else ['bitop-a','bitop-b','bitop-c','bitop-missing']
                compare('SET','bitop-target','old','PXAT',4102444800000)
                compare('BITOP',operation,'bitop-target',*sources)
                compare('GET','bitop-target');compare('PEXPIRETIME','bitop-target')
        for operation in ['AND','OR','XOR','NOT','DIFF','DIFF1','ANDOR','ONE']:
            compare('SET','bitop-alias',b'\xff\x80\0')
            sources=['bitop-alias'] if operation=='NOT' else ['bitop-alias','bitop-alias','bitop-b']
            compare('BITOP',operation,'bitop-alias',*sources);compare('GET','bitop-alias')
        for command in [('BITOP','bad','bitop-target','bitop-a'),
                        ('BITOP','NOT','bitop-target','bitop-a','bitop-b'),
                        ('BITOP','DIFF','bitop-target','bitop-a'),
                        ('BITOP','OR','bitop-target','bitmap-wrong'),
                        ('BITOP','AND','bitop-target','bitop-missing','bitmap-wrong'),
                        ('BITOP','OR','bitop-target','bitop-missing'),('GET','bitop-target')]:
            compare(*command)
        for width in ['i1','i8','i63','i64','u1','u8','u63']:
            for mode in ['WRAP','SAT','FAIL']:
                for number in [-9223372036854775808,-256,-129,-1,0,1,127,255,256,9223372036854775807]:
                    compare('SET','bitfield',b'\xff\x80\0')
                    compare('PEXPIREAT','bitfield',4102444800000)
                    compare('BITFIELD','bitfield','OVERFLOW',mode,'SET',width,3,number,
                            'GET',width,3,'INCRBY',width,3,number,'GET',width,3,
                            'INCRBY',width,'#2',number)
                    compare('GET','bitfield');compare('PEXPIRETIME','bitfield')
                    compare('BITFIELD_RO','bitfield','GET',width,3,'GET',width,'#2')
        for command in [('BITFIELD','missing-field'),('BITFIELD_RO','missing-field','OVERFLOW','FAIL'),
                        ('BITFIELD','failed-field','OVERFLOW','FAIL','SET','u1',8,2),('GET','failed-field'),
                        ('BITFIELD_RO','bitfield','SET','u8',0,1),
                        ('BITFIELD','bitmap-wrong','GET','u8',0),
                        ('BITFIELD','bitfield','SET','u8',0,1,'bad'),
                        ('BITFIELD','bitfield','GET','u64',0),('BITFIELD','bitfield','GET','i0',0),
                        ('BITFIELD','bitfield','GET','U8',0),('BITFIELD','bitfield','GET','u08',0),
                        ('BITFIELD','bitfield','GET','u8',-1),('BITFIELD','bitfield','GET','u8','#9223372036854775807')]:
            compare(*command)
        compare('RPUSH','list-position','a',b'b\0','a','c','a',b'b\0')
        compare('PEXPIREAT','list-position',4102444800000)
        for key in ['list-position','missing-position']:
            for member in ['a',b'b\0','missing']:
                compare('LPOS',key,member)
                for rank in [-100,-3,-1,1,2,100]:
                    for count in [0,1,2,100]:
                        for maxlen in [0,1,3,100]:
                            compare('LPOS',key,member,'RANK',rank,'COUNT',count,'MAXLEN',maxlen)
        for command in [('LPOS','list-position','a','RANK',0),
                        ('LPOS','list-position','a','RANK',-9223372036854775808),
                        ('LPOS','list-position','a','COUNT',-1),('LPOS','list-position','a','MAXLEN',-1),
                        ('LPOS','list-position','a','RANK',1,'RANK',-1),
                        ('LPOS','bitmap-wrong','a','RANK',0),('LPOS','bitmap-wrong','a'),
                        ('LINSERT','missing-position','BEFORE','a','v'),
                        ('LINSERT','list-position','BEFORE','missing','v'),
                        ('LINSERT','list-position','BEFORE','a','first'),
                        ('LINSERT','list-position','AFTER','a',b'after\0'),
                        ('LINSERT','list-position','bad','a','v'),
                        ('LINSERT','bitmap-wrong','bad','a','v'),
                        ('LINSERT','bitmap-wrong','AFTER','a','v'),
                        ('LRANGE','list-position',0,-1),('PEXPIRETIME','list-position')]:
            compare(*command)
        for source_kind in ['missing','string','list']:
            for target_kind in ['missing','string','list']:
                for source_side in ['LEFT','RIGHT']:
                    for target_side in ['LEFT','RIGHT']:
                        compare('DEL','move-source','move-target')
                        for key,kind in [('move-source',source_kind),('move-target',target_kind)]:
                            if kind=='string': compare('SET',key,'wrong')
                            elif kind=='list':
                                compare('RPUSH',key,b'a\0','b','c')
                                compare('PEXPIREAT',key,4102444800000 if key=='move-source' else 4102444900000)
                        compare('LMOVE','move-source','move-target',source_side,target_side)
                        compare('LRANGE','move-source',0,-1);compare('LRANGE','move-target',0,-1)
                        compare('PEXPIRETIME','move-source');compare('PEXPIRETIME','move-target')
        for source_side in ['LEFT','RIGHT']:
            for target_side in ['LEFT','RIGHT']:
                compare('DEL','rotate');compare('RPUSH','rotate','a','b','c')
                compare('LMOVE','rotate','rotate',source_side,target_side)
                compare('LRANGE','rotate',0,-1)
        compare('DEL','move-source','move-target');compare('RPUSH','move-source','only')
        compare('RPOPLPUSH','move-source','move-target');compare('TYPE','move-source')
        compare('RPOPLPUSH','move-target','move-target');compare('LRANGE','move-target',0,-1)
        compare('LMOVE','move-target','move-source','bad','LEFT')
        for source_kind in ['missing','string','set']:
            for target_kind in ['missing','string','set']:
                for member in [b'a\0','b','missing']:
                    compare('DEL','set-source','set-target')
                    for key,kind in [('set-source',source_kind),('set-target',target_kind)]:
                        if kind=='string': compare('SET',key,'wrong')
                        elif kind=='set':
                            compare('SADD',key,*([b'a\0','b'] if key=='set-source' else ['b','c']))
                            compare('PEXPIREAT',key,4102444800000 if key=='set-source' else 4102444900000)
                    compare('SMOVE','set-source','set-target',member)
                    compare('SMEMBERS','set-source');compare('SMEMBERS','set-target')
                    compare('PEXPIRETIME','set-source');compare('PEXPIRETIME','set-target')
        compare('DEL','set-source','set-target');compare('SADD','set-source','only')
        compare('SMOVE','set-source','set-source','only');compare('SMOVE','set-source','set-source','missing')
        compare('SMOVE','set-source','set-target','only');compare('TYPE','set-source')
        for key in ['rank-score','missing-rank']:
            if key=='rank-score': compare('ZADD',key,'-inf','a',0.1,'b',0.1,'c',1.5,'d','+inf','e')
            for member in ['a','b','c','d','e','absent']:
                for command in ['ZRANK','ZREVRANK']:
                    compare(command,key,member,'WITHSCORE')
        compare('ZRANK','bitmap-wrong','a','WITHSCORE')
        compare('ZRANK','bitmap-wrong','a','bad')
        compare('ZRANK','rank-score','a','WITHSCORE','WITHSCORE')
        compare('SADD','card-a','a','b','c','d');compare('SADD','card-b','b','c','d','e')
        for keys in [('card-a',),('card-a','card-b'),('card-a','card-a','card-b'),('card-a','card-missing')]:
            compare('SINTERCARD',len(keys),*keys)
            for limit in [0,1,2,3,100]: compare('SINTERCARD',len(keys),*keys,'LIMIT',limit)
        for command in [('SINTERCARD',2,'card-missing','bitmap-wrong'),
                        ('SINTERCARD',1,'bitmap-wrong'),('SINTERCARD',0,'card-a'),
                        ('SINTERCARD',100,'card-a'),('SINTERCARD','bad','card-a'),
                        ('SINTERCARD',1,'card-a','LIMIT',-1),('SINTERCARD',1,'card-a','LIMIT','bad'),
                        ('SINTERCARD',1,'card-a','LIMIT',1,'LIMIT',2),
                        ('SINTERCARD',1,'card-a','bad',2),('SINTERCARD',1,'card-a','LIMIT')]:
            compare(*command)
        for side in ['LEFT','RIGHT']:
            for count in [1,2,100]:
                compare('DEL','mpop-a','mpop-b')
                compare('RPUSH','mpop-a','a','b','c');compare('RPUSH','mpop-b','d','e')
                compare('PEXPIREAT','mpop-a',4102444800000)
                compare('LMPOP',3,'mpop-missing','mpop-a','mpop-b',side,'COUNT',count)
                compare('LRANGE','mpop-a',0,-1);compare('LRANGE','mpop-b',0,-1)
                compare('PEXPIRETIME','mpop-a')
        for command in [('LMPOP',2,'mpop-missing','mpop-missing','LEFT'),
                        ('LMPOP',1,'bitmap-wrong','LEFT'),('LMPOP',0,'mpop-a','LEFT'),
                        ('LMPOP',100,'mpop-a','LEFT'),('LMPOP',1,'mpop-b','bad'),
                        ('LMPOP',1,'mpop-b','LEFT','COUNT',0),('LMPOP',1,'mpop-b','LEFT','COUNT',-1),
                        ('LMPOP',1,'mpop-b','LEFT','COUNT',1,'COUNT',2),
                        ('LMPOP',2,'mpop-b','bitmap-wrong','LEFT'),('LRANGE','mpop-b',0,-1)]:
            compare(*command)
        for side in ['MIN','MAX']:
            for count in [1,2,100]:
                compare('DEL','zmpop-a','zmpop-b')
                compare('ZADD','zmpop-a','-inf','a',1.5,'b',1.5,'c','+inf','d')
                compare('ZADD','zmpop-b',1,'e');compare('PEXPIREAT','zmpop-a',4102444800000)
                compare('ZMPOP',3,'zmpop-missing','zmpop-a','zmpop-b',side,'COUNT',count)
                compare('ZRANGE','zmpop-a',0,-1,'WITHSCORES');compare('ZRANGE','zmpop-b',0,-1,'WITHSCORES')
                compare('PEXPIRETIME','zmpop-a')
        for command in [('ZMPOP',2,'zmpop-missing','zmpop-missing','MIN'),
                        ('ZMPOP',1,'bitmap-wrong','MIN'),('ZMPOP',0,'zmpop-a','MIN'),
                        ('ZMPOP',100,'zmpop-a','MIN'),('ZMPOP',1,'zmpop-b','bad'),
                        ('ZMPOP',1,'zmpop-b','MIN','COUNT',0),
                        ('ZMPOP',1,'zmpop-b','MIN','COUNT',1,'COUNT',2),
                        ('ZMPOP',2,'zmpop-b','bitmap-wrong','MIN'),('ZCARD','zmpop-b')]:
            compare(*command)
        compare('ZADD','range-source',0,b'',0,b'a',0,b'b',0,b'b\0',0,b'c',0,b'\xff')
        for reverse in [False,True]:
            for low,high in [('-', '+'),('[a','[c'),('(a','(c'),('[b',b'[b\0'),('+','-')]:
                bounds=(high,low) if reverse else (low,high)
                flags=['BYLEX']+(['REV'] if reverse else [])
                for limit in [[],['LIMIT',0,2],['LIMIT',1,-1],['LIMIT',-1,2]]:
                    compare('ZRANGE','range-source',*bounds,*flags,*limit)
                    compare('ZRANGESTORE','range-dest','range-source',*bounds,*flags,*limit)
                    compare('ZRANGE','range-dest',0,-1,'WITHSCORES')
        for flags in [[],['REV'],['BYSCORE'],['BYSCORE','REV'],['BYSCORE','LIMIT',0,2]]:
            compare('SET','range-dest','old','PXAT',4102444800000)
            compare('ZRANGESTORE','range-dest','range-source',0,-1,*flags)
            compare('ZRANGE','range-dest',0,-1,'WITHSCORES');compare('PEXPIRETIME','range-dest')
        compare('ZRANGESTORE','range-source','range-source',1,3)
        compare('ZRANGE','range-source',0,-1,'WITHSCORES')
        for command in [('ZRANGESTORE','range-dest','missing-range',0,-1),('TYPE','range-dest'),
                        ('ZRANGESTORE','range-dest','bitmap-wrong',0,-1),
                        ('ZRANGESTORE','range-dest','range-source',0,-1,'WITHSCORES'),
                        ('ZRANGESTORE','range-dest','range-source','bad','+','BYLEX'),
                        ('ZRANGE','range-source','-','+','BYLEX','BYSCORE')]:
            compare(*command)
        for lower,upper in [('-', '+'),('[a','[c'),('(a','(c'),('[b',b'[b\0'),('+','-'),('[','[a')]:
            compare('DEL','lex-family');compare('ZADD','lex-family',0,b'',0,'a',0,'b',0,b'b\0',0,'c',0,b'\xff')
            compare('PEXPIREAT','lex-family',4102444800000)
            compare('ZLEXCOUNT','lex-family',lower,upper)
            compare('ZRANGEBYLEX','lex-family',lower,upper)
            compare('ZREVRANGEBYLEX','lex-family',upper,lower,'LIMIT',1,2)
            compare('ZREMRANGEBYLEX','lex-family',lower,upper)
            compare('ZRANGE','lex-family',0,-1,'WITHSCORES');compare('PEXPIRETIME','lex-family')
        for command in [('ZLEXCOUNT','missing-lex','-','+'),('ZREMRANGEBYLEX','missing-lex','-','+'),
                        ('ZRANGEBYLEX','lex-family','-','+','WITHSCORES'),
                        ('ZRANGEBYLEX','lex-family','-','+','LIMIT',0,-1),
                        ('ZLEXCOUNT','lex-family','bad','+'),('ZLEXCOUNT','bitmap-wrong','-','+')]:
            compare(*command)
        compare('ZADD','diff-a',1.5,'a',2,'b',2,'c','+inf','d')
        compare('ZADD','diff-b',99,'b',0,'x');compare('SADD','diff-set','c','e')
        for keys in [('diff-a',),('diff-a','diff-b'),('diff-a','diff-set'),
                     ('diff-a','diff-b','diff-set'),('diff-a','diff-a'),('diff-set','diff-b'),
                     ('missing-diff','diff-a'),('diff-a','missing-diff')]:
            compare('ZDIFF',len(keys),*keys)
            compare('ZDIFF',len(keys),*keys,'WITHSCORES')
            compare('SET','diff-target','old','PXAT',4102444800000)
            compare('ZDIFFSTORE','diff-target',len(keys),*keys)
            compare('ZRANGE','diff-target',0,-1,'WITHSCORES');compare('PEXPIRETIME','diff-target')
        compare('ZDIFFSTORE','diff-a',2,'diff-a','diff-b')
        compare('ZRANGE','diff-a',0,-1,'WITHSCORES')
        for command in [('ZDIFF',2,'missing-diff','bitmap-wrong'),('ZDIFF',0,'diff-a'),
                        ('ZDIFF',100,'diff-a'),('ZDIFF',1,'diff-a','bad'),
                        ('ZDIFF',1,'diff-a','WITHSCORES','WITHSCORES'),
                        ('ZDIFFSTORE','diff-target',1,'diff-a','WITHSCORES')]:
            compare(*command)
        compare('ZADD','algebra-a',1,'a',2,'b','+inf','infinite','-inf','negative')
        compare('ZADD','algebra-b',3,'b',4,'c','-inf','infinite','+inf','negative')
        compare('SADD','algebra-set','a','b','set-member')
        for keys in [('algebra-a','algebra-b'),('algebra-a','algebra-set'),
                     ('algebra-a','algebra-a','algebra-b'),('algebra-a','algebra-missing')]:
            for aggregate in ['SUM','MIN','MAX']:
                for weights in [[1]*len(keys),[0]*len(keys),list(range(-1,len(keys)-1))]:
                    for command in ['ZUNION','ZINTER']:
                        options=['WEIGHTS',*weights,'AGGREGATE',aggregate]
                        compare(command,len(keys),*keys,*options,'WITHSCORES')
                        compare('SET','algebra-target','old','PXAT',4102444800000)
                        compare(command+'STORE','algebra-target',len(keys),*keys,*options)
                        compare('ZRANGE','algebra-target',0,-1,'WITHSCORES');compare('PEXPIRETIME','algebra-target')
            for limit in [0,1,2,100]: compare('ZINTERCARD',len(keys),*keys,'LIMIT',limit)
        compare('ZUNIONSTORE','algebra-a',2,'algebra-a','algebra-set')
        compare('ZRANGE','algebra-a',0,-1,'WITHSCORES')
        for command in [('ZINTER',2,'algebra-missing','bitmap-wrong'),
                        ('ZUNION',1,'algebra-a','WEIGHTS','nan'),
                        ('ZUNION',1,'algebra-a','AGGREGATE','bad'),
                        ('ZINTERCARD',1,'algebra-a','WEIGHTS',1),
                        ('ZINTERCARD',1,'algebra-a','LIMIT',-1),
                        ('ZINTERCARD',1,'algebra-a','LIMIT',1,'LIMIT',2),
                        ('ZUNIONSTORE','algebra-target',1,'algebra-a','WITHSCORES')]:
            compare(*command)
        # Absolute deadlines make conditional expiration comparisons independent
        # of wall-clock drift between the two servers.
        deadline=4102444800000
        for kind in ['string','hash','list','set','zset']:
            key='expiry-'+kind
            create={'string':('SET',key,'v'),'hash':('HSET',key,'f','v'),
                    'list':('RPUSH',key,'v'),'set':('SADD',key,'v'),
                    'zset':('ZADD',key,1,'v')}[kind]
            for previous in [None,deadline]:
                for flags in [[],['NX'],['XX'],['GT'],['LT'],['XX','GT'],['XX','LT'],
                              ['NX','NX'],['XX','XX'],['NX','XX'],['GT','LT'],['NX','GT'],['bad']]:
                    for proposed in [0,deadline-1000,deadline,deadline+1000]:
                        compare('DEL',key);compare(*create)
                        if previous is not None: compare('PEXPIREAT',key,previous)
                        compare('PEXPIREAT',key,proposed,*flags)
                        compare('PEXPIRETIME',key);compare('EXPIRETIME',key)
        for command in [('EXPIREAT','expiry-string',4102444800),
                        ('EXPIRE','expiry-string',10,'NX'),('PEXPIRE','expiry-string',10000,'XX'),
                        ('EXPIRE','absent-expiry','bad'),('PEXPIREAT','absent-expiry',0,'NX','LT'),
                        ('EXPIRE','absent-expiry',9223372036854775807),
                        ('PEXPIRE','absent-expiry',9223372036854775807),
                        ('PEXPIREAT','absent-expiry',-9223372036854775808),
                        ('EXPIRETIME','absent-expiry'),('PEXPIRETIME','absent-expiry')]:
            compare(*command)
        compare('SET','keep-expiry','v')
        compare('PEXPIREAT','keep-expiry',deadline+501)
        compare('SET','range-string',b'a\0bc\xffdef')
        for start in [-9223372036854775808,-100,-9,-8,-2,-1,0,1,7,8,100,9223372036854775807]:
            for end in [-9223372036854775808,-100,-9,-8,-2,-1,0,1,7,8,100,9223372036854775807]:
                compare('GETRANGE','range-string',start,end)
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
        compare('SORT','sort:list','DESC','STORE','keep-sort')
        checks_to_repeat=[('HGETALL','wide:hash'),('LRANGE','wide:list',0,-1),('SMEMBERS','wide:set'),('ZRANGE','wide:zset',0,-1,'WITHSCORES'),('GET','restore:string'),('GET','restore:tx'),('HGETALL','restore:hash-expiry'),('HPEXPIRETIME','restore:hash-expiry','FIELDS',2,b'f\0','plain'),('LRANGE','restore:list',0,-1),('SMEMBERS','restore:set'),('ZRANGE','restore:zset',0,-1,'WITHSCORES'),('EXISTS','generic:string','generic:hash','generic:list','generic:set','generic:zset','generic:tx','generic:watch'),('HGETALL','hfe:ex-recovery'),('HPEXPIRETIME','hfe:ex-recovery','FIELDS',2,'a','b'),('HPEXPIRETIME','hfe:recovery','FIELDS',2,'field','plain'),('HGETALL','hfe:recovery'),('ZRANGE','geo:radius-dist',0,-1,'WITHSCORES'),('ZRANGE','geo:distances',0,-1,'WITHSCORES'),('ZRANGE','geo:alias',0,-1,'WITHSCORES'),('ZRANGE','geo:points',0,-1,'WITHSCORES'),('GEOPOS','geo:points','Palermo','transaction'),('PEXPIRETIME','geo:points'),('GET','hll:debug'),('PFDEBUG','ENCODING','hll:debug'),('PFCOUNT','hll:a'),('GET','hll:a'),('PEXPIRETIME','hll:a'),('PFCOUNT','hll:merge'),('PFCOUNT','hll:tx'),('ZRANGE','blocking:zkeep',0,-1,'WITHSCORES'),('LRANGE','blocking:src',0,-1),('LRANGE','blocking:a',0,-1),('GET','watch:counter'),('GET','tx:string'),('HGETALL','tx:hash'),('LRANGE','tx:list',0,-1),('GET','tx:aborted'),('GET','tx:pipeline'),('LRANGE','keep-sort',0,-1),('PTTL','keep-sort'),('HGETALL','getdel-hash'),('PEXPIRETIME','getdel-hash'),('GET','float-string'),('HGET','float-hash','f'),('SMEMBERS','random-recovery'),('ZRANGE','algebra-a',0,-1,'WITHSCORES'),('ZRANGE','algebra-target',0,-1,'WITHSCORES'),('ZRANGE','diff-a',0,-1,'WITHSCORES'),('ZRANGE','diff-target',0,-1,'WITHSCORES'),('ZRANGE','lex-family',0,-1,'WITHSCORES'),('ZRANGE','range-source',0,-1,'WITHSCORES'),('TYPE','range-dest'),('ZRANGE','zmpop-a',0,-1,'WITHSCORES'),('ZRANGE','zmpop-b',0,-1,'WITHSCORES'),('LRANGE','mpop-a',0,-1),('LRANGE','mpop-b',0,-1),('SMEMBERS','set-source'),('SMEMBERS','set-target'),('LRANGE','move-source',0,-1),('LRANGE','move-target',0,-1),('LRANGE','rotate',0,-1),('LRANGE','list-position',0,-1),('PEXPIRETIME','list-position'),('GET','bitfield'),('PEXPIRETIME','bitfield'),('GET','failed-field'),('GET','bitop-alias'),('GET','bitop-target'),('GET','bitmap'),('PEXPIRETIME','bitmap'),('GET','bitmap-empty'),('HGETALL','rename-hash'),('HGETALL','rename-hash-copy'),('GET','rename-string-target'),('PEXPIRETIME','rename-hash'),('GET','keep-getex'),('PEXPIRETIME','keep-getex'),('PEXPIRETIME','keep-expiry'),('GET','keep-expiry'),('GET','range-string'),('MGET','nx-a','nx-b','nx-c'),('GET','new-string'),('GET','string-ext'),('GET','number-ext'),('HGETALL','keep-h'),('LRANGE','keep-l',0,-1),('SMEMBERS','keep-s'),('ZRANGE','keep-z',0,-1,'WITHSCORES'),('HGETALL','rh'),('LRANGE','rl',0,-1),('SMEMBERS','rs'),('ZRANGE','rz',0,-1,'WITHSCORES')]
        expected=[result(actual,cmd) for cmd in checks_to_repeat]
        eventually(shard.committed)
        if args.journal:
            assert shard.sql('SELECT materialized<committed FROM cache_journal.state')=='t'
            assert shard.sql("SELECT count(*) FROM cache_journal.entries WHERE key=convert_to('keep-h','UTF8')")=='0'
        actual.close();shard.crash_restart();actual=redis.Redis(port=shard.port,socket_timeout=10,protocol=args.protocol)
        for cmd,want in zip(checks_to_repeat,expected): assert result(actual,cmd)==want,(cmd,want,result(actual,cmd))
        if compaction_lock:
            compaction_lock.communicate("\\q\n",timeout=5);compaction_lock=None
        if args.journal:
            eventually(lambda: shard.sql('SELECT materialized=committed FROM cache_journal.state')=='t')
            actual.close();shard.crash_restart();actual=redis.Redis(port=shard.port,socket_timeout=10,protocol=args.protocol)
            for cmd,want in zip(checks_to_repeat,expected): assert result(actual,cmd)==want
        for key in ('keep-h','keep-l','keep-s','keep-z'): assert actual.pttl(key)>0
        assert actual.get('legacy')==b'COL1\0\xff'
        # Last-element deletion must persist, including across changing key types.
        for key,command in [('keep-h',('HDEL','keep-h',b'\xff')),('keep-l',('LTRIM','keep-l',1,0)),('keep-s',('SREM','keep-s','a','b')),('keep-z',('ZREMRANGEBYSCORE','keep-z','-inf','+inf'))]:
            actual.execute_command(*command);assert actual.type(key)==b'none'
            actual.set(key,b'COL1\x00string')
        eventually(shard.committed);actual.close();shard.crash_restart();actual=redis.Redis(port=shard.port,socket_timeout=10,protocol=args.protocol)
        for key in ('keep-h','keep-l','keep-s','keep-z'): assert actual.get(key)==b'COL1\x00string'
        actual=check_hash_storage(shard, actual, args.journal, args.protocol)
        print(f'PASS: {checks} RESP{args.protocol} Redis differential/contract checks, four typed values, legacy upgrade, TTLs, PostgreSQL persistence and crash recovery ({"journal" if args.journal else "direct"})')
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
