#!/usr/bin/env python3
"""Real redis-py 6.4.0 RESP2 integration; targets only explicitly supplied endpoints.

Pipelines must use transaction=False: MULTI/EXEC are outside the command subset.
Only keys under --prefix are written. Run write then verify across a restart or
promotion. The barrier confirms local PostgreSQL commit, not replica replay.
"""
import argparse
import json
import time

import redis


def connect(host, port):
    return redis.Redis(host=host, port=port, protocol=2, socket_timeout=5,
                       socket_connect_timeout=5, decode_responses=False)


def barrier(client):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        stats = json.loads(client.execute_command('PGCACHE.STATS'))
        if stats['accepted_sequence'] == stats['committed_sequence'] and stats['pending_bytes'] == 0:
            return stats['committed_sequence']
        time.sleep(.025)
    raise AssertionError('PostgreSQL commit barrier timed out')


def verify(client, prefix):
    assert client.ping()
    assert client.get(prefix + b'binary\x00\xff') == bytes(range(256)) * 16
    assert client.mget([prefix + b'a', prefix + b'b']) == [b'one', b'two']
    assert client.get(prefix + b'conditional') == b'replaced'
    assert client.get(prefix + b'counter') == b'42'
    assert client.get(prefix + b'deleted') is None
    assert client.get(prefix + b'expired') is None
    assert client.get(prefix + b'persistent') == b'stays'
    assert client.pttl(prefix + b'persistent') == -1


def exercise(client, prefix):
    assert client.ping()
    assert client.set(prefix + b'binary\x00\xff', bytes(range(256)) * 16)
    assert client.mset({prefix + b'a': b'one', prefix + b'b': b'two'})
    client.delete(prefix + b'conditional')
    assert client.set(prefix + b'conditional', b'first', nx=True)
    assert client.set(prefix + b'conditional', b'ignored', nx=True) is None
    assert client.set(prefix + b'conditional', b'replaced', xx=True)
    assert client.set(prefix + b'counter', 40)
    assert client.incr(prefix + b'counter') == 41
    assert client.incrby(prefix + b'counter', 1) == 42
    assert client.psetex(prefix + b'expired', 150, b'goes')
    assert 0 < client.pttl(prefix + b'expired') <= 150
    assert client.setex(prefix + b'persistent', 60, b'stays')
    assert client.persist(prefix + b'persistent')
    with client.pipeline(transaction=False) as pipe:
        pipe.set(prefix + b'deleted', b'delete me')
        pipe.get(prefix + b'deleted')
        pipe.delete(prefix + b'deleted')
        pipe.get(prefix + b'deleted')
        assert pipe.execute() == [True, b'delete me', 1, None]
    time.sleep(.2)
    verify(client, prefix)
    sequence = barrier(client)
    client.connection_pool.disconnect()
    verify(client, prefix)
    return sequence


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--host', required=True)
    p.add_argument('--port', type=int, default=6380)
    p.add_argument('--prefix', required=True)
    p.add_argument('--phase', choices=['write', 'verify', 'watch'], required=True)
    p.add_argument('--seconds', type=float, default=60)
    args = p.parse_args()
    client = connect(args.host, args.port)
    try:
        if args.phase == 'watch':
            assert args.seconds > 0
            verify(client, args.prefix.encode())
            print(json.dumps({'watch_started': True}), flush=True)
            deadline = time.monotonic() + args.seconds
            successes, disconnects = 0, 0
            while time.monotonic() < deadline:
                try:
                    verify(client, args.prefix.encode())
                    successes += 1
                except (redis.ConnectionError, redis.TimeoutError):
                    disconnects += 1
                time.sleep(.2)
            verify(client, args.prefix.encode())
            print(json.dumps({'passed': True, 'phase': 'watch', 'redis_py': redis.__version__,
                              'seconds': args.seconds, 'successful_checks': successes,
                              'connection_errors': disconnects}))
            return
        sequence = exercise(client, args.prefix.encode()) if args.phase == 'write' else None
        verify(client, args.prefix.encode())
        print(json.dumps({'passed': True, 'redis_py': redis.__version__, 'protocol': 2,
                          'phase': args.phase, 'prefix': args.prefix,
                          'committed_sequence': sequence}))
    finally:
        client.close()


if __name__ == '__main__':
    main()
