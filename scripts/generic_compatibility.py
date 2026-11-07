"""UNLINK state, WATCH/EXEC, and RANDOMKEY membership contracts."""
import redis


def check_generic(actual, reference):
    checks = 0
    def compare(*command):
        nonlocal checks
        replies = []
        for client in (actual, reference):
            try:
                replies.append(('ok', client.execute_command(*command)))
            except redis.ResponseError as error:
                replies.append(('error', str(error)))
        assert replies[0] == replies[1], (command, replies)
        checks += 1
        return replies[0]

    compare('UNLINK')
    compare('RANDOMKEY', 'extra')
    for create in [('SET', 'generic:string', b'\0\xff'),
                   ('HSET', 'generic:hash', 'f', 'v'),
                   ('RPUSH', 'generic:list', 'a', 'b'),
                   ('SADD', 'generic:set', 'a', 'b'),
                   ('ZADD', 'generic:zset', 1, 'a')]:
        compare(*create)
        key = create[1]
        compare('PEXPIREAT', key, 4102444800000)
        compare('UNLINK', key, key, 'generic:missing')
        compare('TYPE', key)
        compare('UNLINK', key)
    compare('SET', 'generic:expired', 'v')
    compare('PEXPIREAT', 'generic:expired', 1)
    compare('UNLINK', 'generic:expired')
    for client in (actual, reference):
        client.set('generic:watch', 'v')
        with client.pipeline() as pipe:
            pipe.watch('generic:watch')
            assert client.unlink('generic:watch') == 1
            pipe.multi()
            pipe.set('generic:watch', 'bad')
            try:
                pipe.execute()
                raise AssertionError('UNLINK must invalidate WATCH')
            except redis.WatchError:
                pass
        with client.pipeline() as pipe:
            pipe.watch('generic:missing')
            assert client.unlink('generic:missing') == 0
            pipe.multi()
            pipe.get('generic:missing')
            assert pipe.execute() == [None]
        with client.pipeline() as pipe:
            pipe.hset('generic:tx', 'f', 'v')
            pipe.unlink('generic:tx', 'generic:tx')
            pipe.exists('generic:tx')
            assert pipe.execute() == [1, 1, 0]
        client.set(b'generic:\0\xff', 'v')
        keys = set(client.keys('*'))
        for _ in range(128):
            assert client.randomkey() in keys
            checks += 1
        checks += 3
    return checks
