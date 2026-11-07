"""Native Redis DUMP -> Flaro RESTORE, command errors and replacement semantics."""
import redis


def check_restore(actual, reference):
    checks = 0
    source, target = 'restore:source', 'restore:target'
    types = set()
    def compare(*command):
        nonlocal checks
        answers = []
        for client in (actual, reference):
            try: answers.append(('ok', client.execute_command(*command)))
            except redis.ResponseError as error: answers.append(('error', str(error)))
        assert answers[0] == answers[1], (command[:3], answers)
        checks += 1
        return answers[0]
    def import_value(query, *tail, suffix='target'):
        nonlocal checks
        key = 'restore:' + suffix
        payload = reference.dump(source)
        types.add(payload[0])
        compare('RESTORE', key, 0, payload, 'REPLACE')
        compare(query, key, *tail)
        assert actual.execute_command(query, key, *tail) == reference.execute_command(query, source, *tail)
        checks += 1
        return payload

    for value in [b'', b'0', b'-128', b'-32768', b'-2147483648', b'9223372036854775807', b'\0\xff', b'x' * 100000]:
        reference.set(source, value)
        payload = import_value('GET', suffix='string')
    for values in [[b'a', b'1', b'1', b'\0\xff'], [str(n).encode() for n in (-4097, -4096, 127, 128, 4095, 4096, -32769, 2**23, 2**31)], [b'x' * 100000]]:
        reference.delete(source)
        reference.rpush(source, *values)
        import_value('LRANGE', 0, -1, suffix='list')
    for values in [[str(n) for n in (-2**40, -32769, 0, 32768, 2**40)], ['a', 'b', 'c'], ['x' * 100, 'b']]:
        reference.delete(source)
        reference.sadd(source, *values)
        import_value('SMEMBERS', suffix='set')
    for value in [b'v', b'v' * 100]:
        reference.delete(source)
        reference.hset(source, mapping={b'f\0': value, b'plain': b'\xff'})
        import_value('HGETALL', suffix='hash')
        reference.execute_command('HPEXPIREAT', source, 4102444800000, 'FIELDS', 1, b'f\0')
        import_value('HGETALL', suffix='hash-expiry')
        compare('HPEXPIRETIME', 'restore:hash-expiry', 'FIELDS', 2, b'f\0', 'plain')
    for member in [b'b', b'b' * 100]:
        reference.delete(source)
        reference.zadd(source, {b'a': float('-inf'), member: 1.125, b'\xff': float('inf')})
        import_value('ZRANGE', 0, -1, 'WITHSCORES', suffix='zset')
    assert {0, 2, 4, 5, 11, 16, 17, 18, 20, 24, 25} <= types, types

    # Error ordering: options, BUSYKEY, TTL, checksum, object validation.
    reference.set(source, b'good')
    payload = reference.dump(source)
    for existing in (False, True):
        for ttl, blob, options in [
            (0, payload, []), (0, payload, ['REPLACE']),
            (-1, payload, []), ('bad', payload, []), (2**63, payload, []),
            (0, b'bad', []), (0, payload[:-1], ['REPLACE']),
            (0, payload, ['bad']), (0, payload, ['IDLETIME']),
            (0, payload, ['IDLETIME', -1]), (0, payload, ['IDLETIME', 'bad']),
            (0, payload, ['FREQ', -1]), (0, payload, ['FREQ', 256]),
            (0, payload, ['IDLETIME', 2, 'FREQ', 1]),
            (0, payload, ['FREQ', 2, 'IDLETIME', 1]),
            (0, payload, ['REPLACE', 'IDLETIME', 2, 'IDLETIME', 3]),
            (0, payload, ['REPLACE', 'FREQ', 2, 'FREQ', 3]),
            (0, payload, ['REPLACE', 'REPLACE', 'ABSTTL', 'ABSTTL']),
            (0, payload, [b'REPLACE\0ignored']),
            (1, payload, ['REPLACE', 'ABSTTL']),
            (4102444800000, payload, ['REPLACE', 'ABSTTL']),
            (2**63-1, payload, ['REPLACE']),
        ]:
            for client in (actual, reference):
                client.delete(target)
                if existing: client.set(target, 'old')
            compare('RESTORE', target, ttl, blob, *options)
            compare('GET', target)
            compare('PEXPIRETIME', target)
    compare('RESTORE')
    compare('RESTORE', target, 0)
    for client in (actual, reference):
        client.delete(target)
        assert client.restore(target, 5000, payload)
        assert 0 < client.pttl(target) <= 5000
        with client.pipeline() as pipe:
            pipe.watch(target)
            client.restore(target, 0, payload, replace=True)
            pipe.multi(); pipe.get(target)
            try:
                pipe.execute()
                raise AssertionError('RESTORE must invalidate WATCH')
            except redis.WatchError:
                pass
        with client.pipeline() as pipe:
            pipe.restore('restore:tx', 0, payload, replace=True)
            pipe.get('restore:tx')
            assert pipe.execute() == [b'OK', b'good']
        checks += 3
    return checks


def check_legacy_restore(actual, reference):
    import struct
    def length(n):
        if n < 64: return bytes([n])
        if n < 16384: return struct.pack('>H', n | 0x4000)
        if n <= 0xffffffff: return b'\x80' + struct.pack('>I', n)
        return b'\x81' + struct.pack('>Q', n)
    def string(value): return length(len(value)) + value
    def payload(body):
        data = body + b'\x0c\x00'
        crc = 0
        for byte in data:
            crc ^= byte
            for _ in range(8): crc = (crc >> 1) ^ (0x95ac9329ac4bc9b5 if crc & 1 else 0)
        return data + struct.pack('<Q', crc)
    def ziplist(values):
        entries = bytearray(); previous = 0; tail = 10
        for value in values:
            assert len(value) < 64
            tail = 10 + len(entries)
            entry = bytes([previous, len(value)]) + value
            entries.extend(entry); previous = len(entry)
        return struct.pack('<IIH', 11 + len(entries), tail, len(values)) + entries + b'\xff'
    checks = 0
    fixtures = [
        (b'\x09' + string(b'\x01\x01f\x01\x00v\xff'), ('HGETALL',)),
        (b'\x0a' + string(ziplist([b'a', b'a', b'\0\xff'])), ('LRANGE', 0, -1)),
        (b'\x0c' + string(ziplist([b'a', b'1.25'])), ('ZRANGE', 0, -1, 'WITHSCORES')),
        (b'\x0d' + string(ziplist([b'f', b'v'])), ('HGETALL',)),
        (b'\x0e\x01' + string(ziplist([b'a', b'b'])), ('LRANGE', 0, -1)),
        (b'\x12\x01\x01' + string(b'plain\0\xff'), ('LRANGE', 0, -1)),
        (b'\x16\x02' + length(4102444800000) + string(b'f') + string(b'v') + b'\x00' + string(b'plain') + string(b'v'), ('HGETALL',)),
    ]
    reference.delete('restore:legacy-source')
    reference.hset('restore:legacy-source', 'f', 'v')
    reference.execute_command('HPEXPIREAT','restore:legacy-source',4102444800000,'FIELDS',1,'f')
    modern = reference.dump('restore:legacy-source')
    assert modern[0] == 25
    fixtures.append((b'\x17' + modern[9:-10], ('HGETALL',)))
    for body, query in fixtures:
        key = 'restore:legacy'
        data = payload(body)
        for client in (actual, reference): assert client.restore(key, 0, data, replace=True) == b'OK'
        assert actual.execute_command(query[0], key, *query[1:]) == reference.execute_command(query[0], key, *query[1:])
        checks += 1
        if body[0] in (22, 23):
            assert actual.execute_command('HPEXPIRETIME',key,'FIELDS',1,'f') == reference.execute_command('HPEXPIRETIME',key,'FIELDS',1,'f')
            checks += 1
    # Correct checksum with invalid packed metadata must still fail, preserving target.
    previous_validation = reference.config_get('sanitize-dump-payload')['sanitize-dump-payload']
    reference.config_set('sanitize-dump-payload', 'yes')
    for body in [b'\x00\xc3\x01\x1e\x00', b'\x01\x00', b'\x12\x01\x03\x01x',
                 b'\x10'+string(b'\x07\x00\x00\x00\x01\x00\xff'), b'\x04\x02\x01f\x01v\x01f\x01x']:
        outcomes = []
        for client in (actual, reference):
            client.set('restore:malformed','old')
            try: outcomes.append(client.restore('restore:malformed',0,payload(body),replace=True))
            except redis.ResponseError as error: outcomes.append(str(error))
        assert outcomes[0] == outcomes[1], (body, outcomes)
        for client in (actual, reference): assert client.get('restore:malformed') == b'old'
        checks += 1
    reference.config_set('sanitize-dump-payload', previous_validation)
    return checks
