"""Verify exported DUMP bytes by restoring them in native Redis."""
import redis


def check_dump(actual, reference):
    checks = 0
    key = b'dump:source\0\xff'
    target = b'dump:target\0\xff'

    def restore_and_compare(query, *tail):
        nonlocal checks
        payload = actual.dump(key)
        assert isinstance(payload, bytes) and len(payload) >= 11
        assert payload[-10:-8] == b'\x0c\x00'
        assert reference.restore(target, 0, payload, replace=True)
        assert actual.execute_command(query, key, *tail) == reference.execute_command(query, target, *tail)
        assert reference.pttl(target) == -1  # Key TTL is not part of DUMP.
        checks += 3
        return payload

    for command in [('DUMP',), ('DUMP', key, 'extra'), ('DUMP', 'dump:absent')]:
        results = []
        for client in (actual, reference):
            try:
                results.append(('ok', client.execute_command(*command)))
            except redis.ResponseError as error:
                results.append(('error', str(error)))
        assert results[0] == results[1], (command, results)
        checks += 1

    # Raw integer-looking strings, embedded NULs, and length-encoding boundaries.
    for value in [b'', b'0', b'-2147483648', b'9223372036854775807', b'\0\xff',
                  *[b'x' * length for length in (63, 64, 16383, 16384, 262144)]]:
        actual.set(key, value)
        actual.pexpireat(key, 4102444800000)
        payload = restore_and_compare('GET')
    corrupt = bytearray(payload)
    corrupt[2] ^= 1
    try:
        reference.restore(target, 0, bytes(corrupt), replace=True)
        raise AssertionError('corrupted checksum must fail')
    except redis.ResponseError as error:
        assert 'checksum' in str(error).lower()
        checks += 1

    for create, query in [
        (('RPUSH', key, b'\0', b'\xff', b'1', b'1'), ('LRANGE', 0, -1)),
        (('SADD', key, b'\0', b'\xff', b'1'), ('SMEMBERS',)),
        (('HSET', key, b'\0', b'\xff', b'1', b'2'), ('HGETALL',)),
        (('ZADD', key, '-inf', 'a', -0.125, b'\0', 0, b'\xff', '+inf', 'z'), ('ZRANGE', 0, -1, 'WITHSCORES')),
    ]:
        actual.delete(key)
        actual.execute_command(*create)
        restore_and_compare(*query)

    actual.delete(key)
    actual.hset(key, mapping={str(i): b'v' * 65 for i in range(520)})
    restore_and_compare('HGETALL')
    actual.execute_command('HPEXPIREAT', key, 4102444800000, 'FIELDS', 2, '0', '1')
    actual.execute_command('HPEXPIREAT', key, 4102444800000 + (1 << 33), 'FIELDS', 1, '2')
    actual.execute_command('HPEXPIREAT', key, 1, 'FIELDS', 1, '3')
    restore_and_compare('HGETALL')
    fields = ('FIELDS', 5, '0', '1', '2', '3', '4')
    assert actual.execute_command('HPEXPIRETIME', key, *fields) == reference.execute_command('HPEXPIRETIME', target, *fields)
    checks += 1

    # DUMP is read-only for WATCH and can return a value inside EXEC.
    with actual.pipeline() as pipe:
        pipe.watch(key)
        actual.dump(key)
        pipe.multi()
        pipe.dump(key)
        result = pipe.execute()
        assert reference.restore(target, 0, result[0], replace=True)
        checks += 1
    actual.pexpireat(key, 1)
    assert actual.dump(key) is None
    checks += 1
    actual.delete(key)
    reference.delete(target)
    return checks
