"""Blocking list differential tests using independent wire connections."""
import time
from contextlib import ExitStack
from integration import Client, RedisError, frame


def check_blocking(actual, reference, protocol):
    results = []
    for endpoint in (actual, reference):
        port = endpoint.connection_pool.connection_kwargs['port']
        with ExitStack() as stack:
            def connect():
                peer = Client(port)
                stack.callback(peer.close)
                if protocol == 3:
                    assert isinstance(peer.cmd('HELLO', 3), dict)
                return peer
            writer, first, second = connect(), connect(), connect()
            observed = []
            def record(value):
                observed.append(('error', str(value)) if isinstance(value, RedisError) else value)
            def reset():
                writer.cmd('DEL', 'blocking:a', 'blocking:b')
            def wait(peer, *keys, command='BLPOP'):
                peer.socket.sendall(frame(command, *keys, 2))
                time.sleep(.05)
                assert writer.cmd('PING') == b'PONG'
            for command in ('BLPOP', 'BRPOP'):
                reset()
                writer.cmd('RPUSH', 'blocking:a', b'a\x00\xff', 'b')
                record(first.cmd(command, 'blocking:absent', 'blocking:a', 0))
                for timeout in ('nan', 'inf', '-1', '1e9999', 'bad', '0x1p-1'):
                    record(first.cmd(command, 'blocking:a', timeout))
                start = time.monotonic()
                record(first.cmd(command, 'blocking:absent', .02))
                assert .015 <= time.monotonic() - start < 2
                record(first.cmd(command, 'blocking:a'))
            reset()
            wait(first, 'blocking:a')
            wait(second, 'blocking:a')
            record(writer.cmd('RPUSH', 'blocking:a', 'one', 'two'))
            record(first.read()); record(second.read())
            reset()
            wait(first, 'blocking:a')
            writer.socket.sendall(frame('RPUSH', 'blocking:a', 'one') + frame('LPOP', 'blocking:a'))
            record(writer.read()); record(writer.read()); record(first.read())
            reset()
            wait(first, 'blocking:a', 'blocking:b')
            writer.cmd('SET', 'blocking:a', 'wrongtype')
            writer.cmd('RPUSH', 'blocking:b', 'ready')
            record(first.read())
            reset()
            wait(first, 'blocking:a', 'blocking:b')
            writer.cmd('MULTI')
            writer.cmd('RPUSH', 'blocking:b', 'first')
            writer.cmd('RPUSH', 'blocking:a', 'second')
            record(writer.cmd('EXEC')); record(first.read())
            reset()
            first.cmd('MULTI')
            record(first.cmd('BLPOP', 'blocking:a', 0))
            record(first.cmd('BRPOP', 'blocking:a', 0))
            record(first.cmd('EXEC'))
            first.socket.sendall(frame('BLPOP', 'blocking:a', 2) + frame('ECHO', 'after'))
            time.sleep(.05)
            writer.cmd('RPUSH', 'blocking:a', 'value')
            record(first.read()); record(first.read())
            orphan = Client(port)
            orphan.socket.sendall(frame('BLPOP', 'blocking:a', 0))
            time.sleep(.05)
            orphan.close()
            time.sleep(.05)
            writer.cmd('RPUSH', 'blocking:a', 'survives')
            time.sleep(.05)
            record(writer.cmd('LRANGE', 'blocking:a', 0, -1))
            assert observed[-1] == [b'survives']
            results.append(observed)
    assert results[0] == results[1], list(zip(results[0], results[1]))
    return len(results[0])


def check_blocking_family(actual, reference, protocol):
    """Moves, multi-pop and sorted-set blocking, including exact RESP shapes."""
    results = []
    cases = [
        ('BLMOVE', ('blocking:src', 'blocking:dst', 'LEFT', 'RIGHT', .03), False),
        ('BRPOPLPUSH', ('blocking:src', 'blocking:dst', .03), False),
        ('BLMPOP', (.03, 2, 'blocking:missing', 'blocking:src', 'RIGHT', 'COUNT', 2), False),
        ('BZPOPMIN', ('blocking:missing', 'blocking:src', .03), True),
        ('BZPOPMAX', ('blocking:missing', 'blocking:src', .03), True),
        ('BZMPOP', (.03, 2, 'blocking:missing', 'blocking:src', 'MAX', 'COUNT', 2), True),
    ]
    for endpoint in (actual, reference):
        port = endpoint.connection_pool.connection_kwargs['port']
        observed = []
        with ExitStack() as stack:
            def connect():
                peer = Client(port)
                stack.callback(peer.close)
                if protocol == 3:
                    assert isinstance(peer.cmd('HELLO', 3), dict)
                return peer
            writer, reader = connect(), connect()
            def normalize(value):
                if isinstance(value, RedisError): return ('error', str(value))
                if isinstance(value, list): return [normalize(v) for v in value]
                return value
            def record(value): observed.append(normalize(value))
            def clear(): writer.cmd('DEL', 'blocking:src', 'blocking:dst', 'blocking:missing')
            def populate(sorted_set):
                return writer.cmd('ZADD', 'blocking:src', '1.5', b'one\x00\xff', '-2', 'two', '3', 'three') if sorted_set else writer.cmd('RPUSH', 'blocking:src', b'one\x00\xff', 'two', 'three')
            for command, arguments, sorted_set in cases:
                clear()
                start = time.monotonic()
                record(reader.cmd(command, *arguments))
                assert .02 <= time.monotonic() - start < 2
                populate(sorted_set)
                record(reader.cmd(command, *arguments))
                record(writer.cmd('ZRANGE' if sorted_set else 'LRANGE', 'blocking:src', 0, -1))
                if not sorted_set: record(writer.cmd('LRANGE', 'blocking:dst', 0, -1))
                # Timeout parsing and arity errors must not mutate the data.
                for timeout in ('bad', '-1', 'inf', 'nan'):
                    args = list(arguments)
                    args[0 if command in ('BLMPOP', 'BZMPOP') else -1] = timeout
                    record(reader.cmd(command, *args))
                record(reader.cmd(command))
                clear()
                writer.cmd('SET', 'blocking:src', 'wrong')
                record(reader.cmd(command, *arguments))
                clear()
                args = list(arguments)
                args[0 if command in ('BLMPOP', 'BZMPOP') else -1] = 0
                reader.cmd('MULTI')
                record(reader.cmd(command, *args))
                record(reader.cmd('EXEC'))
                # A truly blocked request must resume once the source is ready.
                reader.socket.sendall(frame(command, *args) + frame('PING'))
                time.sleep(.05)
                assert writer.cmd('PING') == b'PONG'
                populate(sorted_set)
                record(reader.read()); record(reader.read())
                # Moves validate the destination only once the source exists.
                if command in ('BLMOVE', 'BRPOPLPUSH'):
                    clear()
                    writer.cmd('SET', 'blocking:dst', 'wrong')
                    reader.socket.sendall(frame(command, *args))
                    time.sleep(.05)
                    populate(False)
                    record(reader.read())
                    record(writer.cmd('LRANGE', 'blocking:src', 0, -1))
                    clear()
                    populate(False)
                    args[1] = args[0]  # Rotation on one key is atomic.
                    record(reader.cmd(command, *args))
                    record(writer.cmd('LRANGE', 'blocking:src', 0, -1))
            # Validate options before the timeout, as the Redis implementation does.
            for command, args in [
                ('BLMOVE', ('a','b','bad','bad','bad')),
                ('BLMPOP', ('bad',0,'a','LEFT')),
                ('BLMPOP', ('bad',1,'a','bad')),
                ('BLMPOP', ('bad',1,'a','LEFT','COUNT',0)),
                ('BZMPOP', ('bad',0,'a','MIN')),
                ('BZMPOP', ('bad',1,'a','bad')),
                ('BZMPOP', ('bad',1,'a','MIN','COUNT',0)),
            ]:
                record(reader.cmd(command,*args))
            clear()
            # Moving into a key must wake an already blocked consumer on it.
            receiver = connect()
            receiver.socket.sendall(frame('BLPOP', 'blocking:dst', 2))
            time.sleep(.05)
            reader.socket.sendall(frame('BLMOVE', 'blocking:src', 'blocking:dst', 'LEFT', 'RIGHT', 2))
            time.sleep(.05)
            populate(False)
            record(reader.read()); record(receiver.read())
            writer.cmd('DEL', 'blocking:zkeep')
            writer.cmd('ZADD', 'blocking:zkeep', 1, 'removed', 2, 'survivor')
            record(reader.cmd('BZMPOP', 0, 1, 'blocking:zkeep', 'MIN'))
            results.append(observed)
    assert results[0] == results[1], [(i,a,b) for i,(a,b) in enumerate(zip(*results)) if a != b]
    return len(results[0])


def check_unblock(actual, reference, protocol):
    results = []
    commands = [
        ('BLPOP', 'cancel:src', 0), ('BRPOP', 'cancel:src', 0),
        ('BLMOVE', 'cancel:src', 'cancel:dst', 'LEFT', 'RIGHT', 0),
        ('BRPOPLPUSH', 'cancel:src', 'cancel:dst', 0),
        ('BLMPOP', 0, 1, 'cancel:src', 'LEFT'),
        ('BZPOPMIN', 'cancel:src', 0), ('BZPOPMAX', 'cancel:src', 0),
        ('BZMPOP', 0, 1, 'cancel:src', 'MIN'),
    ]
    for endpoint in (actual, reference):
        observed = []
        port = endpoint.connection_pool.connection_kwargs['port']
        with ExitStack() as stack:
            def connect():
                peer = Client(port)
                stack.callback(peer.close)
                if protocol == 3: peer.cmd('HELLO', 3)
                return peer
            caller, target = connect(), connect()
            target_id = target.cmd('CLIENT','ID')
            caller_id = caller.cmd('CLIENT','ID')
            def normalized(value):
                if isinstance(value, RedisError): return ('error',str(value))
                if isinstance(value,list): return [normalized(v) for v in value]
                return value
            def record(value): observed.append(normalized(value))
            def block(command):
                caller.cmd('DEL','cancel:src','cancel:dst')
                target.socket.sendall(frame(*command) + frame('ECHO','after-cancel'))
                time.sleep(.05)
            for command in commands:
                for mode in [(), ('TIMEOUT',), ('ERROR',)]:
                    block(command)
                    record(caller.cmd('CLIENT','UNBLOCK',target_id,*mode))
                    record(target.read()); record(target.read())
                    record(caller.cmd('CLIENT','UNBLOCK',target_id,*mode))
                    assert caller.cmd('EXISTS','cancel:src') == 0
            for args in [(caller_id,),(-1,),('bad',),('01',),('9223372036854775808',),
                         (target_id,'bad'),('bad','bad'),(),(target_id,'ERROR','extra')]:
                record(caller.cmd('CLIENT','UNBLOCK',*args))
            # No unblock is published by a discarded transaction.
            block(commands[0])
            caller.cmd('MULTI')
            record(caller.cmd('CLIENT','UNBLOCK',target_id,'ERROR'))
            record(caller.cmd('DISCARD'))
            record(caller.cmd('CLIENT','UNBLOCK',target_id))
            record(target.read()); record(target.read())
            # EXEC can unblock once; repeated requests in it return zero.
            block(commands[0])
            caller.cmd('MULTI')
            caller.cmd('CLIENT','UNBLOCK',target_id,'ERROR')
            caller.cmd('CLIENT','UNBLOCK',target_id)
            caller.cmd('RPUSH','cancel:src','unconsumed')
            record(caller.cmd('EXEC'))
            record(target.read()); record(target.read())
            record(caller.cmd('LRANGE','cancel:src',0,-1))
            # A pipeline may not overwrite a successful earlier cancellation.
            block(commands[0])
            caller.socket.sendall(frame('CLIENT','UNBLOCK',target_id,'ERROR') + frame('CLIENT','UNBLOCK',target_id))
            record(caller.read()); record(caller.read())
            record(target.read()); record(target.read())
            results.append(observed)
    assert results[0] == results[1], [(i,a,b) for i,(a,b) in enumerate(zip(*results)) if a != b]
    return len(results[0])
