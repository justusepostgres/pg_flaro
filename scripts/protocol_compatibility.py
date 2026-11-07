"""Live RESP and inline protocol checks shared by PostgreSQL compatibility tests."""
from integration import Client, RedisError, frame
import time


def check_client_metadata(actual_port, reference_port):
    checks=0
    peers=[Client(actual_port),Client(reference_port)]
    try:
        ids=[peer.cmd('CLIENT','ID') for peer in peers]
        assert all(isinstance(value,int) and value>0 for value in ids)
        commands=[('CLIENT','GETNAME')]
        for name in [b'worker',b'',b'!~',b'bad name',b'a\nb',b'\xff',b'a\0 bad\xff']:
            commands += [('CLIENT','SETNAME',name),('CLIENT','GETNAME')]
        for attr in ['LIB-NAME','lib-ver',b'LIB-NAME\0suffix']:
            for value in [b'redis-py',b'6.4.0',b'',b'bad name',b'\xff',b'x\0 bad']:
                commands += [('CLIENT','SETINFO',attr,value)]
        commands += [('CLIENT','SETINFO','unknown','x'),('CLIENT','ID','extra'),
                     ('CLIENT','GETNAME','extra'),('CLIENT','SETNAME'),
                     ('CLIENT','SETINFO','LIB-NAME'),('CLIENT',),('CLIENT','bogus'),
                     ('RESET','extra'),('TIME','extra')]
        for command in commands:
            replies=[peer.cmd(*command) for peer in peers]
            normalized=[('error',str(r)) if isinstance(r,RedisError) else ('ok',r) for r in replies]
            assert normalized[0]==normalized[1],(command,normalized)
            checks+=1
        for peer,original in zip(peers,ids):
            assert peer.cmd('CLIENT','ID')==original
            peer.socket.sendall(frame('CLIENT','SETNAME','pipeline-name')+frame('CLIENT','GETNAME'))
            assert peer.read()==b'OK' and peer.read()==b'pipeline-name'
            assert peer.cmd('SET','reset-data','untouched')==b'OK'
            peer.socket.sendall(frame('RESET')+frame('CLIENT','GETNAME')+frame('CLIENT','ID'))
            assert peer.read()==b'RESET' and peer.read() is None and peer.read()==original
            assert peer.cmd('GET','reset-data')==b'untouched'
            for _ in range(3):
                before=time.time_ns()//1000
                clock=peer.cmd('TIME')
                after=time.time_ns()//1000
                assert isinstance(clock,list) and len(clock)==2 and all(v.isdigit() for v in clock)
                seconds,micros=map(int,clock)
                assert 0<=micros<1000000
                # Redis caches wall time during its event-loop cycle.
                assert before-1000000<=seconds*1000000+micros<=after
            checks+=4
        for port,original in zip((actual_port,reference_port),ids):
            another=Client(port)
            try:
                assert another.cmd('CLIENT','GETNAME') is None
                assert another.cmd('CLIENT','ID')!=original
            finally:
                another.close()
        checks+=3
    finally:
        for peer in peers:
            peer.close()
    return checks


def check_resp_headers(actual_port, reference_port):
    checks = 0
    for header in [b'*0\r\n',b'*-1\r\n',b'*-9\r\n',b'*-9223372036854775808\r\n']:
        for split in range(1,len(header)):
            for port in (actual_port,reference_port):
                peer=Client(port)
                try:
                    peer.socket.sendall(frame('PING')+header[:split])
                    assert peer.read()==b'PONG'
                    peer.socket.sendall(header[split:]+frame('ECHO','after-empty-array'))
                    assert peer.read()==b'after-empty-array'
                finally:
                    peer.close()
            checks+=1
    for prefix, values, message in [
        (b'*',['00','01','+1','-0','-01','-9223372036854775809','9223372036854775808','2147483648'],
         'ERR Protocol error: invalid multibulk length'),
        (b'*1\r\n$',['-1','-0','00','01','+1','9223372036854775808'],
         'ERR Protocol error: invalid bulk length'),
    ]:
        for value in values:
            for port in (actual_port,reference_port):
                peer=Client(port)
                try:
                    peer.socket.sendall(prefix+value.encode()+b'\r\n')
                    error=peer.read()
                    assert isinstance(error,RedisError) and str(error)==message,(prefix,value,error)
                    assert peer.file.read(1)==b''
                finally:
                    peer.close()
            checks+=1
    return checks


def check_inline(actual_port, reference_port):
    checks = 0
    cases = [
        b'PING\n',
        b'  PING \t\r\n',
        b'\n\r\n \t\nPING\r\n',
        b'ECHO ""\n',
        b"ECHO ''\r\n",
        b'ECHO "hello world"\r\n',
        b'ECHO abc"def"\n',
        b"ECHO abc'def'\n",
        b'ECHO "a\\x00\\xff\\n\\r\\t\\b\\a"\n',
        b'ECHO "\\x4G\\q"\n',
        b"ECHO 'a\\'b'\n",
        b"ECHO '\\n\\x00'\n",
        b'ECHO raw\\backslash\n',
        b'ECHO "\xff"\n',
        b'ECHO "v"\v\n',
        b'\v\fECHO "value"\n',
    ]
    # Exercise representative split points in quoted arguments with a RESP command
    # before and after it. Reading PONG ensures the prefix was serviced.
    for payload in cases:
        for split in sorted({0, 1, len(payload)//2, len(payload)-1}):
            peers = [Client(actual_port), Client(reference_port)]
            try:
                results = []
                for peer in peers:
                    peer.socket.sendall(frame('PING') + payload[:split])
                    assert peer.read() == b'PONG'
                    peer.socket.sendall(payload[split:] + frame('ECHO', 'inline-tail'))
                    reply = peer.read()
                    results.append(str(reply) if isinstance(reply, RedisError) else reply)
                    assert peer.read() == b'inline-tail'
                assert results[0] == results[1], (payload, split, results)
                checks += 1
            finally:
                for peer in peers:
                    peer.close()
    malformed = [b'ECHO "unterminated\n', b"ECHO 'unterminated\n",
                 b'ECHO "closed"tail\n', b"ECHO 'closed'tail\n"]
    for payload in malformed:
        for port in (actual_port, reference_port):
            peer = Client(port)
            try:
                peer.socket.sendall(payload)
                error = peer.read()
                assert isinstance(error, RedisError), (payload, error)
                assert str(error) == 'ERR Protocol error: unbalanced quotes in request'
                assert peer.file.read(1) == b''
            finally:
                peer.close()
        checks += 1
    return checks


def check_hello(actual_port, reference_port):
    """Use redis-py's RESP3 parser across live protocol switches and errors."""
    import redis
    peers=[redis.Connection(port=p,protocol=3,socket_timeout=10) for p in (actual_port,reference_port)]
    def run(peer,*args):
        peer.send_command(*args)
        try:
            return peer.read_response()
        except (redis.ResponseError, redis.AuthenticationError) as error:
            return ('error',str(error))
    checks=0
    try:
        for peer in peers:
            peer.connect()
        for command in [('HELLO',),('HELLO',2,'SETNAME','switch'),('HELLO',3),
                        ('HELLO',2,'SETNAME','changed','bogus'),('CLIENT','GETNAME'),
                        ('HELLO',4),('HELLO','03'),('HELLO',2,'AUTH','unknown','pw'),
                        ('HELLO',3,'AUTH','default','anything'),('RESET',),
                        ('HELLO',),('HELLO',3),('CLIENT','GETNAME')]:
            replies=[run(peer,*command) for peer in peers]
            if command[0]=='HELLO' and all(isinstance(r,(dict,list)) for r in replies):
                assert type(replies[0]) is type(replies[1]),replies
                maps=[r if isinstance(r,dict) else dict(zip(r[::2],r[1::2])) for r in replies]
                assert maps[0][b'modules']==[]
                assert isinstance(maps[1][b'modules'],list)
                for value in maps:
                    assert value[b'id']>0 and value[b'server'] and value[b'version']
                    for key in [b'id',b'server',b'version',b'modules']:
                        del value[key]
                assert maps[0]==maps[1],(command,maps)
            else:
                assert replies[0]==replies[1],(command,replies)
            checks+=1
    finally:
        for peer in peers:
            peer.disconnect()
    return checks


def check_wide_commands(actual, reference):
    """Large argument vectors must execute, survive fragmentation, and keep framing."""
    checks = 0
    fields = {f'f:{i}': str(i) for i in range(2048)}
    for client in (actual, reference):
        client.delete('wide:hash', 'wide:list', 'wide:set', 'wide:zset')
        assert client.hset('wide:hash', mapping=fields) == len(fields)
        assert client.hlen('wide:hash') == len(fields)
        assert client.hmget('wide:hash', list(fields)) == [v.encode() for v in fields.values()]
        members = [f'm:{i}' for i in range(2048)]
        assert client.rpush('wide:list', *members) == len(members)
        assert client.sadd('wide:set', *members) == len(members)
        assert client.zadd('wide:zset', {m: i for i, m in enumerate(members)}) == len(members)
        assert client.zrange('wide:zset', 0, -1) == [m.encode() for m in members]
        with client.pipeline() as pipe:
            pipe.hset('wide:hash', mapping=fields)
            pipe.hmget('wide:hash', list(fields))
            pipe.ping()
            assert pipe.execute() == [0, [v.encode() for v in fields.values()], True]
        checks += 8
        # 100,001 arguments fit within the existing 1-MiB frame bound.
        assert client.execute_command('EXISTS', *([b''] * 100000)) == 0
        checks += 1
        peer = Client(client.connection_pool.connection_kwargs['port'])
        try:
            command = frame('EXISTS', *(['wide:hash'] * 4096))
            peer.socket.sendall(frame('PING') + command[:8191])
            assert peer.read() == b'PONG'
            peer.socket.sendall(command[8191:] + frame('ECHO', b'after\0wide\xff'))
            assert peer.read() == 4096
            assert peer.read() == b'after\0wide\xff'
            # Inline framing uses its byte limit rather than the old 1,024 cap.
            peer.socket.sendall(b'EXISTS ' + b'wide:hash ' * 2048 + b'\r\n' + frame('PING'))
            assert peer.read() == 2048 and peer.read() == b'PONG'
            checks += 2
        finally:
            peer.close()
    return checks
