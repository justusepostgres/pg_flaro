"""Live transaction protocol checks, including redis-py default pipelines."""
import redis


def check_transactions(actual, reference, protocol):
    peers=[redis.Connection(host='127.0.0.1',port=c.connection_pool.connection_kwargs['port'],protocol=protocol,socket_timeout=10) for c in (actual,reference)]
    checks=0
    queued=[]
    def norm(value):
        if isinstance(value,redis.ResponseError):
            text=str(value)
            if text.startswith('unknown command'): return ('error','unknown command')
            return ('error',text)
        if isinstance(value,list):return [norm(v) for v in value]
        return value
    def command(*args):
        nonlocal checks
        values=[]
        for peer in peers:
            peer.send_command(*args)
            try: value=peer.read_response()
            except redis.ResponseError as e:value=e
            values.append(norm(value))
        if args[0]=='EXEC' and len(args)==1:
            for value in values:
                if isinstance(value,list):
                    for i,cmd in enumerate(queued):
                        if cmd[0]=='SMEMBERS': value[i]=sorted(value[i])
            queued.clear()
        elif values[0]==b'QUEUED': queued.append(args)
        elif args[0] in ('DISCARD','RESET') and len(args)==1: queued.clear()
        assert values[0]==values[1],(args,values)
        checks+=1
        return values[0]
    try:
        for peer in peers:peer.connect()
        for args in [
            ('EXEC',),('DISCARD',),('WATCH','watch:raw'),('MULTI',),('DISCARD',),('SET','watch:raw','v'),('MULTI',),('EXEC',),
            ('WATCH','watch:raw'),('MULTI',),('WATCH','watch:raw'),('UNWATCH',),('EXEC',),
            ('MULTI',),('MULTI',),('SET','tx:string','1'),('INCR','tx:string'),('GET','tx:string'),
            ('RPUSH','tx:list','a','b'),('LRANGE','tx:list',0,-1),('HSET','tx:hash','f','v'),('HGETALL','tx:hash'),
            ('SADD','tx:set','a','b'),('SMEMBERS','tx:set'),('ZADD','tx:zset',1.5,'a'),('ZSCORE','tx:zset','a'),
            ('GET','tx:absent'),('EXEC',),
            ('MULTI',),('SET','tx:discard','v'),('DISCARD',),('GET','tx:discard'),
            ('MULTI',),('SET','tx:runtime','text'),('INCR','tx:runtime'),('HSET','tx:runtime','f','v'),
            ('SET','tx:after','yes'),('EXEC',),('GET','tx:after'),
            ('MULTI',),('SET','tx:aborted','v'),('SET','arity'),('GET','tx:aborted'),('EXEC',),('GET','tx:aborted'),
            ('MULTI',),('does-not-exist',),('EXEC',),
            ('MULTI',),('EXEC','bad'),('EXEC',),
            ('MULTI',),('DISCARD','bad'),('EXEC',),
            ('MULTI',),('RESET','bad'),('EXEC',),
            ('MULTI',),('CLIENT','SETNAME'),('EXEC',),
            ('MULTI',),('COMMAND','COUNT','bad'),('EXEC',),
            ('MULTI',),('CLIENT','SETNAME','tx-name'),('CLIENT','GETNAME'),('EXEC',),('CLIENT','GETNAME'),
            ('WATCH','watch:raw'),('MULTI',),('SET','tx:reset','v'),('RESET',),('EXEC',),
            ('SET','watch:raw','after-reset'),('MULTI',),('EXEC',),
        ]:
            command(*args)
        # RESET resets the parser protocol too; return to RESP3 before nullable replies.
        if protocol==3:
            for peer in peers:
                peer.send_command('HELLO',3);peer.read_response()
        command('GET','tx:reset')
        for args in [('MULTI',),('SET','tx:isolation','pending')]:command(*args)
        assert actual.get('tx:isolation') is None and reference.get('tx:isolation') is None
        command('EXEC')
        for client in (actual,reference):assert client.get('tx:isolation')==b'pending'
        checks+=2
        # Real redis-py transaction pipelines apply callbacks to EXEC children.
        results=[]
        for client in (actual,reference):
            with client.pipeline() as pipe:
                pipe.set('tx:pipeline','10').incr('tx:pipeline').get('tx:pipeline')
                pipe.hgetall('tx:hash').smembers('tx:set').zscore('tx:zset','a')
                results.append(pipe.execute())
        assert results[0]==results[1],results
        checks+=1
    finally:
        for peer in peers:peer.disconnect()
    return checks
