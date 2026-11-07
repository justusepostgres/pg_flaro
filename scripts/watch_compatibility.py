"""WATCH differential checks through real redis-py optimistic pipelines."""
import time
import redis


def check_watch(actual, reference):
    checks=0
    scenarios=[
        ([('SET','watch:k','old')],[('SET','watch:k','old')]),
        ([('SET','watch:k','old')],[('SET','watch:k','new'),('SET','watch:k','old')]),
        ([],[('SET','watch:k','new'),('DEL','watch:k')]),
        ([],[('DEL','watch:k')]),
        ([],[('SORT','watch:absent','STORE','watch:k')]),
        ([('SET','watch:k','old')],[('GET','watch:k')]),
        ([('SET','watch:k','old')],[('SET','watch:k','new','NX')]),
        ([('HSET','watch:k','f','v')],[('HSET','watch:k','f','v')]),
        ([('SADD','watch:k','a')],[('SADD','watch:k','a')]),
        ([('SADD','watch:k','a')],[('SADD','watch:k','b')]),
        ([('SADD','watch:k','a')],[('SREM','watch:k','missing')]),
        ([('RPUSH','watch:k','a')],[('LSET','watch:k',0,'a')]),
        ([('ZADD','watch:k',1,'a')],[('ZINCRBY','watch:k',0,'a')]),
        ([('ZADD','watch:k',1,'a')],[('ZINCRBY','watch:k',1,'a')]),
        ([('SET','watch:k','old')],[('PEXPIRE','watch:k',100000)]),
        ([('SET','watch:k','old')],[('INCR','watch:k')]),
    ]
    for setup,mutations in scenarios:
        results=[]
        for client in (actual,reference):
            client.delete('watch:k')
            for cmd in setup:client.execute_command(*cmd)
            with client.pipeline() as pipe:
                pipe.watch('watch:k')
                for cmd in mutations:
                    try:client.execute_command(*cmd)
                    except redis.ResponseError:pass
                pipe.multi();pipe.get('watch:missing')
                try:results.append(pipe.execute())
                except redis.WatchError:results.append('aborted')
        assert results[0]==results[1],(setup,mutations,results)
        checks+=1
    for mode in ['expire','unwatch','repeat_watch','own_write','transaction_write','expired_before_watch']:
        results=[]
        for client in (actual,reference):
            client.set('watch:k','old')
            with client.pipeline() as pipe:
                if mode=='expired_before_watch':
                    client.pexpire('watch:k',1);time.sleep(.02)
                pipe.watch('watch:k')
                if mode=='expire':
                    # Change the deadline before watching again with a new pipeline.
                    pipe.unwatch();client.pexpire('watch:k',20);pipe.watch('watch:k');time.sleep(.05)
                elif mode=='transaction_write':
                    with client.pipeline() as writer:
                        writer.set('watch:k','changed').set('watch:k','old').execute()
                elif mode=='own_write':pipe.set('watch:k','old')
                elif mode not in ('expired_before_watch',):client.set('watch:k','changed')
                if mode=='unwatch':pipe.unwatch()
                if mode=='repeat_watch':pipe.watch('watch:k')
                pipe.multi();pipe.set('watch:result','done')
                try:results.append(pipe.execute())
                except redis.WatchError:results.append('aborted')
        assert results[0]==results[1],(mode,results)
        checks+=1
    # Successful optimistic read/modify/write through redis-py's canonical API.
    for client in (actual,reference):
        client.set('watch:counter',0)
        with client.pipeline() as pipe:
            pipe.watch('watch:counter')
            value=int(pipe.get('watch:counter'))
            pipe.multi();pipe.set('watch:counter',value+1)
            assert pipe.execute()==[True]
        assert client.get('watch:counter')==b'1'
    checks+=1
    return checks
