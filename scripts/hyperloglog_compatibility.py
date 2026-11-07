"""HyperLogLog differential, binary interchange, cache and WATCH checks."""
import random
import redis


def check_hyperloglog(actual, reference):
    checks = 0
    def result(client, command):
        try: return ('ok', client.execute_command(*command))
        except redis.ResponseError as error: return ('error', str(error))
    def compare(*command):
        nonlocal checks
        replies=[result(client,command) for client in (actual,reference)]
        assert replies[0]==replies[1], (command[:5],replies)
        checks+=1
        return replies[0]
    for client in (actual,reference):
        client.delete('hll:a','hll:b','hll:empty','hll:bad','hll:typed','hll:merge')
    for command in [('PFADD','hll:empty'),('PFADD','hll:empty'),('PFCOUNT','hll:empty'),
                    ('PFCOUNT','hll:missing'),('PFCOUNT','hll:missing','hll:empty'),
                    ('PFMERGE','hll:merge'),('PFCOUNT','hll:merge'),('PFADD',),('PFCOUNT',),('PFMERGE',)]:
        compare(*command)
    compare('PFSELFTEST')
    compare('PFSELFTEST','extra')
    values=[bytes(range(size)) for size in range(32)] + [b'a\x00\xff',b'',b'a'*70]
    compare('PFADD','hll:a',*values)
    compare('PFCOUNT','hll:a')
    compare('PFADD','hll:a',*values)
    compare('PFCOUNT','hll:a')
    rng=random.Random(79237)
    for key,total in [('hll:a',18000),('hll:b',12000)]:
        for start in range(0,total,512):
            batch=[str(rng.randrange(30000)).encode()+b'\x00' for _ in range(min(512,total-start))]
            compare('PFADD',key,*batch)
            compare('PFCOUNT',key)
        compare('TYPE',key)
    compare('PFCOUNT','hll:a','hll:b','hll:a','hll:missing')
    compare('PEXPIREAT','hll:merge',4102444800000)
    compare('PFMERGE','hll:merge','hll:a','hll:b')
    compare('PFCOUNT','hll:merge')
    compare('PEXPIRETIME','hll:merge')
    compare('PFMERGE','hll:a','hll:a','hll:b')
    compare('PFCOUNT','hll:a')
    # Neither PFADD nor an ordinary count cache refresh clears a key TTL.
    compare('PEXPIREAT','hll:a',4102444800000)
    compare('PFADD','hll:a','new-for-ttl')
    compare('PFCOUNT','hll:a')
    compare('PEXPIRETIME','hll:a')
    for client in (actual,reference):
        client.set('hll:bad',b'not a sketch')
        client.hset('hll:typed','field','value')
    for key in ('hll:bad','hll:typed'):
        for command in [('PFADD',key),('PFADD',key,'v'),('PFCOUNT',key),('PFCOUNT','hll:a',key),('PFMERGE','hll:merge',key)]: compare(*command)
    compare('PFCOUNT','hll:merge')
    # A real sparse reference sketch and a dense one must work in both directions.
    for client in (actual,reference):
        client.delete('hll:small')
        client.pfadd('hll:small','one','two')
    for source in ('hll:small','hll:a'):
        for producer,consumer in ((reference,actual),(actual,reference)):
            payload=producer.get(source)
            assert payload[:4]==b'HYLL' and payload[4] in (0,1)
            uncached=bytearray(payload);uncached[15]|=128
            consumer.set('hll:import',bytes(uncached))
            assert consumer.pfcount('hll:import') == producer.pfcount(source)
            checks+=1
        # With identical incoming bytes, count-cache changes must match byte-for-byte.
        payload=bytearray(reference.get(source));payload[15]|=128
        for client in (actual,reference):client.set('hll:import',bytes(payload))
        compare('PFCOUNT','hll:import')
        compare('GET','hll:import')
        compare('PFADD','hll:import','after-import')
        compare('PFCOUNT','hll:import')
    # Validate envelope errors and malformed sparse payloads separately.
    valid=bytearray(b'HYLL'+bytes([1])+bytes(11));valid[15]=128
    malformed=[b'',b'HYLL',b'HYLL'+bytes([2])+bytes(11),b'HYLL'+bytes(12),bytes(valid),bytes(valid)+b'\x40',bytes(valid)+b'\x7f\xff\x00']
    for payload in malformed:
        for client in (actual,reference):client.set('hll:corrupt',payload)
        for command in [('PFCOUNT','hll:corrupt'),('PFCOUNT','hll:corrupt','hll:missing'),('PFMERGE','hll:out','hll:corrupt')]:compare(*command)
    # Debug conversion must preserve bytes, TTL and cached cardinality.
    for source in ('hll:small', 'hll:a'):
        payload=reference.get(source)
        for client in (actual,reference):client.set('hll:debug',payload)
        compare('PEXPIREAT','hll:debug',4102444800000)
        compare('PFDEBUG','ENCODING','hll:debug')
        compare('PFDEBUG','DECODE','hll:debug')
        compare('PFDEBUG','GETREG','hll:debug')
        compare('GET','hll:debug')
        compare('PFDEBUG','TODENSE','hll:debug')
        compare('PFDEBUG','DECODE','hll:debug')
        compare('PEXPIRETIME','hll:debug')
    for payload in malformed:
        for client in (actual,reference):client.set('hll:debug',payload)
        for sub in ('ENCODING','DECODE','TODENSE','GETREG','unknown'):
            compare('PFDEBUG',sub,'hll:debug')
    for command in [('PFDEBUG',),('PFDEBUG','GETREG'),('PFDEBUG','GETREG','hll:missing'),
                    ('PFDEBUG','unknown','hll:missing'),('PFDEBUG','GETREG','hll:typed'),
                    ('PFDEBUG','GETREG','hll:small','extra'),('PFDEBUG','SIMD','OFF')]:
        compare(*command)
    for conversion in ('GETREG','TODENSE'):
        outcomes=[]
        for client in (actual,reference):
            client.set('hll:debug',reference.get('hll:small'))
            with client.pipeline() as pipe:
                pipe.watch('hll:debug')
                client.execute_command('PFDEBUG',conversion,'hll:debug')
                pipe.multi();pipe.pfcount('hll:debug')
                try:outcomes.append(pipe.execute())
                except redis.WatchError:outcomes.append('aborted')
        assert outcomes[0]==outcomes[1], (conversion,outcomes)
        checks+=1
    # Redis trusts a valid cached count after header validation, even on invalid sparse data.
    cached=bytearray(b'HYLL'+bytes([1])+bytes(11));cached[8:16]=(123).to_bytes(8,'little')
    for client in (actual,reference):client.set('hll:cached',bytes(cached))
    compare('PFCOUNT','hll:cached');compare('PFADD','hll:cached')
    compare('PFCOUNT','hll:cached','hll:missing')
    # Cache refresh is observable through WATCH; a cache hit and duplicate add aren't.
    for mode in ('refresh','cached','duplicate','merge'):
        results=[]
        for client in (actual,reference):
            client.delete('hll:watch');client.pfadd('hll:watch','a')
            if mode=='cached':client.pfcount('hll:watch')
            with client.pipeline() as pipe:
                pipe.watch('hll:watch')
                if mode in ('refresh','cached'):client.pfcount('hll:watch')
                elif mode=='duplicate':client.pfadd('hll:watch','a')
                else:client.pfmerge('hll:watch','hll:watch')
                pipe.multi();pipe.pfcount('hll:watch')
                try:results.append(pipe.execute())
                except redis.WatchError:results.append('aborted')
        assert results[0]==results[1], (mode,results)
        checks+=1
    results=[]
    for client in (actual,reference):
        client.delete('hll:tx')
        with client.pipeline() as pipe:
            pipe.pfadd('hll:tx','a','b').pfcount('hll:tx').pfmerge('hll:tx','hll:small').pfcount('hll:tx')
            results.append(pipe.execute())
    assert results[0]==results[1], results
    checks+=1
    return checks
