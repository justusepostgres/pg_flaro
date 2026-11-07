"""Hash-field TTL command and expiration differential tests."""
import time
import redis

def check_hash_expiry(actual, reference):
    checks=0
    def compare(*command):
        nonlocal checks
        results=[]
        for client in (actual,reference):
            try:results.append(('ok',client.execute_command(*command)))
            except redis.ResponseError as error:results.append(('error',str(error)))
        assert results[0]==results[1],(command,results)
        checks+=1
        return results[0]
    for client in (actual,reference):
        client.delete('hfe:hash','hfe:bad')
        client.set('hfe:bad','string')
    compare('HSET','hfe:hash','a',1,'b',2,'plain',3)
    compare('PEXPIREAT','hfe:hash',4102444800000)
    for option in [(),('NX',),('XX',),('GT',),('LT',)]:
        compare('HPEXPIREAT','hfe:hash',4102444700123,*option,'FIELDS',4,'a','a','b','missing')
        compare('HPEXPIRETIME','hfe:hash','FIELDS',3,'a','b','plain')
        compare('HEXPIRETIME','hfe:hash','FIELDS',3,'a','b','plain')
    compare('HINCRBY','hfe:hash','a',1)
    compare('HINCRBYFLOAT','hfe:hash','a',.5)
    compare('HSETNX','hfe:hash','a','ignored')
    compare('HPEXPIRETIME','hfe:hash','FIELDS',1,'a')
    compare('HPERSIST','hfe:hash','FIELDS',4,'a','a','plain','missing')
    compare('HSET','hfe:hash','b','replacement')
    compare('HPEXPIRETIME','hfe:hash','FIELDS',2,'a','b')
    for name in ('HEXPIRE','HPEXPIRE','HEXPIREAT','HPEXPIREAT'):
        for key in ('hfe:hash','hfe:missing','hfe:bad'):
            for args in [(100,'FIELDS',2,'a','missing'),(-1,'FIELDS',1,'a'),('bad','FIELDS',1,'a'),
                         (2**63-1,'FIELDS',1,'a'),(100,'NX','XX','FIELDS',1,'a'),
                         (100,'FIELDS',0,'a'),(100,'FIELDS','bad','a'),(100,'FIELDS',2,'a'),
                         (100,'BAD',1,'a'),(100,'FIELDS',1,'a','extra')]:
                compare(name,key,*args)
        compare('HSET','hfe:hash','a',1)
    for name in ('HTTL','HPTTL','HEXPIRETIME','HPEXPIRETIME','HPERSIST'):
        for key in ('hfe:hash','hfe:missing','hfe:bad'):
            for args in [('FIELDS',2,'plain','missing'),('FIELDS',0,'plain'),('FIELDS','bad','plain'),
                         ('FIELDS',2,'plain'),('BAD',1,'plain')]:compare(name,key,*args)
    compare('HPEXPIREAT','hfe:hash',0,'FIELDS',3,'a','a','missing')
    compare('HGETALL','hfe:hash')
    compare('PEXPIRETIME','hfe:hash')
    # Expiry must affect field reads and the entire key when its last field disappears.
    for client in (actual,reference):
        client.hset('hfe:elapsed',mapping={'gone':'v','keep':'v'})
        client.hset('hfe:last','gone','v')
    deadline=int(time.time()*1000)+150
    compare('HPEXPIREAT','hfe:elapsed',deadline,'FIELDS',1,'gone')
    compare('HPEXPIREAT','hfe:last',deadline,'FIELDS',1,'gone')
    time.sleep(.25)
    for command in [('HGET','hfe:elapsed','gone'),('HMGET','hfe:elapsed','gone','keep'),
                    ('HSCAN','hfe:elapsed',0),('HRANDFIELD','hfe:elapsed'),('HLEN','hfe:elapsed'),('HKEYS','hfe:elapsed'),('HGETALL','hfe:elapsed'),
                    ('HPEXPIRETIME','hfe:elapsed','FIELDS',2,'gone','keep'),
                    ('TYPE','hfe:last'),('EXISTS','hfe:last'),('GET','hfe:last')]:compare(*command)
    # A live watched field expiring must abort even if other fields remain.
    outcomes=[]
    for client in (actual,reference):
        client.delete('hfe:watch');client.hset('hfe:watch',mapping={'gone':'v','keep':'v'})
        client.execute_command('HPEXPIRE','hfe:watch',100,'FIELDS',1,'gone')
        with client.pipeline() as pipe:
            pipe.watch('hfe:watch');time.sleep(.2)
            client.hget('hfe:watch','gone')
            pipe.multi();pipe.hget('hfe:watch','keep')
            try:outcomes.append(pipe.execute())
            except redis.WatchError:outcomes.append('aborted')
    assert outcomes==['aborted','aborted'],outcomes
    checks+=1
    compare('HSET','hfe:recovery','field',1,'plain',2)
    compare('HPEXPIREAT','hfe:recovery',4102444700123,'FIELDS',1,'field')
    for client in (actual,reference):
        now=int(time.time()*1000)
        client.execute_command('HPEXPIREAT','hfe:recovery',now+10001,'FIELDS',1,'field')
        assert 9000 <= client.execute_command('HPTTL','hfe:recovery','FIELDS',1,'field')[0] <=10001
        assert 9 <= client.execute_command('HTTL','hfe:recovery','FIELDS',1,'field')[0] <=11
        checks+=2
    compare('HPEXPIREAT','hfe:recovery',4102444700123,'FIELDS',1,'field')
    for client in (actual,reference):client.delete('hfe:ex')
    compare('HSETEX','hfe:ex','FIELDS',2,'a',1,'b',2)
    compare('PEXPIREAT','hfe:ex',4102444800000)
    for options in [(),('FNX',),('FXX',),('KEEPTTL',),('PXAT',4102444700123),('EXAT',4102444700),
                    ('FNX','KEEPTTL'),('FXX','PXAT',4102444700123)]:
        compare('HSETEX','hfe:ex',*options,'FIELDS',3,'a',3,'a',4,'new',5)
        compare('HGETALL','hfe:ex')
        compare('HPEXPIRETIME','hfe:ex','FIELDS',3,'a','b','new')
    for options in [(),('PERSIST',),('PXAT',4102444700123),('PXAT',0),('EX',0)]:
        compare('HSET','hfe:ex','a','value')
        compare('HGETEX','hfe:ex',*options,'FIELDS',4,'a','a','b','missing')
        compare('HPEXPIRETIME','hfe:ex','FIELDS',3,'a','b','new')
    compare('PEXPIRETIME','hfe:ex')
    for key in ('hfe:ex','hfe:missing','hfe:bad'):
        for args in [('FIELDS',0,'a','v'),('FIELDS','bad','a','v'),('FIELDS',2,'a','v'),
                     ('FIELDS',1,'a','v','extra'),('BAD','FIELDS',1,'a','v'),
                     ('FNX','FNX','FIELDS',1,'a','v'),('FNX','FXX','FIELDS',1,'a','v'),
                     ('EX',1,'PX',1,'FIELDS',1,'a','v'),('KEEPTTL','KEEPTTL','FIELDS',1,'a','v'),
                     ('EX',-1,'FIELDS',1,'a','v'),('EX','bad','FIELDS',1,'a','v'),
                     ('PX',2**63-1,'FIELDS',1,'a','v'),('FIELDS',2**31,'a','v')]:
            compare('HSETEX',key,*args)
        for args in [('FIELDS',0,'a'),('FIELDS','bad','a'),('FIELDS',2,'a'),('FIELDS',1,'a','extra'),
                     ('BAD',1,'a'),('EX',1,'PX',1,'FIELDS',1,'a'),('EX',-1,'FIELDS',1,'a'),
                     ('EX','bad','FIELDS',1,'a'),('PX',2**63-1,'FIELDS',1,'a'),('EX',1,'FIELDS')]:
            compare('HGETEX',key,*args)
    # Conditions inspect the pre-write state; duplicate newly created fields are valid FNX inputs.
    compare('DEL','hfe:conditional')
    compare('HSETEX','hfe:conditional','FNX','FIELDS',2,'a',1,'a',2)
    compare('HSETEX','hfe:conditional','FXX','FIELDS',2,'a',3,'missing',4)
    compare('HGETALL','hfe:conditional')
    for mutation in [('HGETEX','hfe:watch-ex','FIELDS',1,'a'),
                     ('HGETEX','hfe:watch-ex','PERSIST','FIELDS',1,'a'),
                     ('HGETEX','hfe:watch-ex','PXAT',4102444700123,'FIELDS',1,'a'),
                     ('HSETEX','hfe:watch-ex','FNX','FIELDS',1,'a','v'),
                     ('HSETEX','hfe:watch-ex','PXAT',0,'FIELDS',1,'missing','v')]:
        outcomes=[]
        for client in (actual,reference):
            client.delete('hfe:watch-ex')
            if mutation[-2]!='missing':client.hset('hfe:watch-ex','a','v')
            with client.pipeline() as pipe:
                pipe.watch('hfe:watch-ex');client.execute_command(*mutation)
                pipe.multi();pipe.exists('hfe:watch-ex')
                try:outcomes.append(pipe.execute())
                except redis.WatchError:outcomes.append('aborted')
        assert outcomes[0]==outcomes[1],(mutation,outcomes)
        checks+=1
    compare('HSETEX','hfe:ex-recovery','PXAT',4102444700123,'FIELDS',2,'a','one','b','two')
    compare('HGETEX','hfe:ex-recovery','PERSIST','FIELDS',1,'b')
    return checks
