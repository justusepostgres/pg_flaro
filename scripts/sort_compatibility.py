"""SORT checks against the pinned real Redis, shared by persistence/protocol modes."""
import itertools
import redis


def check_sort(actual, reference):
    checks=0
    def compare(*command, equal_alpha_weights=False):
        nonlocal checks
        replies=[]
        for client in (actual,reference):
            try:
                replies.append(client.execute_command(*command))
            except redis.ResponseError as error:
                replies.append(('error',str(error)))
        if equal_alpha_weights:
            # Redis does not tie-break equal ALPHA weights; internal set/zset
            # iteration order is not part of the contract. Preserve each GET pair.
            replies=[sorted(zip(r[::2],r[1::2])) for r in replies]
        assert replies[0]==replies[1],(command,replies)
        checks+=1
        return replies[0]
    for kind in ['list','set','zset']:
        key='sort:'+kind
        values=['3','1','2','02','-1','0']
        compare('DEL',key)
        if kind=='list': compare('RPUSH',key,*values)
        elif kind=='set': compare('SADD',key,*values)
        else: compare('ZADD',key,*itertools.chain.from_iterable((i,v) for i,v in enumerate(values)))
        for cmd,order,alpha,start,count in itertools.product(['SORT','SORT_RO'],['ASC','DESC'],[[],['ALPHA']],[-5,0,2,99],[-2,0,2,99]):
            compare(cmd,key,order,*alpha,'LIMIT',start,count)
        for cmd in ['SORT','SORT_RO']:
            for extra in [[],['GET','#'],['GET','fixed'],['GET','sort:missing:*'],['GET','#','GET','sort:missing:*']]:
                compare(cmd,key,*extra)
        compare('SET','sort:out','old','PX',100000)
        compare('SORT',key,'STORE','sort:out')
        compare('LRANGE','sort:out',0,-1)
        compare('PTTL','sort:out')
        compare('SORT',key,'GET','sort:missing:*','STORE','sort:out')
        compare('LRANGE','sort:out',0,-1)
        compare('SORT',key,'LIMIT',99,1,'STORE','sort:out')
        compare('EXISTS','sort:out')
        for rank,value in enumerate(values):
            compare('SET','sort:weight:'+value,str(rank))
            compare('HSET','sort:hash:'+value,'rank',str(rank),'label','v'+value)
        for by in ['sort:weight:*','sort:hash:*->rank','sort:missing:*']:
            for alpha in [[],['ALPHA']]:
                compare('SORT',key,'BY',by,*alpha,'GET','#','GET','sort:hash:*->label',equal_alpha_weights=bool(alpha) and by=='sort:missing:*')
        if kind!='set':
            for order in ['ASC','DESC']:
                compare('SORT',key,'BY','nosort',order,'LIMIT',1,3)
                compare('SORT',key,'BY','nosort','BY','sort:weight:*',order)
        compare('SORT',key,'BY','nosort','STORE','sort:out')
        compare('LRANGE','sort:out',0,-1)
    for value in [b'',b' 1',b'1 ',b'1\0junk',b'0x1.8p1',b'+1',b'+inf',b'inf',b'Infinity',b'-inf',b'nan',b'1e-320',b'1e-999',b'1e999',b'\xff']:
        compare('DEL','sort:edge')
        compare('RPUSH','sort:edge',value,b'0')
        compare('SORT','sort:edge')
        compare('SORT','sort:edge','LIMIT',0,0)
        compare('SORT','sort:edge','ALPHA')
    for key in ['sort:absent','sort:wrong']:
        if key.endswith('wrong'): compare('SET',key,'string')
        for extra in [[],['wat'],['LIMIT'],['LIMIT','x',2],['BY'],['GET'],['STORE'],['STORE','sort:out']]:
            # Generic existing wrong-arity messages differ; syntax/semantic errors must match.
            compare('SORT',key,*extra)
            compare('SORT_RO',key,*extra)
    compare('SORT','sort:list','STORE','sort:list')
    compare('LRANGE','sort:list',0,-1)
    compare('SORT','sort:absent','STORE','sort:out')
    compare('EXISTS','sort:out')
    return checks
