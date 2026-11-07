"""Geospatial storage and coordinate differential tests against Redis."""
import random
import redis


def check_geo(actual, reference):
    checks = 0
    def difference(first,second,path=()):
        if isinstance(first,(list,tuple)) and isinstance(second,(list,tuple)) and len(first)==len(second):
            for i,(x,y) in enumerate(zip(first,second)):
                if x!=y:return difference(x,y,path+(i,))
        return (path,first,second)
    def compare(*command):
        nonlocal checks
        results = []
        for client in (actual, reference):
            try: results.append(('ok', client.execute_command(*command)))
            except redis.ResponseError as error: results.append(('error', str(error)))
        assert results[0] == results[1], (command, difference(*results))
        checks += 1
        return results[0]
    for client in (actual, reference):
        client.delete('geo:points', 'geo:bad')
        client.set('geo:bad', 'wrong type')
    compare('GEOADD', 'geo:points', 13.361389, 38.115556, 'Palermo', 15.087269, 37.502669, 'Catania')
    compare('PEXPIREAT','geo:points',4102444800000)
    for options in [(), ('CH',), ('NX',), ('XX',), ('CH','XX'), ('NX','CH'), ('NX','XX'), ('CH','CH')]:
        compare('GEOADD','geo:points',*options,13.361389,38.115556,'Palermo',12,32,'new')
    for coords in [(0,0),(180,85.05112878),(-180,-85.05112878),(-0.0,0.0),(180,0),(0,85.05112878)]:
        compare('GEOADD','geo:points',*coords,'edge')
        compare('GEOPOS','geo:points','edge')
        compare('GEOHASH','geo:points','edge')
        compare('ZSCORE','geo:points','edge')
    for lon,lat in [(181,-0.0),(181,0),(0,86),(-181,0),(0,-86),('inf',0),('nan',0),('1e999',0),(' 1',0),('0x1p+1','0x1p+2')]:
        compare('GEOADD','geo:points',lon,lat,'invalid')
        compare('GEOADD','geo:bad',lon,lat,'invalid')
    rng=random.Random(72034)
    members=[]
    for index in range(150):
        member=str(index).encode()+b'\x00\xff'
        members.append(member)
        compare('GEOADD','geo:points',rng.uniform(-180,180),rng.uniform(-85,85),member)
    compare('ZRANGE','geo:points',0,-1,'WITHSCORES')
    compare('GEOPOS','geo:points',*members,'missing')
    compare('GEOHASH','geo:points',*members,'missing')
    for unit in ('m','km','ft','mi','KM','m\x00tail','invalid'):
        for first,second in zip(members[:30],members[30:60]):
            compare('GEODIST','geo:points',first,second,unit)
    for key in ('geo:points','geo:bad','geo:missing'):
        for command in [('GEOPOS',key),('GEOHASH',key),('GEOPOS',key,'missing'),('GEOHASH',key,'missing'),
                        ('GEODIST',key,'Palermo','Catania'),('GEODIST',key,'missing','Palermo'),
                        ('GEODIST',key,'a','b','invalid'),('GEODIST',key,'a','b','m','extra'),
                        ('GEOADD',key,1,2),('GEOADD',key,'NX','XX','CH'),('GEOADD',key,'CH','CH','CH')]:
            compare(*command)
    # Ordinary sorted-set scores are decoded as geo hashes too.
    for score in (-1,-2,-1e20,'inf','-inf',0,1,1.5,2**52,2**63):
        compare('ZADD','geo:forged',score,'member')
        compare('GEOPOS','geo:forged','member')
        compare('GEOHASH','geo:forged','member')
    compare('GEOADD','geo:points','CH\x00tail',12,32,'new')
    compare('PEXPIRETIME','geo:points')
    # Validate all coordinates before writing any members, including destination aliases.
    compare('GEOADD','geo:points',1,2,'atomic-first',999,2,'atomic-second')
    compare('ZSCORE','geo:points','atomic-first')
    results=[]
    for client in (actual,reference):
        with client.pipeline() as pipe:
            pipe.execute_command('GEOADD','geo:points',0,0,'transaction')
            pipe.execute_command('GEOPOS','geo:points','transaction')
            results.append(pipe.execute())
    assert results[0]==results[1],results
    checks+=1
    # Search ordering is defined when ASC/DESC (or COUNT without ANY) is used.
    for origin in [('FROMMEMBER','Palermo'),('FROMLONLAT',13.5,38.0)]:
        for shape in [('BYRADIUS',200,'km'),('BYBOX',400,300,'km'),('BYRADIUS',0,'m'),('BYRADIUS',20000,'km')]:
            for options in [('ASC',),('DESC',),('COUNT',2),('ASC','WITHDIST','WITHHASH','WITHCOORD')]:
                compare('GEOSEARCH','geo:points',*origin,*shape,*options)
            compare('GEOSEARCHSTORE','geo:stored','geo:points',*origin,*shape,'COUNT',4)
            compare('ZRANGE','geo:stored',0,-1,'WITHSCORES')
            compare('GEOSEARCHSTORE','geo:distances','geo:points',*origin,*shape,'DESC','STOREDIST')
            compare('ZRANGE','geo:distances',0,-1,'WITHSCORES')
    for key in ('geo:points','geo:bad','geo:missing'):
        for options in [
            ('FROMMEMBER','missing','BYRADIUS',100,'km'),
            ('FROMMEMBER','missing','BYRADIUS','bad','km'),
            ('FROMLONLAT',0,0,'BYRADIUS',-1,'m'),
            ('FROMLONLAT',0,0,'BYBOX','bad',1,'m'),
            ('FROMLONLAT',0,0,'BYBOX',1,-1,'m'),
            ('FROMLONLAT',0,0,'BYRADIUS',1,'bad'),
            ('FROMLONLAT',0,0,'BYRADIUS',1,'m','ANY'),
            ('FROMLONLAT',0,0,'BYRADIUS',1,'m','COUNT',0),
            ('FROMLONLAT',0,0,'BYRADIUS',1,'m','COUNT','bad'),
            ('FROMLONLAT',0,0,'BYRADIUS',1,'m','FROMMEMBER','Palermo'),
            ('FROMLONLAT',0,0,'BYRADIUS',1,'m','BYBOX',1,1,'m'),
            ('FROMLONLAT',0,0,'ASC','WITHDIST','COUNT',1),
            ('BYRADIUS',1,'m','ASC','WITHDIST','COUNT',1),
            ('FROMMEMBER','Palermo','FROMMEMBER','Catania','BYRADIUS',200,'km','ASC'),
        ]:
            compare('GEOSEARCH',key,*options)
            compare('GEOSEARCHSTORE','geo:errors',key,*options)
    compare('GEOSEARCHSTORE','geo:stored','geo:points','FROMMEMBER','Palermo','BYRADIUS',200,'km','WITHDIST')
    compare('SET','geo:stored','replace me')
    compare('PEXPIREAT','geo:stored',4102444800000)
    compare('GEOSEARCHSTORE','geo:stored','geo:points','FROMMEMBER','Palermo','BYRADIUS',200,'km')
    compare('PEXPIRETIME','geo:stored')
    compare('GEOSEARCHSTORE','geo:stored','geo:missing','FROMMEMBER','missing','BYBOX',100,100,'km')
    compare('EXISTS','geo:stored')
    # ANY is allowed to choose any matching members; validate its actual contract.
    for client in (actual,reference):
        all_members=client.execute_command('GEOSEARCH','geo:points','FROMLONLAT',0,0,'BYRADIUS',20000,'km')
        subset=client.execute_command('GEOSEARCH','geo:points','FROMLONLAT',0,0,'BYRADIUS',20000,'km','COUNT',3,'ANY')
        assert len(subset)==3 and len(set(subset))==3 and set(subset)<=set(all_members)
        checks+=1
    compare('COPY','geo:points','geo:alias','REPLACE')
    compare('GEOSEARCHSTORE','geo:alias','geo:alias','FROMMEMBER','Palermo','BYRADIUS',200,'km','STOREDIST')
    compare('ZRANGE','geo:alias',0,-1,'WITHSCORES')
    for name,origin in [('GEORADIUS',(13.5,38)),('GEORADIUS_RO',(13.5,38)),
                        ('GEORADIUSBYMEMBER',('Palermo',)),('GEORADIUSBYMEMBER_RO',('Palermo',))]:
        for suffix in [('ASC',),('DESC','COUNT',2),('WITHDIST','WITHHASH','WITHCOORD','ASC'),
                       ('COUNT',1),('ANY',),('COUNT',0),('COUNT','bad'),('BYBOX',1,1,'m'),
                       ('FROMMEMBER','Palermo'),('STORE','geo:radius'),('STOREDIST','geo:radius'),
                       ('STORE','geo:radius','WITHDIST'),('STORE','geo:ignored','STOREDIST','geo:radius'),
                       ('STOREDIST','geo:ignored','STORE','geo:radius'),('STORE',)]:
            compare(name,'geo:points',*origin,200,'km',*suffix)
        for key in ('geo:points','geo:missing','geo:bad'):
            for radius,unit in [('bad','invalid'),(-1,'m'),(100,'invalid'),(100,'km')]:
                compare(name,key,*origin,radius,unit,'ASC')
                compare(name,key,*origin,radius,unit,'STORE','geo:radius')
        compare(name,'geo:points',*origin,200,'km','ASC','COUNT',2)
    compare('GEORADIUSBYMEMBER','geo:points','missing','bad','invalid')
    compare('GEORADIUSBYMEMBER','geo:missing','missing','bad','invalid','STORE','geo:radius')
    compare('EXISTS','geo:radius')
    compare('GEORADIUS','geo:points',13.5,38,200,'km','STORE','geo:radius','STOREDIST','geo:radius-dist')
    compare('ZRANGE','geo:radius-dist',0,-1,'WITHSCORES')
    compare('GEOHASH','geo:ignored','Palermo')
    for name in ('GEORADIUS','GEORADIUSBYMEMBER'):
        origin=(13.5,38) if name=='GEORADIUS' else ('Palermo',)
        for suffix in [(),('STORE','d'),('STOREDIST','d'),('STORE','a','STOREDIST','b'),
                       ('STORE','a','STORE','geo:points'),('STORE',),('invalid','STORE','d')]:
            compare('COMMAND GETKEYS',name,'geo:points',*origin,200,'km',*suffix)
            compare('COMMAND GETKEYSANDFLAGS',name,'geo:points',*origin,200,'km',*suffix)
    # Indexed traversal must match the native cell order, including COUNT ANY.
    compare('GEOSEARCH','geo:points','FROMLONLAT',0,0,'BYRADIUS',20000,'km')
    compare('GEOSEARCH','geo:points','FROMLONLAT',0,0,'BYRADIUS',20000,'km','COUNT',3,'ANY')
    centers=[(0,0),(179.99,0),(-179.99,0),(10,66.01),(10,80.01),(10,85.04),(-10,-85.04)]
    for client in (actual,reference): client.delete('geo:bounds')
    rng=random.Random(48291)
    for index,(lon,lat) in enumerate(centers):
        batch=[]
        for member in range(40):
            x=(lon+rng.uniform(-.1,.1)+180)%360-180
            y=lat+rng.uniform(-.02,.02)
            if abs(y)>85.05112878:continue
            batch.extend([x,y,f'{index}:{member}'])
        compare('GEOADD','geo:bounds',*batch)
    for lon,lat in centers+[(180,85.05112878),(-180,-85.05112878)]:
        for radius in (0,.0001,100,10000,1000000,20000000):
            for options in [(),('COUNT',3,'ANY'),('ASC',),('DESC','COUNT',4)]:
                compare('GEORADIUS','geo:bounds',lon,lat,radius,'m',*options)
        for width,height in [(100,1000),(10000,10),(20000000,1000000)]:
            compare('GEOSEARCH','geo:bounds','FROMLONLAT',lon,lat,'BYBOX',width,height,'m')
            compare('GEOSEARCH','geo:bounds','FROMLONLAT',lon,lat,'BYBOX',width,height,'m','COUNT',3,'ANY')
    return checks
