"""Internal hash timestamp codec/recovery checks; expiry commands are not enabled yet."""
import struct
import redis
from cluster_integration import eventually


def check_hash_storage(shard, actual, journal, protocol):
    table='cache_journal.entries' if journal else 'cache_store.entries'
    key='hash-storage-fixture'
    deadline=4102444800000
    def flush():
        eventually(shard.committed)
        if journal: eventually(lambda: shard.sql('SELECT materialized=committed FROM cache_journal.state')=='t')
    def read():
        encoded=bytes.fromhex(shard.sql(f"SELECT encode(value,'hex') FROM {table} WHERE key=convert_to('{key}','UTF8')"))
        version=encoded[:4]
        count=struct.unpack_from('<I',encoded,4)[0]
        cursor=8
        def field():
            nonlocal cursor
            size=struct.unpack_from('<I',encoded,cursor)[0];cursor+=4
            result=encoded[cursor:cursor+size];cursor+=size
            return result
        fields={}
        for _ in range(count):
            name,value=field(),field()
            expiry=field() if version==b'COL2' else b''
            fields[name]=(value,struct.unpack('<q',expiry)[0] if expiry else None)
        assert cursor==len(encoded)
        return version,fields
    def restart():
        actual.close();shard.crash_restart()
        return redis.Redis(port=shard.port,socket_timeout=10,protocol=protocol)
    flush()
    pieces=[b'COL2',struct.pack('<I',3)]
    for item in (b'field\0',b'1',struct.pack('<q',deadline),b'plain',b'value\xff',b'',b'expired',b'gone',struct.pack('<q',1)):
        pieces.extend([struct.pack('<I',len(item)),item])
    payload=b''.join(pieces).hex()
    shard.sql(f"INSERT INTO {table}(key,value,data_type) VALUES(convert_to('{key}','UTF8'),decode('{payload}','hex'),1)")
    actual=restart()
    assert actual.hgetall(key)=={b'field\0':b'1',b'plain':b'value\xff'}
    assert actual.hincrby(key,b'field\0',1)==2
    flush()
    assert read()==(b'COL2',{b'field\0':(b'2',deadline),b'plain':(b'value\xff',None)})
    actual=restart()
    assert actual.hget(key,b'field\0')==b'2'
    assert actual.hincrbyfloat(key,b'field\0',.5)==2.5
    flush()
    assert read()[1][b'field\0']==(b'2.5',deadline)
    assert actual.hset(key,b'field\0',b'new')==0
    flush()
    assert read()==(b'COL1',{b'field\0':(b'new',None),b'plain':(b'value\xff',None)})
    actual=restart()
    assert actual.hgetall(key)=={b'field\0':b'new',b'plain':b'value\xff'}
    return actual
