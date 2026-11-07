"""Native Redis Cluster oracle for transaction routing on disposable services."""
from contextlib import ExitStack
import subprocess
import time
import redis
from redis.crc import key_slot
from integration import Client, RedisError, free_port


def check_native_cluster_transactions(shards, work, redis_server, protocol=2):
    def eventually(fn):
        end=time.monotonic()+20
        while time.monotonic()<end:
            try:
                if fn():return
            except (OSError,redis.RedisError):pass
            time.sleep(.05)
        raise AssertionError('native Redis cluster did not become ready')
    with ExitStack() as stack:
        native=[];ports=[];bus=[]
        reserved=set()
        for _ in range(6):
            port=free_port()
            while port in reserved:port=free_port()
            reserved.add(port)
        allocated=iter(reserved)
        for i in range(3):
            path=work/f'native-redis-{i}';path.mkdir()
            port=next(allocated);cluster_port=next(allocated)
            ports.append(port);bus.append(cluster_port)
            log=stack.enter_context((path/'redis.log').open('w'))
            process=subprocess.Popen([str(redis_server),'--bind','127.0.0.1','--port',str(port),
                '--cluster-enabled','yes','--cluster-port',str(cluster_port),'--cluster-config-file','nodes.conf',
                '--cluster-node-timeout','1000','--save','','--appendonly','no','--dir',str(path)],stdout=log,stderr=subprocess.STDOUT)
            def stop(p=process):
                if p.poll() is None:p.terminate()
                p.wait(timeout=10)
            stack.callback(stop)
            client=redis.Redis(port=port,socket_timeout=5);stack.callback(client.close)
            eventually(client.ping);native.append(client)
        for i,client in enumerate(native):
            client.execute_command('CLUSTER','SET-CONFIG-EPOCH',i+1)
            first,last=[(0,5460),(5461,10922),(10923,16383)][i]
            client.execute_command('CLUSTER','ADDSLOTSRANGE',first,last)
        for i in (1,2):native[0].execute_command('CLUSTER','MEET','127.0.0.1',ports[i],bus[i])
        eventually(lambda:all(b'cluster_state:ok' in c.execute_command('CLUSTER','INFO') for c in native))
        ids=[c.execute_command('CLUSTER','MYID').decode() for c in native]
        ours=[Client(s.port) for s in shards];refs=[Client(p) for p in ports]
        for peer in ours+refs:
            stack.callback(peer.close)
            if protocol==3:assert peer.cmd('HELLO',3)[b'proto']==3
        counts=0
        def normalize(reply,ports):
            if isinstance(reply,RedisError):
                text=str(reply)
                if text.startswith(('MOVED ','ASK ')):
                    kind,slot,address=text.split()
                    return (kind,int(slot),ports.index(int(address.rsplit(':',1)[1])))
                return ('error',text)
            if isinstance(reply,list):return [normalize(v,ports) for v in reply]
            return reply
        def compare(index,*args):
            nonlocal counts
            actual=normalize(ours[index].cmd(*args),[s.port for s in shards])
            expected=normalize(refs[index].cmd(*args),ports)
            assert actual==expected,(index,args,actual,expected)
            counts+=1
            return actual
        other=next(f'native-high:{i}' for i in range(1000) if 10923<=key_slot(f'native-high:{i}'.encode()) and key_slot(f'native-high:{i}'.encode())!=12182)
        remote=next(f'native-mid:{i}' for i in range(1000) if 5461<=key_slot(f'native-mid:{i}'.encode())<=10922)
        for command in [
            ('MULTI',),('SET','{foo}native-tx','one'),('SET',other,'two'),('EXEC',),
            ('GET','{foo}native-tx'),('GET',other),('EXEC',),
            ('MULTI',),('GET','{foo}native-tx'),('GET',other),('SET','arity'),('EXEC',),
            ('MULTI',),('ASKING',),('EXEC',),
            ('MULTI',),('SET','{foo}native-tx','one'),('SET',remote,'two'),('EXEC',),
            ('GET','{foo}native-tx'),
            ('WATCH','{foo}native-tx'),('MULTI',),('GET','{foo}native-tx'),('EXEC',),
        ]:compare(2,*command)
        # Commands can queue successfully, then the slot starts migrating.
        compare(2,'SET','{foo}native-present','value')
        compare(2,'MULTI')
        compare(2,'GET','{foo}native-present')
        compare(2,'GET','{foo}native-missing')
        admin_ours=[Client(s.port) for s in shards]
        admin_refs=[Client(p) for p in ports]
        for peer in admin_ours+admin_refs:stack.callback(peer.close)
        for peers,nodeids in [(admin_ours,[s.id for s in shards]),(admin_refs,ids)]:
            assert peers[1].cmd('CLUSTER','SETSLOT',12182,'IMPORTING',nodeids[2])==b'OK'
            assert peers[2].cmd('CLUSTER','SETSLOT',12182,'MIGRATING',nodeids[1])==b'OK'
        compare(2,'EXEC')
        # ASKING survives MULTI and queued commands, then clears after EXEC.
        for command in [('ASKING',),('MULTI',),('SET','{foo}native-import','v'),
                        ('GET','{foo}native-import'),('EXEC',),('GET','{foo}native-import')]:
            compare(1,*command)
        for command in [('ASKING',),('MGET','{foo}native-import','{foo}native-missing'),
                        ('ASKING',),('MULTI',),('GET','{foo}native-import'),
                        ('GET','{foo}native-missing'),('EXEC',)]:
            compare(1,*command)
        return counts
