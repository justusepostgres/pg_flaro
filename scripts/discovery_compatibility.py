"""Compare command discovery for the advertised subset against Redis."""
from contextlib import ExitStack
from integration import Client, RedisError, frame


def check_discovery(actual, reference, protocol):
    checks = 0
    with ExitStack() as stack:
        peers = []
        for endpoint in (actual, reference):
            peer = Client(endpoint.connection_pool.connection_kwargs['port'])
            stack.callback(peer.close)
            if protocol == 3: peer.cmd('HELLO', 3)
            peers.append(peer)
        current, oracle = peers
        advertised = set(current.cmd('COMMAND','LIST'))
        known = set(oracle.cmd('COMMAND','LIST'))
        assert advertised - known == {b'pgcache.stats'}
        assert len(current.cmd('COMMAND')) == current.cmd('COMMAND','COUNT')
        assert current.cmd('COMMAND','INFO') == current.cmd('COMMAND')
        checks += 3
        for category in ['read','write','fast','slow','blocking','list','sortedset','hash','set','string','connection','transaction','dangerous','admin','keyspace','all','unknown','@read','READ']:
            ours = set(current.cmd('COMMAND','LIST','FILTERBY','ACLCAT',category))
            native = set(oracle.cmd('COMMAND','LIST','FILTERBY','ACLCAT',category))
            assert ours == native & advertised, (category, ours ^ (native & advertised))
            checks += 1
        for pattern in ['*','*pop*','client|*','command|*','cluster|*','?get*','[hz]*','[!a]*','*BLOCK*',r'\*','[a-z]*','[Z-a]*','[a-Z]*',r'[\A]*','[A-Z]*']:
            ours = set(current.cmd('COMMAND','LIST','FILTERBY','PATTERN',pattern))
            native = set(oracle.cmd('COMMAND','LIST','FILTERBY','PATTERN',pattern))
            assert (ours - {b'pgcache.stats'}) == native & advertised, (pattern, ours ^ (native & advertised))
            checks += 1
        assert current.cmd('COMMAND','LIST','FILTERBY','MODULE','unknown') == []
        checks += 1
        # Every advertised reference command has the same descriptive categories.
        for name in sorted(advertised & known):
            ours = current.cmd('COMMAND','INFO',name)[0]
            native = oracle.cmd('COMMAND','INFO',name)[0]
            if protocol == 3:
                assert isinstance(ours[2],set) and isinstance(ours[6],set)
            assert set(ours[6]) == set(native[6]), (name,ours[6],native[6])
            assert ours[:6] == native[:6], (name,ours[:6],native[:6])
            assert len(ours) == 10, (name,len(ours))
            assert ours[7:9] == native[7:9], (name,ours[7:9],native[7:9])
            children = {child[0]:child for child in ours[9]}
            expected_children = {child[0]:child for child in native[9] if child[0] in advertised}
            assert children.keys() == expected_children.keys(), (name,children.keys(),expected_children.keys())
            for child_name, child in children.items():
                assert child == current.cmd('COMMAND','INFO',child_name)[0]
                assert child[7:9] == expected_children[child_name][7:9]
            checks += 3

            checks += 1
        errors = [
            ('COMMAND','LIST','FILTERBY'), ('COMMAND','LIST','FILTERBY','bad','a'),
            ('COMMAND','LIST','bad','PATTERN','a'), ('COMMAND','GETKEYS'),
            ('COMMAND','GETKEYS','unknown'), ('COMMAND','GETKEYS','PING'), ('COMMAND','GETKEYS','CLIENT','unknown'),
            ('COMMAND','GETKEYS','GET'), ('COMMAND','GETKEYS','GET','a','extra'),
            ('COMMAND','GETKEYS','BLMPOP',0,0,'a','LEFT'),
        ]
        for command in errors:
            ours, native = [p.cmd(*command) for p in peers]
            assert isinstance(ours,RedisError) and isinstance(native,RedisError)
            assert str(ours) == str(native), (command,ours,native)
            checks += 1
        for command in [('MGET','a','b'), ('BLMOVE','a','b','LEFT','RIGHT',0), ('BZMPOP',0,2,'a','b','MAX'), ('SORT','a','STORE','b'), ('ZUNIONSTORE','dest',2,'a','b')]:
            ours, native = [p.cmd('COMMAND','GETKEYS',*command) for p in peers]
            assert ours == native, (command,ours,native)
            checks += 1
        def mapping(value):
            return value if isinstance(value,dict) else dict(zip(value[::2], value[1::2]))
        def arguments(value):
            result=[]
            for raw in value:
                arg=mapping(raw)
                if b'arguments' in arg: arg[b'arguments']=arguments(arg[b'arguments'])
                if b'flags' in arg: arg[b'flags']=set(arg[b'flags'])
                result.append(arg)
            return result
        def document(raw):
            doc=mapping(raw)
            if b'arguments' in doc: doc[b'arguments']=arguments(doc[b'arguments'])
            if b'doc_flags' in doc: doc[b'doc_flags']=set(doc[b'doc_flags'])
            if b'subcommands' in doc:
                doc[b'subcommands']={name:document(child) for name,child in mapping(doc[b'subcommands']).items() if name in advertised}
            return doc
        doc_mismatches = []
        for name in sorted(advertised & known):
            ours, native = [mapping(p.cmd('COMMAND','DOCS',name.upper())) for p in peers]
            assert ours.keys() == native.keys() == {name}, name
            left,right=document(ours[name]),document(native[name])
            if left != right: doc_mismatches.append((name,left,right))
            checks += 1
        assert not doc_mismatches, doc_mismatches
        ours, native = [mapping(p.cmd('COMMAND','DOCS')) for p in peers]
        assert set(ours) == {name for name in advertised if b'|' not in name}
        assert b'summary' in document(ours[b'pgcache.stats'])
        for name in set(ours) & set(native):
            assert document(ours[name]) == document(native[name]), name
        checks += 1
        for names in [('GET','GET','unknown'), ('unknown',), ('command|docs','client|id')]:
            ours, native = [p.cmd('COMMAND','DOCS',*names) for p in peers]
            assert ours == native, (names,ours,native)
            checks += 1
        assert current.cmd('COMMAND','HELP') == oracle.cmd('COMMAND','HELP')
        checks += 1
        for command in [('COMMAND','HELP','extra'), ('COMMAND','COUNT','extra'), ('COMMAND','unknown')]:
            ours,native=[p.cmd(*command) for p in peers]
            assert isinstance(ours,RedisError) and isinstance(native,RedisError) and str(ours)==str(native), (command,ours,native)
            checks += 1
        # Preserve Redis's simple-string flag encoding, not just decoded values.
        for peer in peers:
            peer.socket.sendall(frame('COMMAND','GETKEYSANDFLAGS','GET','a'))
            assert peer.file.readline() == b'*1\r\n'
            assert peer.file.readline() == b'*2\r\n'
            assert peer.file.readline() == b'$1\r\n'
            assert peer.file.readline() == b'a\r\n'
            assert peer.file.readline() == (b'~2\r\n' if protocol==3 else b'*2\r\n')
            assert peer.file.readline() == b'+RO\r\n'
            assert peer.file.readline() == b'+access\r\n'
        checks += 1
        candidates = []
        for name in sorted(advertised & known):
            if b'|' in name: continue
            info = oracle.cmd('COMMAND','INFO',name)[0]
            if not info[8]: continue
            candidates.append([name] + [b'1'] * (abs(info[1])-1))
        candidates += [
            ['SET','a','v','GET'], ['SET','a','v','EX','GET'], ['SET','a','v','GET\0tail'],
            ['BITFIELD','a'], ['BITFIELD','a','GET','i8',0],
            ['BITFIELD','a','GET'], ['BITFIELD','a','OVERFLOW','FAIL','GET','i8',0],
            ['BITFIELD','a','GET','i8',0,'SET','i8',0,1],
            ['BITFIELD','a','SET','i8',0,1], ['BITFIELD','a','bad'],
            ['LMOVE','same','same','LEFT','RIGHT'], ['SMOVE','same','same','m'],
            ['COPY','a','b'], ['RENAME','a','a'], ['RENAMENX','a','b'],
            ['MGET','a','a','b'], ['MSET','a',1,'a',2],
            ['SORT','a','BY','weight:*','GET','value:*','STORE','a'],
            ['ZUNIONSTORE','a',2,'a','b','WEIGHTS',1,2],
            ['BITOP','AND','a','a','b'], ['SINTERSTORE','a','a','b'],
        ]
        for command in candidates:
            ours, native = [p.cmd('COMMAND','GETKEYSANDFLAGS',*command) for p in peers]
            if isinstance(ours,RedisError) or isinstance(native,RedisError):
                assert isinstance(ours,RedisError) and isinstance(native,RedisError) and str(ours)==str(native), (command,ours,native)
            else:
                assert ours == native, (command,ours,native)
            checks += 1
        for command in errors:
            if command[1] != 'GETKEYS': continue
            changed = ('COMMAND','GETKEYSANDFLAGS',*command[2:])
            ours, native = [p.cmd(*changed) for p in peers]
            assert isinstance(ours,RedisError) and isinstance(native,RedisError) and str(ours)==str(native), (changed,ours,native)
            checks += 1
        # The native negative QUIT arity is behavioral, not just documentation:
        # extra arguments are ignored, including during a transaction.
        for in_transaction in (False, True):
            for tail in ((), (b'ignored', b'\x00extra')):
                for endpoint, observer in zip((actual, reference), peers):
                    quitter = Client(endpoint.connection_pool.connection_kwargs['port'])
                    try:
                        if protocol == 3: quitter.cmd('HELLO',3)
                        observer.cmd('DEL','discovery:quit')
                        if in_transaction:
                            quitter.cmd('MULTI')
                            assert quitter.cmd('SET','discovery:quit','uncommitted') == b'QUEUED'
                        assert quitter.cmd('QUIT',*tail) == b'OK'
                        assert quitter.file.read(1) == b''
                        assert observer.cmd('GET','discovery:quit') is None
                    finally:
                        quitter.close()
                checks += 1
        for peer in peers:
            peer.cmd('MULTI')
            assert peer.cmd('COMMAND','LIST','FILTERBY','MODULE','missing') == b'QUEUED'
            assert peer.cmd('COMMAND','GETKEYSANDFLAGS','SET','a','v','GET') == b'QUEUED'
            expected_flags = {b'RW',b'access',b'update'} if protocol==3 else [b'RW',b'access',b'update']
            assert peer.cmd('COMMAND','DOCS','unknown') == b'QUEUED'
            assert peer.cmd('EXEC') == [[], [[b'a',expected_flags]], {} if protocol==3 else []]
        checks += 1
    return checks
