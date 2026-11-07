#!/usr/bin/env python3
"""Generate categories, access flags and INFO fields from the pinned Redis catalog.

Usage: python3 scripts/generate_command_categories.py .local/redis-8.2.11/src/commands
These categories describe command semantics, not authentication enforcement.
"""
import json
from pathlib import Path
import re
import sys

root = Path(__file__).resolve().parents[1]
source = Path(sys.argv[1])
commands = (root/'crates/cache-core/src/cluster/commands.rs').read_text()
collections = (root/'crates/cache-core/src/collections/mod.rs').read_text()
names = set(re.findall(r'spec!\(\s*"([a-z0-9_.|]+)"', commands))
names.update(re.findall(r'\(\s*"([a-z0-9_]+)",\s*-?\d+,\s*\d+,\s*(?:true|false)', collections))
def legacy(specs):
    if not specs: return [0,0,0], False
    if len(specs)==1 and 'index' in specs[0]['begin_search'] and 'range' in specs[0]['find_keys']:
        spec=specs[0]; first=spec['begin_search']['index']['pos']; r=spec['find_keys']['range']
        return [first, r['lastkey'] if r['lastkey']<0 else first+r['lastkey'], r['step']], 'INCOMPLETE' in spec['flags']
    first=None; last=0; previous=0; movable=False
    for spec in specs:
        if 'index' not in spec['begin_search'] or 'range' not in spec['find_keys']:
            movable=True; continue
        start=spec['begin_search']['index']['pos']; r=spec['find_keys']['range']
        if r['step']!=1 or (previous and previous!=start-1):
            movable=True; continue
        movable |= 'INCOMPLETE' in spec['flags']
        first=start if first is None else min(first,start)
        end=r['lastkey'] if r['lastkey']<0 else start+r['lastkey']
        if (end & 0xffffffff) > (last & 0xffffffff): last=end
        previous=last
    return ([first,last,1],movable) if first is not None else ([0,0,0],True)

command_flag_order = ['WRITE','READONLY','DENYOOM','MODULE','ADMIN','PUBSUB','NOSCRIPT','BLOCKING','LOADING','STALE','SKIP_MONITOR','SKIP_SLOWLOG','ASKING','FAST','NO_AUTH','NO_MANDATORY_KEYS','NO_ASYNC_LOADING','NO_MULTI','MOVABLEKEYS','ALLOW_BUSY']
metadata = {}
key_metadata = {}
info_metadata = {}
for path in sorted(source.glob('*.json')):
    for name, definition in json.loads(path.read_text()).items():
        fullname = (definition.get('container', '') + '|' if 'container' in definition else '') + name
        fullname = fullname.lower()
        if fullname not in names: continue
        if definition.get("key_specs"):
            key_metadata[fullname] = definition["key_specs"]
        info_metadata[fullname] = {'tips': [tip.lower() for tip in definition.get('command_tips', [])], 'key_specs': definition.get('key_specs', [])}
        info_metadata[fullname]['documentation'] = {k: definition[k] for k in ['summary','since','group','complexity','doc_flags','deprecated_since','replaced_by','history','arguments'] if k in definition}
        info_metadata[fullname]['documentation']['group'] = definition['group'].replace('_', '-')
        positions, movable = legacy(definition.get('key_specs', []))
        public_flags = set(definition.get('command_flags', []))
        if movable: public_flags.add('MOVABLEKEYS')
        info_metadata[fullname].update(arity=definition['arity'], legacy=positions, flags=[flag.lower() for flag in command_flag_order if flag in public_flags])

        flags = set(definition.get('command_flags', []))
        cats = set(definition.get('acl_categories', []))
        if 'WRITE' in flags: cats.add('WRITE')
        if 'READONLY' in flags and 'SCRIPTING' not in cats: cats.add('READ')
        if 'ADMIN' in flags: cats.update(['ADMIN', 'DANGEROUS'])
        if 'PUBSUB' in flags: cats.add('PUBSUB')
        if 'BLOCKING' in flags: cats.add('BLOCKING')
        cats.add('FAST' if 'FAST' in flags else 'SLOW')
        metadata[fullname] = sorted(c.lower() for c in cats)
lines = ['//! Generated from Redis 8.2.11 command JSON and implicit ACL categories.',
         '//! Regenerate with scripts/generate_command_categories.py; descriptive only.',
         'pub(super) fn categories(name: &str) -> &\'static [&\'static str] {', '    match name {']
for name, cats in sorted(metadata.items()):
    lines.append(f'        {json.dumps(name)} => &[{", ".join(json.dumps(c) for c in cats)}],')
lines += ['        _ => &[],', '    }', '}', '']
(root/'crates/cache-core/src/cluster/command_categories.rs').write_text('\n'.join(lines))

order = ['RO','RW','OW','RM','ACCESS','UPDATE','INSERT','DELETE','NOT_KEY','INCOMPLETE','VARIABLE_FLAGS']
def rust_flags(flags):
    return '&[' + ', '.join(json.dumps(f if f in order[:4] else f.lower()) for f in order if f in flags) + ']'
lines = ['//! Generated Redis 8.2.11 static per-key access flags.',
         '//! Dynamic flags for SET/BITFIELD are handled in key_flags.rs.',
         'pub(super) fn flags(name: &str, ordinal: usize) -> &\'static [&\'static str] {', '    match name {']
for name, specs in sorted(key_metadata.items()):
    if name in ('set','bitfield'): continue
    assert all('VARIABLE_FLAGS' not in spec['flags'] for spec in specs), name
    if name in ('sort','georadius','georadiusbymember'):
        first, rest = specs[0]['flags'], specs[-1]['flags']
        if name.startswith('georadius'): rest = [flag for flag in rest if flag != 'INCOMPLETE']
    elif name == 'sort_ro':
        first = rest = specs[0]['flags']
    elif len(specs) == 1:
        first = rest = specs[0]['flags']
    else:
        assert len(specs) == 2, name
        assert specs[0]['find_keys']['range']['lastkey'] == 0, name
        first, rest = (spec['flags'] for spec in specs)
    expr = rust_flags(first) if first == rest else f'if ordinal == 0 {{ {rust_flags(first)} }} else {{ {rust_flags(rest)} }}'
    lines.append(f'        {json.dumps(name)} => {expr},')
lines += ['        _ => &[],', '    }', '}', '']
(root/'crates/cache-core/src/cluster/static_key_flags.rs').write_text('\n'.join(lines))

(root/'crates/cache-core/src/cluster/command_info.json').write_text('{\n' + ',\n'.join('  '+json.dumps(name)+': '+json.dumps(info, separators=(',', ':')) for name, info in sorted(info_metadata.items())) + '\n}\n')

help_source = (source.parent/'server.c').read_text().split('void commandHelpCommand(client *c) {',1)[1].split('NULL',1)[0]
help_lines = [json.loads(line.strip().rstrip(',')) for line in help_source.splitlines() if line.startswith('"')]
help_lines = ['COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:'] + help_lines + ['HELP','    Print this help.']
(root/'crates/cache-core/src/cluster/command_help.rs').write_text('//! Generated COMMAND HELP text from Redis 8.2.11.\npub(super) const HELP: &[&str] = &[\n' + ''.join('    '+json.dumps(line)+',\n' for line in help_lines) + '];\n')
