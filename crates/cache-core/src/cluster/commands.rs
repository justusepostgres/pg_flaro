use super::{bulk, keyslot, number, Cluster, SLOTS};
use crate::{engine::Engine, now_ms, protocol::Reply, server::Sink};
use smallvec::SmallVec;
use std::io;

// One source of key positions for routing and Redis client COMMAND discovery.
#[derive(Clone, Copy)]
struct Spec {
    name: &'static str,
    arity: i64,
    first: usize,
    last: i64,
    step: usize,
    write: bool,
}
macro_rules! spec {
    ($name:literal, $arity:expr, $first:expr, $last:expr, $step:expr, $write:expr) => {
        Spec {
            name: $name,
            arity: $arity,
            first: $first,
            last: $last,
            step: $step,
            write: $write,
        }
    };
}
static SPECS: &[Spec] = &[
    spec!("type", 2, 1, 1, 1, false),
    spec!("get", 2, 1, 1, 1, false),
    spec!("set", -3, 1, 1, 1, true),
    spec!("mget", -2, 1, -1, 1, false),
    spec!("mset", -3, 1, -1, 2, true),
    spec!("del", -2, 1, -1, 1, true),
    spec!("exists", -2, 1, -1, 1, false),
    spec!("incr", 2, 1, 1, 1, true),
    spec!("incrby", 3, 1, 1, 1, true),
    spec!("setex", 4, 1, 1, 1, true),
    spec!("psetex", 4, 1, 1, 1, true),
    spec!("expire", 3, 1, 1, 1, true),
    spec!("pexpire", 3, 1, 1, 1, true),
    spec!("ttl", 2, 1, 1, 1, false),
    spec!("pttl", 2, 1, 1, 1, false),
    spec!("persist", 2, 1, 1, 1, true),
    spec!("ping", -1, 0, 0, 0, false),
    spec!("echo", 2, 0, 0, 0, false),
    spec!("quit", 1, 0, 0, 0, false),
    spec!("asking", 1, 0, 0, 0, false),
    spec!("cluster", -2, 0, 0, 0, false),
    spec!("command", -1, 0, 0, 0, false),
    spec!("pgcache.stats", 1, 0, 0, 0, false),
];
fn collection_specs() -> impl Iterator<Item = Spec> {
    crate::collections::COMMANDS
        .iter()
        .map(|&(name, arity, _, write, last)| Spec {
            name,
            arity,
            first: 1,
            last,
            step: 1,
            write,
        })
}
fn specs() -> impl Iterator<Item = Spec> {
    SPECS.iter().copied().chain(collection_specs())
}
fn spec(name: &[u8]) -> Option<Spec> {
    specs().find(|s| name.eq_ignore_ascii_case(s.name.as_bytes()))
}
pub(super) fn keys<'a>(args: &[&'a [u8]]) -> Option<SmallVec<[&'a [u8]; 4]>> {
    let s = spec(args.first()?)?;
    if s.first == 0
        || (s.arity > 0 && args.len() != s.arity as usize)
        || (s.arity < 0 && args.len() < (-s.arity) as usize)
        || (s.name == "mset" && args.len().is_multiple_of(2))
    {
        return None;
    }
    let last = if s.last < 0 {
        (args.len() as i64 + s.last) as usize
    } else {
        s.last as usize
    };
    Some((s.first..=last).step_by(s.step).map(|i| args[i]).collect())
}
fn description(s: &Spec) -> Reply {
    Reply::Array(vec![
        bulk(s.name),
        Reply::Integer(s.arity),
        Reply::Array(vec![bulk(if s.write { "write" } else { "readonly" })]),
        Reply::Integer(s.first as i64),
        Reply::Integer(s.last),
        Reply::Integer(s.step as i64),
    ])
}
pub(crate) fn command(args: &[&[u8]]) -> Reply {
    match args {
        [_] => Reply::Array(specs().map(|s| description(&s)).collect()),
        [_, sub] if sub.eq_ignore_ascii_case(b"COUNT") => Reply::Integer(specs().count() as i64),
        [_, sub, names @ ..] if sub.eq_ignore_ascii_case(b"INFO") => Reply::Array(
            names
                .iter()
                .map(|name| spec(name).map_or(Reply::Bulk(None), |s| description(&s)))
                .collect(),
        ),
        [_, sub, cmd @ ..] if sub.eq_ignore_ascii_case(b"GETKEYS") => keys(cmd).map_or(
            Reply::Error("ERR Invalid arguments or command has no key arguments"),
            |keys| Reply::Array(keys.into_iter().map(bulk).collect()),
        ),
        _ => Reply::Error("ERR unsupported COMMAND subcommand"),
    }
}
impl Cluster {
    pub(crate) fn execute(
        &mut self,
        args: &[&[u8]],
        engine: &mut Engine,
        sink: &mut Sink,
    ) -> io::Result<Option<Reply>> {
        if args[0].eq_ignore_ascii_case(b"PGCACHE.MIGRATION") && args.len() == 1 {
            return Ok(Some(self.topology.pending.as_ref().map_or(
                Reply::Bulk(None),
                |p| {
                    Reply::Array(vec![
                        Reply::Integer(p.slot as i64),
                        bulk(&p.key),
                        bulk(&self.topology.config.nodes[p.target as usize].id),
                    ])
                },
            )));
        }
        if args[0].eq_ignore_ascii_case(b"MIGRATE") {
            return self.migrate(args, engine, sink).map(Some);
        }
        if args[0].eq_ignore_ascii_case(b"PGCACHE.FINISH") {
            return self.finish(args, engine, sink).map(Some);
        }
        if args[0].eq_ignore_ascii_case(b"PGCACHE.IMPORT") {
            return self.import(args, engine, sink).map(Some);
        }
        if !args[0].eq_ignore_ascii_case(b"CLUSTER") {
            return Ok(None);
        }
        let reply = match args {
            [_, sub] if sub.eq_ignore_ascii_case(b"SHARDS") => self.topology.shards(),
            [_, sub] if sub.eq_ignore_ascii_case(b"SLOTS") => self.topology.slots(),
            [_, sub] if sub.eq_ignore_ascii_case(b"NODES") => self.topology.nodes(),
            [_, sub] if sub.eq_ignore_ascii_case(b"MYID") => bulk(&self.topology.config.local_id),
            [_, sub] if sub.eq_ignore_ascii_case(b"INFO") => bulk(format!("cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:{}\r\ncluster_size:{}\r\ncluster_current_epoch:{}\r\ncluster_my_epoch:{}\r\n", self.topology.config.nodes.len(), self.topology.config.nodes.iter().enumerate().filter(|(i,_)| self.topology.owners.contains(&(*i as u16))).count(), self.topology.epoch, self.topology.epoch)),
            [_, sub, key] if sub.eq_ignore_ascii_case(b"KEYSLOT") => Reply::Integer(keyslot(key) as i64),
            [_, sub, slot] if sub.eq_ignore_ascii_case(b"COUNTKEYSINSLOT") => match number(slot).filter(|s| (*s as usize) < SLOTS) {
                Some(slot) => Reply::Integer(engine.slot_count(slot, now_ms()) as i64),
                None => Reply::Error("ERR Invalid slot"),
            },
            [_, sub, slot, count] if sub.eq_ignore_ascii_case(b"GETKEYSINSLOT") => match (number(slot).filter(|s| (*s as usize) < SLOTS), number(count).filter(|n| *n <= 1024)) {
                (Some(slot), Some(count)) => {
                    match engine.slot_keys(slot, now_ms(), count as usize) {
                        Ok(keys) => Reply::Array(keys.into_iter().map(bulk).collect()),
                        Err(()) => Reply::Error("ERR reply too large; reduce count"),
                    }
                }

                _ => Reply::Error("ERR invalid slot or count (maximum 1024)"),
            },
            [_, sub, slot, mode, tail @ ..] if sub.eq_ignore_ascii_case(b"SETSLOT") => self.setslot(slot, mode, tail, engine, sink)?,
            _ => Reply::Error("ERR unsupported CLUSTER subcommand or wrong arity"),
        };
        Ok(Some(reply))
    }
    fn setslot(
        &mut self,
        slot: &[u8],
        mode: &[u8],
        tail: &[&[u8]],
        engine: &Engine,
        sink: &mut Sink,
    ) -> io::Result<Reply> {
        let Some(slot) = number(slot).filter(|s| (*s as usize) < SLOTS) else {
            return Ok(Reply::Error("ERR Invalid slot"));
        };
        if self
            .topology
            .pending
            .as_ref()
            .is_some_and(|p| p.slot == slot)
        {
            return Ok(Reply::Error(
                "TRYAGAIN Resume pending key transfer before changing slot state",
            ));
        }
        let mut next = self.topology.clone();
        let owner = next.owners[slot as usize];
        if mode.eq_ignore_ascii_case(b"STABLE") && tail.is_empty() {
            if next.migrating.contains_key(&slot) || next.importing.contains_key(&slot) {
                return Ok(Reply::Error("ERR active migrations must be resumed to completion; cancellation is unsupported"));
            }
            return Ok(Reply::Simple("OK"));
        }
        let [id] = tail else {
            return Ok(Reply::Error("ERR wrong number of arguments"));
        };
        let Some(peer) = next.node_index(id) else {
            return Ok(Reply::Error("ERR Unknown node"));
        };
        if mode.eq_ignore_ascii_case(b"MIGRATING") {
            if owner != next.local
                || peer == next.local
                || next.migrating.get(&slot).is_some_and(|p| *p != peer)
            {
                return Ok(Reply::Error(
                    "ERR slot is not locally owned or has another migration target",
                ));
            }
            next.migrating.insert(slot, peer);
        } else if mode.eq_ignore_ascii_case(b"IMPORTING") {
            if owner != peer || peer == next.local {
                return Ok(Reply::Error("ERR import source must own the slot"));
            }
            next.importing.insert(slot, peer);
        } else if mode.eq_ignore_ascii_case(b"NODE") {
            if owner == peer
                && !next.migrating.contains_key(&slot)
                && !next.importing.contains_key(&slot)
            {
                return Ok(Reply::Simple("OK"));
            }
            if owner == next.local
                && peer != next.local
                && (next.migrating.get(&slot) != Some(&peer)
                    || engine.slot_count(slot, now_ms()) != 0)
            {
                return Ok(Reply::Error(
                    "ERR migrate all slot keys before assigning the new owner",
                ));
            }
            if peer == next.local
                && owner != next.local
                && next.importing.get(&slot) != Some(&owner)
            {
                return Ok(Reply::Error(
                    "ERR slot must be importing from its current owner",
                ));
            }
            if next.importing.contains_key(&slot) && peer != next.local
                || next.migrating.contains_key(&slot) && peer == next.local
            {
                return Ok(Reply::Error("ERR cannot cancel an active migration"));
            }
            next.owners[slot as usize] = peer;
            next.migrating.remove(&slot);
            next.importing.remove(&slot);
        } else {
            return Ok(Reply::Error("ERR unsupported SETSLOT state"));
        }
        next.epoch = next
            .epoch
            .checked_add(1)
            .ok_or_else(|| io::Error::other("cluster epoch exhausted"))?;
        // Persist earlier key writes before publishing metadata which depends on them.
        sink.barrier(engine)?;
        self.persist(next)?;
        Ok(Reply::Simple("OK"))
    }
}
