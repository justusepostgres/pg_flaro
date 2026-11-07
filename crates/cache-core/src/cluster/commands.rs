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
    spec!("restore", -4, 1, 1, 1, true),
    spec!("dump", 2, 1, 1, 1, false),
    spec!("unlink", -2, 1, -1, 1, true),
    spec!("randomkey", 1, 0, 0, 0, false),
    spec!("hgetex", -5, 1, 1, 1, true),
    spec!("hsetex", -6, 1, 1, 1, true),
    spec!("hexpire", -6, 1, 1, 1, true),
    spec!("hpexpire", -6, 1, 1, 1, true),
    spec!("hexpireat", -6, 1, 1, 1, true),
    spec!("hpexpireat", -6, 1, 1, 1, true),
    spec!("httl", -5, 1, 1, 1, false),
    spec!("hpttl", -5, 1, 1, 1, false),
    spec!("hexpiretime", -5, 1, 1, 1, false),
    spec!("hpexpiretime", -5, 1, 1, 1, false),
    spec!("hpersist", -5, 1, 1, 1, true),
    spec!("georadius", -6, 1, 1, 1, true),
    spec!("georadius_ro", -6, 1, 1, 1, false),
    spec!("georadiusbymember", -5, 1, 1, 1, true),
    spec!("georadiusbymember_ro", -5, 1, 1, 1, false),
    spec!("geosearch", -7, 1, 1, 1, false),
    spec!("geosearchstore", -8, 1, 2, 1, true),
    spec!("geoadd", -5, 1, 1, 1, true),
    spec!("geopos", -2, 1, 1, 1, false),
    spec!("geohash", -2, 1, 1, 1, false),
    spec!("geodist", -4, 1, 1, 1, false),
    spec!("blmove", 6, 1, 2, 1, true),
    spec!("brpoplpush", 4, 1, 2, 1, true),
    spec!("blmpop", -5, 0, 0, 0, true),
    spec!("bzmpop", -5, 0, 0, 0, true),
    spec!("bzpopmin", -3, 1, -2, 1, true),
    spec!("bzpopmax", -3, 1, -2, 1, true),
    spec!("pfadd", -2, 1, 1, 1, true),
    spec!("pfcount", -2, 1, -1, 1, false),
    spec!("pfselftest", 1, 0, 0, 0, false),
    spec!("pfdebug", 3, 2, 2, 1, true),
    spec!("pfmerge", -2, 1, -1, 1, true),
    spec!("blpop", -3, 1, -2, 1, true),
    spec!("brpop", -3, 1, -2, 1, true),
    spec!("asking", 1, 0, 0, 0, false),
    spec!("watch", -2, 1, -1, 1, false),
    spec!("unwatch", 1, 0, 0, 0, false),
    spec!("multi", 1, 0, 0, 0, false),
    spec!("exec", 1, 0, 0, 0, false),
    spec!("discard", 1, 0, 0, 0, false),
    spec!("sort", -2, 1, 1, 1, true),
    spec!("sort_ro", -2, 1, 1, 1, false),
    spec!("hello", -1, 0, 0, 0, false),
    spec!("reset", 1, 0, 0, 0, false),
    spec!("time", 1, 0, 0, 0, false),
    spec!("client", -2, 0, 0, 0, false),
    spec!("incrbyfloat", 3, 1, 1, 1, true),
    spec!("keys", 2, 0, 0, 0, false),
    spec!("dbsize", 1, 0, 0, 0, false),
    spec!("touch", -2, 1, -1, 1, false),
    spec!("scan", -2, 0, 0, 0, false),
    spec!("hscan", -3, 1, 1, 1, false),
    spec!("sscan", -3, 1, 1, 1, false),
    spec!("zscan", -3, 1, 1, 1, false),
    spec!("lcs", -3, 1, 2, 1, false),
    spec!("getbit", 3, 1, 1, 1, false),
    spec!("setbit", 4, 1, 1, 1, true),
    spec!("bitfield", -2, 1, 1, 1, true),
    spec!("bitfield_ro", -2, 1, 1, 1, false),
    spec!("bitop", -4, 2, -1, 1, true),
    spec!("bitpos", -3, 1, 1, 1, false),
    spec!("bitcount", -2, 1, 1, 1, false),
    spec!("copy", -3, 1, 2, 1, true),
    spec!("rename", 3, 1, 2, 1, true),
    spec!("renamenx", 3, 1, 2, 1, true),
    spec!("type", 2, 1, 1, 1, false),
    spec!("getex", -2, 1, 1, 1, true),
    spec!("getdel", 2, 1, 1, 1, true),
    spec!("getset", 3, 1, 1, 1, true),
    spec!("setnx", 3, 1, 1, 1, true),
    spec!("msetnx", -3, 1, -1, 2, true),
    spec!("getrange", 4, 1, 1, 1, false),
    spec!("substr", 4, 1, 1, 1, false),
    spec!("setrange", 4, 1, 1, 1, true),
    spec!("get", 2, 1, 1, 1, false),
    spec!("set", -3, 1, 1, 1, true),
    spec!("mget", -2, 1, -1, 1, false),
    spec!("mset", -3, 1, -1, 2, true),
    spec!("del", -2, 1, -1, 1, true),
    spec!("exists", -2, 1, -1, 1, false),
    spec!("append", 3, 1, 1, 1, true),
    spec!("strlen", 2, 1, 1, 1, false),
    spec!("decr", 2, 1, 1, 1, true),
    spec!("decrby", 3, 1, 1, 1, true),
    spec!("incr", 2, 1, 1, 1, true),
    spec!("incrby", 3, 1, 1, 1, true),
    spec!("setex", 4, 1, 1, 1, true),
    spec!("psetex", 4, 1, 1, 1, true),
    spec!("expire", -3, 1, 1, 1, true),
    spec!("pexpire", -3, 1, 1, 1, true),
    spec!("expireat", -3, 1, 1, 1, true),
    spec!("pexpireat", -3, 1, 1, 1, true),
    spec!("expiretime", 2, 1, 1, 1, false),
    spec!("pexpiretime", 2, 1, 1, 1, false),
    spec!("ttl", 2, 1, 1, 1, false),
    spec!("pttl", 2, 1, 1, 1, false),
    spec!("persist", 2, 1, 1, 1, true),
    spec!("ping", -1, 0, 0, 0, false),
    spec!("echo", 2, 0, 0, 0, false),
    spec!("quit", -1, 0, 0, 0, false),
    spec!("asking", 1, 0, 0, 0, false),
    spec!("cluster", -2, 0, 0, 0, false),
    spec!("command", -1, 0, 0, 0, false),
    spec!("pgcache.stats", 1, 0, 0, 0, false),
];
const SUBCOMMANDS: &[Spec] = &[
    spec!("client|id", 2, 0, 0, 0, false),
    spec!("client|getname", 2, 0, 0, 0, false),
    spec!("client|setname", 3, 0, 0, 0, false),
    spec!("client|setinfo", 4, 0, 0, 0, false),
    spec!("client|unblock", -3, 0, 0, 0, false),
    spec!("command|docs", -2, 0, 0, 0, false),
    spec!("command|help", 2, 0, 0, 0, false),
    spec!("command|count", 2, 0, 0, 0, false),
    spec!("command|info", -2, 0, 0, 0, false),
    spec!("command|list", -2, 0, 0, 0, false),
    spec!("command|getkeysandflags", -3, 0, 0, 0, false),
    spec!("command|getkeys", -3, 0, 0, 0, false),
    spec!("cluster|shards", 2, 0, 0, 0, false),
    spec!("cluster|slots", 2, 0, 0, 0, false),
    spec!("cluster|nodes", 2, 0, 0, 0, false),
    spec!("cluster|myid", 2, 0, 0, 0, false),
    spec!("cluster|info", 2, 0, 0, 0, false),
    spec!("cluster|keyslot", 3, 0, 0, 0, false),
    spec!("cluster|countkeysinslot", 3, 0, 0, 0, false),
    spec!("cluster|getkeysinslot", 4, 0, 0, 0, false),
    spec!("cluster|setslot", -4, 0, 0, 0, false),
];
fn listed_specs() -> impl Iterator<Item = Spec> {
    specs().chain(SUBCOMMANDS.iter().copied())
}
fn collection_specs() -> impl Iterator<Item = Spec> {
    crate::collections::COMMANDS
        .iter()
        .map(|&(name, arity, _, write, last)| Spec {
            name,
            arity,
            first: if matches!(
                name,
                "sintercard"
                    | "lmpop"
                    | "zmpop"
                    | "zdiff"
                    | "zdiffstore"
                    | "zunion"
                    | "zunionstore"
                    | "zinter"
                    | "zinterstore"
                    | "zintercard"
            ) {
                0
            } else {
                1
            },
            last,
            step: if matches!(
                name,
                "sintercard"
                    | "lmpop"
                    | "zmpop"
                    | "zdiff"
                    | "zdiffstore"
                    | "zunion"
                    | "zunionstore"
                    | "zinter"
                    | "zinterstore"
                    | "zintercard"
            ) {
                0
            } else {
                1
            },
            write,
        })
}
fn specs() -> impl Iterator<Item = Spec> {
    SPECS.iter().copied().chain(collection_specs())
}
fn spec(name: &[u8]) -> Option<Spec> {
    specs().find(|s| name.eq_ignore_ascii_case(s.name.as_bytes()))
}
pub(crate) fn validate_command(args: &[&[u8]]) -> Result<(), Reply> {
    let name = args.first().copied().unwrap_or_default();
    let Some(s) = spec(name) else {
        let name = String::from_utf8_lossy(name).replace(['\r', '\n'], " ");
        return Err(Reply::OwnedError(format!("ERR unknown command '{name}'")));
    };
    if (s.arity > 0 && args.len() != s.arity as usize)
        || (s.arity < 0 && args.len() < (-s.arity) as usize)
    {
        return Err(Reply::OwnedError(format!(
            "ERR wrong number of arguments for '{}' command",
            s.name
        )));
    }
    if (s.name == "client" || s.name == "command") && args.len() > 1 {
        let subcommands: &[(&str, i64)] = if s.name == "client" {
            &[
                ("id", 2),
                ("getname", 2),
                ("setname", 3),
                ("setinfo", 4),
                ("unblock", -3),
            ]
        } else {
            &[
                ("docs", -2),
                ("help", 2),
                ("count", 2),
                ("info", -2),
                ("list", -2),
                ("getkeys", -3),
                ("getkeysandflags", -3),
            ]
        };
        let Some((sub, arity)) = subcommands
            .iter()
            .find(|(sub, _)| args[1].eq_ignore_ascii_case(sub.as_bytes()))
        else {
            let sub = String::from_utf8_lossy(args[1]).replace(['\r', '\n'], " ");
            return Err(Reply::OwnedError(format!(
                "ERR unknown subcommand '{sub}'. Try {} HELP.",
                s.name.to_ascii_uppercase()
            )));
        };
        if (*arity > 0 && args.len() != *arity as usize)
            || (*arity < 0 && args.len() < (-*arity) as usize)
        {
            return Err(Reply::OwnedError(format!(
                "ERR wrong number of arguments for '{}|{sub}' command",
                s.name
            )));
        }
    }
    Ok(())
}
pub(crate) fn command_keys<'a>(args: &[&'a [u8]]) -> SmallVec<[&'a [u8]; 4]> {
    keys(args).unwrap_or_default()
}
pub(super) fn keys<'a>(args: &[&'a [u8]]) -> Option<SmallVec<[&'a [u8]; 4]>> {
    let s = spec(args.first()?)?;
    if matches!(s.name, "georadius" | "georadiusbymember") {
        let mut keys = SmallVec::new();
        keys.push(*args.get(1)?);
        let mut destination = None;
        let mut i = 5;
        while i + 1 < args.len() {
            let arg = args[i].split(|b| *b == 0).next().unwrap();
            if arg.eq_ignore_ascii_case(b"STORE") || arg.eq_ignore_ascii_case(b"STOREDIST") {
                destination = Some(args[i + 1]);
                i += 1;
            }
            i += 1;
        }
        if let Some(destination) = destination {
            keys.push(destination);
        }
        return Some(keys);
    }
    if matches!(s.name, "blmpop" | "bzmpop") {
        let count = crate::collections::zset_algebra::key_count(args, 2)?;
        return Some(args[3..3 + count].iter().copied().collect());
    }
    if s.name == "sort" || s.name == "sort_ro" {
        let o = crate::engine::sort::options(args).ok()?;
        let mut keys = SmallVec::new();
        keys.push(args[1]);
        if let Some(destination) = o.store {
            keys.push(destination);
        }
        return Some(keys);
    }
    if matches!(
        s.name,
        "zdiff" | "zdiffstore" | "zunion" | "zunionstore" | "zinter" | "zinterstore" | "zintercard"
    ) {
        let index = if s.name.ends_with("store") { 2 } else { 1 };
        let count = crate::collections::zset_algebra::key_count(args, index)?;
        let mut keys = SmallVec::new();
        if index == 2 {
            keys.push(args[1]);
        }
        keys.extend_from_slice(&args[index + 1..index + 1 + count]);
        return Some(keys);
    }
    if matches!(
        s.name,
        "sintercard"
            | "lmpop"
            | "zmpop"
            | "zdiff"
            | "zdiffstore"
            | "zunion"
            | "zunionstore"
            | "zinter"
            | "zinterstore"
            | "zintercard"
    ) {
        let count = crate::collections::set_card::key_count(args)?;
        return Some(args[2..2 + count].iter().copied().collect());
    }
    if s.first == 0
        || (s.arity > 0 && args.len() != s.arity as usize)
        || (s.arity < 0 && args.len() < (-s.arity) as usize)
        || (matches!(s.name, "mset" | "msetnx") && args.len().is_multiple_of(2))
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
    let (arity, legacy, flags) = super::command_info::header(s.name).unwrap_or_else(|| {
        (
            s.arity,
            [s.first as i64, s.last, s.step as i64],
            Reply::Set(vec![bulk(if s.write { "write" } else { "readonly" })]),
        )
    });
    Reply::Array(vec![
        bulk(s.name),
        Reply::Integer(arity),
        flags,
        Reply::Integer(legacy[0]),
        Reply::Integer(legacy[1]),
        Reply::Integer(legacy[2]),
        // Categories describe commands; they do not imply ACL enforcement.
        Reply::Set(
            super::command_categories::categories(s.name)
                .iter()
                .map(|c| Reply::OwnedSimple(format!("@{c}")))
                .collect(),
        ),
        super::command_info::tips(s.name),
        super::command_info::key_specs(s.name),
        {
            let prefix = format!("{}|", s.name);
            let subcommands: Vec<_> = SUBCOMMANDS
                .iter()
                .filter(|sub| sub.name.starts_with(&prefix))
                .map(description)
                .collect();
            if subcommands.is_empty() {
                Reply::Set(vec![])
            } else {
                Reply::Array(subcommands)
            }
        },
    ])
}
fn documentation(s: &Spec) -> Reply {
    let mut fields = super::command_docs::fields(s.name);
    let prefix = format!("{}|", s.name);
    let children: Vec<_> = SUBCOMMANDS
        .iter()
        .filter(|sub| sub.name.starts_with(&prefix))
        .map(|sub| (bulk(sub.name), documentation(sub)))
        .collect();
    if !children.is_empty() {
        fields.push((bulk("subcommands"), Reply::Map(children)));
    }
    Reply::Map(fields)
}
pub(crate) fn command(args: &[&[u8]]) -> Reply {
    if let Err(error) = validate_command(args) {
        return error;
    }
    match args {
        [_, sub] if sub.eq_ignore_ascii_case(b"HELP") => Reply::Array(
            super::command_help::HELP
                .iter()
                .map(|text| Reply::Simple(text))
                .collect(),
        ),
        [_, sub, names @ ..] if sub.eq_ignore_ascii_case(b"DOCS") => {
            if names.is_empty() {
                Reply::Map(specs().map(|s| (bulk(s.name), documentation(&s))).collect())
            } else {
                Reply::Map(
                    names
                        .iter()
                        .filter_map(|name| {
                            listed_specs().find(|s| name.eq_ignore_ascii_case(s.name.as_bytes()))
                        })
                        .map(|s| (bulk(s.name), documentation(&s)))
                        .collect(),
                )
            }
        }
        [_] => Reply::Array(specs().map(|s| description(&s)).collect()),
        [_, sub] if sub.eq_ignore_ascii_case(b"INFO") => {
            Reply::Array(specs().map(|s| description(&s)).collect())
        }
        [_, sub, options @ ..] if sub.eq_ignore_ascii_case(b"LIST") => list(options),
        [_, sub] if sub.eq_ignore_ascii_case(b"COUNT") => Reply::Integer(specs().count() as i64),
        [_, sub, names @ ..] if sub.eq_ignore_ascii_case(b"INFO") => Reply::Array(
            names
                .iter()
                .map(|name| {
                    listed_specs()
                        .find(|s| name.eq_ignore_ascii_case(s.name.as_bytes()))
                        .map_or(Reply::Bulk(None), |s| description(&s))
                })
                .collect(),
        ),
        [_, sub, cmd @ ..]
            if sub.eq_ignore_ascii_case(b"GETKEYS")
                || sub.eq_ignore_ascii_case(b"GETKEYSANDFLAGS") =>
        {
            if cmd.is_empty() {
                return Reply::OwnedError(format!(
                    "ERR wrong number of arguments for 'command|{}' command",
                    String::from_utf8_lossy(sub).to_ascii_lowercase()
                ));
            }
            let Some(s) = spec(cmd[0]) else {
                return Reply::Error("ERR Invalid command specified");
            };
            if matches!(s.name, "client" | "command" | "cluster") && cmd.len() > 1 {
                let full = format!(
                    "{}|{}",
                    s.name,
                    String::from_utf8_lossy(cmd[1]).to_ascii_lowercase()
                );
                if !SUBCOMMANDS.iter().any(|sub| sub.name == full) {
                    return Reply::Error("ERR Invalid command specified");
                }
            }
            if s.first == 0
                && !matches!(
                    s.name,
                    "sort"
                        | "sort_ro"
                        | "sintercard"
                        | "lmpop"
                        | "zmpop"
                        | "blmpop"
                        | "bzmpop"
                        | "zdiff"
                        | "zdiffstore"
                        | "zunion"
                        | "zunionstore"
                        | "zinter"
                        | "zinterstore"
                        | "zintercard"
                )
            {
                return Reply::Error("ERR The command has no key arguments");
            }
            if (s.arity > 0 && cmd.len() != s.arity as usize)
                || (s.arity < 0 && cmd.len() < (-s.arity) as usize)
            {
                return Reply::Error("ERR Invalid number of arguments specified for command");
            }
            // Redis executes destination-only PFMERGE, but its key-spec discovery
            // rejects that form because the second spec requires a source key.
            if s.name == "pfmerge" && cmd.len() == 2 {
                return Reply::Error("ERR Invalid arguments specified for command");
            }
            keys(cmd).map_or(
                Reply::Error("ERR Invalid arguments specified for command"),
                |keys| {
                    Reply::Array(
                        keys.into_iter()
                            .enumerate()
                            .map(|(ordinal, key)| {
                                if sub.eq_ignore_ascii_case(b"GETKEYSANDFLAGS") {
                                    Reply::Array(vec![
                                        bulk(key),
                                        Reply::Array(
                                            super::key_flags::flags(s.name, cmd, ordinal)
                                                .iter()
                                                .map(|flag| Reply::Simple(flag))
                                                .collect(),
                                        ),
                                    ])
                                } else {
                                    bulk(key)
                                }
                            })
                            .collect(),
                    )
                },
            )
        }
        _ => Reply::Error("ERR unsupported COMMAND subcommand"),
    }
}
fn list(options: &[&[u8]]) -> Reply {
    if options.is_empty() {
        return Reply::Array(listed_specs().map(|s| bulk(s.name)).collect());
    }
    let [filterby, kind, arg] = options else {
        return Reply::Error("ERR syntax error");
    };
    if !filterby.eq_ignore_ascii_case(b"FILTERBY") {
        return Reply::Error("ERR syntax error");
    }
    if kind.eq_ignore_ascii_case(b"MODULE") {
        return Reply::Array(vec![]);
    }
    if kind.eq_ignore_ascii_case(b"ACLCAT") {
        return Reply::Array(
            listed_specs()
                .filter(|s| {
                    super::command_categories::categories(s.name)
                        .iter()
                        .any(|c| arg.eq_ignore_ascii_case(c.as_bytes()))
                })
                .map(|s| bulk(s.name))
                .collect(),
        );
    }
    if kind.eq_ignore_ascii_case(b"PATTERN") {
        let pattern = crate::engine::scan::glob::Pattern::nocase(arg);
        return Reply::Array(
            listed_specs()
                .filter(|s| pattern.matches(s.name.as_bytes()))
                .map(|s| bulk(s.name))
                .collect(),
        );
    }
    Reply::Error("ERR syntax error")
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

#[cfg(test)]
mod string_metadata_tests {
    use super::*;
    #[test]
    fn advertised_reference_arities_match_command_validation() {
        for spec in listed_specs() {
            if let Some((arity, _, _)) = super::super::command_info::header(spec.name) {
                assert_eq!(spec.arity, arity, "{}", spec.name);
            }
        }
    }

    #[test]
    fn string_keys_and_access_modes_match_execution() {
        for (name, arity, write) in [
            ("append", 3, true),
            ("strlen", 2, false),
            ("decr", 2, true),
            ("decrby", 3, true),
            ("getdel", 2, true),
            ("getset", 3, true),
            ("setnx", 3, true),
            ("getrange", 4, false),
            ("substr", 4, false),
            ("setrange", 4, true),
        ] {
            let spec = spec(name.as_bytes()).unwrap();
            assert_eq!(spec.arity, arity);
            assert_eq!(spec.write, write);
            let mut args = vec![name.as_bytes(), b"{tag}key"];
            args.resize(arity as usize, b"0");
            assert_eq!(keys(&args).unwrap().as_slice(), &[b"{tag}key".as_slice()]);
            args.pop();
            assert!(keys(&args).is_none());
        }
        assert_eq!(
            keys(&[b"MSETNX", b"{t}a", b"a", b"{t}b", b"b"])
                .unwrap()
                .as_slice(),
            &[b"{t}a".as_slice(), b"{t}b"]
        );
        assert!(keys(&[b"MSETNX", b"a", b"v", b"b"]).is_none());
    }
}
