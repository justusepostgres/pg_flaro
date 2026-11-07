mod hash;
mod list;
mod list_move;
pub(crate) mod mpop;
mod random;
pub(crate) mod score;
mod set;
pub(crate) mod set_card;
mod set_move;
mod zset;
pub(crate) mod zset_algebra;
use crate::{
    engine::{Engine, Entry, Mutations},
    protocol::Reply,
    value::{Collection, Value},
};
use std::borrow::Cow;
pub(crate) const WRONGTYPE: Reply =
    Reply::Error("WRONGTYPE Operation against a key holding the wrong kind of value");
pub(super) const SYNTAX: Reply = Reply::Error("ERR syntax error");
pub(super) const INTEGER: Reply = Reply::Error("ERR value is not an integer or out of range");
pub(super) type Result<T> = std::result::Result<T, Reply>;
// name, arity, kind, write, last-key-position. Shared with COMMAND discovery/routing.
pub(crate) const COMMANDS: &[(&str, i64, u8, bool, i64)] = &[
    ("hgetdel", -5, 1, true, 1),
    ("hincrbyfloat", 4, 1, true, 1),
    ("hrandfield", -2, 1, false, 1),
    ("zrandmember", -2, 4, false, 1),
    ("srandmember", -2, 3, false, 1),
    ("spop", -2, 3, true, 1),
    ("hset", -4, 1, true, 1),
    ("hmset", -4, 1, true, 1),
    ("hsetnx", 4, 1, true, 1),
    ("hget", 3, 1, false, 1),
    ("hmget", -3, 1, false, 1),
    ("hgetall", 2, 1, false, 1),
    ("hdel", -3, 1, true, 1),
    ("hexists", 3, 1, false, 1),
    ("hlen", 2, 1, false, 1),
    ("hkeys", 2, 1, false, 1),
    ("hvals", 2, 1, false, 1),
    ("hincrby", 4, 1, true, 1),
    ("hstrlen", 3, 1, false, 1),
    ("zunion", -3, 4, false, 0),
    ("zunionstore", -4, 4, true, 0),
    ("zinter", -3, 4, false, 0),
    ("zinterstore", -4, 4, true, 0),
    ("zintercard", -3, 4, false, 0),
    ("zdiff", -3, 4, false, 0),
    ("zdiffstore", -4, 4, true, 0),
    ("zrangebylex", -4, 4, false, 1),
    ("zrevrangebylex", -4, 4, false, 1),
    ("zlexcount", 4, 4, false, 1),
    ("zremrangebylex", 4, 4, true, 1),
    ("zrangestore", -5, 4, true, 2),
    ("zmpop", -4, 4, true, 0),
    ("lmpop", -4, 2, true, 0),
    ("lmove", 5, 2, true, 2),
    ("rpoplpush", 3, 2, true, 2),
    ("lpos", -3, 2, false, 1),
    ("linsert", 5, 2, true, 1),
    ("lpush", -3, 2, true, 1),
    ("rpush", -3, 2, true, 1),
    ("lpushx", -3, 2, true, 1),
    ("rpushx", -3, 2, true, 1),
    ("lpop", -2, 2, true, 1),
    ("rpop", -2, 2, true, 1),
    ("llen", 2, 2, false, 1),
    ("lrange", 4, 2, false, 1),
    ("lindex", 3, 2, false, 1),
    ("lset", 4, 2, true, 1),
    ("ltrim", 4, 2, true, 1),
    ("lrem", 4, 2, true, 1),
    ("sintercard", -3, 3, false, 0),
    ("smove", 4, 3, true, 2),
    ("sadd", -3, 3, true, 1),
    ("srem", -3, 3, true, 1),
    ("smembers", 2, 3, false, 1),
    ("sismember", 3, 3, false, 1),
    ("smismember", -3, 3, false, 1),
    ("scard", 2, 3, false, 1),
    ("sunion", -2, 3, false, -1),
    ("sinter", -2, 3, false, -1),
    ("sdiff", -2, 3, false, -1),
    ("sunionstore", -3, 3, true, -1),
    ("sinterstore", -3, 3, true, -1),
    ("sdiffstore", -3, 3, true, -1),
    ("zadd", -4, 4, true, 1),
    ("zincrby", 4, 4, true, 1),
    ("zrem", -3, 4, true, 1),
    ("zscore", 3, 4, false, 1),
    ("zmscore", -3, 4, false, 1),
    ("zcard", 2, 4, false, 1),
    ("zcount", 4, 4, false, 1),
    ("zrank", -3, 4, false, 1),
    ("zrevrank", -3, 4, false, 1),
    ("zrange", -4, 4, false, 1),
    ("zrevrange", -4, 4, false, 1),
    ("zrangebyscore", -4, 4, false, 1),
    ("zrevrangebyscore", -4, 4, false, 1),
    ("zpopmin", -2, 4, true, 1),
    ("zpopmax", -2, 4, true, 1),
    ("zremrangebyrank", 4, 4, true, 1),
    ("zremrangebyscore", 4, 4, true, 1),
];
pub(crate) fn execute(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    if !matches!(
        a[0].first(),
        Some(b'h' | b'H' | b'l' | b'L' | b'r' | b'R' | b's' | b'S' | b'z' | b'Z')
    ) {
        return None;
    }
    let spec = COMMANDS
        .iter()
        .find(|s| a[0].eq_ignore_ascii_case(s.0.as_bytes()))?;
    if spec.1 > 0 && a.len() != spec.1 as usize || spec.1 < 0 && a.len() < (-spec.1) as usize {
        return Some(Reply::Error("ERR wrong number of arguments"));
    }
    if matches!(
        spec.0,
        "zdiff" | "zdiffstore" | "zunion" | "zunionstore" | "zinter" | "zinterstore" | "zintercard"
    ) {
        return Some(zset_algebra::execute(engine, a, now, submit));
    }
    if spec.0 == "zrangestore" {
        return Some(zset::store(engine, a, now, submit));
    }
    if matches!(spec.0, "lmpop" | "zmpop") {
        return Some(mpop::execute(engine, a, now, submit));
    }
    if matches!(
        spec.0,
        "srandmember" | "spop" | "hrandfield" | "zrandmember"
    ) {
        return Some(random::execute(engine, a, now, submit));
    }
    if spec.0 == "sintercard" {
        return Some(set_card::execute(engine, a, now));
    }
    if spec.0 == "smove" {
        return Some(set_move::execute(engine, a, now, submit));
    }
    if matches!(spec.0, "lmove" | "rpoplpush") {
        return Some(list_move::execute(engine, a, now, submit));
    }
    if spec.4 == -1 {
        return Some(set::algebra(engine, a, now, spec.0, submit));
    }
    if let Err(error) = zset::validate(spec.0, a) {
        return Some(error);
    }
    if let Err(error) = list::validate(spec.0, a) {
        return Some(error);
    }
    let old = engine.get(a[1], now).cloned();
    if old.as_ref().is_some_and(|e| e.value.kind() != spec.2) {
        return Some(WRONGTYPE);
    }
    let mut data = old
        .as_ref()
        .and_then(|e| e.value.collection())
        .map_or_else(|| Cow::Owned(Collection::empty(spec.2)), Cow::Borrowed);
    let result = match spec.2 {
        1 => hash::run(spec.0, a, &mut data),
        2 => list::run(spec.0, a, &mut data),
        3 => set::run(spec.0, a, &mut data),
        4 => zset::run(spec.0, a, &mut data),
        _ => unreachable!(),
    };
    Some(match result {
        Err(reply) => reply,
        Ok((reply, false)) => reply,
        Ok((reply, true)) => {
            let entry = if data.is_empty() {
                None
            } else {
                match Value::from_updated(data.into_owned(), old.as_ref().map(|e| &e.value), a) {
                    Ok(value) => Some(Entry {
                        value,
                        expires_at_ms: old.and_then(|e| e.expires_at_ms),
                    }),
                    Err(_) => return Some(Reply::Error("ERR collection exceeds maximum size")),
                }
            };
            engine.apply_typed(vec![(a[1].to_vec(), entry)], reply, submit)
        }
    })
}
pub(super) fn integer(v: &[u8]) -> Result<i64> {
    let s = std::str::from_utf8(v).map_err(|_| INTEGER)?;
    let n = s.parse::<i64>().map_err(|_| INTEGER)?;
    if n.to_string() != s {
        return Err(INTEGER);
    }
    Ok(n)
}
pub(super) fn bulk(v: impl AsRef<[u8]>) -> Reply {
    Reply::Bulk(Some(v.as_ref().to_vec()))
}
pub(super) fn array<'a>(values: impl IntoIterator<Item = Option<&'a [u8]>>) -> Result<Reply> {
    let mut bytes = 32;
    let mut out = Vec::new();
    for v in values {
        bytes += v.map_or(0, |v| v.len()) + 32;
        if bytes > 2 * crate::protocol::MAX_FRAME {
            return Err(Reply::Error("ERR reply exceeds output limit"));
        }
        out.push(Reply::Bulk(v.map(|v| v.to_vec())));
    }
    Ok(Reply::Array(out))
}
pub(super) fn range(start: i64, end: i64, len: usize) -> std::ops::Range<usize> {
    let len = len as i64;
    let start = if start < 0 {
        len.saturating_add(start)
    } else {
        start
    };
    let end = if end < 0 {
        len.saturating_add(end)
    } else {
        end
    };
    let start = start.max(0).min(len) as usize;
    let stop = end.saturating_add(1).max(0).min(len) as usize;
    start..stop.max(start)
}

#[cfg(test)]
mod tests;
