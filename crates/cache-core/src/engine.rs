mod bitfield;
mod bitmap;
mod bitop;
pub(crate) mod blocking;
mod collection_write;
mod expiry;
mod float;
mod geo;
mod hash_expiry;
mod hyperloglog;
mod keys;
mod lazyfree;
mod lcs;
mod rdb;
pub(crate) mod scan;
pub(crate) mod sort;
mod string_options;
mod strings;
mod transaction;
pub(crate) mod watch;
use crate::protocol::Reply;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub type Bytes = Arc<[u8]>;
pub type Mutations = smallvec::SmallVec<[Mutation; 1]>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub value: crate::value::Value,
    pub expires_at_ms: Option<i64>,
}
#[derive(Clone, Debug)]
pub struct Mutation {
    pub key: Bytes,
    pub entry: Option<Entry>,
}
impl Mutation {
    /// Stable PGC1 accounting, independent of the in-memory collection representation.
    pub fn journal_bytes(&self) -> usize {
        self.bytes()
            - self.entry.as_ref().map_or(0, |e| {
                if e.value.kind() == 0 {
                    0
                } else {
                    crate::value::COLLECTION_BASE_MEMORY
                }
            })
    }

    pub fn bytes(&self) -> usize {
        self.key.len() + self.entry.as_ref().map_or(0, |e| e.value.memory_bytes()) + 128
    }
}
pub struct Engine {
    undo: Option<transaction::Undo>,
    watches: HashMap<Bytes, watch::Version>,
    entries: HashMap<Bytes, Entry>,
    used: usize,
    limit: usize,
    // Conservative earliest deadline: deletes/overwrites may leave an earlier
    // stale bound, but never one later than any entry's actual expiry.
    next_expiry: Option<i64>,
    hash_expirations: im::OrdSet<(i64, Bytes)>,
}
fn cost(k: &[u8], e: &Entry) -> usize {
    k.len() + e.value.memory_bytes() + 128
}
fn integer(b: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(b).ok()?;
    let n: i64 = s.parse().ok()?;
    (n.to_string() == s).then_some(n)
}
fn eq(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}
const SYNTAX: Reply = Reply::Error("ERR syntax error");
const ARITY: Reply = Reply::Error("ERR wrong number of arguments");

impl Engine {
    pub(crate) fn snapshot(&self, key: &[u8]) -> Option<Entry> {
        self.entries.get(key).cloned()
    }

    pub fn new(limit: usize) -> Self {
        Self {
            undo: None,
            watches: HashMap::new(),
            entries: HashMap::new(),
            used: 0,
            limit,
            next_expiry: None,
            hash_expirations: im::OrdSet::new(),
        }
    }
    pub fn restore(
        &mut self,
        key: Vec<u8>,
        mut entry: Entry,
        now: i64,
    ) -> Result<(), &'static str> {
        if entry.expires_at_ms.is_some_and(|t| t <= now) {
            return Ok(());
        }
        if let Some(cleaned) = entry.value.expire_hash_fields(now) {
            let Some(value) = cleaned else {
                return Ok(());
            };
            entry.value = value;
        }
        let old = self
            .entries
            .get(key.as_slice())
            .map_or(0, |e| cost(&key, e));
        let used = self.used - old + cost(&key, &entry);
        if used > self.limit {
            return Err("persisted dataset exceeds memory limit");
        }
        self.used = used;
        self.track_expiry(entry.expires_at_ms);
        self.sync_hash_expiry(&key, Some(&entry));
        self.touch_watch(&key);
        self.entries.insert(key.into(), entry);
        Ok(())
    }
    pub(crate) fn get(&self, key: &[u8], now: i64) -> Option<&Entry> {
        self.entries
            .get(key)
            .filter(|e| e.expires_at_ms.is_none_or(|t| t > now) && e.value.has_live_fields(now))
    }
    pub fn validate_cluster_ownership(
        &self,
        topology: &crate::cluster::Topology,
    ) -> Result<(), &'static str> {
        for key in self.entries.keys() {
            let slot = crate::cluster::keyslot(key);
            if topology.owners[slot as usize] != topology.local
                && !topology.importing.contains_key(&slot)
            {
                return Err("persisted key belongs to another shard");
            }
        }
        Ok(())
    }
    pub(crate) fn slot_keys(&self, slot: u16, now: i64, limit: usize) -> Result<Vec<Vec<u8>>, ()> {
        let mut result = Vec::new();
        let mut bytes = 0;
        for (key, _) in self
            .entries
            .iter()
            .filter(|(key, entry)| {
                crate::cluster::keyslot(key) == slot && entry.expires_at_ms.is_none_or(|t| t > now)
            })
            .take(limit)
        {
            bytes += key.len() + 32;
            if bytes > crate::protocol::MAX_FRAME {
                return Err(());
            }
            result.push(key.to_vec());
        }
        Ok(result)
    }
    pub(crate) fn slot_count(&self, slot: u16, now: i64) -> usize {
        self.entries
            .iter()
            .filter(|(key, entry)| {
                crate::cluster::keyslot(key) == slot && entry.expires_at_ms.is_none_or(|t| t > now)
            })
            .count()
    }
    /// Exactly one deadline per expiring hash. Persistent tree snapshots keep
    /// transaction setup constant-time and rollback restores the full schedule.
    fn sync_hash_expiry(&mut self, key: &[u8], entry: Option<&Entry>) {
        let old = self
            .entries
            .get(key)
            .and_then(|e| e.value.hash_expiration());
        let new = entry.and_then(|e| e.value.hash_expiration());
        if old == new {
            return;
        }
        let key = self
            .entries
            .get_key_value(key)
            .map_or_else(|| Bytes::from(key), |(stored, _)| stored.clone());
        if let Some(deadline) = old {
            self.hash_expirations.remove(&(deadline, key.clone()));
        }
        if let Some(deadline) = new {
            self.hash_expirations.insert((deadline, key));
        }
    }
    /// Deadlines are durable already; removing expired fields, like whole-key
    /// reaping, requires no new persistence admission. Recovery repeats cleanup.
    pub(crate) fn expire_hash_fields(&mut self, now: i64) {
        while let Some((deadline, key)) = self.hash_expirations.iter().next().cloned() {
            if deadline > now {
                break;
            }
            let old = self.entries.get(&key).expect("scheduled hash exists");
            let entry = old
                .value
                .expire_hash_fields(now)
                .expect("scheduled hash has expired fields")
                .map(|value| Entry {
                    value,
                    expires_at_ms: old.expires_at_ms,
                });
            if old.expires_at_ms.is_some_and(|deadline| deadline <= now) {
                self.touch_watch_deletion(&key);
            } else {
                self.touch_watch(&key);
            }
            self.sync_hash_expiry(&key, entry.as_ref());
            self.used -= self.entries.get(&key).map_or(0, |old| cost(&key, old));
            if let Some(entry) = entry {
                self.used += cost(&key, &entry);
                self.entries.insert(key, entry);
            } else {
                self.entries.remove(&key);
            }
        }
    }
    fn track_expiry(&mut self, expiry: Option<i64>) {
        if let Some(expiry) = expiry {
            self.next_expiry = Some(self.next_expiry.map_or(expiry, |old| old.min(expiry)));
        }
    }
    pub fn get_into(&self, key: &[u8], now: i64, output: &mut Vec<u8>) {
        if self.get(key, now).is_some_and(|e| e.value.kind() != 0) {
            crate::collections::WRONGTYPE.encode(output);
        } else {
            crate::protocol::bulk(self.get(key, now).map(|e| e.value.as_ref()), output);
        }
    }
    pub(crate) fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        expires_at_ms: Option<i64>,
        submit: &mut impl FnMut(Mutations) -> bool,
    ) -> Reply {
        self.put_string(key, value, expires_at_ms, submit, true)
    }
    // Encoding-only HLL changes persist but do not invalidate Redis WATCH.
    fn put_string(
        &mut self,
        key: &[u8],
        value: &[u8],
        expires_at_ms: Option<i64>,
        submit: &mut impl FnMut(Mutations) -> bool,
        logical_change: bool,
    ) -> Reply {
        let existing = self.entries.get_key_value(key);
        let used =
            self.used - existing.map_or(0, |(_, e)| cost(key, e)) + key.len() + value.len() + 128;
        if used > self.limit {
            return Reply::Error("OOM command not allowed when used memory > 'maxmemory'");
        }
        let key: Bytes = existing.map_or_else(|| Arc::from(key), |(stored, _)| stored.clone());
        let entry = Entry {
            value: value.into(),
            expires_at_ms,
        };
        if !submit(smallvec::smallvec![Mutation {
            key: key.clone(),
            entry: Some(entry.clone()),
        }]) {
            return Reply::Error("MISCONF persistence backlog full or unavailable");
        }
        self.track_expiry(entry.expires_at_ms);
        self.sync_hash_expiry(&key, Some(&entry));
        if logical_change {
            self.touch_watch(&key);
        } else {
            self.record_change(&key);
        }
        self.entries.insert(key, entry);
        self.used = used;
        Reply::Simple("OK")
    }
    pub(crate) fn apply_typed(
        &mut self,
        changes: Vec<(Vec<u8>, Option<Entry>)>,
        reply: Reply,
        submit: &mut impl FnMut(Mutations) -> bool,
    ) -> Reply {
        let mut used = self.used;
        for (key, entry) in &changes {
            used -= self.entries.get(key.as_slice()).map_or(0, |e| cost(key, e));
            used += entry.as_ref().map_or(0, |e| cost(key, e));
            if entry.as_ref().is_some_and(|e| {
                e.value.kind() != 0
                    && (key.len() + e.value.len() + 128 > crate::protocol::MAX_FRAME
                        || cost(key, e) > crate::server::MAX_PENDING_BYTES / 2)
            }) {
                return Reply::Error("ERR collection exceeds maximum size");
            }
        }
        if used > self.limit {
            return Reply::Error("OOM command not allowed when used memory > 'maxmemory'");
        }
        let mutations: Mutations = changes
            .iter()
            .map(|(k, e)| Mutation {
                key: k.clone().into(),
                entry: e.clone(),
            })
            .collect();
        if !submit(mutations) {
            return Reply::Error("MISCONF persistence backlog full or unavailable");
        }
        for (key, entry) in changes {
            self.sync_hash_expiry(&key, entry.as_ref());
            if entry.is_some() {
                self.touch_watch(&key);
            } else {
                self.touch_watch_deletion(&key);
            }
            if let Some(entry) = entry {
                self.track_expiry(entry.expires_at_ms);
                self.entries.insert(key.into(), entry);
            } else {
                self.entries.remove(key.as_slice());
            }
        }
        self.used = used;
        reply
    }
    pub fn reap(&mut self, now: i64) {
        if self.undo.is_some() {
            return;
        }
        self.expire_hash_fields(now);
        self.prune_watches();
        if self.next_expiry.is_none_or(|expiry| expiry > now) {
            return;
        }
        let mut next = None;
        self.entries.retain(|k, e| {
            if e.expires_at_ms.is_some_and(|t| t <= now) {
                if let Some(deadline) = e.value.hash_expiration() {
                    self.hash_expirations.remove(&(deadline, k.clone()));
                }
                self.used -= cost(k, e);
                false
            } else {
                if let Some(expiry) = e.expires_at_ms {
                    next = Some(next.map_or(expiry, |old: i64| old.min(expiry)));
                }
                true
            }
        });
        self.next_expiry = next;
    }
    pub fn execute(
        &mut self,
        a: &[&[u8]],
        now: i64,
        mut submit: impl FnMut(Mutations) -> bool,
    ) -> Reply {
        self.expire_hash_fields(now);
        if a.is_empty() {
            return ARITY;
        }
        if let Some(reply) = rdb::execute(self, a, now, &mut submit) {
            return reply;
        }
        if let Some(reply) = hash_expiry::execute(self, a, now, &mut submit) {
            return reply;
        }
        if blocking::supported(a[0]) {
            return blocking::execute(self, a, now, &mut submit);
        }
        if let Some(reply) = geo::execute(self, a, now, &mut submit) {
            return reply;
        }
        if let Some(reply) = hyperloglog::execute(self, a, now, &mut submit) {
            return reply;
        }
        if let Some(reply) = float::execute(self, a, now, &mut submit) {
            return reply;
        }
        if eq(a[0], b"SORT") || eq(a[0], b"SORT_RO") {
            return sort::execute(self, a, now, &mut submit);
        }
        if eq(a[0], b"LCS") {
            return lcs::execute(self, a, now);
        }
        if let Some(reply) = scan::execute(self, a, now) {
            return reply;
        }
        if let Some(reply) = bitfield::execute(self, a, now, &mut submit) {
            return reply;
        }
        if let Some(reply) = bitop::execute(self, a, now, &mut submit) {
            return reply;
        }
        if let Some(reply) = bitmap::execute(self, a, now, &mut submit) {
            return reply;
        }
        if let Some(reply) = keys::execute(self, a, now, &mut submit) {
            return reply;
        }
        if let Some(reply) = expiry::execute(self, a, now, &mut submit) {
            return reply;
        }
        if let Some(reply) = strings::execute(self, a, now, &mut submit) {
            return reply;
        }
        if a.len() == 3 && eq(a[0], b"SET") {
            return self.put(a[1], a[2], None, &mut submit);
        }
        if let Some(reply) = string_options::execute(self, a, now, &mut submit) {
            return reply;
        }
        if let Some(reply) = crate::collections::execute(self, a, now, &mut submit) {
            return reply;
        }
        if a.len() >= 2
            && [
                b"GET".as_slice(),
                b"INCR",
                b"INCRBY",
                b"DECR",
                b"DECRBY",
                b"APPEND",
                b"STRLEN",
            ]
            .iter()
            .any(|cmd| eq(a[0], cmd))
            && self.get(a[1], now).is_some_and(|e| e.value.kind() != 0)
        {
            return crate::collections::WRONGTYPE;
        }
        if eq(a[0], b"TYPE") {
            return if a.len() == 2 {
                Reply::Simple(self.get(a[1], now).map_or("none", |e| e.value.type_name()))
            } else {
                ARITY
            };
        }
        let cmd = a[0].to_ascii_uppercase();
        let mut changes: HashMap<Vec<u8>, Option<Entry>> = HashMap::new();
        let reply = match cmd.as_slice() {
            b"PING" if a.len() == 1 => Reply::Simple("PONG"),
            b"PING" | b"ECHO" if a.len() == 2 => Reply::Bulk(Some(a[1].to_vec())),
            b"GET" if a.len() == 2 => Reply::Bulk(self.get(a[1], now).map(|e| e.value.to_vec())),
            b"STRLEN" if a.len() == 2 => {
                Reply::Integer(self.get(a[1], now).map_or(0, |e| e.value.len()) as i64)
            }
            b"APPEND" if a.len() == 3 => {
                let old = self.get(a[1], now);
                let len = old.map_or(0, |e| e.value.len()).saturating_add(a[2].len());
                if len > crate::protocol::MAX_FRAME - a[1].len().min(crate::protocol::MAX_FRAME) {
                    return Reply::Error("ERR string exceeds maximum size");
                }
                let expiry = old.and_then(|e| e.expires_at_ms);
                let mut value = Vec::with_capacity(len);
                if let Some(old) = old {
                    value.extend_from_slice(&old.value);
                }
                value.extend_from_slice(a[2]);
                return match self.put(a[1], &value, expiry, &mut submit) {
                    Reply::Simple("OK") => Reply::Integer(len as i64),
                    error => error,
                };
            }
            b"MGET" if a.len() >= 2 => {
                // Bound before cloning values: repeated keys can amplify a tiny
                // request into an otherwise enormous allocation.
                let bytes: usize = a[1..]
                    .iter()
                    .map(|k| {
                        self.get(k, now)
                            .filter(|e| e.value.kind() == 0)
                            .map_or(5, |e| e.value.len() + 32)
                    })
                    .sum();
                if bytes > 2 * crate::protocol::MAX_FRAME - 32 {
                    return Reply::Error("ERR response exceeds configured limit");
                }
                Reply::Array(
                    a[1..]
                        .iter()
                        .map(|k| {
                            Reply::Bulk(
                                self.get(k, now)
                                    .filter(|e| e.value.kind() == 0)
                                    .map(|e| e.value.to_vec()),
                            )
                        })
                        .collect(),
                )
            }
            b"EXISTS" if a.len() >= 2 => {
                Reply::Integer(a[1..].iter().filter(|k| self.get(k, now).is_some()).count() as i64)
            }
            b"SETEX" | b"PSETEX" if a.len() == 4 => {
                let Some(ttl) = integer(a[2]) else {
                    return Reply::Error("ERR value is not an integer or out of range");
                };
                let seconds = cmd == b"SETEX";
                let Some(expiry) = ttl
                    .checked_mul(if seconds { 1000 } else { 1 })
                    .filter(|duration| *duration > 0)
                    .and_then(|duration| now.checked_add(duration))
                else {
                    return Reply::Error(if seconds {
                        "ERR invalid expire time in 'setex' command"
                    } else {
                        "ERR invalid expire time in 'psetex' command"
                    });
                };
                return self.put(a[1], a[3], Some(expiry), &mut submit);
            }
            b"MSET" if a.len() >= 3 && a.len() % 2 == 1 => {
                for pair in a[1..].chunks_exact(2) {
                    changes.insert(
                        pair[0].to_vec(),
                        Some(Entry {
                            value: pair[1].into(),
                            expires_at_ms: None,
                        }),
                    );
                }
                Reply::Simple("OK")
            }
            b"DEL" if a.len() >= 2 => {
                let mut count = 0;
                for key in a[1..].iter().collect::<HashSet<_>>() {
                    if self.get(key, now).is_some() {
                        changes.insert(key.to_vec(), None);
                        count += 1;
                    }
                }
                Reply::Integer(count)
            }
            b"INCR" | b"INCRBY" | b"DECR" | b"DECRBY"
                if a.len()
                    == if cmd == b"INCR" || cmd == b"DECR" {
                        2
                    } else {
                        3
                    } =>
            {
                let increment = if cmd == b"INCR" || cmd == b"DECR" {
                    1
                } else {
                    let Some(increment) = integer(a[2]) else {
                        return Reply::Error("ERR value is not an integer or out of range");
                    };
                    increment
                };
                if cmd == b"DECRBY" && increment == i64::MIN {
                    return Reply::Error("ERR decrement would overflow");
                }
                let old = self.get(a[1], now);
                let Some(n) = old.map_or(Some(0), |e| integer(&e.value)) else {
                    return Reply::Error("ERR value is not an integer or out of range");
                };
                let result = if cmd == b"DECR" || cmd == b"DECRBY" {
                    n.checked_sub(increment)
                } else {
                    n.checked_add(increment)
                };
                let Some(n) = result else {
                    return Reply::Error("ERR increment or decrement would overflow");
                };
                changes.insert(
                    a[1].to_vec(),
                    Some(Entry {
                        value: n.to_string().into_bytes().into(),
                        expires_at_ms: old.and_then(|e| e.expires_at_ms),
                    }),
                );
                Reply::Integer(n)
            }
            b"PERSIST" if a.len() == 2 => {
                let Some(old) = self.get(a[1], now).filter(|e| e.expires_at_ms.is_some()) else {
                    return Reply::Integer(0);
                };
                changes.insert(
                    a[1].to_vec(),
                    Some(Entry {
                        value: old.value.clone(),
                        expires_at_ms: None,
                    }),
                );
                Reply::Integer(1)
            }
            b"SELECT" if a.len() == 2 => {
                if a[1] == b"0" {
                    Reply::Simple("OK")
                } else {
                    Reply::Error("ERR only database 0 is supported")
                }
            }
            b"PING" | b"ECHO" | b"GET" | b"MGET" | b"EXISTS" | b"SET" | b"MSET" | b"DEL"
            | b"INCR" | b"INCRBY" | b"DECR" | b"DECRBY" | b"APPEND" | b"STRLEN" | b"SETEX"
            | b"PSETEX" | b"TTL" | b"PTTL" | b"EXPIRE" | b"PEXPIRE" | b"PERSIST" | b"SELECT" => {
                ARITY
            }
            _ => Reply::Error("ERR unknown or unsupported command"),
        };
        if changes.is_empty() {
            return reply;
        }
        let mut used = self.used;
        for (k, v) in &changes {
            used -= self.entries.get(k.as_slice()).map_or(0, |e| cost(k, e));
            used += v.as_ref().map_or(0, |e| cost(k, e));
        }
        if used > self.limit {
            return Reply::Error("OOM command not allowed when used memory > 'maxmemory'");
        }
        let mutations = changes
            .iter()
            .map(|(k, e)| Mutation {
                key: k.clone().into(),
                entry: e.clone(),
            })
            .collect();
        // Reserve persistence capacity before changing visible state or acknowledging.
        if !submit(mutations) {
            return Reply::Error("MISCONF persistence backlog full or unavailable");
        }
        for (k, v) in changes {
            self.sync_hash_expiry(&k, v.as_ref());
            if v.is_some() {
                self.touch_watch(&k);
            } else {
                self.touch_watch_deletion(&k);
            }
            if let Some(e) = v {
                self.track_expiry(e.expires_at_ms);
                self.entries.insert(k.into(), e);
            } else {
                self.entries.remove(k.as_slice());
            }
        }
        self.used = used;
        reply
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(e: &mut Engine, a: &[&str], now: i64) -> Reply {
        e.execute(
            &a.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
            now,
            |_| true,
        )
    }
    #[test]
    fn string_extensions_preserve_ttl_and_reject_without_mutating() {
        let mut e = Engine::new(65536);
        assert_eq!(run(&mut e, &["STRLEN", "k"], 0), Reply::Integer(0));
        assert_eq!(run(&mut e, &["APPEND", "k", ""], 0), Reply::Integer(0));
        assert_eq!(run(&mut e, &["EXISTS", "k"], 0), Reply::Integer(1));
        run(&mut e, &["SET", "k", "-1", "PX", "100"], 0);
        assert_eq!(
            run(&mut e, &["DECRBY", "k", "-9223372036854775808"], 1),
            Reply::Error("ERR decrement would overflow")
        );
        run(&mut e, &["SET", "k", "9223372036854775807", "PX", "99"], 1);
        assert!(matches!(
            run(&mut e, &["DECRBY", "k", "-1"], 1),
            Reply::Error(_)
        ));
        assert_eq!(run(&mut e, &["PTTL", "k"], 1), Reply::Integer(99));
        let old = e.snapshot(b"k");
        let used = e.used;
        for args in [vec![b"APPEND".as_slice(), b"k", b"x"], vec![b"DECR", b"k"]] {
            assert!(matches!(e.execute(&args, 1, |_| false), Reply::Error(_)));
            assert_eq!(e.snapshot(b"k"), old);
            assert_eq!(e.used, used);
        }
        assert_eq!(run(&mut e, &["DECR", "k"], 100), Reply::Integer(-1));
        assert_eq!(run(&mut e, &["PTTL", "k"], 100), Reply::Integer(-1));
        assert_eq!(
            e.execute(&[b"APPEND", b"k", b"\0\xff"], 100, |_| true),
            Reply::Integer(4)
        );
        assert_eq!(run(&mut e, &["STRLEN", "k"], 100), Reply::Integer(4));
        run(&mut e, &["HSET", "h", "f", "v"], 100);
        for args in [
            vec!["STRLEN", "h"],
            vec!["APPEND", "h", "v"],
            vec!["DECR", "h"],
            vec!["DECRBY", "h", "1"],
        ] {
            assert_eq!(run(&mut e, &args, 100), crate::collections::WRONGTYPE);
        }
    }
    #[test]
    fn expiry_deadline_survives_restore_rejection_and_expiry_changes() {
        let mut e = Engine::new(4096);
        run(&mut e, &["SET", "later", "1", "PX", "100"], 0);
        e.restore(
            b"restored".to_vec(),
            Entry {
                value: b"v".as_slice().into(),
                expires_at_ms: Some(30),
            },
            0,
        )
        .unwrap();
        assert!(matches!(
            e.execute(&[b"PEXPIRE", b"later", b"1"], 0, |_| false),
            Reply::Error(_)
        ));
        let used = e.used;
        e.reap(29);
        assert_eq!(e.entries.len(), 2);
        assert_eq!(e.used, used);
        e.reap(30);
        assert!(!e.entries.contains_key(b"restored".as_slice()));
        assert_eq!(run(&mut e, &["PTTL", "later"], 30), Reply::Integer(70));
        run(&mut e, &["PERSIST", "later"], 30);
        e.reap(100);
        e.reap(i64::MAX);
        assert_eq!(e.entries.len(), 1);
        // New earlier expiry must wake maintenance after an expiry-free period.
        run(&mut e, &["PEXPIRE", "later", "20"], 100);
        run(&mut e, &["INCR", "later"], 101);
        e.reap(119);
        assert_eq!(e.entries.len(), 1);
        e.reap(120);
        assert!(e.entries.is_empty());
        assert_eq!(e.used, 0);
        run(&mut e, &["SET", "k", "old", "PX", "10"], 200);
        run(&mut e, &["SET", "k", "new"], 201);
        e.reap(210);
        assert_eq!(
            run(&mut e, &["GET", "k"], 210),
            Reply::Bulk(Some(b"new".to_vec()))
        );
        run(&mut e, &["PEXPIRE", "k", "1"], 210);
        run(&mut e, &["DEL", "k"], 210);
        e.reap(211);
        assert!(e.entries.is_empty());
        assert_eq!(e.used, 0);
    }
    #[test]
    fn expiry_conditions_and_counter() {
        let mut e = Engine::new(4096);
        assert_eq!(
            run(&mut e, &["SET", "k", "4", "PX", "100", "NX"], 0),
            Reply::Simple("OK")
        );
        assert_eq!(run(&mut e, &["SET", "k", "x", "NX"], 1), Reply::Bulk(None));
        assert_eq!(run(&mut e, &["INCR", "k"], 2), Reply::Integer(5));
        assert_eq!(run(&mut e, &["PTTL", "k"], 2), Reply::Integer(98));
        assert_eq!(run(&mut e, &["GET", "k"], 100), Reply::Bulk(None));
        assert_eq!(
            run(&mut e, &["SET", "k", "new", "NX"], 100),
            Reply::Simple("OK")
        );
    }
    #[test]
    fn expiry_aliases_preserve_atomicity_and_absolute_deadlines() {
        let mut e = Engine::new(4096);
        assert_eq!(
            run(&mut e, &["SETEX", "k", "2", "old"], 100),
            Reply::Simple("OK")
        );
        assert_eq!(run(&mut e, &["PTTL", "k"], 101), Reply::Integer(1999));
        for ttl in ["0", "-1", "9223372036854775807"] {
            assert_eq!(
                run(&mut e, &["SETEX", "k", ttl, "new"], 101),
                Reply::Error("ERR invalid expire time in 'setex' command")
            );
        }
        assert_eq!(
            run(&mut e, &["PSETEX", "k", "invalid", "new"], 101),
            Reply::Error("ERR value is not an integer or out of range")
        );
        assert_eq!(
            e.execute(&[b"PSETEX", b"k", b"10", b"new"], 101, |_| false),
            Reply::Error("MISCONF persistence backlog full or unavailable")
        );
        assert_eq!(
            run(&mut e, &["GET", "k"], 101),
            Reply::Bulk(Some(b"old".to_vec()))
        );
        assert_eq!(run(&mut e, &["PTTL", "k"], 101), Reply::Integer(1999));
        let mut persisted = Vec::new();
        assert_eq!(
            e.execute(&[b"PSETEX", b"k", b"10", b"new"], 101, |m| {
                persisted.extend(m);
                true
            }),
            Reply::Simple("OK")
        );
        assert_eq!(
            persisted[0].entry.as_ref().unwrap().expires_at_ms,
            Some(111)
        );
        assert_eq!(run(&mut e, &["GET", "k"], 111), Reply::Bulk(None));
        e.reap(111);
        assert_eq!(e.used, 0);
    }

    #[test]
    fn increment_by_checks_signed_bounds_and_preserves_ttl_on_rejection() {
        let mut e = Engine::new(4096);
        assert_eq!(
            run(&mut e, &["INCRBY", "k", "-9223372036854775808"], 0),
            Reply::Integer(i64::MIN)
        );
        assert_eq!(
            run(&mut e, &["INCRBY", "k", "-1"], 0),
            Reply::Error("ERR increment or decrement would overflow")
        );
        assert_eq!(
            run(&mut e, &["INCRBY", "k", "9223372036854775807"], 0),
            Reply::Integer(-1)
        );
        assert_eq!(run(&mut e, &["PEXPIRE", "k", "100"], 0), Reply::Integer(1));
        assert_eq!(
            e.execute(&[b"INCRBY", b"k", b"10"], 1, |_| false),
            Reply::Error("MISCONF persistence backlog full or unavailable")
        );
        assert_eq!(
            run(&mut e, &["GET", "k"], 1),
            Reply::Bulk(Some(b"-1".to_vec()))
        );
        assert_eq!(run(&mut e, &["PTTL", "k"], 1), Reply::Integer(99));
        assert_eq!(run(&mut e, &["INCRBY", "k", "2"], 1), Reply::Integer(1));
        assert_eq!(run(&mut e, &["PTTL", "k"], 1), Reply::Integer(99));
        assert_eq!(
            run(&mut e, &["INCRBY", "k", "01"], 1),
            Reply::Error("ERR value is not an integer or out of range")
        );
        assert_eq!(run(&mut e, &["INCRBY", "k", "5"], 100), Reply::Integer(5));
        assert_eq!(run(&mut e, &["PTTL", "k"], 100), Reply::Integer(-1));
    }

    #[test]
    fn queued_values_remain_immutable_across_overwrites() {
        let mut e = Engine::new(1024);
        let mut queued = Vec::new();
        for value in [b"first".as_slice(), b"second".as_slice()] {
            assert_eq!(
                e.execute(&[b"SET", b"k", value], 0, |m| {
                    queued.extend(m);
                    true
                }),
                Reply::Simple("OK")
            );
        }
        assert_eq!(queued[0].entry.as_ref().unwrap().value.as_ref(), b"first");
        assert_eq!(queued[1].entry.as_ref().unwrap().value.as_ref(), b"second");
        assert_eq!(
            run(&mut e, &["GET", "k"], 0),
            Reply::Bulk(Some(b"second".to_vec()))
        );
    }
    #[test]
    fn single_key_write_rejection_preserves_value_and_expiry() {
        let mut e = Engine::new(1024);
        run(&mut e, &["SET", "k", "old", "PX", "10"], 0);
        assert!(matches!(
            e.execute(&[b"SET", b"k", b"new"], 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(
            run(&mut e, &["GET", "k"], 1),
            Reply::Bulk(Some(b"old".to_vec()))
        );
        assert_eq!(run(&mut e, &["PTTL", "k"], 1), Reply::Integer(9));
        assert_eq!(run(&mut e, &["SET", "k", "new"], 2), Reply::Simple("OK"));
        assert_eq!(run(&mut e, &["PTTL", "k"], 2), Reply::Integer(-1));
    }
    #[test]
    fn direct_read_encoding_preserves_binary_and_expiry() {
        let mut e = Engine::new(1024);
        e.restore(
            b"k".to_vec(),
            Entry {
                value: vec![0, 255].into(),
                expires_at_ms: Some(10),
            },
            0,
        )
        .unwrap();
        let mut out = Vec::new();
        e.get_into(b"k", 9, &mut out);
        e.get_into(b"k", 10, &mut out);
        assert_eq!(out, b"$2\r\n\0\xff\r\n$-1\r\n");
    }
    #[test]
    fn rejected_persistence_is_atomic() {
        let mut e = Engine::new(4096);
        let a = ["MSET", "a", "1", "b", "2"].map(|s| s.as_bytes());
        assert!(matches!(e.execute(&a, 0, |_| false), Reply::Error(_)));
        assert_eq!(run(&mut e, &["EXISTS", "a", "b"], 0), Reply::Integer(0));
    }
    #[test]
    fn memory_rejection_and_duplicates() {
        let mut e = Engine::new(260);
        assert!(matches!(
            run(&mut e, &["MSET", "a", "1", "b", "22"], 0),
            Reply::Error(_)
        ));
        assert_eq!(run(&mut e, &["GET", "a"], 0), Reply::Bulk(None));
        assert_eq!(
            run(&mut e, &["MSET", "a", "1", "a", "2"], 0),
            Reply::Simple("OK")
        );
        assert_eq!(run(&mut e, &["DEL", "a", "a"], 0), Reply::Integer(1));
    }
    #[test]
    fn mget_repeated_keys_cannot_amplify_memory_without_bound() {
        let mut e = Engine::new(2 * 1024 * 1024);
        e.restore(
            b"x".to_vec(),
            Entry {
                value: vec![0; 512 * 1024].into(),
                expires_at_ms: None,
            },
            0,
        )
        .unwrap();
        assert!(matches!(
            run(&mut e, &["MGET", "x", "x", "x", "x", "x"], 0),
            Reply::Error(_)
        ));
    }
    #[test]
    fn overflow_and_recovery() {
        let mut e = Engine::new(4096);
        e.restore(
            b"x".to_vec(),
            Entry {
                value: i64::MAX.to_string().into_bytes().into(),
                expires_at_ms: None,
            },
            0,
        )
        .unwrap();
        assert!(matches!(run(&mut e, &["INCR", "x"], 0), Reply::Error(_)));
        assert_eq!(
            run(&mut e, &["GET", "x"], 0),
            Reply::Bulk(Some(i64::MAX.to_string().into_bytes()))
        );
        e.restore(
            b"expired".to_vec(),
            Entry {
                value: vec![0].into(),
                expires_at_ms: Some(9),
            },
            10,
        )
        .unwrap();
        assert_eq!(run(&mut e, &["EXISTS", "expired"], 10), Reply::Integer(0));
    }
}
