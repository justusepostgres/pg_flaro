use crate::protocol::Reply;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

pub type Bytes = Arc<[u8]>;
pub type Mutations = smallvec::SmallVec<[Mutation; 1]>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub value: Bytes,
    pub expires_at_ms: Option<i64>,
}
#[derive(Clone, Debug)]
pub struct Mutation {
    pub key: Bytes,
    pub entry: Option<Entry>,
}
impl Mutation {
    pub fn bytes(&self) -> usize {
        self.key.len() + self.entry.as_ref().map_or(0, |e| e.value.len()) + 128
    }
}
pub struct Engine {
    entries: HashMap<Bytes, Entry>,
    used: usize,
    limit: usize,
    // Conservative earliest deadline: deletes/overwrites may leave an earlier
    // stale bound, but never one later than any entry's actual expiry.
    next_expiry: Option<i64>,
}
fn cost(k: &[u8], e: &Entry) -> usize {
    k.len() + e.value.len() + 128
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
    pub fn new(limit: usize) -> Self {
        Self {
            entries: HashMap::new(),
            used: 0,
            limit,
            next_expiry: None,
        }
    }
    pub fn restore(&mut self, key: Vec<u8>, entry: Entry, now: i64) -> Result<(), &'static str> {
        if entry.expires_at_ms.is_some_and(|t| t <= now) {
            return Ok(());
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
        self.entries.insert(key.into(), entry);
        Ok(())
    }
    fn get(&self, key: &[u8], now: i64) -> Option<&Entry> {
        self.entries
            .get(key)
            .filter(|e| e.expires_at_ms.is_none_or(|t| t > now))
    }
    fn track_expiry(&mut self, expiry: Option<i64>) {
        if let Some(expiry) = expiry {
            self.next_expiry = Some(self.next_expiry.map_or(expiry, |old| old.min(expiry)));
        }
    }
    pub fn get_into(&self, key: &[u8], now: i64, output: &mut Vec<u8>) {
        crate::protocol::bulk(self.get(key, now).map(|e| e.value.as_ref()), output);
    }
    fn put(
        &mut self,
        key: &[u8],
        value: &[u8],
        expires_at_ms: Option<i64>,
        submit: &mut impl FnMut(Mutations) -> bool,
    ) -> Reply {
        let existing = self.entries.get_key_value(key);
        let used =
            self.used - existing.map_or(0, |(_, e)| cost(key, e)) + key.len() + value.len() + 128;
        if used > self.limit {
            return Reply::Error("OOM command not allowed when used memory > 'maxmemory'");
        }
        let key: Bytes = existing.map_or_else(|| Arc::from(key), |(stored, _)| stored.clone());
        let entry = Entry {
            value: Arc::from(value),
            expires_at_ms,
        };
        if !submit(smallvec::smallvec![Mutation {
            key: key.clone(),
            entry: Some(entry.clone()),
        }]) {
            return Reply::Error("MISCONF persistence backlog full or unavailable");
        }
        self.track_expiry(entry.expires_at_ms);
        self.entries.insert(key, entry);
        self.used = used;
        Reply::Simple("OK")
    }
    pub fn reap(&mut self, now: i64) {
        if self.next_expiry.is_none_or(|expiry| expiry > now) {
            return;
        }
        let mut next = None;
        self.entries.retain(|k, e| {
            if e.expires_at_ms.is_some_and(|t| t <= now) {
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
        if a.is_empty() {
            return ARITY;
        }
        if a.len() == 3 && eq(a[0], b"SET") {
            return self.put(a[1], a[2], None, &mut submit);
        }
        let cmd = a[0].to_ascii_uppercase();
        let mut changes: HashMap<Vec<u8>, Option<Entry>> = HashMap::new();
        let reply = match cmd.as_slice() {
            b"PING" if a.len() == 1 => Reply::Simple("PONG"),
            b"PING" | b"ECHO" if a.len() == 2 => Reply::Bulk(Some(a[1].to_vec())),
            b"GET" if a.len() == 2 => Reply::Bulk(self.get(a[1], now).map(|e| e.value.to_vec())),
            b"MGET" if a.len() >= 2 => {
                // Bound before cloning values: repeated keys can amplify a tiny
                // request into an otherwise enormous allocation.
                let bytes: usize = a[1..]
                    .iter()
                    .map(|k| self.get(k, now).map_or(5, |e| e.value.len() + 32))
                    .sum();
                if bytes > 2 * crate::protocol::MAX_FRAME - 32 {
                    return Reply::Error("ERR response exceeds configured limit");
                }
                Reply::Array(
                    a[1..]
                        .iter()
                        .map(|k| Reply::Bulk(self.get(k, now).map(|e| e.value.to_vec())))
                        .collect(),
                )
            }
            b"EXISTS" if a.len() >= 2 => {
                Reply::Integer(a[1..].iter().filter(|k| self.get(k, now).is_some()).count() as i64)
            }
            b"SET" if a.len() >= 3 => {
                let (mut condition, mut expiry) = (None, None);
                let mut i = 3;
                while i < a.len() {
                    if eq(a[i], b"NX") || eq(a[i], b"XX") {
                        if condition.is_some() {
                            return SYNTAX;
                        }
                        condition = Some(eq(a[i], b"NX"));
                        i += 1;
                    } else if eq(a[i], b"EX") || eq(a[i], b"PX") {
                        if expiry.is_some() || i + 1 >= a.len() {
                            return SYNTAX;
                        }
                        let factor = if eq(a[i], b"EX") { 1000 } else { 1 };
                        let Some(t) = integer(a[i + 1])
                            .filter(|n| *n > 0)
                            .and_then(|n| n.checked_mul(factor))
                            .and_then(|n| now.checked_add(n))
                        else {
                            return Reply::Error("ERR invalid expire time in 'set' command");
                        };
                        expiry = Some(t);
                        i += 2;
                    } else {
                        return SYNTAX;
                    }
                }
                let exists = self.get(a[1], now).is_some();
                if condition.is_some_and(|nx| nx == exists) {
                    return Reply::Bulk(None);
                }
                return self.put(a[1], a[2], expiry, &mut submit);
            }
            b"MSET" if a.len() >= 3 && a.len() % 2 == 1 => {
                for pair in a[1..].chunks_exact(2) {
                    changes.insert(
                        pair[0].to_vec(),
                        Some(Entry {
                            value: Arc::from(pair[1]),
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
            b"INCR" if a.len() == 2 => {
                let old = self.get(a[1], now);
                let Some(n) = old.map_or(Some(0), |e| integer(&e.value)) else {
                    return Reply::Error("ERR value is not an integer or out of range");
                };
                let Some(n) = n.checked_add(1) else {
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
            b"TTL" | b"PTTL" if a.len() == 2 => Reply::Integer(match self.get(a[1], now) {
                None => -2,
                Some(Entry {
                    expires_at_ms: None,
                    ..
                }) => -1,
                Some(e) => {
                    let ms = e.expires_at_ms.unwrap() - now;
                    if cmd == b"TTL" {
                        (ms + 500) / 1000
                    } else {
                        ms
                    }
                }
            }),
            b"EXPIRE" | b"PEXPIRE" if a.len() == 3 => {
                let factor = if cmd == b"EXPIRE" { 1000 } else { 1 };
                let Some(t) = integer(a[2])
                    .and_then(|n| n.checked_mul(factor))
                    .and_then(|n| now.checked_add(n))
                else {
                    return Reply::Error("ERR value is not an integer or out of range");
                };
                let Some(old) = self.get(a[1], now) else {
                    return Reply::Integer(0);
                };
                changes.insert(
                    a[1].to_vec(),
                    if t <= now {
                        None
                    } else {
                        Some(Entry {
                            value: old.value.clone(),
                            expires_at_ms: Some(t),
                        })
                    },
                );
                Reply::Integer(1)
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
            | b"INCR" | b"TTL" | b"PTTL" | b"EXPIRE" | b"PEXPIRE" | b"PERSIST" | b"SELECT" => ARITY,
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
