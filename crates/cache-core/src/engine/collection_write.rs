//! Validated single-item mutations. Admission happens before touching live data;
//! the network thread captures dirty keys as immutable snapshots at handoff.
use super::*;
use crate::{
    server::Sink,
    value::{Collection, Value},
};
use std::sync::OnceLock;

enum Edit {
    Hash(Bytes, Bytes),
    List(usize, Bytes),
    Add(Bytes),
    Remove(Bytes),
    Score(Bytes, f64),
}
impl Engine {
    pub(crate) fn execute_collection_in_place(
        &mut self,
        a: &[&[u8]],
        now: i64,
        sink: &mut Sink,
    ) -> Option<Reply> {
        let cmd = *a.first()?;
        let supported = a.len() == 4
            && (eq(cmd, b"HSET") || eq(cmd, b"LSET") || eq(cmd, b"ZINCRBY"))
            || a.len() == 3 && (eq(cmd, b"SADD") || eq(cmd, b"SREM"));
        if !supported {
            return None;
        }
        let key = a[1];
        let entry = self.entries.get_mut(key)?;
        if entry.expires_at_ms.is_some_and(|t| t <= now) {
            return None;
        }
        let data = entry.value.collection()?;
        let (edit, reply, payload_delta, count_delta, fields) = match data {
            Collection::Hash(h) if eq(cmd, b"HSET") => {
                let old = h.get_key_value(a[2]);
                let delta = if let Some((_, v)) = old {
                    a[3].len() as isize - v.len() as isize
                } else {
                    (a[2].len() + a[3].len()) as isize
                };
                (
                    Edit::Hash(
                        old.map_or_else(|| a[2].into(), |(k, _)| k.clone()),
                        a[3].into(),
                    ),
                    Reply::Integer(i64::from(old.is_none())),
                    delta,
                    isize::from(old.is_none()),
                    2,
                )
            }
            Collection::List(l) if eq(cmd, b"LSET") => {
                let index = integer(a[2])?;
                let index = if index < 0 {
                    (l.len() as i64).checked_add(index)?
                } else {
                    index
                };
                let index = usize::try_from(index).ok()?;
                let old = l.get(index)?;
                (
                    Edit::List(index, a[3].into()),
                    Reply::Simple("OK"),
                    a[3].len() as isize - old.len() as isize,
                    0,
                    1,
                )
            }
            Collection::Set(s) if eq(cmd, b"SADD") || eq(cmd, b"SREM") => {
                let exists = s.contains(a[2]);
                let add = eq(cmd, b"SADD");
                if add == exists {
                    return Some(Reply::Integer(0));
                }
                // Generic deletion handles the final member and key removal.
                if !add && s.len() == 1 {
                    return None;
                }
                let sign = if add { 1 } else { -1 };
                (
                    if add {
                        Edit::Add(a[2].into())
                    } else {
                        Edit::Remove(a[2].into())
                    },
                    Reply::Integer(1),
                    sign * a[2].len() as isize,
                    sign,
                    1,
                )
            }
            Collection::ZSet(z) if eq(cmd, b"ZINCRBY") => {
                let by = crate::collections::score::parse(a[2]).ok()?;
                let old = z.scores.get_key_value(a[3]);
                let score = by + old.map_or(0.0, |(_, s)| s.0);
                if score.is_nan() {
                    return Some(Reply::Error("ERR resulting score is not a number (NaN)"));
                }
                let reply = Reply::Bulk(Some(if score == 0.0 {
                    b"0".to_vec()
                } else {
                    score.to_string().into_bytes()
                }));
                (
                    Edit::Score(old.map_or_else(|| a[3].into(), |(k, _)| k.clone()), score),
                    reply,
                    if old.is_none() {
                        (a[3].len() + 8) as isize
                    } else {
                        0
                    },
                    isize::from(old.is_none()),
                    2,
                )
            }
            _ => return None,
        };
        let encoded_len =
            (entry.value.len() as isize + payload_delta + count_delta * fields * 4) as usize;
        let memory = (entry.value.memory_bytes() as isize
            + 2 * payload_delta
            + count_delta * (fields * 4 + 160)) as usize;
        let bytes = key.len() + memory + 128;
        if key.len() + encoded_len + 128 > crate::protocol::MAX_FRAME
            || bytes > crate::server::MAX_PENDING_BYTES / 2
        {
            return Some(Reply::Error("ERR collection exceeds maximum size"));
        }
        let used = self.used - cost(key, entry) + bytes;
        if used > self.limit {
            return Some(Reply::Error(
                "OOM command not allowed when used memory > 'maxmemory'",
            ));
        }
        if !sink.reserve_collection(key, bytes) {
            return Some(Reply::Error(
                "MISCONF persistence backlog full or unavailable",
            ));
        }
        // No fallible command validation remains after admission. Previously
        // handed-off snapshots keep their own roots; private roots mutate in place.
        let Value::Collection {
            data,
            encoded,
            encoded_len: size,
            memory: accounted,
        } = &mut entry.value
        else {
            unreachable!()
        };
        match (Arc::make_mut(data), edit) {
            (Collection::Hash(h), Edit::Hash(k, v)) => {
                h.insert(k, v);
            }
            (Collection::List(l), Edit::List(i, v)) => {
                l.set(i, v);
            }
            (Collection::Set(s), Edit::Add(v)) => {
                s.insert(v);
            }
            (Collection::Set(s), Edit::Remove(v)) => {
                s.remove(&v);
            }
            (Collection::ZSet(z), Edit::Score(m, s)) => z.insert(m, s),
            _ => unreachable!(),
        }
        if let Some(cache) = Arc::get_mut(encoded) {
            cache.take();
        } else {
            *encoded = Arc::new(OnceLock::new());
        }
        *size = encoded_len;
        *accounted = memory;
        self.used = used;
        Some(reply)
    }
}
