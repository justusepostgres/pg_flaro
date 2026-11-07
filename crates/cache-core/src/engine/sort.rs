//! SORT snapshots inputs before admitting any destination mutation.
use super::{eq, integer, Bytes, Engine, Entry, Mutations, ARITY, SYNTAX};
use crate::{
    collections::WRONGTYPE,
    protocol::{Reply, MAX_FRAME},
    value::{Collection, Value},
};

pub(crate) struct Options<'a> {
    pub(crate) by: Option<&'a [u8]>,
    pub(crate) get: Vec<&'a [u8]>,
    pub(crate) store: Option<&'a [u8]>,
    desc: bool,
    alpha: bool,
    skip: usize,
    count: usize,
    nosort: bool,
}
// Redis option comparisons and pattern searches use NUL-terminated strings.
fn cstr(v: &[u8]) -> &[u8] {
    v.split(|b| *b == 0).next().unwrap_or_default()
}
pub(crate) fn options<'a>(a: &[&'a [u8]]) -> Result<Options<'a>, Reply> {
    if a.len() < 2 {
        return Err(ARITY);
    }
    let mut o = Options {
        by: None,
        get: vec![],
        store: None,
        desc: false,
        alpha: false,
        skip: 0,
        count: usize::MAX,
        nosort: false,
    };
    let mut i = 2;
    while i < a.len() {
        let flag = cstr(a[i]);
        if eq(flag, b"ASC") {
            o.desc = false;
        } else if eq(flag, b"DESC") {
            o.desc = true;
        } else if eq(flag, b"ALPHA") {
            o.alpha = true;
        } else if eq(flag, b"LIMIT") && i + 2 < a.len() {
            let start = integer(a[i + 1]).ok_or(crate::collections::INTEGER)?;
            let count = integer(a[i + 2]).ok_or(crate::collections::INTEGER)?;
            o.skip = start.max(0) as usize;
            o.count = if count < 0 {
                usize::MAX
            } else {
                count as usize
            };
            i += 2;
        } else if eq(flag, b"BY") && i + 1 < a.len() {
            i += 1;
            o.by = Some(a[i]);
            // Redis preserves dontsort if an earlier BY was constant.
            o.nosort |= !cstr(a[i]).contains(&b'*');
        } else if eq(flag, b"GET") && i + 1 < a.len() {
            i += 1;
            o.get.push(a[i]);
        } else if eq(flag, b"STORE") && !eq(a[0], b"SORT_RO") && i + 1 < a.len() {
            i += 1;
            o.store = Some(a[i]);
        } else {
            return Err(SYNTAX);
        }
        i += 1;
    }
    Ok(o)
}
fn lookup(engine: &Engine, pattern: &[u8], member: &Bytes, now: i64) -> Option<Bytes> {
    if cstr(pattern) == b"#" {
        return Some(member.clone());
    }
    let star = cstr(pattern).iter().position(|b| *b == b'*')?;
    let field = cstr(&pattern[star + 1..])
        .windows(2)
        .position(|p| p == b"->")
        .map(|i| star + 1 + i)
        .filter(|i| pattern.get(i + 2).is_some_and(|b| *b != 0));
    let end = field.unwrap_or(pattern.len());
    let mut key = pattern[..star].to_vec();
    key.extend_from_slice(member);
    key.extend_from_slice(&pattern[star + 1..end]);
    let entry = engine.get(&key, now)?;
    if let Some(field) = field {
        let Collection::Hash(hash) = entry.value.collection()? else {
            return None;
        };
        hash.get(&pattern[field + 2..]).cloned()
    } else if let Value::String(value) = &entry.value {
        Some(value.clone())
    } else {
        None
    }
}
fn number(value: &[u8]) -> Result<f64, Reply> {
    let value = cstr(value);
    if value.is_empty() {
        return Ok(0.0);
    }
    // SORT uses fast_float rather than Redis's general strtod parser:
    // hexadecimal is rejected; NUL terminates the input.
    std::str::from_utf8(value)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|n| !n.is_nan())
        .ok_or(Reply::Error(
            "ERR One or more scores can't be converted into double",
        ))
}
pub(super) fn execute(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Reply {
    let mut o = match options(a) {
        Ok(o) => o,
        Err(e) => return e,
    };
    let mut members: Vec<Bytes> = match engine.get(a[1], now).map(|e| &e.value) {
        None => vec![],
        Some(v) => match v.collection() {
            Some(Collection::List(v)) => {
                let mut members: Vec<_> = v.iter().cloned().collect();
                if o.nosort && o.desc {
                    members.reverse();
                }
                members
            }
            Some(Collection::Set(v)) => {
                if o.nosort && o.store.is_some() {
                    o.nosort = false;
                    o.alpha = true;
                    o.by = None;
                }
                v.iter().cloned().collect()
            }
            Some(Collection::ZSet(v)) => {
                let mut members: Vec<_> = v.order.iter().map(|(_, m)| m.clone()).collect();
                if o.nosort && o.desc {
                    members.reverse();
                }
                members
            }
            _ => return WRONGTYPE,
        },
    };
    if !o.nosort {
        let mut weighted = Vec::with_capacity(members.len());
        for member in members {
            let weight = match o.by {
                Some(p) => lookup(engine, p, &member, now),
                None => Some(member.clone()),
            };
            let score = if o.alpha {
                0.0
            } else {
                match weight.as_ref().map_or(Ok(0.0), |v| number(v)) {
                    Ok(v) => v,
                    Err(e) => return e,
                }
            };
            weighted.push((member, weight, score));
        }
        weighted.sort_by(|a, b| {
            let cmp = if o.alpha {
                let left =
                    a.1.as_deref()
                        .map(|s| if o.store.is_some() { s } else { cstr(s) });
                let right =
                    b.1.as_deref()
                        .map(|s| if o.store.is_some() { s } else { cstr(s) });
                left.cmp(&right)
            } else {
                a.2.partial_cmp(&b.2).unwrap().then_with(|| a.0.cmp(&b.0))
            };
            if o.desc {
                cmp.reverse()
            } else {
                cmp
            }
        });
        members = weighted.into_iter().map(|v| v.0).collect();
    }
    let mut result = Vec::new();
    let mut bytes = 32usize;
    for member in members.into_iter().skip(o.skip).take(o.count) {
        let values = if o.get.is_empty() {
            vec![Some(member)]
        } else {
            o.get
                .iter()
                .map(|p| lookup(engine, p, &member, now))
                .collect()
        };
        for value in values {
            bytes = bytes.saturating_add(32 + value.as_ref().map_or(0, |v| v.len()));
            if bytes > 2 * MAX_FRAME - 32 {
                return Reply::Error("ERR response exceeds maximum size");
            }
            result.push(value);
        }
    }
    if let Some(destination) = o.store {
        let count = result.len();
        let entry = if count == 0 {
            None
        } else {
            let list = result
                .into_iter()
                .map(|v| v.unwrap_or_else(|| Bytes::from(&b""[..])))
                .collect();
            let value = match Value::from_collection(Collection::List(list)) {
                Ok(v) => v,
                Err(e) => return Reply::Error(e),
            };
            Some(Entry {
                value,
                expires_at_ms: None,
            })
        };
        engine.apply_typed(
            vec![(destination.to_vec(), entry)],
            Reply::Integer(count as i64),
            submit,
        )
    } else {
        Reply::Array(
            result
                .into_iter()
                .map(|v| Reply::Bulk(v.map(|v| v.to_vec())))
                .collect(),
        )
    }
}

pub(crate) fn cluster_patterns(a: &[&[u8]]) -> Option<Reply> {
    let o = options(a).ok()?;
    let slot = crate::cluster::keyslot(a[1]);
    let pattern_slot = |pattern: &[u8]| {
        let mut start = None;
        let mut empty = false;
        for (i, b) in pattern.iter().enumerate() {
            if matches!(b, b'*' | b'?' | b'[' | b'\\') {
                return None;
            }
            if *b == b'{' && start.is_none() && !empty {
                start = Some(i);
            } else if *b == b'}' {
                if let Some(s) = start {
                    if i == s + 1 {
                        start = None;
                        empty = true;
                    } else {
                        return Some(crate::cluster::keyslot(&pattern[..=i]));
                    }
                }
            }
        }
        Some(crate::cluster::keyslot(pattern))
    };
    if o.by
        .is_some_and(|p| cstr(p).contains(&b'*') && pattern_slot(p) != Some(slot))
    {
        return Some(Reply::Error("ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots."));
    }
    if o.get
        .iter()
        .any(|p| cstr(p) != b"#" && pattern_slot(p) != Some(slot))
    {
        return Some(Reply::Error("ERR GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots."));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn store_admission_is_atomic_and_clears_destination_expiry() {
        let mut engine = Engine::new(1024 * 1024);
        engine.execute(&[b"RPUSH", b"source", b"3", b"1", b"2"], 0, |_| true);
        engine.execute(&[b"SET", b"destination", b"old", b"PX", b"100"], 0, |_| {
            true
        });
        let old = engine.snapshot(b"destination");
        let reply = engine.execute(&[b"SORT", b"source", b"STORE", b"destination"], 1, |_| {
            false
        });
        assert!(matches!(reply, Reply::Error(_)));
        assert_eq!(engine.snapshot(b"destination"), old);
        assert_eq!(
            engine.execute(&[b"SORT", b"source", b"STORE", b"destination"], 1, |_| true),
            Reply::Integer(3)
        );
        assert_eq!(
            engine.execute(&[b"PTTL", b"destination"], 1, |_| true),
            Reply::Integer(-1)
        );
        assert_eq!(
            engine.execute(&[b"LRANGE", b"destination", b"0", b"-1"], 1, |_| true),
            Reply::Array(
                [b"1", b"2", b"3"]
                    .into_iter()
                    .map(|v| Reply::Bulk(Some(v.to_vec())))
                    .collect()
            )
        );
    }
    #[test]
    fn cluster_patterns_require_a_fixed_matching_slot() {
        assert!(
            cluster_patterns(&[b"SORT", b"{x}list", b"BY", b"{x}weight:*", b"GET", b"#"]).is_none()
        );
        assert!(cluster_patterns(&[b"SORT", b"{x}list", b"BY", b"weight:*"]).is_some());
        assert!(cluster_patterns(&[b"SORT", b"{x}list", b"GET", b"{y}weight:*"]).is_some());
    }
}
