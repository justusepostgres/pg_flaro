//! Stateless cursors ordered by stable key hash. No server-side scan sessions.
pub(crate) mod glob;
use super::{eq, integer, Engine, ARITY, SYNTAX};
use crate::{
    collections::WRONGTYPE,
    protocol::{Reply, MAX_FRAME},
    value::Collection,
};
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
};

enum Value<'a> {
    Bytes(&'a [u8]),
    Score(f64),
}
struct Row<'a> {
    key: &'a [u8],
    value: Option<Value<'a>>,
    kind: &'static str,
}

fn hash(key: &[u8]) -> u64 {
    // Unlike HashMap's randomized iteration order, this is unchanged by resize,
    // insertions and deletions, so continuously present keys cannot be skipped.
    let mut state = DefaultHasher::new();
    key.hash(&mut state);
    state.finish()
}
fn response(cursor: u64, rows: Vec<Reply>) -> Reply {
    Reply::Array(vec![
        Reply::Bulk(Some(cursor.to_string().into_bytes())),
        Reply::Array(rows),
    ])
}
fn cursor(raw: &[u8]) -> Option<u64> {
    // Redis's cursor parser first tries canonical signed decimal, then C's
    // strtoull fallback, including leading whitespace/zeros and a plus sign.
    let raw = raw.split(|c| *c == 0).next()?;
    if let Some(n) = integer(raw) {
        return u64::try_from(n).ok();
    }
    let raw = raw.trim_ascii_start();
    let (negative, digits) = match raw.first()? {
        b'-' => (true, &raw[1..]),
        b'+' => (false, &raw[1..]),
        _ => (false, raw),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let n: u64 = std::str::from_utf8(digits).ok()?.parse().ok()?;
    Some(if negative { n.wrapping_neg() } else { n })
}

pub(super) fn execute(engine: &Engine, args: &[&[u8]], now: i64) -> Option<Reply> {
    let kind = if eq(args[0], b"SCAN") {
        0
    } else if eq(args[0], b"HSCAN") {
        1
    } else if eq(args[0], b"SSCAN") {
        3
    } else if eq(args[0], b"ZSCAN") {
        4
    } else {
        return None;
    };
    Some(run(engine, args, now, kind).unwrap_or_else(|error| error))
}
fn run<'a>(engine: &'a Engine, args: &[&[u8]], now: i64, kind: u8) -> Result<Reply, Reply> {
    let index = if kind == 0 { 1 } else { 2 };
    if args.len() <= index {
        return Err(ARITY);
    }
    let cursor = cursor(args[index]).ok_or(Reply::Error("ERR invalid cursor"))?;
    // Redis validates the cursor first, but missing collection keys return the
    // empty result before option validation, and wrong types precede options.
    let collection = if kind != 0 {
        let Some(entry) = engine.get(args[1], now) else {
            return Ok(response(0, vec![]));
        };
        if entry.value.kind() != kind {
            return Err(WRONGTYPE);
        }
        entry.value.collection()
    } else {
        None
    };
    let (mut count, mut pattern, mut type_name, mut no_values) = (10usize, None, None, false);
    let mut i = index + 1;
    while i < args.len() {
        if eq(args[i], b"COUNT") && i + 1 < args.len() {
            let n = integer(args[i + 1])
                .ok_or(Reply::Error("ERR value is not an integer or out of range"))?;
            if n < 1 {
                return Err(SYNTAX);
            }
            // COUNT is a hint. Bound selection memory independently of it.
            count = (n as usize).min(1024);
            i += 2;
        } else if eq(args[i], b"MATCH") && i + 1 < args.len() {
            pattern = (args[i + 1] != b"*").then(|| glob::Pattern::new(args[i + 1]));
            i += 2;
        } else if kind == 0 && eq(args[i], b"TYPE") && i + 1 < args.len() {
            type_name = Some(args[i + 1]);
            i += 2;
        } else if eq(args[i], b"NOVALUES") {
            if kind != 1 {
                return Err(Reply::Error(
                    "ERR NOVALUES option can only be used in HSCAN",
                ));
            }
            no_values = true;
            i += 1;
        } else {
            return Err(SYNTAX);
        }
    }
    // Keep the smallest count+1 hash groups at/after the cursor. Grouping hash
    // collisions ensures no continuously present key is dropped at a boundary.
    // This bounded-memory implementation still examines the whole source per
    // call; a persistent index is needed for Redis's scan complexity target.
    let mut selected: BTreeMap<u64, Vec<Row<'_>>> = BTreeMap::new();
    let mut select = |row: Row<'a>| {
        let h = hash(row.key);
        if h < cursor {
            return;
        }
        if selected.len() == count + 1
            && selected.last_key_value().is_some_and(|(&last, _)| h > last)
        {
            return;
        }
        selected.entry(h).or_default().push(row);
        if selected.len() > count + 1 {
            selected.pop_last();
        }
    };
    match collection {
        Some(Collection::Hash(values)) => {
            for (key, value) in values.iter() {
                select(Row {
                    key,
                    value: (!no_values).then_some(Value::Bytes(value)),
                    kind: "hash",
                });
            }
        }
        Some(Collection::Set(values)) => {
            for key in values {
                select(Row {
                    key,
                    value: None,
                    kind: "set",
                });
            }
        }
        Some(Collection::ZSet(values)) => {
            for (key, value) in &values.scores {
                select(Row {
                    key,
                    value: Some(Value::Score(value.0)),
                    kind: "zset",
                });
            }
        }
        None => {
            for (key, entry) in &engine.entries {
                if entry.expires_at_ms.is_none_or(|t| t > now) {
                    select(Row {
                        key,
                        value: None,
                        kind: entry.value.type_name(),
                    });
                }
            }
        }
        _ => unreachable!(),
    }
    let mut rows = Vec::new();
    let mut bytes = 64;
    for (visited, (h, group)) in selected.into_iter().enumerate() {
        if visited == count {
            return Ok(response(h, rows));
        }
        let mut batch = Vec::new();
        let mut size = 0;
        for row in group {
            if type_name.is_some_and(|t| !eq(t, row.kind.as_bytes()))
                || pattern.as_ref().is_some_and(|p| !p.matches(row.key))
            {
                continue;
            }
            size += row.key.len() + 32;
            batch.push(Reply::Bulk(Some(row.key.to_vec())));
            if let Some(value) = row.value {
                let value = match value {
                    Value::Bytes(v) => v.to_vec(),
                    Value::Score(v) => {
                        if v == 0.0 {
                            b"0".to_vec()
                        } else {
                            v.to_string().into_bytes()
                        }
                    }
                };
                size += value.len() + 32;
                batch.push(Reply::Bulk(Some(value)));
            }
        }
        if bytes + size > 2 * MAX_FRAME - 32 {
            if rows.is_empty() {
                return Err(Reply::Error("ERR scan response exceeds maximum size"));
            }
            return Ok(response(h, rows));
        }
        bytes += size;
        rows.extend(batch);
    }
    Ok(response(0, rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn scan_keeps_continuously_present_keys_across_mutations() {
        let mut engine = Engine::new(1_000_000);
        let survivors: HashSet<Vec<u8>> =
            (0..100).map(|n| format!("keep:{n}").into_bytes()).collect();
        for key in &survivors {
            engine.execute(&[b"SET", key, b"v"], 0, |_| true);
        }
        engine.execute(&[b"SET", b"expired", b"v", b"PX", b"1"], 0, |_| true);
        let mut cursor = b"0".to_vec();
        let mut seen = HashSet::new();
        for page in 0..500 {
            let result = engine.execute(&[b"SCAN", &cursor, b"COUNT", b"3"], 2, |_| {
                panic!("read only")
            });
            let Reply::Array(reply) = result else {
                panic!("{result:?}");
            };
            let Reply::Bulk(Some(next)) = &reply[0] else {
                panic!("cursor");
            };
            let Reply::Array(keys) = &reply[1] else {
                panic!("keys");
            };
            for key in keys {
                let Reply::Bulk(Some(key)) = key else {
                    panic!("key");
                };
                seen.insert(key.clone());
            }
            cursor = next.clone();
            if cursor == b"0" {
                break;
            }
            // Resize the underlying map and remove transient entries between pages.
            for n in 0..20 {
                let key = format!("transient:{n}");
                if page % 2 == 0 {
                    engine.execute(&[b"SET", key.as_bytes(), b"x"], 2, |_| true);
                } else {
                    engine.execute(&[b"DEL", key.as_bytes()], 2, |_| true);
                }
            }
        }
        assert_eq!(cursor, b"0");
        assert!(survivors.is_subset(&seen));
        assert!(!seen.contains(b"expired".as_slice()));
    }
}
