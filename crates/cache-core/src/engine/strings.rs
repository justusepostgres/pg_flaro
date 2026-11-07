//! String operations that prepare mutations before persistence admission.
use super::{eq, integer, Engine, Entry, Mutations, ARITY};
use crate::{collections::WRONGTYPE, protocol::Reply};
use std::collections::HashMap;

const INTEGER: Reply = Reply::Error("ERR value is not an integer or out of range");

pub(super) fn execute(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    let name = args[0];
    let arity = if eq(name, b"GETDEL") {
        2
    } else if eq(name, b"GETSET") || eq(name, b"SETNX") {
        3
    } else if eq(name, b"GETRANGE") || eq(name, b"SUBSTR") || eq(name, b"SETRANGE") {
        4
    } else if eq(name, b"MSETNX") {
        0
    } else {
        return None;
    };
    if (arity != 0 && args.len() != arity)
        || (arity == 0 && (args.len() < 3 || args.len().is_multiple_of(2)))
    {
        return Some(ARITY);
    }
    Some(execute_valid(engine, args, now, submit))
}

fn execute_valid(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Reply {
    let name = args[0];
    if eq(name, b"MSETNX") {
        if args[1..]
            .chunks_exact(2)
            .any(|p| engine.get(p[0], now).is_some())
        {
            return Reply::Integer(0);
        }
        // Last occurrence wins, but existence is checked against the original
        // dataset, not earlier pairs within this atomic command.
        let changes: HashMap<Vec<u8>, Option<Entry>> = args[1..]
            .chunks_exact(2)
            .map(|p| {
                (
                    p[0].to_vec(),
                    Some(Entry {
                        value: p[1].into(),
                        expires_at_ms: None,
                    }),
                )
            })
            .collect();
        return engine.apply_typed(changes.into_iter().collect(), Reply::Integer(1), submit);
    }
    let old = engine.get(args[1], now);
    if eq(name, b"SETNX") {
        return if old.is_some() {
            Reply::Integer(0)
        } else {
            on_success(
                engine.put(args[1], args[2], None, submit),
                Reply::Integer(1),
            )
        };
    }
    // Redis parses range indexes before looking up the key or checking its type.
    let indexes = if eq(name, b"GETRANGE") || eq(name, b"SUBSTR") {
        match (integer(args[2]), integer(args[3])) {
            (Some(start), Some(end)) => Some((start, end)),
            _ => return INTEGER,
        }
    } else if eq(name, b"SETRANGE") {
        match integer(args[2]) {
            Some(offset) if offset >= 0 => Some((offset, 0)),
            Some(_) => return Reply::Error("ERR offset is out of range"),
            None => return INTEGER,
        }
    } else {
        None
    };
    if old.is_some_and(|e| e.value.kind() != 0) {
        return WRONGTYPE;
    }
    if eq(name, b"GETDEL") {
        return match old {
            None => Reply::Bulk(None),
            Some(old) => {
                let reply = Reply::Bulk(Some(old.value.to_vec()));
                engine.apply_typed(vec![(args[1].to_vec(), None)], reply, submit)
            }
        };
    }
    if eq(name, b"GETSET") {
        let reply = Reply::Bulk(old.map(|e| e.value.to_vec()));
        return on_success(engine.put(args[1], args[2], None, submit), reply);
    }
    let (start, end) = indexes.expect("range command has parsed indexes");
    let len = old.map_or(0, |e| e.value.len());
    if !eq(name, b"SETRANGE") {
        if len == 0 || (start < 0 && end < 0 && start > end) {
            return Reply::Bulk(Some(Vec::new()));
        }
        let normalize = |n: i64| if n < 0 { (len as i64 + n).max(0) } else { n };
        let start = normalize(start);
        let end = normalize(end).min(len as i64 - 1);
        return Reply::Bulk(Some(if start > end {
            Vec::new()
        } else {
            old.unwrap().value[start as usize..=end as usize].to_vec()
        }));
    }
    if args[3].is_empty() {
        return Reply::Integer(len as i64);
    }
    let Some(new_len) = usize::try_from(start)
        .ok()
        .and_then(|s| s.checked_add(args[3].len()))
    else {
        return Reply::Error("ERR string exceeds maximum size");
    };
    // Bound sparse writes before allocating their zero-filled gap. This remains
    // the existing Flaro size limit, not Redis's larger string limit.
    if new_len > crate::protocol::MAX_FRAME.saturating_sub(args[1].len()) {
        return Reply::Error("ERR string exceeds maximum size");
    }
    let new_len = new_len.max(len);
    let expiry = old.and_then(|e| e.expires_at_ms);
    let mut value = Vec::with_capacity(new_len);
    if let Some(old) = old {
        value.extend_from_slice(&old.value);
    }
    value.resize(new_len, 0);
    value[start as usize..start as usize + args[3].len()].copy_from_slice(args[3]);
    on_success(
        engine.put(args[1], &value, expiry, submit),
        Reply::Integer(new_len as i64),
    )
}

fn on_success(result: Reply, reply: Reply) -> Reply {
    match result {
        Reply::Simple("OK") => reply,
        error => error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_mutations_admit_atomically_and_preserve_expected_ttls() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SET", b"key", b"abcdef", b"PX", b"100"], 0, |_| true);
        let original = engine.snapshot(b"key");
        for args in [
            vec![b"GETDEL".as_slice(), b"key"],
            vec![b"GETSET", b"key", b"new"],
            vec![b"SETRANGE", b"key", b"9", b"x"],
        ] {
            assert!(matches!(
                engine.execute(&args, 1, |_| false),
                Reply::Error(_)
            ));
            assert_eq!(engine.snapshot(b"key"), original);
        }
        assert!(matches!(
            engine.execute(&[b"MSETNX", b"a", b"1", b"b", b"2"], 1, |_| false),
            Reply::Error(_)
        ));
        assert!(engine.snapshot(b"a").is_none());
        assert!(engine.snapshot(b"b").is_none());
        assert_eq!(
            engine.execute(&[b"MSETNX", b"a", b"1", b"key", b"2"], 1, |_| panic!(
                "no mutation expected"
            )),
            Reply::Integer(0)
        );
        assert!(engine.snapshot(b"a").is_none());
        assert_eq!(
            engine.execute(&[b"SETRANGE", b"key", b"8", b"x"], 1, |_| true),
            Reply::Integer(9)
        );
        assert_eq!(
            engine.get(b"key", 1).unwrap().value.as_ref(),
            b"abcdef\0\0x"
        );
        assert_eq!(engine.get(b"key", 1).unwrap().expires_at_ms, Some(100));
        assert_eq!(
            engine.execute(&[b"GETSET", b"key", b"new"], 1, |_| true),
            Reply::Bulk(Some(b"abcdef\0\0x".to_vec()))
        );
        assert_eq!(engine.get(b"key", 1).unwrap().expires_at_ms, None);
        assert_eq!(
            engine.execute(&[b"GETDEL", b"key"], 1, |_| true),
            Reply::Bulk(Some(b"new".to_vec()))
        );
        assert!(engine.snapshot(b"key").is_none());
        assert_eq!(
            engine.execute(
                &[b"SETRANGE", b"missing", b"9223372036854775807", b""],
                1,
                |_| panic!("no mutation expected")
            ),
            Reply::Integer(0)
        );
        assert!(engine.snapshot(b"missing").is_none());
        assert!(matches!(
            engine.execute(
                &[b"SETRANGE", b"missing", b"9223372036854775807", b"x"],
                1,
                |_| panic!("no mutation expected")
            ),
            Reply::Error(_)
        ));
    }
}
