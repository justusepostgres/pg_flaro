//! Hash field TTL setters, inspection and persistence removal.
mod ex;
use super::{eq, Engine, Entry, Mutations};
use crate::{
    collections,
    protocol::Reply,
    value::{Collection, Value},
};
const COMMANDS: &[&[u8]] = &[
    b"HGETEX",
    b"HSETEX",
    b"HEXPIRE",
    b"HPEXPIRE",
    b"HEXPIREAT",
    b"HPEXPIREAT",
    b"HTTL",
    b"HPTTL",
    b"HEXPIRETIME",
    b"HPEXPIRETIME",
    b"HPERSIST",
];
pub(super) fn execute(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    if !COMMANDS.iter().any(|name| eq(a[0], name)) {
        return None;
    }
    Some(run(engine, a, now, submit).unwrap_or_else(|error| error))
}
fn run(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply, Reply> {
    crate::cluster::validate_command(a)?;
    if eq(a[0], b"HGETEX") || eq(a[0], b"HSETEX") {
        return ex::run(engine, a, now, submit);
    }
    let old = engine.get(a[1], now);
    let hash = match old.and_then(|entry| entry.value.collection()) {
        Some(Collection::Hash(hash)) => Some(hash),
        None if old.is_none() => None,
        _ => return Err(collections::WRONGTYPE),
    };
    let command = a[0].to_ascii_lowercase();
    let setter = matches!(
        command.as_slice(),
        b"hexpire" | b"hpexpire" | b"hexpireat" | b"hpexpireat"
    );
    let persist = command == b"hpersist";
    let milliseconds = command.starts_with(b"hp");
    let mut position = if setter { 3 } else { 2 };
    let mut condition = b"".as_slice();
    let mut deadline = 0;
    if setter {
        deadline = parse_deadline(
            a[2],
            if command.ends_with(b"at") { 0 } else { now },
            milliseconds,
            &command,
        )?;
        let option = cstring(a[position]);
        if [b"NX".as_slice(), b"XX", b"GT", b"LT"]
            .iter()
            .any(|flag| eq(option, flag))
        {
            condition = option;
            position += 1;
        }
    }
    if !a
        .get(position)
        .is_some_and(|value| eq(cstring(value), b"FIELDS"))
    {
        return Err(Reply::Error(
            "ERR Mandatory argument FIELDS is missing or not at the right position",
        ));
    }
    let count_error = if setter {
        "ERR Parameter `numFields` should be greater than 0"
    } else {
        "ERR Number of fields must be a positive integer"
    };
    let count = a
        .get(position + 1)
        .and_then(|value| collections::integer(value).ok())
        .filter(|count| *count > 0)
        .ok_or(Reply::Error(count_error))?;
    if count as usize != a.len() - position - 2 {
        return Err(Reply::Error(
            "ERR The `numfields` parameter must match the number of arguments",
        ));
    }
    let fields = &a[position + 2..];
    let Some(hash) = hash else {
        return Ok(Reply::Array(
            fields.iter().map(|_| Reply::Integer(-2)).collect(),
        ));
    };
    if !setter && !persist {
        let base = if command.ends_with(b"time") { 0 } else { now };
        return Ok(Reply::Array(
            fields
                .iter()
                .map(|field| {
                    Reply::Integer(if !hash.contains_key(*field) {
                        -2
                    } else if let Some(expiry) = hash.expiration(field) {
                        if milliseconds {
                            expiry - base
                        } else {
                            (expiry + 999 - base) / 1000
                        }
                    } else {
                        -1
                    })
                })
                .collect(),
        ));
    }
    let mut next = hash.clone();
    let mut changed = false;
    let mut replies = Vec::with_capacity(fields.len());
    for field in fields {
        let expiry = next.expiration(field);
        let result = if !next.contains_key(*field) {
            -2
        } else if persist {
            if expiry.is_none() {
                -1
            } else {
                next.set_expiration(field, None);
                changed = true;
                1
            }
        } else if (eq(condition, b"NX") && expiry.is_some())
            || (eq(condition, b"XX") && expiry.is_none())
            || (eq(condition, b"GT") && expiry.is_none_or(|old| deadline <= old))
            || (eq(condition, b"LT") && expiry.is_some_and(|old| deadline >= old))
        {
            0
        } else {
            changed = true;
            if deadline <= now {
                next.remove(*field);
                2
            } else {
                next.set_expiration(field, Some(deadline));
                1
            }
        };
        replies.push(Reply::Integer(result));
    }
    let reply = Reply::Array(replies);
    if !changed {
        return Ok(reply);
    }
    let entry = if next.is_empty() {
        None
    } else {
        Some(Entry {
            value: Value::from_collection(Collection::Hash(next))
                .map_err(|_| Reply::Error("ERR collection exceeds maximum size"))?,
            expires_at_ms: old.and_then(|entry| entry.expires_at_ms),
        })
    };
    Ok(engine.apply_typed(vec![(a[1].to_vec(), entry)], reply, submit))
}
fn cstring(bytes: &[u8]) -> &[u8] {
    bytes.split(|byte| *byte == 0).next().unwrap()
}

fn parse_deadline(
    value: &[u8],
    base: i64,
    milliseconds: bool,
    command: &[u8],
) -> Result<i64, Reply> {
    let value = collections::integer(value)?;
    if value < 0 {
        return Err(Reply::Error("ERR invalid expire time, must be >= 0"));
    }
    value
        .checked_mul(if milliseconds { 1 } else { 1000 })
        .and_then(|v| v.checked_add(base))
        .filter(|deadline| *deadline <= 0x3fff_ffff_ffff)
        .ok_or_else(|| {
            Reply::OwnedError(format!(
                "ERR invalid expire time in '{}' command",
                String::from_utf8_lossy(command)
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn assert_schedule(engine: &Engine) {
        let expected: im::OrdSet<_> = engine
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                entry
                    .value
                    .hash_expiration()
                    .map(|deadline| (deadline, key.clone()))
            })
            .collect();
        assert_eq!(engine.hash_expirations, expected);
    }
    #[test]
    fn schedule_tracks_replacement_copy_rename_and_whole_key_expiry() {
        let mut engine = Engine::new(8 << 20);
        for i in 0..2000 {
            engine.execute(
                &[b"SET", format!("unrelated:{i}").as_bytes(), b"v"],
                0,
                |_| true,
            );
        }
        engine.execute(&[b"HSET", b"h", b"a", b"1", b"b", b"2"], 0, |_| true);
        for deadline in 10..110 {
            engine.execute(
                &[
                    b"HPEXPIREAT",
                    b"h",
                    deadline.to_string().as_bytes(),
                    b"FIELDS",
                    b"1",
                    b"a",
                ],
                0,
                |_| true,
            );
            assert_schedule(&engine);
            assert_eq!(engine.hash_expirations.len(), 1);
        }
        engine.execute(
            &[b"HPEXPIREAT", b"h", b"200", b"FIELDS", b"1", b"b"],
            0,
            |_| true,
        );
        for command in [
            vec![b"COPY".as_slice(), b"h", b"copy"],
            vec![b"RENAME", b"copy", b"renamed"],
            vec![b"SET", b"renamed", b"plain"],
            vec![b"COPY", b"h", b"short"],
            vec![b"PEXPIREAT", b"short", b"5"],
            vec![b"COPY", b"h", b"deleted"],
            vec![b"DEL", b"deleted"],
        ] {
            assert!(!matches!(
                engine.execute(&command, 0, |_| true),
                Reply::Error(_) | Reply::OwnedError(_)
            ));
            assert_schedule(&engine);
        }
        engine.reap(5);
        assert_schedule(&engine);
        assert_eq!(engine.hash_expirations.len(), 1);
        engine.expire_hash_fields(109);
        assert_schedule(&engine);
        assert_eq!(engine.hash_expirations.iter().next().unwrap().0, 200);
        assert_eq!(
            engine.execute(&[b"HLEN", b"h"], 109, |_| true),
            Reply::Integer(1)
        );
        engine.expire_hash_fields(200);
        assert_schedule(&engine);
        assert!(engine.hash_expirations.is_empty());
        assert_eq!(engine.entries.len(), 2001);
    }
    #[test]
    fn schedule_and_expiration_cleanup_roll_back_on_unwind() {
        let mut engine = Engine::new(1 << 20);
        engine.execute(&[b"HSET", b"h", b"a", b"1"], 0, |_| true);
        engine.execute(
            &[b"HPEXPIREAT", b"h", b"10", b"FIELDS", b"1", b"a"],
            0,
            |_| true,
        );
        let original = engine.snapshot(b"h");
        let used = engine.used;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = engine.transact(
                |engine, _| {
                    engine.expire_hash_fields(10);
                    assert!(engine.hash_expirations.is_empty());
                    panic!("abort speculative cleanup");
                },
                |_| true,
            );
        }));
        assert!(result.is_err());
        assert_schedule(&engine);
        assert_eq!(engine.snapshot(b"h"), original);
        assert_eq!(engine.used, used);
        engine.expire_hash_fields(10);
        assert_schedule(&engine);
        assert!(engine.snapshot(b"h").is_none());
    }
    #[test]
    fn expiry_removes_fields_without_admission_and_invalidates_live_watches() {
        let mut engine = Engine::new(1 << 20);
        engine.execute(&[b"HSET", b"h", b"a", b"1", b"b", b"2"], 0, |_| true);
        engine.execute(
            &[b"HPEXPIREAT", b"h", b"5", b"FIELDS", b"1", b"a"],
            0,
            |_| true,
        );
        let watch = engine.watch(b"h", 0);
        let before = engine.snapshot(b"h");
        assert!(matches!(
            engine.execute(&[b"HPERSIST", b"h", b"FIELDS", b"1", b"a"], 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"h"), before);
        assert_eq!(
            engine.execute(&[b"HGET", b"h", b"a"], 5, |_| panic!(
                "expiry needs no write admission"
            )),
            Reply::Bulk(None)
        );
        assert_eq!(
            engine.execute(&[b"HLEN", b"h"], 5, |_| true),
            Reply::Integer(1)
        );
        assert!(engine.watch_dirty(b"h", &watch, 5));
        let current = engine.watch(b"h", 5);
        engine.reap(10);
        assert!(!engine.watch_dirty(b"h", &current, 10));
        engine.execute(
            &[b"HPEXPIREAT", b"h", b"15", b"FIELDS", b"1", b"b"],
            10,
            |_| true,
        );
        let mut wire = Vec::new();
        engine.get_into(b"h", 15, &mut wire);
        assert_eq!(wire, b"$-1\r\n");
        engine.reap(15);
        assert!(engine.snapshot(b"h").is_none());
        assert_eq!(engine.used, 0);
    }
    #[test]
    fn rollback_restores_the_expiry_schedule_and_absent_watches_stay_clean() {
        let mut engine = Engine::new(1 << 20);
        engine.execute(&[b"HSET", b"h", b"a", b"1"], 0, |_| true);
        engine.execute(
            &[b"HPEXPIREAT", b"h", b"10", b"FIELDS", b"1", b"a"],
            0,
            |_| true,
        );
        let before = engine.snapshot(b"h");
        let rejected = engine.transact(
            |engine, submit| {
                vec![engine.execute(
                    &[b"HPEXPIREAT", b"h", b"5", b"FIELDS", b"1", b"a"],
                    0,
                    submit,
                )]
            },
            |_| false,
        );
        assert!(rejected.is_err());
        assert_eq!(engine.snapshot(b"h"), before);
        assert_eq!(
            engine.execute(&[b"HGET", b"h", b"a"], 6, |_| true),
            Reply::Bulk(Some(b"1".to_vec()))
        );
        engine.execute(&[b"PEXPIREAT", b"h", b"7"], 6, |_| true);
        let absent = engine.watch(b"h", 8);
        engine.reap(10);
        assert!(!engine.watch_dirty(b"h", &absent, 10));
    }
}
