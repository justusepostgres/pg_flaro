//! Atomic field reads/writes with expiration, including conditional multi-field sets.
use super::*;
use crate::value::Hash;
use std::borrow::Cow;
#[derive(Clone, Copy, Default)]
enum Expiration {
    #[default]
    Unchanged,
    Clear,
    At(i64),
}
struct Options<'a> {
    fields: &'a [&'a [u8]],
    expiry: Expiration,
    condition: u8,
}
fn arity(set: bool) -> Reply {
    Reply::Error(if set {
        "ERR wrong number of arguments for 'hsetex' command"
    } else {
        "ERR wrong number of arguments for 'hgetex' command"
    })
}
fn expiration(option: &[u8], value: &[u8], now: i64, set: bool) -> Result<Expiration, Reply> {
    let milliseconds = eq(option, b"PX") || eq(option, b"PXAT");
    let relative = eq(option, b"PX") || eq(option, b"EX");
    parse_deadline(
        value,
        if relative { now } else { 0 },
        milliseconds,
        if set { b"hsetex" } else { b"hgetex" },
    )
    .map(Expiration::At)
}
fn is_expiration(option: &[u8]) -> bool {
    [b"EX".as_slice(), b"PX", b"EXAT", b"PXAT"]
        .iter()
        .any(|name| eq(option, name))
}
fn parse<'a>(a: &'a [&'a [u8]], now: i64, set: bool) -> Result<Options<'a>, Reply> {
    let mut i = 2;
    let mut expiry = Expiration::Unchanged;
    let mut condition = 0;
    let mut saw_expiry = false;
    if !set {
        let option = cstring(a[i]);
        if is_expiration(option) {
            expiry = expiration(option, a[i + 1], now, false)?;
            i += 2;
        } else if eq(option, b"PERSIST") {
            expiry = Expiration::Clear;
            i += 1;
        }
        if i + 1 >= a.len() {
            return Err(arity(false));
        }
        if !eq(cstring(a[i]), b"FIELDS") {
            return Err(Reply::Error(
                "ERR Mandatory argument FIELDS is missing or not at the right position",
            ));
        }
    } else {
        while i < a.len() && !eq(cstring(a[i]), b"FIELDS") {
            let option = cstring(a[i]);
            if is_expiration(option) || eq(option, b"KEEPTTL") {
                if saw_expiry {
                    return Err(Reply::Error(
                        "ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified",
                    ));
                }
                saw_expiry = true;
                if !eq(option, b"KEEPTTL") {
                    if i + 1 >= a.len() {
                        return Err(Reply::Error("ERR missing expire time"));
                    }
                    expiry = expiration(option, a[i + 1], now, true)?;
                    i += 1;
                }
            } else if eq(option, b"FNX") || eq(option, b"FXX") {
                if condition != 0 {
                    return Err(Reply::Error(
                        "ERR Only one of FXX or FNX arguments can be specified",
                    ));
                }
                condition = if eq(option, b"FNX") { 1 } else { 2 };
            } else {
                return Err(Reply::OwnedError(format!(
                    "ERR unknown argument: {}",
                    String::from_utf8_lossy(option).replace(['\r', '\n'], " ")
                )));
            }
            i += 1;
        }
        if i + 3 >= a.len() {
            return Err(arity(true));
        }
        if !saw_expiry {
            expiry = Expiration::Clear;
        }
    }
    let count = collections::integer(a[i + 1])
        .ok()
        .filter(|n| *n > 0 && (!set || *n <= i32::MAX as i64))
        .ok_or(Reply::Error(if set {
            "ERR invalid number of fields"
        } else {
            "ERR Number of fields must be a positive integer"
        }))?;
    let fields = &a[i + 2..];
    if fields.len() != count as usize * if set { 2 } else { 1 } {
        return Err(if set {
            arity(true)
        } else {
            Reply::Error("ERR The `numfields` parameter must match the number of arguments")
        });
    }
    Ok(Options {
        fields,
        expiry,
        condition,
    })
}
pub(super) fn run(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply, Reply> {
    let set = eq(a[0], b"HSETEX");
    // HSETEX parses options before type lookup; HGETEX does the reverse.
    let parsed = if set {
        Some(parse(a, now, true)?)
    } else {
        None
    };
    let old = engine.get(a[1], now);
    let hash = match old.and_then(|entry| entry.value.collection()) {
        Some(Collection::Hash(hash)) => Some(hash),
        None if old.is_none() => None,
        _ => return Err(collections::WRONGTYPE),
    };
    let options = match parsed {
        Some(options) => options,
        None => parse(a, now, false)?,
    };
    if set
        && ((options.condition == 1
            && options
                .fields
                .chunks_exact(2)
                .any(|pair| hash.is_some_and(|h| h.contains_key(pair[0]))))
            || (options.condition == 2
                && options
                    .fields
                    .chunks_exact(2)
                    .any(|pair| hash.is_none_or(|h| !h.contains_key(pair[0])))))
    {
        return Ok(Reply::Integer(0));
    }
    let mut next = hash.map_or_else(|| Cow::Owned(Hash::default()), Cow::Borrowed);
    let mut changed = false;
    let mut replies = Vec::new();
    let mut bytes = 32;
    for field in options.fields.chunks_exact(if set { 2 } else { 1 }) {
        if set {
            next.to_mut()
                .insert_preserving_expiry(field[0].into(), field[1].into());
            changed = true;
        } else {
            let value = next.get(field[0]);
            bytes += value.map_or(0, |value| value.len()) + 32;
            if bytes > 2 * crate::protocol::MAX_FRAME {
                return Err(Reply::Error("ERR reply exceeds output limit"));
            }
            replies.push(Reply::Bulk(value.map(|value| value.to_vec())));
            if value.is_none() {
                continue;
            }
        }
        match options.expiry {
            Expiration::Unchanged => (),
            Expiration::Clear => {
                if next.expiration(field[0]).is_some() {
                    changed = true;
                    next.to_mut().set_expiration(field[0], None);
                }
            }
            Expiration::At(deadline) => {
                changed = true;
                if deadline <= now {
                    next.to_mut().remove(field[0]);
                } else {
                    next.to_mut().set_expiration(field[0], Some(deadline));
                }
            }
        }
    }
    let reply = if set {
        Reply::Integer(1)
    } else {
        Reply::Array(replies)
    };
    if !changed {
        return Ok(reply);
    }
    let transient = set && old.is_none() && next.is_empty();
    let entry = if next.is_empty() {
        None
    } else {
        Some(Entry {
            value: Value::from_collection(Collection::Hash(next.into_owned()))
                .map_err(|_| Reply::Error("ERR collection exceeds maximum size"))?,
            expires_at_ms: old.and_then(|entry| entry.expires_at_ms),
        })
    };
    let reply = engine.apply_typed(vec![(a[1].to_vec(), entry)], reply, submit);
    if transient && matches!(reply, Reply::Integer(1)) {
        engine.touch_watch(a[1]);
    }
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn expiration_reads_and_sets_are_atomic_on_rejected_admission() {
        let mut engine = Engine::new(1 << 20);
        engine.execute(
            &[
                b"HSETEX", b"h", b"PXAT", b"5000", b"FIELDS", b"1", b"a", b"old",
            ],
            0,
            |_| true,
        );
        let before = engine.snapshot(b"h");
        let watch = engine.watch(b"h", 0);
        for command in [
            vec![
                b"HGETEX".as_slice(),
                b"h",
                b"PXAT",
                b"0",
                b"FIELDS",
                b"2",
                b"a",
                b"a",
            ],
            vec![b"HGETEX", b"h", b"PERSIST", b"FIELDS", b"1", b"a"],
            vec![b"HSETEX", b"h", b"FIELDS", b"1", b"a", b"new"],
        ] {
            assert!(matches!(
                engine.execute(&command, 1, |_| false),
                Reply::Error(_)
            ));
            assert_eq!(engine.snapshot(b"h"), before);
            assert!(!engine.watch_dirty(b"h", &watch, 1));
        }
        assert_eq!(
            engine.execute(&[b"HGETEX", b"h", b"FIELDS", b"1", b"a"], 1, |_| panic!(
                "read admission"
            )),
            Reply::Array(vec![Reply::Bulk(Some(b"old".to_vec()))])
        );
        assert_eq!(
            engine.execute(
                &[b"HSETEX", b"h", b"FNX", b"FIELDS", b"1", b"a", b"new"],
                1,
                |_| panic!("condition admission")
            ),
            Reply::Integer(0)
        );
    }
    #[test]
    fn immediately_expired_creation_invalidates_watch_only_after_admission() {
        let mut engine = Engine::new(1 << 20);
        let watch = engine.watch(b"missing", 0);
        let command = [
            b"HSETEX".as_slice(),
            b"missing",
            b"PXAT",
            b"0",
            b"FIELDS",
            b"1",
            b"a",
            b"value",
        ];
        assert!(engine
            .transact(
                |engine, submit| vec![engine.execute(&command, 1, submit)],
                |_| false
            )
            .is_err());
        assert!(!engine.watch_dirty(b"missing", &watch, 1));
        assert_eq!(engine.execute(&command, 1, |_| true), Reply::Integer(1));
        assert!(engine.get(b"missing", 1).is_none());
        assert!(engine.watch_dirty(b"missing", &watch, 1));
    }
}
