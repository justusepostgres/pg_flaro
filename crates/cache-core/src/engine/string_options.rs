//! SET/GETEX option parsing, with Redis's validation and lookup ordering.
use super::{eq, integer, Engine, Mutations, ARITY, SYNTAX};
use crate::{collections::WRONGTYPE, protocol::Reply};

#[derive(Clone, Copy, PartialEq)]
enum ExpiryKind {
    Ex,
    Px,
    Exat,
    Pxat,
}
#[derive(Default)]
struct Options<'a> {
    condition: Option<bool>, // true: NX, false: XX
    get: bool,
    keep: bool,
    persist: bool,
    expiry: Option<(ExpiryKind, &'a [u8])>,
}

fn parse<'a>(args: &[&'a [u8]], is_set: bool) -> Result<Options<'a>, Reply> {
    let mut result = Options::default();
    let mut i = if is_set { 3 } else { 2 };
    while i < args.len() {
        let opt = args[i];
        if is_set && (eq(opt, b"NX") || eq(opt, b"XX")) {
            let nx = eq(opt, b"NX");
            if result.condition.is_some_and(|old| old != nx) {
                return Err(SYNTAX);
            }
            result.condition = Some(nx);
        } else if is_set && eq(opt, b"GET") {
            result.get = true;
        } else if is_set && eq(opt, b"KEEPTTL") && result.expiry.is_none() {
            result.keep = true;
        } else if !is_set && eq(opt, b"PERSIST") && result.expiry.is_none() {
            result.persist = true;
        } else {
            let kind = if eq(opt, b"EX") {
                ExpiryKind::Ex
            } else if eq(opt, b"PX") {
                ExpiryKind::Px
            } else if eq(opt, b"EXAT") {
                ExpiryKind::Exat
            } else if eq(opt, b"PXAT") {
                ExpiryKind::Pxat
            } else {
                return Err(SYNTAX);
            };
            if result.keep
                || result.persist
                || i + 1 == args.len()
                || result.expiry.is_some_and(|(old, _)| old != kind)
            {
                return Err(SYNTAX);
            }
            i += 1;
            result.expiry = Some((kind, args[i]));
        }
        i += 1;
    }
    Ok(result)
}

fn deadline(options: &Options<'_>, now: i64, is_set: bool) -> Result<Option<i64>, Reply> {
    let Some((kind, value)) = options.expiry else {
        return Ok(None);
    };
    let value =
        integer(value).ok_or(Reply::Error("ERR value is not an integer or out of range"))?;
    let invalid = Reply::Error(if is_set {
        "ERR invalid expire time in 'set' command"
    } else {
        "ERR invalid expire time in 'getex' command"
    });
    if value <= 0 {
        return Err(invalid);
    }
    let value = value
        .checked_mul(if matches!(kind, ExpiryKind::Ex | ExpiryKind::Exat) {
            1000
        } else {
            1
        })
        .and_then(|v| {
            v.checked_add(if matches!(kind, ExpiryKind::Ex | ExpiryKind::Px) {
                now
            } else {
                0
            })
        });
    value.map(Some).ok_or(invalid)
}

pub(super) fn execute(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    let is_set = eq(args[0], b"SET");
    if !is_set && !eq(args[0], b"GETEX") {
        return None;
    }
    if args.len() < if is_set { 3 } else { 2 } {
        return Some(ARITY);
    }
    Some(run(engine, args, now, submit, is_set))
}

fn run(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
    is_set: bool,
) -> Reply {
    let options = match parse(args, is_set) {
        Ok(options) => options,
        Err(error) => return error,
    };
    // SET validates its deadline before lookup; GETEX validates after lookup.
    let expiry = if is_set {
        match deadline(&options, now, true) {
            Ok(t) => t,
            Err(error) => return error,
        }
    } else {
        None
    };
    let old = engine.get(args[1], now);
    if !is_set && old.is_none() {
        return Reply::Bulk(None);
    }
    if (!is_set || options.get) && old.is_some_and(|e| e.value.kind() != 0) {
        return WRONGTYPE;
    }
    let reply = if !is_set || options.get {
        Reply::Bulk(old.map(|e| e.value.to_vec()))
    } else {
        Reply::Simple("OK")
    };
    if is_set {
        if options.condition.is_some_and(|nx| nx == old.is_some()) {
            return if options.get {
                reply
            } else {
                Reply::Bulk(None)
            };
        }
        let expiry = if options.keep {
            old.and_then(|e| e.expires_at_ms)
        } else {
            expiry
        };
        return match engine.put(args[1], args[2], expiry, submit) {
            Reply::Simple("OK") => reply,
            error => error,
        };
    }
    let expiry = match deadline(&options, now, false) {
        Ok(t) => t,
        Err(error) => return error,
    };
    let old = old.unwrap();
    if expiry.is_none() && (!options.persist || old.expires_at_ms.is_none()) {
        return reply;
    }
    let entry = if expiry.is_some_and(|t| t <= now) {
        None
    } else {
        let mut entry = old.clone();
        entry.expires_at_ms = expiry;
        Some(entry)
    };
    engine.apply_typed(vec![(args[1].to_vec(), entry)], reply, submit)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejected_options_preserve_value_and_ttl() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SET", b"k", b"old", b"PXAT", b"5000"], 1000, |_| true);
        let original = engine.snapshot(b"k");
        for command in [
            vec![b"SET".as_slice(), b"k", b"new", b"GET", b"KEEPTTL"],
            vec![b"GETEX", b"k", b"PERSIST"],
            vec![b"GETEX", b"k", b"PXAT", b"1"],
            vec![b"GETEX", b"k", b"EX", b"10"],
        ] {
            assert!(matches!(
                engine.execute(&command, 1000, |_| false),
                Reply::Error(_)
            ));
            assert_eq!(engine.snapshot(b"k"), original);
        }
        assert_eq!(
            engine.execute(&[b"SET", b"k", b"new", b"NX", b"GET"], 1000, |_| panic!(
                "condition failed"
            )),
            Reply::Bulk(Some(b"old".to_vec()))
        );
        assert_eq!(
            engine.execute(&[b"SET", b"k", b"new", b"GET", b"KEEPTTL"], 1000, |_| true),
            Reply::Bulk(Some(b"old".to_vec()))
        );
        assert_eq!(engine.snapshot(b"k").unwrap().expires_at_ms, Some(5000));
        assert_eq!(
            engine.execute(&[b"GETEX", b"k", b"PXAT", b"1"], 1000, |_| true),
            Reply::Bulk(Some(b"new".to_vec()))
        );
        assert!(engine.snapshot(b"k").is_none());
        assert_eq!(
            engine.execute(&[b"GETEX", b"missing", b"EX", b"bad"], 1000, |_| panic!(
                "no write"
            )),
            Reply::Bulk(None)
        );
        assert!(matches!(
            engine.execute(&[b"SET", b"missing", b"v", b"EX", b"bad"], 1000, |_| false),
            Reply::Error(_)
        ));
    }
}
