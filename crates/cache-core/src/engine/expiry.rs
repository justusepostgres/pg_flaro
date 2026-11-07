//! Expiration commands share option validation and atomic persistence admission.
use super::{eq, integer, Engine, Mutations, ARITY};
use crate::protocol::Reply;

pub(super) fn execute(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    let name = args[0];
    let query = [b"TTL".as_slice(), b"PTTL", b"EXPIRETIME", b"PEXPIRETIME"]
        .iter()
        .any(|n| eq(name, n));
    let update = [b"EXPIRE".as_slice(), b"PEXPIRE", b"EXPIREAT", b"PEXPIREAT"]
        .iter()
        .any(|n| eq(name, n));
    if !query && !update {
        return None;
    }
    if (query && args.len() != 2) || (update && args.len() < 3) {
        return Some(ARITY);
    }
    Some(if query {
        query_expiry(engine, args, now)
    } else {
        set_expiry(engine, args, now, submit)
    })
}

fn query_expiry(engine: &Engine, args: &[&[u8]], now: i64) -> Reply {
    let Some(entry) = engine.get(args[1], now) else {
        return Reply::Integer(-2);
    };
    let Some(deadline) = entry.expires_at_ms else {
        return Reply::Integer(-1);
    };
    let absolute = eq(args[0], b"EXPIRETIME") || eq(args[0], b"PEXPIRETIME");
    let milliseconds = eq(args[0], b"PTTL") || eq(args[0], b"PEXPIRETIME");
    // Wider arithmetic avoids overflowing on valid far-future deadlines.
    let value = if absolute {
        deadline as i128
    } else {
        deadline as i128 - now as i128
    };
    Reply::Integer(if milliseconds {
        value as i64
    } else {
        ((value + 500) / 1000) as i64
    })
}

fn set_expiry(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Reply {
    let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
    for option in &args[3..] {
        if eq(option, b"NX") {
            nx = true;
        } else if eq(option, b"XX") {
            xx = true;
        } else if eq(option, b"GT") {
            gt = true;
        } else if eq(option, b"LT") {
            lt = true;
        } else {
            return Reply::OwnedError(format!(
                "ERR Unsupported option {}",
                String::from_utf8_lossy(option).replace(['\r', '\n'], " ")
            ));
        }
    }
    if nx && (xx || gt || lt) {
        return Reply::Error("ERR NX and XX, GT or LT options at the same time are not compatible");
    }
    if gt && lt {
        return Reply::Error("ERR GT and LT options at the same time are not compatible");
    }
    let Some(value) = integer(args[2]) else {
        return Reply::Error("ERR value is not an integer or out of range");
    };
    let seconds = eq(args[0], b"EXPIRE") || eq(args[0], b"EXPIREAT");
    let absolute = eq(args[0], b"EXPIREAT") || eq(args[0], b"PEXPIREAT");
    let Some(deadline) = value
        .checked_mul(if seconds { 1000 } else { 1 })
        .and_then(|v| v.checked_add(if absolute { 0 } else { now }))
    else {
        return Reply::OwnedError(format!(
            "ERR invalid expire time in '{}' command",
            String::from_utf8_lossy(args[0]).to_ascii_lowercase()
        ));
    };
    let Some(old) = engine.get(args[1], now) else {
        return Reply::Integer(0);
    };
    let previous = old.expires_at_ms;
    if (nx && previous.is_some())
        || (xx && previous.is_none())
        || (gt && previous.is_none_or(|t| deadline <= t))
        || (lt && previous.is_some_and(|t| deadline >= t))
    {
        return Reply::Integer(0);
    }
    let entry = if deadline <= now {
        None
    } else {
        let mut entry = old.clone();
        entry.expires_at_ms = Some(deadline);
        Some(entry)
    };
    engine.apply_typed(vec![(args[1].to_vec(), entry)], Reply::Integer(1), submit)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn conditions_and_absolute_expiry_are_atomic() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SET", b"k", b"v"], 1000, |_| true);
        assert_eq!(
            engine.execute(&[b"EXPIRE", b"k", b"10", b"GT"], 1000, |_| panic!(
                "no write"
            )),
            Reply::Integer(0)
        );
        assert_eq!(
            engine.execute(&[b"PEXPIREAT", b"k", b"2500", b"LT"], 1000, |_| true),
            Reply::Integer(1)
        );
        assert_eq!(
            engine.execute(&[b"EXPIRETIME", b"k"], 1000, |_| true),
            Reply::Integer(3)
        );
        assert_eq!(
            engine.execute(&[b"PTTL", b"k"], 1000, |_| true),
            Reply::Integer(1500)
        );
        let snapshot = engine.snapshot(b"k");
        for command in [
            vec![b"PEXPIREAT".as_slice(), b"k", b"3000", b"XX", b"GT"],
            vec![b"EXPIREAT", b"k", b"0", b"LT"],
        ] {
            assert!(matches!(
                engine.execute(&command, 1000, |_| false),
                Reply::Error(_)
            ));
            assert_eq!(engine.snapshot(b"k"), snapshot);
        }
        assert_eq!(
            engine.execute(&[b"PEXPIREAT", b"k", b"2500", b"GT"], 1000, |_| panic!(
                "no write"
            )),
            Reply::Integer(0)
        );
        assert_eq!(
            engine.execute(&[b"EXPIREAT", b"k", b"0", b"XX", b"LT"], 1000, |_| true),
            Reply::Integer(1)
        );
        assert_eq!(
            engine.execute(&[b"PEXPIRETIME", b"k"], 1000, |_| true),
            Reply::Integer(-2)
        );
    }
}
