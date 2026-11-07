//! Redis bit numbering is most-significant-bit first within each string byte.
use super::{eq, integer, Engine, Mutations, ARITY, SYNTAX};
use crate::{collections::WRONGTYPE, protocol::Reply};

pub(super) fn execute(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    if eq(args[0], b"BITCOUNT") {
        return Some(count(engine, args, now));
    }
    if eq(args[0], b"BITPOS") {
        return Some(position(engine, args, now));
    }
    let write = eq(args[0], b"SETBIT");
    if !write && !eq(args[0], b"GETBIT") {
        return None;
    }
    if args.len() != if write { 4 } else { 3 } {
        return Some(ARITY);
    }
    Some(bit(engine, args, now, submit, write))
}

fn bit(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
    write: bool,
) -> Reply {
    let Some(offset) = integer(args[2]).filter(|n| (0..(512_i64 * 1024 * 1024 * 8)).contains(n))
    else {
        return Reply::Error("ERR bit offset is not an integer or out of range");
    };
    let on = if write {
        let Some(on) = integer(args[3]).filter(|n| *n == 0 || *n == 1) else {
            return Reply::Error("ERR bit is not an integer or out of range");
        };
        on as u8
    } else {
        0
    };
    let old = engine.get(args[1], now);
    if old.is_some_and(|e| e.value.kind() != 0) {
        return WRONGTYPE;
    }
    let byte = offset as usize / 8;
    let shift = 7 - offset as usize % 8;
    let previous = old
        .and_then(|e| e.value.get(byte))
        .map_or(0, |b| (b >> shift) & 1);
    let reply = Reply::Integer(previous as i64);
    if !write || (old.is_some_and(|e| byte < e.value.len()) && previous == on) {
        return reply;
    }
    if byte >= crate::protocol::MAX_FRAME.saturating_sub(args[1].len()) {
        return Reply::Error("ERR string exceeds maximum size");
    }
    let expiry = old.and_then(|e| e.expires_at_ms);
    let mut value = old.map_or_else(Vec::new, |e| e.value.to_vec());
    value.resize(value.len().max(byte + 1), 0);
    value[byte] = (value[byte] & !(1 << shift)) | (on << shift);
    match engine.put(args[1], &value, expiry, submit) {
        Reply::Simple("OK") => reply,
        error => error,
    }
}

fn count(engine: &Engine, args: &[&[u8]], now: i64) -> Reply {
    if args.len() < 2 {
        return ARITY;
    }
    if !matches!(args.len(), 2 | 4 | 5) {
        return SYNTAX;
    }
    let range = if args.len() >= 4 {
        match (integer(args[2]), integer(args[3])) {
            (Some(start), Some(end)) => Some((start, end)),
            _ => return Reply::Error("ERR value is not an integer or out of range"),
        }
    } else {
        None
    };
    let bits = if args.len() == 5 {
        if eq(args[4], b"BIT") {
            true
        } else if eq(args[4], b"BYTE") {
            false
        } else {
            return SYNTAX;
        }
    } else {
        false
    };
    let Some(entry) = engine.get(args[1], now) else {
        return Reply::Integer(0);
    };
    if entry.value.kind() != 0 {
        return WRONGTYPE;
    }
    let value = entry.value.as_ref();
    if value.is_empty() {
        return Reply::Integer(0);
    }
    let (first, last) = if let Some((start, end)) = range {
        if start < 0 && end < 0 && start > end {
            return Reply::Integer(0);
        }
        let length = (value.len() * if bits { 8 } else { 1 }) as i64;
        let normalize = |n: i64| if n < 0 { (length + n).max(0) } else { n };
        let start = normalize(start);
        let end = normalize(end).min(length - 1);
        if start > end {
            return Reply::Integer(0);
        }
        if bits {
            (start as usize, end as usize)
        } else {
            (start as usize * 8, end as usize * 8 + 7)
        }
    } else {
        (0, value.len() * 8 - 1)
    };
    let first_byte = first / 8;
    let last_byte = last / 8;
    let mut total: u64 = value[first_byte..=last_byte]
        .iter()
        .map(|b| b.count_ones() as u64)
        .sum();
    total -= (value[first_byte] & !(0xff_u8 >> (first % 8))).count_ones() as u64;
    total -= (value[last_byte] & !(0xff_u8 << (7 - last % 8))).count_ones() as u64;
    Reply::Integer(total as i64)
}

fn position(engine: &Engine, args: &[&[u8]], now: i64) -> Reply {
    if args.len() < 3 {
        return ARITY;
    }
    let Some(bit) = integer(args[2]) else {
        return Reply::Error("ERR value is not an integer or out of range");
    };
    if bit != 0 && bit != 1 {
        return Reply::Error("ERR The bit argument must be 1 or 0.");
    }
    if args.len() > 6 {
        return SYNTAX;
    }
    let start = if args.len() >= 4 {
        let Some(n) = integer(args[3]) else {
            return Reply::Error("ERR value is not an integer or out of range");
        };
        n
    } else {
        0
    };
    let bits = if args.len() == 6 {
        if eq(args[5], b"BIT") {
            true
        } else if eq(args[5], b"BYTE") {
            false
        } else {
            return SYNTAX;
        }
    } else {
        false
    };
    let end = if args.len() >= 5 {
        let Some(n) = integer(args[4]) else {
            return Reply::Error("ERR value is not an integer or out of range");
        };
        Some(n)
    } else {
        None
    };
    let Some(entry) = engine.get(args[1], now) else {
        return Reply::Integer(if bit == 0 { 0 } else { -1 });
    };
    if entry.value.kind() != 0 {
        return WRONGTYPE;
    }
    let value = entry.value.as_ref();
    let length = (value.len() * if bits { 8 } else { 1 }) as i64;
    let normalize = |n: i64| if n < 0 { (length + n).max(0) } else { n };
    let start = normalize(start);
    let last = normalize(end.unwrap_or(length - 1)).min(length - 1);
    if start > last {
        return Reply::Integer(-1);
    }
    let (first, last) = if bits {
        (start as usize, last as usize)
    } else {
        (start as usize * 8, last as usize * 8 + 7)
    };
    for (index, byte) in value.iter().enumerate().take(last / 8 + 1).skip(first / 8) {
        let mut candidates = if bit == 1 { *byte } else { !byte };
        if index == first / 8 {
            candidates &= 0xff_u8 >> (first % 8);
        }
        if index == last / 8 {
            candidates &= 0xff_u8 << (7 - last % 8);
        }
        if candidates != 0 {
            return Reply::Integer((index * 8 + candidates.leading_zeros() as usize) as i64);
        }
    }
    Reply::Integer(if bit == 0 && end.is_none() {
        value.len() as i64 * 8
    } else {
        -1
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bit_mutation_is_atomic_and_retains_ttl() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SET", b"k", b"\x80", b"PX", b"100"], 0, |_| true);
        assert_eq!(
            engine.execute(&[b"GETBIT", b"k", b"0"], 1, |_| true),
            Reply::Integer(1)
        );
        let before = engine.snapshot(b"k");
        assert!(matches!(
            engine.execute(&[b"SETBIT", b"k", b"8", b"1"], 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"k"), before);
        assert_eq!(
            engine.execute(&[b"SETBIT", b"k", b"0", b"1"], 1, |_| panic!("unchanged")),
            Reply::Integer(1)
        );
        assert_eq!(
            engine.execute(&[b"SETBIT", b"k", b"8", b"1"], 1, |_| true),
            Reply::Integer(0)
        );
        assert_eq!(engine.snapshot(b"k").unwrap().value.as_ref(), b"\x80\x80");
        assert_eq!(engine.snapshot(b"k").unwrap().expires_at_ms, Some(100));
        assert_eq!(
            engine.execute(&[b"BITCOUNT", b"k", b"1", b"8", b"BIT"], 1, |_| true),
            Reply::Integer(1)
        );
        assert_eq!(
            engine.execute(&[b"SETBIT", b"missing", b"8", b"0"], 1, |_| true),
            Reply::Integer(0)
        );
        assert_eq!(engine.snapshot(b"missing").unwrap().value.as_ref(), b"\0\0");
    }
}
