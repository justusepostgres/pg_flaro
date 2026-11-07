//! Bitfield operations are parsed in full and applied to a private string copy.
use super::{eq, integer, Engine, Mutations, ARITY, SYNTAX};
use crate::{collections::WRONGTYPE, protocol::Reply};

#[derive(Clone, Copy)]
enum Overflow {
    Wrap,
    Sat,
    Fail,
}
#[derive(Clone, Copy)]
enum Action {
    Get,
    Set(i64),
    Increment(i64),
}
struct Operation {
    signed: bool,
    width: usize,
    offset: usize,
    action: Action,
    overflow: Overflow,
}
const TYPE: Reply = Reply::Error("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.");
const OFFSET: Reply = Reply::Error("ERR bit offset is not an integer or out of range");

fn parse(args: &[&[u8]]) -> Result<Vec<Operation>, Reply> {
    let mut ops = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut i = 2;
    while i < args.len() {
        if eq(args[i], b"OVERFLOW") && i + 1 < args.len() {
            overflow = if eq(args[i + 1], b"WRAP") {
                Overflow::Wrap
            } else if eq(args[i + 1], b"SAT") {
                Overflow::Sat
            } else if eq(args[i + 1], b"FAIL") {
                Overflow::Fail
            } else {
                return Err(Reply::Error("ERR Invalid OVERFLOW type specified"));
            };
            i += 2;
            continue;
        }
        let get = eq(args[i], b"GET");
        let set = eq(args[i], b"SET");
        if (!get && !set && !eq(args[i], b"INCRBY")) || i + if get { 2 } else { 3 } >= args.len() {
            return Err(SYNTAX);
        }
        let kind = args[i + 1];
        let signed = match kind.first() {
            Some(b'i') => true,
            Some(b'u') => false,
            _ => return Err(TYPE),
        };
        let width = integer(&kind[1..])
            .filter(|n| *n >= 1 && *n <= if signed { 64 } else { 63 })
            .ok_or(TYPE)? as usize;
        let raw = args[i + 2];
        let scaled = raw.first() == Some(&b'#');
        let offset = integer(if scaled { &raw[1..] } else { raw })
            .and_then(|n| n.checked_mul(if scaled { width as i64 } else { 1 }))
            .filter(|n| (0..(512_i64 * 1024 * 1024 * 8)).contains(n))
            .ok_or(OFFSET)? as usize;
        let action = if get {
            Action::Get
        } else {
            let value = integer(args[i + 3])
                .ok_or(Reply::Error("ERR value is not an integer or out of range"))?;
            if set {
                Action::Set(value)
            } else {
                Action::Increment(value)
            }
        };
        ops.push(Operation {
            signed,
            width,
            offset,
            action,
            overflow,
        });
        i += if get { 3 } else { 4 };
    }
    Ok(ops)
}

fn read(value: &[u8], op: &Operation) -> i128 {
    let mut result = 0_i128;
    for offset in op.offset..op.offset + op.width {
        result = (result << 1)
            | value
                .get(offset / 8)
                .map_or(0, |b| ((b >> (7 - offset % 8)) & 1) as i128);
    }
    if op.signed && result & (1_i128 << (op.width - 1)) != 0 {
        result -= 1_i128 << op.width;
    }
    result
}
fn write(value: &mut [u8], op: &Operation, number: i128) {
    for bit in 0..op.width {
        let offset = op.offset + bit;
        let mask = 1 << (7 - offset % 8);
        let on = (number >> (op.width - 1 - bit)) & 1 != 0;
        value[offset / 8] = (value[offset / 8] & !mask) | if on { mask } else { 0 };
    }
}

pub(super) fn execute(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    let ro = eq(args[0], b"BITFIELD_RO");
    if !ro && !eq(args[0], b"BITFIELD") {
        return None;
    }
    if args.len() < 2 {
        return Some(ARITY);
    }
    Some(run(engine, args, now, submit, ro))
}
fn run(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
    ro: bool,
) -> Reply {
    let ops = match parse(args) {
        Ok(ops) => ops,
        Err(error) => return error,
    };
    let required = ops
        .iter()
        .filter(|op| !matches!(op.action, Action::Get))
        .map(|op| (op.offset + op.width).div_ceil(8))
        .max();
    if ro && required.is_some() {
        return Reply::Error("ERR BITFIELD_RO only supports the GET subcommand");
    }
    let old = engine.get(args[1], now);
    if old.is_some_and(|e| e.value.kind() != 0) {
        return WRONGTYPE;
    }
    if required.is_some_and(|len| len > crate::protocol::MAX_FRAME.saturating_sub(args[1].len())) {
        return Reply::Error("ERR string exceeds maximum size");
    }
    let original = old.map_or(&[][..], |e| e.value.as_ref());
    let expiry = old.and_then(|e| e.expires_at_ms);
    let mut value = if required.is_some() {
        original.to_vec()
    } else {
        Vec::new()
    };
    if let Some(len) = required {
        value.resize(value.len().max(len), 0);
    }
    let mut replies = Vec::with_capacity(ops.len());
    for op in ops {
        let previous = read(if required.is_some() { &value } else { original }, &op);
        let candidate = match op.action {
            Action::Get => {
                replies.push(Reply::Integer(previous as i64));
                continue;
            }
            // Redis treats unsigned SET's negative argument as a uint64 first.
            Action::Set(n) => {
                if op.signed {
                    n as i128
                } else {
                    n as u64 as i128
                }
            }
            Action::Increment(n) => previous + n as i128,
        };
        let min = if op.signed {
            -(1_i128 << (op.width - 1))
        } else {
            0
        };
        let max = if op.signed {
            (1_i128 << (op.width - 1)) - 1
        } else {
            (1_i128 << op.width) - 1
        };
        // Preserve Redis 8.2's wrapping-bound checks, including SET of an
        // extreme i64 into a narrow signed field (where SAT can choose max).
        let direction = if op.signed {
            let (base, increment) = match op.action {
                Action::Set(n) => (n, 0),
                Action::Increment(n) => (previous as i64, n),
                Action::Get => unreachable!(),
            };
            let max_increment = (max as i64).wrapping_sub(base);
            let min_increment = (min as i64).wrapping_sub(base);
            if base > max as i64
                || (op.width != 64 && increment > max_increment)
                || (base >= 0 && increment > 0 && increment > max_increment)
            {
                1
            } else if base < min as i64
                || (op.width != 64 && increment < min_increment)
                || (base < 0 && increment < 0 && increment < min_increment)
            {
                -1
            } else {
                0
            }
        } else if candidate > max {
            1
        } else if candidate < min {
            -1
        } else {
            0
        };
        let number = if direction != 0 {
            match op.overflow {
                Overflow::Fail => {
                    replies.push(Reply::Bulk(None));
                    continue;
                }
                Overflow::Sat => {
                    if direction > 0 {
                        max
                    } else {
                        min
                    }
                }
                Overflow::Wrap => (candidate - min).rem_euclid(1_i128 << op.width) + min,
            }
        } else {
            candidate
        };
        write(&mut value, &op, number);
        replies.push(Reply::Integer(if matches!(op.action, Action::Set(_)) {
            previous
        } else {
            number
        } as i64));
    }
    let reply = Reply::Array(replies);
    if required.is_none() || (old.is_some() && value == original) {
        return reply;
    }
    match engine.put(args[1], &value, expiry, submit) {
        Reply::Simple("OK") => reply,
        error => error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn writes_are_atomic_and_ordered_with_ttl_preserved() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SET", b"k", b"\0", b"PX", b"100"], 0, |_| true);
        let args: &[&[u8]] = &[
            b"BITFIELD",
            b"k",
            b"SET",
            b"i8",
            b"0",
            b"127",
            b"INCRBY",
            b"i8",
            b"0",
            b"1",
            b"GET",
            b"i8",
            b"0",
        ];
        let before = engine.snapshot(b"k");
        assert!(matches!(
            engine.execute(args, 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"k"), before);
        assert_eq!(
            engine.execute(args, 1, |_| true),
            Reply::Array(vec![
                Reply::Integer(0),
                Reply::Integer(-128),
                Reply::Integer(-128)
            ])
        );
        assert_eq!(engine.snapshot(b"k").unwrap().expires_at_ms, Some(100));
        let before = engine.snapshot(b"k");
        assert!(matches!(
            engine.execute(
                &[b"BITFIELD", b"k", b"SET", b"u8", b"0", b"1", b"bad"],
                1,
                |_| panic!("parse before write")
            ),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"k"), before);
        assert_eq!(
            engine.execute(
                &[
                    b"BITFIELD",
                    b"fail",
                    b"OVERFLOW",
                    b"FAIL",
                    b"SET",
                    b"u1",
                    b"8",
                    b"2"
                ],
                1,
                |_| true
            ),
            Reply::Array(vec![Reply::Bulk(None)])
        );
        assert_eq!(engine.snapshot(b"fail").unwrap().value.as_ref(), b"\0\0");
    }
}
