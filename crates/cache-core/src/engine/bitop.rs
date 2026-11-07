//! Multi-source bitmap operations read all inputs before replacing the destination.
use super::{eq, Engine, Mutations, ARITY, SYNTAX};
use crate::{collections::WRONGTYPE, protocol::Reply};

pub(super) fn execute(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    if !eq(args[0], b"BITOP") {
        return None;
    }
    if args.len() < 4 {
        return Some(ARITY);
    }
    Some(run(engine, args, now, submit))
}

fn run(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Reply {
    let op = args[1].to_ascii_uppercase();
    if ![
        b"AND".as_slice(),
        b"OR",
        b"XOR",
        b"NOT",
        b"DIFF",
        b"DIFF1",
        b"ANDOR",
        b"ONE",
    ]
    .contains(&op.as_slice())
    {
        return SYNTAX;
    }
    if op == b"NOT" && args.len() != 4 {
        return Reply::Error("ERR BITOP NOT must be called with a single source key.");
    }
    if matches!(op.as_slice(), b"DIFF" | b"DIFF1" | b"ANDOR") && args.len() < 5 {
        return Reply::OwnedError(format!(
            "ERR BITOP {} must be called with at least two source keys.",
            String::from_utf8_lossy(&op)
        ));
    }
    let mut inputs = Vec::with_capacity(args.len() - 3);
    let mut length = 0;
    for key in &args[3..] {
        let value = match engine.get(key, now) {
            Some(entry) if entry.value.kind() != 0 => return WRONGTYPE,
            Some(entry) => entry.value.as_ref(),
            None => &[],
        };
        length = length.max(value.len());
        inputs.push(value);
    }
    if length == 0 {
        return if engine.get(args[2], now).is_some() {
            engine.apply_typed(vec![(args[2].to_vec(), None)], Reply::Integer(0), submit)
        } else {
            Reply::Integer(0)
        };
    }
    if length > crate::protocol::MAX_FRAME.saturating_sub(args[2].len()) {
        return Reply::Error("ERR string exceeds maximum size");
    }
    let mut result = vec![0_u8; length];
    for (index, output) in result.iter_mut().enumerate() {
        let first = inputs[0].get(index).copied().unwrap_or(0);
        let mut value = first;
        let mut others = 0;
        let mut seen = first;
        let mut repeated = 0;
        for input in &inputs[1..] {
            let byte = input.get(index).copied().unwrap_or(0);
            match op.as_slice() {
                b"AND" => value &= byte,
                b"OR" => value |= byte,
                b"XOR" => value ^= byte,
                b"ONE" => {
                    repeated |= seen & byte;
                    seen |= byte;
                }
                _ => others |= byte,
            }
        }
        *output = match op.as_slice() {
            b"NOT" => !first,
            b"DIFF" => first & !others,
            b"DIFF1" => !first & others,
            b"ANDOR" => first & others,
            b"ONE" => seen & !repeated,
            _ => value,
        };
    }
    match engine.put(args[2], &result, None, submit) {
        Reply::Simple("OK") => Reply::Integer(length as i64),
        error => error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replacement_and_deletion_are_atomic() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SET", b"a", b"\xf0", b"PX", b"100"], 0, |_| true);
        engine.execute(&[b"SET", b"b", b"\x0f\xff"], 0, |_| true);
        let before = engine.snapshot(b"a");
        for args in [
            vec![b"BITOP".as_slice(), b"OR", b"a", b"a", b"b"],
            vec![b"BITOP", b"OR", b"a", b"missing"],
        ] {
            assert!(matches!(
                engine.execute(&args, 1, |_| false),
                Reply::Error(_)
            ));
            assert_eq!(engine.snapshot(b"a"), before);
        }
        assert_eq!(
            engine.execute(&[b"BITOP", b"OR", b"a", b"a", b"b"], 1, |_| true),
            Reply::Integer(2)
        );
        assert_eq!(engine.snapshot(b"a").unwrap().value.as_ref(), b"\xff\xff");
        assert_eq!(engine.snapshot(b"a").unwrap().expires_at_ms, None);
        assert_eq!(
            engine.execute(&[b"BITOP", b"OR", b"a", b"missing"], 1, |_| true),
            Reply::Integer(0)
        );
        assert!(engine.snapshot(b"a").is_none());
    }
}
