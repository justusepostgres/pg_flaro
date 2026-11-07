use super::{
    super::{eq, integer, Engine, Entry, Mutations, SYNTAX},
    decode,
};
use crate::protocol::Reply;

pub(super) fn execute(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Reply {
    run(engine, args, now, submit).unwrap_or_else(|error| error)
}
fn run(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply, Reply> {
    if args.len() < 4 {
        return Err(Reply::Error(
            "ERR wrong number of arguments for 'restore' command",
        ));
    }
    let mut replace = false;
    let mut absolute = false;
    let mut idle = false;
    let mut frequency = false;
    let mut i = 4;
    while i < args.len() {
        // Redis's option parser uses C-string comparisons.
        let option = args[i].split(|byte| *byte == 0).next().unwrap();
        if eq(option, b"REPLACE") {
            replace = true;
        } else if eq(option, b"ABSTTL") {
            absolute = true;
        } else if eq(option, b"IDLETIME") && i + 1 < args.len() && !frequency {
            i += 1;
            let value = number(args[i])?;
            if value < 0 {
                return Err(Reply::Error("ERR Invalid IDLETIME value, must be >= 0"));
            }
            idle = true;
        } else if eq(option, b"FREQ") && i + 1 < args.len() && !idle {
            i += 1;
            let value = number(args[i])?;
            if !(0..=255).contains(&value) {
                return Err(Reply::Error(
                    "ERR Invalid FREQ value, must be >= 0 and <= 255",
                ));
            }
            frequency = true;
        } else {
            return Err(SYNTAX);
        }
        i += 1;
    }
    if !replace && engine.get(args[1], now).is_some() {
        return Err(Reply::Error("BUSYKEY Target key name already exists."));
    }
    let ttl = number(args[2])?;
    if ttl < 0 {
        return Err(Reply::Error("ERR Invalid TTL value, must be >= 0"));
    }
    let value = decode::decode(args[3])?;
    // Redis uses signed addition here; match its 64-bit deadline behavior.
    let expires = (ttl != 0).then(|| if absolute { ttl } else { ttl.wrapping_add(now) });
    if expires.is_some_and(|deadline| deadline <= now) {
        if engine.get(args[1], now).is_none() {
            return Ok(Reply::Simple("OK"));
        }
        return Ok(engine.apply_typed(vec![(args[1].to_vec(), None)], Reply::Simple("OK"), submit));
    }
    // All validation precedes admission. IDLETIME/FREQ are validated above;
    // access/eviction metadata is not yet retained by this engine.
    Ok(engine.apply_typed(
        vec![(
            args[1].to_vec(),
            Some(Entry {
                value,
                expires_at_ms: expires,
            }),
        )],
        Reply::Simple("OK"),
        submit,
    ))
}
fn number(value: &[u8]) -> Result<i64, Reply> {
    integer(value).ok_or(Reply::Error("ERR value is not an integer or out of range"))
}
