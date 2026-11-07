//! Encoding inspection and conversion; conversion preserves count caches and WATCH.
use super::*;

pub(super) fn execute(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply, Reply> {
    // Redis compares subcommands as C strings.
    let command = args[1].split(|b| *b == 0).next().unwrap();
    if eq(command, b"SIMD") {
        return if eq(args[2], b"ON") || eq(args[2], b"OFF") {
            // This implementation has no optional AVX2 path.
            Ok(Reply::Simple("disabled"))
        } else {
            Err(Reply::Error("ERR Argument must be ON or OFF"))
        };
    }
    let old = engine
        .get(args[2], now)
        .ok_or(Reply::Error("ERR The specified key does not exist"))?;
    let bytes = validate(old)?;
    if eq(command, b"ENCODING") {
        return Ok(Reply::Simple(if bytes[4] == 0 {
            "dense"
        } else {
            "sparse"
        }));
    }
    if eq(command, b"DECODE") {
        if bytes[4] != 1 {
            return Err(Reply::Error("ERR HLL encoding is not sparse"));
        }
        let mut parts = Vec::new();
        let mut cursor = HEADER;
        while cursor < bytes.len() {
            let op = bytes[cursor];
            cursor += 1;
            parts.push(if op & 0x80 != 0 {
                format!("v:{},{}", ((op >> 2) & 31) + 1, (op & 3) + 1)
            } else if op & 0x40 != 0 {
                // Redis's SDS terminator supplies zero for a truncated XZERO.
                let low = bytes.get(cursor).copied().unwrap_or(0);
                cursor += 1;
                format!("Z:{}", (((op as usize & 63) << 8) | low as usize) + 1)
            } else {
                format!("z:{}", op as usize + 1)
            });
        }
        return Ok(Reply::Bulk(Some(parts.join(" ").into_bytes())));
    }
    if !eq(command, b"GETREG") && !eq(command, b"TODENSE") {
        return Err(Reply::OwnedError(format!(
            "ERR Unknown PFDEBUG subcommand '{}'",
            String::from_utf8_lossy(command)
        )));
    }
    let sparse = bytes[4] == 1;
    let registers = decode(bytes)?;
    if sparse {
        let header: [u8; HEADER] = bytes[..HEADER].try_into().unwrap();
        let mut next = encode(&registers, header, true);
        // Conversion is a representation change, not a cardinality change.
        next[8..16].copy_from_slice(&header[8..16]);
        let expiry = old.expires_at_ms;
        match engine.put_string(args[2], &next, expiry, submit, false) {
            Reply::Simple("OK") => (),
            error => return Err(error),
        }
    }
    Ok(if eq(command, b"TODENSE") {
        Reply::Integer(i64::from(sparse))
    } else {
        Reply::Array(
            registers
                .into_iter()
                .map(|r| Reply::Integer(r as i64))
                .collect(),
        )
    })
}
