//! Native Redis DUMP/RESTORE. PostgreSQL persistence retains COL1/2 encoding.
mod decode;
mod legacy;
mod packed;
mod read;
mod restore;
#[cfg(test)]
mod tests;
use super::{eq, Engine, Mutations};
use crate::{
    protocol::Reply,
    value::{Collection, Value},
};
use std::sync::OnceLock;

pub(super) fn execute(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    if eq(args[0], b"RESTORE") {
        return Some(restore::execute(engine, args, now, submit));
    }
    if !eq(args[0], b"DUMP") {
        return None;
    }
    Some(if args.len() != 2 {
        Reply::Error("ERR wrong number of arguments for 'dump' command")
    } else if let Some(entry) = engine.get(args[1], now) {
        let payload = dump(&entry.value);
        if payload.len() > 2 * crate::protocol::MAX_FRAME - 32 {
            Reply::Error("ERR response exceeds configured limit")
        } else {
            Reply::Bulk(Some(payload))
        }
    } else {
        Reply::Bulk(None)
    })
}

fn length(out: &mut Vec<u8>, value: u64) {
    if value < 64 {
        out.push(value as u8);
    } else if value < 16384 {
        out.extend_from_slice(&((value as u16) | 0x4000).to_be_bytes());
    } else if let Ok(value) = u32::try_from(value) {
        out.push(0x80);
        out.extend_from_slice(&value.to_be_bytes());
    } else {
        out.push(0x81);
        out.extend_from_slice(&value.to_be_bytes());
    }
}

fn string(out: &mut Vec<u8>, value: &[u8]) {
    // Raw strings are valid for all binary and integer-looking values. Redis may
    // choose integer/LZF encodings, but RESTORE accepts this representation too.
    length(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn checksum(bytes: &[u8]) -> u64 {
    static TABLE: OnceLock<[u64; 256]> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
        std::array::from_fn(|i| {
            let mut value = i as u64;
            for _ in 0..8 {
                value = (value >> 1)
                    ^ if value & 1 != 0 {
                        0xad93_d235_94c9_35a9u64.reverse_bits()
                    } else {
                        0
                    };
            }
            value
        })
    });
    bytes.iter().fold(0u64, |crc, byte| {
        (crc >> 8) ^ table[((crc as u8) ^ byte) as usize]
    })
}

fn dump(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        Value::String(bytes) => {
            out.push(0);
            string(&mut out, bytes);
        }
        Value::Collection { data, .. } => match data.as_ref() {
            Collection::List(items) => {
                out.push(1); // Legacy plain list is still accepted by RDB v12.
                length(&mut out, items.len() as u64);
                for item in items {
                    string(&mut out, item);
                }
            }
            Collection::Set(items) => {
                out.push(2);
                length(&mut out, items.len() as u64);
                for item in items {
                    string(&mut out, item);
                }
            }
            Collection::ZSet(items) => {
                out.push(5); // ZSET_2 uses little-endian IEEE754 scores.
                length(&mut out, items.scores.len() as u64);
                for (member, score) in &items.scores {
                    string(&mut out, member);
                    out.extend_from_slice(&score.0.to_le_bytes());
                }
            }
            Collection::Hash(items) => {
                let minimum = items.next_expiration();
                out.push(if minimum.is_some() { 24 } else { 4 });
                if let Some(minimum) = minimum {
                    out.extend_from_slice(&minimum.to_le_bytes());
                }
                length(&mut out, items.len() as u64);
                for (field, value) in items.iter() {
                    if let Some(minimum) = minimum {
                        length(
                            &mut out,
                            items
                                .expiration(field)
                                .map_or(0, |expiry| (expiry - minimum + 1) as u64),
                        );
                    }
                    string(&mut out, field);
                    string(&mut out, value);
                }
            }
        },
    }
    out.extend_from_slice(&12u16.to_le_bytes());
    let crc = checksum(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

#[cfg(test)]
mod export_tests {
    use super::*;
    #[test]
    fn redis_crc_and_length_boundary_vectors() {
        assert_eq!(checksum(b"123456789"), 0xe9c6_d914_c4b8_d9ca);
        for (number, expected) in [
            (0, vec![0]),
            (63, vec![63]),
            (64, vec![0x40, 0x40]),
            (16383, vec![0x7f, 0xff]),
            (16384, vec![0x80, 0, 0, 0x40, 0]),
            (u32::MAX as u64 + 1, vec![0x81, 0, 0, 0, 1, 0, 0, 0, 0]),
        ] {
            let mut out = Vec::new();
            length(&mut out, number);
            assert_eq!(out, expected);
        }
    }
    #[test]
    fn dump_is_read_only_and_excludes_expired_data() {
        let mut engine = Engine::new(1 << 20);
        engine.execute(&[b"SET", b"k", b"\0\xff", b"PX", b"10"], 0, |_| true);
        let before = engine.snapshot(b"k");
        let watch = engine.watch(b"k", 0);
        let Reply::Bulk(Some(payload)) =
            engine.execute(&[b"DUMP", b"k"], 1, |_| panic!("read only"))
        else {
            panic!("expected payload")
        };
        assert_eq!(&payload[..6], &[0, 2, 0, 255, 12, 0]);
        assert_eq!(engine.snapshot(b"k"), before);
        assert!(!engine.watch_dirty(b"k", &watch, 1));
        assert_eq!(
            engine.execute(&[b"DUMP", b"k"], 10, |_| true),
            Reply::Bulk(None)
        );
    }
}
