use super::{
    checksum, legacy, packed,
    read::{Reader, Result, BAD},
};
use crate::value::{Collection, Hash, SortedSet, Value};

pub(super) fn decode(payload: &[u8]) -> Result<Value> {
    if payload.len() < 10 {
        return Err(CHECKSUM);
    }
    let footer = payload.len() - 10;
    if u16::from_le_bytes(payload[footer..footer + 2].try_into().unwrap()) > 12
        || checksum(&payload[..payload.len() - 8])
            != u64::from_le_bytes(payload[payload.len() - 8..].try_into().unwrap())
    {
        return Err(CHECKSUM);
    }
    let mut reader = Reader::new(&payload[..footer]);
    let kind = reader.byte()?;
    let value = match kind {
        0 => Value::from(reader.string()?),
        1 | 2 | 3 | 4 | 5 | 22 | 24 => {
            let minimum = if kind == 24 { reader.uint(8)? } else { 0 };
            let count = reader.count()?;
            let mut items = Vec::new();
            for _ in 0..count {
                let expiry = if matches!(kind, 22 | 24) {
                    let ttl = reader.length()?;
                    let absolute = if ttl == 0 {
                        0
                    } else if kind == 24 {
                        minimum.checked_add(ttl - 1).ok_or(BAD)?
                    } else {
                        ttl
                    };
                    deadline(absolute)?
                } else {
                    None
                };
                let first = reader.string()?;
                match kind {
                    1 | 2 => items.push((first, None, None)),
                    3 | 5 => {
                        let score = if kind == 5 {
                            f64::from_bits(reader.uint(8)?)
                        } else {
                            match reader.byte()? {
                                255 => f64::NEG_INFINITY,
                                254 => f64::INFINITY,
                                253 => return Err(BAD),
                                n => score(reader.take(n as usize)?)?,
                            }
                        };
                        if score.is_nan() {
                            return Err(BAD);
                        }
                        items.push((first, Some(score.to_string().into_bytes()), None));
                    }
                    _ => items.push((first, Some(reader.string()?), expiry)),
                }
            }
            build(
                match kind {
                    1 => 2,
                    2 => 3,
                    3 | 5 => 4,
                    _ => 1,
                },
                items,
            )?
        }
        9 | 10 | 11 | 12 | 13 | 14 | 16 | 17 | 18 | 20 | 23 | 25 => {
            let items = if matches!(kind, 14 | 18) {
                let count = reader.count()?;
                let mut all = Vec::new();
                for _ in 0..count {
                    let container = if kind == 18 { reader.length()? } else { 2 };
                    let bytes = reader.string()?;
                    match container {
                        1 => all.push(bytes),
                        2 => all.extend(if kind == 18 {
                            packed::listpack(&bytes)?
                        } else {
                            legacy::ziplist(&bytes)?
                        }),
                        _ => return Err(BAD),
                    }
                    if all.len() > crate::protocol::MAX_FRAME / 2 {
                        return Err(BAD);
                    }
                }
                all
            } else {
                if kind == 25 {
                    reader.uint(8)?;
                }
                let bytes = reader.string()?;
                match kind {
                    9 => legacy::zipmap(&bytes)?,
                    10 | 12 | 13 => legacy::ziplist(&bytes)?,
                    11 => packed::intset(&bytes)?,
                    _ => packed::listpack(&bytes)?,
                }
            };
            let width = if matches!(kind, 23 | 25) {
                3
            } else if matches!(kind, 9 | 12 | 13 | 16 | 17) {
                2
            } else {
                1
            };
            if items.len() % width != 0 {
                return Err(BAD);
            }
            let mut tuples = Vec::new();
            let mut items = items.into_iter();
            while let Some(first) = items.next() {
                let second = if width >= 2 { items.next() } else { None };
                let expiry = if width == 3 {
                    let raw = items.next().unwrap();
                    let value = super::super::integer(&raw).ok_or(BAD)?;
                    deadline(u64::try_from(value).map_err(|_| BAD)?)?
                } else {
                    None
                };
                tuples.push((first, second, expiry));
            }
            build(
                match kind {
                    10 | 14 | 18 => 2,
                    11 | 20 => 3,
                    12 | 17 => 4,
                    _ => 1,
                },
                tuples,
            )?
        }
        _ => return Err(BAD),
    };
    if reader.pos != reader.bytes.len() {
        return Err(BAD);
    }
    Ok(value)
}
const CHECKSUM: crate::protocol::Reply =
    crate::protocol::Reply::Error("ERR DUMP payload version or checksum are wrong");
fn deadline(value: u64) -> Result<Option<i64>> {
    if value > 0x3fff_ffff_ffff {
        return Err(BAD);
    }
    Ok((value != 0).then_some(value as i64))
}
fn score(value: &[u8]) -> Result<f64> {
    let value = std::str::from_utf8(value)
        .map_err(|_| BAD)?
        .parse::<f64>()
        .map_err(|_| BAD)?;
    if value.is_nan() {
        Err(BAD)
    } else {
        Ok(value)
    }
}
type Item = (Vec<u8>, Option<Vec<u8>>, Option<i64>);
fn build(kind: u8, items: Vec<Item>) -> Result<Value> {
    let collection = match kind {
        1 => {
            let mut hash = Hash::default();
            for (field, value, expiry) in items {
                if hash.contains_key(field.as_slice()) {
                    return Err(BAD);
                }
                hash.insert(field.clone().into(), value.ok_or(BAD)?.into());
                hash.set_expiration(&field, expiry);
            }
            Collection::Hash(hash)
        }
        2 => Collection::List(
            items
                .into_iter()
                .map(|(value, _, _)| value.into())
                .collect(),
        ),
        3 => {
            let count = items.len();
            let set: std::collections::HashSet<_> = items
                .into_iter()
                .map(|(value, _, _)| value.into())
                .collect();
            if count != set.len() {
                return Err(BAD);
            }
            Collection::Set(set)
        }
        _ => {
            let mut set = SortedSet::default();
            for (member, value, _) in items {
                if set.scores.contains_key(member.as_slice()) {
                    return Err(BAD);
                }
                set.insert(member.into(), score(&value.ok_or(BAD)?)?);
            }
            Collection::ZSet(set)
        }
    };
    Value::from_collection(collection).map_err(|_| BAD)
}
