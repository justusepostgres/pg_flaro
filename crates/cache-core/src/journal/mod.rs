//! Versioned binary journal; each PostgreSQL row contains one atomic committed batch.
//! Prototype codec and rotation rules; the active server still uses direct updates.
pub mod chunks;
pub mod replay;
pub mod rotation;
use crate::engine::{Entry, Mutation};

const MAGIC: &[u8; 4] = b"PGC1";
const HEADER: usize = 17;
pub const MAX_ACCOUNTED_BYTES: usize = 16 * 1024 * 1024;
const MAX_BLOCK: usize = MAX_ACCOUNTED_BYTES + 8;

pub fn encode(mutations: &[Mutation]) -> Result<Vec<u8>, &'static str> {
    let mut size = 8usize;
    let mut accounted = 0usize;
    for m in mutations {
        let value_len = m.entry.as_ref().map_or(0, |e| e.value.len());
        if m.key.len() > u32::MAX as usize || value_len > u32::MAX as usize {
            return Err("journal field too large");
        }
        accounted = accounted
            .checked_add(m.bytes())
            .ok_or("journal accounting overflow")?;
        if accounted > MAX_ACCOUNTED_BYTES {
            return Err("journal batch too large");
        }
        size = size
            .checked_add(HEADER + m.key.len() + value_len)
            .ok_or("journal size overflow")?;
    }
    if size > MAX_BLOCK || mutations.len() > u32::MAX as usize {
        return Err("journal batch too large");
    }
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(mutations.len() as u32).to_le_bytes());
    for m in mutations {
        let value = m.entry.as_ref().map_or(&[][..], |e| e.value.as_ref());
        let expiry = m.entry.as_ref().and_then(|e| e.expires_at_ms);
        let flags = u8::from(m.entry.is_some())
            | (u8::from(expiry.is_some()) << 1)
            | (m.entry.as_ref().map_or(0, |e| e.value.kind()) << 2);
        out.push(flags);
        out.extend_from_slice(&(m.key.len() as u32).to_le_bytes());
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(&expiry.unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&m.key);
        out.extend_from_slice(value);
    }
    Ok(out)
}

pub fn decode(bytes: &[u8]) -> Result<Vec<Mutation>, &'static str> {
    if bytes.len() < 8 || bytes.len() > MAX_BLOCK || &bytes[..4] != MAGIC {
        return Err("invalid journal header");
    }
    let count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    if count > (bytes.len() - 8) / HEADER || count > MAX_ACCOUNTED_BYTES / 128 {
        return Err("invalid journal count");
    }
    let mut out = Vec::with_capacity(count);
    let mut pos = 8;
    let mut accounted = 0usize;
    for _ in 0..count {
        let h = bytes
            .get(pos..pos + HEADER)
            .ok_or("truncated journal record")?;
        let flags = h[0];
        let key_len = u32::from_le_bytes(h[1..5].try_into().unwrap()) as usize;
        let value_len = u32::from_le_bytes(h[5..9].try_into().unwrap()) as usize;
        let expiry = i64::from_le_bytes(h[9..17].try_into().unwrap());
        let kind = flags >> 2;
        let base = flags & 3;
        if kind > 4
            || !matches!(base, 0 | 1 | 3)
            || (base == 0 && (value_len != 0 || kind != 0))
            || (base != 3 && expiry != 0)
        {
            return Err("invalid journal flags");
        }
        pos += HEADER;
        let key_end = pos.checked_add(key_len).ok_or("journal key overflow")?;
        let value_end = key_end
            .checked_add(value_len)
            .ok_or("journal value overflow")?;
        let key = bytes.get(pos..key_end).ok_or("truncated journal key")?;
        let value = bytes
            .get(key_end..value_end)
            .ok_or("truncated journal value")?;
        let mutation = Mutation {
            key: key.into(),
            entry: if base == 0 {
                None
            } else {
                Some(Entry {
                    value: crate::value::Value::from_stored(kind, value)?,
                    expires_at_ms: (base == 3).then_some(expiry),
                })
            },
        };
        accounted = accounted
            .checked_add(mutation.bytes())
            .ok_or("journal accounting overflow")?;
        if accounted > MAX_ACCOUNTED_BYTES {
            return Err("journal batch too large");
        }
        out.push(mutation);
        pos = value_end;
    }
    if pos != bytes.len() {
        return Err("trailing journal data");
    }
    Ok(out)
}

/// Partition a coalesced snapshot without splitting any individual mutation.
pub fn batch_ranges(mutations: &[Mutation]) -> Result<Vec<std::ops::Range<usize>>, &'static str> {
    let mut ranges = Vec::new();
    let (mut start, mut bytes) = (0, 0);
    for (index, mutation) in mutations.iter().enumerate() {
        let cost = mutation.bytes();
        if cost > MAX_ACCOUNTED_BYTES {
            return Err("journal mutation too large");
        }
        if bytes + cost > MAX_ACCOUNTED_BYTES {
            ranges.push(start..index);
            start = index;
            bytes = 0;
        }
        bytes += cost;
    }
    if start < mutations.len() {
        ranges.push(start..mutations.len());
    }
    Ok(ranges)
}

#[cfg(test)]
mod tests {
    #[test]
    fn snapshot_partitions_preserve_mutation_boundaries() {
        let mutation = crate::engine::Mutation {
            key: b"k".as_slice().into(),
            entry: Some(crate::engine::Entry {
                value: vec![7; 1024 * 1024].into(),
                expires_at_ms: None,
            }),
        };
        let values = vec![mutation; 40];
        let ranges = super::batch_ranges(&values).unwrap();
        assert_eq!(ranges, vec![0..15, 15..30, 30..40]);
        for range in ranges {
            assert!(super::encode(&values[range]).is_ok());
        }
        assert!(super::batch_ranges(&[]).unwrap().is_empty());
    }
    use super::*;
    #[test]
    fn accounting_limits_and_impossible_record_counts_are_rejected() {
        let mutation = Mutation {
            key: b"k".as_slice().into(),
            entry: Some(Entry {
                value: vec![0; MAX_ACCOUNTED_BYTES].into(),
                expires_at_ms: None,
            }),
        };
        assert!(encode(&[mutation]).is_err());
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode(&bytes).is_err());
    }
    #[test]
    fn binary_empty_delete_and_expiry_round_trip() {
        let input = vec![
            Mutation {
                key: b"\0key\xff".as_slice().into(),
                entry: Some(Entry {
                    value: b"\r\n\xff".as_slice().into(),
                    expires_at_ms: Some(i64::MIN),
                }),
            },
            Mutation {
                key: b"empty".as_slice().into(),
                entry: Some(Entry {
                    value: b"".as_slice().into(),
                    expires_at_ms: None,
                }),
            },
            Mutation {
                key: b"deleted".as_slice().into(),
                entry: None,
            },
        ];
        let encoded = encode(&input).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), input.len());
        for (a, b) in input.iter().zip(decoded.iter()) {
            assert_eq!(a.key, b.key);
            assert_eq!(a.entry, b.entry);
        }
        for end in 0..encoded.len() {
            assert!(decode(&encoded[..end]).is_err());
        }
        let mut extra = encoded.clone();
        extra.push(0);
        assert!(decode(&extra).is_err());
        let mut corrupt = encoded;
        corrupt[8] = 2;
        assert!(decode(&corrupt).is_err());
    }
}
