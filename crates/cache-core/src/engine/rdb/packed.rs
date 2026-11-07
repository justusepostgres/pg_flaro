//! Validate and unpack Redis listpacks, including back-lengths and element counts.
use super::read::{Reader, Result, BAD};
pub(super) fn listpack(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut reader = Reader::new(bytes);
    if reader.uint(4)? != bytes.len() as u64 {
        return Err(BAD);
    }
    let expected = reader.uint(2)?;
    let mut items = Vec::new();
    loop {
        let start = reader.pos;
        let head = reader.byte()?;
        if head == 255 {
            break;
        }
        let value = match head {
            0..=127 => (head as i64).to_string().into_bytes(),
            128..=191 => reader.take((head & 63) as usize)?.to_vec(),
            192..=223 => {
                let raw = (((head & 31) as i16) << 8) | reader.byte()? as i16;
                ((raw << 3) >> 3).to_string().into_bytes()
            }
            224..=239 => {
                let n = (((head & 15) as usize) << 8) | reader.byte()? as usize;
                reader.take(n)?.to_vec()
            }
            240 => {
                let n = reader.uint(4)? as usize;
                reader.take(n)?.to_vec()
            }
            241 => reader.signed(2)?,
            242 => reader.signed(3)?,
            243 => reader.signed(4)?,
            244 => reader.signed(8)?,
            _ => return Err(BAD),
        };
        let length = reader.pos - start;
        // Listpack's back-length uses big-endian groups, with the high bit set
        // on every byte after the first (read backwards during reverse scans).
        let width = if length <= 127 {
            1
        } else if length < 16383 {
            2
        } else if length < 2097151 {
            3
        } else if length < 268435455 {
            4
        } else {
            5
        };
        for i in 0..width {
            let group = ((length >> (7 * (width - i - 1))) & 127) as u8;
            if reader.byte()? != (group | if i == 0 { 0 } else { 128 }) {
                return Err(BAD);
            }
        }
        reader.charge(value.len())?;
        items.push(value);
    }
    if reader.pos != bytes.len() || (expected != 65535 && expected as usize != items.len()) {
        return Err(BAD);
    }
    Ok(items)
}

pub(super) fn intset(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut reader = Reader::new(bytes);
    let width = reader.uint(4)? as usize;
    if !matches!(width, 2 | 4 | 8) {
        return Err(BAD);
    }
    let count = reader.uint(4)? as usize;
    if count.checked_mul(width).and_then(|n| n.checked_add(8)) != Some(bytes.len()) {
        return Err(BAD);
    }
    let mut result = Vec::new();
    let mut previous = None;
    for _ in 0..count {
        let text = reader.signed(width)?;
        let value: i64 = std::str::from_utf8(&text).unwrap().parse().unwrap();
        if previous.is_some_and(|old| old >= value) {
            return Err(BAD);
        }
        previous = Some(value);
        reader.charge(text.len())?;
        result.push(text);
    }
    Ok(result)
}
