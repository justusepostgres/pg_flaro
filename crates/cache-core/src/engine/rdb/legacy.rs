//! Earlier Redis packed representations still accepted by native RESTORE.
use super::read::{Reader, Result, BAD};
pub(super) fn ziplist(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut r = Reader::new(bytes);
    if r.uint(4)? != bytes.len() as u64 {
        return Err(BAD);
    }
    let tail = r.uint(4)? as usize;
    let expected = r.uint(2)?;
    let mut previous = 0;
    let mut last = 10;
    let mut items = Vec::new();
    loop {
        let start = r.pos;
        let head = r.byte()?;
        if head == 255 {
            break;
        }
        let prev = if head == 254 {
            r.uint(4)? as usize
        } else {
            head as usize
        };
        if prev != previous {
            return Err(BAD);
        }
        let encoding = r.byte()?;
        let value = match encoding {
            0..=63 => r.take(encoding as usize)?.to_vec(),
            64..=127 => {
                let size = (((encoding & 63) as usize) << 8) | r.byte()? as usize;
                r.take(size)?.to_vec()
            }
            128 => {
                let size = u32::from_be_bytes(r.take(4)?.try_into().unwrap()) as usize;
                r.take(size)?.to_vec()
            }
            192 => r.signed(2)?,
            208 => r.signed(4)?,
            224 => r.signed(8)?,
            240 => r.signed(3)?,
            241..=253 => ((encoding & 15) - 1).to_string().into_bytes(),
            254 => r.signed(1)?,
            _ => return Err(BAD),
        };
        r.charge(value.len())?;
        previous = r.pos - start;
        last = start;
        items.push(value);
    }
    if r.pos != bytes.len()
        || tail != last
        || (expected != 65535 && expected as usize != items.len())
    {
        return Err(BAD);
    }
    Ok(items)
}
pub(super) fn zipmap(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut r = Reader::new(bytes);
    let expected = r.byte()?;
    let mut items = Vec::new();
    loop {
        let head = r.byte()?;
        if head == 255 {
            break;
        }
        let n = if head == 254 {
            r.uint(4)? as usize
        } else {
            head as usize
        };
        r.charge(n)?;
        let key = r.take(n)?.to_vec();
        let head = r.byte()?;
        if head == 255 {
            return Err(BAD);
        }
        let n = if head == 254 {
            r.uint(4)? as usize
        } else {
            head as usize
        };
        let free = r.byte()? as usize;
        r.charge(n)?;
        let value = r.take(n)?.to_vec();
        r.take(free)?;
        items.push(key);
        items.push(value);
    }
    if r.pos != bytes.len() || (expected < 254 && expected as usize != items.len() / 2) {
        return Err(BAD);
    }
    Ok(items)
}
