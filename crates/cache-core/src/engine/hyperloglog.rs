//! Redis-compatible HYLL strings: MurmurHash64A, 14-bit registers and Ertl count.
mod debug;
mod selftest;
use super::{eq, Engine, Entry, Mutations};
use crate::{collections::WRONGTYPE, protocol::Reply};
const N: usize = 1 << 14;
const HEADER: usize = 16;
const DENSE_SIZE: usize = HEADER + N * 6 / 8;
const INVALID: Reply = Reply::Error("INVALIDOBJ Corrupted HLL object detected");
const NOT_HLL: Reply = Reply::Error("WRONGTYPE Key is not a valid HyperLogLog string value.");
unsafe extern "C" {
    fn pgcache_hll_round(value: f64) -> i64;
}

pub(super) fn execute(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    if ![
        b"PFADD".as_slice(),
        b"PFCOUNT",
        b"PFMERGE",
        b"PFDEBUG",
        b"PFSELFTEST",
    ]
    .iter()
    .any(|name| eq(a[0], name))
    {
        return None;
    }
    Some(run(engine, a, now, submit).unwrap_or_else(|error| error))
}
fn run(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply, Reply> {
    crate::cluster::validate_command(a)?;
    if eq(a[0], b"PFSELFTEST") {
        return Ok(selftest::execute());
    }
    if eq(a[0], b"PFDEBUG") {
        return debug::execute(engine, a, now, submit);
    }
    if eq(a[0], b"PFCOUNT") && a.len() == 2 {
        let Some(old) = engine.get(a[1], now) else {
            return Ok(Reply::Integer(0));
        };
        let bytes = validate(old)?;
        if bytes[15] & 0x80 == 0 {
            return Ok(Reply::Integer(i64::from_le_bytes(
                bytes[8..16].try_into().unwrap(),
            )));
        }
        let count = estimate(&decode(bytes)?);
        let mut next = bytes.to_vec();
        next[8..16].copy_from_slice(&count.to_le_bytes());
        let expiry = old.expires_at_ms;
        return persist(engine, a[1], next, expiry, Reply::Integer(count), submit);
    }
    let old = engine.get(a[1], now);
    let expiry = old.and_then(|e| e.expires_at_ms);
    let mut header = [0; HEADER];
    header[..4].copy_from_slice(b"HYLL");
    header[4] = 1;
    if let Some(old) = old {
        header.copy_from_slice(&validate(old)?[..HEADER]);
    }
    let mut dense = header[4] == 0;
    if eq(a[0], b"PFADD") && dense {
        let bytes = old.expect("dense sketch exists").value.as_ref();
        return match dense_add(bytes, &a[2..]) {
            Some(next) => persist(engine, a[1], next, expiry, Reply::Integer(1), submit),
            None => Ok(Reply::Integer(0)),
        };
    }
    let mut registers = vec![0; N];
    if eq(a[0], b"PFADD") {
        // Header validation is sufficient for a no-element call, as in Redis.
        if let Some(old) = old {
            if a.len() == 2 {
                return Ok(Reply::Integer(0));
            }
            registers = decode(old.value.as_ref())?;
        }
        let mut changed = old.is_none();
        for element in &a[2..] {
            let hash = hash(element);
            let index = (hash as usize) & (N - 1);
            let rank = ((hash >> 14) | (1u64 << 50)).trailing_zeros() as u8 + 1;
            if registers[index] < rank {
                registers[index] = rank;
                changed = true;
            }
        }
        if !changed {
            return Ok(Reply::Integer(0));
        }
        return persist(
            engine,
            a[1],
            encode(&registers, header, dense),
            expiry,
            Reply::Integer(1),
            submit,
        );
    }
    // PFMERGE includes the destination's existing sketch in its union.
    // Multi-key PFCOUNT never changes the input caches.
    for key in &a[1..] {
        let Some(entry) = engine.get(key, now) else {
            continue;
        };
        let bytes = validate(entry)?;
        dense |= bytes[4] == 0;
        for (target, value) in registers.iter_mut().zip(decode(bytes)?) {
            *target = (*target).max(value);
        }
    }
    if eq(a[0], b"PFCOUNT") {
        return Ok(Reply::Integer(estimate(&registers)));
    }
    persist(
        engine,
        a[1],
        encode(&registers, header, dense),
        expiry,
        Reply::Simple("OK"),
        submit,
    )
}
fn dense_add(bytes: &[u8], elements: &[&[u8]]) -> Option<Vec<u8>> {
    let mut changed: Option<Vec<u8>> = None;
    for element in elements {
        let hash = hash(element);
        let index = (hash as usize) & (N - 1);
        let rank = ((hash >> 14) | (1u64 << 50)).trailing_zeros() as u8 + 1;
        let bit = index * 6;
        let byte = HEADER + bit / 8;
        let shift = bit % 8;
        let current = changed.as_deref().unwrap_or(bytes);
        let word =
            current[byte] as u16 | ((current.get(byte + 1).copied().unwrap_or(0) as u16) << 8);
        if ((word >> shift) & 63) >= rank as u16 {
            continue;
        }
        let target = changed.get_or_insert_with(|| bytes.to_vec());
        let updated = (word & !(63 << shift)) | ((rank as u16) << shift);
        target[byte] = updated as u8;
        if let Some(next) = target.get_mut(byte + 1) {
            *next = (updated >> 8) as u8;
        }
    }
    if let Some(target) = &mut changed {
        target[15] |= 0x80;
    }
    changed
}
fn persist(
    engine: &mut Engine,
    key: &[u8],
    value: Vec<u8>,
    expiry: Option<i64>,
    reply: Reply,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply, Reply> {
    match engine.put(key, &value, expiry, submit) {
        Reply::Simple("OK") => Ok(reply),
        error => Err(error),
    }
}
fn validate(entry: &Entry) -> Result<&[u8], Reply> {
    if entry.value.kind() != 0 {
        return Err(WRONGTYPE);
    }
    let bytes = entry.value.as_ref();
    if bytes.len() < HEADER
        || &bytes[..4] != b"HYLL"
        || bytes[4] > 1
        || (bytes[4] == 0 && bytes.len() != DENSE_SIZE)
    {
        return Err(NOT_HLL);
    }
    Ok(bytes)
}
fn decode(bytes: &[u8]) -> Result<Vec<u8>, Reply> {
    let mut registers = vec![0; N];
    let payload = &bytes[HEADER..];
    if bytes[4] == 0 {
        for (i, register) in registers.iter_mut().enumerate() {
            let bit = i * 6;
            let word = payload[bit / 8] as u16
                | ((payload.get(bit / 8 + 1).copied().unwrap_or(0) as u16) << 8);
            *register = ((word >> (bit % 8)) & 63) as u8;
        }
    } else {
        let (mut cursor, mut index) = (0, 0);
        while cursor < payload.len() {
            let opcode = payload[cursor];
            cursor += 1;
            let (run, value) = if opcode & 0x80 != 0 {
                ((opcode as usize & 3) + 1, ((opcode >> 2) & 31) + 1)
            } else if opcode & 0x40 != 0 {
                let low = *payload.get(cursor).ok_or(INVALID)?;
                cursor += 1;
                ((((opcode as usize & 63) << 8) | low as usize) + 1, 0)
            } else {
                (opcode as usize + 1, 0)
            };
            let end = index + run;
            if end > N {
                return Err(INVALID);
            }
            registers[index..end].fill(value);
            index = end;
        }
        if index != N {
            return Err(INVALID);
        }
    }
    Ok(registers)
}
fn encode(registers: &[u8], mut header: [u8; HEADER], force_dense: bool) -> Vec<u8> {
    header[15] |= 0x80;
    let mut sparse = Vec::new();
    let mut index = 0;
    if !force_dense && registers.iter().all(|v| *v <= 32) {
        while index < N {
            let value = registers[index];
            let max_run = if value == 0 { N } else { 4 };
            let mut end = index + 1;
            while end < N && end - index < max_run && registers[end] == value {
                end += 1;
            }
            let length = end - index - 1;
            if value != 0 {
                sparse.push(0x80 | ((value - 1) << 2) | length as u8);
            } else if length < 64 {
                sparse.push(length as u8);
            } else {
                sparse.push(0x40 | (length >> 8) as u8);
                sparse.push(length as u8);
            }
            index = end;
        }
        if sparse.len() + HEADER <= 3000 {
            header[4] = 1;
            let mut result = header.to_vec();
            result.extend(sparse);
            return result;
        }
    }
    header[4] = 0;
    let mut result = vec![0; DENSE_SIZE];
    result[..HEADER].copy_from_slice(&header);
    for (index, value) in registers.iter().enumerate() {
        let bit = index * 6;
        let word = (*value as u16) << (bit % 8);
        result[HEADER + bit / 8] |= word as u8;
        if let Some(next) = result.get_mut(HEADER + bit / 8 + 1) {
            *next |= (word >> 8) as u8;
        }
    }
    result
}
fn hash(bytes: &[u8]) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    let mut hash = 0xadc83b19 ^ (bytes.len() as u64).wrapping_mul(M);
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        let mut word = u64::from_le_bytes(chunk.try_into().unwrap()).wrapping_mul(M);
        word ^= word >> 47;
        hash ^= word.wrapping_mul(M);
        hash = hash.wrapping_mul(M);
    }
    let tail = chunks.remainder();
    if !tail.is_empty() {
        for (i, byte) in tail.iter().enumerate() {
            hash ^= (*byte as u64) << (i * 8);
        }
        hash = hash.wrapping_mul(M);
    }
    hash ^= hash >> 47;
    hash = hash.wrapping_mul(M);
    hash ^ (hash >> 47)
}
fn estimate(registers: &[u8]) -> i64 {
    let mut histogram = [0u32; 64];
    for value in registers {
        histogram[*value as usize] += 1;
    }
    let m = N as f64;
    let mut z = m * tau((m - histogram[51] as f64) / m);
    for value in histogram[1..=50].iter().rev() {
        z = (z + *value as f64) * 0.5;
    }
    z += m * sigma(histogram[0] as f64 / m);
    // SAFETY: scalar double input, scalar integer output; native llroundl matches
    // Redis's rounding, including platform overflow behavior for forged sketches.
    unsafe { pgcache_hll_round(std::f64::consts::LOG2_E * 0.5 * m * m / z) }
}
fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let (mut y, mut z) = (1.0, x);
    loop {
        x *= x;
        let previous = z;
        z += x * y;
        y += y;
        if previous == z {
            return z;
        }
    }
}
fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let (mut y, mut z) = (1.0, 1.0 - x);
    loop {
        x = x.sqrt();
        let previous = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if previous == z {
            return z / 3.0;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dense_and_sparse_roundtrip_bounds() {
        let mut registers = vec![0; N];
        for index in (0..N).step_by(137) {
            registers[index] = (index % 31 + 1) as u8;
        }
        let mut header = [0; HEADER];
        header[..4].copy_from_slice(b"HYLL");
        for dense in [false, true] {
            assert_eq!(
                decode(&encode(&registers, header, dense)).unwrap(),
                registers
            );
        }
        let full: Vec<_> = (0..N).map(|i| (i % 64) as u8).collect();
        assert_eq!(decode(&encode(&full, header, true)).unwrap(), full);
        let empty = encode(&vec![0; N], header, true);
        let elements: Vec<_> = (0u32..20000).map(|n| n.to_le_bytes()).collect();
        let borrowed: Vec<_> = elements.iter().map(|v| v.as_slice()).collect();
        let updated = dense_add(&empty, &borrowed).unwrap();
        let mut expected = vec![0; N];
        for value in borrowed.iter() {
            let h = hash(value);
            let index = h as usize & (N - 1);
            expected[index] =
                expected[index].max(((h >> 14) | (1u64 << 50)).trailing_zeros() as u8 + 1);
        }
        assert_eq!(decode(&updated).unwrap(), expected);
        assert!(dense_add(&updated, &borrowed).is_none());
        let mut broken = header.to_vec();
        broken[4] = 1;
        broken.push(0x40);
        assert_eq!(decode(&broken), Err(INVALID));
    }
    #[test]
    fn writes_and_count_cache_are_admitted_before_visibility() {
        let mut engine = Engine::new(1 << 20);
        assert_eq!(
            engine.execute(&[b"PFADD", b"h", b"a", b"b"], 0, |_| true),
            Reply::Integer(1)
        );
        engine.execute(&[b"PEXPIREAT", b"h", b"5000"], 0, |_| true);
        let before = engine.snapshot(b"h");
        for args in [
            vec![b"PFADD".as_slice(), b"h", b"c"],
            vec![b"PFCOUNT", b"h"],
            vec![b"PFMERGE", b"h"],
            vec![b"PFDEBUG", b"TODENSE", b"h"],
            vec![b"PFDEBUG", b"GETREG", b"h"],
        ] {
            assert_eq!(
                engine.execute(&args, 1, |_| false),
                Reply::Error("MISCONF persistence backlog full or unavailable")
            );
            assert_eq!(engine.snapshot(b"h"), before);
        }
        assert_eq!(
            engine.execute(&[b"PFCOUNT", b"h"], 1, |_| true),
            Reply::Integer(2)
        );
        assert_eq!(
            engine.execute(&[b"PFCOUNT", b"h"], 1, |_| panic!("cached count")),
            Reply::Integer(2)
        );
        assert_eq!(engine.get(b"h", 1).unwrap().expires_at_ms, Some(5000));
        assert_eq!(
            engine.execute(&[b"PFADD", b"h", b"a"], 1, |_| panic!("duplicate")),
            Reply::Integer(0)
        );
        let watch = engine.watch(b"h", 1);
        let cached = engine.get(b"h", 1).unwrap().value.as_ref()[8..16].to_vec();
        let before = engine.snapshot(b"h");
        let rejected = engine.transact(
            |engine, submit| vec![engine.execute(&[b"PFDEBUG", b"TODENSE", b"h"], 1, submit)],
            |_| false,
        );
        assert!(rejected.is_err());
        assert_eq!(engine.snapshot(b"h"), before);
        assert!(!engine.watch_dirty(b"h", &watch, 1));
        assert_eq!(
            engine.execute(&[b"PFDEBUG", b"TODENSE", b"h"], 1, |_| true),
            Reply::Integer(1)
        );
        assert!(!engine.watch_dirty(b"h", &watch, 1));
        let entry = engine.get(b"h", 1).unwrap();
        assert_eq!(&entry.value.as_ref()[8..16], cached);
        assert_eq!(entry.expires_at_ms, Some(5000));
        assert_eq!(
            engine.execute(&[b"PFDEBUG", b"TODENSE", b"h"], 1, |_| panic!(
                "already dense"
            )),
            Reply::Integer(0)
        );
    }
}
