//! Runtime diagnostics. No database keys or persistence state are touched.
use super::*;

pub(super) fn execute() -> Reply {
    match check() {
        Ok(()) => Reply::Simple("OK"),
        Err(error) => error,
    }
}

fn check() -> Result<(), Reply> {
    let mut header = [0; HEADER];
    header[..4].copy_from_slice(b"HYLL");
    let mut random = 0x243f6a8885a308d3u64;
    let mut registers = vec![0; N];
    for _ in 0..1000 {
        for register in &mut registers {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            *register = (random & 63) as u8;
        }
        let packed = encode(&registers, header, true);
        let decoded = decode(&packed)?;
        for (index, (&expected, &actual)) in registers.iter().zip(&decoded).enumerate() {
            if actual != expected {
                return Err(Reply::OwnedError(format!(
                    "TESTFAILED Register {index} should be {expected} but is {actual}"
                )));
            }
        }
    }
    registers.fill(0);
    let mut dense = encode(&registers, header, true);
    let mut checkpoint = 1u64;
    for cardinality in 1u64..=10_000_000 {
        let element = (cardinality ^ 0x243f6a8885a308d3).to_le_bytes();
        if let Some(updated) = dense_add(&dense, &[&element]) {
            dense = updated;
        }
        let hash = hash(&element);
        let index = hash as usize & (N - 1);
        let rank = ((hash >> 14) | (1u64 << 50)).trailing_zeros() as u8 + 1;
        registers[index] = registers[index].max(rank);
        if cardinality != checkpoint {
            continue;
        }
        let sparse = encode(&registers, header, false);
        if cardinality < 1500 && sparse[4] != 1 {
            return Err(Reply::Error("TESTFAILED sparse encoding not used"));
        }
        let dense_registers = decode(&dense)?;
        let sparse_registers = decode(&sparse)?;
        let count = estimate(&dense_registers);
        if dense_registers != sparse_registers || count != estimate(&sparse_registers) {
            return Err(Reply::Error("TESTFAILED dense/sparse disagree"));
        }
        let error = count.abs_diff(cardinality as i64);
        let max_error = if cardinality == 10 {
            1
        } else {
            (1.04 / (N as f64).sqrt() * 6.0 * cardinality as f64).ceil() as u64
        };
        if error > max_error {
            return Err(Reply::OwnedError(format!(
                "TESTFAILED Too big error. card:{cardinality} abserr:{error}"
            )));
        }
        checkpoint *= 10;
    }
    Ok(())
}
