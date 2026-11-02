//! Bounded reconstruction of ordered inline journal chunks.
use super::MAX_BLOCK;
pub const CHUNK_BYTES: usize = 6144;
pub type Block = (u64, u64, Vec<u8>);
#[derive(Default)]
pub struct Chunks {
    current: Option<(u64, u64, i32, i32, Vec<u8>)>,
}
impl Chunks {
    pub fn push(
        &mut self,
        previous: u64,
        sequence: u64,
        part: i32,
        parts: i32,
        bytes: &[u8],
    ) -> Result<Option<Block>, &'static str> {
        if parts < 1
            || parts as usize > MAX_BLOCK.div_ceil(CHUNK_BYTES)
            || part < 0
            || part >= parts
            || bytes.is_empty()
            || bytes.len() > CHUNK_BYTES
            || (part + 1 < parts && bytes.len() != CHUNK_BYTES)
        {
            return Err("invalid journal chunk");
        }
        if self.current.is_none() {
            if part != 0 {
                return Err("missing first journal chunk");
            }
            self.current = Some((previous, sequence, parts, 0, Vec::new()));
        }
        let (expected_previous, expected_sequence, expected_parts, next, payload) =
            self.current.as_mut().unwrap();
        if (
            *expected_previous,
            *expected_sequence,
            *expected_parts,
            *next,
        ) != (previous, sequence, parts, part)
        {
            return Err("out-of-order journal chunk");
        }
        if payload.len() + bytes.len() > MAX_BLOCK {
            return Err("journal block too large");
        }
        payload.extend_from_slice(bytes);
        *next += 1;
        if *next == parts {
            let (previous, sequence, _, _, payload) = self.current.take().unwrap();
            Ok(Some((previous, sequence, payload)))
        } else {
            Ok(None)
        }
    }
    pub fn finish(self) -> Result<(), &'static str> {
        if self.current.is_some() {
            Err("incomplete journal chunks")
        } else {
            Ok(())
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reconstructs_and_bounds_chunks() {
        let data = vec![37; CHUNK_BYTES * 2 + 17];
        let mut chunks = Chunks::default();
        for (part, bytes) in data.chunks(CHUNK_BYTES).enumerate() {
            let result = chunks.push(3, 9, part as i32, 3, bytes).unwrap();
            if part == 2 {
                assert_eq!(result, Some((3, 9, data.clone())));
            } else {
                assert!(result.is_none());
            }
        }
        chunks.finish().unwrap();
        assert!(Chunks::default().push(0, 1, 0, 3000, &[1]).is_err());
        assert!(Chunks::default()
            .push(0, 1, 0, 1, &vec![0; CHUNK_BYTES + 1])
            .is_err());
        assert!(Chunks::default().push(0, 1, 0, 2, &[1]).is_err());
    }
    #[test]
    fn rejects_missing_reordered_or_mixed_parts() {
        let full = vec![0; CHUNK_BYTES];
        assert!(Chunks::default().push(0, 1, 1, 2, &[1]).is_err());
        for (previous, sequence, part, parts) in
            [(1, 1, 1, 2), (0, 2, 1, 2), (0, 1, 0, 2), (0, 1, 1, 3)]
        {
            let mut chunks = Chunks::default();
            chunks.push(0, 1, 0, 2, &full).unwrap();
            assert!(chunks.push(previous, sequence, part, parts, &full).is_err());
        }
        let mut chunks = Chunks::default();
        chunks.push(0, 1, 0, 2, &full).unwrap();
        assert!(chunks.finish().is_err());
    }
}
