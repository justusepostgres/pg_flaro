//! Bounded binary primitives shared by native RDB and packed collection readers.
use crate::protocol::Reply;
pub(super) const BAD: Reply = Reply::Error("ERR Bad data format");
pub(super) type Result<T> = std::result::Result<T, Reply>;
pub(super) struct Reader<'a> {
    pub bytes: &'a [u8],
    pub pos: usize,
    budget: usize,
}
impl<'a> Reader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            pos: 0,
            budget: crate::server::MAX_PENDING_BYTES / 2,
        }
    }
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or(BAD)?;
        let data = self.bytes.get(self.pos..end).ok_or(BAD)?;
        self.pos = end;
        Ok(data)
    }
    pub fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn uint(&mut self, n: usize) -> Result<u64> {
        let mut bytes = [0; 8];
        bytes
            .get_mut(..n)
            .ok_or(BAD)?
            .copy_from_slice(self.take(n)?);
        Ok(u64::from_le_bytes(bytes))
    }
    pub fn signed(&mut self, n: usize) -> Result<Vec<u8>> {
        let value = self.uint(n)?;
        let shift = 64 - n * 8;
        Ok((((value << shift) as i64) >> shift)
            .to_string()
            .into_bytes())
    }
    fn length_head(&mut self) -> Result<(u64, bool)> {
        let head = self.byte()?;
        match head >> 6 {
            0 => Ok((head as u64, false)),
            1 => Ok(((((head & 63) as u64) << 8) | self.byte()? as u64, false)),
            2 => match head {
                0x80 => Ok((
                    u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as u64,
                    false,
                )),
                0x81 => Ok((u64::from_be_bytes(self.take(8)?.try_into().unwrap()), false)),
                _ => Err(BAD),
            },
            _ => Ok(((head & 63) as u64, true)),
        }
    }
    pub fn length(&mut self) -> Result<u64> {
        let (n, encoded) = self.length_head()?;
        if encoded {
            Err(BAD)
        } else {
            Ok(n)
        }
    }
    pub fn count(&mut self) -> Result<usize> {
        let n = usize::try_from(self.length()?).map_err(|_| BAD)?;
        if n > crate::protocol::MAX_FRAME {
            return Err(BAD);
        }
        Ok(n)
    }
    pub fn charge(&mut self, n: usize) -> Result<()> {
        self.budget = self
            .budget
            .checked_sub(n.checked_add(32).ok_or(BAD)?)
            .ok_or(BAD)?;
        Ok(())
    }
    pub fn string(&mut self) -> Result<Vec<u8>> {
        let (n, encoded) = self.length_head()?;
        if !encoded {
            let n = usize::try_from(n).map_err(|_| BAD)?;
            self.charge(n)?;
            return Ok(self.take(n)?.to_vec());
        }
        match n {
            0..=2 => {
                self.charge(20)?;
                self.signed(1 << n)
            }
            3 => {
                let compressed = self.count()?;
                let expanded = self.count()?;
                self.charge(expanded)?;
                lzf(self.take(compressed)?, expanded)
            }
            _ => Err(BAD),
        }
    }
}
fn lzf(input: &[u8], size: usize) -> Result<Vec<u8>> {
    let mut reader = Reader::new(input);
    let mut output = Vec::with_capacity(size);
    while reader.pos < input.len() {
        let control = reader.byte()?;
        if control < 32 {
            let count = control as usize + 1;
            if output.len().checked_add(count).is_none_or(|n| n > size) {
                return Err(BAD);
            }
            output.extend_from_slice(reader.take(count)?);
        } else {
            let mut count = (control >> 5) as usize;
            let mut distance = ((control & 31) as usize) << 8;
            if count == 7 {
                count += reader.byte()? as usize;
            }
            distance += reader.byte()? as usize + 1;
            count += 2;
            if distance > output.len() || output.len().checked_add(count).is_none_or(|n| n > size) {
                return Err(BAD);
            }
            for _ in 0..count {
                output.push(output[output.len() - distance]);
            }
        }
    }
    if output.len() != size {
        return Err(BAD);
    }
    Ok(output)
}
