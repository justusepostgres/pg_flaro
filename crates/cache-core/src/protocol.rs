//! Bounded, binary-safe RESP2 command framing. Incomplete input is not an error.
pub const MAX_FRAME: usize = 1024 * 1024;
const MAX_ARGS: usize = 1024;
pub type Command<'a> = smallvec::SmallVec<[&'a [u8]; 8]>;
pub type Parsed<'a> = Result<Option<(Command<'a>, usize)>, &'static str>;

fn number(buf: &[u8], pos: &mut usize, prefix: u8) -> Result<Option<usize>, &'static str> {
    if *pos == buf.len() {
        return Ok(None);
    }
    if buf[*pos] != prefix {
        return Err("expected RESP array of bulk strings");
    }
    let start = *pos + 1;
    let mut n = 0usize;
    for (offset, &byte) in buf[start..].iter().enumerate() {
        if byte == b'\r' {
            if offset == 0 {
                return Err("invalid length");
            }
            return match buf.get(start + offset + 1) {
                None => Ok(None),
                Some(b'\n') => {
                    *pos = start + offset + 2;
                    Ok(Some(n))
                }
                Some(_) => Err("invalid length"),
            };
        }
        if !byte.is_ascii_digit() {
            return Err("invalid length");
        }
        n = n
            .checked_mul(10)
            .and_then(|n| n.checked_add((byte - b'0') as usize))
            .ok_or("invalid length")?;
    }
    if buf.len() - start > 20 {
        Err("invalid length")
    } else {
        Ok(None)
    }
}

pub fn parse(buf: &[u8]) -> Parsed<'_> {
    let mut pos = 0;
    let Some(count) = number(buf, &mut pos, b'*')? else {
        return Ok(None);
    };
    if count == 0 || count > MAX_ARGS {
        return Err("invalid argument count");
    }
    let mut args = Command::with_capacity(count);
    for _ in 0..count {
        let Some(len) = number(buf, &mut pos, b'$')? else {
            return Ok(None);
        };
        let end = pos
            .checked_add(len)
            .and_then(|v| v.checked_add(2))
            .ok_or("frame too large")?;
        if end > MAX_FRAME {
            return Err("frame too large");
        }
        if end > buf.len() {
            return Ok(None);
        }
        if &buf[end - 2..end] != b"\r\n" {
            return Err("invalid bulk terminator");
        }
        args.push(&buf[pos..end - 2]);
        pos = end;
    }
    Ok(Some((args, pos)))
}

#[derive(Debug, PartialEq, Eq)]
pub enum Reply {
    Simple(&'static str),
    Error(&'static str),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Reply>),
}
pub fn bulk(value: Option<&[u8]>, out: &mut Vec<u8>) {
    if let Some(value) = value {
        out.push(b'$');
        out.extend_from_slice(itoa::Buffer::new().format(value.len()).as_bytes());
        out.extend_from_slice(b"\r\n");
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    } else {
        out.extend_from_slice(b"$-1\r\n");
    }
}
impl Reply {
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Simple(s) | Self::Error(s) => {
                out.push(if matches!(self, Self::Simple(_)) {
                    b'+'
                } else {
                    b'-'
                });
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Self::Integer(n) => {
                out.push(b':');
                out.extend_from_slice(itoa::Buffer::new().format(*n).as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Self::Bulk(b) => bulk(b.as_deref(), out),
            Self::Array(a) => {
                out.push(b'*');
                out.extend_from_slice(itoa::Buffer::new().format(a.len()).as_bytes());
                out.extend_from_slice(b"\r\n");
                for r in a {
                    r.encode(out);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fragmented_binary_pipeline() {
        let frame = b"*2\r\n$4\r\nECHO\r\n$4\r\n\0\r\nx\r\n";
        for end in 0..frame.len() {
            assert_eq!(parse(&frame[..end]), Ok(None));
        }
        let mut pipeline = frame.to_vec();
        pipeline.extend_from_slice(frame);
        let (args, used) = parse(&pipeline).unwrap().unwrap();
        assert_eq!(used, frame.len());
        assert_eq!(args[1], b"\0\r\nx");
    }
    #[test]
    fn decimal_headers_preserve_framing_and_reject_overflow() {
        for length in [0, 1, 9, 10, 99, 100, 1024, 65536] {
            let mut frame = format!("*02\r\n$4\r\nECHO\r\n${length}\r\n").into_bytes();
            let payload_start = frame.len();
            frame.resize(payload_start + length, 255);
            frame.extend_from_slice(b"\r\n");
            for end in 0..payload_start {
                assert_eq!(parse(&frame[..end]), Ok(None));
            }
            let (args, used) = parse(&frame).unwrap().unwrap();
            assert_eq!(used, frame.len());
            assert_eq!(args[1], &frame[payload_start..payload_start + length]);
            assert_eq!(parse(&frame[..frame.len() - 1]), Ok(None));
        }
        for frame in [
            b"*18446744073709551616\r\n".as_slice(),
            b"*1\r\n$18446744073709551616\r\n",
            b"*1\r\n$1x\r\n",
            b"*1\r\n$1\rx",
        ] {
            assert!(parse(frame).is_err());
        }
    }
    #[test]
    fn rejects_unbounded_and_malformed_frames() {
        for b in [
            b"*999999\r\n".as_slice(),
            b"*1\r\n$9999999999\r\n",
            b"*1\r\n$-1\r\n",
            b"*1\r\n$1\r\nx!!",
        ] {
            assert!(parse(b).is_err());
        }
    }
}
