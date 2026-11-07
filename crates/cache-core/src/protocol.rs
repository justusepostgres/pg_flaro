//! Bounded, binary-safe RESP2 command framing. Incomplete input is not an error.
mod inline;
pub(crate) mod number;
pub(crate) mod resp3;
pub const MAX_FRAME: usize = 1024 * 1024;
// Administrative transfer envelopes add IDs, endpoint and absolute TTL fields
// around a key/value that already fits the ordinary 1-MiB request bound.
pub const MAX_TRANSFER_FRAME: usize = MAX_FRAME + 512;
// Redis accepts an INT_MAX array header. Frame bytes bound completed requests;
// a declared count must never determine a large allocation before data arrives.
const MAX_ARGS: usize = i32::MAX as usize;
pub type Command<'a> = smallvec::SmallVec<[&'a [u8]; 8]>;
pub type Parsed<'a> = Result<Option<(Command<'a>, usize)>, &'static str>;

/// RESP bulk strings borrow the input directly. Only inline quoting/escapes
/// require owned argument storage, supplied by the caller for this command.
pub fn parse_command<'a>(buf: &'a [u8], storage: &'a mut Vec<Vec<u8>>) -> Parsed<'a> {
    if buf.first().is_none_or(|b| *b == b'*') {
        return parse(buf);
    }
    let Some((args, consumed)) = inline::parse(buf)? else {
        return Ok(None);
    };
    *storage = args;
    Ok(Some((
        storage.iter().map(Vec::as_slice).collect(),
        consumed,
    )))
}

fn number(buf: &[u8], pos: &mut usize, prefix: u8) -> Result<Option<i64>, &'static str> {
    if *pos == buf.len() {
        return Ok(None);
    }
    if buf[*pos] != prefix {
        return Err("expected RESP array of bulk strings");
    }
    let invalid = if prefix == b'*' {
        "invalid multibulk length"
    } else {
        "invalid bulk length"
    };
    let start = *pos + 1;
    let negative = buf.get(start) == Some(&b'-');
    let digits = start + usize::from(negative);
    let mut n = 0i64;
    for (offset, &byte) in buf[digits..].iter().enumerate() {
        if byte == b'\r' {
            if offset == 0 || (negative && n == 0) || (offset > 1 && buf[digits] == b'0') {
                return Err(invalid);
            }
            return match buf.get(digits + offset + 1) {
                None => Ok(None),
                Some(b'\n') => {
                    *pos = digits + offset + 2;
                    Ok(Some(n))
                }
                Some(_) => Err(invalid),
            };
        }
        if !byte.is_ascii_digit() {
            return Err(invalid);
        }
        n = n
            .checked_mul(10)
            .and_then(|n| {
                if negative {
                    n.checked_sub((byte - b'0') as i64)
                } else {
                    n.checked_add((byte - b'0') as i64)
                }
            })
            .ok_or(invalid)?;
    }
    if buf.len() - start > 20 {
        Err(invalid)
    } else {
        Ok(None)
    }
}

pub fn parse(buf: &[u8]) -> Parsed<'_> {
    let mut pos = 0;
    let Some(count) = number(buf, &mut pos, b'*')? else {
        return Ok(None);
    };
    if count <= 0 {
        return Ok(Some((Command::new(), pos)));
    }
    if count > MAX_ARGS as i64 {
        return Err("invalid multibulk length");
    }
    // Each bulk argument needs at least six bytes ($0\r\n\r\n). Start
    // small on fragmented or adversarial headers and grow only for parsed args.
    let capacity = (count as usize).min(1024).min(buf.len() / 6);
    let mut args = Command::with_capacity(capacity);
    for _ in 0..count {
        let Some(len) = number(buf, &mut pos, b'$')? else {
            return Ok(None);
        };
        let len = usize::try_from(len).map_err(|_| "invalid bulk length")?;
        let end = pos
            .checked_add(len)
            .and_then(|v| v.checked_add(2))
            .ok_or("frame too large")?;
        let limit = if args.first().is_some_and(|name: &&[u8]| {
            name.eq_ignore_ascii_case(b"PGCACHE.IMPORT") || name.eq_ignore_ascii_case(b"MIGRATE")
        }) {
            MAX_TRANSFER_FRAME
        } else {
            MAX_FRAME
        };
        if end > limit {
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
    /// CR/LF-free owned status text.
    OwnedSimple(String),
    Error(&'static str),
    OwnedError(String),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Vec<Reply>),
    Set(Vec<Reply>),
    Map(Vec<(Reply, Reply)>),
    NullArray,
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
            Self::OwnedError(s) | Self::OwnedSimple(s) => {
                out.push(if matches!(self, Self::OwnedSimple(_)) {
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
            Self::NullArray => out.extend_from_slice(b"*-1\r\n"),
            Self::Map(pairs) => {
                out.push(b'*');
                out.extend_from_slice(itoa::Buffer::new().format(pairs.len() * 2).as_bytes());
                out.extend_from_slice(b"\r\n");
                for (key, value) in pairs {
                    key.encode(out);
                    value.encode(out);
                }
            }
            Self::Array(a) | Self::Set(a) => {
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
    fn wide_arrays_preserve_fragmentation_and_pipeline_boundaries() {
        let mut frame = b"*4097\r\n".to_vec();
        bulk(Some(b"EXISTS"), &mut frame);
        for _ in 0..4096 {
            bulk(Some(b""), &mut frame);
        }
        for end in [0, 1, 8, 100, 8191, frame.len() - 1] {
            assert_eq!(parse(&frame[..end]), Ok(None));
        }
        let length = frame.len();
        frame.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        let (args, consumed) = parse(&frame).unwrap().unwrap();
        assert_eq!(args.len(), 4097);
        assert_eq!(consumed, length);
        assert!(args[1..].iter().all(|arg| arg.is_empty()));
        assert_eq!(parse(&frame[consumed..]).unwrap().unwrap().0[0], b"PING");
    }
    #[test]
    fn huge_declared_arrays_do_not_reserve_the_declared_size() {
        for header in [b"*2147483647\r\n".as_slice(), b"*2147483647\r\n$1\r\nx\r\n"] {
            assert_eq!(parse(header), Ok(None));
        }
        assert_eq!(parse(b"*2147483648\r\n"), Err("invalid multibulk length"));
        // Completed arguments are still constrained by the frame size.
        let mut frame = b"*200000\r\n".to_vec();
        for _ in 0..200000 {
            bulk(Some(b""), &mut frame);
        }
        assert_eq!(parse(&frame), Err("frame too large"));
    }
    #[test]
    fn nested_metadata_maps_and_sets_preserve_protocol_shapes() {
        let reply = Reply::Array(vec![Reply::Map(vec![(
            Reply::Bulk(Some(b"flags".to_vec())),
            Reply::Set(vec![Reply::Bulk(Some(b"RO".to_vec()))]),
        )])]);
        let mut resp2 = Vec::new();
        reply.encode(&mut resp2);
        assert_eq!(resp2, b"*1\r\n*2\r\n$5\r\nflags\r\n*1\r\n$2\r\nRO\r\n");
        let mut resp3 = Vec::new();
        super::resp3::encode(&[b"COMMAND", b"INFO"], &reply, &mut resp3);
        assert_eq!(resp3, b"*1\r\n%1\r\n$5\r\nflags\r\n~1\r\n$2\r\nRO\r\n");
    }

    #[test]
    fn signed_array_headers_and_canonical_decimal_lengths() {
        let oversized = format!("*{}", "0".repeat(32));
        assert_eq!(parse(oversized.as_bytes()), Err("invalid multibulk length"));
        for header in [
            b"*0\r\n".as_slice(),
            b"*-1\r\n",
            b"*-9\r\n",
            b"*-9223372036854775808\r\n",
        ] {
            for end in 0..header.len() {
                assert_eq!(parse(&header[..end]), Ok(None));
            }
            let mut pipeline = header.to_vec();
            pipeline.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
            let (args, used) = parse(&pipeline).unwrap().unwrap();
            assert!(args.is_empty());
            assert_eq!(used, header.len());
            assert_eq!(parse(&pipeline[used..]).unwrap().unwrap().0[0], b"PING");
        }
        for header in [
            b"*00\r\n".as_slice(),
            b"*01\r\n",
            b"*+1\r\n",
            b"*-0\r\n",
            b"*-01\r\n",
            b"*-9223372036854775809\r\n",
        ] {
            assert_eq!(parse(header), Err("invalid multibulk length"));
        }
        for length in ["-1", "-0", "00", "01", "+1", "9223372036854775808"] {
            let input = format!("*1\r\n${length}\r\n");
            assert_eq!(parse(input.as_bytes()), Err("invalid bulk length"));
        }
    }
    #[test]
    fn transfer_envelope_reserve_is_not_available_to_data_commands() {
        let payload = vec![b'x'; MAX_FRAME - 10];
        let mut frame = b"*2\r\n".to_vec();
        bulk(Some(b"PGCACHE.IMPORT"), &mut frame);
        bulk(Some(&payload), &mut frame);
        assert!(frame.len() > MAX_FRAME && frame.len() <= MAX_TRANSFER_FRAME);
        assert!(parse(&frame).unwrap().is_some());
        let mut ordinary = b"*2\r\n".to_vec();
        bulk(Some(b"ECHO"), &mut ordinary);
        bulk(Some(&payload), &mut ordinary);
        assert!(parse(&ordinary).is_err());
    }
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
            let mut frame = format!("*2\r\n$4\r\nECHO\r\n${length}\r\n").into_bytes();
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
            b"*2147483648\r\n".as_slice(),
            b"*1\r\n$9999999999\r\n",
            b"*1\r\n$-1\r\n",
            b"*1\r\n$1\r\nx!!",
        ] {
            assert!(parse(b).is_err());
        }
    }
}
