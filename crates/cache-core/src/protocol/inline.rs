//! Redis inline argument grammar. Escapes are decoded only inside quotes.
const MAX_INLINE: usize = 64 * 1024;
const QUOTES: &str = "unbalanced quotes in request";
type Parsed = Result<Option<(Vec<Vec<u8>>, usize)>, &'static str>;
fn space(b: u8) -> bool {
    matches!(b, b' ' | 9..=13)
}
fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
pub(super) fn parse(buf: &[u8]) -> Parsed {
    // Redis searches the C string for LF: a literal NUL blocks framing. Use
    // quoted hexadecimal escapes to carry NUL in an inline argument.
    let Some(end) = buf
        .iter()
        .take_while(|b| **b != 0)
        .position(|b| *b == b'\n')
    else {
        return if buf.len() > MAX_INLINE {
            Err("too big inline request")
        } else {
            Ok(None)
        };
    };
    if end > MAX_INLINE {
        return Err("too big inline request");
    }
    let line = &buf[..if end > 0 && buf[end - 1] == b'\r' {
        end - 1
    } else {
        end
    }];
    let mut args = Vec::new();
    let mut pos = 0;
    while pos < line.len() {
        while pos < line.len() && space(line[pos]) {
            pos += 1;
        }
        if pos == line.len() {
            break;
        }
        if args.len() == super::MAX_ARGS {
            return Err("invalid argument count");
        }
        let mut arg = Vec::new();
        let mut quote = None;
        loop {
            let byte = line.get(pos).copied();
            match (quote, byte) {
                (Some(_), None) => return Err(QUOTES),
                (None, None) => break,
                (None, Some(b' ' | b'\t' | b'\r')) => {
                    pos += 1;
                    break;
                }
                (None, Some(b @ (b'"' | b'\''))) => {
                    quote = Some(b);
                    pos += 1;
                }
                (Some(q), Some(b)) if q == b => {
                    pos += 1;
                    if line.get(pos).is_some_and(|b| !space(*b)) {
                        return Err(QUOTES);
                    }
                    break;
                }
                (Some(b'"'), Some(b'\\')) if pos + 1 < line.len() => {
                    if line.get(pos + 1) == Some(&b'x') {
                        if let Some((high, low)) = line
                            .get(pos + 2)
                            .and_then(|b| hex(*b))
                            .zip(line.get(pos + 3).and_then(|b| hex(*b)))
                        {
                            arg.push(high * 16 + low);
                            pos += 4;
                            continue;
                        }
                    }
                    pos += 1;
                    arg.push(match line[pos] {
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'b' => 8,
                        b'a' => 7,
                        b => b,
                    });
                    pos += 1;
                }
                (Some(b'\''), Some(b'\\')) if line.get(pos + 1) == Some(&b'\'') => {
                    arg.push(b'\'');
                    pos += 2;
                }
                (_, Some(b)) => {
                    arg.push(b);
                    pos += 1;
                }
            }
        }
        args.push(arg);
    }
    Ok(Some((args, end + 1)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fragmented_quoted_binary_inline_and_mixed_pipeline() {
        let input = b"ECHO \"a\\x00\\xff\\n\\t\\b\\a\\\\\\\"\"\r\n";
        for end in 0..input.len() {
            assert!(parse(&input[..end]).unwrap().is_none());
        }
        let (args, n) = parse(input).unwrap().unwrap();
        assert_eq!(n, input.len());
        assert_eq!(args[1], b"a\0\xff\n\t\x08\x07\\\"");
        let mut pipeline = input.to_vec();
        pipeline.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        let mut storage = Vec::new();
        assert_eq!(
            super::super::parse_command(&pipeline, &mut storage)
                .unwrap()
                .unwrap()
                .1,
            input.len()
        );
        assert_eq!(
            super::super::parse_command(&pipeline[n..], &mut storage)
                .unwrap()
                .unwrap()
                .0[0],
            b"PING"
        );
    }
    #[test]
    fn blank_lines_quotes_and_limits() {
        assert!(parse(b" \t\r\n").unwrap().unwrap().0.is_empty());
        assert_eq!(
            parse(b"ECHO abc\"def\"\n").unwrap().unwrap().0[1],
            b"abcdef"
        );
        assert_eq!(parse(b"ECHO 'a\\'b'\n").unwrap().unwrap().0[1], b"a'b");
        assert_eq!(parse(b"ECHO '\\n'\n").unwrap().unwrap().0[1], b"\\n");
        for input in [
            b"ECHO \"a\n".as_slice(),
            b"ECHO \"a\"b\n",
            b"ECHO 'a\n",
            b"ECHO 'a'b\n",
        ] {
            assert_eq!(parse(input), Err(QUOTES));
        }
        assert!(parse(&vec![b'a'; MAX_INLINE + 1]).is_err());
        assert!(parse(b"ECHO a\0b\n").unwrap().is_none());
    }
}
