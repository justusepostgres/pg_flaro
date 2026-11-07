//! Wire shapes for supported commands whose RESP3 replies differ from RESP2.
//! The engine's existing reply tree remains the RESP2 representation.
use super::Reply;
fn header(prefix: u8, n: usize, out: &mut Vec<u8>) {
    out.push(prefix);
    out.extend_from_slice(itoa::Buffer::new().format(n).as_bytes());
    out.extend_from_slice(b"\r\n");
}
fn generic(reply: &Reply, out: &mut Vec<u8>) {
    match reply {
        Reply::Bulk(None) | Reply::NullArray => out.extend_from_slice(b"_\r\n"),
        Reply::Map(pairs) => {
            header(b'%', pairs.len(), out);
            for (key, value) in pairs {
                generic(key, out);
                generic(value, out);
            }
        }
        Reply::Array(values) | Reply::Set(values) => {
            header(
                if matches!(reply, Reply::Set(_)) {
                    b'~'
                } else {
                    b'*'
                },
                values.len(),
                out,
            );
            for value in values {
                generic(value, out);
            }
        }
        other => other.encode(out),
    }
}
fn double(reply: &Reply, out: &mut Vec<u8>) {
    if let Reply::Bulk(Some(value)) = reply {
        out.push(b',');
        out.extend_from_slice(value);
        out.extend_from_slice(b"\r\n");
    } else {
        generic(reply, out);
    }
}
fn pairs(values: &[Reply], scores: bool, nested: bool, out: &mut Vec<u8>) {
    header(
        b'*',
        if nested {
            values.len() / 2
        } else {
            values.len()
        },
        out,
    );
    for pair in values.chunks_exact(2) {
        if nested {
            header(b'*', 2, out);
        }
        generic(&pair[0], out);
        if scores {
            double(&pair[1], out);
        } else {
            generic(&pair[1], out);
        }
    }
}
pub(crate) fn encode(args: &[&[u8]], reply: &Reply, out: &mut Vec<u8>) {
    let name = args[0];
    let is = |name2: &[u8]| name.eq_ignore_ascii_case(name2);
    let option = |start: usize, flag: &[u8]| {
        args.get(start..)
            .is_some_and(|a| a.iter().any(|v| v.eq_ignore_ascii_case(flag)))
    };
    if is(b"ZSCORE") || is(b"ZINCRBY") || is(b"ZADD") {
        double(reply, out);
        return;
    }
    let Reply::Array(values) = reply else {
        generic(reply, out);
        return;
    };
    if is(b"GEOSEARCH") || name.to_ascii_lowercase().starts_with(b"georadius") {
        header(b'*', values.len(), out);
        for value in values {
            if let Reply::Array(row) = value {
                header(b'*', row.len(), out);
                for field in row {
                    if let Reply::Array(coordinates) = field {
                        header(b'*', coordinates.len(), out);
                        for coordinate in coordinates {
                            double(coordinate, out);
                        }
                    } else {
                        generic(field, out);
                    }
                }
            } else {
                generic(value, out);
            }
        }
    } else if is(b"GEOPOS") {
        header(b'*', values.len(), out);
        for value in values {
            if let Reply::Array(coordinates) = value {
                header(b'*', coordinates.len(), out);
                for coordinate in coordinates {
                    double(coordinate, out);
                }
            } else {
                generic(value, out);
            }
        }
    } else if is(b"COMMAND")
        && (args.len() == 1 || args.get(1).is_some_and(|s| s.eq_ignore_ascii_case(b"INFO")))
    {
        generic(reply, out);
    } else if is(b"COMMAND")
        && args
            .get(1)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"GETKEYSANDFLAGS"))
    {
        header(b'*', values.len(), out);
        for key in values {
            if let Reply::Array(pair) = key {
                header(b'*', pair.len(), out);
                generic(&pair[0], out);
                if let Reply::Array(flags) = &pair[1] {
                    header(b'~', flags.len(), out);
                    for flag in flags {
                        generic(flag, out);
                    }
                } else {
                    generic(&pair[1], out);
                }
            } else {
                generic(key, out);
            }
        }
    } else if is(b"HELLO") || is(b"HGETALL") || (is(b"LCS") && option(3, b"IDX")) {
        header(b'%', values.len() / 2, out);
        for value in values {
            generic(value, out);
        }
    } else if is(b"SMEMBERS")
        || is(b"SUNION")
        || is(b"SINTER")
        || is(b"SDIFF")
        || (is(b"SPOP") && args.len() == 3)
    {
        header(b'~', values.len(), out);
        for value in values {
            generic(value, out);
        }
    } else if is(b"ZMSCORE") {
        header(b'*', values.len(), out);
        for value in values {
            double(value, out);
        }
    } else if (is(b"ZRANK") || is(b"ZREVRANK")) && args.len() == 4 && values.len() == 2 {
        header(b'*', 2, out);
        generic(&values[0], out);
        double(&values[1], out);
    } else if is(b"HRANDFIELD") && args.len() == 4 {
        pairs(values, false, true, out);
    } else if is(b"ZRANDMEMBER") && args.len() == 4 {
        pairs(values, true, true, out);
    } else if is(b"ZPOPMIN") || is(b"ZPOPMAX") {
        pairs(values, true, args.len() == 3, out);
    } else if (is(b"ZRANGE") || is(b"ZREVRANGE") || is(b"ZRANGEBYSCORE") || is(b"ZREVRANGEBYSCORE"))
        && option(4, b"WITHSCORES")
        || (is(b"ZUNION") || is(b"ZINTER") || is(b"ZDIFF"))
            && args
                .get(1)
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|s| s.parse::<usize>().ok())
                .is_some_and(|n| option(2 + n, b"WITHSCORES"))
    {
        pairs(values, true, true, out);
    } else if (is(b"BZPOPMIN") || is(b"BZPOPMAX")) && values.len() == 3 {
        header(b'*', 3, out);
        generic(&values[0], out);
        generic(&values[1], out);
        double(&values[2], out);
    } else if (is(b"ZMPOP") || is(b"BZMPOP")) && values.len() == 2 {
        header(b'*', 2, out);
        generic(&values[0], out);
        if let Reply::Array(members) = &values[1] {
            header(b'*', members.len(), out);
            for member in members {
                if let Reply::Array(pair) = member {
                    pairs(pair, true, false, out);
                } else {
                    generic(member, out);
                }
            }
        } else {
            generic(&values[1], out);
        }
    } else {
        generic(reply, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn bulk(v: &[u8]) -> Reply {
        Reply::Bulk(Some(v.to_vec()))
    }
    #[test]
    fn resp3_null_maps_sets_and_scores_have_distinct_wire_types() {
        let mut out = Vec::new();
        encode(&[b"GET", b"k"], &Reply::Bulk(None), &mut out);
        assert_eq!(out, b"_\r\n");
        out.clear();
        encode(
            &[b"HGETALL", b"h"],
            &Reply::Array(vec![bulk(b"f"), bulk(b"v")]),
            &mut out,
        );
        assert_eq!(out, b"%1\r\n$1\r\nf\r\n$1\r\nv\r\n");
        out.clear();
        encode(
            &[b"SMEMBERS", b"s"],
            &Reply::Array(vec![bulk(b"v")]),
            &mut out,
        );
        assert_eq!(out, b"~1\r\n$1\r\nv\r\n");
        out.clear();
        encode(
            &[b"ZRANGE", b"z", b"0", b"-1", b"WITHSCORES"],
            &Reply::Array(vec![bulk(b"m"), bulk(b"1.5")]),
            &mut out,
        );
        assert_eq!(out, b"*1\r\n*2\r\n$1\r\nm\r\n,1.5\r\n");
    }
}
