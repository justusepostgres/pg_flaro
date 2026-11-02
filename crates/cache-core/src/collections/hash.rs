use super::*;
pub(super) fn run(cmd: &str, a: &[&[u8]], data: &mut Cow<'_, Collection>) -> Result<(Reply, bool)> {
    let Collection::Hash(h) = data.as_ref() else {
        unreachable!()
    };
    let read = match cmd {
        "hget" => Some(Reply::Bulk(h.get(a[2]).map(|v| v.to_vec()))),
        "hmget" => Some(array(a[2..].iter().map(|k| h.get(*k).map(|v| v.as_ref())))?),
        "hexists" => Some(Reply::Integer(i64::from(h.contains_key(a[2])))),
        "hlen" => Some(Reply::Integer(h.len() as i64)),
        "hstrlen" => Some(Reply::Integer(h.get(a[2]).map_or(0, |v| v.len()) as i64)),
        "hkeys" => Some(array(h.keys().map(|k| Some(k.as_ref())))?),
        "hvals" => Some(array(h.values().map(|v| Some(v.as_ref())))?),
        "hgetall" => Some(array(
            h.iter()
                .flat_map(|(k, v)| [Some(k.as_ref()), Some(v.as_ref())]),
        )?),
        _ => None,
    };
    if let Some(r) = read {
        return Ok((r, false));
    }
    if matches!(cmd, "hset" | "hmset") && !a.len().is_multiple_of(2) {
        return Err(SYNTAX);
    }
    let Collection::Hash(h) = data.to_mut() else {
        unreachable!()
    };
    match cmd {
        "hset" | "hmset" => {
            let mut n = 0;
            for pair in a[2..].chunks_exact(2) {
                n += i64::from(h.insert(pair[0].into(), pair[1].into()).is_none());
            }
            Ok((
                if cmd == "hmset" {
                    Reply::Simple("OK")
                } else {
                    Reply::Integer(n)
                },
                true,
            ))
        }
        "hsetnx" => {
            if h.contains_key(a[2]) {
                Ok((Reply::Integer(0), false))
            } else {
                h.insert(a[2].into(), a[3].into());
                Ok((Reply::Integer(1), true))
            }
        }
        "hdel" => {
            let n = a[2..].iter().filter(|k| h.remove(**k).is_some()).count();
            Ok((Reply::Integer(n as i64), n > 0))
        }
        "hincrby" => {
            let by = integer(a[3])?;
            let old = h
                .get(a[2])
                .map_or(Ok(0), |v| integer(v))
                .map_err(|_| Reply::Error("ERR hash value is not an integer"))?;
            let n = old
                .checked_add(by)
                .ok_or(Reply::Error("ERR increment or decrement would overflow"))?;
            h.insert(a[2].into(), n.to_string().into_bytes().into());
            Ok((Reply::Integer(n), true))
        }
        _ => unreachable!(),
    }
}
