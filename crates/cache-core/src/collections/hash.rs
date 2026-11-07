use super::*;
pub(super) fn run(cmd: &str, a: &[&[u8]], data: &mut Cow<'_, Collection>) -> Result<(Reply, bool)> {
    let Collection::Hash(h) = data.as_ref() else {
        unreachable!()
    };
    if cmd == "hgetdel" {
        // The generic dispatcher has already checked type, matching Redis's
        // precedence over FIELDS/count validation for this command.
        if !a[2].eq_ignore_ascii_case(b"FIELDS") {
            return Err(Reply::Error(
                "ERR Mandatory argument FIELDS is missing or not at the right position",
            ));
        }
        let count = integer(a[3]).ok().filter(|n| *n > 0).ok_or(Reply::Error(
            "ERR Number of fields must be a positive integer",
        ))?;
        if count as usize != a.len() - 4 {
            return Err(Reply::Error(
                "ERR The `numfields` parameter must match the number of arguments",
            ));
        }
        let mut seen = std::collections::HashSet::new();
        let mut changed = false;
        let reply = array(a[4..].iter().map(|field| {
            // Each field is deleted as it is returned: a later occurrence is
            // null even if that field existed at command entry.
            let value = if seen.insert(*field) {
                h.get(*field).map(|v| v.as_ref())
            } else {
                None
            };
            changed |= value.is_some();
            value
        }))?;
        if changed {
            let Collection::Hash(h) = data.to_mut() else {
                unreachable!()
            };
            for field in seen {
                h.remove(field);
            }
        }
        return Ok((reply, changed));
    }
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
            h.insert_preserving_expiry(a[2].into(), n.to_string().into_bytes().into());
            Ok((Reply::Integer(n), true))
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn getdel_duplicates_atomic_rejection_and_last_field_deletion() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"HSET", b"h", b"a", b"one", b"b", b"two"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"h", b"100"], 0, |_| true);
        let original = engine.snapshot(b"h");
        let args = [
            b"HGETDEL".as_slice(),
            b"h",
            b"FIELDS",
            b"3",
            b"a",
            b"a",
            b"absent",
        ];
        assert!(matches!(
            engine.execute(&args, 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"h"), original);
        assert_eq!(
            engine.execute(&args, 1, |_| true),
            Reply::Array(vec![bulk(b"one"), Reply::Bulk(None), Reply::Bulk(None)])
        );
        assert_eq!(engine.get(b"h", 1).unwrap().expires_at_ms, Some(100));
        let value = &engine.get(b"h", 1).unwrap().value;
        let canonical = Value::from_collection(value.collection().unwrap().clone()).unwrap();
        assert_eq!(
            value.len(),
            value.as_ref().len(),
            "encoded size must match journal payload"
        );
        assert_eq!(
            value.memory_bytes(),
            canonical.memory_bytes(),
            "incremental accounting must match full recalculation"
        );
        assert_eq!(
            engine.execute(&args, 1, |_| panic!("no changes")),
            Reply::Array((0..3).map(|_| Reply::Bulk(None)).collect())
        );
        assert_eq!(
            engine.execute(&[b"HGETDEL", b"h", b"FIELDS", b"1", b"b"], 1, |_| true),
            Reply::Array(vec![bulk(b"two")])
        );
        assert!(engine.snapshot(b"h").is_none());
    }
}
