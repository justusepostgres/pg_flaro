use super::*;
use crate::random::Random;

pub(super) fn execute(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Reply {
    if a[0].eq_ignore_ascii_case(b"HRANDFIELD") || a[0].eq_ignore_ascii_case(b"ZRANDMEMBER") {
        fields(engine, a, now).unwrap_or_else(|error| error)
    } else {
        run(engine, a, now, submit).unwrap_or_else(|error| error)
    }
}
fn run(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply> {
    if a.len() > 3 {
        return Err(SYNTAX);
    }
    let pop = a[0].eq_ignore_ascii_case(b"SPOP");
    let counted = a.len() == 3;
    let count = if counted { integer(a[2])? } else { 1 };
    if count == i64::MIN || (pop && count < 0) {
        return Err(Reply::Error("ERR value is out of range"));
    }
    let Some(old) = engine.get(a[1], now) else {
        return Ok(if counted {
            Reply::Array(vec![])
        } else {
            Reply::Bulk(None)
        });
    };
    let Some(Collection::Set(set)) = old.value.collection() else {
        return Err(WRONGTYPE);
    };
    let repeat = count < 0;
    let count = if repeat {
        count.unsigned_abs() as usize
    } else {
        (count as usize).min(set.len())
    };
    if count == 0 || set.is_empty() {
        return Ok(Reply::Array(vec![]));
    }
    if count > 2 * crate::protocol::MAX_FRAME / 32 {
        return Err(Reply::Error("ERR reply exceeds output limit"));
    }
    let selected = sample(set.iter().collect(), count, repeat);
    let reply = if counted {
        array(selected.iter().map(|member| Some(member.as_ref())))?
    } else {
        bulk(selected[0])
    };
    if !pop {
        return Ok(reply);
    }
    let entry = if count == set.len() {
        None
    } else {
        let mut remaining = set.clone();
        for member in selected {
            remaining.remove(member.as_ref());
        }
        Some(Entry {
            value: Value::from_collection(Collection::Set(remaining))
                .map_err(|_| Reply::Error("ERR collection exceeds maximum size"))?,
            expires_at_ms: old.expires_at_ms,
        })
    };
    Ok(engine.apply_typed(vec![(a[1].to_vec(), entry)], reply, submit))
}

fn sample<T: Copy>(mut members: Vec<T>, count: usize, repeat: bool) -> Vec<T> {
    let mut random = Random::new();
    if repeat {
        (0..count)
            .map(|_| members[random.below(members.len())])
            .collect()
    } else {
        // Partial Fisher-Yates selects distinct members without modulo bias.
        for i in 0..count {
            let j = i + random.below(members.len() - i);
            members.swap(i, j);
        }
        members.truncate(count);
        members
    }
}

fn fields(engine: &Engine, a: &[&[u8]], now: i64) -> Result<Reply> {
    let hash = a[0].eq_ignore_ascii_case(b"HRANDFIELD");
    let counted = a.len() >= 3;
    let count = if counted { integer(a[2])? } else { 1 };
    if count == i64::MIN {
        return Err(Reply::Error("ERR value is out of range"));
    }
    let paired = a.len() == 4;
    let option = if hash {
        b"WITHVALUES".as_slice()
    } else {
        b"WITHSCORES"
    };
    if a.len() > 4 || (paired && !a[3].eq_ignore_ascii_case(option)) {
        return Err(SYNTAX);
    }
    if paired && !(-i64::MAX / 2..=i64::MAX / 2).contains(&count) {
        return Err(Reply::Error("ERR value is out of range"));
    }
    let Some(entry) = engine.get(a[1], now) else {
        return Ok(if counted {
            Reply::Array(vec![])
        } else {
            Reply::Bulk(None)
        });
    };
    let Some(collection) = entry.value.collection() else {
        return Err(WRONGTYPE);
    };
    if collection.kind() != if hash { 1 } else { 4 } {
        return Err(WRONGTYPE);
    }
    let repeat = count < 0;
    let count = if repeat {
        count.unsigned_abs() as usize
    } else {
        (count as usize).min(collection.len())
    };
    if count == 0 || collection.is_empty() {
        return Ok(Reply::Array(vec![]));
    }
    if count > 2 * crate::protocol::MAX_FRAME / if paired { 64 } else { 32 } {
        return Err(Reply::Error("ERR reply exceeds output limit"));
    }
    let keys = match collection {
        Collection::Hash(h) => h.keys().collect(),
        Collection::ZSet(z) => z.scores.keys().collect(),
        _ => unreachable!(),
    };
    let selected = sample(keys, count, repeat);
    if !counted {
        return Ok(bulk(selected[0]));
    }
    if !paired {
        return array(selected.iter().map(|key| Some(key.as_ref())));
    }
    let mut reply = Vec::new();
    let mut bytes = 32;
    for key in selected {
        let value: Cow<'_, [u8]> = match collection {
            Collection::Hash(h) => Cow::Borrowed(h[key].as_ref()),
            Collection::ZSet(z) => Cow::Owned(zset::format(z.scores[key].0)),
            _ => unreachable!(),
        };
        bytes += key.len() + value.len() + 64;
        if bytes > 2 * crate::protocol::MAX_FRAME {
            return Err(Reply::Error("ERR reply exceeds output limit"));
        }
        reply.push(bulk(key));
        reply.push(bulk(value));
    }
    Ok(Reply::Array(reply))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn random_pairs_are_read_only_bounded_and_expire() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"HSET", b"h", b"field", b"value"], 0, |_| true);
        engine.execute(&[b"ZADD", b"z", b"1.5", b"member"], 0, |_| true);
        for (name, key, option, member, value) in [
            (
                b"HRANDFIELD".as_slice(),
                b"h".as_slice(),
                b"WITHVALUES".as_slice(),
                b"field".as_slice(),
                b"value".as_slice(),
            ),
            (b"ZRANDMEMBER", b"z", b"WITHSCORES", b"member", b"1.5"),
        ] {
            engine.execute(&[b"PEXPIREAT", key, b"100"], 0, |_| true);
            let original = engine.snapshot(key);
            assert_eq!(
                engine.execute(&[name, key, b"-2", option], 1, |_| panic!("read only")),
                Reply::Array(vec![bulk(member), bulk(value), bulk(member), bulk(value)])
            );
            assert!(matches!(
                engine.execute(&[name, key, b"-100000", option], 1, |_| panic!("read only")),
                Reply::Error(_)
            ));
            assert_eq!(engine.snapshot(key), original);
            assert_eq!(
                engine.execute(&[name, key], 100, |_| panic!("read only")),
                Reply::Bulk(None)
            );
            assert_eq!(
                engine.execute(&[name, key, b"1", option], 100, |_| panic!("read only")),
                Reply::Array(vec![])
            );
            assert!(matches!(
                engine.execute(
                    &[name, key, b"4611686018427387904", option],
                    100,
                    |_| panic!("read only")
                ),
                Reply::Error(_)
            ));
        }
    }

    #[test]
    fn random_pop_admission_is_atomic_and_preserves_ttl() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SADD", b"s", b"a", b"b", b"c"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"s", b"100"], 0, |_| true);
        let original = engine.snapshot(b"s");
        for args in [
            vec![b"SPOP".as_slice(), b"s"],
            vec![b"SPOP", b"s", b"2"],
            vec![b"SPOP", b"s", b"999"],
        ] {
            assert!(matches!(
                engine.execute(&args, 1, |_| false),
                Reply::Error(_)
            ));
            assert_eq!(engine.snapshot(b"s"), original);
        }
        let Reply::Bulk(Some(member)) = engine.execute(&[b"SPOP", b"s"], 1, |_| true) else {
            panic!("single pop");
        };
        assert_eq!(
            engine.execute(&[b"SISMEMBER", b"s", &member], 1, |_| panic!("read")),
            Reply::Integer(0)
        );
        assert_eq!(engine.get(b"s", 1).unwrap().expires_at_ms, Some(100));
        let Reply::Array(rest) = engine.execute(&[b"SPOP", b"s", b"999"], 1, |_| true) else {
            panic!("multiple pop");
        };
        assert_eq!(rest.len(), 2);
        assert!(engine.snapshot(b"s").is_none());
    }
    #[test]
    fn random_reads_and_output_rejection_leave_value_unchanged() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SADD", b"s", b"a", b"b"], 0, |_| true);
        let original = engine.snapshot(b"s");
        let Reply::Array(values) =
            engine.execute(&[b"SRANDMEMBER", b"s", b"-10"], 1, |_| panic!("read"))
        else {
            panic!("array");
        };
        assert_eq!(values.len(), 10);
        assert!(values.iter().all(|v| *v == bulk(b"a") || *v == bulk(b"b")));
        assert!(matches!(
            engine.execute(
                &[b"SRANDMEMBER", b"s", b"-9223372036854775807"],
                1,
                |_| panic!("read")
            ),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"s"), original);
    }
}
