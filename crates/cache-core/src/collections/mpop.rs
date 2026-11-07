use super::*;

pub(super) fn execute(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Reply {
    match run(engine, a, now, submit) {
        Ok(reply) | Err(reply) => reply,
    }
}
pub(crate) fn options(a: &[&[u8]]) -> Result<(usize, bool, usize)> {
    let numkeys =
        integer(a[1]).map_err(|_| Reply::Error("ERR numkeys should be greater than 0"))?;
    if numkeys <= 0 {
        return Err(Reply::Error("ERR numkeys should be greater than 0"));
    }
    if numkeys as u64 >= a.len().saturating_sub(2) as u64 {
        return Err(SYNTAX);
    }
    let side_index = numkeys as usize + 2;
    let sorted = a[0].eq_ignore_ascii_case(b"ZMPOP");
    let left = if a[side_index].eq_ignore_ascii_case(if sorted { b"MIN" } else { b"LEFT" }) {
        true
    } else if a[side_index].eq_ignore_ascii_case(if sorted { b"MAX" } else { b"RIGHT" }) {
        false
    } else {
        return Err(SYNTAX);
    };
    let count = if a.len() == side_index + 1 {
        1
    } else {
        if a.len() != side_index + 3 || !a[side_index + 1].eq_ignore_ascii_case(b"COUNT") {
            return Err(SYNTAX);
        }
        integer(a[side_index + 2])
            .ok()
            .filter(|n| *n > 0)
            .ok_or(Reply::Error("ERR count should be greater than 0"))? as usize
    };
    Ok((side_index, left, count))
}
fn run(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply> {
    let (side_index, left, count) = options(a)?;
    let sorted = a[0].eq_ignore_ascii_case(b"ZMPOP");
    for key in &a[2..side_index] {
        let Some(old) = engine.get(key, now) else {
            continue;
        };
        let mut data = old.value.collection().ok_or(WRONGTYPE)?.clone();
        let mut members = Vec::new();
        let mut bytes = key.len() + 64;
        match &mut data {
            Collection::List(list) if !sorted => {
                if list.is_empty() {
                    continue;
                }
                for _ in 0..count.min(list.len()) {
                    let member = if left {
                        list.pop_front()
                    } else {
                        list.pop_back()
                    }
                    .unwrap();
                    bytes += member.len() + 32;
                    if bytes > 2 * crate::protocol::MAX_FRAME {
                        return Err(Reply::Error("ERR response exceeds configured limit"));
                    }
                    members.push(bulk(member));
                }
            }
            Collection::ZSet(set) if sorted => {
                if set.scores.is_empty() {
                    continue;
                }
                for _ in 0..count.min(set.scores.len()) {
                    let (score, member) = if left {
                        set.order.iter().next()
                    } else {
                        set.order.iter().next_back()
                    }
                    .unwrap()
                    .clone();
                    let score = super::zset::format(score.0);
                    bytes += member.len() + score.len() + 64;
                    if bytes > 2 * crate::protocol::MAX_FRAME {
                        return Err(Reply::Error("ERR response exceeds configured limit"));
                    }
                    set.remove(&member);
                    members.push(Reply::Array(vec![bulk(member), bulk(score)]));
                }
            }
            _ => return Err(WRONGTYPE),
        }
        let reply = Reply::Array(vec![bulk(key), Reply::Array(members)]);
        let entry = if data.is_empty() {
            None
        } else {
            Some(Entry {
                value: Value::from_collection(data)
                    .map_err(|_| Reply::Error("ERR collection exceeds maximum size"))?,
                expires_at_ms: old.expires_at_ms,
            })
        };
        return Ok(engine.apply_typed(vec![(key.to_vec(), entry)], reply, submit));
    }
    Ok(Reply::NullArray)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sorted_pop_rejection_preserves_scores_and_deadline() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"ZADD", b"z", b"1.5", b"a", b"1.5", b"b"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"z", b"5000"], 0, |_| true);
        let before = engine.snapshot(b"z");
        let args: &[&[u8]] = &[b"ZMPOP", b"1", b"z", b"MAX"];
        assert!(matches!(
            engine.execute(args, 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"z"), before);
        assert_eq!(
            engine.execute(args, 1, |_| true),
            Reply::Array(vec![
                bulk(b"z"),
                Reply::Array(vec![Reply::Array(vec![bulk(b"b"), bulk(b"1.5")])])
            ])
        );
        assert_eq!(engine.snapshot(b"z").unwrap().expires_at_ms, Some(5000));
    }
    #[test]
    fn selects_first_list_and_reserves_before_popping() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"RPUSH", b"a", b"x", b"y", b"z"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"a", b"5000"], 0, |_| true);
        engine.execute(&[b"SET", b"wrong", b"v"], 0, |_| true);
        let before = engine.snapshot(b"a");
        let args: &[&[u8]] = &[
            b"LMPOP", b"3", b"missing", b"a", b"wrong", b"RIGHT", b"COUNT", b"2",
        ];
        assert!(matches!(
            engine.execute(args, 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"a"), before);
        assert_eq!(
            engine.execute(args, 1, |_| true),
            Reply::Array(vec![bulk(b"a"), Reply::Array(vec![bulk(b"z"), bulk(b"y")])])
        );
        assert_eq!(engine.snapshot(b"a").unwrap().expires_at_ms, Some(5000));
        assert_eq!(
            engine.execute(&[b"LMPOP", b"1", b"absent", b"LEFT"], 1, |_| true),
            Reply::NullArray
        );
    }
}
