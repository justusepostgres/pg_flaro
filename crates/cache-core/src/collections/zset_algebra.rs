use super::*;
use crate::value::SortedSet;

pub(crate) fn key_count(a: &[&[u8]], index: usize) -> Option<usize> {
    let count = usize::try_from(integer(a.get(index)?).ok()?).ok()?;
    (count > 0 && count <= a.len().saturating_sub(index + 1)).then_some(count)
}
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
fn run(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply> {
    let command = a[0].to_ascii_lowercase();
    let store = command.ends_with(b"store");
    let difference = command.starts_with(b"zdiff");
    let intersection = command.starts_with(b"zinter");
    let cardinality = command == b"zintercard";
    let index = if store { 2 } else { 1 };
    let count = integer(a[index])?;
    if count < 1 {
        return Err(Reply::OwnedError(format!(
            "ERR at least 1 input key is needed for '{}' command",
            String::from_utf8_lossy(&command)
        )));
    }
    let count = key_count(a, index).ok_or(SYNTAX)?;
    let start = index + 1;
    let mut inputs = Vec::with_capacity(count);
    for key in &a[start..start + count] {
        let data = match engine.get(key, now) {
            None => None,
            Some(entry) => match entry.value.collection() {
                Some(data @ (Collection::Set(_) | Collection::ZSet(_))) => Some(data),
                _ => return Err(WRONGTYPE),
            },
        };
        inputs.push(data);
    }
    let mut withscores = false;
    let mut weights = vec![1.0; count];
    let mut aggregate = Aggregate::Sum;
    let mut limit = 0;
    let mut i = start + count;
    while i < a.len() {
        if !store && !cardinality && a[i].eq_ignore_ascii_case(b"WITHSCORES") {
            withscores = true;
            i += 1;
        } else if !difference
            && !cardinality
            && a[i].eq_ignore_ascii_case(b"WEIGHTS")
            && a.len() - i > count
        {
            for (weight, raw) in weights.iter_mut().zip(&a[i + 1..i + 1 + count]) {
                *weight = super::score::parse(raw)
                    .map_err(|_| Reply::Error("ERR weight value is not a float"))?;
            }
            i += 1 + count;
        } else if !difference
            && !cardinality
            && a[i].eq_ignore_ascii_case(b"AGGREGATE")
            && i + 1 < a.len()
        {
            aggregate = if a[i + 1].eq_ignore_ascii_case(b"SUM") {
                Aggregate::Sum
            } else if a[i + 1].eq_ignore_ascii_case(b"MIN") {
                Aggregate::Min
            } else if a[i + 1].eq_ignore_ascii_case(b"MAX") {
                Aggregate::Max
            } else {
                return Err(SYNTAX);
            };
            i += 2;
        } else if cardinality && a[i].eq_ignore_ascii_case(b"LIMIT") && i + 1 < a.len() {
            limit = u64::try_from(integer(a[i + 1])?)
                .map_err(|_| Reply::Error("ERR LIMIT can't be negative"))?;
            i += 2;
        } else {
            return Err(SYNTAX);
        }
    }
    let mut sources: Vec<_> = inputs.into_iter().zip(weights).collect();
    if !difference {
        sources.sort_by_key(|(data, _)| data.map_or(0, Collection::len));
    }
    let mut result = SortedSet::default();
    if difference || intersection {
        let mut matched = 0_u64;
        for (member, score) in members(sources[0].0) {
            let mut total = normalize(score * sources[0].1);
            let mut include = true;
            for (data, weight) in &sources[1..] {
                match lookup(*data, &member) {
                    Some(_) if difference => {
                        include = false;
                        break;
                    }
                    None if intersection => {
                        include = false;
                        break;
                    }
                    Some(score) => total = aggregate.apply(total, score * weight),
                    None => {}
                }
            }
            if include {
                matched += 1;
                if cardinality {
                    if limit != 0 && matched >= limit {
                        break;
                    }
                } else {
                    result.insert(member, if difference { score } else { total });
                }
            }
        }
        if cardinality {
            return Ok(Reply::Integer(matched as i64));
        }
    } else {
        for (data, weight) in sources {
            for (member, score) in members(data) {
                let value = normalize(score * weight);
                let score = result
                    .scores
                    .get(&member)
                    .map_or(value, |old| aggregate.apply(old.0, value));
                result.insert(member, score);
            }
        }
    }
    if !store {
        return super::zset::pairs(
            &result.order.iter().cloned().collect::<Vec<_>>(),
            withscores,
        );
    }
    let count = result.scores.len();
    let entry = if count == 0 {
        None
    } else {
        Some(Entry {
            value: Value::from_collection(Collection::ZSet(result))
                .map_err(|_| Reply::Error("ERR collection exceeds maximum size"))?,
            expires_at_ms: None,
        })
    };
    if entry.is_none() && engine.get(a[1], now).is_none() {
        return Ok(Reply::Integer(0));
    }
    Ok(engine.apply_typed(
        vec![(a[1].to_vec(), entry)],
        Reply::Integer(count as i64),
        submit,
    ))
}

#[derive(Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
}
impl Aggregate {
    fn apply(self, old: f64, value: f64) -> f64 {
        match self {
            Self::Sum => normalize(old + value),
            Self::Min => {
                if value < old {
                    value
                } else {
                    old
                }
            }
            Self::Max => {
                if value > old {
                    value
                } else {
                    old
                }
            }
        }
    }
}
fn normalize(value: f64) -> f64 {
    if value.is_nan() {
        0.0
    } else {
        value
    }
}
fn members(data: Option<&Collection>) -> Vec<(crate::engine::Bytes, f64)> {
    match data {
        Some(Collection::Set(set)) => set.iter().map(|m| (m.clone(), 1.0)).collect(),
        Some(Collection::ZSet(set)) => set.scores.iter().map(|(m, s)| (m.clone(), s.0)).collect(),
        _ => Vec::new(),
    }
}
fn lookup(data: Option<&Collection>, member: &[u8]) -> Option<f64> {
    match data {
        Some(Collection::Set(set)) => set.contains(member).then_some(1.0),
        Some(Collection::ZSet(set)) => set.scores.get(member).map(|s| s.0),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn weighted_stores_are_atomic() {
        for command in [b"ZUNIONSTORE".as_slice(), b"ZINTERSTORE"] {
            let mut engine = Engine::new(65536);
            engine.execute(&[b"ZADD", b"a", b"2", b"x"], 0, |_| true);
            engine.execute(&[b"SADD", b"b", b"x"], 0, |_| true);
            engine.execute(&[b"PEXPIREAT", b"a", b"5000"], 0, |_| true);
            let before = engine.snapshot(b"a");
            let args: &[&[u8]] = &[command, b"a", b"2", b"a", b"b", b"WEIGHTS", b"3", b"4"];
            assert!(matches!(
                engine.execute(args, 1, |_| false),
                Reply::Error(_)
            ));
            assert_eq!(engine.snapshot(b"a"), before);
            assert_eq!(engine.execute(args, 1, |_| true), Reply::Integer(1));
            assert_eq!(engine.snapshot(b"a").unwrap().expires_at_ms, None);
            assert_eq!(
                engine.execute(&[b"ZSCORE", b"a", b"x"], 1, |_| true),
                bulk(b"10")
            );
        }
    }
    #[test]
    fn store_overlapping_source_is_atomic_and_clears_ttl() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"ZADD", b"a", b"1.5", b"x", b"2", b"y"], 0, |_| true);
        engine.execute(&[b"SADD", b"b", b"y"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"a", b"5000"], 0, |_| true);
        let before = engine.snapshot(b"a");
        let args: &[&[u8]] = &[b"ZDIFFSTORE", b"a", b"2", b"a", b"b"];
        assert!(matches!(
            engine.execute(args, 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"a"), before);
        assert_eq!(engine.execute(args, 1, |_| true), Reply::Integer(1));
        assert_eq!(engine.snapshot(b"a").unwrap().expires_at_ms, None);
        assert_eq!(
            engine.execute(&[b"ZSCORE", b"a", b"x"], 1, |_| true),
            bulk(b"1.5")
        );
    }
}
