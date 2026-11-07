use super::*;

pub(crate) fn key_count(a: &[&[u8]]) -> Option<usize> {
    let count = usize::try_from(integer(a.get(1)?).ok()?).ok()?;
    (count > 0 && count <= a.len().saturating_sub(2)).then_some(count)
}
pub(super) fn execute(engine: &Engine, a: &[&[u8]], now: i64) -> Reply {
    match run(engine, a, now) {
        Ok(reply) | Err(reply) => reply,
    }
}
fn run(engine: &Engine, a: &[&[u8]], now: i64) -> Result<Reply> {
    let count = integer(a[1]).map_err(|_| Reply::Error("ERR numkeys should be greater than 0"))?;
    if count <= 0 {
        return Err(Reply::Error("ERR numkeys should be greater than 0"));
    }
    if count as u64 > a.len().saturating_sub(2) as u64 {
        return Err(Reply::Error(
            "ERR Number of keys can't be greater than number of args",
        ));
    }
    let count = count as usize;
    let mut limit = 0;
    let mut i = count + 2;
    while i < a.len() {
        if !a[i].eq_ignore_ascii_case(b"LIMIT") || i + 1 >= a.len() {
            return Err(SYNTAX);
        }
        limit = u64::try_from(integer(a[i + 1])?)
            .map_err(|_| Reply::Error("ERR LIMIT can't be negative"))?;
        i += 2;
    }
    let mut sets = Vec::with_capacity(count);
    let mut missing = false;
    for key in &a[2..2 + count] {
        match engine.get(key, now) {
            None => missing = true,
            Some(entry) => match entry.value.collection() {
                Some(Collection::Set(set)) => sets.push(set),
                _ => return Err(WRONGTYPE),
            },
        }
    }
    if missing {
        return Ok(Reply::Integer(0));
    }
    sets.sort_unstable_by_key(|set| set.len());
    let mut result = 0;
    for member in sets[0] {
        if sets[1..].iter().all(|set| set.contains(member)) {
            result += 1;
            if limit != 0 && result >= limit {
                break;
            }
        }
    }
    Ok(Reply::Integer(result as i64))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn key_count_excludes_options_and_invalid_counts() {
        assert_eq!(
            key_count(&[b"SINTERCARD", b"2", b"a", b"b", b"LIMIT", b"1"]),
            Some(2)
        );
        for args in [
            vec![b"SINTERCARD".as_slice(), b"0", b"a"],
            vec![b"SINTERCARD", b"3", b"a"],
            vec![b"SINTERCARD", b"01", b"a"],
        ] {
            assert_eq!(key_count(&args), None);
        }
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SADD", b"a", b"x", b"y"], 0, |_| true);
        assert_eq!(
            engine.execute(
                &[b"SINTERCARD", b"2", b"a", b"a", b"LIMIT", b"1"],
                0,
                |_| panic!("read only")
            ),
            Reply::Integer(1)
        );
        engine.execute(&[b"SET", b"wrong", b"v"], 0, |_| true);
        assert_eq!(
            engine.execute(&[b"SINTERCARD", b"2", b"missing", b"wrong"], 0, |_| true),
            WRONGTYPE
        );
    }
}
