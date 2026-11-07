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
fn run(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply> {
    let Some(source) = engine.get(a[1], now) else {
        return Ok(Reply::Integer(0));
    };
    let Some(Collection::Set(source_set)) = source.value.collection() else {
        return Err(WRONGTYPE);
    };
    let destination = engine.get(a[2], now);
    if destination.is_some_and(|entry| entry.value.kind() != 3) {
        return Err(WRONGTYPE);
    }
    let present = source_set.contains(a[3]);
    if !present || a[1] == a[2] {
        return Ok(Reply::Integer(i64::from(present)));
    }
    let mut remaining = source_set.clone();
    remaining.remove(a[3]);
    let source_entry = entry(remaining, source.expires_at_ms)?;
    let mut changes = vec![(a[1].to_vec(), source_entry)];
    let mut target = match destination.and_then(|entry| entry.value.collection()) {
        Some(Collection::Set(set)) => set.clone(),
        _ => std::collections::HashSet::new(),
    };
    if target.insert(a[3].into()) {
        changes.push((
            a[2].to_vec(),
            entry(target, destination.and_then(|entry| entry.expires_at_ms))?,
        ));
    }
    Ok(engine.apply_typed(changes, Reply::Integer(1), submit))
}
fn entry(
    set: std::collections::HashSet<crate::engine::Bytes>,
    expires_at_ms: Option<i64>,
) -> Result<Option<Entry>> {
    if set.is_empty() {
        return Ok(None);
    }
    Ok(Some(Entry {
        value: Value::from_collection(Collection::Set(set))
            .map_err(|_| Reply::Error("ERR collection exceeds maximum size"))?,
        expires_at_ms,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admission_is_atomic_and_duplicates_do_not_rewrite_destination() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SADD", b"a", b"x", b"y"], 0, |_| true);
        engine.execute(&[b"SADD", b"b", b"y"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"a", b"5000"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"b", b"6000"], 0, |_| true);
        let a = engine.snapshot(b"a");
        let b = engine.snapshot(b"b");
        assert!(matches!(
            engine.execute(&[b"SMOVE", b"a", b"b", b"x"], 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"a"), a);
        assert_eq!(engine.snapshot(b"b"), b);
        assert_eq!(
            engine.execute(&[b"SMOVE", b"a", b"a", b"x"], 1, |_| panic!("same key")),
            Reply::Integer(1)
        );
        assert_eq!(
            engine.execute(&[b"SMOVE", b"a", b"b", b"x"], 1, |changes| {
                assert_eq!(changes.len(), 2);
                true
            }),
            Reply::Integer(1)
        );
        assert_eq!(engine.snapshot(b"a").unwrap().expires_at_ms, Some(5000));
        assert_eq!(engine.snapshot(b"b").unwrap().expires_at_ms, Some(6000));
        assert_eq!(
            engine.execute(&[b"SMOVE", b"a", b"b", b"y"], 1, |changes| {
                assert_eq!(changes.len(), 1);
                true
            }),
            Reply::Integer(1)
        );
        assert!(engine.snapshot(b"a").is_none());
    }
}
