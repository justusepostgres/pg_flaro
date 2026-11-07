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
    let (from_left, to_left) = if a[0].eq_ignore_ascii_case(b"LMOVE") {
        (side(a[3])?, side(a[4])?)
    } else {
        (false, true)
    };
    let Some(source) = engine.get(a[1], now) else {
        return Ok(Reply::Bulk(None));
    };
    let Some(Collection::List(list)) = source.value.collection() else {
        return Err(WRONGTYPE);
    };
    if list.is_empty() {
        return Ok(Reply::Bulk(None));
    }
    let destination = engine.get(a[2], now);
    if destination.is_some_and(|e| e.value.kind() != 2) {
        return Err(WRONGTYPE);
    }
    let source_expiry = source.expires_at_ms;
    let destination_expiry = destination.and_then(|e| e.expires_at_ms);
    let mut source_list = list.clone();
    let value = if from_left {
        source_list.pop_front()
    } else {
        source_list.pop_back()
    }
    .unwrap();
    let reply = bulk(&value);
    let changes = if a[1] == a[2] {
        if to_left {
            source_list.push_front(value);
        } else {
            source_list.push_back(value);
        }
        vec![(a[1].to_vec(), entry(source_list, source_expiry)?)]
    } else {
        let mut target_list = match destination.and_then(|e| e.value.collection()) {
            Some(Collection::List(list)) => list.clone(),
            _ => im::Vector::new(),
        };
        if to_left {
            target_list.push_front(value);
        } else {
            target_list.push_back(value);
        }
        vec![
            (a[1].to_vec(), entry(source_list, source_expiry)?),
            (a[2].to_vec(), entry(target_list, destination_expiry)?),
        ]
    };
    Ok(engine.apply_typed(changes, reply, submit))
}
fn side(value: &[u8]) -> Result<bool> {
    if value.eq_ignore_ascii_case(b"LEFT") {
        Ok(true)
    } else if value.eq_ignore_ascii_case(b"RIGHT") {
        Ok(false)
    } else {
        Err(SYNTAX)
    }
}
fn entry(list: im::Vector<crate::engine::Bytes>, expiry: Option<i64>) -> Result<Option<Entry>> {
    if list.is_empty() {
        return Ok(None);
    }
    Ok(Some(Entry {
        value: Value::from_collection(Collection::List(list))
            .map_err(|_| Reply::Error("ERR collection exceeds maximum size"))?,
        expires_at_ms: expiry,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn moves_reserve_both_changes_before_mutation() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"RPUSH", b"a", b"x", b"y"], 0, |_| true);
        engine.execute(&[b"RPUSH", b"b", b"z"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"a", b"5000"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"b", b"6000"], 0, |_| true);
        let a = engine.snapshot(b"a");
        let b = engine.snapshot(b"b");
        let args: &[&[u8]] = &[b"LMOVE", b"a", b"b", b"RIGHT", b"LEFT"];
        assert!(matches!(
            engine.execute(args, 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"a"), a);
        assert_eq!(engine.snapshot(b"b"), b);
        assert_eq!(
            engine.execute(args, 1, |changes| {
                assert_eq!(changes.len(), 2);
                true
            }),
            bulk(b"y")
        );
        assert_eq!(engine.snapshot(b"a").unwrap().expires_at_ms, Some(5000));
        assert_eq!(engine.snapshot(b"b").unwrap().expires_at_ms, Some(6000));
        assert_eq!(
            engine.execute(&[b"RPOPLPUSH", b"a", b"b"], 1, |_| true),
            bulk(b"x")
        );
        assert!(engine.snapshot(b"a").is_none());
        assert_eq!(
            engine.execute(&[b"LMOVE", b"b", b"b", b"LEFT", b"RIGHT"], 1, |changes| {
                assert_eq!(changes.len(), 1);
                true
            }),
            bulk(b"x")
        );
    }
}
