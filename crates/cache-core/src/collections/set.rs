use super::*;
pub(super) fn run(cmd: &str, a: &[&[u8]], data: &mut Cow<'_, Collection>) -> Result<(Reply, bool)> {
    let Collection::Set(set) = data.as_ref() else {
        unreachable!()
    };
    match cmd {
        "scard" => return Ok((Reply::Integer(set.len() as i64), false)),
        "sismember" => return Ok((Reply::Integer(i64::from(set.contains(a[2]))), false)),
        "smismember" => {
            return Ok((
                Reply::Array(
                    a[2..]
                        .iter()
                        .map(|k| Reply::Integer(i64::from(set.contains(*k))))
                        .collect(),
                ),
                false,
            ))
        }
        "smembers" => return Ok((array(set.iter().map(|v| Some(v.as_ref())))?, false)),
        _ => {}
    }
    let Collection::Set(set) = data.to_mut() else {
        unreachable!()
    };
    let mut n = 0;
    for member in &a[2..] {
        n += i64::from(if cmd == "sadd" {
            set.insert((*member).into())
        } else {
            set.remove(*member)
        });
    }
    Ok((Reply::Integer(n), n > 0))
}
pub(super) fn algebra(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    cmd: &str,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Reply {
    let store = cmd.ends_with("store");
    let keys = &a[if store { 2 } else { 1 }..];
    // Validate every input, even when an earlier empty input decides the result.
    let mut sets = Vec::new();
    for key in keys {
        match engine.get(key, now).map(|e| &e.value) {
            None => sets.push(None),
            Some(v) => match v.collection() {
                Some(Collection::Set(s)) => sets.push(Some(s)),
                _ => return WRONGTYPE,
            },
        }
    }
    let mut result = sets[0].cloned().unwrap_or_default();
    for set in &sets[1..] {
        if cmd.starts_with("sunion") {
            if let Some(s) = set {
                result.extend(s.iter().cloned());
            }
        } else if cmd.starts_with("sinter") {
            result.retain(|k| set.is_some_and(|s| s.contains(k)));
        } else if let Some(s) = set {
            result.retain(|k| !s.contains(k));
        }
        // Bound union work before constructing the canonical snapshot.
        if result.iter().map(|k| k.len() + 160).sum::<usize>()
            > crate::server::MAX_PENDING_BYTES / 2
        {
            return Reply::Error("ERR collection exceeds maximum size");
        }
    }
    if !store {
        return array(result.iter().map(|v| Some(v.as_ref()))).unwrap_or_else(|e| e);
    }
    let count = result.len();
    let entry = if result.is_empty() {
        None
    } else {
        match Value::from_collection(Collection::Set(result)) {
            Ok(value) => Some(Entry {
                value,
                expires_at_ms: None,
            }),
            Err(_) => return Reply::Error("ERR collection exceeds maximum size"),
        }
    };
    engine.apply_typed(
        vec![(a[1].to_vec(), entry)],
        Reply::Integer(count as i64),
        submit,
    )
}
