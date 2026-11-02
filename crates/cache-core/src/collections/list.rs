use super::*;
pub(super) fn run(cmd: &str, a: &[&[u8]], data: &mut Cow<'_, Collection>) -> Result<(Reply, bool)> {
    let Collection::List(list) = data.as_ref() else {
        unreachable!()
    };
    match cmd {
        "llen" => return Ok((Reply::Integer(list.len() as i64), false)),
        "lindex" => {
            let n = integer(a[2])?;
            let i = if n < 0 {
                (list.len() as i64).saturating_add(n)
            } else {
                n
            };
            return Ok((
                Reply::Bulk(
                    usize::try_from(i)
                        .ok()
                        .and_then(|i| list.get(i))
                        .map(|v| v.to_vec()),
                ),
                false,
            ));
        }
        "lrange" => {
            let r = range(integer(a[2])?, integer(a[3])?, list.len());
            return Ok((
                array(
                    list.iter()
                        .skip(r.start)
                        .take(r.len())
                        .map(|v| Some(v.as_ref())),
                )?,
                false,
            ));
        }
        _ => {}
    }
    let Collection::List(list) = data.to_mut() else {
        unreachable!()
    };
    match cmd {
        "lpush" | "rpush" | "lpushx" | "rpushx" => {
            if cmd.ends_with('x') && list.is_empty() {
                return Ok((Reply::Integer(0), false));
            }
            for v in &a[2..] {
                if cmd.starts_with('l') {
                    list.push_front((*v).into());
                } else {
                    list.push_back((*v).into());
                }
            }
            Ok((Reply::Integer(list.len() as i64), true))
        }
        "lpop" | "rpop" => {
            if a.len() > 3 {
                return Err(SYNTAX);
            }
            let count = if a.len() == 3 {
                usize::try_from(integer(a[2])?)
                    .map_err(|_| Reply::Error("ERR value is out of range, must be positive"))?
            } else {
                1
            };
            if list.is_empty() {
                return Ok((Reply::Bulk(None), false));
            }
            let n = count.min(list.len());
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                out.push(if cmd == "lpop" {
                    list.pop_front().unwrap()
                } else {
                    list.pop_back().unwrap()
                });
            }
            let reply = if a.len() == 2 {
                bulk(&out[0])
            } else {
                array(out.iter().map(|v| Some(v.as_ref())))?
            };
            Ok((reply, n > 0))
        }
        "lset" => {
            let n = integer(a[2])?;
            if list.is_empty() {
                return Err(Reply::Error("ERR no such key"));
            }
            let n = if n < 0 {
                (list.len() as i64).saturating_add(n)
            } else {
                n
            };
            let value = usize::try_from(n)
                .ok()
                .and_then(|n| list.get_mut(n))
                .ok_or(Reply::Error("ERR index out of range"))?;
            *value = a[3].into();
            Ok((Reply::Simple("OK"), true))
        }
        "ltrim" => {
            let r = range(integer(a[2])?, integer(a[3])?, list.len());
            let changed = r.len() != list.len();
            list.truncate(r.end);
            *list = list.skip(r.start);
            Ok((Reply::Simple("OK"), changed))
        }
        "lrem" => {
            let count = integer(a[2])?;
            let max = if count == 0 {
                u64::MAX
            } else {
                count.unsigned_abs()
            };
            let mut removed = 0;
            if count < 0 {
                *list = list.iter().rev().cloned().collect();
            }
            list.retain(|v| {
                if removed < max && v.as_ref() == a[3] {
                    removed += 1;
                    false
                } else {
                    true
                }
            });
            if count < 0 {
                *list = list.iter().rev().cloned().collect();
            }
            Ok((Reply::Integer(removed as i64), removed > 0))
        }
        _ => unreachable!(),
    }
}
