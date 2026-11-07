use super::*;
pub(super) fn run(cmd: &str, a: &[&[u8]], data: &mut Cow<'_, Collection>) -> Result<(Reply, bool)> {
    let Collection::List(list) = data.as_ref() else {
        unreachable!()
    };
    match cmd {
        "lpos" => {
            let (rank, count, maxlen) = position_options(a)?;
            let mut matches = 0_u64;
            let mut found = Vec::new();
            for step in 0..list.len().min(if maxlen == 0 {
                usize::MAX
            } else {
                maxlen as usize
            }) {
                let index = if rank < 0 {
                    list.len() - step - 1
                } else {
                    step
                };
                if list[index].as_ref() != a[2] {
                    continue;
                }
                matches += 1;
                if matches < rank.unsigned_abs() {
                    continue;
                }
                if count.is_none() {
                    return Ok((Reply::Integer(index as i64), false));
                }
                if found.len() >= (2 * crate::protocol::MAX_FRAME - 32) / 32 {
                    return Err(Reply::Error("ERR response exceeds configured limit"));
                }
                found.push(Reply::Integer(index as i64));
                if count.is_some_and(|n| n != 0 && found.len() as u64 >= n) {
                    break;
                }
            }
            return Ok((
                if count.is_some() {
                    Reply::Array(found)
                } else {
                    Reply::Bulk(None)
                },
                false,
            ));
        }
        "linsert" => {
            if list.is_empty() {
                return Ok((Reply::Integer(0), false));
            }
            let Some(index) = list.iter().position(|v| v.as_ref() == a[3]) else {
                return Ok((Reply::Integer(-1), false));
            };
            let after = a[2].eq_ignore_ascii_case(b"AFTER");
            let Collection::List(list) = data.to_mut() else {
                unreachable!()
            };
            list.insert(index + usize::from(after), a[4].into());
            return Ok((Reply::Integer(list.len() as i64), true));
        }
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
                return Ok((
                    if a.len() == 3 {
                        Reply::NullArray
                    } else {
                        Reply::Bulk(None)
                    },
                    false,
                ));
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

// Validate before key lookup, matching Redis's syntax/type error precedence.
pub(super) fn validate(cmd: &str, a: &[&[u8]]) -> Result<()> {
    if cmd == "lpos" {
        position_options(a)?;
    }
    if cmd == "linsert"
        && !a[2].eq_ignore_ascii_case(b"BEFORE")
        && !a[2].eq_ignore_ascii_case(b"AFTER")
    {
        return Err(SYNTAX);
    }
    Ok(())
}
fn position_options(a: &[&[u8]]) -> Result<(i64, Option<u64>, u64)> {
    let (mut rank, mut count, mut maxlen) = (1, None, 0);
    let mut i = 3;
    while i < a.len() {
        if i + 1 == a.len() {
            return Err(SYNTAX);
        }
        if a[i].eq_ignore_ascii_case(b"RANK") {
            rank = integer(a[i + 1])?;
            if rank == i64::MIN {
                return Err(INTEGER);
            }
            if rank == 0 {
                return Err(Reply::Error("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list"));
            }
        } else if a[i].eq_ignore_ascii_case(b"COUNT") {
            count = Some(
                u64::try_from(integer(a[i + 1])?)
                    .map_err(|_| Reply::Error("ERR COUNT can't be negative"))?,
            );
        } else if a[i].eq_ignore_ascii_case(b"MAXLEN") {
            maxlen = u64::try_from(integer(a[i + 1])?)
                .map_err(|_| Reply::Error("ERR MAXLEN can't be negative"))?;
        } else {
            return Err(SYNTAX);
        }
        i += 2;
    }
    Ok((rank, count, maxlen))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_pop_with_count_uses_null_array() {
        let mut engine = Engine::new(65536);
        for command in [b"LPOP".as_slice(), b"RPOP"] {
            let mut out = Vec::new();
            engine
                .execute(&[command, b"missing", b"0"], 0, |_| true)
                .encode(&mut out);
            assert_eq!(out, b"*-1\r\n");
            out.clear();
            engine
                .execute(&[command, b"missing"], 0, |_| true)
                .encode(&mut out);
            assert_eq!(out, b"$-1\r\n");
        }
    }
    #[test]
    fn insert_rejection_preserves_list_and_deadline() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"RPUSH", b"k", b"a", b"b", b"a"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"k", b"5000"], 0, |_| true);
        let before = engine.snapshot(b"k");
        assert!(matches!(
            engine.execute(&[b"LINSERT", b"k", b"AFTER", b"a", b"x"], 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"k"), before);
        assert_eq!(
            engine.execute(
                &[b"LINSERT", b"k", b"AFTER", b"missing", b"x"],
                1,
                |_| panic!("no insertion")
            ),
            Reply::Integer(-1)
        );
        assert_eq!(
            engine.execute(&[b"LINSERT", b"k", b"AFTER", b"a", b"x"], 1, |_| true),
            Reply::Integer(4)
        );
        assert_eq!(engine.snapshot(b"k").unwrap().expires_at_ms, Some(5000));
        assert_eq!(
            engine.execute(
                &[b"LPOS", b"k", b"a", b"COUNT", b"0", b"RANK", b"-1"],
                1,
                |_| true
            ),
            Reply::Array(vec![Reply::Integer(3), Reply::Integer(0)])
        );
    }
}
