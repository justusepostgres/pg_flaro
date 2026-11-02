use super::score::parse as score;
use super::*;
use crate::value::{Score, SortedSet};
fn format(n: f64) -> Vec<u8> {
    if n == 0.0 {
        b"0".to_vec()
    } else {
        n.to_string().into_bytes()
    }
}
fn bound(v: &[u8]) -> Result<(f64, bool)> {
    if let Some(rest) = v.strip_prefix(b"(") {
        Ok((score(rest)?, true))
    } else {
        Ok((score(v)?, false))
    }
}
fn in_range(s: f64, lo: (f64, bool), hi: (f64, bool)) -> bool {
    (s > lo.0 || s == lo.0 && !lo.1) && (s < hi.0 || s == hi.0 && !hi.1)
}
fn pairs(values: &[(Score, crate::engine::Bytes)], with_scores: bool) -> Result<Reply> {
    let mut bytes = 32;
    let mut out = Vec::new();
    for (s, m) in values {
        let score = format(s.0);
        bytes += m.len() + 32 + if with_scores { score.len() + 32 } else { 0 };
        if bytes > 2 * crate::protocol::MAX_FRAME {
            return Err(Reply::Error("ERR reply exceeds output limit"));
        }
        out.push(bulk(m));
        if with_scores {
            out.push(bulk(score));
        }
    }
    Ok(Reply::Array(out))
}
pub(super) fn run(cmd: &str, a: &[&[u8]], data: &mut Cow<'_, Collection>) -> Result<(Reply, bool)> {
    let Collection::ZSet(z) = data.as_ref() else {
        unreachable!()
    };
    match cmd {
        "zcard" => return Ok((Reply::Integer(z.scores.len() as i64), false)),
        "zscore" => {
            return Ok((
                z.scores
                    .get(a[2])
                    .map_or(Reply::Bulk(None), |s| bulk(format(s.0))),
                false,
            ))
        }
        "zmscore" => {
            return Ok((
                Reply::Array(
                    a[2..]
                        .iter()
                        .map(|k| {
                            z.scores
                                .get(*k)
                                .map_or(Reply::Bulk(None), |s| bulk(format(s.0)))
                        })
                        .collect(),
                ),
                false,
            ))
        }
        "zrank" | "zrevrank" => {
            let rank = z.scores.get(a[2]).and_then(|s| {
                z.order
                    .iter()
                    .position(|(sc, k)| sc == s && k.as_ref() == a[2])
            });
            return Ok((
                rank.map_or(Reply::Bulk(None), |r| {
                    Reply::Integer(if cmd == "zrank" {
                        r
                    } else {
                        z.scores.len() - 1 - r
                    } as i64)
                }),
                false,
            ));
        }
        "zcount" => {
            let lo = bound(a[2])?;
            let hi = bound(a[3])?;
            return Ok((
                Reply::Integer(
                    z.order
                        .iter()
                        .filter(|(s, _)| in_range(s.0, lo, hi))
                        .count() as i64,
                ),
                false,
            ));
        }
        "zrange" | "zrevrange" | "zrangebyscore" | "zrevrangebyscore" => {
            return Ok((get_range(cmd, a, z)?, false))
        }
        _ => {}
    }
    let Collection::ZSet(z) = data.to_mut() else {
        unreachable!()
    };
    match cmd {
        "zadd" | "zincrby" => add(cmd, a, z),
        "zrem" => {
            let n = a[2..].iter().filter(|k| z.remove(k)).count();
            Ok((Reply::Integer(n as i64), n > 0))
        }
        "zpopmin" | "zpopmax" => {
            if a.len() > 3 {
                return Err(SYNTAX);
            }
            let count = if a.len() == 3 {
                usize::try_from(integer(a[2])?).map_err(|_| INTEGER)?
            } else {
                1
            };
            let values: Vec<_> = if cmd == "zpopmin" {
                z.order.iter().take(count).cloned().collect()
            } else {
                z.order.iter().rev().take(count).cloned().collect()
            };
            let reply = pairs(&values, true)?;
            for (_, m) in &values {
                z.remove(m);
            }
            Ok((reply, !values.is_empty()))
        }
        "zremrangebyrank" | "zremrangebyscore" => {
            let selected: Vec<_> = if cmd == "zremrangebyrank" {
                let r = range(integer(a[2])?, integer(a[3])?, z.scores.len());
                z.order
                    .iter()
                    .skip(r.start)
                    .take(r.len())
                    .map(|(_, k)| k.clone())
                    .collect()
            } else {
                let lo = bound(a[2])?;
                let hi = bound(a[3])?;
                z.order
                    .iter()
                    .filter(|(s, _)| in_range(s.0, lo, hi))
                    .map(|(_, k)| k.clone())
                    .collect()
            };
            for k in &selected {
                z.remove(k);
            }
            Ok((Reply::Integer(selected.len() as i64), !selected.is_empty()))
        }
        _ => unreachable!(),
    }
}
fn add(cmd: &str, a: &[&[u8]], z: &mut SortedSet) -> Result<(Reply, bool)> {
    let (mut nx, mut xx, mut gt, mut lt, mut ch, mut incr) =
        (false, false, false, false, false, cmd == "zincrby");
    let mut i = 2;
    if cmd == "zadd" {
        while i < a.len() {
            match a[i].to_ascii_uppercase().as_slice() {
                b"NX" => nx = true,
                b"XX" => xx = true,
                b"GT" => gt = true,
                b"LT" => lt = true,
                b"CH" => ch = true,
                b"INCR" => incr = true,
                _ => break,
            }
            i += 1;
        }
    }
    if nx && (xx || gt || lt)
        || gt && lt
        || i == a.len()
        || !(a.len() - i).is_multiple_of(2)
        || incr && a.len() - i != 2
    {
        return Err(SYNTAX);
    }
    let mut added = 0;
    let mut changed = 0;
    let mut result = Reply::Bulk(None);
    for pair in a[i..].chunks_exact(2) {
        let mut value = score(pair[0])?;
        let old = z.scores.get(pair[1]).map(|s| s.0);
        if nx && old.is_some() || xx && old.is_none() {
            continue;
        }
        if incr {
            value += old.unwrap_or(0.0);
            if value.is_nan() {
                return Err(Reply::Error("ERR resulting score is not a number (NaN)"));
            }
        }
        if old.is_some_and(|old| gt && value <= old || lt && value >= old) {
            continue;
        }
        added += i64::from(old.is_none());
        changed += i64::from(old != Some(value));
        z.insert(pair[1].into(), value);
        result = bulk(format(value));
    }
    Ok((
        if incr {
            result
        } else {
            Reply::Integer(if ch { changed } else { added })
        },
        changed > 0,
    ))
}
fn get_range(cmd: &str, a: &[&[u8]], z: &SortedSet) -> Result<Reply> {
    let mut reverse = cmd.starts_with("zrev");
    let mut byscore = cmd.ends_with("byscore");
    let mut withscores = false;
    let mut limit = None;
    let mut i = 4;
    while i < a.len() {
        match a[i].to_ascii_uppercase().as_slice() {
            b"REV" if cmd == "zrange" => reverse = true,
            b"BYSCORE" if cmd == "zrange" => byscore = true,
            b"WITHSCORES" => withscores = true,
            b"LIMIT" if i + 2 < a.len() => {
                limit = Some((integer(a[i + 1])?, integer(a[i + 2])?));
                i += 2;
            }
            _ => return Err(SYNTAX),
        }
        i += 1;
    }
    if !byscore && limit.is_some() {
        return Err(SYNTAX);
    }
    let ordered: Vec<_> = if reverse {
        z.order.iter().rev().cloned().collect()
    } else {
        z.order.iter().cloned().collect()
    };
    let selected: Vec<_> = if byscore {
        let (lo, hi) = if reverse {
            (bound(a[3])?, bound(a[2])?)
        } else {
            (bound(a[2])?, bound(a[3])?)
        };
        let (offset, count) = limit.unwrap_or((0, -1));
        if offset < 0 {
            return Ok(Reply::Array(Vec::new()));
        }
        ordered
            .into_iter()
            .filter(|(s, _)| in_range(s.0, lo, hi))
            .skip(offset as usize)
            .take(if count < 0 {
                usize::MAX
            } else {
                count as usize
            })
            .collect()
    } else {
        let r = range(integer(a[2])?, integer(a[3])?, ordered.len());
        ordered.into_iter().skip(r.start).take(r.len()).collect()
    };
    pairs(&selected, withscores)
}
