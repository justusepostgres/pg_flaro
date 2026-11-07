use super::score::parse as score;
use super::*;
use crate::value::{Score, SortedSet};
pub(super) fn format(n: f64) -> Vec<u8> {
    if n == 0.0 {
        b"0".to_vec()
    } else {
        crate::protocol::number::double(n).into_bytes()
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
pub(super) fn pairs(values: &[(Score, crate::engine::Bytes)], with_scores: bool) -> Result<Reply> {
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
            let withscore = a.len() == 4;
            return Ok((
                rank.map_or(
                    if withscore {
                        Reply::NullArray
                    } else {
                        Reply::Bulk(None)
                    },
                    |r| {
                        let rank = Reply::Integer(if cmd == "zrank" {
                            r
                        } else {
                            z.scores.len() - 1 - r
                        } as i64);
                        if withscore {
                            Reply::Array(vec![rank, bulk(format(z.scores.get(a[2]).unwrap().0))])
                        } else {
                            rank
                        }
                    },
                ),
                false,
            ));
        }

        "zlexcount" => {
            let lo = lex_bound(a[2])?;
            let hi = lex_bound(a[3])?;
            return Ok((
                Reply::Integer(
                    z.order
                        .iter()
                        .filter(|(_, member)| lex_contains(member, &lo, &hi))
                        .count() as i64,
                ),
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
        "zrange" | "zrevrange" | "zrangebyscore" | "zrevrangebyscore" | "zrangebylex"
        | "zrevrangebylex" => return Ok((get_range(cmd, a, z)?, false)),
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
        "zremrangebylex" => {
            let lo = lex_bound(a[2])?;
            let hi = lex_bound(a[3])?;
            let members: Vec<_> = z
                .order
                .iter()
                .filter(|(_, member)| lex_contains(member, &lo, &hi))
                .map(|(_, member)| member.clone())
                .collect();
            for member in &members {
                z.remove(member);
            }
            Ok((Reply::Integer(members.len() as i64), !members.is_empty()))
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
    let (selected, withscores) = select_range(cmd, a, z)?;
    pairs(&selected, withscores)
}
type Selected = Vec<(Score, crate::engine::Bytes)>;
fn select_range(cmd: &str, a: &[&[u8]], z: &SortedSet) -> Result<(Selected, bool)> {
    let mut reverse = cmd.starts_with("zrev");
    let mut byscore = cmd.ends_with("byscore");
    let mut bylex = cmd.ends_with("bylex");
    let mut withscores = false;
    let mut limit = None;
    let mut i = 4;
    while i < a.len() {
        match a[i].to_ascii_uppercase().as_slice() {
            b"REV" if matches!(cmd, "zrange" | "zrangestore") => reverse = true,
            b"BYSCORE" if matches!(cmd, "zrange" | "zrangestore") && !bylex => byscore = true,
            b"BYLEX" if matches!(cmd, "zrange" | "zrangestore") && !byscore => bylex = true,
            b"WITHSCORES" if cmd != "zrangestore" && !cmd.ends_with("bylex") => withscores = true,
            b"LIMIT" if i + 2 < a.len() => {
                limit = Some((integer(a[i + 1])?, integer(a[i + 2])?));
                i += 2;
            }
            _ => return Err(SYNTAX),
        }
        i += 1;
    }
    if !byscore && !bylex && limit.is_some() {
        return Err(SYNTAX);
    }
    let ordered: Vec<_> = if reverse {
        z.order.iter().rev().cloned().collect()
    } else {
        z.order.iter().cloned().collect()
    };
    let selected: Vec<_> = if bylex {
        let (lo, hi) = if reverse {
            (lex_bound(a[3])?, lex_bound(a[2])?)
        } else {
            (lex_bound(a[2])?, lex_bound(a[3])?)
        };
        let (offset, count) = limit.unwrap_or((0, -1));
        if offset < 0 {
            return Ok((Vec::new(), withscores));
        }
        ordered
            .into_iter()
            .filter(|(_, member)| lex_contains(member, &lo, &hi))
            .skip(offset as usize)
            .take(if count < 0 {
                usize::MAX
            } else {
                count as usize
            })
            .collect()
    } else if byscore {
        let (lo, hi) = if reverse {
            (bound(a[3])?, bound(a[2])?)
        } else {
            (bound(a[2])?, bound(a[3])?)
        };
        let (offset, count) = limit.unwrap_or((0, -1));
        if offset < 0 {
            return Ok((Vec::new(), withscores));
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
    Ok((selected, withscores))
}

pub(super) fn validate(cmd: &str, a: &[&[u8]]) -> Result<()> {
    if matches!(cmd, "zrank" | "zrevrank") {
        if a.len() > 4 {
            return Err(Reply::Error("ERR wrong number of arguments"));
        }
        if a.len() == 4 && !a[3].eq_ignore_ascii_case(b"WITHSCORE") {
            return Err(SYNTAX);
        }
    }
    Ok(())
}

enum LexBound<'a> {
    Negative,
    Positive,
    Value(&'a [u8], bool),
}
fn lex_bound(value: &[u8]) -> Result<LexBound<'_>> {
    match value {
        b"-" => Ok(LexBound::Negative),
        b"+" => Ok(LexBound::Positive),
        [b'[', rest @ ..] => Ok(LexBound::Value(rest, true)),
        [b'(', rest @ ..] => Ok(LexBound::Value(rest, false)),
        _ => Err(Reply::Error("ERR min or max not valid string range item")),
    }
}
fn lex_contains(member: &[u8], lo: &LexBound<'_>, hi: &LexBound<'_>) -> bool {
    let lower = match lo {
        LexBound::Negative => true,
        LexBound::Positive => false,
        LexBound::Value(value, inclusive) => member > *value || (*inclusive && member == *value),
    };
    let upper = match hi {
        LexBound::Positive => true,
        LexBound::Negative => false,
        LexBound::Value(value, inclusive) => member < *value || (*inclusive && member == *value),
    };
    lower && upper
}
pub(super) fn store(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Reply {
    match store_inner(engine, a, now, submit) {
        Ok(reply) | Err(reply) => reply,
    }
}
fn store_inner(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply> {
    let empty = SortedSet::default();
    let source = match engine.get(a[2], now) {
        None => &empty,
        Some(entry) => match entry.value.collection() {
            Some(Collection::ZSet(set)) => set,
            _ => return Err(WRONGTYPE),
        },
    };
    // Range parsing uses the same positions as ZRANGE after removing destination.
    let mut args = vec![a[0]];
    args.extend_from_slice(&a[2..]);
    let (selected, _) = select_range("zrangestore", &args, source)?;
    let count = selected.len();
    let entry = if selected.is_empty() {
        None
    } else {
        let mut set = SortedSet::default();
        for (score, member) in selected {
            set.insert(member, score.0);
        }
        Some(Entry {
            value: Value::from_collection(Collection::ZSet(set))
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

#[cfg(test)]
mod rank_tests {
    use super::*;
    #[test]
    fn lex_removal_is_atomic_and_preserves_ttl() {
        let mut engine = Engine::new(65536);
        engine.execute(
            &[b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c"],
            0,
            |_| true,
        );
        engine.execute(&[b"PEXPIREAT", b"z", b"5000"], 0, |_| true);
        let before = engine.snapshot(b"z");
        let args: &[&[u8]] = &[b"ZREMRANGEBYLEX", b"z", b"(a", b"[c"];
        assert!(matches!(
            engine.execute(args, 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"z"), before);
        assert_eq!(engine.execute(args, 1, |_| true), Reply::Integer(2));
        assert_eq!(engine.snapshot(b"z").unwrap().expires_at_ms, Some(5000));
        assert_eq!(
            engine.execute(&[b"ZREMRANGEBYLEX", b"z", b"-", b"+"], 1, |_| true),
            Reply::Integer(1)
        );
        assert!(engine.snapshot(b"z").is_none());
    }
    #[test]
    fn range_store_is_atomic_and_clears_destination_ttl() {
        let mut engine = Engine::new(65536);
        engine.execute(
            &[b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c"],
            0,
            |_| true,
        );
        engine.execute(&[b"PEXPIREAT", b"z", b"5000"], 0, |_| true);
        let before = engine.snapshot(b"z");
        let args: &[&[u8]] = &[b"ZRANGESTORE", b"z", b"z", b"[a", b"(c", b"BYLEX"];
        assert!(matches!(
            engine.execute(args, 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"z"), before);
        assert_eq!(engine.execute(args, 1, |_| true), Reply::Integer(2));
        assert_eq!(engine.snapshot(b"z").unwrap().expires_at_ms, None);
        assert_eq!(
            engine.execute(&[b"ZRANGE", b"z", b"0", b"-1"], 1, |_| true),
            Reply::Array(vec![bulk(b"a"), bulk(b"b")])
        );
    }
    #[test]
    fn rank_with_score_uses_null_array_on_wire() {
        let mut engine = Engine::new(65536);
        let reply = engine.execute(&[b"ZRANK", b"missing", b"member", b"WITHSCORE"], 0, |_| {
            true
        });
        let mut encoded = Vec::new();
        reply.encode(&mut encoded);
        assert_eq!(encoded, b"*-1\r\n");
        engine.execute(&[b"ZADD", b"z", b"1.5", b"b", b"1.5", b"a"], 0, |_| true);
        assert_eq!(
            engine.execute(&[b"ZREVRANK", b"z", b"a", b"WITHSCORE"], 0, |_| true),
            Reply::Array(vec![Reply::Integer(1), bulk(b"1.5")])
        );
    }
}
