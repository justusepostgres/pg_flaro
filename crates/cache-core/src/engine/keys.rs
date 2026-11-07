//! Generic key operations preserve typed values and absolute expiration deadlines.
use super::{eq, integer, Engine, Mutations, ARITY, SYNTAX};
use crate::protocol::Reply;

pub(super) fn execute(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    if eq(args[0], b"UNLINK") {
        return Some(super::lazyfree::unlink(engine, args, now, submit));
    }
    if eq(args[0], b"RANDOMKEY") {
        return Some(if args.len() == 1 {
            // Reservoir sampling is uniform over live keys, with O(1) storage.
            // An indexed key table is still needed for Redis's O(1) draw cost.
            let mut random = crate::random::Random::new();
            let mut selected = None;
            let mut count = 0;
            for key in engine.entries.keys() {
                if engine.get(key, now).is_none() {
                    continue;
                }
                count += 1;
                if random.below(count) == 0 {
                    selected = Some(key);
                }
            }
            Reply::Bulk(selected.map(|key| key.to_vec()))
        } else {
            Reply::Error("ERR wrong number of arguments for 'randomkey' command")
        });
    }
    if eq(args[0], b"KEYS") {
        return Some(if args.len() == 2 {
            keys(engine, args[1], now)
        } else {
            ARITY
        });
    }
    if eq(args[0], b"DBSIZE") {
        // Redis counts resident entries, including expirations not yet reaped.
        return Some(if args.len() == 1 {
            Reply::Integer(engine.entries.len() as i64)
        } else {
            ARITY
        });
    }
    if eq(args[0], b"TOUCH") {
        return Some(if args.len() >= 2 {
            Reply::Integer(
                args[1..]
                    .iter()
                    .filter(|key| engine.get(key, now).is_some())
                    .count() as i64,
            )
        } else {
            ARITY
        });
    }
    if eq(args[0], b"COPY") {
        return Some(copy(engine, args, now, submit));
    }
    let nx = eq(args[0], b"RENAMENX");
    if !nx && !eq(args[0], b"RENAME") {
        return None;
    }
    if args.len() != 3 {
        return Some(ARITY);
    }
    let Some(source) = engine.get(args[1], now) else {
        return Some(Reply::Error("ERR no such key"));
    };
    if args[1] == args[2] {
        return Some(if nx {
            Reply::Integer(0)
        } else {
            Reply::Simple("OK")
        });
    }
    if nx && engine.get(args[2], now).is_some() {
        return Some(Reply::Integer(0));
    }
    let source = source.clone();
    Some(engine.apply_typed(
        vec![(args[1].to_vec(), None), (args[2].to_vec(), Some(source))],
        if nx {
            Reply::Integer(1)
        } else {
            Reply::Simple("OK")
        },
        submit,
    ))
}

fn keys(engine: &Engine, pattern: &[u8], now: i64) -> Reply {
    let pattern = (pattern != b"*").then(|| super::scan::glob::Pattern::new(pattern));
    let mut result = Vec::new();
    let mut bytes = 32;
    for (key, entry) in &engine.entries {
        if entry.expires_at_ms.is_some_and(|deadline| deadline <= now)
            || pattern.as_ref().is_some_and(|p| !p.matches(key))
        {
            continue;
        }
        bytes += key.len() + 32;
        if bytes > 2 * crate::protocol::MAX_FRAME - 32 {
            return Reply::Error("ERR response exceeds maximum size; use SCAN");
        }
        result.push(Reply::Bulk(Some(key.to_vec())));
    }
    Reply::Array(result)
}

fn copy(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Reply {
    if args.len() < 3 {
        return ARITY;
    }
    let mut replace = false;
    let mut i = 3;
    while i < args.len() {
        if eq(args[i], b"REPLACE") {
            replace = true;
        } else if eq(args[i], b"DB") && i + 1 < args.len() {
            i += 1;
            match integer(args[i]) {
                Some(0) => {}
                Some(_) => return Reply::Error("ERR only database 0 is supported"),
                None => return Reply::Error("ERR value is not an integer or out of range"),
            }
        } else {
            return SYNTAX;
        }
        i += 1;
    }
    if args[1] == args[2] {
        return Reply::Error("ERR source and destination objects are the same");
    }
    let Some(source) = engine.get(args[1], now) else {
        return Reply::Integer(0);
    };
    if !replace && engine.get(args[2], now).is_some() {
        return Reply::Integer(0);
    }
    // Immutable value sharing is safe: collection writes use copy-on-write.
    let source = source.clone();
    engine.apply_typed(
        vec![(args[2].to_vec(), Some(source))],
        Reply::Integer(1),
        submit,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn enumeration_and_touch_are_read_only_and_respect_expiration() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SET", b"a\0x", b"v", b"PX", b"100"], 0, |_| true);
        engine.execute(&[b"HSET", b"hash", b"f", b"v"], 0, |_| true);
        let snapshot = engine.snapshot(b"a\0x");
        assert_eq!(
            engine.execute(
                &[b"TOUCH", b"a\0x", b"a\0x", b"missing", b"hash"],
                1,
                |_| panic!("read only")
            ),
            Reply::Integer(3)
        );
        assert_eq!(engine.snapshot(b"a\0x"), snapshot);
        assert_eq!(
            engine.execute(&[b"KEYS", b"a?x"], 1, |_| panic!("read only")),
            Reply::Array(vec![Reply::Bulk(Some(b"a\0x".to_vec()))])
        );
        assert_eq!(
            engine.execute(&[b"KEYS", b"a*"], 100, |_| panic!("read only")),
            Reply::Array(vec![])
        );
        assert_eq!(
            engine.execute(&[b"TOUCH", b"a\0x"], 100, |_| panic!("read only")),
            Reply::Integer(0)
        );
        assert_eq!(
            engine.execute(&[b"DBSIZE"], 100, |_| panic!("read only")),
            Reply::Integer(2)
        );
        engine.reap(100);
        assert_eq!(
            engine.execute(&[b"DBSIZE"], 100, |_| panic!("read only")),
            Reply::Integer(1)
        );
    }

    #[test]
    fn keys_bounds_reply_without_changing_entries() {
        let mut engine = Engine::new(4_000_000);
        for n in 0..8 {
            let mut key = vec![b'x'; 300_000];
            key[0] = n;
            assert_eq!(
                engine.execute(&[b"SET", &key, b"v"], 0, |_| true),
                Reply::Simple("OK")
            );
        }
        assert!(matches!(
            engine.execute(&[b"KEYS", b"*"], 0, |_| panic!("read only")),
            Reply::Error(_)
        ));
        assert_eq!(
            engine.execute(&[b"DBSIZE"], 0, |_| panic!("read only")),
            Reply::Integer(8)
        );
    }

    #[test]
    fn rename_is_atomic_and_preserves_expiration_and_type() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"HSET", b"source", b"f", b"v"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"source", b"5000"], 0, |_| true);
        engine.execute(&[b"SET", b"target", b"old"], 0, |_| true);
        let source = engine.snapshot(b"source");
        let target = engine.snapshot(b"target");
        let used = engine.used;
        assert!(matches!(
            engine.execute(&[b"RENAME", b"source", b"target"], 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"source"), source);
        assert_eq!(engine.snapshot(b"target"), target);
        assert_eq!(engine.used, used);
        assert_eq!(
            engine.execute(&[b"RENAMENX", b"source", b"target"], 1, |_| panic!(
                "no mutation"
            )),
            Reply::Integer(0)
        );
        assert_eq!(
            engine.execute(&[b"RENAME", b"source", b"source"], 1, |_| panic!(
                "no mutation"
            )),
            Reply::Simple("OK")
        );
        assert_eq!(
            engine.execute(&[b"RENAME", b"source", b"target"], 1, |changes| {
                assert_eq!(changes.len(), 2);
                assert!(changes[0].entry.is_none());
                assert_eq!(changes[1].entry, source);
                true
            }),
            Reply::Simple("OK")
        );
        assert!(engine.snapshot(b"source").is_none());
        assert_eq!(engine.snapshot(b"target"), source);
        assert_eq!(
            engine.execute(&[b"RENAME", b"target", b"missing"], 5000, |_| panic!(
                "expired"
            )),
            Reply::Error("ERR no such key")
        );
    }

    #[test]
    fn larger_destination_key_can_reject_without_losing_source() {
        let mut engine = Engine::new(140);
        assert_eq!(
            engine.execute(&[b"SET", b"a", b"v"], 0, |_| true),
            Reply::Simple("OK")
        );
        let before = engine.snapshot(b"a");
        assert!(matches!(
            engine.execute(&[b"RENAME", b"a", b"long-destination-key"], 0, |_| panic!(
                "over memory limit"
            )),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"a"), before);
        assert!(engine.snapshot(b"long-destination-key").is_none());
    }
}
