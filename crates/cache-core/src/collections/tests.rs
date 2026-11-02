use super::*;
fn run(e: &mut Engine, args: &[&str]) -> Reply {
    e.execute(
        &args.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
        1000,
        |_| true,
    )
}
#[test]
fn types_ttls_empty_keys_and_wrongtype() {
    for (kind, create, modify, remove) in [
        (
            "hash",
            vec!["HSET", "k", "field", "1"],
            vec!["HINCRBY", "k", "field", "1"],
            vec!["HDEL", "k", "field"],
        ),
        (
            "list",
            vec!["RPUSH", "k", "a"],
            vec!["LSET", "k", "0", "b"],
            vec!["LPOP", "k"],
        ),
        (
            "set",
            vec!["SADD", "k", "a"],
            vec!["SADD", "k", "a"],
            vec!["SREM", "k", "a"],
        ),
        (
            "zset",
            vec!["ZADD", "k", "1", "a"],
            vec!["ZINCRBY", "k", "1", "a"],
            vec!["ZREM", "k", "a"],
        ),
    ] {
        let mut e = Engine::new(1 << 20);
        run(&mut e, &create);
        assert_eq!(run(&mut e, &["TYPE", "k"]), Reply::Simple(kind));
        assert_eq!(run(&mut e, &["PEXPIRE", "k", "500"]), Reply::Integer(1));
        run(&mut e, &modify);
        assert_eq!(run(&mut e, &["PTTL", "k"]), Reply::Integer(500));
        assert_eq!(run(&mut e, &["GET", "k"]), WRONGTYPE);
        let mut encoded = Vec::new();
        e.get_into(b"k", 1000, &mut encoded);
        assert!(encoded.starts_with(b"-WRONGTYPE"));
        assert_eq!(
            run(&mut e, &["MGET", "k", "missing"]),
            Reply::Array(vec![Reply::Bulk(None), Reply::Bulk(None)])
        );
        run(&mut e, &remove);
        assert_eq!(run(&mut e, &["TYPE", "k"]), Reply::Simple("none"));
        run(&mut e, &create);
        run(&mut e, &["SET", "k", "plain"]);
        assert_eq!(run(&mut e, &["TYPE", "k"]), Reply::Simple("string"));
        assert_eq!(run(&mut e, &modify), WRONGTYPE);
    }
}
#[test]
fn immutable_snapshots_and_rejected_admission_are_atomic() {
    let mut e = Engine::new(1 << 20);
    run(&mut e, &["HSET", "k", "f", "old"]);
    let old = e.get(b"k", 1000).unwrap().clone();
    assert_eq!(
        e.execute(&[b"HSET", b"k", b"f", b"new"], 1000, |_| false),
        Reply::Error("MISCONF persistence backlog full or unavailable")
    );
    assert_eq!(run(&mut e, &["HGET", "k", "f"]), bulk("old"));
    run(&mut e, &["HSET", "k", "f", "new"]);
    assert_ne!(old.value, e.get(b"k", 1000).unwrap().value);
    assert_eq!(
        Value::from_stored(old.value.kind(), old.value.as_ref()).unwrap(),
        old.value
    );
    run(&mut e, &["ZADD", "z", "1", "a"]);
    assert!(matches!(
        run(&mut e, &["ZADD", "z", "2", "a", "nan", "b"]),
        Reply::Error(_)
    ));
    assert_eq!(run(&mut e, &["ZSCORE", "z", "a"]), bulk("1"));
    let mut tiny = Engine::new(200);
    assert!(matches!(run(&mut tiny,&["HSET","k","f","v"]),Reply::Error(s) if s.starts_with("OOM")));
    assert_eq!(run(&mut tiny, &["TYPE", "k"]), Reply::Simple("none"));
}
#[test]
fn collection_codec_and_journal_preserve_types_and_binary_payloads() {
    let mut e = Engine::new(1 << 20);
    for args in [
        vec![b"HSET".as_slice(), b"h", b"\0\xff", b""],
        vec![b"RPUSH", b"l", b"a", b"\0"],
        vec![b"SADD", b"s", b"", b"\xff"],
        vec![b"ZADD", b"z", b"-inf", b"a", b"inf", b"b"],
    ] {
        e.execute(&args, 1000, |_| true);
    }
    let mutations: Vec<_> = [b"h", b"l", b"s", b"z"]
        .iter()
        .map(|key| crate::engine::Mutation {
            key: key.as_slice().into(),
            entry: e.get(key.as_slice(), 1000).cloned(),
        })
        .collect();
    let encoded = crate::journal::encode(&mutations).unwrap();
    let restored = crate::journal::decode(&encoded).unwrap();
    for (a, b) in mutations.iter().zip(restored) {
        assert_eq!(a.entry, b.entry);
        assert_eq!(a.bytes(), b.bytes());
    }
    let raw = b"COL1\0binary";
    let string = Value::from_stored(0, raw).unwrap();
    assert_eq!(string.as_ref(), raw);
    assert_eq!(string.kind(), 0);
    for kind in 1..=4 {
        assert!(Value::from_stored(kind, b"COL1\xff\xff\xff\xff").is_err());
    }
}
#[test]
fn ranges_ties_set_algebra_and_duplicate_counts() {
    let mut e = Engine::new(1 << 20);
    assert_eq!(
        run(&mut e, &["HSET", "h", "f", "1", "f", "2"]),
        Reply::Integer(1)
    );
    run(&mut e, &["LPUSH", "l", "a", "b", "c"]);
    assert_eq!(
        run(&mut e, &["LRANGE", "l", "-2", "-1"]),
        Reply::Array(vec![bulk("b"), bulk("a")])
    );
    run(&mut e, &["LTRIM", "l", "99", "100"]);
    assert_eq!(run(&mut e, &["TYPE", "l"]), Reply::Simple("none"));
    run(&mut e, &["SADD", "a", "x", "y"]);
    run(&mut e, &["SADD", "b", "y", "z"]);
    assert_eq!(
        run(&mut e, &["SINTERSTORE", "a", "a", "b"]),
        Reply::Integer(1)
    );
    assert_eq!(
        run(&mut e, &["SMEMBERS", "a"]),
        Reply::Array(vec![bulk("y")])
    );
    run(&mut e, &["ZADD", "z", "2", "b", "1", "x", "2", "a"]);
    assert_eq!(
        run(&mut e, &["ZRANGE", "z", "0", "-1"]),
        Reply::Array(vec![bulk("x"), bulk("a"), bulk("b")])
    );
    assert_eq!(
        run(
            &mut e,
            &[
                "ZRANGE",
                "z",
                "(1",
                "+inf",
                "BYSCORE",
                "LIMIT",
                "0",
                "1",
                "WITHSCORES"
            ]
        ),
        Reply::Array(vec![bulk("a"), bulk("2")])
    );
    assert_eq!(
        run(&mut e, &["ZADD", "z", "NX", "CH", "3", "a"]),
        Reply::Integer(0)
    );
    assert_eq!(
        run(&mut e, &["ZADD", "z", "XX", "GT", "INCR", "1", "a"]),
        bulk("3")
    );
}

#[test]
fn incremental_sizes_match_codec_after_mutations() {
    let mut e = Engine::new(64 << 20);
    let commands = [
        vec!["HSET", "h", "a", "short", "b", "longer"],
        vec!["HSET", "h", "a", "", "c", "new"],
        vec!["HDEL", "h", "b", "missing"],
        vec!["RPUSH", "l", "a", "bb", "ccc"],
        vec!["LSET", "l", "-1", "longer value"],
        vec!["LSET", "l", "0", ""],
        vec!["LTRIM", "l", "1", "-1"],
        vec!["SADD", "s", "a", "bb", "ccc"],
        vec!["SREM", "s", "bb", "missing"],
        vec!["SADD", "s", "a", "dddd"],
        vec!["ZADD", "z", "1", "a", "2", "bb"],
        vec!["ZINCRBY", "z", "5", "a"],
        vec!["ZADD", "z", "3", "ccc"],
        vec!["ZREM", "z", "bb"],
    ];
    for command in commands {
        assert!(!matches!(run(&mut e, &command), Reply::Error(_)));
        let value = &e.get(command[1].as_bytes(), 1000).unwrap().value;
        assert_eq!(value.len(), value.as_ref().len(), "{command:?}");
        let restored = Value::from_stored(value.kind(), value.as_ref()).unwrap();
        assert_eq!(value.memory_bytes(), restored.memory_bytes());
    }
    // Exercise shared internal tree nodes, rather than only tiny inline trees.
    for i in 0..2000 {
        let member = format!("field-{i}");
        run(&mut e, &["HSET", "h", &member, &member]);
        run(&mut e, &["SADD", "s", &member]);
        run(&mut e, &["ZADD", "z", "1", &member]);
    }
    for i in (0..2000).step_by(7) {
        let member = format!("field-{i}");
        run(&mut e, &["HSET", "h", &member, "replacement"]);
        run(&mut e, &["SREM", "s", &member]);
        run(&mut e, &["ZREM", "z", &member]);
        for key in [b"h", b"s", b"z"] {
            let value = &e.get(key, 1000).unwrap().value;
            assert_eq!(value.len(), value.as_ref().len());
            let restored = Value::from_stored(value.kind(), value.as_ref()).unwrap();
            assert_eq!(value.memory_bytes(), restored.memory_bytes());
        }
    }
}
