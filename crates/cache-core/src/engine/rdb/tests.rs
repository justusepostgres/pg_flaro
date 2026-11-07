use super::*;
fn payload(mut body: Vec<u8>) -> Vec<u8> {
    body.extend_from_slice(&12u16.to_le_bytes());
    body.extend_from_slice(&checksum(&body).to_le_bytes());
    body
}
#[test]
fn malformed_objects_are_bounded_and_do_not_panic() {
    let valid = dump(&Value::from(b"binary\0\xff".as_slice()));
    for end in 0..valid.len() - 10 {
        assert!(decode::decode(&payload(valid[..end].to_vec())).is_err());
    }
    let mut seed = 811u64;
    for size in 0..256 {
        for _ in 0..16 {
            let body: Vec<u8> = (0..size)
                .map(|_| {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    (seed >> 32) as u8
                })
                .collect();
            let _ = decode::decode(&payload(body));
        }
    }
    // A tiny compressed string must not allocate an attacker-selected huge buffer.
    assert!(decode::decode(&payload(vec![0, 0xc3, 1, 0x80, 0x7f, 0xff, 0xff, 0xff, 0])).is_err());
}
#[test]
fn all_exported_types_roundtrip_and_truncated_bodies_fail() {
    let mut engine = Engine::new(1 << 20);
    for command in [
        vec![b"SET".as_slice(), b"s", b"\0\xff"],
        vec![b"HSET", b"h", b"a", b"1", b"b", b"2"],
        vec![b"HPEXPIREAT", b"h", b"4102444800000", b"FIELDS", b"1", b"a"],
        vec![b"RPUSH", b"l", b"a", b"a", b"b"],
        vec![b"SADD", b"set", b"a", b"b"],
        vec![b"ZADD", b"z", b"-inf", b"a", b"1.25", b"b", b"+inf", b"c"],
    ] {
        engine.execute(&command, 0, |_| true);
    }
    for key in [b"s".as_slice(), b"h", b"l", b"set", b"z"] {
        let original = &engine.get(key, 0).unwrap().value;
        let bytes = dump(original);
        assert_eq!(decode::decode(&bytes).unwrap(), *original);
        for end in 0..bytes.len() - 10 {
            assert!(decode::decode(&payload(bytes[..end].to_vec())).is_err());
        }
    }
}
#[test]
fn restore_rejection_and_transactions_preserve_value_expiry_and_watches() {
    let mut engine = Engine::new(1 << 20);
    engine.execute(&[b"SET", b"k", b"old", b"PX", b"100"], 0, |_| true);
    let old = engine.snapshot(b"k");
    let watch = engine.watch(b"k", 0);
    let bytes = dump(&Value::from(b"new".as_slice()));
    let command: &[&[u8]] = &[b"RESTORE", b"k", b"200", &bytes, b"REPLACE"];
    assert!(matches!(
        engine.execute(command, 1, |_| false),
        Reply::Error(_)
    ));
    assert_eq!(engine.snapshot(b"k"), old);
    assert!(!engine.watch_dirty(b"k", &watch, 1));
    assert!(engine
        .transact(
            |engine, submit| vec![engine.execute(command, 1, submit)],
            |_| false
        )
        .is_err());
    assert_eq!(engine.snapshot(b"k"), old);
    assert!(!engine.watch_dirty(b"k", &watch, 1));
    assert_eq!(engine.execute(command, 1, |_| true), Reply::Simple("OK"));
    assert_eq!(engine.get(b"k", 1).unwrap().expires_at_ms, Some(201));
    assert!(engine.watch_dirty(b"k", &watch, 1));
    let watch = engine.watch(b"k", 1);
    let past: &[&[u8]] = &[b"RESTORE", b"k", b"1", &bytes, b"REPLACE", b"ABSTTL"];
    assert!(matches!(
        engine.execute(past, 2, |_| false),
        Reply::Error(_)
    ));
    assert!(!engine.watch_dirty(b"k", &watch, 2));
    assert_eq!(engine.execute(past, 2, |_| true), Reply::Simple("OK"));
    assert!(engine.watch_dirty(b"k", &watch, 2));
    assert!(engine.get(b"k", 2).is_none());
}
