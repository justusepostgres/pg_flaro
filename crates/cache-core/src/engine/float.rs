//! Redis floating increments use native long double, not Rust binary64.
use super::{eq, Engine, Entry, Mutations, ARITY};
use crate::{
    collections::WRONGTYPE,
    protocol::Reply,
    value::{Collection, Value},
};
use std::sync::OnceLock;

unsafe extern "C" {
    fn pgcache_ld_validate(bytes: *const u8, len: usize, locale: libc::locale_t) -> libc::c_int;
    fn pgcache_ld_add(
        old: *const u8,
        old_len: usize,
        incr: *const u8,
        incr_len: usize,
        locale: libc::locale_t,
        out: *mut u8,
        capacity: usize,
    ) -> libc::c_int;
}
static LOCALE: OnceLock<usize> = OnceLock::new();
const INVALID: Reply = Reply::Error("ERR value is not a valid float");

pub(super) fn execute(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Option<Reply> {
    let hash = eq(a[0], b"HINCRBYFLOAT");
    if !hash && !eq(a[0], b"INCRBYFLOAT") {
        return None;
    }
    if a.len() != if hash { 4 } else { 3 } {
        return Some(ARITY);
    }
    Some(run(engine, a, now, hash, submit).unwrap_or_else(|error| error))
}
fn run(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    hash: bool,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Result<Reply, Reply> {
    // SAFETY: this immutable C locale lives for the process lifetime. The shim
    // reads only the supplied byte lengths and writes at most the output capacity.
    let locale = unsafe {
        *LOCALE.get_or_init(|| {
            libc::newlocale(libc::LC_NUMERIC_MASK, c"C".as_ptr(), std::ptr::null_mut()) as usize
        })
    } as libc::locale_t;
    if locale.is_null() {
        return Err(Reply::Error("ERR could not initialize numeric locale"));
    }
    let increment = a[if hash { 3 } else { 2 }];
    if hash {
        // HINCRBYFLOAT validates its increment before looking up the hash.
        match unsafe { pgcache_ld_validate(increment.as_ptr(), increment.len(), locale) } {
            0 => {}
            2 => return Err(Reply::Error("ERR value is NaN or Infinity")),
            _ => return Err(INVALID),
        }
    }
    let old = engine.get(a[1], now);
    if old.is_some_and(|entry| entry.value.kind() != if hash { 1 } else { 0 }) {
        return Err(WRONGTYPE);
    }
    let fields = match old.and_then(|entry| entry.value.collection()) {
        Some(Collection::Hash(fields)) => Some(fields),
        _ => None,
    };
    let previous: &[u8] = if hash {
        fields
            .and_then(|h| h.get(a[2]))
            .map_or(b"0", |v| v.as_ref())
    } else {
        old.map_or(b"0", |entry| entry.value.as_ref())
    };
    let mut value = vec![0u8; 5120];
    let length = unsafe {
        pgcache_ld_add(
            previous.as_ptr(),
            previous.len(),
            increment.as_ptr(),
            increment.len(),
            locale,
            value.as_mut_ptr(),
            value.len(),
        )
    };
    match length {
        -1 if hash => return Err(Reply::Error("ERR hash value is not a float")),
        -1 | -2 => return Err(INVALID),
        -3 => return Err(Reply::Error("ERR increment would produce NaN or Infinity")),
        n if n <= 0 => return Err(Reply::Error("ERR floating point formatting failed")),
        _ => {}
    }
    value.truncate(length as usize);
    let reply = Reply::Bulk(Some(value.clone()));
    let expiry = old.and_then(|entry| entry.expires_at_ms);
    if !hash {
        return Ok(match engine.put(a[1], &value, expiry, submit) {
            Reply::Simple("OK") => reply,
            error => error,
        });
    }
    let mut fields = fields.cloned().unwrap_or_default();
    fields.insert_preserving_expiry(a[2].into(), value.into());
    let value = Value::from_collection(Collection::Hash(fields))
        .map_err(|_| Reply::Error("ERR collection exceeds maximum size"))?;
    Ok(engine.apply_typed(
        vec![(
            a[1].to_vec(),
            Some(Entry {
                value,
                expires_at_ms: expiry,
            }),
        )],
        reply,
        submit,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn float_writes_preserve_ttl_and_admit_atomically() {
        let mut engine = Engine::new(65536);
        engine.execute(&[b"SET", b"s", b"1.25", b"PX", b"100"], 0, |_| true);
        engine.execute(&[b"HSET", b"h", b"f", b"1.25"], 0, |_| true);
        engine.execute(&[b"PEXPIREAT", b"h", b"100"], 0, |_| true);
        for (args, key) in [
            (
                vec![b"INCRBYFLOAT".as_slice(), b"s", b"0.25"],
                b"s".as_slice(),
            ),
            (vec![b"HINCRBYFLOAT", b"h", b"f", b"0.25"], b"h"),
        ] {
            let original = engine.snapshot(key);
            assert!(matches!(
                engine.execute(&args, 1, |_| false),
                Reply::Error(_)
            ));
            assert_eq!(engine.snapshot(key), original);
            assert_eq!(
                engine.execute(&args, 1, |_| true),
                Reply::Bulk(Some(b"1.5".to_vec()))
            );
            assert_eq!(engine.get(key, 1).unwrap().expires_at_ms, Some(100));
        }
        assert!(matches!(
            engine.execute(&[b"HINCRBYFLOAT", b"absent", b"f", b"inf"], 1, |_| panic!(
                "invalid"
            )),
            Reply::Error(_)
        ));
        assert!(engine.snapshot(b"absent").is_none());
        assert_eq!(
            engine.execute(&[b"INCRBYFLOAT", b"zero", b"-0"], 1, |_| true),
            Reply::Bulk(Some(b"0".to_vec()))
        );
    }
}
