//! Immediate attempt shared by blocking sockets and nonblocking EXEC execution.
use super::{Engine, Mutations};
use crate::{collections::WRONGTYPE, protocol::Reply};
use std::sync::OnceLock;
unsafe extern "C" {
    fn pgcache_ld_timeout(
        bytes: *const u8,
        len: usize,
        locale: libc::locale_t,
        now: i64,
        deadline: *mut i64,
    ) -> libc::c_int;
}
static LOCALE: OnceLock<usize> = OnceLock::new();
pub(crate) fn supported(name: &[u8]) -> bool {
    [
        b"BLPOP".as_slice(),
        b"BRPOP",
        b"BLMOVE",
        b"BRPOPLPUSH",
        b"BLMPOP",
        b"BZPOPMIN",
        b"BZPOPMAX",
        b"BZMPOP",
    ]
    .iter()
    .any(|command| name.eq_ignore_ascii_case(command))
}
pub(crate) fn kind(name: &[u8]) -> u8 {
    if name.get(1).is_some_and(|c| c.eq_ignore_ascii_case(&b'Z')) {
        4
    } else {
        2
    }
}
fn is_move(name: &[u8]) -> bool {
    name.eq_ignore_ascii_case(b"BLMOVE") || name.eq_ignore_ascii_case(b"BRPOPLPUSH")
}
fn is_mpop(name: &[u8]) -> bool {
    name.eq_ignore_ascii_case(b"BLMPOP") || name.eq_ignore_ascii_case(b"BZMPOP")
}
pub(crate) fn null(name: &[u8]) -> Reply {
    if is_move(name) {
        Reply::Bulk(None)
    } else {
        Reply::NullArray
    }
}
/// Only the source of a move blocks; the destination still participates in routing.
pub(crate) fn waiting_keys<'a>(a: &[&'a [u8]]) -> Vec<&'a [u8]> {
    if is_move(a[0]) {
        vec![a[1]]
    } else if is_mpop(a[0]) {
        let count: usize = std::str::from_utf8(a[2]).unwrap().parse().unwrap();
        a[3..3 + count].to_vec()
    } else {
        a[1..a.len() - 1].to_vec()
    }
}
fn immediate<'a>(a: &[&'a [u8]]) -> Vec<&'a [u8]> {
    if is_mpop(a[0]) {
        let mut args = vec![if kind(a[0]) == 4 {
            b"ZMPOP".as_slice()
        } else {
            b"LMPOP"
        }];
        args.extend_from_slice(&a[2..]);
        args
    } else {
        let mut args = a[..a.len() - 1].to_vec();
        args[0] = if a[0].eq_ignore_ascii_case(b"BLMOVE") {
            b"LMOVE"
        } else {
            b"RPOPLPUSH"
        };
        args
    }
}
pub(crate) fn deadline(a: &[&[u8]], now: i64) -> Result<i64, Reply> {
    crate::cluster::validate_command(a)?;
    if a[0].eq_ignore_ascii_case(b"BLMOVE")
        && a[3..5]
            .iter()
            .any(|v| !v.eq_ignore_ascii_case(b"LEFT") && !v.eq_ignore_ascii_case(b"RIGHT"))
    {
        return Err(Reply::Error("ERR syntax error"));
    }
    if is_mpop(a[0]) {
        crate::collections::mpop::options(&immediate(a))?;
    }
    // SAFETY: immutable process-lifetime C locale, length-delimited input, and
    // one valid output pointer. The C helper bounds parsing and signed casts.
    let locale = unsafe {
        *LOCALE.get_or_init(|| {
            libc::newlocale(libc::LC_NUMERIC_MASK, c"C".as_ptr(), std::ptr::null_mut()) as usize
        })
    } as libc::locale_t;
    if locale.is_null() {
        return Err(Reply::Error("ERR could not initialize numeric locale"));
    }
    let timeout = if is_mpop(a[0]) { a[1] } else { a[a.len() - 1] };
    let mut deadline = 0;
    match unsafe { pgcache_ld_timeout(timeout.as_ptr(), timeout.len(), locale, now, &mut deadline) }
    {
        0 => Ok(deadline),
        2 => Err(Reply::Error("ERR timeout is out of range")),
        3 => Err(Reply::Error("ERR timeout is negative")),
        _ => Err(Reply::Error("ERR timeout is not a float or out of range")),
    }
}
pub(super) fn execute(
    engine: &mut Engine,
    a: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Reply {
    if let Err(error) = deadline(a, now) {
        return error;
    }
    if is_move(a[0]) || is_mpop(a[0]) {
        return crate::collections::execute(engine, &immediate(a), now, submit)
            .expect("known blocking counterpart");
    }
    for key in &a[1..a.len() - 1] {
        let Some(entry) = engine.get(key, now) else {
            continue;
        };
        if entry.value.kind() != kind(a[0]) {
            return WRONGTYPE;
        }
        let command: &[u8] = if a[0].eq_ignore_ascii_case(b"BLPOP") {
            b"LPOP"
        } else if a[0].eq_ignore_ascii_case(b"BRPOP") {
            b"RPOP"
        } else if a[0].eq_ignore_ascii_case(b"BZPOPMIN") {
            b"ZPOPMIN"
        } else {
            b"ZPOPMAX"
        };
        let reply = crate::collections::execute(engine, &[command, key], now, submit)
            .expect("known list pop");
        return match reply {
            Reply::Bulk(Some(value)) => Reply::Array(vec![
                Reply::Bulk(Some(key.to_vec())),
                Reply::Bulk(Some(value)),
            ]),
            Reply::Bulk(None) => Reply::NullArray,
            Reply::Array(mut values) if kind(a[0]) == 4 => {
                values.insert(0, Reply::Bulk(Some(key.to_vec())));
                Reply::Array(values)
            }
            other => other,
        };
    }
    Reply::NullArray
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn blocking_family_rejection_preserves_source_destination_and_ttl() {
        let cases: &[&[&[u8]]] = &[
            &[b"BLMOVE", b"src", b"dst", b"RIGHT", b"LEFT", b"0"],
            &[b"BRPOPLPUSH", b"src", b"dst", b"0"],
            &[b"BLMPOP", b"0", b"1", b"src", b"LEFT", b"COUNT", b"2"],
            &[b"BZPOPMIN", b"src", b"0"],
            &[b"BZPOPMAX", b"src", b"0"],
            &[b"BZMPOP", b"0", b"1", b"src", b"MIN", b"COUNT", b"2"],
        ];
        for args in cases {
            let mut engine = Engine::new(1024 * 1024);
            if kind(args[0]) == 4 {
                engine.execute(&[b"ZADD", b"src", b"1", b"a", b"2", b"b"], 0, |_| true);
            } else {
                engine.execute(&[b"RPUSH", b"src", b"a", b"b"], 0, |_| true);
            }
            engine.execute(&[b"RPUSH", b"dst", b"old"], 0, |_| true);
            engine.execute(&[b"PEXPIREAT", b"src", b"5000"], 0, |_| true);
            engine.execute(&[b"PEXPIREAT", b"dst", b"6000"], 0, |_| true);
            let before = (engine.snapshot(b"src"), engine.snapshot(b"dst"));
            assert_eq!(
                engine.execute(args, 1, |_| false),
                Reply::Error("MISCONF persistence backlog full or unavailable")
            );
            assert_eq!((engine.snapshot(b"src"), engine.snapshot(b"dst")), before);
            assert!(!matches!(
                engine.execute(args, 1, |_| true),
                Reply::Error(_)
            ));
        }
    }

    #[test]
    fn timeout_rounding_and_range_match_redis_units() {
        for (text, want) in [
            (b"0".as_slice(), 0),
            (b"0.0001", 101),
            (b"0.0011", 102),
            (b"-0.0001", 0),
            (b"0x1p-1", 600),
        ] {
            assert_eq!(deadline(&[b"BLPOP", b"k", text], 100).unwrap(), want);
        }
        for text in [b"nan".as_slice(), b"-1", b"inf", b"1e9999", b"1\0"] {
            assert!(deadline(&[b"BLPOP", b"k", text], 100).is_err());
        }
    }
    #[test]
    fn attempts_preserve_key_priority_and_admission_atomicity() {
        let mut e = Engine::new(1024 * 1024);
        e.execute(&[b"RPUSH", b"a", b"one", b"two"], 0, |_| true);
        e.execute(&[b"SET", b"wrong", b"v"], 0, |_| true);
        assert!(matches!(
            e.execute(&[b"BLPOP", b"a", b"wrong", b"0"], 0, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(e.execute(&[b"LLEN", b"a"], 0, |_| true), Reply::Integer(2));
        assert_eq!(
            e.execute(&[b"BLPOP", b"wrong", b"a", b"0"], 0, |_| true),
            WRONGTYPE
        );
        assert_eq!(
            e.execute(&[b"BLPOP", b"missing", b"a", b"wrong", b"0"], 0, |_| true),
            Reply::Array(vec![
                Reply::Bulk(Some(b"a".to_vec())),
                Reply::Bulk(Some(b"one".to_vec()))
            ])
        );
        assert_eq!(
            e.execute(&[b"BRPOP", b"missing", b"0"], 0, |_| panic!(
                "read only miss"
            )),
            Reply::NullArray
        );
    }
}
