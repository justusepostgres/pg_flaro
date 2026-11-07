//! Option-dependent access modes for COMMAND GETKEYSANDFLAGS.
//! This mirrors key discovery, which does not validate complete command syntax.
pub(super) fn flags(name: &str, args: &[&[u8]], ordinal: usize) -> &'static [&'static str] {
    if name == "set" {
        if args[3..]
            .iter()
            .any(|arg| c_string(arg).eq_ignore_ascii_case(b"GET"))
        {
            return &["RW", "access", "update"];
        }
        return &["OW", "update"];
    }
    if name == "bitfield" {
        let mut i = 2;
        while i < args.len() {
            let sub = c_string(args[i]);
            if sub.eq_ignore_ascii_case(b"GET") && args.len() - i >= 3 {
                i += 3;
            } else if sub.eq_ignore_ascii_case(b"OVERFLOW") && args.len() - i >= 2 {
                i += 2;
            } else {
                // Both write operations and malformed operations are conservatively RW.
                return &["RW", "access", "update"];
            }
        }
        return &["RO", "access"];
    }
    super::static_key_flags::flags(name, ordinal)
}
fn c_string(value: &[u8]) -> &[u8] {
    value.split(|b| *b == 0).next().unwrap_or_default()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dynamic_flags_and_duplicate_key_roles_are_position_sensitive() {
        assert_eq!(flags("set", &[b"SET", b"a", b"v"], 0), &["OW", "update"]);
        assert_eq!(
            flags("set", &[b"SET", b"a", b"v", b"GET"], 0),
            &["RW", "access", "update"]
        );
        assert_eq!(
            flags("bitfield", &[b"BITFIELD", b"a", b"GET", b"i8", b"0"], 0),
            &["RO", "access"]
        );
        assert_eq!(
            flags("bitfield", &[b"BITFIELD", b"a", b"GET"], 0),
            &["RW", "access", "update"]
        );
        let a: &[&[u8]] = &[b"LMOVE", b"same", b"same", b"LEFT", b"RIGHT"];
        assert_eq!(flags("lmove", a, 0), &["RW", "access", "delete"]);
        assert_eq!(flags("lmove", a, 1), &["RW", "insert"]);
    }
}
