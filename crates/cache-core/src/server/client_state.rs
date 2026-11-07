//! Metadata belongs to a connection and never enters the persistence queue.
use crate::protocol::Reply;
use std::sync::atomic::{AtomicU64, Ordering};
pub(super) type BlockedClients =
    std::rc::Rc<std::cell::RefCell<std::collections::HashMap<u64, usize>>>;
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub(super) struct State {
    pub(super) watches: std::collections::HashMap<Vec<u8>, crate::engine::watch::Watch>,
    pub(super) transaction: Option<super::transaction::Transaction>,
    pub(super) protocol: u8,
    pub(super) id: u64,
    pub(super) blocked_clients: BlockedClients,
    pub(super) unblocks: std::collections::HashMap<u64, bool>,
    name: Vec<u8>,
    lib_name: Vec<u8>,
    lib_version: Vec<u8>,
}
impl State {
    pub(super) fn reset(&mut self) -> Reply {
        self.watches.clear();
        self.transaction = None;
        self.protocol = 2;
        // Library identity and the connection ID survive RESET in Redis.
        self.name.clear();
        Reply::Simple("RESET")
    }
    pub(super) fn new() -> Self {
        Self {
            watches: std::collections::HashMap::new(),
            transaction: None,
            protocol: 2,
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            blocked_clients: Default::default(),
            unblocks: Default::default(),
            name: Vec::new(),
            lib_name: Vec::new(),
            lib_version: Vec::new(),
        }
    }
    pub(super) fn encode(&self, args: &[&[u8]], reply: &Reply, out: &mut Vec<u8>) {
        if self.protocol == 3 {
            crate::protocol::resp3::encode(args, reply, out);
        } else {
            reply.encode(out);
        }
    }
    pub(super) fn hello(&mut self, args: &[&[u8]], cluster: bool) -> Reply {
        let version = if let Some(raw) = args.get(1) {
            let parsed = std::str::from_utf8(raw)
                .ok()
                .and_then(|s| s.parse::<i64>().ok().filter(|n| n.to_string() == s));
            match parsed {
                Some(2 | 3) => parsed.unwrap() as u8,
                Some(_) => return Reply::Error("NOPROTO unsupported protocol version"),
                None => {
                    return Reply::Error("ERR Protocol version is not an integer or out of range")
                }
            }
        } else {
            self.protocol
        };
        let (mut name, mut user) = (None, None);
        let mut i = 2;
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"SETNAME") && i + 1 < args.len() {
                if !valid(args[i + 1]) {
                    return Reply::Error(
                        "ERR Client names cannot contain spaces, newlines or special characters.",
                    );
                }
                name = Some(args[i + 1]);
                i += 2;
            } else if args[i].eq_ignore_ascii_case(b"AUTH") && i + 2 < args.len() {
                user = Some(args[i + 1]);
                i += 3;
            } else {
                return Reply::OwnedError(format!(
                    "ERR Syntax error in HELLO option '{}'",
                    display(args[i])
                ));
            }
        }
        // The current server has only the default nopass user. Configurable ACL
        // authentication is still pending; do not claim password enforcement.
        if user.is_some_and(|u| u != b"default") {
            return Reply::Error("WRONGPASS invalid username-password pair or user is disabled.");
        }
        if let Some(name) = name {
            self.name = name.to_vec();
        }
        self.protocol = version;
        let b = |v: &[u8]| Reply::Bulk(Some(v.to_vec()));
        Reply::Array(vec![
            b(b"server"),
            b(b"flaro"),
            b(b"version"),
            b(env!("CARGO_PKG_VERSION").as_bytes()),
            b(b"proto"),
            Reply::Integer(version as i64),
            b(b"id"),
            Reply::Integer(self.id as i64),
            b(b"mode"),
            b(if cluster { b"cluster" } else { b"standalone" }),
            b(b"role"),
            b(b"master"),
            b(b"modules"),
            Reply::Array(vec![]),
        ])
    }
    pub(super) fn execute(&mut self, a: &[&[u8]]) -> Reply {
        if a.len() < 2 {
            return Reply::Error("ERR wrong number of arguments for 'client' command");
        }
        let sub = a[1];
        if sub.eq_ignore_ascii_case(b"UNBLOCK") {
            if a.len() < 3 {
                return Reply::Error("ERR wrong number of arguments for 'client|unblock' command");
            }
            if a.len() > 4 {
                return Reply::OwnedError(format!("ERR unknown subcommand or wrong number of arguments for '{}'. Try CLIENT HELP.", display(sub)));
            }
            let error = if a.len() == 3 || a[3].eq_ignore_ascii_case(b"TIMEOUT") {
                false
            } else if a[3].eq_ignore_ascii_case(b"ERROR") {
                true
            } else {
                return Reply::Error("ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
            };
            let id = match std::str::from_utf8(a[2])
                .ok()
                .and_then(|s| s.parse::<i64>().ok().filter(|n| n.to_string() == s))
            {
                Some(id) => id as u64,
                None => return Reply::Error("ERR value is not an integer or out of range"),
            };
            if !self.blocked_clients.borrow().contains_key(&id) || self.unblocks.contains_key(&id) {
                return Reply::Integer(0);
            }
            // EXEC clones this queue and publishes it only after admission.
            // Never mutate another connection during speculative execution.
            self.unblocks.insert(id, error);
            return Reply::Integer(1);
        }

        let expected = if sub.eq_ignore_ascii_case(b"ID") {
            Some((2, "id"))
        } else if sub.eq_ignore_ascii_case(b"GETNAME") {
            Some((2, "getname"))
        } else if sub.eq_ignore_ascii_case(b"SETNAME") {
            Some((3, "setname"))
        } else if sub.eq_ignore_ascii_case(b"SETINFO") {
            Some((4, "setinfo"))
        } else {
            None
        };
        if let Some((arity, name)) = expected {
            if a.len() != arity {
                return Reply::OwnedError(format!(
                    "ERR wrong number of arguments for 'client|{name}' command"
                ));
            }
        }
        if sub.eq_ignore_ascii_case(b"ID") && a.len() == 2 {
            return Reply::Integer(self.id as i64);
        }
        if sub.eq_ignore_ascii_case(b"GETNAME") && a.len() == 2 {
            return Reply::Bulk((!self.name.is_empty()).then(|| self.name.clone()));
        }
        if sub.eq_ignore_ascii_case(b"SETNAME") && a.len() == 3 {
            if !valid(a[2]) {
                return Reply::Error(
                    "ERR Client names cannot contain spaces, newlines or special characters.",
                );
            }
            self.name = a[2].to_vec();
            return Reply::Simple("OK");
        }
        if sub.eq_ignore_ascii_case(b"SETINFO") {
            let attribute = a[2].split(|b| *b == 0).next().unwrap_or_default();
            let destination = if attribute.eq_ignore_ascii_case(b"LIB-NAME") {
                &mut self.lib_name
            } else if attribute.eq_ignore_ascii_case(b"LIB-VER") {
                &mut self.lib_version
            } else {
                return Reply::OwnedError(format!("ERR Unrecognized option '{}'", display(a[2])));
            };
            if !valid(a[3]) {
                return Reply::OwnedError(format!(
                    "ERR {} cannot contain spaces, newlines or special characters.",
                    display(a[2])
                ));
            }
            *destination = a[3].to_vec();
            return Reply::Simple("OK");
        }
        Reply::OwnedError(format!(
            "ERR unknown subcommand '{}'. Try CLIENT HELP.",
            display(sub)
        ))
    }
}

pub(super) fn time() -> Reply {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before Unix epoch");
    Reply::Array(vec![
        Reply::Bulk(Some(now.as_secs().to_string().into_bytes())),
        Reply::Bulk(Some(now.subsec_micros().to_string().into_bytes())),
    ])
}
fn valid(value: &[u8]) -> bool {
    // Redis's validateClientAttr checks a C string: validation stops at NUL,
    // while the stored name and GETNAME preserve the original bulk-string bytes.
    value
        .iter()
        .take_while(|b| **b != 0)
        .all(|b| (b'!'..=b'~').contains(b))
}
fn display(value: &[u8]) -> String {
    let prefix = value.split(|b| *b == 0).next().unwrap_or_default();
    String::from_utf8_lossy(prefix).replace(['\r', '\n'], " ")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hello_negotiation_is_atomic_and_reset_restores_resp2() {
        let mut state = State::new();
        let id = state.id;
        let args: &[&[u8]] = &[b"HELLO", b"3", b"SETNAME", b"worker"];
        let reply = state.hello(args, false);
        let mut wire = Vec::new();
        state.encode(args, &reply, &mut wire);
        assert!(wire.starts_with(b"%7\r\n"));
        for invalid in [
            vec![b"HELLO".as_slice(), b"2", b"SETNAME", b"changed", b"bad"],
            vec![b"HELLO".as_slice(), b"2", b"AUTH", b"unknown", b"pw"],
            vec![b"HELLO".as_slice(), b"4"],
            vec![b"HELLO".as_slice(), b"03"],
        ] {
            assert!(matches!(
                state.hello(&invalid, false),
                Reply::Error(_) | Reply::OwnedError(_)
            ));
            assert_eq!(state.protocol, 3);
            assert_eq!(state.name, b"worker");
        }
        state.hello(&[b"HELLO", b"2", b"AUTH", b"default", b"anything"], false);
        assert_eq!(state.protocol, 2);
        state.hello(&[b"HELLO", b"3"], false);
        state.reset();
        assert_eq!(state.protocol, 2);
        assert!(state.name.is_empty());
        assert_eq!(state.id, id);
    }
    #[test]
    fn client_metadata_is_isolated_and_failed_validation_preserves_it() {
        let mut a = State::new();
        let mut b = State::new();
        assert_ne!(a.id, b.id);
        assert_eq!(
            a.execute(&[b"CLIENT", b"SETNAME", b"worker"]),
            Reply::Simple("OK")
        );
        assert_eq!(b.execute(&[b"CLIENT", b"GETNAME"]), Reply::Bulk(None));
        assert!(matches!(
            a.execute(&[b"CLIENT", b"SETNAME", b"bad name"]),
            Reply::Error(_)
        ));
        assert_eq!(
            a.execute(&[b"CLIENT", b"GETNAME"]),
            Reply::Bulk(Some(b"worker".to_vec()))
        );
        assert_eq!(
            a.execute(&[b"CLIENT", b"SETINFO", b"LIB-NAME", b"redis-py"]),
            Reply::Simple("OK")
        );
        assert!(matches!(
            a.execute(&[b"CLIENT", b"SETINFO", b"LIB-NAME", b"bad\nname"]),
            Reply::OwnedError(_)
        ));
        assert_eq!(a.lib_name, b"redis-py");
        assert_eq!(
            a.execute(&[b"CLIENT", b"SETNAME", b""]),
            Reply::Simple("OK")
        );
        assert_eq!(a.execute(&[b"CLIENT", b"GETNAME"]), Reply::Bulk(None));
        assert_eq!(
            a.execute(&[b"CLIENT", b"SETNAME", b"\0\xff name"]),
            Reply::Simple("OK")
        );
        assert_eq!(
            a.execute(&[b"CLIENT", b"GETNAME"]),
            Reply::Bulk(Some(b"\0\xff name".to_vec()))
        );
    }
    #[test]
    fn reset_keeps_connection_and_library_identity() {
        let mut state = State::new();
        let id = state.id;
        state.execute(&[b"CLIENT", b"SETNAME", b"worker"]);
        state.execute(&[b"CLIENT", b"SETINFO", b"LIB-NAME", b"library"]);
        state.execute(&[b"CLIENT", b"SETINFO", b"LIB-VER", b"1.2"]);
        assert_eq!(state.reset(), Reply::Simple("RESET"));
        assert!(state.name.is_empty());
        assert_eq!(state.id, id);
        assert_eq!(state.lib_name, b"library");
        assert_eq!(state.lib_version, b"1.2");
    }
}
