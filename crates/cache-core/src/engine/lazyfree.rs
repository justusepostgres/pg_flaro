//! UNLINK detaches keys atomically and releases values on a non-PostgreSQL thread.
use super::{Engine, Entry, Mutations};
use crate::protocol::Reply;
use std::{
    collections::HashSet,
    sync::{
        mpsc::{self, Sender},
        OnceLock,
    },
};

fn worker() -> Result<&'static Sender<Vec<Entry>>, &'static str> {
    static WORKER: OnceLock<Result<Sender<Vec<Entry>>, &'static str>> = OnceLock::new();
    WORKER
        .get_or_init(|| {
            let (sender, receiver) = mpsc::channel::<Vec<Entry>>();
            std::thread::Builder::new()
                .name("pg-cache-lazyfree".into())
                .spawn(move || {
                    for entries in receiver {
                        drop(entries);
                    }
                })
                .map_err(|_| "ERR unable to start lazy-free worker")?;
            Ok(sender)
        })
        .as_ref()
        .map_err(|error| *error)
}

pub(super) fn reclaim(entries: Vec<Entry>) {
    if entries.is_empty() {
        return;
    }
    // The worker is initialized before admitting UNLINK. If it has unexpectedly
    // exited, SendError owns the values and dropping it still releases memory.
    if let Ok(sender) = worker() {
        let _ = sender.send(entries);
    }
}

pub(super) fn unlink(
    engine: &mut Engine,
    args: &[&[u8]],
    now: i64,
    submit: &mut impl FnMut(Mutations) -> bool,
) -> Reply {
    if args.len() < 2 {
        return Reply::Error("ERR wrong number of arguments for 'unlink' command");
    }
    let mut seen = HashSet::new();
    let mut retained = Vec::new();
    let mut changes = Vec::new();
    for key in &args[1..] {
        if !seen.insert(*key) {
            continue;
        }
        if let Some(entry) = engine.get(key, now) {
            retained.push(entry.clone());
            changes.push((key.to_vec(), None));
        }
    }
    if changes.is_empty() {
        return Reply::Integer(0);
    }
    if let Err(error) = worker() {
        return Reply::Error(error);
    }
    let count = changes.len() as i64;
    let reply = engine.apply_typed(changes, Reply::Integer(count), submit);
    if reply == Reply::Integer(count) {
        if let Some(undo) = &mut engine.undo {
            undo.unlinked.extend(retained);
        } else {
            reclaim(retained);
        }
    }
    reply
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    #[test]
    fn unlink_admission_rollback_and_watch_semantics() {
        let mut engine = Engine::new(1 << 20);
        engine.execute(&[b"HSET", b"h", b"a", b"1"], 0, |_| true);
        engine.execute(
            &[b"HPEXPIREAT", b"h", b"100", b"FIELDS", b"1", b"a"],
            0,
            |_| true,
        );
        let before = engine.snapshot(b"h");
        let watch = engine.watch(b"h", 0);
        let command: &[&[u8]] = &[b"UNLINK", b"h", b"h", b"missing"];
        assert!(matches!(
            engine.execute(command, 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"h"), before);
        assert!(!engine.watch_dirty(b"h", &watch, 1));
        assert!(engine
            .transact(
                |engine, submit| vec![engine.execute(command, 1, submit)],
                |_| false
            )
            .is_err());
        assert_eq!(engine.snapshot(b"h"), before);
        assert!(!engine.watch_dirty(b"h", &watch, 1));
        assert_eq!(engine.hash_expirations.len(), 1);
        assert_eq!(
            engine.transact(
                |engine, submit| vec![engine.execute(command, 1, submit)],
                |_| true
            ),
            Ok(vec![Reply::Integer(1)])
        );
        assert!(engine.snapshot(b"h").is_none());
        assert!(engine.watch_dirty(b"h", &watch, 1));
        assert!(engine.hash_expirations.is_empty());
        assert_eq!(engine.used, 0);
        assert_eq!(
            engine.execute(command, 2, |_| panic!("missing keys need no admission")),
            Reply::Integer(0)
        );
    }

    #[test]
    fn committed_unlink_eventually_releases_collection_storage() {
        let mut engine = Engine::new(1 << 20);
        engine.execute(&[b"HSET", b"h", b"a", b"1"], 0, |_| true);
        let weak = match &engine.get(b"h", 0).unwrap().value {
            Value::Collection { data, .. } => Arc::downgrade(data),
            _ => unreachable!(),
        };
        engine
            .transact(
                |engine, submit| vec![engine.execute(&[b"UNLINK", b"h"], 0, submit)],
                |_| true,
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while weak.strong_count() != 0 && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(weak.strong_count(), 0);
    }

    #[test]
    fn randomkey_excludes_expired_keys_and_accepts_binary_names() {
        let mut engine = Engine::new(1 << 20);
        assert_eq!(
            engine.execute(&[b"RANDOMKEY"], 0, |_| true),
            Reply::Bulk(None)
        );
        engine.execute(&[b"SET", b"expired", b"v", b"PX", b"1"], 0, |_| true);
        engine.execute(&[b"HSET", b"expired-hash", b"f", b"v"], 0, |_| true);
        engine.execute(
            &[b"HPEXPIREAT", b"expired-hash", b"1", b"FIELDS", b"1", b"f"],
            0,
            |_| true,
        );
        engine.execute(&[b"SET", b"\0\xff", b"v"], 0, |_| true);
        for _ in 0..100 {
            assert_eq!(
                engine.execute(&[b"RANDOMKEY"], 1, |_| panic!("read only")),
                Reply::Bulk(Some(b"\0\xff".to_vec()))
            );
        }
        assert_eq!(
            engine.execute(&[b"RANDOMKEY", b"extra"], 1, |_| true),
            Reply::Error("ERR wrong number of arguments for 'randomkey' command")
        );
        engine.execute(&[b"UNLINK", b"\0\xff"], 1, |_| true);
        assert_eq!(
            engine.execute(&[b"RANDOMKEY"], 1, |_| true),
            Reply::Bulk(None)
        );
    }
}
