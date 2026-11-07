//! Speculative execution followed by a single persistence admission.
//!
//! This is an engine primitive, not the Redis MULTI/WATCH connection protocol.
//! The caller must serialize this operation with normal commands and provide one
//! timestamp for the entire execution. Runtime command errors are ordinary
//! replies; a persistence admission failure rejects the whole speculative state.
use super::{Bytes, Engine, Entry, Mutation, Mutations};
use crate::protocol::Reply;
use std::collections::HashMap;

pub(super) struct Undo {
    pub(super) unlinked: Vec<Entry>,
    originals: HashMap<Bytes, (Option<Entry>, Option<super::watch::Version>)>,
    used: usize,
    next_expiry: Option<i64>,
    hash_expirations: im::OrdSet<(i64, Bytes)>,
}

/// Restores both cache data and watch versions on rejection or unwinding.
struct Rollback<'a>(&'a mut Engine);
impl Drop for Rollback<'_> {
    fn drop(&mut self) {
        let Some(undo) = self.0.undo.take() else {
            return;
        };
        for (key, (entry, watch)) in undo.originals {
            if let Some(entry) = entry {
                self.0.entries.insert(key.clone(), entry);
            } else {
                self.0.entries.remove(key.as_ref());
            }
            if let Some(watch) = watch {
                self.0.watches.insert(key, watch);
            } else {
                self.0.watches.remove(key.as_ref());
            }
        }
        self.0.used = undo.used;
        self.0.next_expiry = undo.next_expiry;
        self.0.hash_expirations = undo.hash_expirations;
    }
}

impl Engine {
    /// Record the original state before the first write to a transaction key.
    /// Values share immutable storage until a command creates its new value.
    pub(super) fn record_change(&mut self, key: &[u8]) {
        let Some(undo) = &mut self.undo else {
            return;
        };
        if undo.originals.contains_key(key) {
            return;
        }
        let stored = self.entries.get_key_value(key);
        let owned = stored.map_or_else(|| Bytes::from(key), |(k, _)| k.clone());
        undo.originals.insert(
            owned,
            (
                stored.map(|(_, e)| e.clone()),
                self.watches.get(key).cloned(),
            ),
        );
    }

    /// Execute a batch without exposing intermediate writes or admitting partial
    /// persistence records. The caller serializes execution, stages connection
    /// metadata separately, and uses one timestamp for all commands.
    ///
    /// Setup is O(1). Rollback storage and final persistence coalescing depend on
    /// changed keys, not database size. Expiration maintenance waits until after
    /// execution; normal command lookups still use the supplied timestamp.
    pub fn transact(
        &mut self,
        run: impl FnOnce(&mut Engine, &mut dyn FnMut(Mutations) -> bool) -> Vec<Reply>,
        mut submit: impl FnMut(Mutations) -> bool,
    ) -> Result<Vec<Reply>, Reply> {
        if self.undo.is_some() {
            return Err(Reply::Error(
                "ERR nested engine transactions are not supported",
            ));
        }
        self.undo = Some(Undo {
            unlinked: Vec::new(),
            originals: HashMap::new(),
            used: self.used,
            next_expiry: self.next_expiry,
            hash_expirations: self.hash_expirations.clone(),
        });
        let guard = Rollback(self);
        let mut changes = HashMap::<Bytes, Option<Entry>>::new();
        let mut ready_collections = Vec::new();
        let mut seen_lists = std::collections::HashSet::new();
        let replies = run(guard.0, &mut |mutations| {
            for mutation in mutations {
                if mutation
                    .entry
                    .as_ref()
                    .is_some_and(|e| matches!(e.value.kind(), 2 | 4))
                    && seen_lists.insert(mutation.key.clone())
                {
                    ready_collections.push(mutation.key.clone());
                }
                changes.insert(mutation.key, mutation.entry);
            }
            true
        });
        if !changes.is_empty() {
            // Persistence stores final values, but blocked consumers need the
            // order in which collection keys first became ready within this EXEC.
            let mut mutations: Mutations = ready_collections
                .into_iter()
                .filter_map(|key| changes.remove(&key).map(|entry| Mutation { key, entry }))
                .collect();
            mutations.extend(
                changes
                    .into_iter()
                    .map(|(key, entry)| Mutation { key, entry }),
            );
            if !submit(mutations) {
                return Err(Reply::Error(
                    "MISCONF persistence backlog full or unavailable",
                ));
            }
        }
        // Dropping the checkpoint publishes all changes together. The guard's
        // Drop implementation is now a no-op; rejected/unwinding paths restore it.
        let mut undo = guard.0.undo.take().unwrap();
        let unlinked = std::mem::take(&mut undo.unlinked);
        // Release rollback references before the worker can free unlinked values.
        drop(undo);
        super::lazyfree::reclaim(unlinked);
        Ok(replies)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{journal, server};

    fn run(
        engine: &mut Engine,
        commands: &[&[&[u8]]],
        now: i64,
        submit: impl FnMut(Mutations) -> bool,
    ) -> Result<Vec<Reply>, Reply> {
        engine.transact(
            |staged, admit| {
                commands
                    .iter()
                    .map(|args| staged.execute(args, now, &mut *admit))
                    .collect()
            },
            submit,
        )
    }
    fn bulk(value: &[u8]) -> Reply {
        Reply::Bulk(Some(value.to_vec()))
    }

    #[test]
    fn coalesced_list_writes_preserve_first_ready_order() {
        let mut engine = Engine::new(1024 * 1024);
        let mut keys = Vec::new();
        run(
            &mut engine,
            &[
                &[b"SET", b"a", b"string"],
                &[b"RPUSH", b"b", b"first"],
                &[b"DEL", b"a"],
                &[b"RPUSH", b"a", b"second"],
                &[b"RPUSH", b"b", b"third"],
            ],
            0,
            |mutations| {
                keys = mutations.into_iter().map(|m| m.key).collect();
                true
            },
        )
        .unwrap();
        assert_eq!(
            keys,
            vec![Bytes::from(b"b".as_slice()), Bytes::from(b"a".as_slice())]
        );
    }

    #[test]
    fn checkpoint_only_retains_changed_keys_and_never_copies_unrelated_values() {
        let mut engine = Engine::new(16 * 1024 * 1024);
        for i in 0..4096 {
            engine.execute(&[b"SET", i.to_string().as_bytes(), b"value"], 0, |_| true);
        }
        let crate::value::Value::String(untouched) = &engine.get(b"4095", 0).unwrap().value else {
            unreachable!()
        };
        let untouched = untouched.clone();
        let references = std::sync::Arc::strong_count(&untouched);
        engine
            .transact(
                |staged, admit| {
                    assert_eq!(std::sync::Arc::strong_count(&untouched), references);
                    let mut replies = Vec::new();
                    for _ in 0..5 {
                        replies.push(staged.execute(&[b"SET", b"0", b"changed"], 1, &mut *admit));
                    }
                    replies.push(staged.execute(&[b"DEL", b"1"], 1, &mut *admit));
                    assert_eq!(staged.undo.as_ref().unwrap().originals.len(), 2);
                    assert_eq!(std::sync::Arc::strong_count(&untouched), references);
                    replies
                },
                |_| true,
            )
            .unwrap();
        assert_eq!(std::sync::Arc::strong_count(&untouched), references);
        engine
            .transact(
                |staged, _| {
                    assert!(staged.undo.as_ref().unwrap().originals.is_empty());
                    vec![staged.execute(&[b"GET", b"4095"], 2, |_| panic!("read only"))]
                },
                |_| panic!("read only"),
            )
            .unwrap();
    }

    #[test]
    fn unwinding_restores_values_versions_expiration_and_memory_accounting() {
        let mut engine = Engine::new(1024 * 1024);
        engine.execute(&[b"HSET", b"hash", b"f", b"old"], 0, |_| true);
        engine.execute(&[b"PEXPIRE", b"hash", b"100"], 0, |_| true);
        let watched = engine.watch(b"hash", 1);
        let before = (engine.entries.clone(), engine.used, engine.next_expiry);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = engine.transact(
                |staged, admit| {
                    staged.execute(&[b"HSET", b"hash", b"f", b"new"], 2, &mut *admit);
                    staged.execute(&[b"SET", b"created", b"v"], 2, &mut *admit);
                    staged.execute(&[b"DEL", b"hash"], 2, &mut *admit);
                    panic!("intentional callback unwind");
                },
                |_| panic!("must not admit"),
            );
        }));
        assert!(panic.is_err());
        assert_eq!(
            (engine.entries.clone(), engine.used, engine.next_expiry),
            before
        );
        assert!(!engine.watch_dirty(b"hash", &watched, 2));
        assert!(engine.undo.is_none());
        assert!(run(&mut engine, &[&[b"HGET", b"hash", b"f"]], 2, |_| true).is_ok());
    }

    #[test]
    fn transaction_defers_maintenance_and_rejects_nested_checkpoints() {
        let mut engine = Engine::new(1024 * 1024);
        engine.execute(&[b"SET", b"expiring", b"v", b"PX", b"10"], 0, |_| true);
        engine
            .transact(
                |staged, _| {
                    staged.reap(11);
                    assert!(staged.snapshot(b"expiring").is_some());
                    assert!(staged.transact(|_, _| vec![], |_| true).is_err());
                    vec![]
                },
                |_| true,
            )
            .unwrap();
        engine.reap(11);
        assert!(engine.snapshot(b"expiring").is_none());
        assert_eq!(engine.used, 0);
    }

    #[test]
    #[ignore = "manual local scaling probe, not a Redis throughput benchmark"]
    fn transaction_scaling_report() {
        use std::{hint::black_box, time::Instant};
        for keys in [100usize, 10000, 100000] {
            let mut engine = Engine::new(128 * 1024 * 1024);
            for i in 0..keys {
                engine.execute(&[b"SET", i.to_string().as_bytes(), b"v"], 0, |_| true);
            }
            let iterations = 200;
            let started = Instant::now();
            for _ in 0..iterations {
                black_box(
                    run(
                        &mut engine,
                        &[&[b"SET", b"0", b"updated"], &[b"GET", b"0"]],
                        1,
                        |_| true,
                    )
                    .unwrap(),
                );
            }
            let touched_ns = started.elapsed().as_nanos() / iterations;
            // Lower bound for the old path: directory clone/drop only, without
            // running commands, encoding replies or staging persistence.
            let started = Instant::now();
            for _ in 0..iterations {
                black_box(engine.entries.clone());
            }
            let clone_ns = started.elapsed().as_nanos() / iterations;
            println!("keys={keys} touched_transaction_ns={touched_ns} directory_clone_only_ns={clone_ns}");
        }
    }

    #[test]
    fn transaction_reads_its_writes_and_submits_unique_final_keys_once() {
        let mut engine = Engine::new(1024 * 1024);
        let mut admitted = Vec::new();
        let replies = run(
            &mut engine,
            &[
                &[b"SET", b"n", b"1"],
                &[b"INCR", b"n"],
                &[b"GET", b"n"],
                &[b"RPUSH", b"list", b"a"],
                &[b"RPUSH", b"list", b"b"],
                &[b"SET", b"deleted", b"v"],
                &[b"DEL", b"deleted"],
            ],
            100,
            |m| {
                admitted.push(m);
                true
            },
        )
        .unwrap();
        assert_eq!(replies[2], bulk(b"2"));
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].len(), 3);
        assert_eq!(
            engine.execute(&[b"LRANGE", b"list", b"0", b"-1"], 100, |_| panic!("read")),
            Reply::Array(vec![bulk(b"a"), bulk(b"b")])
        );
        // The actual journal serializer can replay the entire admitted state.
        let payload = journal::encode(&admitted[0]).unwrap();
        let recovered = journal::decode(&payload).unwrap();
        assert_eq!(recovered.len(), 3);
        for mutation in recovered {
            assert_eq!(engine.snapshot(&mutation.key), mutation.entry);
        }
    }

    #[test]
    fn runtime_errors_do_not_rollback_successful_commands() {
        let mut engine = Engine::new(1024 * 1024);
        let replies = run(
            &mut engine,
            &[
                &[b"SET", b"key", b"text"],
                &[b"INCR", b"key"],
                &[b"HSET", b"key", b"field", b"value"],
                &[b"SET", b"after", b"yes"],
            ],
            0,
            |_| true,
        )
        .unwrap();
        assert!(matches!(replies[1], Reply::Error(_)));
        assert_eq!(replies[2], crate::collections::WRONGTYPE);
        assert_eq!(
            engine.execute(&[b"GET", b"key"], 0, |_| true),
            bulk(b"text")
        );
        assert_eq!(
            engine.execute(&[b"GET", b"after"], 0, |_| true),
            bulk(b"yes")
        );
    }

    #[test]
    fn rejected_admission_preserves_values_expiry_accounting_and_collection_sharing() {
        let mut engine = Engine::new(1024 * 1024);
        engine.execute(&[b"HSET", b"hash", b"f", b"old"], 0, |_| true);
        engine.execute(&[b"PEXPIRE", b"hash", b"100"], 0, |_| true);
        engine.execute(&[b"COPY", b"hash", b"copy"], 0, |_| true);
        let before = (engine.entries.clone(), engine.used, engine.next_expiry);
        let mut attempts = 0;
        let reply = run(
            &mut engine,
            &[
                &[b"HSET", b"hash", b"f", b"changed"],
                &[b"PERSIST", b"hash"],
                &[b"DEL", b"copy"],
                &[b"SET", b"new", b"value"],
            ],
            50,
            |_| {
                attempts += 1;
                false
            },
        );
        assert!(reply.is_err());
        assert_eq!(attempts, 1);
        assert_eq!(
            (engine.entries.clone(), engine.used, engine.next_expiry),
            before
        );
        assert_eq!(
            engine.execute(&[b"HGET", b"copy", b"f"], 50, |_| true),
            bulk(b"old")
        );
        engine.reap(100);
        assert!(engine.get(b"hash", 100).is_none());
        assert_eq!(engine.used, 0);
    }

    #[test]
    fn read_only_execution_needs_no_persistence_admission() {
        let mut engine = Engine::new(1024 * 1024);
        engine.execute(&[b"SET", b"key", b"v", b"PX", b"100"], 0, |_| true);
        let replies = run(
            &mut engine,
            &[&[b"GET", b"key"], &[b"PTTL", b"key"]],
            50,
            |_| panic!("read-only transaction"),
        )
        .unwrap();
        assert_eq!(replies, vec![bulk(b"v"), Reply::Integer(50)]);
        assert!(run(&mut engine, &[], 50, |_| panic!("empty transaction"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn sink_rejection_can_retry_without_partial_writes() {
        let mut engine = Engine::new(1024 * 1024);
        let (mut small, _receiver) = server::channel_with_limit(0, false, 128);
        let commands: &[&[&[u8]]] = &[&[b"SET", b"a", b"1"], &[b"SET", b"b", b"2"]];
        assert!(run(&mut engine, commands, 0, |m| small.submit(m)).is_err());
        assert!(engine.entries.is_empty());
        let (mut sink, _receiver) = server::channel_with_limit(0, false, 4096);
        assert!(run(&mut engine, commands, 0, |m| sink.submit(m)).is_ok());
        assert_eq!(engine.entries.len(), 2);
    }
}
