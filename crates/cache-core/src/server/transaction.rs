//! Connection queues, optimistic WATCH guards, and atomic EXEC admission.
use super::{client_state::State, Engine, Sink, MAX_OUTPUT};
use crate::{
    cluster::{self, Cluster},
    now_ms,
    protocol::{Reply, MAX_FRAME},
};

#[derive(Clone, Default)]
pub(super) struct Transaction {
    commands: Vec<Vec<Vec<u8>>>,
    bytes: usize,
    dirty: bool,
}
pub(super) enum Outcome {
    Reply(Reply),
    Encoded(Vec<u8>),
    Deferred,
}
const ABORT: Reply = Reply::Error("EXECABORT Transaction discarded because of previous errors.");

pub(super) fn handle(
    state: &mut State,
    a: &[&[u8]],
    engine: &mut Engine,
    sink: &mut Sink,
    cluster: &Option<Cluster>,
    asking: bool,
) -> Option<Outcome> {
    let is = |name: &[u8]| a[0].eq_ignore_ascii_case(name);
    let control = is(b"MULTI") || is(b"EXEC") || is(b"DISCARD") || is(b"WATCH") || is(b"UNWATCH");
    if !control && state.transaction.is_none() {
        return None;
    }
    // Redis executes these immediately rather than queuing them.
    if (is(b"RESET") && a.len() == 1) || is(b"QUIT") {
        return None;
    }
    if let Err(error) = cluster::validate_command(a) {
        if is(b"EXEC") && state.transaction.take().is_some() {
            state.watches.clear();
            return Some(Outcome::Reply(Reply::Error("EXECABORT Transaction discarded because of: wrong number of arguments for 'exec' command")));
        }
        if let Some(tx) = &mut state.transaction {
            tx.dirty = true;
        }
        return Some(Outcome::Reply(error));
    }
    if is(b"WATCH") {
        if state.transaction.is_some() {
            return Some(Outcome::Reply(Reply::Error(
                "ERR WATCH inside MULTI is not allowed",
            )));
        }
        let now = now_ms();
        engine.expire_hash_fields(now);
        if let Some(cluster) = cluster {
            if let Some(error) = cluster.route(a, asking, engine, now) {
                return Some(Outcome::Reply(error));
            }
        }
        if state
            .watches
            .iter()
            .any(|(key, watch)| engine.watch_dirty(key, watch, now))
        {
            return Some(Outcome::Reply(Reply::Simple("OK")));
        }
        let bytes = state.watches.keys().map(|k| k.len() + 64).sum::<usize>()
            + a[1..].iter().map(|k| k.len() + 64).sum::<usize>();
        if bytes > MAX_FRAME {
            return Some(Outcome::Reply(Reply::Error(
                "ERR watched keys exceed maximum size",
            )));
        }
        for key in &a[1..] {
            state
                .watches
                .entry(key.to_vec())
                .or_insert_with(|| engine.watch(key, now));
        }
        return Some(Outcome::Reply(Reply::Simple("OK")));
    }
    if is(b"UNWATCH") && state.transaction.is_none() {
        state.watches.clear();
        return Some(Outcome::Reply(Reply::Simple("OK")));
    }
    if is(b"MULTI") {
        if state.transaction.is_some() {
            return Some(Outcome::Reply(Reply::Error(
                "ERR MULTI calls can not be nested",
            )));
        }
        state.transaction = Some(Transaction::default());
        return Some(Outcome::Reply(Reply::Simple("OK")));
    }
    if is(b"DISCARD") {
        return Some(Outcome::Reply(if state.transaction.take().is_some() {
            state.watches.clear();
            Reply::Simple("OK")
        } else {
            Reply::Error("ERR DISCARD without MULTI")
        }));
    }
    if is(b"EXEC") {
        return Some(exec(state, engine, sink, cluster, asking));
    }
    let tx = state.transaction.as_mut().expect("active transaction");
    if let Some(cluster) = cluster {
        if let Some(error) = cluster.route(a, asking, engine, now_ms()) {
            tx.dirty = true;
            return Some(Outcome::Reply(error));
        }
    }
    let bytes = a.iter().map(|v| v.len() + 32).sum::<usize>();
    if tx.bytes.saturating_add(bytes) > MAX_FRAME || tx.commands.len() >= 4096 {
        tx.dirty = true;
        return Some(Outcome::Reply(Reply::Error(
            "ERR transaction queue exceeds maximum size",
        )));
    }
    tx.bytes += bytes;
    tx.commands.push(a.iter().map(|v| v.to_vec()).collect());
    Some(Outcome::Reply(Reply::Simple("QUEUED")))
}
fn exec(
    state: &mut State,
    engine: &mut Engine,
    sink: &mut Sink,
    cluster: &Option<Cluster>,
    asking: bool,
) -> Outcome {
    let Some(tx) = state.transaction.take() else {
        return Outcome::Reply(Reply::Error("ERR EXEC without MULTI"));
    };
    let now = now_ms();
    engine.expire_hash_fields(now);
    // Redis checks the aggregate key set before running EXEC, including before
    // dirty-queue and WATCH checks. Check migration presence on the entire set.
    if let Some(cluster) = cluster {
        let keys: Vec<_> = tx
            .commands
            .iter()
            .flat_map(|command| {
                let a: Vec<_> = command.iter().map(Vec::as_slice).collect();
                cluster::command_keys(&a)
            })
            .collect();
        if let Some(error) = cluster.route_keys(keys, asking, engine, now) {
            state.watches.clear();
            return Outcome::Reply(error);
        }
    }
    if tx.dirty {
        state.watches.clear();
        return Outcome::Reply(ABORT);
    }
    if state
        .watches
        .iter()
        .any(|(key, watch)| engine.watch_dirty(key, watch, now))
    {
        state.watches.clear();
        return Outcome::Reply(Reply::NullArray);
    }
    let mut shadow = state.clone();
    shadow.watches.clear();
    let mut output = format!("*{}\r\n", tx.commands.len()).into_bytes();
    let output_limit = std::cell::Cell::new(false);
    let mut batch_limit = false;
    // Snapshot stats before borrowing the sink for persistence admission.
    let stats = sink.stats();
    let result = engine.transact(
        |staged, admit| {
            for command in &tx.commands {
                let a: Vec<_> = command.iter().map(Vec::as_slice).collect();
                let reply = if a[0].eq_ignore_ascii_case(b"HELLO") {
                    shadow.hello(&a, cluster.is_some())
                } else if a[0].eq_ignore_ascii_case(b"ASKING") {
                    if cluster.is_some() {
                        Reply::Simple("OK")
                    } else {
                        Reply::Error("ERR cluster support disabled")
                    }
                } else if a[0].eq_ignore_ascii_case(b"UNWATCH") {
                    shadow.watches.clear();
                    Reply::Simple("OK")
                } else if a[0].eq_ignore_ascii_case(b"CLIENT") {
                    shadow.execute(&a)
                } else if a[0].eq_ignore_ascii_case(b"TIME") {
                    super::client_state::time()
                } else if a[0].eq_ignore_ascii_case(b"COMMAND") {
                    cluster::command(&a)
                } else if a[0].eq_ignore_ascii_case(b"PGCACHE.STATS") {
                    shadow.encode(&a, &stats, &mut output);
                    if output.len() > MAX_OUTPUT {
                        output_limit.set(true);
                        break;
                    }
                    continue;
                } else {
                    staged.execute(&a, now, &mut *admit)
                };
                shadow.encode(&a, &reply, &mut output);
                if output.len() > MAX_OUTPUT {
                    output_limit.set(true);
                    break;
                }
            }
            vec![]
        },
        |mutations| {
            batch_limit = mutations.iter().map(|m| m.bytes()).sum::<usize>() > sink.pending_limit();
            !output_limit.get() && !batch_limit && sink.submit(mutations)
        },
    );
    if output_limit.get() {
        state.watches.clear();
        return Outcome::Reply(Reply::Error(
            "ERR transaction response exceeds maximum size",
        ));
    }
    if batch_limit {
        state.watches.clear();
        return Outcome::Reply(Reply::Error(
            "ERR transaction persistence batch exceeds maximum size",
        ));
    }
    if let Err(error) = result {
        if sink.accepted < i64::MAX as u64 {
            state.transaction = Some(tx);
            return Outcome::Deferred;
        }
        state.watches.clear();
        return Outcome::Reply(error);
    }
    *state = shadow;
    Outcome::Encoded(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn call(state: &mut State, args: &[&[u8]], engine: &mut Engine, sink: &mut Sink) -> Outcome {
        handle(state, args, engine, sink, &None, false).unwrap()
    }
    #[test]
    fn backlog_retry_preserves_queue_and_connection_state_until_admission() {
        let mut state = State::new();
        let mut engine = Engine::new(1024 * 1024);
        let (mut sink, receiver) = super::super::channel_with_limit(0, false, 800);
        engine.execute(&[b"SET", b"occupying", &vec![b'x'; 600]], 0, |m| {
            sink.submit(m)
        });
        call(&mut state, &[b"MULTI"], &mut engine, &mut sink);
        call(&mut state, &[b"SET", b"new", b"v"], &mut engine, &mut sink);
        call(
            &mut state,
            &[b"CLIENT", b"SETNAME", b"after"],
            &mut engine,
            &mut sink,
        );
        assert!(matches!(
            call(&mut state, &[b"EXEC"], &mut engine, &mut sink),
            Outcome::Deferred
        ));
        assert!(engine.snapshot(b"new").is_none());
        assert_eq!(state.execute(&[b"CLIENT", b"GETNAME"]), Reply::Bulk(None));
        assert_eq!(state.transaction.as_ref().unwrap().commands.len(), 2);
        sink.flush().unwrap();
        drop(receiver.recv().unwrap());
        assert!(matches!(
            call(&mut state, &[b"EXEC"], &mut engine, &mut sink),
            Outcome::Encoded(_)
        ));
        assert!(state.transaction.is_none());
        assert_eq!(
            state.execute(&[b"CLIENT", b"GETNAME"]),
            Reply::Bulk(Some(b"after".to_vec()))
        );
        assert_eq!(
            engine.execute(&[b"GET", b"new"], 0, |_| true),
            Reply::Bulk(Some(b"v".to_vec()))
        );
    }
    #[test]
    fn unblock_side_effects_wait_for_transaction_admission() {
        let mut state = State::new();
        state.blocked_clients.borrow_mut().insert(999, 1);
        let mut engine = Engine::new(1024 * 1024);
        let (mut sink, receiver) = super::super::channel_with_limit(0, false, 800);
        engine.execute(&[b"SET", b"occupying", &vec![b'x'; 600]], 0, |m| {
            sink.submit(m)
        });
        call(&mut state, &[b"MULTI"], &mut engine, &mut sink);
        call(
            &mut state,
            &[b"CLIENT", b"UNBLOCK", b"999", b"ERROR"],
            &mut engine,
            &mut sink,
        );
        call(
            &mut state,
            &[b"SET", b"new", b"value"],
            &mut engine,
            &mut sink,
        );
        assert!(matches!(
            call(&mut state, &[b"EXEC"], &mut engine, &mut sink),
            Outcome::Deferred
        ));
        assert!(state.unblocks.is_empty());
        assert!(state.blocked_clients.borrow().contains_key(&999));
        sink.flush().unwrap();
        drop(receiver.recv().unwrap());
        assert!(matches!(
            call(&mut state, &[b"EXEC"], &mut engine, &mut sink),
            Outcome::Encoded(_)
        ));
        assert_eq!(state.unblocks.get(&999), Some(&true));
        // The socket loop applies this queue; speculative execution never does.
        assert!(state.blocked_clients.borrow().contains_key(&999));
    }

    #[test]
    fn oversized_response_rejects_all_writes_and_clears_transaction() {
        let mut state = State::new();
        let mut engine = Engine::new(4 * 1024 * 1024);
        let (mut sink, _receiver) = super::super::channel_with_limit(0, false, 4 * 1024 * 1024);
        engine.execute(&[b"SET", b"big", &vec![b'x'; 600000]], 0, |_| true);
        call(&mut state, &[b"MULTI"], &mut engine, &mut sink);
        call(&mut state, &[b"SET", b"new", b"v"], &mut engine, &mut sink);
        for _ in 0..4 {
            call(&mut state, &[b"GET", b"big"], &mut engine, &mut sink);
        }
        assert!(matches!(
            call(&mut state, &[b"EXEC"], &mut engine, &mut sink),
            Outcome::Reply(Reply::Error(
                "ERR transaction response exceeds maximum size"
            ))
        ));
        assert!(engine.snapshot(b"new").is_none());
        assert!(state.transaction.is_none());
        assert_eq!(sink.accepted, 0);
    }
    #[test]
    fn queue_limits_dirty_transaction_without_executing_it() {
        let mut state = State::new();
        let mut engine = Engine::new(1024 * 1024);
        let (mut sink, _receiver) = super::super::channel_with_limit(0, false, 4096);
        call(&mut state, &[b"MULTI"], &mut engine, &mut sink);
        assert!(matches!(
            call(
                &mut state,
                &[b"SET", b"x", &vec![b'x'; MAX_FRAME]],
                &mut engine,
                &mut sink
            ),
            Outcome::Reply(Reply::Error(_))
        ));
        assert!(matches!(
            call(&mut state, &[b"EXEC"], &mut engine, &mut sink),
            Outcome::Reply(Reply::Error(
                "EXECABORT Transaction discarded because of previous errors."
            ))
        ));
        assert!(engine.snapshot(b"x").is_none());
    }
}
