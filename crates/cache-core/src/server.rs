//! Network thread: never calls PostgreSQL or pgrx APIs.
use crate::{
    engine::{Engine, Mutation, Mutations},
    now_ms,
    protocol::{self, Reply},
};
use polling::{Event, Events, PollMode, Poller};
use std::{
    collections::{HashMap, HashSet},
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    ops::{Deref, DerefMut},
    os::fd::{AsFd, AsRawFd},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

pub const MAX_PENDING_BYTES: usize = 16 * 1024 * 1024;
const MAX_CLIENTS: usize = 128;
const MAX_OUTPUT: usize = 2 * protocol::MAX_FRAME;
const HANDOFF_BYTES: usize = 128 * 1024;
const HANDOFF_DELAY: Duration = Duration::from_millis(5);

use crate::handoff::{Receiver, Sender};
pub use crate::persistence::{Batch, PendingWrites};
#[cfg(test)]
use std::sync::mpsc;
pub struct Sink {
    tx: Sender,
    pending: Arc<AtomicUsize>,
    pending_limit: usize,
    pub committed: Arc<AtomicU64>,
    pub metrics: Arc<crate::metrics::PersistenceMetrics>,
    accepted: u64,
    staged: Vec<Mutation>,
    staged_bytes: usize,
    staged_since: Option<Instant>,
}
pub fn channel(sequence: u64) -> (Sink, Receiver) {
    channel_mode(sequence, false)
}
pub fn channel_mode(sequence: u64, coalescing: bool) -> (Sink, Receiver) {
    channel_with_limit(sequence, coalescing, MAX_PENDING_BYTES)
}
pub fn channel_with_limit(
    sequence: u64,
    coalescing: bool,
    pending_limit: usize,
) -> (Sink, Receiver) {
    assert!(pending_limit > 0);
    // Byte reservations bound queued AND locally staged mutations. Batching
    // handoffs avoids waking the persistence thread on every individual command.
    let (tx, rx) = crate::handoff::channel(coalescing);
    (
        Sink {
            tx,
            pending: Arc::new(AtomicUsize::new(0)),
            pending_limit,
            committed: Arc::new(AtomicU64::new(sequence)),
            metrics: Arc::default(),
            accepted: sequence,
            staged: Vec::new(),
            staged_bytes: 0,
            staged_since: None,
        },
        rx,
    )
}
impl Sink {
    fn submit(&mut self, mutations: Mutations) -> bool {
        let bytes = mutations.iter().map(Mutation::bytes).sum();
        let Some(sequence) = self
            .accepted
            .checked_add(1)
            .filter(|n| *n <= i64::MAX as u64)
        else {
            return false;
        };
        if self
            .pending
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                n.checked_add(bytes).filter(|n| *n <= self.pending_limit)
            })
            .is_err()
        {
            return false;
        }
        if self.staged.is_empty() {
            self.staged_since = Some(Instant::now());
        }
        self.staged.extend(mutations);
        self.staged_bytes += bytes;
        self.accepted = sequence;
        true
    }
    fn flush(&mut self) -> io::Result<()> {
        if self.staged.is_empty() {
            return Ok(());
        }
        let batch = Batch {
            sequence: self.accepted,
            mutations: std::mem::take(&mut self.staged),
            bytes: std::mem::take(&mut self.staged_bytes),
            pending: self.pending.clone(),
        };
        self.staged_since = None;
        self.tx
            .send(batch)
            .map_err(|_| io::Error::other("persistence worker disconnected"))
    }
    fn handoff_wait(&self) -> Duration {
        self.staged_since
            .map_or(Duration::from_millis(25), |since| {
                HANDOFF_DELAY.saturating_sub(since.elapsed())
            })
    }
    fn flush_if_due(&mut self) -> io::Result<()> {
        if self.staged_bytes >= HANDOFF_BYTES || self.handoff_wait().is_zero() {
            self.flush()?;
        }
        Ok(())
    }
    fn stats(&self) -> Reply {
        Reply::Bulk(Some(
            format!(
                "{{\"accepted_sequence\":{},\"committed_sequence\":{},\"pending_bytes\":{},\"flushes\":{},\"persisted_rows\":{},\"prepare_us\":{},\"sql_us\":{},\"persist_total_us\":{},\"max_flush_us\":{},\"max_sql_us\":{},\"max_non_sql_us\":{},\"native_rows\":{},\"journal_lock_us\":{},\"journal_max_lock_us\":{},\"journal_insert_us\":{},\"journal_max_insert_us\":{},\"journal_save_us\":{},\"journal_max_save_us\":{},\"journal_tx_overhead_us\":{},\"journal_max_tx_overhead_us\":{},\"journal_append_us\":{},\"journal_max_append_us\":{},\"journal_capacity_retries\":{},\"journal_help_compactions\":{},\"journal_capacity_us\":{},\"journal_max_capacity_us\":{}}}",
                self.accepted,
                self.committed.load(Ordering::Acquire),
                self.pending.load(Ordering::Relaxed),
                self.metrics.flushes.load(Ordering::Acquire),
                self.metrics.rows.load(Ordering::Relaxed),
                self.metrics.prepare_us.load(Ordering::Relaxed),
                self.metrics.sql_us.load(Ordering::Relaxed),
                self.metrics.total_us.load(Ordering::Relaxed),
                self.metrics.max_flush_us.load(Ordering::Relaxed),
                self.metrics.max_sql_us.load(Ordering::Relaxed),
                self.metrics.max_non_sql_us.load(Ordering::Relaxed),
                self.metrics.native_rows.load(Ordering::Relaxed),
                self.metrics.journal_lock_us.load(Ordering::Relaxed),
                self.metrics.journal_max_lock_us.load(Ordering::Relaxed),
                self.metrics.journal_insert_us.load(Ordering::Relaxed),
                self.metrics.journal_max_insert_us.load(Ordering::Relaxed),
                self.metrics.journal_save_us.load(Ordering::Relaxed),
                self.metrics.journal_max_save_us.load(Ordering::Relaxed),
                self.metrics.journal_tx_overhead_us.load(Ordering::Relaxed),
                self.metrics.journal_max_tx_overhead_us.load(Ordering::Relaxed),
                self.metrics.journal_append_us.load(Ordering::Relaxed),
                self.metrics.journal_max_append_us.load(Ordering::Relaxed),
                self.metrics.journal_capacity_retries.load(Ordering::Relaxed),
                self.metrics.journal_help_compactions.load(Ordering::Relaxed),
                self.metrics.journal_capacity_us.load(Ordering::Relaxed),
                self.metrics.journal_max_capacity_us.load(Ordering::Relaxed),

            )
            .into_bytes(),
        ))
    }
}
impl Drop for Sink {
    fn drop(&mut self) {
        self.pending.fetch_sub(self.staged_bytes, Ordering::Relaxed);
    }
}

// Registration owns the socket and unregisters it on every exit path.
struct RegisteredSocket<T: AsFd> {
    socket: T,
    poller: Arc<Poller>,
}
impl<T: AsFd + AsRawFd> RegisteredSocket<T> {
    fn new(socket: T, key: usize, poller: Arc<Poller>) -> io::Result<Self> {
        // SAFETY: Drop unregisters the source before the owned socket is closed.
        unsafe {
            poller.add_with_mode(&socket, Event::readable(key), PollMode::Level)?;
        }
        Ok(Self { socket, poller })
    }
}
impl<T: AsFd> Drop for RegisteredSocket<T> {
    fn drop(&mut self) {
        let _ = self.poller.delete(&self.socket);
    }
}
impl<T: AsFd> Deref for RegisteredSocket<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.socket
    }
}
impl<T: AsFd> DerefMut for RegisteredSocket<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.socket
    }
}

struct Client {
    socket: RegisteredSocket<TcpStream>,
    input: Vec<u8>,
    output: Vec<u8>,
    written: usize,
    closing: bool,
    writable: bool,
    readable: bool,
}
impl Client {
    fn commands(&mut self, engine: &mut Engine, sink: &mut Sink) -> io::Result<bool> {
        let mut consumed = 0;
        let mut blocked = false;
        loop {
            match protocol::parse(&self.input[consumed..]) {
                Ok(Some((args, n))) => {
                    if args.len() == 2 && args[0].eq_ignore_ascii_case(b"GET") {
                        engine.get_into(args[1], now_ms(), &mut self.output);
                        consumed += n;
                        if self.output.len() > MAX_OUTPUT {
                            return Err(io::Error::other("slow client output limit"));
                        }
                        continue;
                    }
                    #[cfg(feature = "io-diagnostics")]
                    if args.len() == 1 && args[0].eq_ignore_ascii_case(b"PGCACHE.IOSTATS") {
                        Reply::Bulk(Some(crate::io_diagnostics::snapshot().into_bytes()))
                            .encode(&mut self.output);
                        consumed += n;
                        if self.output.len() > MAX_OUTPUT {
                            return Err(io::Error::other("slow client output limit"));
                        }
                        continue;
                    }
                    let reply = if args[0].eq_ignore_ascii_case(b"PGCACHE.STATS") && args.len() == 1
                    {
                        sink.stats()
                    } else if args[0].eq_ignore_ascii_case(b"QUIT") && args.len() == 1 {
                        self.closing = true;
                        Reply::Simple("OK")
                    } else {
                        engine.execute(&args, now_ms(), |m| sink.submit(m))
                    };
                    if reply == Reply::Error("MISCONF persistence backlog full or unavailable")
                        && sink.accepted < i64::MAX as u64
                    {
                        // Admission has changed no state. Keep this command in
                        // the bounded input buffer until persistence makes room;
                        // read interest is disabled while it is deferred. Other
                        // connections can still read. Sequence exhaustion is
                        // permanent and must return an error instead of waiting.
                        blocked = true;
                        break;
                    }
                    consumed += n;
                    reply.encode(&mut self.output);
                    if self.output.len() > MAX_OUTPUT {
                        return Err(io::Error::other("slow client output limit"));
                    }
                    if self.closing {
                        break;
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    Reply::Error("ERR Protocol error").encode(&mut self.output);
                    self.closing = true;
                    break;
                }
            }
        }
        self.input.drain(..consumed);
        Ok(blocked)
    }
    fn read(&mut self, engine: &mut Engine, sink: &mut Sink) -> io::Result<bool> {
        let mut chunk = [0u8; 8192];
        let mut budget: usize = 64 * 1024;
        loop {
            if self.commands(engine, sink)? {
                return Ok(true);
            }
            if self.input.len() > protocol::MAX_FRAME {
                return Err(io::Error::other("input limit"));
            }
            if self.closing {
                break;
            }
            if budget == 0 {
                return Ok(false);
            }
            #[cfg(feature = "io-diagnostics")]
            let result = crate::io_diagnostics::measure(false, || self.socket.read(&mut chunk));
            #[cfg(not(feature = "io-diagnostics"))]
            let result = self.socket.read(&mut chunk);
            match result {
                Ok(0) => {
                    self.closing = true;
                    break;
                }
                Ok(n) => {
                    self.input.extend_from_slice(&chunk[..n]);
                    budget = budget.saturating_sub(n);
                    if n < chunk.len() {
                        // Level readiness reports unread or newly arriving bytes.
                        let blocked = self.commands(engine, sink)?;
                        if self.input.len() > protocol::MAX_FRAME {
                            return Err(io::Error::other("input limit"));
                        }
                        return Ok(blocked);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(false)
    }
    fn write(&mut self) -> io::Result<()> {
        while self.written < self.output.len() {
            #[cfg(feature = "io-diagnostics")]
            let result = crate::io_diagnostics::measure(true, || {
                self.socket.write(&self.output[self.written..])
            });
            #[cfg(not(feature = "io-diagnostics"))]
            let result = self.socket.write(&self.output[self.written..]);
            match result {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => self.written += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        self.output.clear();
        self.written = 0;
        Ok(())
    }
}

pub fn serve(
    address: SocketAddr,
    mut engine: Engine,
    mut sink: Sink,
    stop: Arc<AtomicBool>,
) -> io::Result<()> {
    #[cfg(feature = "io-diagnostics")]
    let _diagnostics = crate::io_diagnostics::Session::start();
    let listener = TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    let poll = Arc::new(Poller::new()?);
    let listener = RegisteredSocket::new(listener, 0, poll.clone())?;
    let mut events = Events::new();
    let mut clients = HashMap::<usize, Client>::new();
    let mut next = 1usize;
    let mut reaped = Instant::now();
    let mut deferred = HashSet::new();
    let mut ready = Vec::with_capacity(256);
    let mut serviced = Vec::with_capacity(256);
    while !stop.load(Ordering::Acquire) {
        events.clear();
        poll.wait(
            &mut events,
            Some(sink.handoff_wait().min(if deferred.is_empty() {
                Duration::from_millis(25)
            } else {
                Duration::from_millis(1)
            })),
        )?;
        if reaped.elapsed() >= Duration::from_secs(1) {
            engine.reap(now_ms());
            reaped = Instant::now();
        }
        ready.clear();
        ready.extend(deferred.drain().map(|t| (t, true)));
        ready.extend(events.iter().map(|e| (e.key, e.readable)));
        for &(token, readable) in &ready {
            if token == 0 {
                loop {
                    match listener.accept() {
                        Ok((socket, _)) => {
                            if clients.len() >= MAX_CLIENTS {
                                continue;
                            }
                            socket.set_nodelay(true)?;
                            let token = next;
                            next = next
                                .checked_add(1)
                                .ok_or_else(|| io::Error::other("token overflow"))?;
                            socket.set_nonblocking(true)?;
                            let socket = RegisteredSocket::new(socket, token, poll.clone())?;
                            clients.insert(
                                token,
                                Client {
                                    socket,
                                    input: Vec::new(),
                                    output: Vec::new(),
                                    written: 0,
                                    closing: false,
                                    writable: false,
                                    readable: true,
                                },
                            );
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                }
                continue;
            }
            let Some(client) = clients.get_mut(&token) else {
                continue;
            };
            let read_result = if readable && !client.closing {
                client.read(&mut engine, &mut sink)
            } else {
                Ok(false)
            };
            serviced.push((token, read_result));
        }
        // Small writes share a bounded staging window to avoid waking the
        // persistence thread on every socket poll. Reservations cover staging.
        sink.flush_if_due()?;
        for (token, read_result) in serviced.drain(..) {
            let Some(client) = clients.get_mut(&token) else {
                continue;
            };
            let result: io::Result<()> = (|| {
                let blocked = read_result?;
                if blocked {
                    deferred.insert(token);
                }
                client.write()?;
                if client.closing && client.output.is_empty() {
                    return Err(io::ErrorKind::ConnectionAborted.into());
                }
                let writable = !client.output.is_empty();
                let readable = !blocked;
                if writable != client.writable || readable != client.readable {
                    let mut interest = Event::none(token);
                    interest.readable = readable;
                    interest.writable = writable;
                    poll.modify_with_mode(&client.socket.socket, interest, PollMode::Level)?;
                    client.writable = writable;
                    client.readable = readable;
                }
                Ok(())
            })();
            if result.is_err() {
                deferred.remove(&token);
                clients.remove(&token);
            }
        }
    }
    sink.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Entry;

    #[test]
    fn local_staging_remains_bounded_when_a_consumer_releases_a_batch() {
        let (mut sink, rx) = channel_with_limit(0, false, 258);
        let change = || {
            smallvec::smallvec![Mutation {
                key: b"k".as_slice().into(),
                entry: None
            }]
        };
        assert!(sink.submit(change()));
        sink.flush().unwrap();
        assert!(sink.submit(change()));
        assert_eq!(sink.pending.load(Ordering::Relaxed), 258);
        assert!(!sink.submit(change()));
        let reader = std::thread::spawn(move || {
            drop(rx.recv().unwrap());
            rx
        });
        let rx = reader.join().unwrap();
        assert_eq!(sink.pending.load(Ordering::Relaxed), 129);
        assert!(sink.submit(change()));
        assert!(!sink.submit(change()));
        sink.flush().unwrap();
        let batch = rx.recv().unwrap();
        assert_eq!(batch.sequence, 3);
        assert_eq!(batch.accounted_bytes(), 258);
        let published = sink.pending.clone();
        drop(sink);
        assert_eq!(published.load(Ordering::Relaxed), 258);
        drop(batch);
        assert_eq!(published.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn dropping_staging_preserves_committing_reservation() {
        let (mut sink, rx) = channel_with_limit(0, false, 258);
        for _ in 0..2 {
            assert!(sink.submit(smallvec::smallvec![Mutation {
                key: b"k".as_slice().into(),
                entry: None
            }]));
            if sink.accepted == 1 {
                sink.flush().unwrap();
            }
        }
        let batch = rx.recv().unwrap();
        let published = sink.pending.clone();
        assert_eq!(sink.pending.load(Ordering::Relaxed), 258);
        drop(sink);
        assert_eq!(published.load(Ordering::Relaxed), 129);
        drop(batch);
        assert_eq!(published.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn small_handoffs_wait_for_deadline_but_large_handoffs_do_not() {
        let (mut sink, rx) = channel(0);
        assert!(sink.submit(smallvec::smallvec![Mutation {
            key: b"a".as_slice().into(),
            entry: None
        }]));
        sink.flush_if_due().unwrap();
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert_eq!(sink.pending.load(Ordering::Relaxed), 129);
        sink.staged_since = Some(Instant::now() - HANDOFF_DELAY);
        assert!(sink.handoff_wait().is_zero());
        sink.flush_if_due().unwrap();
        let first = rx.try_recv().unwrap();
        assert_eq!(first.sequence, 1);
        assert!(sink.staged_since.is_none());
        drop(first);
        assert!(sink.submit(smallvec::smallvec![Mutation {
            key: vec![0; HANDOFF_BYTES].into(),
            entry: None
        }]));
        sink.flush_if_due().unwrap();
        let second = rx.try_recv().unwrap();
        assert_eq!(second.sequence, 2);
        drop(second);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn pending_writes_release_superseded_values_and_keep_latest_state() {
        let (mut sink, rx) = channel(20);
        let change = |key: &[u8], value: Option<&[u8]>, expires_at_ms| Mutation {
            key: key.into(),
            entry: value.map(|value| Entry {
                value: value.into(),
                expires_at_ms,
            }),
        };
        assert!(sink.submit(smallvec::smallvec![change(b"a", Some(b"first"), None)]));
        assert!(sink.submit(smallvec::smallvec![change(b"a", None, None)]));
        sink.flush().unwrap();
        let mut writes = PendingWrites::default();
        writes.push(rx.recv().unwrap());
        assert_eq!(writes.len(), 1);
        assert_eq!(writes.accounted_bytes(), 129);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 129);
        assert!(writes.mutations().next().unwrap().entry.is_none());
        assert!(sink.submit(smallvec::smallvec![
            change(b"a", Some(b"last"), Some(123)),
            change(b"b", Some(b"second"), None),
        ]));
        sink.flush().unwrap();
        writes.push(rx.recv().unwrap());
        assert_eq!(writes.sequence(), Some(23));
        assert_eq!(writes.len(), 2);
        assert_eq!(writes.accounted_bytes(), 268);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 268);
        let a = writes.mutations().find(|m| m.key.as_ref() == b"a").unwrap();
        assert_eq!(a.entry.as_ref().unwrap().value.as_ref(), b"last");
        assert_eq!(a.entry.as_ref().unwrap().expires_at_ms, Some(123));
        assert_eq!(sink.committed.load(Ordering::Acquire), 20);
        writes.clear();
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
        assert!(writes.is_empty());
        assert_eq!(writes.sequence(), None);
    }

    #[test]
    fn queued_handoff_coalesces_while_committing_snapshot_is_held() {
        let (mut sink, rx) = channel_mode(0, true);
        let change = |value: Option<&[u8]>| Mutation {
            key: b"key".as_slice().into(),
            entry: value.map(|value| Entry {
                value: value.into(),
                expires_at_ms: None,
            }),
        };
        assert!(sink.submit(smallvec::smallvec![change(Some(b"old"))]));
        sink.flush().unwrap();
        let committing = rx.recv().unwrap();
        for _ in 0..10000 {
            assert!(sink.submit(smallvec::smallvec![change(Some(b"new"))]));
            sink.flush().unwrap();
            assert_eq!(sink.pending.load(Ordering::Relaxed), 268);
        }
        assert!(sink.submit(smallvec::smallvec![change(None)]));
        sink.flush().unwrap();
        assert_eq!(
            committing.mutations[0]
                .entry
                .as_ref()
                .unwrap()
                .value
                .as_ref(),
            b"old"
        );
        let queued = rx.recv().unwrap();
        assert_eq!(queued.sequence, 10002);
        assert_eq!(queued.mutations.len(), 1);
        assert!(queued.mutations[0].entry.is_none());
        assert_eq!(sink.pending.load(Ordering::Relaxed), 265);
        drop(queued);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 134);
        drop(committing);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn configured_pending_limit_is_enforced_across_staging_and_queue() {
        let (mut sink, rx) = channel_with_limit(0, true, 258);
        let deletion = |key: &[u8]| Mutation {
            key: key.into(),
            entry: None,
        };
        assert!(sink.submit(smallvec::smallvec![deletion(b"a")]));
        sink.flush().unwrap();
        assert!(sink.submit(smallvec::smallvec![deletion(b"b")]));
        assert!(!sink.submit(smallvec::smallvec![deletion(b"c")]));
        assert_eq!(sink.accepted, 2);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 258);
        drop(rx);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 129);
        drop(sink);
    }

    #[test]
    fn coalescing_handoff_disconnect_releases_queue_and_wakes_reader() {
        let (mut sink, rx) = channel_mode(0, true);
        assert!(matches!(
            rx.recv_timeout(Duration::ZERO),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(sink.submit(smallvec::smallvec![Mutation {
            key: b"k".as_slice().into(),
            entry: None
        }]));
        sink.flush().unwrap();
        drop(rx);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
        assert!(sink.submit(smallvec::smallvec![Mutation {
            key: b"k".as_slice().into(),
            entry: None
        }]));
        assert!(sink.flush().is_err());
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
        let (sink, rx) = channel_mode(0, true);
        let reader = std::thread::spawn(move || rx.recv_timeout(Duration::from_secs(2)));
        drop(sink);
        assert!(matches!(
            reader.join().unwrap(),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
    }

    #[test]
    fn committing_snapshot_retains_its_reservation_and_value() {
        let (mut sink, rx) = channel(0);
        let mut committing = PendingWrites::default();
        let mut next = PendingWrites::default();
        for (snapshot, value) in [(&mut committing, b"old"), (&mut next, b"new")] {
            assert!(sink.submit(smallvec::smallvec![Mutation {
                key: b"key".as_slice().into(),
                entry: Some(Entry {
                    value: value.as_slice().into(),
                    expires_at_ms: None
                }),
            }]));
            sink.flush().unwrap();
            snapshot.push(rx.recv().unwrap());
        }
        assert_eq!(sink.pending.load(Ordering::Relaxed), 268);
        assert_eq!(
            committing
                .mutations()
                .next()
                .unwrap()
                .entry
                .as_ref()
                .unwrap()
                .value
                .as_ref(),
            b"old"
        );
        drop(committing);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 134);
        assert_eq!(next.sequence(), Some(2));
        drop(next);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn grouped_handoff_keeps_sequence_and_byte_reservations() {
        let (mut sink, rx) = channel(7);
        for key in [b"a".as_slice(), b"b".as_slice()] {
            assert!(sink.submit(smallvec::smallvec![Mutation {
                key: key.into(),
                entry: None
            }]));
        }
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert_eq!(sink.pending.load(Ordering::Relaxed), 258);
        sink.flush().unwrap();
        let batch = rx.recv().unwrap();
        assert_eq!(batch.sequence, 9);
        assert_eq!(batch.mutations.len(), 2);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 258);
        drop(batch);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn backlog_limit_and_disconnection_release_reservations() {
        let (mut sink, rx) = channel(0);
        let change = || {
            smallvec::smallvec![Mutation {
                key: vec![0; MAX_PENDING_BYTES / 2].into(),
                entry: None,
            }]
        };
        assert!(sink.submit(change()));
        assert!(!sink.submit(change()));
        sink.flush().unwrap();
        drop(rx.recv().unwrap());
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
        drop(rx);
        assert!(sink.submit(change()));
        assert!(sink.flush().is_err());
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
        assert_eq!(sink.accepted, 2);
    }
}
