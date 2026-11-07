//! Network thread: never calls PostgreSQL or pgrx APIs.
mod client_state;
mod transaction;
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
    track_ready: bool,
    ready_collections: Vec<crate::engine::Bytes>,
    tx: Sender,
    pending: Arc<AtomicUsize>,
    pending_limit: usize,
    pub committed: Arc<AtomicU64>,
    pub metrics: Arc<crate::metrics::PersistenceMetrics>,
    accepted: u64,
    staged: Vec<Mutation>,
    staged_bytes: usize,
    staged_costs: Vec<usize>,
    deferred_collections: HashSet<usize>,
    staged_collections: HashMap<crate::engine::Bytes, usize>,
    staged_since: Option<Instant>,
    collection_delay: Duration,
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
            track_ready: false,
            ready_collections: Vec::new(),
            tx,
            pending: Arc::new(AtomicUsize::new(0)),
            pending_limit,
            committed: Arc::new(AtomicU64::new(sequence)),
            metrics: Arc::default(),
            accepted: sequence,
            staged: Vec::new(),
            staged_bytes: 0,
            staged_costs: Vec::new(),
            deferred_collections: HashSet::new(),
            staged_collections: HashMap::new(),
            staged_since: None,
            collection_delay: HANDOFF_DELAY,
        },
        rx,
    )
}
impl Sink {
    pub fn set_collection_delay(&mut self, delay: Duration) {
        self.collection_delay = delay.max(HANDOFF_DELAY);
    }
    pub fn pending_limit(&self) -> usize {
        self.pending_limit
    }

    pub(crate) fn reserve_collection(&mut self, key: &[u8], bytes: usize) -> bool {
        if let Some(&index) = self.staged_collections.get(key) {
            let Some(sequence) = self
                .accepted
                .checked_add(1)
                .filter(|n| *n <= i64::MAX as u64)
            else {
                return false;
            };
            let previous = self.staged_costs[index];
            if self
                .pending
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    n.checked_sub(previous)
                        .and_then(|n| n.checked_add(bytes))
                        .filter(|n| *n <= self.pending_limit)
                })
                .is_err()
            {
                return false;
            }
            self.staged[index].entry = None;
            self.staged_costs[index] = bytes;
            self.deferred_collections.insert(index);
            self.staged_bytes = self.staged_bytes - previous + bytes;
            self.accepted = sequence;
            return true;
        }
        self.stage(
            smallvec::smallvec![Mutation {
                key: key.into(),
                entry: None
            }],
            Some(bytes),
        )
    }
    pub(crate) fn submit(&mut self, mutations: Mutations) -> bool {
        self.stage(mutations, None)
    }
    fn stage(&mut self, mutations: Mutations, deferred_bytes: Option<usize>) -> bool {
        // Engine mutation lists contain unique keys. Once a key has a deferred
        // snapshot, all later writes to that key replace it until the handoff.
        let bytes: usize =
            deferred_bytes.unwrap_or_else(|| mutations.iter().map(Mutation::bytes).sum());
        let reclaimed: usize = mutations
            .iter()
            .filter_map(|m| self.staged_collections.get(&m.key))
            .map(|i| self.staged_costs[*i])
            .sum();
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
                n.checked_sub(reclaimed)
                    .and_then(|n| n.checked_add(bytes))
                    .filter(|n| *n <= self.pending_limit)
            })
            .is_err()
        {
            return false;
        }
        if self.staged.is_empty() {
            self.staged_since = Some(Instant::now());
        }
        for mutation in mutations {
            if self.track_ready
                && mutation
                    .entry
                    .as_ref()
                    .is_some_and(|e| matches!(e.value.kind(), 2 | 4))
            {
                self.ready_collections.push(mutation.key.clone());
            }
            let cost = deferred_bytes.unwrap_or_else(|| mutation.bytes());
            let typed = deferred_bytes.is_some()
                || mutation.entry.as_ref().is_some_and(|e| e.value.kind() != 0);
            let index = self.staged_collections.get(&mutation.key).copied();
            let index = if let Some(index) = index {
                self.staged[index] = mutation;
                self.staged_costs[index] = cost;
                index
            } else {
                let index = self.staged.len();
                if typed {
                    self.staged_collections.insert(mutation.key.clone(), index);
                }
                self.staged.push(mutation);
                self.staged_costs.push(cost);
                index
            };
            if deferred_bytes.is_some() {
                self.deferred_collections.insert(index);
            } else {
                self.deferred_collections.remove(&index);
            }
        }
        self.staged_bytes = self.staged_bytes - reclaimed + bytes;
        self.accepted = sequence;
        true
    }
    pub(crate) fn barrier(&mut self, engine: &Engine) -> io::Result<()> {
        self.capture(engine);
        self.flush()?;
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.committed.load(Ordering::Acquire) < self.accepted {
            if Instant::now() >= deadline {
                return Err(io::Error::other(
                    "persistence barrier timed out; restart required",
                ));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }
    fn capture(&mut self, engine: &Engine) {
        for index in self.deferred_collections.drain() {
            let mutation = &mut self.staged[index];
            mutation.entry = engine.snapshot(&mutation.key);
            let actual = mutation.bytes();
            assert!(
                actual <= self.staged_costs[index],
                "unreserved collection growth"
            );
            let released = self.staged_costs[index] - actual;
            self.pending.fetch_sub(released, Ordering::Relaxed);
            self.staged_bytes -= released;
            self.staged_costs[index] = actual;
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        assert!(
            self.deferred_collections.is_empty(),
            "capture collections before handoff"
        );
        self.staged_costs.clear();
        if self.staged.is_empty() {
            return Ok(());
        }
        self.staged_collections.clear();
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
                let delay = if self.staged_collections.len() == self.staged.len() {
                    self.collection_delay
                } else {
                    HANDOFF_DELAY
                };
                delay.saturating_sub(since.elapsed())
            })
    }
    fn flush_if_due(&mut self, engine: &Engine) -> io::Result<()> {
        let threshold = if self.staged_collections.is_empty() {
            HANDOFF_BYTES
        } else {
            self.pending_limit / 4
        };
        if self.staged_bytes >= threshold || self.handoff_wait().is_zero() {
            self.capture(engine);
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

static NEXT_WAITER: AtomicU64 = AtomicU64::new(1);
struct Waiting {
    cancelled: Option<bool>,
    kind: u8,
    keys: Vec<crate::engine::Bytes>,
    order: u64,
    deadline: i64,
}
struct Client {
    waiting: Option<Waiting>,
    yield_commands: bool,
    state: client_state::State,
    socket: RegisteredSocket<TcpStream>,
    input: Vec<u8>,
    output: Vec<u8>,
    written: usize,
    closing: bool,
    writable: bool,
    readable: bool,
    asking: bool,
}
impl Client {
    fn commands(
        &mut self,
        engine: &mut Engine,
        sink: &mut Sink,
        cluster: &mut Option<crate::cluster::Cluster>,
        ready_collections: &HashMap<crate::engine::Bytes, u64>,
    ) -> io::Result<bool> {
        if self.waiting.is_some() {
            let mut byte = [0];
            match self.socket.peek(&mut byte) {
                Ok(0) => return Err(io::ErrorKind::ConnectionAborted.into()),
                Err(e)
                    if e.kind() != io::ErrorKind::WouldBlock
                        && e.kind() != io::ErrorKind::Interrupted =>
                {
                    return Err(e)
                }
                _ => {}
            }
        }
        let mut consumed = 0;
        let mut blocked = false;
        loop {
            let mut inline_storage = Vec::new();
            match protocol::parse_command(&self.input[consumed..], &mut inline_storage) {
                Ok(Some((args, n))) => {
                    if args.is_empty() {
                        consumed += n;
                        continue;
                    }
                    if let Some(error) = self.waiting.as_ref().and_then(|w| w.cancelled) {
                        let reply = if error {
                            Reply::Error("UNBLOCKED client unblocked via CLIENT UNBLOCK")
                        } else {
                            crate::engine::blocking::null(args[0])
                        };
                        self.state.encode(&args, &reply, &mut self.output);
                        self.waiting = None;
                        self.asking = false;
                        consumed += n;
                        if self.yield_commands {
                            blocked = true;
                            break;
                        }
                        continue;
                    }
                    engine.expire_hash_fields(now_ms());
                    if let Some(outcome) = transaction::handle(
                        &mut self.state,
                        &args,
                        engine,
                        sink,
                        cluster,
                        self.asking,
                    ) {
                        match outcome {
                            transaction::Outcome::Deferred => {
                                blocked = true;
                                break;
                            }
                            transaction::Outcome::Reply(reply) => {
                                self.state.encode(&args, &reply, &mut self.output)
                            }
                            transaction::Outcome::Encoded(bytes) => {
                                self.output.extend_from_slice(&bytes)
                            }
                        }
                        if self.state.transaction.is_none() {
                            self.asking = false;
                        }
                        consumed += n;
                        if self.output.len() > MAX_OUTPUT {
                            return Err(io::Error::other("slow client output limit"));
                        }
                        if self.yield_commands {
                            blocked = true;
                            break;
                        }
                        continue;
                    }
                    if self.state.protocol == 2
                        && cluster.is_none()
                        && args.len() == 2
                        && args[0].eq_ignore_ascii_case(b"GET")
                    {
                        engine.get_into(args[1], now_ms(), &mut self.output);
                        consumed += n;
                        if self.output.len() > MAX_OUTPUT {
                            return Err(io::Error::other("slow client output limit"));
                        }
                        if self.yield_commands {
                            blocked = true;
                            break;
                        }
                        continue;
                    }
                    let asking = self.asking;
                    // ASKING covers the next command or transaction; transaction
                    // lifetime is handled above.
                    self.asking = false;
                    let special = if args[0].eq_ignore_ascii_case(b"ASKING") && args.len() == 1 {
                        if cluster.is_some() {
                            self.asking = true;
                            Some(Reply::Simple("OK"))
                        } else {
                            Some(Reply::Error("ERR cluster support disabled"))
                        }
                    } else if args[0].eq_ignore_ascii_case(b"HELLO") {
                        Some(self.state.hello(&args, cluster.is_some()))
                    } else if args[0].eq_ignore_ascii_case(b"RESET") {
                        Some(if args.len() == 1 {
                            self.state.reset()
                        } else {
                            Reply::Error("ERR wrong number of arguments for 'reset' command")
                        })
                    } else if args[0].eq_ignore_ascii_case(b"TIME") {
                        Some(if args.len() == 1 {
                            client_state::time()
                        } else {
                            Reply::Error("ERR wrong number of arguments for 'time' command")
                        })
                    } else if args[0].eq_ignore_ascii_case(b"CLIENT") {
                        Some(self.state.execute(&args))
                    } else if args[0].eq_ignore_ascii_case(b"COMMAND") {
                        Some(crate::cluster::command(&args))
                    } else if let Some(cluster) = cluster {
                        cluster
                            .execute(&args, engine, sink)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                            .or_else(|| cluster.route(&args, asking, engine, now_ms()))
                    } else if args[0].eq_ignore_ascii_case(b"CLUSTER") {
                        Some(Reply::Error("ERR cluster support disabled"))
                    } else {
                        None
                    };
                    if let Some(reply) = special {
                        if reply == Reply::Error("MISCONF persistence backlog full or unavailable")
                            && sink.accepted < i64::MAX as u64
                        {
                            self.asking = asking;
                            blocked = true;
                            break;
                        }
                        self.waiting = None;
                        self.state.encode(&args, &reply, &mut self.output);
                        consumed += n;
                        if self.output.len() > MAX_OUTPUT {
                            return Err(io::Error::other("slow client output limit"));
                        }
                        if self.yield_commands {
                            blocked = true;
                            break;
                        }
                        continue;
                    }
                    if self.state.protocol == 2
                        && args.len() == 2
                        && args[0].eq_ignore_ascii_case(b"GET")
                    {
                        engine.get_into(args[1], now_ms(), &mut self.output);
                        consumed += n;
                        if self.output.len() > MAX_OUTPUT {
                            return Err(io::Error::other("slow client output limit"));
                        }
                        if self.yield_commands {
                            blocked = true;
                            break;
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
                        if self.yield_commands {
                            blocked = true;
                            break;
                        }
                        continue;
                    }
                    let now = now_ms();
                    let pop = crate::engine::blocking::supported(args[0]);
                    let timed_out = self
                        .waiting
                        .as_ref()
                        .is_some_and(|w| w.deadline != 0 && now >= w.deadline);
                    let ready_key = self.waiting.as_ref().and_then(|waiting| {
                        waiting
                            .keys
                            .iter()
                            .filter(|key| {
                                ready_collections.contains_key(*key)
                                    && engine
                                        .get(key, now)
                                        .is_some_and(|e| e.value.kind() == waiting.kind)
                            })
                            .min_by_key(|key| ready_collections.get(*key).copied())
                            .cloned()
                    });
                    let reply = if timed_out {
                        crate::engine::blocking::null(args[0])
                    } else if self.waiting.is_some() {
                        if ready_key.is_some() {
                            // Redis 8.2 re-executes the original command when any
                            // watched collection becomes ready, preserving argument order.
                            engine.execute(&args, now, |m| sink.submit(m))
                        } else {
                            crate::engine::blocking::null(args[0])
                        }
                    } else if args[0].eq_ignore_ascii_case(b"PGCACHE.STATS") && args.len() == 1 {
                        sink.stats()
                    } else if args[0].eq_ignore_ascii_case(b"QUIT") {
                        self.closing = true;
                        Reply::Simple("OK")
                    } else {
                        engine
                            .execute_collection_in_place(&args, now, sink)
                            .unwrap_or_else(|| engine.execute(&args, now, |m| sink.submit(m)))
                    };
                    if reply == Reply::Error("MISCONF persistence backlog full or unavailable")
                        && sink.accepted < i64::MAX as u64
                    {
                        // Admission has changed no state. Keep this command in
                        // the bounded input buffer until persistence makes room;
                        // read interest is disabled while it is deferred. Other
                        // connections can still read. Sequence exhaustion is
                        // permanent and must return an error instead of waiting.
                        self.asking = asking;
                        blocked = true;
                        break;
                    }
                    if pop && !timed_out && reply == crate::engine::blocking::null(args[0]) {
                        if self.waiting.is_none() {
                            self.waiting = Some(Waiting {
                                cancelled: None,
                                kind: crate::engine::blocking::kind(args[0]),
                                keys: crate::engine::blocking::waiting_keys(&args)
                                    .iter()
                                    .map(|key| crate::engine::Bytes::from(*key))
                                    .collect(),
                                order: NEXT_WAITER.fetch_add(1, Ordering::Relaxed),
                                deadline: crate::engine::blocking::deadline(&args, now)
                                    .expect("validated blocking timeout"),
                            });
                        }
                        self.asking = asking;
                        blocked = true;
                        break;
                    }
                    self.waiting = None;
                    consumed += n;
                    self.state.encode(&args, &reply, &mut self.output);
                    if self.output.len() > MAX_OUTPUT {
                        return Err(io::Error::other("slow client output limit"));
                    }
                    if self.closing {
                        break;
                    }
                    if self.yield_commands {
                        blocked = true;
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    Reply::OwnedError(format!("ERR Protocol error: {error}"))
                        .encode(&mut self.output);
                    self.closing = true;
                    break;
                }
            };
        }
        self.input.drain(..consumed);
        Ok(blocked)
    }
    fn read(
        &mut self,
        engine: &mut Engine,
        sink: &mut Sink,
        cluster: &mut Option<crate::cluster::Cluster>,
        ready_collections: &HashMap<crate::engine::Bytes, u64>,
    ) -> io::Result<bool> {
        let mut chunk = [0u8; 8192];
        let mut budget: usize = 64 * 1024;
        loop {
            if self.commands(engine, sink, cluster, ready_collections)? {
                return Ok(true);
            }
            if self.input.len() > protocol::MAX_TRANSFER_FRAME {
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
                        let blocked = self.commands(engine, sink, cluster, ready_collections)?;
                        if self.input.len() > protocol::MAX_TRANSFER_FRAME {
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
    engine: Engine,
    sink: Sink,
    stop: Arc<AtomicBool>,
) -> io::Result<()> {
    serve_cluster(address, engine, sink, stop, None)
}
pub fn serve_cluster(
    address: SocketAddr,
    mut engine: Engine,
    mut sink: Sink,
    stop: Arc<AtomicBool>,
    mut cluster: Option<crate::cluster::Cluster>,
) -> io::Result<()> {
    #[cfg(feature = "io-diagnostics")]
    let _diagnostics = crate::io_diagnostics::Session::start();
    let listener = TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    let poll = Arc::new(Poller::new()?);
    let listener = RegisteredSocket::new(listener, 0, poll.clone())?;
    let mut events = Events::new();
    let mut clients = HashMap::<usize, Client>::new();
    let blocked_clients: client_state::BlockedClients = Default::default();
    let mut next = 1usize;
    let mut reaped = Instant::now();
    let mut deferred = HashSet::new();
    let mut ready = Vec::with_capacity(256);
    let mut serviced = Vec::with_capacity(256);
    let mut retry_now = false;
    let mut ready_collections = HashMap::<crate::engine::Bytes, u64>::new();
    let mut ready_order = 0u64;
    while !stop.load(Ordering::Acquire) {
        events.clear();
        poll.wait(
            &mut events,
            Some(sink.handoff_wait().min(if retry_now {
                Duration::ZERO
            } else if deferred.is_empty() {
                Duration::from_millis(25)
            } else {
                Duration::from_millis(1)
            })),
        )?;
        retry_now = false;
        if reaped.elapsed() >= Duration::from_secs(1) {
            engine.reap(now_ms());
            reaped = Instant::now();
        }
        ready.clear();
        ready.extend(deferred.drain().map(|t| (t, true)));
        ready.extend(events.iter().map(|e| (e.key, e.readable)));
        // Existing waiters run in arrival order before new readers. Duplicate
        // poll/deferred events must not let one connection run twice per round.
        ready.sort_by_key(|(token, _)| {
            clients
                .get(token)
                .and_then(|c| c.waiting.as_ref())
                .map_or((1, 0, *token as u64), |w| {
                    let first_ready = w
                        .keys
                        .iter()
                        .filter_map(|k| ready_collections.get(k))
                        .min()
                        .copied()
                        .unwrap_or(u64::MAX);
                    (0, first_ready, w.order)
                })
        });
        ready.dedup_by_key(|(token, _)| *token);
        let mut waiter_count = clients.values().filter(|c| c.waiting.is_some()).count();
        for (ready_index, &(token, readable)) in ready.iter().enumerate() {
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
                                    waiting: None,
                                    yield_commands: false,
                                    state: {
                                        let mut state = client_state::State::new();
                                        state.blocked_clients = blocked_clients.clone();
                                        state
                                    },
                                    socket,
                                    input: Vec::new(),
                                    output: Vec::new(),
                                    written: 0,
                                    closing: false,
                                    writable: false,
                                    readable: true,
                                    asking: false,
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
            let has_waiters = waiter_count != 0;
            let accepted_before = sink.accepted;
            let Some(client) = clients.get_mut(&token) else {
                continue;
            };
            let was_waiting = client.waiting.is_some();
            client.yield_commands = has_waiters;
            sink.track_ready = has_waiters;
            let read_result = if readable && !client.closing {
                client.read(&mut engine, &mut sink, &mut cluster, &ready_collections)
            } else {
                Ok(false)
            };
            if read_result
                .as_ref()
                .is_err_and(|e| e.kind() == io::ErrorKind::InvalidData)
            {
                return read_result.map(|_| ());
            }
            let waiting_changed = was_waiting != client.waiting.is_some();
            if waiting_changed {
                if was_waiting {
                    waiter_count -= 1;
                    blocked_clients.borrow_mut().remove(&client.state.id);
                } else {
                    waiter_count += 1;
                    blocked_clients.borrow_mut().insert(client.state.id, token);
                }
            }
            let unblocks = std::mem::take(&mut client.state.unblocks);
            for (id, error) in unblocks {
                if let Some(target) = blocked_clients.borrow_mut().remove(&id) {
                    if let Some(waiting) = clients.get_mut(&target).and_then(|c| c.waiting.as_mut())
                    {
                        waiting.cancelled = Some(error);
                        deferred.insert(target);
                        retry_now = true;
                    }
                }
            }
            serviced.push((token, read_result));
            if waiting_changed
                || (has_waiters && sink.accepted != accepted_before)
                || !sink.ready_collections.is_empty()
            {
                let interests: HashSet<_> = clients
                    .values()
                    .filter_map(|c| c.waiting.as_ref())
                    .flat_map(|w| w.keys.iter().cloned())
                    .collect();
                for key in sink.ready_collections.drain(..) {
                    if interests.contains(&key) {
                        ready_collections.entry(key).or_insert_with(|| {
                            ready_order += 1;
                            ready_order
                        });
                    }
                }
                ready_collections.retain(|key, _| {
                    interests.contains(key)
                        && engine
                            .get(key, now_ms())
                            .is_some_and(|e| matches!(e.value.kind(), 2 | 4))
                });
            }
            if has_waiters && sink.accepted != accepted_before {
                // Revisit waiters before the writer's next pipelined command or
                // a newly arrived consumer can take the value just produced.
                deferred.extend(ready[ready_index + 1..].iter().map(|(token, _)| *token));
                retry_now = true;
                break;
            }
        }
        // Small writes share a bounded staging window to avoid waking the
        // persistence thread on every socket poll. Reservations cover staging.
        sink.flush_if_due(&engine)?;
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
                if let Some(client) = clients.remove(&token) {
                    blocked_clients.borrow_mut().remove(&client.state.id);
                }
            }
        }
        if !clients.values().any(|c| c.waiting.is_some()) {
            ready_collections.clear();
        }
    }
    sink.capture(&engine);
    sink.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transaction_handoff_is_one_coherent_snapshot_in_both_modes() {
        for coalescing in [false, true] {
            let mut engine = Engine::new(1024 * 1024);
            let (mut sink, receiver) = channel_with_limit(0, coalescing, 1024 * 1024);
            // A staged collection snapshot must be replaced by the final
            // transaction value without double-counting its reservation.
            let initial: &[&[u8]] = &[b"HSET", b"hash", b"f", b"before"];
            engine.execute(initial, 0, |m| sink.submit(m));
            let commands: &[&[&[u8]]] = &[
                &[b"HSET", b"hash", b"f", b"during"],
                &[b"HSET", b"hash", b"f", b"after"],
                &[b"SET", b"other", b"value"],
            ];
            engine
                .transact(
                    |staged, admit| {
                        commands
                            .iter()
                            .map(|a| staged.execute(a, 1, &mut *admit))
                            .collect()
                    },
                    |m| sink.submit(m),
                )
                .unwrap();
            assert_eq!(sink.accepted, 2);
            sink.capture(&engine);
            sink.flush().unwrap();
            let batch = receiver.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(batch.sequence, 2);
            assert_eq!(batch.mutations.len(), 2);
            assert_eq!(
                batch.accounted_bytes(),
                batch.mutations.iter().map(Mutation::bytes).sum::<usize>()
            );
            for mutation in &batch.mutations {
                assert_eq!(mutation.entry, engine.snapshot(&mutation.key));
            }
            assert!(receiver.try_recv().is_err());
            drop(batch);
            assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
        }
    }

    use crate::engine::Entry;

    #[test]
    fn collection_handoff_captures_at_configured_interval() {
        let (mut sink, rx) = channel(0);
        sink.set_collection_delay(Duration::from_millis(100));
        let mut live = Engine::new(1 << 20);
        live.execute(&[b"HSET", b"h", b"f", b"old"], 1000, |m| sink.submit(m));
        live.execute_collection_in_place(&[b"HSET", b"h", b"f", b"new"], 1000, &mut sink);
        assert!(sink.handoff_wait() > Duration::from_millis(50));
        sink.flush_if_due(&live).unwrap();
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        sink.staged_since = Some(Instant::now() - Duration::from_millis(101));
        sink.flush_if_due(&live).unwrap();
        let batch = rx.recv().unwrap();
        assert_eq!(batch.sequence, 2);
        assert_eq!(batch.mutations[0].entry, live.snapshot(b"h"));
        drop(batch);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn deferred_mutations_match_generic_execution_and_keep_committing_snapshots() {
        let (mut sink, rx) = channel(0);
        let mut live = Engine::new(32 << 20);
        let mut reference = Engine::new(32 << 20);
        let commands: Vec<Vec<&[u8]>> = vec![
            vec![b"HSET", b"h", b"f", b"initial"],
            vec![b"HSET", b"h", b"f", b"next"],
            vec![b"HSET", b"h", b"new", b"longer"],
            vec![b"RPUSH", b"l", b"a", b"b"],
            vec![b"LSET", b"l", b"-1", b"replacement"],
            vec![b"LSET", b"l", b"0", b""],
            vec![b"SADD", b"s", b"a", b"b"],
            vec![b"SREM", b"s", b"a"],
            vec![b"SADD", b"s", b"new"],
            vec![b"SADD", b"s", b"new"],
            vec![b"ZADD", b"z", b"1", b"a"],
            vec![b"ZINCRBY", b"z", b"2", b"a"],
            vec![b"ZINCRBY", b"z", b"-1", b"b"],
            vec![b"PEXPIRE", b"h", b"500"],
            vec![b"HSET", b"h", b"f", b"ttl"],
            vec![b"LSET", b"l", b"90", b"bad"],
            vec![b"ZINCRBY", b"z", b"nan", b"a"],
        ];
        for args in &commands {
            let expected = reference.execute(args, 1000, |_| true);
            let actual = live
                .execute_collection_in_place(args, 1000, &mut sink)
                .unwrap_or_else(|| live.execute(args, 1000, |m| sink.submit(m)));
            assert_eq!(actual, expected, "{args:?}");
            for key in [b"h", b"l", b"s", b"z"] {
                assert_eq!(live.snapshot(key), reference.snapshot(key));
                if let Some(entry) = live.snapshot(key) {
                    assert_eq!(entry.value.len(), entry.value.as_ref().len());
                    assert_eq!(
                        entry.value.memory_bytes(),
                        reference.snapshot(key).unwrap().value.memory_bytes()
                    );
                }
            }
        }
        sink.capture(&live);
        sink.flush().unwrap();
        let committing = rx.recv().unwrap();
        let before: Vec<_> = committing
            .mutations
            .iter()
            .map(|m| m.entry.clone())
            .collect();
        for args in &commands {
            live.execute_collection_in_place(args, 1000, &mut sink)
                .unwrap_or_else(|| live.execute(args, 1000, |m| sink.submit(m)));
        }
        // Replacements, deletion and expiration must all cancel or capture the
        // correct deferred state, including multi-key generic commands.
        live.execute(&[b"MSET", b"h", b"plain", b"l", b"plain"], 1000, |m| {
            sink.submit(m)
        });
        live.execute(&[b"DEL", b"z"], 1000, |m| sink.submit(m));
        live.execute(&[b"PEXPIRE", b"s", b"1"], 1000, |m| sink.submit(m));
        live.execute_collection_in_place(&[b"SADD", b"s", b"expires"], 1000, &mut sink);
        live.reap(1002);
        sink.capture(&live);
        sink.flush().unwrap();
        let latest = rx.recv().unwrap();
        assert!(latest
            .mutations
            .iter()
            .any(|m| m.key.as_ref() == b"s" && m.entry.is_none()));
        assert_eq!(
            before,
            committing
                .mutations
                .iter()
                .map(|m| m.entry.clone())
                .collect::<Vec<_>>()
        );
        drop(committing);
        drop(latest);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn deferred_admission_rejection_leaves_live_collection_unchanged() {
        let mut live = Engine::new(1 << 20);
        live.execute(&[b"HSET", b"h", b"f", b"old"], 1000, |_| true);
        let before = live.snapshot(b"h");
        let (mut sink, _rx) = channel_with_limit(0, false, 1);
        let result = live
            .execute_collection_in_place(&[b"HSET", b"h", b"f", b"new"], 1000, &mut sink)
            .unwrap();
        assert_eq!(
            result,
            Reply::Error("MISCONF persistence backlog full or unavailable")
        );
        assert_eq!(live.snapshot(b"h"), before);
        assert_eq!(sink.accepted, 0);
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn collection_staging_replaces_only_superseded_snapshots() {
        let make = |value: &[u8]| Mutation {
            key: b"k".as_slice().into(),
            entry: Some(Entry {
                value: crate::value::Value::from_collection(crate::value::Collection::Hash(
                    [(
                        crate::engine::Bytes::from(b"f".as_slice()),
                        crate::engine::Bytes::from(value),
                    )]
                    .into_iter()
                    .collect(),
                ))
                .unwrap(),
                expires_at_ms: None,
            }),
        };
        let first = make(b"first");
        let last = make(b"last");
        let (mut sink, rx) = channel_with_limit(0, false, first.bytes());
        assert!(sink.submit(smallvec::smallvec![first.clone()]));
        assert!(sink.submit(smallvec::smallvec![last.clone()]));
        assert_eq!(sink.pending.load(Ordering::Relaxed), last.bytes());
        sink.flush().unwrap();
        let batch = rx.recv().unwrap();
        assert_eq!(batch.sequence, 2);
        assert_eq!(batch.mutations.len(), 1);
        assert_eq!(batch.mutations[0].entry, last.entry);
        assert!(!sink.submit(smallvec::smallvec![first.clone()]));
        drop(batch);
        assert!(sink.submit(smallvec::smallvec![first]));
        sink.flush().unwrap();
        drop(rx.recv().unwrap());
        assert_eq!(sink.pending.load(Ordering::Relaxed), 0);

        let (mut sink, rx) = channel(0);
        assert!(sink.submit(smallvec::smallvec![make(b"old")]));
        assert!(sink.submit(smallvec::smallvec![Mutation {
            key: b"k".as_slice().into(),
            entry: None
        }]));
        assert!(sink.submit(smallvec::smallvec![make(b"new")]));
        sink.flush().unwrap();
        let batch = rx.recv().unwrap();
        assert_eq!(batch.mutations.len(), 1);
        assert_eq!(batch.mutations[0].entry, make(b"new").entry);
    }

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
        sink.flush_if_due(&Engine::new(1024)).unwrap();
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert_eq!(sink.pending.load(Ordering::Relaxed), 129);
        sink.staged_since = Some(Instant::now() - HANDOFF_DELAY);
        assert!(sink.handoff_wait().is_zero());
        sink.flush_if_due(&Engine::new(1024)).unwrap();
        let first = rx.try_recv().unwrap();
        assert_eq!(first.sequence, 1);
        assert!(sink.staged_since.is_none());
        drop(first);
        assert!(sink.submit(smallvec::smallvec![Mutation {
            key: vec![0; HANDOFF_BYTES].into(),
            entry: None
        }]));
        sink.flush_if_due(&Engine::new(1024)).unwrap();
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
