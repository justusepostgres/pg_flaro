//! Optional coalescing handoff. A committing snapshot is never mutated; only
//! queued, not-yet-taken writes are replaced and their reservations released.
use crate::persistence::{Batch, PendingWrites};
use std::{
    sync::{mpsc, Arc, Condvar, Mutex},
    time::{Duration, Instant},
};
#[derive(Default)]
struct State {
    writes: PendingWrites,
    sender: bool,
    receiver: bool,
}
pub struct Mailbox {
    state: Mutex<State>,
    ready: Condvar,
}
pub enum Sender {
    Fifo(mpsc::Sender<Batch>),
    Coalescing(Arc<Mailbox>),
}
pub enum Receiver {
    Fifo(mpsc::Receiver<Batch>),
    Coalescing(Arc<Mailbox>),
}
pub fn channel(coalescing: bool) -> (Sender, Receiver) {
    if !coalescing {
        let (tx, rx) = mpsc::channel();
        return (Sender::Fifo(tx), Receiver::Fifo(rx));
    }
    let shared = Arc::new(Mailbox {
        state: Mutex::new(State {
            sender: true,
            receiver: true,
            ..State::default()
        }),
        ready: Condvar::new(),
    });
    (
        Sender::Coalescing(shared.clone()),
        Receiver::Coalescing(shared),
    )
}
impl Sender {
    pub fn send(&self, batch: Batch) -> Result<(), mpsc::SendError<Batch>> {
        match self {
            Self::Fifo(tx) => tx.send(batch),
            Self::Coalescing(shared) => {
                let mut state = shared.state.lock().expect("handoff mutex");
                if !state.receiver {
                    return Err(mpsc::SendError(batch));
                }
                state.writes.push(batch);
                drop(state);
                shared.ready.notify_one();
                Ok(())
            }
        }
    }
}
impl Drop for Sender {
    fn drop(&mut self) {
        if let Self::Coalescing(shared) = self {
            shared.state.lock().expect("handoff mutex").sender = false;
            shared.ready.notify_all();
        }
    }
}
impl Receiver {
    fn take(&self, timeout: Option<Duration>) -> Result<Batch, mpsc::RecvTimeoutError> {
        let Self::Coalescing(shared) = self else {
            unreachable!()
        };
        let deadline = timeout.map(|time| Instant::now() + time);
        let mut state = shared.state.lock().expect("handoff mutex");
        loop {
            if !state.writes.is_empty() {
                let writes = std::mem::take(&mut state.writes);
                drop(state);
                return Ok(writes.into_batch());
            }
            if !state.sender {
                return Err(mpsc::RecvTimeoutError::Disconnected);
            }
            state = if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(mpsc::RecvTimeoutError::Timeout);
                }
                shared
                    .ready
                    .wait_timeout(state, remaining)
                    .expect("handoff wait")
                    .0
            } else {
                shared.ready.wait(state).expect("handoff wait")
            };
        }
    }
    pub fn recv(&self) -> Result<Batch, mpsc::RecvError> {
        match self {
            Self::Fifo(rx) => rx.recv(),
            _ => self.take(None).map_err(|_| mpsc::RecvError),
        }
    }
    pub fn recv_timeout(&self, timeout: Duration) -> Result<Batch, mpsc::RecvTimeoutError> {
        match self {
            Self::Fifo(rx) => rx.recv_timeout(timeout),
            _ => self.take(Some(timeout)),
        }
    }
    pub fn try_recv(&self) -> Result<Batch, mpsc::TryRecvError> {
        match self {
            Self::Fifo(rx) => rx.try_recv(),
            _ => self
                .take(Some(Duration::ZERO))
                .map_err(|error| match error {
                    mpsc::RecvTimeoutError::Timeout => mpsc::TryRecvError::Empty,
                    mpsc::RecvTimeoutError::Disconnected => mpsc::TryRecvError::Disconnected,
                }),
        }
    }
    pub fn try_iter(&self) -> impl Iterator<Item = Batch> + '_ {
        std::iter::from_fn(|| self.try_recv().ok())
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        if let Self::Coalescing(shared) = self {
            let mut state = shared.state.lock().expect("handoff mutex");
            state.receiver = false;
            state.writes.clear();
        }
    }
}
