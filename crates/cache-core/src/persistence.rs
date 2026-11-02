//! Owned write-behind snapshots. Reservations include queued and committing data.
use crate::engine::{Bytes, Mutation};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

pub struct Batch {
    pub sequence: u64,
    pub mutations: Vec<Mutation>,
    pub(crate) bytes: usize,
    pub(crate) pending: Arc<AtomicUsize>,
}
impl Batch {
    pub fn accounted_bytes(&self) -> usize {
        self.bytes
    }
}
impl Drop for Batch {
    fn drop(&mut self) {
        self.pending.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[derive(Default)]
pub struct PendingWrites {
    latest: HashMap<Bytes, Mutation>,
    pending: Option<Arc<AtomicUsize>>,
    bytes: usize,
    collections: usize,
    sequence: Option<u64>,
}
impl PendingWrites {
    pub fn push(&mut self, mut batch: Batch) {
        if let Some(pending) = &self.pending {
            assert!(
                Arc::ptr_eq(pending, &batch.pending),
                "mixed persistence channels"
            );
        } else {
            self.pending = Some(batch.pending.clone());
        }
        assert!(
            self.sequence.is_none_or(|n| batch.sequence > n),
            "out-of-order batch"
        );
        self.sequence = Some(batch.sequence);
        // Transfer ownership of reservations before the incoming batch is dropped.
        self.bytes += std::mem::take(&mut batch.bytes);
        for mutation in batch.mutations.drain(..) {
            self.collections +=
                usize::from(mutation.entry.as_ref().is_some_and(|e| e.value.kind() != 0));
            if let Some(old) = self.latest.insert(mutation.key.clone(), mutation) {
                self.collections -=
                    usize::from(old.entry.as_ref().is_some_and(|e| e.value.kind() != 0));
                let released = old.bytes();
                drop(old);
                self.bytes -= released;
                batch.pending.fetch_sub(released, Ordering::Relaxed);
            }
        }
    }
    pub(crate) fn into_batch(mut self) -> Batch {
        Batch {
            sequence: self.sequence.take().expect("nonempty handoff"),
            mutations: std::mem::take(&mut self.latest).into_values().collect(),
            bytes: std::mem::take(&mut self.bytes),
            pending: self.pending.take().expect("handoff reservation"),
        }
    }
    pub fn mutations(&self) -> impl Iterator<Item = &Mutation> {
        self.latest.values()
    }
    pub fn sequence(&self) -> Option<u64> {
        self.sequence
    }
    pub fn len(&self) -> usize {
        self.latest.len()
    }
    pub fn is_empty(&self) -> bool {
        self.latest.is_empty()
    }
    pub fn accounted_bytes(&self) -> usize {
        self.bytes
    }
    pub fn flush_threshold(&self, pending_limit: usize) -> usize {
        if self.collections == 0 {
            2 * 1024 * 1024
        } else {
            pending_limit / 4
        }
    }
    pub fn clear(&mut self) {
        self.collections = 0;
        self.latest.clear();
        if let Some(pending) = &self.pending {
            pending.fetch_sub(std::mem::take(&mut self.bytes), Ordering::Relaxed);
        }
        self.sequence = None;
    }
}
impl Drop for PendingWrites {
    fn drop(&mut self) {
        self.clear();
    }
}
