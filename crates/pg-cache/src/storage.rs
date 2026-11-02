//! Tuple locations are disposable hints, never identities. Native updates validate
//! the visible row's key; stale hints fall back to a serialized SQL MERGE.
use pg_cache_core::{engine::Bytes, metrics::PersistenceMetrics, server::PendingWrites};
use pgrx::{bgworkers::BackgroundWorker, prelude::*};
use std::collections::HashMap;

const MAX_HINT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Default)]
pub struct Store {
    locations: HashMap<Bytes, pg_sys::ItemPointerData>,
    hint_bytes: usize,
}
impl Store {
    fn remember(&mut self, key: Bytes, tid: pg_sys::ItemPointerData) {
        if let Some(location) = self.locations.get_mut(&key) {
            *location = tid;
            return;
        }
        let cost = key.len() + 128;
        if cost > MAX_HINT_BYTES {
            return;
        }
        if self.hint_bytes + cost > MAX_HINT_BYTES {
            self.locations.clear();
            self.hint_bytes = 0;
        }
        self.hint_bytes += cost;
        self.locations.insert(key, tid);
    }
    fn forget(&mut self, key: &[u8]) {
        if self.locations.remove(key).is_some() {
            self.hint_bytes -= key.len() + 128;
        }
    }
    pub fn persist(&mut self, writes: &PendingWrites, metrics: &PersistenceMetrics) {
        #[cfg(feature = "journal-prototype")]
        if super::JOURNAL.get() {
            super::journal_worker::persist(writes, metrics);
            return;
        }
        if writes.is_empty() {
            return;
        }
        let started = std::time::Instant::now();
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut expirations = Vec::new();
        let mut locations = Vec::new();
        let mut deletes = Vec::new();
        for mutation in writes.mutations() {
            match &mutation.entry {
                Some(entry) => {
                    keys.push(&mutation.key);
                    values.push(entry.value.as_ref());
                    expirations.push(entry.expires_at_ms);
                    locations.push(self.locations.get(&mutation.key).copied());
                }
                None => deletes.push(mutation.key.as_ref()),
            }
        }
        let key_bytes: Vec<_> = keys.iter().map(|key| key.as_ref()).collect();
        let sequence = writes.sequence().unwrap() as i64;
        let prepare_us = started.elapsed().as_micros() as u64;
        let (sql_us, learned, native_rows) = BackgroundWorker::transaction(|| {
            let mut learned = Vec::new();
            let sql_started = std::time::Instant::now();
            Spi::run("SET LOCAL synchronous_commit = on").expect("synchronous commit");
            // Hash indexes do not enforce uniqueness. Serialize the read/modify/
            // insert decision; the in-memory batch already has distinct keys.
            Spi::run("LOCK TABLE cache_store.entries IN SHARE ROW EXCLUSIVE MODE")
                .expect("serialize cache writes");
            let mut updated = vec![false; keys.len()];
            if locations.iter().any(Option::is_some) {
                for (index, tid) in
                    super::heap::update_existing(&key_bytes, &values, &expirations, &locations)
                {
                    updated[index] = true;
                    learned.push((keys[index].clone(), tid));
                }
            }
            let mut missing_keys = Vec::new();
            let mut missing_values = Vec::new();
            let mut missing_expirations = Vec::new();
            for (index, was_updated) in updated.iter().enumerate() {
                if !was_updated {
                    missing_keys.push(key_bytes[index]);
                    missing_values.push(values[index]);
                    missing_expirations.push(expirations[index]);
                }
            }
            if !missing_keys.is_empty() {
                super::point_lookup::run(|| {
                    Spi::connect_mut(|client| {
                        // A SELECT around MERGE RETURNING avoids SPI status19,
                        // which pgrx0.16.1 does not yet recognize.
                        let rows = client.update(
                        "WITH changed AS (MERGE INTO cache_store.entries AS target USING (SELECT * FROM unnest($1::bytea[], $2::bytea[], $3::bigint[])) AS source(key,value,expires_at_ms) ON target.key=source.key WHEN MATCHED THEN UPDATE SET value=source.value,expires_at_ms=source.expires_at_ms WHEN NOT MATCHED THEN INSERT(key,value,expires_at_ms) VALUES(source.key,source.value,source.expires_at_ms) RETURNING target.key,target.ctid) SELECT key,ctid FROM changed",
                        None, &[missing_keys.into(), missing_values.into(), missing_expirations.into()],
                    ).expect("persist missing tuple hints");
                        for row in rows {
                            learned.push((
                                row.get::<Vec<u8>>(1).unwrap().unwrap().into(),
                                row.get::<pg_sys::ItemPointerData>(2).unwrap().unwrap(),
                            ));
                        }
                    })
                });
            }
            if !deletes.is_empty() {
                Spi::run_with_args(
                    "DELETE FROM cache_store.entries WHERE key=ANY($1::bytea[])",
                    &[deletes.clone().into()],
                )
                .expect("persist deletes");
            }
            Spi::run_with_args(
                "UPDATE cache_store.checkpoint SET sequence=$1 WHERE singleton",
                &[sequence.into()],
            )
            .expect("persist checkpoint");
            (
                sql_started.elapsed().as_micros() as u64,
                learned,
                updated.iter().filter(|updated| **updated).count() as u64,
            )
        });
        metrics
            .native_rows
            .fetch_add(native_rows, std::sync::atomic::Ordering::Relaxed);
        for (key, tid) in learned {
            self.remember(key, tid);
        }
        for key in &deletes {
            self.forget(key);
        }
        metrics.record(
            (keys.len() + deletes.len()) as u64,
            prepare_us,
            sql_us,
            started.elapsed().as_micros() as u64,
        );
    }
    pub fn expire(&mut self, now: i64) {
        #[cfg(feature = "journal-prototype")]
        if super::JOURNAL.get() {
            return; // Expired values are filtered during recovery and in memory.
        }
        let deleted = BackgroundWorker::transaction(|| {
            Spi::connect_mut(|client| {
                let rows = client.update("DELETE FROM cache_store.entries WHERE key IN (SELECT key FROM cache_store.entries WHERE expires_at_ms <= $1 LIMIT 1000) RETURNING key", None, &[now.into()]).expect("expire persisted values");
                rows.into_iter()
                    .map(|row| row.get::<Vec<u8>>(1).unwrap().unwrap())
                    .collect::<Vec<_>>()
            })
        });
        for key in deleted {
            self.forget(&key);
        }
    }
}
