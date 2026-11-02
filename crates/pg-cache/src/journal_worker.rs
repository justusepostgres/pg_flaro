//! Experimental writer/compactor integration. No PostgreSQL API runs on the
//! network thread. Durability advances only after the append transaction commits.
use crate::journal_store::{
    append, journal_proto_compact, journal_proto_rotate, AppendBatch, AppendTiming,
};
use pg_cache_core::{metrics::PersistenceMetrics, server::PendingWrites};
use pgrx::{
    bgworkers::{BackgroundWorker, SignalWakeFlags},
    prelude::*,
};
use std::{
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

pub(crate) fn schema_ready() -> bool {
    Spi::get_one::<bool>("SELECT to_regclass('cache_journal.state') IS NOT NULL AND to_regclass('cache_journal.entries') IS NOT NULL AND to_regclass('cache_journal.segment0') IS NOT NULL AND to_regclass('cache_journal.segment1') IS NOT NULL").unwrap().unwrap()
}

fn ready() -> bool {
    schema_ready() && Spi::get_one::<bool>("SELECT NOT pg_is_in_recovery() AND EXISTS(SELECT FROM pg_attribute WHERE attrelid=to_regclass('cache_journal.entries') AND attname='data_type' AND NOT attisdropped)").unwrap().unwrap()
}

pub(crate) fn persist(writes: &PendingWrites, metrics: &PersistenceMetrics) {
    if writes.is_empty() {
        return;
    }
    let started = Instant::now();
    let mutations: Vec<_> = writes.mutations().cloned().collect();
    let batches: Vec<_> = pg_cache_core::journal::batch_ranges(&mutations)
        .expect("bounded snapshot records")
        .into_iter()
        .map(|range| AppendBatch::new(&mutations[range]))
        .collect();
    let prepare_us = started.elapsed().as_micros() as u64;
    let sql_started = Instant::now();
    let mut capacity_us = 0;
    loop {
        let append_started = Instant::now();
        let (appended, timing) = BackgroundWorker::transaction(|| {
            let mut timing = AppendTiming::default();
            let appended = append(writes.sequence().unwrap() as i64, &batches, &mut timing);
            (appended, timing)
        });
        let append_us = append_started.elapsed().as_micros() as u64;
        let overhead = append_us.saturating_sub(timing.lock_us + timing.insert_us + timing.save_us);
        for (total, peak, elapsed) in [
            (
                &metrics.journal_lock_us,
                &metrics.journal_max_lock_us,
                timing.lock_us,
            ),
            (
                &metrics.journal_insert_us,
                &metrics.journal_max_insert_us,
                timing.insert_us,
            ),
            (
                &metrics.journal_save_us,
                &metrics.journal_max_save_us,
                timing.save_us,
            ),
            (
                &metrics.journal_tx_overhead_us,
                &metrics.journal_max_tx_overhead_us,
                overhead,
            ),
        ] {
            total.fetch_add(elapsed, Ordering::Relaxed);
            peak.fetch_max(elapsed, Ordering::Relaxed);
        }

        metrics
            .journal_append_us
            .fetch_add(append_us, Ordering::Relaxed);
        metrics
            .journal_max_append_us
            .fetch_max(append_us, Ordering::Relaxed);
        if appended {
            break;
        }
        metrics
            .journal_capacity_retries
            .fetch_add(1, Ordering::Relaxed);
        let capacity_started = Instant::now();
        // Capacity remains bounded. Help reclaim a sealed slot if the independent
        // compactor has stopped, including shutdown where worker exit order is
        // unspecified. A live compactor keeps its transaction advisory lock;
        // polling here never holds the metadata row lock between transactions.
        if !BackgroundWorker::transaction(journal_proto_compact) {
            std::thread::sleep(Duration::from_millis(2));
        } else {
            metrics
                .journal_help_compactions
                .fetch_add(1, Ordering::Relaxed);
        }
        capacity_us += capacity_started.elapsed().as_micros() as u64;
    }
    metrics
        .journal_capacity_us
        .fetch_add(capacity_us, Ordering::Relaxed);
    metrics
        .journal_max_capacity_us
        .fetch_max(capacity_us, Ordering::Relaxed);
    metrics.record(
        mutations.len() as u64,
        prepare_us,
        sql_started.elapsed().as_micros() as u64,
        started.elapsed().as_micros() as u64,
    );
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_cache_compactor_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    let database = crate::DATABASE.get().expect("database");
    BackgroundWorker::connect_worker_to_spi(Some(database.to_str().expect("database UTF8")), None);
    let mut last_seal = Instant::now();
    while BackgroundWorker::wait_latch(Some(Duration::from_millis(10))) {
        if !BackgroundWorker::transaction(ready) {
            continue;
        }
        BackgroundWorker::transaction(journal_proto_compact);
        // Seal partial batches periodically; avoid rewriting every key at the
        // writer's short commit cadence. Full segments rotate during append.
        if last_seal.elapsed() >= Duration::from_secs(1) {
            BackgroundWorker::transaction(journal_proto_rotate);
            last_seal = Instant::now();
        }
        unsafe {
            pg_sys::pgstat_report_stat(true);
        }
    }
}
