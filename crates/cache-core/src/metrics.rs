//! Diagnostic counters written once per successful persistence transaction.
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct PersistenceMetrics {
    pub flushes: AtomicU64,
    pub rows: AtomicU64,
    pub native_rows: AtomicU64,
    pub journal_lock_us: AtomicU64,
    pub journal_max_lock_us: AtomicU64,
    pub journal_insert_us: AtomicU64,
    pub journal_max_insert_us: AtomicU64,
    pub journal_save_us: AtomicU64,
    pub journal_max_save_us: AtomicU64,
    pub journal_tx_overhead_us: AtomicU64,
    pub journal_max_tx_overhead_us: AtomicU64,
    pub journal_append_us: AtomicU64,
    pub journal_max_append_us: AtomicU64,
    pub journal_capacity_retries: AtomicU64,
    pub journal_help_compactions: AtomicU64,
    pub journal_capacity_us: AtomicU64,
    pub journal_max_capacity_us: AtomicU64,
    pub prepare_us: AtomicU64,
    pub sql_us: AtomicU64,
    pub total_us: AtomicU64,
    pub max_flush_us: AtomicU64,
    pub max_sql_us: AtomicU64,
    pub max_non_sql_us: AtomicU64,
}

impl PersistenceMetrics {
    pub fn record(&self, rows: u64, prepare_us: u64, sql_us: u64, total_us: u64) {
        self.rows.fetch_add(rows, Ordering::Relaxed);
        self.prepare_us.fetch_add(prepare_us, Ordering::Relaxed);
        self.sql_us.fetch_add(sql_us, Ordering::Relaxed);
        self.total_us.fetch_add(total_us, Ordering::Relaxed);
        self.max_flush_us.fetch_max(total_us, Ordering::Relaxed);
        self.max_sql_us.fetch_max(sql_us, Ordering::Relaxed);
        self.max_non_sql_us
            .fetch_max(total_us.saturating_sub(sql_us), Ordering::Relaxed);
        self.flushes.fetch_add(1, Ordering::Release);
    }
}
