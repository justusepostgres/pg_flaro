mod heap;
#[cfg(feature = "journal-prototype")]
mod journal_copy;
#[cfg(feature = "journal-prototype")]
mod journal_store;
#[cfg(feature = "journal-prototype")]
mod journal_worker;
mod storage;
use pg_cache_core::{
    engine::{Engine, Entry},
    now_ms,
    server::{self, PendingWrites},
};
use pgrx::{
    bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags},
    guc::{GucContext, GucFlags, GucRegistry, GucSetting},
    prelude::*,
};
use std::{
    ffi::CString,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
pgrx::pg_module_magic!();

#[cfg(feature = "journal-prototype")]
static JOURNAL_PENDING_MB: GucSetting<i32> = GucSetting::<i32>::new(16);
#[cfg(feature = "journal-prototype")]
static JOURNAL: GucSetting<bool> = GucSetting::<bool>::new(false);

static DATABASE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"postgres"));
static LISTEN_ADDRESS: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"127.0.0.1"));
static PORT: GucSetting<i32> = GucSetting::<i32>::new(6380);
static MEMORY_MB: GucSetting<i32> = GucSetting::<i32>::new(64);
static FLUSH_MS: GucSetting<i32> = GucSetting::<i32>::new(100);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    #[cfg(feature = "journal-prototype")]
    GucRegistry::define_int_guc(
        c"pg_cache.journal_pending_mb",
        c"Experimental journal pending accounting limit",
        c"Requires restart; must fit one journal segment",
        &JOURNAL_PENDING_MB,
        16,
        128,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    #[cfg(feature = "journal-prototype")]
    GucRegistry::define_bool_guc(
        c"pg_cache.journal",
        c"Experimental journal persistence",
        c"Requires restart and prototype schema",
        &JOURNAL,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_cache.database",
        c"Persistence database",
        c"Requires restart",
        &DATABASE,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_cache.listen_address",
        c"IP address for the RESP listener",
        c"Requires restart; defaults to loopback",
        &LISTEN_ADDRESS,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    for (name, description, setting, default_min, max) in [
        (c"pg_cache.port", c"RESP port", &PORT, 1024, 65535),
        (
            c"pg_cache.memory_mb",
            c"Cache accounting limit in MiB",
            &MEMORY_MB,
            1,
            16384,
        ),
        (
            c"pg_cache.flush_ms",
            c"Target write-behind flush interval",
            &FLUSH_MS,
            1,
            60000,
        ),
    ] {
        GucRegistry::define_int_guc(
            name,
            description,
            c"Requires restart",
            setting,
            default_min,
            max,
            GucContext::Postmaster,
            GucFlags::default(),
        );
    }
    if unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
        #[cfg(feature = "journal-prototype")]
        if JOURNAL.get() {
            BackgroundWorkerBuilder::new("pg_cache_compactor")
                .set_library("pg_cache")
                .set_function("pg_cache_compactor_main")
                .enable_spi_access()
                .set_restart_time(Some(Duration::from_secs(5)))
                .load();
        }
        BackgroundWorkerBuilder::new("pg_cache")
            .set_library("pg_cache")
            .set_function("pg_cache_main")
            .enable_spi_access()
            .set_restart_time(Some(Duration::from_secs(5)))
            .load();
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_cache_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    let database = DATABASE.get().expect("database");
    BackgroundWorker::connect_worker_to_spi(Some(database.to_str().expect("database UTF8")), None);
    // CREATE EXTENSION can be issued after preload; do not open the Redis port until schema exists.
    loop {
        let ready = BackgroundWorker::transaction(|| {
            #[cfg(feature = "journal-prototype")]
            if JOURNAL.get() {
                return journal_worker::ready();
            }
            Spi::get_one::<bool>("SELECT to_regclass('cache_store.checkpoint') IS NOT NULL")
                .unwrap()
                .unwrap()
        });
        if ready {
            break;
        }
        if !BackgroundWorker::wait_latch(Some(Duration::from_secs(1))) {
            return;
        }
    }
    let (engine, sequence) = BackgroundWorker::transaction(|| {
        #[cfg(feature = "journal-prototype")]
        if JOURNAL.get() {
            return journal_store::restore(
                MEMORY_MB.get() as usize * 1024 * 1024,
                JOURNAL_PENDING_MB.get() as usize * 1024 * 1024,
            );
        }
        let mut engine = Engine::new(MEMORY_MB.get() as usize * 1024 * 1024);
        let now = now_ms();
        Spi::connect(|client| {
            let mut cursor = client.open_cursor(
                "SELECT key,value,expires_at_ms FROM cache_store.entries",
                &[],
            );
            loop {
                let rows = cursor.fetch(256).expect("restore cursor");
                if rows.is_empty() {
                    break;
                }
                for row in rows {
                    engine
                        .restore(
                            row.get::<Vec<u8>>(1).unwrap().unwrap(),
                            Entry {
                                value: row.get::<Vec<u8>>(2).unwrap().unwrap().into(),
                                expires_at_ms: row.get::<i64>(3).unwrap(),
                            },
                            now,
                        )
                        .expect("restore cache");
                }
            }
        });
        let sequence =
            Spi::get_one::<i64>("SELECT sequence FROM cache_store.checkpoint WHERE singleton")
                .unwrap()
                .expect("checkpoint") as u64;
        (engine, sequence)
    });
    #[cfg(feature = "journal-prototype")]
    let (sink, rx) = server::channel_with_limit(
        sequence,
        JOURNAL.get(),
        if JOURNAL.get() {
            JOURNAL_PENDING_MB.get() as usize * 1024 * 1024
        } else {
            server::MAX_PENDING_BYTES
        },
    );
    #[cfg(not(feature = "journal-prototype"))]
    let (sink, rx) = server::channel(sequence);
    let committed = sink.committed.clone();
    let metrics = sink.metrics.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let network_stop = stop.clone();
    let listen_address = LISTEN_ADDRESS.get().expect("listen address");
    let ip: std::net::IpAddr = listen_address
        .to_str()
        .expect("listen address UTF8")
        .parse()
        .expect("pg_cache.listen_address must be an IPv4 or IPv6 address");
    let address = std::net::SocketAddr::new(ip, PORT.get() as u16);
    // Inherit a blocked signal mask in the Rust thread. PostgreSQL's asynchronous
    // signal handlers must run on the worker's main thread, never the network thread.
    let network = unsafe {
        let mut blocked: libc::sigset_t = std::mem::zeroed();
        let mut previous: libc::sigset_t = std::mem::zeroed();
        libc::sigfillset(&mut blocked);
        for signal in [libc::SIGSEGV, libc::SIGBUS, libc::SIGILL, libc::SIGFPE] {
            libc::sigdelset(&mut blocked, signal);
        }
        assert_eq!(
            libc::pthread_sigmask(libc::SIG_BLOCK, &blocked, &mut previous),
            0
        );
        let result = std::thread::Builder::new()
            .name("pg-cache-resp".into())
            .spawn(move || server::serve(address, engine, sink, network_stop));
        assert_eq!(
            libc::pthread_sigmask(libc::SIG_SETMASK, &previous, std::ptr::null_mut()),
            0
        );
        result.expect("spawn network thread")
    };
    let interval = Duration::from_millis(FLUSH_MS.get() as u64);
    let mut store = storage::Store::default();
    let mut writes = PendingWrites::default();
    let mut last_flush = std::time::Instant::now();
    let mut cleanup = std::time::Instant::now();
    let mut stats_report = std::time::Instant::now();
    while BackgroundWorker::wait_latch(Some(Duration::ZERO)) {
        if network.is_finished() {
            break;
        }
        // The channel wakes the writer immediately under load. Waiting only on a
        // PostgreSQL latch until flush_ms would impose an artificial queue/interval
        // throughput ceiling. Check postmaster/shutdown signals at least every 10 ms.
        let wait = if writes.is_empty() {
            Duration::from_millis(10)
        } else {
            interval
                .saturating_sub(last_flush.elapsed())
                .min(Duration::from_millis(10))
        };
        if let Ok(batch) = rx.recv_timeout(wait) {
            writes.push(batch);
        }
        for batch in rx.try_iter().take(1024) {
            writes.push(batch);
            if writes.len() >= 8192 || writes.accounted_bytes() >= 2 * 1024 * 1024 {
                break;
            }
        }
        if writes.len() >= 8192
            || writes.accounted_bytes() >= 2 * 1024 * 1024
            || last_flush.elapsed() >= interval
        {
            store.persist(&writes, &metrics);
            if let Some(sequence) = writes.sequence() {
                committed.store(sequence, Ordering::Release);
            }
            writes.clear();
            last_flush = std::time::Instant::now();
        }
        if stats_report.elapsed() >= Duration::from_secs(1) {
            // Background workers must publish their local table counters themselves.
            unsafe {
                pg_sys::pgstat_report_stat(true);
            }
            stats_report = std::time::Instant::now();
        }
        if cleanup.elapsed() >= Duration::from_secs(60) {
            store.expire(now_ms());
            cleanup = std::time::Instant::now();
        }
    }
    stop.store(true, Ordering::Release);
    network
        .join()
        .expect("network thread panic")
        .expect("network server");
    // No new commands can enter; flush everything accepted before clean shutdown.
    for batch in rx.try_iter() {
        writes.push(batch);
    }
    store.persist(&writes, &metrics);
}
