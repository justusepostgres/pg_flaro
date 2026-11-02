//! Development harness only. Acknowledged writes are NOT persisted.
use pg_cache_core::{engine::Engine, server};
use std::sync::{atomic::AtomicBool, Arc};
fn main() -> std::io::Result<()> {
    let port = std::env::args().nth(1).unwrap_or_else(|| "6380".into());
    let (sink, rx) = server::channel(0);
    let committed = sink.committed.clone();
    std::thread::spawn(move || {
        while let Ok(batch) = rx.recv() {
            committed.store(batch.sequence, std::sync::atomic::Ordering::Release);
        }
    });
    let cluster = std::env::var("PG_CACHE_CLUSTER_CONFIG").ok().map(|json| {
        let topology = pg_cache_core::cluster::Config::parse(&json)
            .expect("cluster config")
            .bootstrap()
            .unwrap();
        let (tx, rx) = std::sync::mpsc::sync_channel::<pg_cache_core::cluster::Save>(1);
        std::thread::spawn(move || {
            while let Ok(save) = rx.recv() {
                let _ = save.done.send(());
            }
        });
        pg_cache_core::cluster::Cluster::new(topology, tx)
    });
    eprintln!("memory-only development server on 127.0.0.1:{port}; no persistence");
    server::serve_cluster(
        format!("127.0.0.1:{port}").parse().expect("port"),
        Engine::new(64 * 1024 * 1024),
        sink,
        Arc::new(AtomicBool::new(false)),
        cluster,
    )
}
