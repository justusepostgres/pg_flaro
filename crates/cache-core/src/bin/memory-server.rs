//! Development harness only. Acknowledged writes are NOT persisted.
use pg_cache_core::{engine::Engine, server};
use std::sync::{atomic::AtomicBool, Arc};
fn main() -> std::io::Result<()> {
    let port = std::env::args().nth(1).unwrap_or_else(|| "6380".into());
    let (sink, rx) = server::channel(0);
    std::thread::spawn(move || while rx.recv().is_ok() {});
    eprintln!("memory-only development server on 127.0.0.1:{port}; no persistence");
    server::serve(
        format!("127.0.0.1:{port}").parse().expect("port"),
        Engine::new(64 * 1024 * 1024),
        sink,
        Arc::new(AtomicBool::new(false)),
    )
}
