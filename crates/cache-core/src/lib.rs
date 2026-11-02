pub mod cluster;
mod collections;
pub mod engine;
mod handoff;
#[cfg(feature = "io-diagnostics")]
mod io_diagnostics;
pub mod journal;
pub mod metrics;
mod persistence;
pub mod protocol;
pub mod server;
pub mod value;

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before Unix epoch")
        .as_millis() as i64
}
