//! Opt-in wall-time diagnostics. Instrumented runs are not throughput evidence.
use std::{cell::RefCell, io, time::Instant};

#[derive(Default, Debug)]
struct Calls {
    calls: u64,
    bytes: u64,
    would_block: u64,
    zero: u64,
    errors: u64,
    total_ns: u128,
    max_ns: u128,
    // <=1us, <=10us, <=100us, <=1ms, <=10ms, >10ms.
    duration_buckets: [u64; 6],
}
impl Calls {
    fn record(&mut self, result: &io::Result<usize>, ns: u128) {
        self.calls += 1;
        self.total_ns += ns;
        self.max_ns = self.max_ns.max(ns);
        let bucket = [1_000, 10_000, 100_000, 1_000_000, 10_000_000]
            .iter()
            .position(|limit| ns <= *limit)
            .unwrap_or(5);
        self.duration_buckets[bucket] += 1;
        match result {
            Ok(0) => self.zero += 1,
            Ok(n) => self.bytes += *n as u64,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => self.would_block += 1,
            Err(_) => self.errors += 1,
        }
    }
}
thread_local! {
    static COUNTERS: RefCell<[Calls; 2]> = RefCell::new(Default::default());
}
pub fn measure(write: bool, call: impl FnOnce() -> io::Result<usize>) -> io::Result<usize> {
    let start = Instant::now();
    let result = call();
    let ns = start.elapsed().as_nanos();
    COUNTERS.with(|c| c.borrow_mut()[usize::from(write)].record(&result, ns));
    result
}
pub fn snapshot() -> String {
    COUNTERS.with(|c| {
        let c = c.borrow();
        format!("read={:?} write={:?}", c[0], c[1])
    })
}
pub struct Session;
impl Session {
    pub fn start() -> Self {
        COUNTERS.with(|c| *c.borrow_mut() = Default::default());
        Self
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        COUNTERS.with(|c| {
            let c = c.borrow();
            eprintln!("PG_CACHE_IO_DIAGNOSTICS read={:?} write={:?}", c[0], c[1]);
        });
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn classifies_results_and_bucket_boundaries() {
        let mut c = Calls::default();
        for ns in [
            1_000, 1_001, 10_000, 10_001, 100_000, 100_001, 1_000_000, 1_000_001, 10_000_000,
            10_000_001,
        ] {
            c.record(&Ok(7), ns);
        }
        c.record(&Ok(0), 0);
        c.record(&Err(io::ErrorKind::WouldBlock.into()), 0);
        c.record(&Err(io::ErrorKind::Interrupted.into()), 0);
        assert_eq!(c.duration_buckets, [4, 2, 2, 2, 2, 1]);
        assert_eq!(
            (c.calls, c.bytes, c.zero, c.would_block, c.errors),
            (13, 70, 1, 1, 1)
        );
        assert_eq!(c.max_ns, 10_000_001);
        assert_eq!(c.total_ns, 22_222_005);
    }
}
