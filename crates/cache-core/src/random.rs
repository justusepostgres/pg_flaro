use std::{collections::hash_map::RandomState, hash::BuildHasher};

// A randomly seeded keyed hash supplies draws without a dependency or shared
// process-global C RNG. Rejection sampling removes modulo bias.
pub(crate) struct Random {
    state: RandomState,
    counter: u64,
}
impl Random {
    pub(crate) fn new() -> Self {
        Self {
            state: RandomState::new(),
            counter: 0,
        }
    }
    pub(crate) fn below(&mut self, upper: usize) -> usize {
        let upper = upper as u64;
        let threshold = upper.wrapping_neg() % upper;
        loop {
            let value = self.state.hash_one(self.counter);
            self.counter = self.counter.wrapping_add(1);
            if value >= threshold {
                return (value % upper) as usize;
            }
        }
    }
}
