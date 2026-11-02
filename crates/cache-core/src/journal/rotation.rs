//! Pure transaction plans for two bounded logged segments. Not yet wired to SQL.
//!
//! Apply a returned state only after the PostgreSQL transaction commits. Append
//! commits its payload and metadata together. Finish commits materialized rows,
//! segment retirement, and the materialized watermark together. A sealed segment
//! is immutable while the other segment accepts writes; its ticket rejects stale
//! compactor completion after slot reuse.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Segment {
    pub previous: u64,
    pub sequence: u64,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ticket {
    pub slot: usize,
    pub segment: Segment,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct State {
    pub materialized: u64,
    pub committed: u64,
    pub active: usize,
    pub segments: [Segment; 2],
}

impl State {
    pub fn validate(&self, limit: u64) -> Result<(), &'static str> {
        if limit == 0
            || self.active > 1
            || self.materialized > self.committed
            || self.committed > i64::MAX as u64
        {
            return Err("invalid journal state");
        }
        for segment in self.segments {
            if segment.bytes > limit
                || segment.previous > segment.sequence
                || segment.sequence > self.committed
                || (segment.bytes == 0) != (segment.previous == segment.sequence)
            {
                return Err("invalid journal segment");
            }
        }
        let active = self.segments[self.active];
        let sealed = self.segments[1 - self.active];
        if active.sequence != self.committed
            || sealed.previous != self.materialized
            || sealed.sequence != active.previous
        {
            return Err("broken journal sequence chain");
        }
        Ok(())
    }

    pub fn sealed(&self) -> Option<Ticket> {
        let slot = 1usize.checked_sub(self.active)?;
        let segment = self.segments[slot];
        (segment.bytes > 0).then_some(Ticket { slot, segment })
    }

    pub fn rotate(&self, limit: u64) -> Result<Self, &'static str> {
        self.validate(limit)?;
        if self.segments[self.active].bytes == 0 {
            return Err("nothing to seal");
        }
        if self.sealed().is_some() {
            return Err("journal full: compaction pending");
        }
        let mut next = *self;
        next.active = 1 - self.active;
        next.segments[next.active] = Segment {
            previous: self.committed,
            sequence: self.committed,
            bytes: 0,
        };
        next.validate(limit)?;
        Ok(next)
    }

    /// Coalesced command batches may skip sequence numbers, but cannot regress.
    pub fn append(&self, sequence: u64, bytes: u64, limit: u64) -> Result<Self, &'static str> {
        self.validate(limit)?;
        if sequence <= self.committed || sequence > i64::MAX as u64 || bytes == 0 || bytes > limit {
            return Err("invalid journal append");
        }
        let mut next = *self;
        if self.segments[self.active].bytes > limit - bytes {
            next = self.rotate(limit)?;
        }
        next.segments[next.active].bytes += bytes;
        next.segments[next.active].sequence = sequence;
        next.committed = sequence;
        next.validate(limit)?;
        Ok(next)
    }

    pub fn finish(&self, ticket: Ticket, limit: u64) -> Result<Self, &'static str> {
        self.validate(limit)?;
        if self.sealed() != Some(ticket) {
            return Err("stale compaction ticket");
        }
        let mut next = *self;
        next.materialized = ticket.segment.sequence;
        next.segments[ticket.slot] = Segment {
            previous: next.materialized,
            sequence: next.materialized,
            bytes: 0,
        };
        next.validate(limit)?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn blocked_compactor_bounds_growth_and_stale_completion_is_rejected() {
        let a = State::default().append(4, 8, 8).unwrap();
        let b = a.append(9, 8, 8).unwrap();
        let old = b.sealed().unwrap();
        assert_eq!(old.segment.sequence, 4);
        assert!(b.append(10, 1, 8).is_err());
        assert_eq!(b.materialized, 0);
        let c = b.finish(old, 8).unwrap().append(12, 1, 8).unwrap();
        assert!(c.finish(old, 8).is_err());
        let d = c.finish(c.sealed().unwrap(), 8).unwrap();
        let e = d.rotate(8).unwrap();
        assert_eq!(e.sealed().unwrap().slot, old.slot);
        assert!(e.finish(old, 8).is_err());
        let drained = e.finish(e.sealed().unwrap(), 8).unwrap();
        assert_eq!(drained.materialized, drained.committed);
        assert!(drained.segments.iter().all(|s| s.bytes == 0));
    }

    #[test]
    fn recovery_rejects_gaps_overlaps_and_impossible_accounting() {
        let state = State::default()
            .append(3, 2, 4)
            .unwrap()
            .rotate(4)
            .unwrap()
            .append(7, 2, 4)
            .unwrap();
        for field in 0..7 {
            let mut corrupt = state;
            match field {
                0 => corrupt.active = 2,
                1 => corrupt.materialized = 1,
                2 => corrupt.committed = 8,
                3 => corrupt.segments[0].bytes = 0,
                4 => corrupt.segments[1].bytes = 5,
                5 => corrupt.segments[1].previous = 2,
                _ => corrupt.segments[1].previous = 4,
            }
            assert!(corrupt.validate(4).is_err());
        }
        assert!(state.append(i64::MAX as u64 + 1, 1, 4).is_err());
        assert!(state.append(7, 1, 4).is_err());
    }

    #[test]
    fn exhaustive_schedules_preserve_recoverable_prefix_and_capacity() {
        fn visit(state: State, depth: usize) {
            state.validate(3).unwrap();
            assert!(state.segments.iter().map(|s| s.bytes).sum::<u64>() <= 6);
            if depth == 0 {
                return;
            }
            let mut candidates = vec![state.rotate(3)];
            for bytes in 1..=3 {
                candidates.push(state.append(state.committed + 2, bytes, 3));
            }
            if let Some(ticket) = state.sealed() {
                candidates.push(state.finish(ticket, 3));
            }
            for next in candidates.into_iter().flatten() {
                assert!(next.committed >= state.committed);
                assert!(next.materialized >= state.materialized);
                // A crash before commit recovers state; after commit recovers next.
                // Every allowed transaction boundary must be independently valid.
                state.validate(3).unwrap();
                visit(next, depth - 1);
            }
        }
        visit(State::default(), 8);
    }
}
