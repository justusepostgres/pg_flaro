//! Validate the durable chain before the SQL adapter exposes recovered state.
use super::{decode, rotation::State};
use crate::engine::Mutation;

pub struct Replay {
    state: State,
    cursor: u64,
    bytes: [u64; 2],
}
impl Replay {
    pub fn new(state: State, segment_limit: u64) -> Result<Self, &'static str> {
        state.validate(segment_limit)?;
        Ok(Self {
            cursor: state.materialized,
            state,
            bytes: [0; 2],
        })
    }
    /// Feed journal rows in increasing sequence order, across both slots.
    pub fn push(
        &mut self,
        slot: usize,
        previous: u64,
        sequence: u64,
        payload: &[u8],
    ) -> Result<Vec<Mutation>, &'static str> {
        let expected_slot = self
            .state
            .sealed()
            .filter(|ticket| self.cursor < ticket.segment.sequence)
            .map_or(self.state.active, |ticket| ticket.slot);
        if slot != expected_slot || previous != self.cursor || sequence <= previous {
            return Err("broken journal row chain");
        }
        let segment = self.state.segments[slot];
        if previous < segment.previous || sequence > segment.sequence {
            return Err("journal row outside segment");
        }
        let mutations = decode(payload)?;
        if mutations.is_empty() {
            return Err("empty journal row");
        }
        let accounted = mutations.iter().map(|m| m.bytes() as u64).sum::<u64>();
        let bytes = self.bytes[slot]
            .checked_add(accounted)
            .ok_or("journal accounting overflow")?;
        if bytes > segment.bytes {
            return Err("journal byte accounting mismatch");
        }
        self.cursor = sequence;
        self.bytes[slot] = bytes;
        Ok(mutations)
    }
    pub fn finish(self) -> Result<(), &'static str> {
        if self.cursor != self.state.committed
            || self.bytes != self.state.segments.map(|segment| segment.bytes)
        {
            return Err("incomplete journal recovery");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{engine::Entry, journal::encode};
    use std::collections::HashMap;

    #[test]
    fn recovery_before_and_after_compaction_has_identical_final_values() {
        let first = Mutation {
            key: b"key".as_slice().into(),
            entry: Some(Entry {
                value: b"old".as_slice().into(),
                expires_at_ms: Some(100),
            }),
        };
        let second = Mutation {
            key: first.key.clone(),
            entry: None,
        };
        let limit = 1024;
        let sealed = State::default()
            .append(3, first.bytes() as u64, limit)
            .unwrap()
            .rotate(limit)
            .unwrap();
        let ticket = sealed.sealed().unwrap();
        let during = sealed.append(8, second.bytes() as u64, limit).unwrap();
        let after = during.finish(ticket, limit).unwrap();
        for state in [during, after] {
            let mut snapshot = HashMap::new();
            let mut replay = Replay::new(state, limit).unwrap();
            if state.materialized == 3 {
                snapshot.insert(first.key.clone(), first.entry.clone());
            } else {
                for m in replay
                    .push(0, 0, 3, &encode(std::slice::from_ref(&first)).unwrap())
                    .unwrap()
                {
                    snapshot.insert(m.key, m.entry);
                }
            }
            for m in replay
                .push(1, 3, 8, &encode(std::slice::from_ref(&second)).unwrap())
                .unwrap()
            {
                snapshot.insert(m.key, m.entry);
            }
            replay.finish().unwrap();
            assert_eq!(snapshot.get(&first.key), Some(&None));
        }
    }

    #[test]
    fn missing_duplicate_wrong_slot_and_misaccounted_rows_are_rejected() {
        let mutation = Mutation {
            key: b"key".as_slice().into(),
            entry: None,
        };
        let bytes = mutation.bytes() as u64;
        let payload = encode(&[mutation]).unwrap();
        let state = State::default().append(7, bytes, 1024).unwrap();
        assert!(Replay::new(state, 1024).unwrap().finish().is_err());
        let mut replay = Replay::new(state, 1024).unwrap();
        assert!(replay.push(2, 0, 7, &payload).is_err());
        assert!(replay.push(0, 1, 7, &payload).is_err());
        assert!(replay.push(0, 0, 8, &payload).is_err());
        replay.push(0, 0, 7, &payload).unwrap();
        assert!(replay.push(0, 0, 7, &payload).is_err());
        replay.finish().unwrap();
        let overstated = State::default().append(7, bytes + 1, 1024).unwrap();
        let mut replay = Replay::new(overstated, 1024).unwrap();
        replay.push(0, 0, 7, &payload).unwrap();
        assert!(replay.finish().is_err());
        let understated = State::default().append(7, bytes - 1, 1024).unwrap();
        assert!(Replay::new(understated, 1024)
            .unwrap()
            .push(0, 0, 7, &payload)
            .is_err());
    }
}
