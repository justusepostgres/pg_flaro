//! Hash fields and optional absolute millisecond expiration metadata.
use super::Bytes;
use std::{
    borrow::Borrow,
    collections::{BTreeSet, HashMap},
    hash::Hash as HashKey,
    ops::Deref,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Hash {
    fields: HashMap<Bytes, Bytes>,
    expirations: HashMap<Bytes, i64>,
    deadlines: BTreeSet<(i64, Bytes)>,
}
impl Deref for Hash {
    type Target = HashMap<Bytes, Bytes>;
    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}
impl FromIterator<(Bytes, Bytes)> for Hash {
    fn from_iter<T: IntoIterator<Item = (Bytes, Bytes)>>(values: T) -> Self {
        Self {
            fields: values.into_iter().collect(),
            expirations: HashMap::new(),
            deadlines: BTreeSet::new(),
        }
    }
}
impl Hash {
    /// Replacing a field with HSET clears its field expiration.
    pub fn insert(&mut self, field: Bytes, value: Bytes) -> Option<Bytes> {
        self.clear_expiration(&field);
        self.fields.insert(field, value)
    }
    /// Numeric increments preserve existing field expiration.
    pub fn insert_preserving_expiry(&mut self, field: Bytes, value: Bytes) -> Option<Bytes> {
        self.fields.insert(field, value)
    }
    pub fn remove<Q>(&mut self, field: &Q) -> Option<Bytes>
    where
        Bytes: Borrow<Q>,
        Q: ?Sized + HashKey + Eq,
    {
        self.clear_expiration(field);
        self.fields.remove(field)
    }
    pub fn expiration(&self, field: &[u8]) -> Option<i64> {
        self.expirations.get(field).copied()
    }
    fn clear_expiration<Q>(&mut self, field: &Q)
    where
        Bytes: Borrow<Q>,
        Q: ?Sized + HashKey + Eq,
    {
        if let Some((stored, deadline)) = self.expirations.remove_entry(field) {
            self.deadlines.remove(&(deadline, stored));
        }
    }
    /// Returns false for a missing field; metadata never creates orphan fields.
    pub fn set_expiration(&mut self, field: &[u8], deadline: Option<i64>) -> bool {
        let Some((stored, _)) = self.fields.get_key_value(field) else {
            return false;
        };
        if self.expiration(field) == deadline {
            return true;
        }
        let stored = stored.clone();
        self.clear_expiration(field);
        if let Some(deadline) = deadline {
            self.expirations.insert(stored.clone(), deadline);
            self.deadlines.insert((deadline, stored));
        }
        true
    }
    pub fn next_expiration(&self) -> Option<i64> {
        self.deadlines.first().map(|(deadline, _)| *deadline)
    }
    pub fn has_live_fields(&self, now: i64) -> bool {
        self.fields.len() > self.expirations.len()
            || self
                .deadlines
                .last()
                .is_some_and(|(deadline, _)| *deadline > now)
    }
    pub fn remove_expired(&mut self, now: i64) -> bool {
        let mut changed = false;
        while self
            .next_expiration()
            .is_some_and(|deadline| deadline <= now)
        {
            let (_, field) = self.deadlines.pop_first().unwrap();
            self.expirations.remove(&field);
            self.fields.remove(&field);
            changed = true;
        }
        changed
    }
    pub fn has_expirations(&self) -> bool {
        !self.expirations.is_empty()
    }
    pub(super) fn expiration_count(&self) -> usize {
        self.expirations.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{Engine, Entry},
        protocol::Reply,
        value::{Collection, Value},
    };
    #[test]
    fn deadline_index_matches_independent_model_through_mixed_mutations() {
        let mut hash = Hash::default();
        let mut model = HashMap::<Bytes, (Bytes, Option<i64>)>::new();
        let mut seed = 123456789u64;
        for step in 0..5000 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let field: Bytes = format!("field:{}", (seed >> 32) % 64).into_bytes().into();
            let value: Bytes = step.to_string().into_bytes().into();
            let now = step / 10;
            match seed % 6 {
                0 => {
                    hash.insert(field.clone(), value.clone());
                    model.insert(field.clone(), (value, None));
                }
                1 => {
                    let deadline = Some(now + ((seed >> 40) % 20) as i64);
                    assert_eq!(
                        hash.set_expiration(&field, deadline),
                        model.contains_key(&field)
                    );
                    if let Some((_, expiry)) = model.get_mut(&field) {
                        *expiry = deadline;
                    }
                }
                2 => {
                    hash.set_expiration(&field, None);
                    if let Some((_, expiry)) = model.get_mut(&field) {
                        *expiry = None;
                    }
                }
                3 => {
                    hash.remove(&field);
                    model.remove(&field);
                }
                4 => {
                    hash.insert_preserving_expiry(field.clone(), value.clone());
                    model
                        .entry(field)
                        .and_modify(|(v, _)| *v = value.clone())
                        .or_insert((value, None));
                }
                _ => {
                    let before = model.len();
                    model.retain(|_, (_, deadline)| deadline.is_none_or(|d| d > now));
                    assert_eq!(hash.remove_expired(now), before != model.len());
                }
            }
            assert_eq!(hash.len(), model.len());
            for (field, (value, deadline)) in &model {
                assert_eq!(hash.get(field), Some(value));
                assert_eq!(hash.expiration(field), *deadline);
            }
            let deadlines: BTreeSet<_> = model
                .iter()
                .filter_map(|(field, (_, deadline))| deadline.map(|d| (d, field.clone())))
                .collect();
            assert_eq!(hash.deadlines, deadlines);
            assert_eq!(
                hash.next_expiration(),
                model.values().filter_map(|(_, d)| *d).min()
            );
            assert_eq!(
                hash.has_live_fields(now),
                model.values().any(|(_, d)| d.is_none_or(|d| d > now))
            );
            if step % 100 == 0 {
                let snapshot = hash.clone();
                hash.remove_expired(i64::MAX);
                hash = snapshot;
                assert_eq!(hash.deadlines, deadlines);
            }
        }
    }
    fn fixture() -> Value {
        let mut hash: Hash = [
            (Bytes::from(&b"field\0"[..]), Bytes::from(&b"1"[..])),
            (Bytes::from(&b"plain"[..]), Bytes::from(&b"value\xff"[..])),
        ]
        .into_iter()
        .collect();
        assert!(hash.set_expiration(b"field\0", Some(4_102_444_800_000)));
        assert!(!hash.set_expiration(b"absent", Some(0)));
        Value::from_collection(Collection::Hash(hash)).unwrap()
    }
    #[test]
    fn timestamp_format_roundtrip_and_strict_decoding() {
        let value = fixture();
        assert_eq!(&value.as_ref()[..4], b"COL2");
        assert_eq!(value.len(), value.as_ref().len());
        let restored = Value::from_stored(1, value.as_ref()).unwrap();
        assert_eq!(value, restored);
        assert_eq!(value.memory_bytes(), restored.memory_bytes());
        for length in 0..value.len() {
            assert!(Value::from_stored(1, &value.as_ref()[..length]).is_err());
        }
        for kind in 2..=4 {
            assert!(Value::from_stored(kind, value.as_ref()).is_err());
        }
        let mut trailing = value.as_ref().to_vec();
        trailing.push(0);
        assert!(Value::from_stored(1, &trailing).is_err());
        let Collection::Hash(mut hash) = restored.collection().unwrap().clone() else {
            unreachable!()
        };
        hash.set_expiration(b"field\0", None);
        let plain = Value::from_collection(Collection::Hash(hash)).unwrap();
        assert_eq!(&plain.as_ref()[..4], b"COL1");
        assert_eq!(Value::from_stored(1, plain.as_ref()).unwrap(), plain);
        assert!(plain.memory_bytes() < value.memory_bytes());
    }
    #[test]
    fn writes_preserve_or_remove_metadata_and_rollback_restores_it() {
        let mut engine = Engine::new(1 << 20);
        engine
            .restore(
                b"hash".to_vec(),
                Entry {
                    value: fixture(),
                    expires_at_ms: Some(5000),
                },
                0,
            )
            .unwrap();
        let before = engine.snapshot(b"hash");
        let command = [b"HSET".as_slice(), b"hash", b"field\0", b"2"];
        assert!(matches!(
            engine.execute(&command, 1, |_| false),
            Reply::Error(_)
        ));
        assert_eq!(engine.snapshot(b"hash"), before);
        assert!(engine
            .transact(
                |engine, submit| vec![engine.execute(&command, 1, submit)],
                |_| false
            )
            .is_err());
        assert_eq!(engine.snapshot(b"hash"), before);
        for command in [
            vec![b"HINCRBY".as_slice(), b"hash", b"field\0", b"1"],
            vec![b"HINCRBYFLOAT", b"hash", b"field\0", b"0.5"],
            vec![b"HSETNX", b"hash", b"field\0", b"ignored"],
        ] {
            engine.execute(&command, 1, |_| true);
            let value = &engine.get(b"hash", 1).unwrap().value;
            let Collection::Hash(hash) = value.collection().unwrap() else {
                unreachable!()
            };
            assert_eq!(hash.expiration(b"field\0"), Some(4_102_444_800_000));
            assert_eq!(value.len(), value.as_ref().len());
        }
        assert_eq!(engine.execute(&command, 1, |_| true), Reply::Integer(0));
        let entry = engine.get(b"hash", 1).unwrap();
        assert_eq!(entry.expires_at_ms, Some(5000));
        assert_eq!(&entry.value.as_ref()[..4], b"COL1");
        assert_eq!(entry.value.len(), entry.value.as_ref().len());
        assert_eq!(
            entry.value.memory_bytes(),
            Value::from_collection(entry.value.collection().unwrap().clone())
                .unwrap()
                .memory_bytes()
        );
    }
    #[test]
    fn deleting_fields_never_leaves_orphan_deadlines() {
        let Collection::Hash(mut hash) = fixture().collection().unwrap().clone() else {
            unreachable!()
        };
        let snapshot = hash.clone();
        hash.remove(&b"field\0"[..]);
        assert!(!hash.has_expirations());
        assert_eq!(snapshot.expiration(b"field\0"), Some(4_102_444_800_000));
        hash.insert(b"field\0".as_slice().into(), b"new".as_slice().into());
        assert_eq!(hash.expiration(b"field\0"), None);
    }
}
