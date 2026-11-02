//! Typed immutable values with a versioned binary representation for persistence.
use crate::engine::Bytes;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, VecDeque},
    ops::Deref,
    sync::Arc,
};
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Score(pub f64);
impl Eq for Score {}
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SortedSet {
    pub scores: BTreeMap<Bytes, Score>,
    pub order: BTreeSet<(Score, Bytes)>,
}
impl SortedSet {
    pub fn insert(&mut self, member: Bytes, score: f64) {
        let score = Score(if score == 0.0 { 0.0 } else { score });
        if let Some(old) = self.scores.insert(member.clone(), score) {
            self.order.remove(&(old, member.clone()));
        }
        self.order.insert((score, member));
    }
    pub fn remove(&mut self, member: &[u8]) -> bool {
        if let Some((member, score)) = self.scores.remove_entry(member) {
            self.order.remove(&(score, member));
            true
        } else {
            false
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Collection {
    Hash(BTreeMap<Bytes, Bytes>),
    List(VecDeque<Bytes>),
    Set(BTreeSet<Bytes>),
    ZSet(SortedSet),
}
impl Collection {
    pub fn empty(kind: u8) -> Self {
        match kind {
            1 => Self::Hash(BTreeMap::new()),
            2 => Self::List(VecDeque::new()),
            3 => Self::Set(BTreeSet::new()),
            4 => Self::ZSet(SortedSet::default()),
            _ => unreachable!(),
        }
    }
    pub fn kind(&self) -> u8 {
        match self {
            Self::Hash(_) => 1,
            Self::List(_) => 2,
            Self::Set(_) => 3,
            Self::ZSet(_) => 4,
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Self::Hash(v) => v.len(),
            Self::List(v) => v.len(),
            Self::Set(v) => v.len(),
            Self::ZSet(v) => v.scores.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    String(Bytes),
    Collection {
        data: Arc<Collection>,
        encoded: Bytes,
        memory: usize,
    },
}
impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Self::String(v.into())
    }
}
impl From<&[u8]> for Value {
    fn from(v: &[u8]) -> Self {
        Self::String(v.into())
    }
}
impl From<Bytes> for Value {
    fn from(v: Bytes) -> Self {
        Self::String(v)
    }
}
impl Deref for Value {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::String(v) => v,
            Self::Collection { encoded, .. } => encoded,
        }
    }
}
impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self
    }
}
impl Value {
    pub fn kind(&self) -> u8 {
        match self {
            Self::String(_) => 0,
            Self::Collection { data, .. } => data.kind(),
        }
    }
    pub fn type_name(&self) -> &'static str {
        ["string", "hash", "list", "set", "zset"][self.kind() as usize]
    }
    pub fn collection(&self) -> Option<&Collection> {
        match self {
            Self::Collection { data, .. } => Some(data),
            _ => None,
        }
    }
    pub fn memory_bytes(&self) -> usize {
        match self {
            Self::String(v) => v.len(),
            Self::Collection { memory, .. } => *memory,
        }
    }
    pub fn from_collection(data: Collection) -> Result<Self, &'static str> {
        if data.is_empty() {
            return Err("empty collections must delete their key");
        }
        let mut encoded = b"COL1".to_vec();
        encoded.extend_from_slice(&(data.len() as u32).to_le_bytes());
        let mut bytes = 0;
        let mut field = |value: &[u8]| {
            bytes += value.len();
            encoded.extend_from_slice(&(value.len() as u32).to_le_bytes());
            encoded.extend_from_slice(value);
        };
        match &data {
            Collection::Hash(v) => {
                for (k, v) in v {
                    field(k);
                    field(v);
                }
            }
            Collection::List(v) => {
                for v in v {
                    field(v);
                }
            }
            Collection::Set(v) => {
                for v in v {
                    field(v);
                }
            }
            Collection::ZSet(v) => {
                for (member, score) in &v.scores {
                    field(member);
                    field(&score.0.to_le_bytes());
                }
            }
        }
        if encoded.len() > crate::protocol::MAX_FRAME {
            return Err("collection exceeds maximum encoded size");
        }
        let memory = encoded.len() + bytes + data.len() * 160;
        Ok(Self::Collection {
            memory,
            data: Arc::new(data),
            encoded: encoded.into(),
        })
    }
    pub fn from_stored(kind: u8, bytes: &[u8]) -> Result<Self, &'static str> {
        if kind == 0 {
            return Ok(bytes.into());
        }
        if !(1..=4).contains(&kind)
            || bytes.len() > crate::protocol::MAX_FRAME
            || bytes.get(..4) != Some(b"COL1")
        {
            return Err("invalid collection header");
        }
        let count = u32::from_le_bytes(
            bytes
                .get(4..8)
                .ok_or("truncated collection")?
                .try_into()
                .unwrap(),
        ) as usize;
        if count == 0 || count > bytes.len() / 4 {
            return Err("invalid collection count");
        }
        let mut pos = 8;
        let mut field = || -> Result<Bytes, &'static str> {
            let n = u32::from_le_bytes(
                bytes
                    .get(pos..pos + 4)
                    .ok_or("truncated field length")?
                    .try_into()
                    .unwrap(),
            ) as usize;
            pos += 4;
            let end = pos.checked_add(n).ok_or("field overflow")?;
            let result = bytes.get(pos..end).ok_or("truncated field")?.into();
            pos = end;
            Ok(result)
        };
        let mut data = Collection::empty(kind);
        for _ in 0..count {
            match &mut data {
                Collection::Hash(v) => {
                    let k = field()?;
                    let value = field()?;
                    if v.insert(k, value).is_some() {
                        return Err("duplicate hash field");
                    }
                }
                Collection::List(v) => v.push_back(field()?),
                Collection::Set(v) => {
                    if !v.insert(field()?) {
                        return Err("duplicate set member");
                    }
                }
                Collection::ZSet(v) => {
                    let member = field()?;
                    let raw = field()?;
                    let score = f64::from_le_bytes(
                        raw.as_ref()
                            .try_into()
                            .map_err(|_| "invalid score encoding")?,
                    );
                    if score.is_nan() || v.scores.contains_key(&member) {
                        return Err("invalid sorted set member");
                    }
                    v.insert(member, score);
                }
            }
        }
        if pos != bytes.len() {
            return Err("trailing collection data");
        }
        Self::from_collection(data)
    }
}
